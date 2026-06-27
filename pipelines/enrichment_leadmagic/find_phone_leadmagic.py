"""Standalone LeadMagic Mobile Finder — direct-mobile enrichment, the LeadMagic twin of
``pipelines/enrichment_blitz/enrich_phone_standalone.py``.

Resolves a direct **mobile phone number** via LeadMagic's ``POST /mobile-finder`` from a
known identity (``person_linkedin_url`` → ``profile_url``, or ``work_email``). Endpoint-less
Modal worker, spawned by the Universal Dispatcher and woken via the Trigger waitpoint
callback. Same SoR/ledger shape as the Blitz phone finder, with two deliberate differences:

  1. NO gateway. Blitz egress is governed by the single-container ``blitz_gateway`` (≤5 RPS).
     LeadMagic is a separate vendor with its own limits, so this worker holds the LeadMagic
     key (Modal secret ``leadmagic-api`` → ``LEADMAGIC_API_KEY``) and calls LeadMagic
     directly, with per-call retry/backoff on 429/5xx.
  2. NO US-only gate. LeadMagic has international mobile coverage AND only charges on a HIT
     (``credits_consumed=0`` on a miss), so there is no credit reason to pre-skip non-US —
     every identity is attempted; misses are free.

SINK. Latest-wins upsert per ``contact_id`` into ``ops.phone_resolutions`` — the SAME
mobile-phone system-of-record the Blitz finder writes, so LeadMagic- and Blitz-found phones
coexist (``source_vendor`` distinguishes) and flow through the existing
``materialize_phone_resolutions`` materializer with no new path. The verbatim LeadMagic
response is stored in ``leadmagic_raw`` (added idempotently); ``phone`` / ``phone_status`` /
``phone_type`` are a convenience projection on top. Per-run terminal state into
``ops.leadmagic_phone_finder_runs``.

IDEMPOTENCY. ``force=False`` (default) skips any contact already resolved to a FOUND phone
by ANY vendor — so re-firing never re-spends on a settled contact, and a LeadMagic run over
Blitz-``unresolved`` contacts (e.g. the 401-locked batch) re-attempts only those.

    modal deploy pipelines/enrichment_leadmagic/find_phone_leadmagic.py
    modal run    pipelines/enrichment_leadmagic/find_phone_leadmagic.py::init_ops
    modal run    pipelines/enrichment_leadmagic/find_phone_leadmagic.py::run_manual \\
                 --contacts-json '[{"contact_id":"c1","person_linkedin_url":"https://www.linkedin.com/in/x"}]'
"""

from __future__ import annotations

import datetime as dt
import os
import re
import time
import uuid
from typing import Any

import modal

FEED = "leadmagic_phone_finder"
LM_URL = "https://api.leadmagic.io/mobile-finder"
HTTP_TIMEOUT = 30.0
VALID_PRIORITIES = {"low", "normal"}   # carried for ledger parity; LeadMagic has no lane

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "psycopg[binary]>=3.2",   # ops.phone_resolutions + ops.leadmagic_phone_finder_runs
    "requests>=2.32",         # LeadMagic API + Trigger callback
)

app = modal.App("leadmagic-phone-finder", image=image)

# hqx-postgres → ops sink; leadmagic-api → LEADMAGIC_API_KEY (the worker holds the key —
# there is no gateway for LeadMagic the way blitz_gateway fronts Blitz).
SECRETS = [
    modal.Secret.from_name("hqx-postgres"),
    modal.Secret.from_name("leadmagic-api"),
]

# ── ops DDL — idempotent; shares ops.phone_resolutions with the Blitz finder, adds a
# leadmagic_raw column + a dedicated LeadMagic run ledger. ─────────────────────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.phone_resolutions (
    contact_id          text        PRIMARY KEY,
    phone               text,
    phone_status        text        NOT NULL,
    source_vendor       text,
    phone_type          text,
    company_domain      text,
    person_linkedin_url text,
    country_code        text,
    blitz_phone_raw     jsonb,
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    batch_label         text,
    resolved_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT phone_resolutions_status_chk CHECK (phone_status IN ('found', 'unresolved'))
);
-- LeadMagic raw response stored VERBATIM (the source of truth; derived columns are a projection).
ALTER TABLE ops.phone_resolutions ADD COLUMN IF NOT EXISTS leadmagic_raw jsonb;
ALTER TABLE ops.phone_resolutions ADD COLUMN IF NOT EXISTS phone_type    text;
ALTER TABLE ops.phone_resolutions ADD COLUMN IF NOT EXISTS country_code  text;
CREATE INDEX IF NOT EXISTS phone_resolutions_status_idx ON ops.phone_resolutions (phone_status);
CREATE INDEX IF NOT EXISTS phone_resolutions_domain_idx ON ops.phone_resolutions (company_domain);
CREATE INDEX IF NOT EXISTS phone_resolutions_phone_idx  ON ops.phone_resolutions (phone);

CREATE TABLE IF NOT EXISTS ops.leadmagic_phone_finder_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,
    batch_label      text,
    run_root         text,
    priority         text        NOT NULL,
    requested        bigint      NOT NULL DEFAULT 0,
    skipped          bigint      NOT NULL DEFAULT 0,
    found            bigint      NOT NULL DEFAULT 0,
    unresolved       bigint      NOT NULL DEFAULT 0,
    failed           bigint      NOT NULL DEFAULT 0,
    api_calls        bigint      NOT NULL DEFAULT 0,
    credits_consumed bigint      NOT NULL DEFAULT 0,
    status           text        NOT NULL,
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT leadmagic_phone_finder_runs_status_chk   CHECK (status   IN ('success', 'error')),
    CONSTRAINT leadmagic_phone_finder_runs_priority_chk CHECK (priority IN ('low', 'normal'))
);
CREATE INDEX IF NOT EXISTS leadmagic_phone_finder_runs_feed_idx        ON ops.leadmagic_phone_finder_runs (feed);
CREATE INDEX IF NOT EXISTS leadmagic_phone_finder_runs_recorded_at_idx ON ops.leadmagic_phone_finder_runs (recorded_at DESC);
"""

_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")


def _normalize_domain(raw: str | None) -> str | None:
    d = (raw or "").strip().lower()
    d = _WWW.sub("", _SCHEME.sub("", d)).split("/", 1)[0].rstrip(".")
    return d or None


def _hqx_dsn() -> str:
    """Transaction-mode (Supavisor :6543) hq-x DSN — same rationale as the Blitz finder:
    a long-lived-but-idle worker must not pin a session-pool backend for the whole run."""
    dsn = os.environ.get("HQX_DB_URL_TRANSACTION")
    if not dsn:
        pooled = os.environ.get("HQX_DB_URL_POOLED")
        if not pooled:
            raise RuntimeError("Neither HQX_DB_URL_TRANSACTION nor HQX_DB_URL_POOLED set in hqx-postgres.")
        dsn = pooled.replace(".pooler.supabase.com:5432", ".pooler.supabase.com:6543")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _open_conn(dsn: str):
    import psycopg
    return psycopg.connect(dsn, autocommit=True, prepare_threshold=None)


# ── LeadMagic mobile-finder call (direct; retry on 429/5xx) ───────────────────
def _lm_call(key: str, body: dict) -> tuple[Any, bool, int | None, str | None]:
    """POST /mobile-finder. Returns (json_or_text, ok, http_status, error). Retries 429/5xx
    with backoff (LeadMagic rate-limits per-key; chunk-workers run concurrently)."""
    import requests

    hdr = {"X-API-Key": key, "Content-Type": "application/json"}
    last_err = None
    for i in range(5):
        try:
            r = requests.post(LM_URL, headers=hdr, json=body, timeout=HTTP_TIMEOUT)
            if r.status_code == 429 or r.status_code >= 500:
                last_err = f"http_{r.status_code}"
                time.sleep(min(2 ** i, 20))
                continue
            try:
                data = r.json()
            except Exception:  # noqa: BLE001
                data = {"_raw_text": r.text[:500]}
            return data, (r.status_code < 300), r.status_code, (None if r.status_code < 300 else data)
        except Exception as exc:  # noqa: BLE001
            last_err = f"{type(exc).__name__}: {exc}"
            time.sleep(min(2 ** i, 20))
    return None, False, None, last_err


def _make_result(c: dict, phone: str | None, status: str, vendor: str | None,
                 phone_type: str | None, attempts: list, lm_raw: Any = None) -> dict:
    return {
        "contact_id": c.get("contact_id"),
        "phone": phone,
        "phone_status": status,
        "source_vendor": vendor,
        "phone_type": phone_type,
        "company_domain": _normalize_domain(c.get("company_domain")),
        "person_linkedin_url": (c.get("person_linkedin_url") or "").strip() or None,
        "country_code": (c.get("country_code") or "").strip().upper() or None,
        "leadmagic_raw": lm_raw,       # LeadMagic /mobile-finder payload, VERBATIM
        "attempts": attempts,
    }


def _resolve_contact(c: dict, key: str, counts: dict) -> dict:
    """One identity → a found/unresolved mobile. LeadMagic /mobile-finder keys on
    ``profile_url`` (LinkedIn) or ``work_email``/``personal_email``. No US gate (intl
    coverage; misses are free). credits_consumed is tracked from the response."""
    purl = (c.get("person_linkedin_url") or "").strip() or None
    wemail = (c.get("work_email") or "").strip() or None
    pemail = (c.get("personal_email") or "").strip() or None
    attempts: list[dict] = []

    if not (purl or wemail or pemail):
        attempts.append({"vendor": "leadmagic", "outcome": "skipped",
                         "reason": "need profile_url, work_email, or personal_email"})
        return _make_result(c, None, "unresolved", None, None, attempts)

    body: dict = {}
    if purl:
        body["profile_url"] = purl
    if wemail:
        body["work_email"] = wemail
    if pemail:
        body["personal_email"] = pemail

    counts["api_calls"] += 1
    raw, ok, http_status, err = _lm_call(key, body)
    data = raw if isinstance(raw, dict) else {}
    counts["credits_consumed"] += int(data.get("credits_consumed") or 0)
    phone = (str(data.get("mobile_number")).strip() or None) if data.get("mobile_number") else None
    found = bool(ok) and phone is not None

    if not found:
        attempts.append({"vendor": "leadmagic", "outcome": "miss", "ok": ok,
                         "http_status": http_status, "error": err,
                         "message": data.get("message")})
        return _make_result(c, None, "unresolved", None, None, attempts, lm_raw=raw)

    attempts.append({"vendor": "leadmagic", "outcome": "hit", "phone": phone,
                     "credits": data.get("credits_consumed")})
    return _make_result(c, phone, "found", "leadmagic", "mobile", attempts, lm_raw=raw)


# ── Sink writers ──────────────────────────────────────────────────────────────
def _upsert_resolution(cur, r: dict, batch_label: str | None) -> None:
    from psycopg.types.json import Jsonb

    def _j(v):
        return Jsonb(v) if v is not None else None

    # Note: blitz_phone_raw is intentionally NOT touched here — a LeadMagic update over an
    # existing row preserves any prior Blitz payload; LeadMagic's payload lands in leadmagic_raw.
    cur.execute(
        """
        INSERT INTO ops.phone_resolutions
            (contact_id, phone, phone_status, source_vendor, phone_type,
             company_domain, person_linkedin_url, country_code, leadmagic_raw,
             attempts, batch_label, resolved_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (contact_id) DO UPDATE SET
            phone               = EXCLUDED.phone,
            phone_status        = EXCLUDED.phone_status,
            source_vendor       = EXCLUDED.source_vendor,
            phone_type          = EXCLUDED.phone_type,
            company_domain      = EXCLUDED.company_domain,
            person_linkedin_url = EXCLUDED.person_linkedin_url,
            country_code        = EXCLUDED.country_code,
            leadmagic_raw       = EXCLUDED.leadmagic_raw,
            attempts            = EXCLUDED.attempts,
            batch_label         = EXCLUDED.batch_label,
            resolved_at         = now()
        """,
        (r["contact_id"], r["phone"], r["phone_status"], r["source_vendor"],
         r["phone_type"], r["company_domain"], r["person_linkedin_url"],
         r["country_code"], _j(r.get("leadmagic_raw")), Jsonb(r["attempts"]), batch_label),
    )


def _record_run(cur, batch_label, run_root, priority, counts, status, error,
                started_at, completed_at) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """
        INSERT INTO ops.leadmagic_phone_finder_runs
            (feed, batch_label, run_root, priority, requested, skipped, found,
             unresolved, failed, api_calls, credits_consumed, status, error,
             started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, batch_label, run_root, priority, counts["requested"], counts["skipped"],
         counts["found"], counts["unresolved"], counts["failed"], counts["api_calls"],
         counts["credits_consumed"], status, error, started_at, completed_at),
    )


def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


def _already_found(cur, contact_ids: list[str]) -> set[str]:
    """Skip-set: contacts already resolved to a FOUND phone by ANY vendor (cross-vendor
    idempotency — never re-spend or clobber a settled contact unless force=True)."""
    if not contact_ids:
        return set()
    try:
        cur.execute(
            "SELECT contact_id FROM ops.phone_resolutions "
            "WHERE phone_status = 'found' AND contact_id = ANY(%s)", (contact_ids,))
        return {r[0] for r in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: skip-set lookup failed ({exc}); proceeding without skip.")
        return set()


def _run(contacts, batch_label, run_id, priority, force, trigger_callback_url) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    priority = priority if priority in VALID_PRIORITIES else "low"
    counts = {"requested": 0, "skipped": 0, "found": 0, "unresolved": 0,
              "failed": 0, "api_calls": 0, "credits_consumed": 0}
    status, error = "error", None
    key = os.environ.get("LEADMAGIC_API_KEY")
    if not key:
        raise RuntimeError("LEADMAGIC_API_KEY not set in the leadmagic-api Modal secret.")
    dsn = _hqx_dsn()
    try:
        contacts = [c for c in (contacts or []) if isinstance(c, dict) and c.get("contact_id")]
        counts["requested"] = len(contacts)
        conn = _open_conn(dsn)
        try:
            cur = conn.cursor()
            cur.execute(OPS_DDL)
            skip = set() if force else _already_found(cur, [c["contact_id"] for c in contacts])
            for c in contacts:
                if c["contact_id"] in skip:
                    counts["skipped"] += 1
                    continue
                try:
                    r = _resolve_contact(c, key, counts)
                    _upsert_resolution(cur, r, batch_label)
                    counts[r["phone_status"]] += 1
                except Exception as exc:  # noqa: BLE001 — one contact must not sink the batch
                    counts["failed"] += 1
                    print(f"WARN: contact {c.get('contact_id')!r} failed: {exc}")
            status = "success"
            _record_run(cur, batch_label, run_root, priority, counts, status, None,
                        started_at, dt.datetime.now(dt.timezone.utc))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
        try:
            conn2 = _open_conn(dsn)
            try:
                _record_run(conn2.cursor(), batch_label, run_root, priority, counts, status,
                            error, started_at, dt.datetime.now(dt.timezone.utc))
            finally:
                conn2.close()
        except Exception as exc2:  # noqa: BLE001
            print(f"WARN: ops.leadmagic_phone_finder_runs write failed: {exc2}")
    finally:
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": FEED, "batch_label": batch_label,
                        "error": error, **counts})
    if status != "success":
        raise RuntimeError(f"leadmagic_phone_finder failed: {error}")
    return {"feed": FEED, "status": status, "batch_label": batch_label, **counts}


@app.function(secrets=SECRETS, timeout=60 * 60, memory=2048, cpu=1.0)
def run_leadmagic_phone(contacts: list[dict], batch_label: str | None = None,
                        run_id: str | None = None, priority: str = "low", force: bool = False,
                        trigger_callback_url: str | None = None) -> dict:
    """Resolve a chunk of identities to direct mobile phones via LeadMagic /mobile-finder.
    Each ``contact``::

        {contact_id, person_linkedin_url? , work_email?, personal_email?,
         company_domain?, country_code?, first_name?, last_name?, company_name?}

    Requires one of person_linkedin_url / work_email / personal_email. No US gate (LeadMagic
    has intl coverage; misses cost 0 credits). ``force=False`` skips contacts already FOUND
    by any vendor."""
    return _run(contacts, batch_label, run_id, priority, force, trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.phone_resolutions (+ leadmagic_raw) + ops.leadmagic_phone_finder_runs."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"tables": ["ops.phone_resolutions", "ops.leadmagic_phone_finder_runs"]}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def verify(limit: int = 8) -> dict:
    """Read-back: latest LeadMagic-sourced phone resolutions + status histogram."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""SELECT contact_id, phone, phone_status, source_vendor, resolved_at
                       FROM ops.phone_resolutions WHERE source_vendor='leadmagic'
                       ORDER BY resolved_at DESC LIMIT %s""", (limit,))
        rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
        cur.execute("""SELECT phone_status, count(*) FROM ops.phone_resolutions
                       WHERE source_vendor='leadmagic' GROUP BY 1""")
        hist = dict(cur.fetchall())
    finally:
        conn.close()
    return {"recent": rows, "status_histogram": hist}


@app.local_entrypoint()
def init_ops() -> None:
    import json
    print(json.dumps(apply_ops_ddl.remote(), indent=2, default=str))


@app.local_entrypoint()
def run_manual(contacts_json: str = "[]", batch_label: str = "manual", force: bool = False) -> None:
    import json
    contacts = json.loads(contacts_json)
    print(json.dumps(run_leadmagic_phone.remote(contacts, batch_label=batch_label, force=force),
                     indent=2, default=str))
