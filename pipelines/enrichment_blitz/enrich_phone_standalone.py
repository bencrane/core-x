"""Standalone Blitz Phone Finder — gateway-routed direct-mobile enrichment.

A single-purpose, dedicated mobile-phone enrichment pipeline: take a known identity
(``person_linkedin_url``) and resolve a direct **mobile phone number** via BlitzAPI's
``/v2/enrichment/phone`` endpoint. Endpoint-less Modal worker, spawned by the Universal
Dispatcher and woken via the Trigger waitpoint callback. The structural twin of
``enrich_email_standalone.py`` — same gateway egress, same fan-out, same SoR/ledger
shape — with the email pipeline's MillionVerifier gate removed.

WHY NO MILLIONVERIFIER. Blitz phone enrichment returns a direct mobile number and is
itself terminal — there is no second-vendor deliverability arbiter the way MV gates
email. Outcome is binary: ``found`` (Blitz returned a number) vs ``unresolved`` (Blitz
miss, a non-US skip, or a contact with no ``person_linkedin_url``).

US-ONLY COVERAGE. Blitz phone is **United States only** (~90-95% of US mobiles; no
international coverage). A contact carrying ``country_code`` other than ``US`` is recorded
``unresolved`` WITHOUT a Blitz call (credit-free skip; the Blitz docs recommend gating on
country before calling). A contact with no ``country_code`` is attempted (Blitz simply
returns a miss for non-US identities).

DECOUPLED EGRESS (Directive 23/28 §1 — strict IPC). This worker holds **NO Blitz key**
and makes **NO direct ``api.blitz-api.ai`` calls**. Every Blitz phone call is routed
through the single-container ``core/blitz_gateway.py`` (``blitz-gateway`` app, fn
``blitz_call``) — the authoritative global ≤5-RPS priority egress. These bulk enrichment
calls ride the **LOW** (or NORMAL) lane so interactive GTM tasks (HIGH) are never starved.
The worker implements NO Blitz rate logic — the gateway governs.

SINK. Latest-wins upsert per ``contact_id`` into ``ops.phone_resolutions`` — the
mobile-phone system-of-record owned by this pipeline. Per-run terminal state into
``ops.blitz_phone_finder_runs``.

RAW PAYLOAD PRESERVATION. The upstream response is persisted **verbatim, exactly as-is,
with no interpretation imposed**: ``blitz_phone_raw`` holds Blitz's full
``/v2/enrichment/phone`` payload. That raw column is the source of truth; ``phone`` /
``phone_status`` / ``phone_type`` are a convenience projection ON TOP of it, never a
replacement. A missed or skipped contact still has its raw Blitz payload saved when a
call was made (only the derived ``phone`` is nulled).

    modal deploy pipelines/enrichment_blitz/enrich_phone_standalone.py
    modal run    pipelines/enrichment_blitz/enrich_phone_standalone.py::init_ops
    modal run    pipelines/enrichment_blitz/enrich_phone_standalone.py::run_manual \\
                 --contacts-json '[{"contact_id":"c1","person_linkedin_url":"https://www.linkedin.com/in/x","country_code":"US"}]'
"""

from __future__ import annotations

import datetime as dt
import os
import re
import time
import uuid
from typing import Any

import modal

FEED = "blitz_phone_finder"

# ── Gateway coordinates (Directive 23) — the ONLY Blitz egress. ──────────────
GATEWAY_APP, GATEWAY_FN = "blitz-gateway", "blitz_call"
PHONE_PATH = "/v2/enrichment/phone"           # raw path (gateway maps logical names OR raw /v2/…)
# Bulk enrichment lanes only — HIGH is reserved for interactive GTM (never starve it).
VALID_PRIORITIES = {"low", "normal"}

HTTP_TIMEOUT = 30.0

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "psycopg[binary]>=3.2",  # ops.phone_resolutions + ops.blitz_phone_finder_runs
    "requests>=2.32",        # Trigger callback
)

app = modal.App("blitz-phone-finder", image=image)

SECRETS = [
    modal.Secret.from_name("hqx-postgres"),   # ops.phone_resolutions + ops.blitz_phone_finder_runs
]
# NOTE: the `blitz-api` secret is deliberately ABSENT — the worker never holds the
# Blitz key. All Blitz egress is the gateway's concern (Directive 23/28 §1). There is
# NO email-cascade secret here either — phone enrichment has no MillionVerifier gate.

# ── ops DDL — verbatim mirror of the .sql sibling, applied idempotently before each
# terminal write. ops.phone_resolutions is the mobile-phone system-of-record owned by
# this pipeline; ops.blitz_phone_finder_runs is its dedicated run-state ledger. ─
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
    CONSTRAINT phone_resolutions_status_chk
        CHECK (phone_status IN ('found', 'unresolved'))
);
-- Forward-compatible upgrades for an existing instance. The Blitz response is stored
-- VERBATIM in blitz_phone_raw — no interpretation imposed; the derived columns above
-- (phone / phone_status / phone_type) are a convenience projection ON TOP of it.
ALTER TABLE ops.phone_resolutions ADD COLUMN IF NOT EXISTS blitz_phone_raw jsonb;
ALTER TABLE ops.phone_resolutions ADD COLUMN IF NOT EXISTS phone_type      text;
ALTER TABLE ops.phone_resolutions ADD COLUMN IF NOT EXISTS country_code    text;
CREATE INDEX IF NOT EXISTS phone_resolutions_status_idx ON ops.phone_resolutions (phone_status);
CREATE INDEX IF NOT EXISTS phone_resolutions_domain_idx ON ops.phone_resolutions (company_domain);
CREATE INDEX IF NOT EXISTS phone_resolutions_phone_idx  ON ops.phone_resolutions (phone);

CREATE TABLE IF NOT EXISTS ops.blitz_phone_finder_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    batch_label   text,
    run_root      text,
    priority      text        NOT NULL,
    requested     bigint      NOT NULL DEFAULT 0,
    skipped       bigint      NOT NULL DEFAULT 0,
    found         bigint      NOT NULL DEFAULT 0,
    unresolved    bigint      NOT NULL DEFAULT 0,
    failed        bigint      NOT NULL DEFAULT 0,
    gateway_calls bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT blitz_phone_finder_runs_status_chk   CHECK (status   IN ('success', 'error')),
    CONSTRAINT blitz_phone_finder_runs_priority_chk CHECK (priority IN ('low', 'normal'))
);
CREATE INDEX IF NOT EXISTS blitz_phone_finder_runs_feed_idx        ON ops.blitz_phone_finder_runs (feed);
CREATE INDEX IF NOT EXISTS blitz_phone_finder_runs_recorded_at_idx ON ops.blitz_phone_finder_runs (recorded_at DESC);
"""

_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")


def _normalize_domain(raw: str | None) -> str | None:
    d = (raw or "").strip().lower()
    d = _SCHEME.sub("", d)
    d = _WWW.sub("", d)
    d = d.split("/", 1)[0].rstrip(".")
    return d or None


def _hqx_dsn() -> str:
    """Transaction-mode (Supavisor :6543) hq-x DSN for this rate-gated, horizontally
    scaled worker.

    The worker holds one Postgres connection for the whole run while it is mostly idle —
    blocked on the rate-governed gateway between sparse DB touches. On the SESSION pooler
    (:5432, pool_size=15) every such connection pins a backend for the entire run, so N
    concurrent workers exhaust the 15-slot ceiling (``EMAXCONNSESSION: max clients reached
    in session mode``). The TRANSACTION pooler checks a backend out only for each autocommit
    statement, returning it between the batched read and the per-entity writes — a long-
    lived-but-idle worker costs ~0 backends. Prefer an explicit HQX_DB_URL_TRANSACTION; else
    derive it from the session DSN (Supavisor maps session→transaction by the pooler port
    5432→6543)."""
    dsn = os.environ.get("HQX_DB_URL_TRANSACTION")
    if not dsn:
        pooled = os.environ.get("HQX_DB_URL_POOLED")
        if not pooled:
            raise RuntimeError(
                "Neither HQX_DB_URL_TRANSACTION nor HQX_DB_URL_POOLED set in the "
                "hqx-postgres Modal secret.")
        dsn = pooled.replace(".pooler.supabase.com:5432", ".pooler.supabase.com:6543")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _open_conn(dsn: str):
    import psycopg

    # prepare_threshold=None disables psycopg3 server-side prepared statements: under
    # transaction-mode pooling consecutive statements may land on different backends, so a
    # prepared statement created on one is absent on the next. autocommit=True keeps each
    # statement its own short transaction so the pooler frees the backend between gateway calls.
    return psycopg.connect(dsn, autocommit=True, prepare_threshold=None)


def _gateway():
    return modal.Function.from_name(GATEWAY_APP, GATEWAY_FN)


# ── Resolution (Blitz phone via the gateway) ─────────────────────────────────
def _make_result(c: dict, phone: str | None, status: str, vendor: str | None,
                 phone_type: str | None, attempts: list, blitz_raw: Any = None) -> dict:
    return {
        "contact_id": c.get("contact_id"),
        "phone": phone,
        "phone_status": status,
        "source_vendor": vendor,
        "phone_type": phone_type,
        "company_domain": _normalize_domain(c.get("company_domain")),
        "person_linkedin_url": (c.get("person_linkedin_url") or "").strip() or None,
        "country_code": (c.get("country_code") or "").strip().upper() or None,
        # Raw upstream response, VERBATIM — the source of truth, no interpretation imposed.
        "blitz_phone_raw": blitz_raw,            # Blitz /v2/enrichment/phone payload, as-is
        "attempts": attempts,
    }


def _resolve_contact(c: dict, gw, priority: str, counts: dict) -> dict:
    """One identity → a found/unresolved mobile phone. Blitz phone is keyed on
    ``person_linkedin_url`` ONLY; a contact without one is unresolved here. Blitz phone is
    US-only, so a contact whose ``country_code`` is present and not ``US`` is skipped
    (unresolved) WITHOUT a gateway call to save credits."""
    purl = (c.get("person_linkedin_url") or "").strip() or None
    cc = (c.get("country_code") or "").strip().upper() or None
    attempts: list[dict] = []

    if not purl:
        attempts.append({"vendor": "blitz", "outcome": "skipped",
                         "reason": "Blitz phone requires person_linkedin_url"})
        return _make_result(c, None, "unresolved", None, None, attempts)

    # US-only gate — skip non-US BEFORE the call (credit-free; Blitz has no intl coverage).
    if cc is not None and cc != "US":
        attempts.append({"vendor": "blitz", "outcome": "skipped",
                         "reason": "non_us_skipped", "country_code": cc})
        return _make_result(c, None, "unresolved", None, None, attempts)

    # ── Blitz phone via the gateway (the ONLY egress), bulk LOW/NORMAL lane ──
    counts["gateway_calls"] += 1
    rb = gw.remote(endpoint=PHONE_PATH, payload={"person_linkedin_url": purl}, priority=priority)
    blitz_raw = rb.get("data")                        # Blitz's response, VERBATIM (kept on hit AND miss)
    data = blitz_raw if isinstance(blitz_raw, dict) else {}
    # The deliverable is the phone string itself; key off its presence (robust whether or
    # not the payload carries an explicit `found` flag).
    phone = (str(data.get("phone")).strip() or None) if data.get("phone") else None
    found = bool(rb.get("ok")) and phone is not None
    phone_type = (data.get("phone_type") or data.get("type") or ("mobile" if found else None))

    if not found:
        attempts.append({"vendor": "blitz", "outcome": "miss", "ok": rb.get("ok"),
                         "http_status": rb.get("http_status"), "error": rb.get("error")})
        return _make_result(c, None, "unresolved", None, None, attempts, blitz_raw=blitz_raw)

    attempts.append({"vendor": "blitz", "outcome": "hit", "phone": phone, "phone_type": phone_type})
    return _make_result(c, phone, "found", "blitz", phone_type, attempts, blitz_raw=blitz_raw)


# ── Sink writers (ops.phone_resolutions upsert) ──────────────────────────────
def _upsert_resolution(cur, r: dict, batch_label: str | None) -> None:
    from psycopg.types.json import Jsonb

    def _j(v):  # jsonb bind, or SQL NULL when there is no payload (call not made)
        return Jsonb(v) if v is not None else None

    cur.execute(
        """
        INSERT INTO ops.phone_resolutions
            (contact_id, phone, phone_status, source_vendor, phone_type,
             company_domain, person_linkedin_url, country_code, blitz_phone_raw,
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
            blitz_phone_raw     = EXCLUDED.blitz_phone_raw,
            attempts            = EXCLUDED.attempts,
            batch_label         = EXCLUDED.batch_label,
            resolved_at         = now()
        """,
        (r["contact_id"], r["phone"], r["phone_status"], r["source_vendor"],
         r["phone_type"], r["company_domain"], r["person_linkedin_url"],
         r["country_code"], _j(r.get("blitz_phone_raw")),
         Jsonb(r["attempts"]), batch_label),
    )


def _record_run(cur, batch_label: str | None, run_root: str, priority: str, counts: dict,
                status: str, error: str | None, started_at: dt.datetime,
                completed_at: dt.datetime) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """
        INSERT INTO ops.blitz_phone_finder_runs
            (feed, batch_label, run_root, priority, requested, skipped, found,
             unresolved, failed, gateway_calls, status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, batch_label, run_root, priority, counts["requested"], counts["skipped"],
         counts["found"], counts["unresolved"], counts["failed"],
         counts["gateway_calls"], status, error, started_at, completed_at),
    )


def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    """POST terminal counts to the Trigger waitpoint url — RAW body, no envelope."""
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
    """Batched skip-set: contacts already resolved to a FOUND phone (idempotency — never
    re-spend a gateway call or clobber a settled contact unless force=True)."""
    if not contact_ids:
        return set()
    try:
        cur.execute(
            "SELECT contact_id FROM ops.phone_resolutions "
            "WHERE phone_status = 'found' AND contact_id = ANY(%s)",
            (contact_ids,),
        )
        return {r[0] for r in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001 — degrade skip, preserve correctness
        print(f"WARN: skip-set lookup failed ({exc}); proceeding without skip.")
        return set()


# ── Runner ────────────────────────────────────────────────────────────────────
def _run(contacts: list[dict], batch_label: str | None, run_id: str | None, priority: str,
         force: bool, trigger_callback_url: str | None) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    priority = priority if priority in VALID_PRIORITIES else "low"
    counts = {"requested": 0, "skipped": 0, "found": 0,
              "unresolved": 0, "failed": 0, "gateway_calls": 0}
    status, error = "error", None
    dsn = _hqx_dsn()

    try:
        contacts = [c for c in (contacts or []) if isinstance(c, dict) and c.get("contact_id")]
        counts["requested"] = len(contacts)
        gw = _gateway()
        conn = _open_conn(dsn)
        try:
            cur = conn.cursor()
            cur.execute(OPS_DDL)  # ensure sinks exist before first write

            skip = set() if force else _already_found(
                cur, [c["contact_id"] for c in contacts])

            for c in contacts:
                if c["contact_id"] in skip:
                    counts["skipped"] += 1
                    continue
                try:
                    r = _resolve_contact(c, gw, priority, counts)
                    _upsert_resolution(cur, r, batch_label)
                    counts[r["phone_status"]] += 1   # found | unresolved
                except Exception as exc:  # noqa: BLE001 — one contact must not sink the batch
                    counts["failed"] += 1
                    print(f"WARN: contact {c.get('contact_id')!r} failed: {exc}")

            status = "success"
            _record_run(cur, batch_label, run_root, priority, counts, status, None,
                        started_at, dt.datetime.now(dt.timezone.utc))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — terminal handling + re-raise
        error = str(exc)
        status = "error"
        try:
            conn2 = _open_conn(dsn)
            try:
                _record_run(conn2.cursor(), batch_label, run_root, priority, counts, status,
                            error, started_at, dt.datetime.now(dt.timezone.utc))
            finally:
                conn2.close()
        except Exception as exc2:  # noqa: BLE001 — audit must not mask the failure
            print(f"WARN: ops.blitz_phone_finder_runs write failed: {exc2}")
    finally:
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "batch_label": batch_label, "error": error, **counts},
        )

    if status != "success":
        raise RuntimeError(f"blitz_phone_finder failed: {error}")
    return {"feed": FEED, "status": status, "batch_label": batch_label, **counts}


@app.function(secrets=SECRETS, timeout=60 * 60, memory=2048, cpu=1.0)
def run_phone_finder(contacts: list[dict], batch_label: str | None = None,
                     run_id: str | None = None, priority: str = "low", force: bool = False,
                     trigger_callback_url: str | None = None) -> dict:
    """Resolve a chunk of identities to direct mobile phones via Blitz (gateway-routed).
    Each ``contact`` is a dict::

        {contact_id, person_linkedin_url, company_domain?, country_code?,
         first_name?, last_name?, company_name?}

    Blitz phone requires ``person_linkedin_url``; contacts without one are unresolved.
    Blitz phone is US-only; a contact whose ``country_code`` is present and not ``US`` is
    skipped (unresolved) WITHOUT a Blitz call. ``priority`` is the gateway lane: ``"low"``
    (default, bulk) or ``"normal"`` — never HIGH (reserved for interactive GTM)."""
    return _run(contacts, batch_label, run_id, priority, force, trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.phone_resolutions + ops.blitz_phone_finder_runs in HQX (idempotent)."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name IN ('phone_resolutions','blitz_phone_finder_runs')
            ORDER BY table_name, ordinal_position
        """)
        cols: dict[str, list[str]] = {}
        for t, col in cur.fetchall():
            cols.setdefault(t, []).append(col)
    finally:
        conn.close()
    print(f"ops tables ready: { {k: len(v) for k, v in cols.items()} }")
    return {"tables": cols}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def verify(limit: int = 8) -> dict:
    """Read-back: latest Blitz-sourced phone resolutions + run-state + status histogram."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute(
            """SELECT contact_id, phone, phone_status, source_vendor, phone_type,
                      blitz_phone_raw, resolved_at
               FROM ops.phone_resolutions WHERE source_vendor = 'blitz'
               ORDER BY resolved_at DESC LIMIT %s""",
            (limit,),
        )
        rcols = [d.name for d in cur.description]
        resolutions = [dict(zip(rcols, r)) for r in cur.fetchall()]
        cur.execute(
            "SELECT phone_status, count(*) FROM ops.phone_resolutions "
            "WHERE source_vendor = 'blitz' GROUP BY 1")
        histogram = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(
            """SELECT batch_label, priority, requested, skipped, found, unresolved,
                      failed, gateway_calls, status, recorded_at
               FROM ops.blitz_phone_finder_runs ORDER BY recorded_at DESC LIMIT 3""")
        runcols = [d.name for d in cur.description]
        runs = [dict(zip(runcols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"blitz phone status histogram: {histogram}")
    return {"histogram": histogram, "recent_resolutions": resolutions, "recent_runs": runs}


@app.function(image=image, secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60)
def verify_egress() -> dict:
    """Evidence the Blitz gateway IPC resolves with PHONE enrichment unlocked — no Blitz
    spend (0-credit key-info). Confirms the hq-x DSN is present and the workspace key's
    allowed_apis include /enrichment/phone (the plan-tier gate)."""
    out = {
        "hqx_dsn_present": bool(os.environ.get("HQX_DB_URL_POOLED")),
        "gateway_keyinfo_ok": False,
        "phone_enrichment_allowed": False,
    }
    try:
        ki = modal.Function.from_name(GATEWAY_APP, "key_info").remote()
        out["gateway_keyinfo_ok"] = bool(ki.get("valid"))
        out["phone_enrichment_allowed"] = "/enrichment/phone" in set(ki.get("allowed_apis") or [])
    except Exception as exc:  # noqa: BLE001
        out["gateway_error"] = str(exc)
    print(out)
    return out


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.phone_resolutions + ops.blitz_phone_finder_runs DDL (HQX)."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def verify_egress_run() -> None:
    """Gateway-egress + phone-enrichment-allowed evidence (no Blitz spend)."""
    import json

    print(json.dumps(verify_egress.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_run(limit: int = 8) -> None:
    """Read-back assertion on the most-recent Blitz phone resolutions."""
    import json

    print(json.dumps(verify.remote(limit), indent=2, default=str))


@app.local_entrypoint()
def run_manual(contacts_json: str, priority: str = "low") -> None:
    """Manual run. --contacts-json '[{"contact_id":"c1",
    "person_linkedin_url":"https://www.linkedin.com/in/x","country_code":"US"}]'"""
    import json

    contacts = json.loads(contacts_json)
    print(json.dumps(
        run_phone_finder.remote(contacts, batch_label="manual", priority=priority),
        indent=2, default=str,
    ))
