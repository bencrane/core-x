"""Standalone LeadMagic Company Enrichment (capture) — the firmographic twin of
``pipelines/enrichment_leadmagic/find_phone_leadmagic.py``.

Enriches a **company** to firmographic data via LeadMagic's ``POST /company-search`` (the wire
endpoint LeadMagic markets as "Company Enrichment"; ``operationId: searchCompany``). Keyed on any
of ``company_domain`` / ``profile_url`` (the company LinkedIn URL) / ``company_name`` — at least
one is required. Endpoint-less Modal worker, spawned by the Universal Dispatcher and woken via the
Trigger waitpoint callback. Deliberately parallels the LeadMagic phone finder:

  1. NO gateway. Blitz egress is governed by the single-container ``blitz_gateway`` (≤5 RPS).
     LeadMagic is a separate vendor with its own limits, so this worker holds the LeadMagic key
     (Modal secret ``leadmagic-api`` → ``LEADMAGIC_API_KEY``) and calls LeadMagic directly, with
     per-call retry/backoff on 429/5xx.
  2. Charges only on a HIT (``credits_consumed=0`` on a miss), so every entity is attempted; a
     ``not_found`` row is a FREE negative cache that prevents re-spend on a settled miss.

STAGE 1 of 2 (capture). This worker only CAPTURES verbatim LeadMagic responses into
``ops.firmographics_leadmagic_capture`` (entity_id PK, latest-wins upsert). The verbatim
``/company-search`` payload lands in ``leadmagic_raw`` — the SoR — with ``company_id`` /
``b2b_profile_url`` extracted as convenience anchors for the skip-set + downstream dedup. The
Gen-3 Lance system-of-record is built by STAGE 2,
``pipelines/firmographics_leadmagic/materialize_leadmagic.py``, which projects/dedups the capture
grain into ``s3://data-sink/active/firmographics_leadmagic/``. Capture and materialize are split
so vendor-spend blast radius never touches the Lance write path.

IDEMPOTENCY. ``force=False`` (default) skips any entity already captured as FOUND — re-firing
never re-spends on a settled entity, and a re-run over a miss cohort re-attempts only those.

    modal deploy pipelines/firmographics_leadmagic/find_company_leadmagic.py
    modal run    pipelines/firmographics_leadmagic/find_company_leadmagic.py::init_ops
    modal run    pipelines/firmographics_leadmagic/find_company_leadmagic.py::run_manual \\
                 --entities-json '[{"entity_id":"e1","company_domain":"leadmagic.io"}]'
"""

from __future__ import annotations

import datetime as dt
import os
import time
import uuid
from typing import Any

import modal

FEED = "firmographics_leadmagic"
# LeadMagic "Company Enrichment" is the wire endpoint POST /company-search (operationId
# searchCompany). Base host is flat (matches the repo's existing /mobile-finder); overridable.
LM_URL = os.environ.get("LEADMAGIC_COMPANY_URL", "https://api.leadmagic.io/company-search")
HTTP_TIMEOUT = 30.0
VALID_PRIORITIES = {"low", "normal"}   # carried for ledger parity; LeadMagic has no lane

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "psycopg[binary]>=3.2",   # ops.firmographics_leadmagic_capture + _finder_runs
    "requests>=2.32",         # LeadMagic API + Trigger callback
)

app = modal.App("firmographics-leadmagic-capture", image=image)

# hqx-postgres → ops sink; leadmagic-api → LEADMAGIC_API_KEY (the worker holds the key — there is
# no gateway for LeadMagic the way blitz_gateway fronts Blitz).
SECRETS = [
    modal.Secret.from_name("hqx-postgres"),
    modal.Secret.from_name("leadmagic-api"),
]

# ── ops DDL — idempotent; verbatim mirror of the two objects this worker owns in
# ops_firmographics_leadmagic_runs.sql (the capture SoR + the capture run ledger). ────────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.firmographics_leadmagic_capture (
    entity_id           text        PRIMARY KEY,
    input_domain        text,
    input_linkedin_url  text,
    input_company_name  text,
    company_id          bigint,
    b2b_profile_url     text,
    company_status      text        NOT NULL,
    leadmagic_raw       jsonb,
    credits_consumed    integer     NOT NULL DEFAULT 0,
    batch_label         text,
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    captured_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT firmographics_leadmagic_capture_status_chk
        CHECK (company_status IN ('found', 'not_found'))
);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_capture_status_idx
    ON ops.firmographics_leadmagic_capture (company_status);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_capture_company_id_idx
    ON ops.firmographics_leadmagic_capture (company_id);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_capture_captured_at_idx
    ON ops.firmographics_leadmagic_capture (captured_at DESC);

CREATE TABLE IF NOT EXISTS ops.firmographics_leadmagic_finder_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,
    batch_label      text,
    run_root         text,
    priority         text        NOT NULL,
    requested        bigint      NOT NULL DEFAULT 0,
    skipped          bigint      NOT NULL DEFAULT 0,
    found            bigint      NOT NULL DEFAULT 0,
    not_found        bigint      NOT NULL DEFAULT 0,
    failed           bigint      NOT NULL DEFAULT 0,
    api_calls        bigint      NOT NULL DEFAULT 0,
    credits_consumed bigint      NOT NULL DEFAULT 0,
    status           text        NOT NULL,
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT firmographics_leadmagic_finder_runs_status_chk
        CHECK (status   IN ('success', 'error')),
    CONSTRAINT firmographics_leadmagic_finder_runs_priority_chk
        CHECK (priority IN ('low', 'normal'))
);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_finder_runs_feed_idx
    ON ops.firmographics_leadmagic_finder_runs (feed);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_finder_runs_recorded_at_idx
    ON ops.firmographics_leadmagic_finder_runs (recorded_at DESC);
"""


def _clean(v: Any) -> str | None:
    return (str(v).strip() or None) if v is not None else None


def _hqx_dsn() -> str:
    """Transaction-mode (Supavisor :6543) hq-x DSN — same rationale as the LeadMagic phone
    finder: horizontally-scaled chunk-workers must not each pin a session-pool backend."""
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


# ── LeadMagic company-search call (direct; retry on 429/5xx) ──────────────────
def _lm_call(key: str, body: dict) -> tuple[Any, bool, int | None, str | None]:
    """POST /company-search. Returns (json_or_text, ok, http_status, error). Retries 429/5xx
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


# The /company-search 200 body is loosely typed in LeadMagic's OpenAPI (only credits_consumed is
# guaranteed), so identity fields are read defensively across camel/snake variants. leadmagic_raw
# holds the verbatim payload — these projections are re-derivable and never the source of truth.
def _first(data: dict, *keys: str) -> Any:
    for k in keys:
        if k in data and data[k] not in (None, ""):
            return data[k]
    return None


def _make_result(e: dict, status: str, company_id: int | None, b2b_url: str | None,
                 credits: int, attempts: list, lm_raw: Any = None) -> dict:
    return {
        "entity_id": e.get("entity_id"),
        "input_domain": _clean(e.get("company_domain")),
        "input_linkedin_url": _clean(e.get("company_linkedin_url")),
        "input_company_name": _clean(e.get("company_name")),
        "company_id": company_id,
        "b2b_profile_url": b2b_url,
        "company_status": status,
        "leadmagic_raw": lm_raw,       # LeadMagic /company-search payload, VERBATIM
        "credits_consumed": int(credits or 0),
        "attempts": attempts,
    }


def _enrich_entity(e: dict, key: str, counts: dict) -> dict:
    """One company → found/not_found firmographics. LeadMagic /company-search keys on
    ``company_domain`` / ``profile_url`` (LinkedIn) / ``company_name`` (≥1 required). Charges
    only on a HIT, so misses are free and never pre-skipped."""
    domain = _clean(e.get("company_domain"))
    linkedin = _clean(e.get("company_linkedin_url"))
    name = _clean(e.get("company_name"))
    attempts: list[dict] = []

    if not (domain or linkedin or name):
        attempts.append({"vendor": "leadmagic", "outcome": "skipped",
                         "reason": "need company_domain, company_linkedin_url, or company_name"})
        return _make_result(e, "not_found", None, None, 0, attempts)

    body: dict = {}
    if domain:
        body["company_domain"] = domain
    if linkedin:
        body["profile_url"] = linkedin        # LeadMagic's field name for the company LinkedIn URL
    if name:
        body["company_name"] = name

    counts["api_calls"] += 1
    raw, ok, http_status, err = _lm_call(key, body)
    data = raw if isinstance(raw, dict) else {}
    credits = int(data.get("credits_consumed") or 0)
    counts["credits_consumed"] += credits

    company_id = _first(data, "companyId", "company_id", "id")
    try:
        company_id = int(company_id) if company_id is not None else None
    except (TypeError, ValueError):
        company_id = None
    b2b_url = _clean(_first(data, "b2b_profile_url", "b2bProfileUrl", "profile_url", "linkedin_url"))
    company_name_hit = _first(data, "companyName", "company_name", "name")

    # HIT = a real company identity came back. credits>0 is the billing-side confirmation
    # (LeadMagic charges only on a hit), used as a fallback signal when identity fields drift.
    found = bool(ok) and (company_id is not None or bool(company_name_hit) or credits > 0)

    if not found:
        attempts.append({"vendor": "leadmagic", "outcome": "miss", "ok": ok,
                         "http_status": http_status, "error": err, "message": data.get("message")})
        return _make_result(e, "not_found", None, None, credits, attempts, lm_raw=raw)

    attempts.append({"vendor": "leadmagic", "outcome": "hit", "company_id": company_id,
                     "credits": credits})
    return _make_result(e, "found", company_id, b2b_url, credits, attempts, lm_raw=raw)


# ── Sink writers ──────────────────────────────────────────────────────────────
def _upsert_capture(cur, r: dict, batch_label: str | None) -> None:
    from psycopg.types.json import Jsonb

    def _j(v):
        return Jsonb(v) if v is not None else None

    cur.execute(
        """
        INSERT INTO ops.firmographics_leadmagic_capture
            (entity_id, input_domain, input_linkedin_url, input_company_name, company_id,
             b2b_profile_url, company_status, leadmagic_raw, credits_consumed, batch_label,
             attempts, captured_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (entity_id) DO UPDATE SET
            input_domain       = EXCLUDED.input_domain,
            input_linkedin_url = EXCLUDED.input_linkedin_url,
            input_company_name = EXCLUDED.input_company_name,
            company_id         = EXCLUDED.company_id,
            b2b_profile_url    = EXCLUDED.b2b_profile_url,
            company_status     = EXCLUDED.company_status,
            leadmagic_raw      = EXCLUDED.leadmagic_raw,
            credits_consumed   = EXCLUDED.credits_consumed,
            batch_label        = EXCLUDED.batch_label,
            attempts           = EXCLUDED.attempts,
            captured_at        = now()
        """,
        (r["entity_id"], r["input_domain"], r["input_linkedin_url"], r["input_company_name"],
         r["company_id"], r["b2b_profile_url"], r["company_status"], _j(r.get("leadmagic_raw")),
         r["credits_consumed"], batch_label, Jsonb(r["attempts"])),
    )


def _record_run(cur, batch_label, run_root, priority, counts, status, error,
                started_at, completed_at) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """
        INSERT INTO ops.firmographics_leadmagic_finder_runs
            (feed, batch_label, run_root, priority, requested, skipped, found,
             not_found, failed, api_calls, credits_consumed, status, error,
             started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, batch_label, run_root, priority, counts["requested"], counts["skipped"],
         counts["found"], counts["not_found"], counts["failed"], counts["api_calls"],
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


def _already_found(cur, entity_ids: list[str]) -> set[str]:
    """Skip-set: entities already captured as FOUND (never re-spend on a settled entity unless
    force=True). A prior not_found is NOT skipped — it is re-attempted (miss was free)."""
    if not entity_ids:
        return set()
    try:
        cur.execute(
            "SELECT entity_id FROM ops.firmographics_leadmagic_capture "
            "WHERE company_status = 'found' AND entity_id = ANY(%s)", (entity_ids,))
        return {r[0] for r in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: skip-set lookup failed ({exc}); proceeding without skip.")
        return set()


def _run(entities, batch_label, run_id, priority, force, trigger_callback_url) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    priority = priority if priority in VALID_PRIORITIES else "low"
    counts = {"requested": 0, "skipped": 0, "found": 0, "not_found": 0,
              "failed": 0, "api_calls": 0, "credits_consumed": 0}
    status, error = "error", None
    key = os.environ.get("LEADMAGIC_API_KEY")
    if not key:
        raise RuntimeError("LEADMAGIC_API_KEY not set in the leadmagic-api Modal secret.")
    dsn = _hqx_dsn()
    try:
        entities = [e for e in (entities or []) if isinstance(e, dict) and e.get("entity_id")]
        counts["requested"] = len(entities)
        conn = _open_conn(dsn)
        try:
            cur = conn.cursor()
            cur.execute(OPS_DDL)
            skip = set() if force else _already_found(cur, [e["entity_id"] for e in entities])
            for e in entities:
                if e["entity_id"] in skip:
                    counts["skipped"] += 1
                    continue
                try:
                    r = _enrich_entity(e, key, counts)
                    _upsert_capture(cur, r, batch_label)
                    counts[r["company_status"]] += 1
                except Exception as exc:  # noqa: BLE001 — one entity must not sink the batch
                    counts["failed"] += 1
                    print(f"WARN: entity {e.get('entity_id')!r} failed: {exc}")
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
            print(f"WARN: ops.firmographics_leadmagic_finder_runs write failed: {exc2}")
    finally:
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": FEED, "batch_label": batch_label,
                        "error": error, **counts})
    if status != "success":
        raise RuntimeError(f"firmographics_leadmagic capture failed: {error}")
    return {"feed": FEED, "status": status, "batch_label": batch_label, **counts}


@app.function(secrets=SECRETS, timeout=60 * 60, memory=2048, cpu=1.0)
def run_leadmagic_company(entities: list[dict], batch_label: str | None = None,
                          run_id: str | None = None, priority: str = "low", force: bool = False,
                          trigger_callback_url: str | None = None) -> dict:
    """Enrich a chunk of companies to firmographics via LeadMagic /company-search. Each
    ``entity``::

        {entity_id, company_domain?, company_linkedin_url?, company_name?}

    Requires one of company_domain / company_linkedin_url / company_name. Misses cost 0 credits.
    ``force=False`` skips entities already captured as FOUND. Verbatim payloads land in
    ops.firmographics_leadmagic_capture; STAGE 2 materializes the Lance SoR from there."""
    return _run(entities, batch_label, run_id, priority, force, trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.firmographics_leadmagic_capture + ops.firmographics_leadmagic_finder_runs."""
    conn = _open_conn(_hqx_dsn())
    try:
        conn.cursor().execute(OPS_DDL)
    finally:
        conn.close()
    return {"tables": ["ops.firmographics_leadmagic_capture",
                       "ops.firmographics_leadmagic_finder_runs"]}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def verify(limit: int = 8) -> dict:
    """Read-back: latest captured companies + status histogram."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""SELECT entity_id, company_id, company_status, b2b_profile_url, captured_at
                       FROM ops.firmographics_leadmagic_capture
                       ORDER BY captured_at DESC LIMIT %s""", (limit,))
        rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
        cur.execute("""SELECT company_status, count(*) FROM ops.firmographics_leadmagic_capture
                       GROUP BY 1""")
        hist = dict(cur.fetchall())
    finally:
        conn.close()
    return {"recent": rows, "status_histogram": hist}


@app.local_entrypoint()
def init_ops() -> None:
    import json
    print(json.dumps(apply_ops_ddl.remote(), indent=2, default=str))


@app.local_entrypoint()
def run_manual(entities_json: str = "[]", batch_label: str = "manual", force: bool = False) -> None:
    import json
    entities = json.loads(entities_json)
    print(json.dumps(run_leadmagic_company.remote(entities, batch_label=batch_label, force=force),
                     indent=2, default=str))
