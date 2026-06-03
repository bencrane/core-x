"""Compute worker — Exa.ai Webset ingestion → Gen-3 Lance (Directive 22).

Part of the ``exa-webset-pipelines`` Modal app. Endpoint-less; spawned by the Universal
Dispatcher (core/modal_dispatcher.py). Clean-room data plane: DuckDB does 100% of the
domain-normalize / dedup / projection, Lance is written straight to R2. No Iceberg, no
Polaris, no catalog round-trip. Contract: docs/reference/EXA_WEBSET_INGESTION_SPEC.md.

WHAT THIS WORKER DOES
    Harvests high-precision custom industry websets (e.g. "OSHA Defense Law Firms") from
    Exa.ai and lands them in the active sink, deduped against the canonical companies spine.

    Tier A (precision, default) — Exa Websets API (POST /v0/websets): async, per-item
        criteria-verified, unbounded by the 100-result cap of /search. The directive's
        max_results_limit:500 is only reachable here. We create the webset, poll to idle
        (bounded), then cursor-pull every item.
    Tier B (harvest) — /findSimilar over a seed-URL list + /search (category=company):
        cheap dollar-priced look-alike expansion; items land flagged verification_status=
        'unverified'. Complementary top-of-funnel, not a verification path.

DATA PLANE
    Exa items  → capture FULL raw payload → R2 landing (ZSTD parquet, best-effort)
               → DuckDB: normalize domain (SAME expression as the companies anchor),
                 LEFT JOIN vs active/companies → split {new, known}, project, DISTINCT
               → lance.write_dataset (append-or-create) to:
                    s3://data-sink/active/discovered_websets/   (NEW domains)
                    s3://data-sink/active/webset_membership/     (KNOWN-company industry edges)
               → BTREE scalar indexes on the resolution keys.

CREDIT-PROTECTION (the directive's hard deliverable — §11 ratified values)
    Exa bills Websets in CREDITS (10/result + 2/text-enrichment row; contact = 5/datapoint,
    FORBIDDEN here — D2). Guardrails, in order, BEFORE any spend:
      1. kill switch        EXA_ENGINE_ENABLED != 'true'        → reject
      2. result clamp       count = min(max_results_limit, 1000)            (D4 HARD_RESULT_CAP)
      3. enrichment sanitise  strip every email/phone enrichment            (D2 global forbid)
      4. per-run ceiling    projected_credits > 5000             → reject   (D1, payload-clamped down only)
      5. monthly ledger     projected_credits > month_remaining  → reject   (D1 month_cap=100k)
    dry_run=true returns the estimate and exits WITHOUT creating the webset. Credits are
    reserved in ops.exa_credit_ledger at create and reconciled to actual at completion.

CONTROL PLANE (Trigger v4 durable callback)
    Spawned with trigger_callback_url. On terminal state (success | timeout_partial |
    rejected | dry_run | failed) it (1) writes the run row to ops.exa_webset_runs and
    reconciles ops.exa_credit_ledger via psycopg (HQX_DB_URL_POOLED), and (2) POSTs a FLAT
    JSON body to the waitpoint url. Trigger owns the wait; this worker is short bursts.

SECRETS
    r2-credentials (R2 Lance writes) · hqx-postgres (ops.* state) · exa-api (EXA_API_KEY,
    EXA_ENGINE_ENABLED) — sourced from Doppler into the Modal secret.

    modal run    pipelines/exa_websets/ingest.py::init_ops   # create ops tables (HQX)
    modal deploy pipelines/exa_websets/ingest.py             # make dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

# ── Gen-3 targets (active tier) + raw landing (transport) ────────────────────────────────
_ACTIVE = "s3://data-sink/active"
_LANDING = "s3://data-sink/landing/exa_websets"
COMPANIES_URI = os.environ.get("GTM_COMPANIES_URI", f"{_ACTIVE}/companies/")
DISCOVERED_URI = os.environ.get("EXA_DISCOVERED_URI", f"{_ACTIVE}/discovered_websets/")
MEMBERSHIP_URI = os.environ.get("EXA_MEMBERSHIP_URI", f"{_ACTIVE}/webset_membership/")

FEED = "exa_websets"
SOURCE_PLATFORM = "exa-websets"          # lineage (extends the existing 'exa-all' convention)
EXA_BASE = "https://api.exa.ai"               # core endpoints: /search, /findSimilar, /contents
WEBSETS_BASE = "https://api.exa.ai/websets/v0"  # Websets API is namespaced under /websets/v0 (NOT root /v0)

# ── Credit-protection constants — §11 RATIFIED (D1–D4). Tunable here; payload cannot exceed. ─
HARD_RESULT_CAP = 1000                    # D4 — absolute clamp on count per run
PER_RUN_CREDIT_CEILING = 5000            # D1 — hard per-run ceiling (payload may lower, never raise)
MONTH_CREDIT_CAP = 100_000               # D1 — monthly budget (lower-tier plan)
CREDITS_PER_RESULT = 10                   # Exa Websets: discovery + verification per item
CREDITS_PER_ENRICH_ROW = 2               # Exa Websets: per non-contact enrichment row
CREDITS_PER_CONTACT = 5                   # Exa Websets: per email/phone datapoint — FORBIDDEN (D2)
CREDIT_USD = 449 / 100_000               # Pro-rate ≈ $0.00449/credit (reporting only)
FORBIDDEN_ENRICH_FORMATS = {"email", "phone"}  # D2 — contact data never sourced from Exa

# ── Poll budget — keep the worker comfortably under the Modal function timeout. ──────────
POLL_BUDGET_S = 3000                      # 50 min; function timeout is 60 min
POLL_BACKOFF_S = (5, 10, 15, 30)         # caps at 30s; never a tight loop

# Lance fragment sizing — fleet defaults (02_lancedb_storage.md §2.3); net-new → v2.1.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan — BTREE only (the directive's hard deliverable).
INDEXES: dict[str, dict[str, list[str]]] = {
    "discovered_websets": {"BTREE": ["discovered_domain", "exa_webset_id", "exa_item_id"]},
    "webset_membership": {"BTREE": ["normalized_domain", "exa_webset_id"]},
}
DATASET_URI = {"discovered_websets": DISCOVERED_URI, "webset_membership": MEMBERSHIP_URI}

# Strict output schemas — every field String; the bool is `nullable`. Also pins column ORDER.
# exa_item_id is the idempotency key (never null); the anchor is NOT-NULL only on the
# membership grain (it came from a matched known company), nullable on discovery (a candidate
# may have an unresolvable domain — kept, not dropped).
STRICT_SCHEMA: dict[str, list[tuple[str, bool]]] = {
    "discovered_websets": [
        ("discovered_domain", True),
        ("company_name", True),
        ("company_url", True),
        ("exa_item_id", False),
        ("exa_webset_id", True),
        ("webset_label", True),
        ("webset_identifier", True),
        ("search_prompt", True),
        ("entity_type", True),
        ("verification_status", True),
        ("verification_json", True),
        ("match_criteria_json", True),
        ("description", True),
        ("enrichment_json", True),
        ("linkedin_url", True),
        ("source_platform", True),
        ("raw_payload_uri", True),
        ("raw_item_json", True),
        ("exa_webset_run_id", True),
        ("discovered_at", True),
        ("snapshot_date", True),
    ],
    "webset_membership": [
        ("normalized_domain", False),
        ("exa_webset_id", True),
        ("webset_label", True),
        ("exa_item_id", False),
        ("verification_status", True),
        ("verification_json", True),
        ("raw_item_json", True),
        ("exa_webset_run_id", True),
        ("discovered_at", True),
        ("source_platform", True),
    ],
}

# ── ops DDL — verbatim mirror of the canonical .sql sibling. Self-applied defensively before
# every terminal write (HQX control-plane DB), so the tables bootstrap on first real run. ──
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.exa_webset_runs (
    run_id            text PRIMARY KEY,
    exa_webset_id     text,
    webset_identifier text NOT NULL,
    webset_label      text NOT NULL,
    search_prompt     text NOT NULL,
    tier              text NOT NULL DEFAULT 'precision',
    status            text NOT NULL,
    requested         integer,
    returned          integer,
    new_count         integer,
    known_count       integer,
    credits_estimated integer,
    credits_actual    integer,
    usd_actual        numeric(12,4),
    rejected_reason   text,
    started_at        timestamptz,
    finished_at       timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS exa_webset_runs_status_idx     ON ops.exa_webset_runs (status);
CREATE INDEX IF NOT EXISTS exa_webset_runs_label_idx      ON ops.exa_webset_runs (webset_label);
CREATE INDEX IF NOT EXISTS exa_webset_runs_recorded_idx   ON ops.exa_webset_runs (recorded_at DESC);

CREATE TABLE IF NOT EXISTS ops.exa_credit_ledger (
    month            date   PRIMARY KEY,
    credits_reserved bigint NOT NULL DEFAULT 0,
    credits_actual   bigint NOT NULL DEFAULT 0,
    month_cap        bigint NOT NULL
);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "psycopg[binary]>=3.2",  # ops.* state + ledger
    "requests>=2.32",        # Exa HTTP + Trigger callback
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("exa-webset-pipelines", image=image)


# ── Domain anchor — IDENTICAL expression to the companies worker. The dedup join key MUST be
# byte-identical to companies.normalized_domain or the JIT check silently fails. ──────────
def _normalized_domain(col: str) -> str:
    """Canonical domain-anchor normalization (a DuckDB SQL expression).
    lower/trim → strip scheme → strip leading www. → strip path/query → strip trailing dots
    → NULL if emptied. Copied verbatim from pipelines/gtm/companies_people_bulk.py."""
    return (
        "nullif("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "lower(trim(CAST(" + col + " AS VARCHAR))),"
        " '^https?://', '', 'g'),"
        " '^www\\.', '', 'g'),"
        " '/.*$', '', 'g'),"
        " '\\.+$', '', 'g'),"
        " '')"
    )


# ── Exa HTTP — explicit requests (not exa-py) so retries / rate-governance / cost-capture are
# fully under our control. 429 → jittered exponential backoff honouring Retry-After. ─────
def _exa_headers() -> dict[str, str]:
    key = os.environ.get("EXA_API_KEY")
    if not key:
        raise RuntimeError("EXA_API_KEY not set in the exa-api Modal secret.")
    return {"x-api-key": key, "Content-Type": "application/json"}


def _exa_request(method: str, path: str, *, base: str = EXA_BASE, json_body=None, params=None, attempts: int = 6):
    """One Exa API call with backoff. Raises on non-2xx after exhausting attempts.
    `base` selects the host+prefix: EXA_BASE for core endpoints, WEBSETS_BASE for the Websets API."""
    import random
    import time

    import requests

    url = f"{base}{path}"
    last = None
    for i in range(attempts):
        try:
            resp = requests.request(
                method, url, headers=_exa_headers(),
                json=json_body, params=params, timeout=(30, 120),
            )
            if resp.status_code < 300:
                return resp.json() if resp.content else {}
            last = f"{resp.status_code} {resp.text[:300]}"
            # 429 / 5xx are retryable; 4xx (except 429) are terminal.
            if resp.status_code != 429 and resp.status_code < 500:
                raise RuntimeError(f"Exa {method} {path} → {last}")
            retry_after = resp.headers.get("Retry-After")
            sleep = float(retry_after) if retry_after else min(30.0, (2 ** i) + random.random())
        except requests.RequestException as exc:
            last = str(exc)
            sleep = min(30.0, (2 ** i) + random.random())
        if i < attempts - 1:
            time.sleep(sleep)
    raise RuntimeError(f"Exa {method} {path} failed after {attempts} attempts: {last}")


def _g(d, *keys, default=None):
    """First present, non-null value among keys (defensive against snake/camel variants)."""
    if not isinstance(d, dict):
        return default
    for k in keys:
        if d.get(k) is not None:
            return d[k]
    return default


# ── R2 plumbing ──────────────────────────────────────────────────────────────────────────
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the r2-credentials secret."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _configure_duckdb_r2(con) -> None:
    """Point DuckDB's httpfs at R2 for the landing-zone parquet COPY (transport layer)."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    host = (endpoint or f"https://{account_id}.r2.cloudflarestorage.com").replace("https://", "").replace("http://", "")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{host}';")
    con.execute(f"SET s3_access_key_id='{os.environ['R2_ACCESS_KEY_ID']}';")
    con.execute(f"SET s3_secret_access_key='{os.environ['R2_SECRET_ACCESS_KEY']}';")
    con.execute("SET s3_region='auto'; SET s3_url_style='path'; SET s3_use_ssl=true;")


# ── Lance read/write helpers ─────────────────────────────────────────────────────────────
def _lance_column_set(uri: str, col: str, so: dict) -> set:
    """All values of one column from a Lance dataset (empty set if the dataset is absent)."""
    import lance

    try:
        ds = lance.dataset(uri, storage_options=so)
    except Exception:  # noqa: BLE001 — dataset not yet created
        return set()
    vals = ds.to_table(columns=[col]).column(col).to_pylist()
    return {v for v in vals if v is not None}


def _enforce_schema(ds_name: str, table):
    """Select + order the declared columns, fail loud on NOT-NULL nulls, cast to exact schema."""
    import pyarrow as pa

    spec = STRICT_SCHEMA[ds_name]
    schema = pa.schema([pa.field(n, pa.string(), nullable=nl) for n, nl in spec])
    table = table.select([f.name for f in schema])
    for f in schema:
        if not f.nullable and table.column(f.name).null_count:
            raise ValueError(
                f"schema contract violation: {ds_name}.{f.name} declared NOT NULL but has "
                f"{table.column(f.name).null_count} null value(s)")
    return table.cast(schema)


def _write_lance_append_or_create(ds_name: str, table, so: dict) -> int:
    """Append to the staging dataset (create on first write). No-op on an empty table."""
    import lance

    if table.num_rows == 0:
        return 0
    uri = DATASET_URI[ds_name]
    try:
        lance.dataset(uri, storage_options=so)
        mode = "append"
    except Exception:  # noqa: BLE001 — first write
        mode = "overwrite"
    lance.write_dataset(
        table, uri, mode=mode, data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    return table.num_rows


def _create_indexes(ds_name: str, so: dict) -> None:
    """Build the mandated BTREE scalar indexes (idempotent; replace=True default)."""
    import lance

    ds = lance.dataset(DATASET_URI[ds_name], storage_options=so)
    for col in INDEXES[ds_name].get("BTREE", []):
        try:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE  ✓ {ds_name}.{col}")
        except Exception as exc:  # noqa: BLE001 — an index miss must not fail a good load
            print(f"  BTREE  ✗ {ds_name}.{col}: {exc}")


# ── Credit ledger (HQX control-plane DB) ─────────────────────────────────────────────────
def _month_bucket() -> "object":
    import datetime as dt

    return dt.date.today().replace(day=1)


def _ledger_reserve(projected: int) -> tuple[bool, int]:
    """Atomically check the monthly budget and reserve `projected` credits.
    Returns (ok, month_remaining_before_reservation). Fails open (ok=True) only if the DB is
    unreachable — the per-run ceiling (already enforced) remains the hard backstop."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; ledger reservation skipped.")
        return True, MONTH_CREDIT_CAP
    month = _month_bucket()
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "INSERT INTO ops.exa_credit_ledger (month, month_cap) VALUES (%s, %s) "
                "ON CONFLICT (month) DO NOTHING",
                (month, MONTH_CREDIT_CAP),
            )
            cur.execute(
                "SELECT credits_reserved, credits_actual, month_cap FROM ops.exa_credit_ledger "
                "WHERE month = %s FOR UPDATE",
                (month,),
            )
            reserved, actual, cap = cur.fetchone()
            remaining = int(cap) - max(int(reserved), int(actual))
            if projected > remaining:
                conn.rollback()
                return False, remaining
            cur.execute(
                "UPDATE ops.exa_credit_ledger SET credits_reserved = credits_reserved + %s "
                "WHERE month = %s",
                (projected, month),
            )
            conn.commit()
            return True, remaining
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ledger reserve failed ({exc}); proceeding under per-run ceiling only.")
        return True, MONTH_CREDIT_CAP


def _ledger_reconcile(projected: int, actual: int, release_reservation: bool) -> None:
    """Record actual spend; release the reservation only on a clean terminal (success / pre-
    create abort). On partial/failed-after-create the reservation is held (conservative —
    the webset may still be accruing credits server-side)."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return
    month = _month_bucket()
    rel = projected if release_reservation else 0
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "UPDATE ops.exa_credit_ledger "
                "SET credits_actual = credits_actual + %s, "
                "    credits_reserved = GREATEST(0, credits_reserved - %s) "
                "WHERE month = %s",
                (actual, rel, month),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ledger reconcile failed: {exc}")


def _record_run(row: dict) -> None:
    """Terminal run row → ops.exa_webset_runs (upsert on run_id). Best-effort."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.exa_webset_runs
                    (run_id, exa_webset_id, webset_identifier, webset_label, search_prompt,
                     tier, status, requested, returned, new_count, known_count,
                     credits_estimated, credits_actual, usd_actual, rejected_reason,
                     started_at, finished_at)
                VALUES (%(run_id)s, %(exa_webset_id)s, %(webset_identifier)s, %(webset_label)s,
                        %(search_prompt)s, %(tier)s, %(status)s, %(requested)s, %(returned)s,
                        %(new_count)s, %(known_count)s, %(credits_estimated)s, %(credits_actual)s,
                        %(usd_actual)s, %(rejected_reason)s, %(started_at)s, %(finished_at)s)
                ON CONFLICT (run_id) DO UPDATE SET
                    exa_webset_id = EXCLUDED.exa_webset_id, status = EXCLUDED.status,
                    returned = EXCLUDED.returned, new_count = EXCLUDED.new_count,
                    known_count = EXCLUDED.known_count, credits_actual = EXCLUDED.credits_actual,
                    usd_actual = EXCLUDED.usd_actual, rejected_reason = EXCLUDED.rejected_reason,
                    finished_at = EXCLUDED.finished_at, recorded_at = now()
                """,
                row,
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.exa_webset_runs write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint url. FLAT JSON body — no envelope."""
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

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


# ── Exa item shaping ─────────────────────────────────────────────────────────────────────
def _verification_status(evaluations: list) -> str:
    """Derive a coarse status from per-criterion evaluations. 'match'/'satisfied'==True →
    satisfied; all satisfied → verified; some → partial; none/empty → unverified."""
    if not evaluations:
        return "unverified"
    def _ok(ev):
        s = _g(ev, "satisfied", "match", "status", "result")
        return str(s).lower() in ("match", "yes", "true", "satisfied", "pass")
    flags = [_ok(ev) for ev in evaluations if isinstance(ev, dict)]
    if not flags:
        return "unverified"
    if all(flags):
        return "verified"
    return "partial" if any(flags) else "unverified"


def _item_to_record(item: dict, ctx: dict) -> dict:
    """Flatten one Exa item (Webset item OR synthesized Tier-B result) to a candidate row.
    Constants (webset_label, run_id, …) come from ctx so the downstream SQL stays literal-free."""
    import json

    props = _g(item, "properties", default={}) or {}
    entity = _g(props, "entity", default={}) or {}
    evaluations = _g(item, "evaluations", "verifications", default=[]) or []
    enrichments = _g(item, "enrichments", default=None)
    linkedin = _g(entity, "linkedinUrl", "linkedin_url") or _g(props, "linkedinUrl", "linkedin_url")
    return {
        "exa_item_id": str(_g(item, "id", "itemId", default="")) or None,
        "exa_webset_id": ctx["exa_webset_id"],
        "webset_label": ctx["webset_label"],
        "webset_identifier": ctx["webset_identifier"],
        "search_prompt": ctx["search_prompt"],
        "entity_type": _g(props, "type") or ctx["entity_type"],
        "company_name": _g(props, "name", "title"),
        "company_url": _g(props, "url"),
        "description": _g(props, "description", "summary"),
        "linkedin_url": linkedin,
        "verification_status": _verification_status(evaluations),
        "verification_json": json.dumps(evaluations, default=str) if evaluations else None,
        "match_criteria_json": ctx["match_criteria_json"],
        "enrichment_json": json.dumps(enrichments, default=str) if enrichments else None,
        "raw_item_json": json.dumps(item, default=str),
        "source_platform": SOURCE_PLATFORM,
        "raw_payload_uri": ctx["raw_payload_uri"],
        "exa_webset_run_id": ctx["run_id"],
        "discovered_at": _g(item, "createdAt", "created_at") or ctx["started_iso"],
        "snapshot_date": ctx["snapshot_date"],
    }


# ── Tier A — Websets API ─────────────────────────────────────────────────────────────────
def _tier_a_websets(*, run_id, search_prompt, count, entity_type, criteria, enrichments,
                    exclude_known, known_domains) -> tuple[str, list, str]:
    """Create a webset, poll to idle (bounded), cursor-pull all items.
    Returns (exa_webset_id, items, terminal) where terminal ∈ {'idle','timeout_partial'}."""
    import time

    body = {
        "search": {
            "query": search_prompt,
            "count": count,
            "entity": {"type": entity_type},
        },
        "enrichments": enrichments,           # already sanitised (no contact formats)
        "externalId": f"exa-webset-{run_id}",  # idempotency key
        "metadata": {"feed": FEED, "run_id": run_id},
    }
    if criteria:
        body["search"]["criteria"] = [{"description": c} for c in criteria]
    # Cost lever (§4.3): suppress already-known domains via excludeDomains when the set is
    # small enough for the inline filter (Imports is the scale path; out of scope for v1).
    if exclude_known and known_domains:
        capped = list(known_domains)[:1200]
        if capped:
            body["search"]["excludeDomains"] = capped

    created = _exa_request("POST", "/websets", base=WEBSETS_BASE, json_body=body)
    webset_id = str(_g(created, "id"))
    print(f"  webset created: {webset_id} (status={_g(created, 'status')})")

    # Poll to idle, bounded by POLL_BUDGET_S.
    waited, idx, terminal = 0.0, 0, "timeout_partial"
    while waited < POLL_BUDGET_S:
        sleep = POLL_BACKOFF_S[min(idx, len(POLL_BACKOFF_S) - 1)]
        time.sleep(sleep)
        waited += sleep
        idx += 1
        st = _g(_exa_request("GET", f"/websets/{webset_id}", base=WEBSETS_BASE), "status")
        print(f"  poll +{int(waited)}s → status={st}")
        if st in ("idle", "completed", "paused"):
            terminal = "idle"
            break

    items = _pull_items(webset_id)
    return webset_id, items, terminal


def _pull_items(webset_id: str) -> list:
    """Cursor-paginate GET /v0/websets/{id}/items until exhausted."""
    items, cursor = [], None
    while True:
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        page = _exa_request("GET", f"/websets/{webset_id}/items", base=WEBSETS_BASE, params=params)
        batch = _g(page, "data", "items", default=[]) or []
        items.extend(batch)
        cursor = _g(page, "nextCursor", "next_cursor")
        has_more = _g(page, "hasMore", "has_more", default=bool(cursor))
        if not cursor or not has_more or not batch:
            break
    print(f"  pulled {len(items)} items from webset {webset_id}")
    return items


# ── Tier B — cheap harvest (/findSimilar over seeds + /search) ───────────────────────────
def _tier_b_harvest(*, search_prompt, count, entity_type, seed_urls, exclude_known,
                    known_domains) -> tuple[list, float]:
    """Look-alike expansion. Returns (synthesized_items, usd_spent). Items carry no
    verification (status derives to 'unverified' downstream)."""
    import time

    category = "people" if entity_type == "person" else "company"
    per_call = min(count, 100)
    exclude = list(known_domains)[:1200] if (exclude_known and known_domains) else None
    results, usd = [], 0.0

    def _collect(payload: dict):
        nonlocal usd
        for r in _g(payload, "results", default=[]) or []:
            results.append({"id": _g(r, "id", "url"), "properties": {
                "type": entity_type, "url": _g(r, "url"),
                "name": _g(r, "title", "name"), "description": _g(r, "summary")}})
        cost = _g(payload, "costDollars", default={}) or {}
        usd += float(_g(cost, "total", default=0) or 0)

    for seed in (seed_urls or []):
        body = {"url": seed, "numResults": per_call, "excludeSourceDomain": True,
                "category": category}
        if exclude:
            body["excludeDomains"] = exclude
        _collect(_exa_request("POST", "/findSimilar", json_body=body))
        time.sleep(0.15)  # stay under the 10 QPS /findSimilar bucket

    if search_prompt and (not seed_urls):
        body = {"query": search_prompt, "type": "auto", "category": category,
                "numResults": per_call}
        if exclude:
            body["excludeDomains"] = exclude
        _collect(_exa_request("POST", "/search", json_body=body))

    # Dedup synthesized results by url (Tier B has no stable item id).
    seen, deduped = set(), []
    for r in results:
        u = _g(r["properties"], "url")
        if u and u not in seen:
            seen.add(u)
            deduped.append(r)
    print(f"  Tier B harvested {len(deduped)} unique candidates (${usd:.4f})")
    return deduped, usd


# ── The dispatched worker ────────────────────────────────────────────────────────────────
@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
        modal.Secret.from_name("exa-api"),
    ],
    timeout=60 * 60,
    memory=8192,
    cpu=4.0,
)
def ingest_exa_webset(
    trigger_callback_url: str | None = None,
    run_id: str | None = None,
    webset_identifier: str = "",
    search_prompt: str = "",
    max_results_limit: int = 100,
    entity_type: str = "company",
    criteria: list | None = None,
    tier: str = "harvest",
    seed_urls: list | None = None,
    enrichments: list | None = None,
    exclude_known_domains: bool = True,
    max_credits: int = PER_RUN_CREDIT_CEILING,
    dry_run: bool = False,
) -> dict:
    """Harvest one Exa webset → Gen-3 Lance, deduped vs the companies spine, under the
    ratified credit guardrails. Terminal state → ops.* + Trigger callback. Re-raises only on
    genuine failure (rejected / dry_run / timeout_partial are clean terminals, not errors)."""
    import datetime as dt
    import json
    import re
    import uuid

    import duckdb

    started = dt.datetime.now(dt.timezone.utc)
    run_id = run_id or uuid.uuid4().hex
    criteria = criteria or []
    seed_urls = seed_urls or []
    enrichments = enrichments or []
    year = started.year
    webset_label = f"{webset_identifier}_{year}"

    so = _r2_storage_options()
    counts = {"requested": 0, "returned": 0, "new": 0, "known": 0}
    credits_est = credits_act = 0
    usd_act = 0.0
    exa_webset_id = None
    status = "failed"
    reason = None
    reserved = False

    def _terminal(st, *, reraise_msg=None):
        """Single terminal path: ledger reconcile + ops row + callback."""
        finished = dt.datetime.now(dt.timezone.utc)
        # Release the reservation on any clean terminal, AND on a failure that occurred BEFORE a
        # webset was created (exa_webset_id is None → nothing accrued on Exa's side). Hold it only
        # when a webset exists and may still be accruing credits server-side.
        clean_release = st in ("success", "rejected", "dry_run") or (st == "failed" and exa_webset_id is None)
        if reserved:
            _ledger_reconcile(credits_est, credits_act, release_reservation=clean_release)
        _record_run({
            "run_id": run_id, "exa_webset_id": exa_webset_id,
            "webset_identifier": webset_identifier, "webset_label": webset_label,
            "search_prompt": search_prompt, "tier": tier, "status": st,
            "requested": counts["requested"], "returned": counts["returned"],
            "new_count": counts["new"], "known_count": counts["known"],
            "credits_estimated": credits_est, "credits_actual": credits_act,
            "usd_actual": round(usd_act, 4), "rejected_reason": reason,
            "started_at": started, "finished_at": finished,
        })
        _post_callback(trigger_callback_url, {
            "status": st, "run_id": run_id, "exa_webset_id": exa_webset_id,
            "webset_label": webset_label, "tier": tier,
            "requested": counts["requested"], "returned": counts["returned"],
            "new_count": counts["new"], "known_count": counts["known"],
            "credits_estimated": credits_est, "credits_actual": credits_act,
            "usd_actual": round(usd_act, 4), "rejected_reason": reason,
            "discovered_websets_uri": DISCOVERED_URI,
        })
        if reraise_msg:
            raise RuntimeError(reraise_msg)

    try:
        # ── Guardrails (no spend before all pass) ──────────────────────────────────────
        if os.environ.get("EXA_ENGINE_ENABLED", "true").lower() != "true":
            status, reason = "rejected", "kill switch: EXA_ENGINE_ENABLED is not 'true'"
            return _terminal(status) or {"status": status, "reason": reason}
        if not re.fullmatch(r"[a-z0-9_]{3,64}", webset_identifier or ""):
            status, reason = "rejected", f"invalid webset_identifier {webset_identifier!r}"
            return _terminal(status) or {"status": status, "reason": reason}
        if not (search_prompt and len(search_prompt) >= 8):
            status, reason = "rejected", "search_prompt too short (min 8 chars)"
            return _terminal(status) or {"status": status, "reason": reason}

        # Tier A (Websets precision) is DISABLED BY DEFAULT — the Websets API is Pro-plan-gated.
        # The full Tier A path stays intact below; flip EXA_TIER_A_ENABLED=true on the exa-api
        # secret once the account is upgraded to enable it. Default operational tier is 'harvest'.
        if tier == "precision" and os.environ.get("EXA_TIER_A_ENABLED", "false").lower() != "true":
            status, reason = "rejected", (
                "Tier A (Websets precision) is disabled — requires a Pro plan. Set "
                "EXA_TIER_A_ENABLED=true on the exa-api secret to enable. Default tier is 'harvest'.")
            return _terminal(status) or {"status": status, "reason": reason}

        count = max(1, min(int(max_results_limit), HARD_RESULT_CAP))  # D4
        counts["requested"] = count

        # D2 — strip every contact-format enrichment, unconditionally.
        dropped = [e for e in enrichments if str(_g(e, "format", default="")).lower() in FORBIDDEN_ENRICH_FORMATS]
        if dropped:
            print(f"  D2: dropped {len(dropped)} contact enrichment(s) — Exa is discovery-only.")
        enrichments = [e for e in enrichments if str(_g(e, "format", default="")).lower() not in FORBIDDEN_ENRICH_FORMATS]
        text_enrich = len(enrichments)

        # Per-run ceiling (D1) — payload may lower the cap, never raise it past the ceiling.
        effective_cap = min(int(max_credits), PER_RUN_CREDIT_CEILING)
        credits_est = count * (CREDITS_PER_RESULT + CREDITS_PER_ENRICH_ROW * text_enrich)
        if tier == "precision" and credits_est > effective_cap:
            status, reason = "rejected", (
                f"projected {credits_est} credits exceeds per-run ceiling {effective_cap} "
                f"(count={count} × {CREDITS_PER_RESULT + 2 * text_enrich}/item). Lower max_results_limit.")
            return _terminal(status) or {"status": status, "reason": reason}

        # dry_run: return the estimate with ZERO side effects (before any reservation/spend).
        if dry_run:
            status, reason = "dry_run", f"estimate only — {credits_est} credits (${credits_est * CREDIT_USD:.2f})"
            return _terminal(status) or {"status": status, "credits_estimated": credits_est}

        # Monthly ledger reserve (atomic check-then-reserve) — precision/credits only.
        if tier == "precision":
            ok, remaining = _ledger_reserve(credits_est)
            if not ok:
                status, reason = "rejected", f"projected {credits_est} credits exceeds month remaining {remaining}"
                return _terminal(status) or {"status": status, "reason": reason}
            reserved = True

        # ── Known-domain set (the JIT dedup authority + the suppression lever) ──────────
        known_domains = _lance_column_set(COMPANIES_URI, "normalized_domain", so)
        print(f"  companies spine: {len(known_domains)} known domains")

        # ── Discovery ──────────────────────────────────────────────────────────────────
        terminal = "idle"
        if tier == "precision":
            exa_webset_id, items, terminal = _tier_a_websets(
                run_id=run_id, search_prompt=search_prompt, count=count,
                entity_type=entity_type, criteria=criteria, enrichments=enrichments,
                exclude_known=exclude_known_domains, known_domains=known_domains)
            credits_act = len(items) * (CREDITS_PER_RESULT + CREDITS_PER_ENRICH_ROW * text_enrich)
            usd_act = credits_act * CREDIT_USD
        else:
            exa_webset_id = f"harvest-{run_id}"
            items, usd_act = _tier_b_harvest(
                search_prompt=search_prompt, count=count, entity_type=entity_type,
                seed_urls=seed_urls, exclude_known=exclude_known_domains,
                known_domains=known_domains)
        counts["returned"] = len(items)

        # ── Raw capture (transport) + shape candidates ─────────────────────────────────
        landing_uri = f"{_LANDING}/exa_webset_id={exa_webset_id}/run_id={run_id}/items.parquet"
        ctx = {
            "exa_webset_id": exa_webset_id, "webset_label": webset_label,
            "webset_identifier": webset_identifier, "search_prompt": search_prompt,
            "entity_type": entity_type, "run_id": run_id, "raw_payload_uri": landing_uri,
            "match_criteria_json": json.dumps(criteria) if criteria else None,
            "snapshot_date": started.date().isoformat(), "started_iso": started.isoformat(),
        }
        records = [_item_to_record(it, ctx) for it in items]
        records = [r for r in records if r["exa_item_id"]]  # need the idempotency key

        if not records:
            status = terminal if terminal == "timeout_partial" else "success"
            reason = "no items returned"
            return _terminal(status) or {"status": status, "items": 0}

        import pyarrow as pa
        cand_cols = list(records[0].keys())
        cand_tbl = pa.table({c: pa.array([r[c] for r in records], type=pa.string()) for c in cand_cols})

        # ── DuckDB: normalize domain, JIT dedup vs companies, split new/known ───────────
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            con.register("candidates_raw", cand_tbl)
            known_tbl = pa.table({"normalized_domain": pa.array(sorted(known_domains), type=pa.string())})
            con.register("known_companies", known_tbl)

            con.execute(f"""
                CREATE TEMP TABLE joined AS
                WITH norm AS (
                    SELECT *, {_normalized_domain('company_url')} AS discovered_domain
                    FROM candidates_raw
                )
                SELECT n.*, (k.normalized_domain IS NOT NULL) AS is_known
                FROM norm n
                LEFT JOIN known_companies k ON n.discovered_domain = k.normalized_domain
            """)

            new_arrow = con.execute("""
                SELECT discovered_domain, company_name, company_url, exa_item_id, exa_webset_id,
                       webset_label, webset_identifier, search_prompt, entity_type,
                       verification_status, verification_json, match_criteria_json, description,
                       enrichment_json, linkedin_url, source_platform, raw_payload_uri,
                       raw_item_json, exa_webset_run_id, discovered_at, snapshot_date
                FROM joined WHERE NOT is_known
            """).fetch_arrow_table()
            known_arrow = con.execute("""
                SELECT discovered_domain AS normalized_domain, exa_webset_id, webset_label,
                       exa_item_id, verification_status, verification_json, raw_item_json,
                       exa_webset_run_id, discovered_at, source_platform
                FROM joined WHERE is_known AND discovered_domain IS NOT NULL
            """).fetch_arrow_table()

            # Best-effort landing parquet (ZSTD) — full fidelity also lives in raw_item_json.
            try:
                _configure_duckdb_r2(con)
                con.execute(
                    f"COPY (SELECT * FROM candidates_raw) TO '{landing_uri}' "
                    "(FORMAT PARQUET, COMPRESSION ZSTD);")
                print(f"  landing parquet → {landing_uri}")
            except Exception as exc:  # noqa: BLE001 — SoR fidelity is guaranteed in Lance
                print(f"  WARN landing parquet skipped: {exc}")
        finally:
            con.close()

        # ── Idempotency: drop items already present in the staging datasets ─────────────
        import pyarrow.compute as pc

        def _drop_existing(tbl, ds_name):
            if tbl.num_rows == 0:
                return tbl
            existing = _lance_column_set(DATASET_URI[ds_name], "exa_item_id", so)
            if not existing:
                return tbl
            mask = pc.invert(pc.is_in(tbl.column("exa_item_id"), value_set=pa.array(sorted(existing))))
            return tbl.filter(mask)

        new_arrow = _drop_existing(new_arrow, "discovered_websets")
        known_arrow = _drop_existing(known_arrow, "webset_membership")

        # ── Write Lance + index ────────────────────────────────────────────────────────
        new_n = _write_lance_append_or_create("discovered_websets", _enforce_schema("discovered_websets", new_arrow), so)
        if new_n:
            _create_indexes("discovered_websets", so)
        known_n = _write_lance_append_or_create("webset_membership", _enforce_schema("webset_membership", known_arrow), so)
        if known_n:
            _create_indexes("webset_membership", so)
        counts["new"], counts["known"] = new_n, known_n
        print(f"  wrote {new_n} new → discovered_websets · {known_n} known → webset_membership")

        status = "timeout_partial" if terminal == "timeout_partial" else "success"
        return _terminal(status) or {
            "status": status, "run_id": run_id, "exa_webset_id": exa_webset_id,
            "returned": counts["returned"], "new": new_n, "known": known_n,
            "credits_actual": credits_act, "usd_actual": round(usd_act, 4)}

    except Exception as exc:  # noqa: BLE001 — terminal handling + re-raise so Modal marks failed
        status, reason = "failed", str(exc)
        _terminal("failed", reraise_msg=f"exa webset ingest failed: {exc}")


# ── Ops bootstrap + read-back ────────────────────────────────────────────────────────────
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.exa_webset_runs + ops.exa_credit_ledger in the HQX control-plane DB."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        cur.execute(
            "INSERT INTO ops.exa_credit_ledger (month, month_cap) VALUES (date_trunc('month', now())::date, %s) "
            "ON CONFLICT (month) DO NOTHING",
            (MONTH_CREDIT_CAP,))
        conn.commit()
    print(f"ops.exa_webset_runs + ops.exa_credit_ledger ready (month_cap={MONTH_CREDIT_CAP}).")
    return {"tables": ["ops.exa_webset_runs", "ops.exa_credit_ledger"], "month_cap": MONTH_CREDIT_CAP}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10)
def verify() -> dict:
    """Read-back assertions on the staging datasets (row counts, committed indexes)."""
    import lance

    so = _r2_storage_options()
    out: dict[str, dict] = {}
    for ds_name, uri in DATASET_URI.items():
        try:
            ds = lance.dataset(uri, storage_options=so)
            idx = [ix.get("name", str(ix)) if isinstance(ix, dict) else getattr(ix, "name", str(ix))
                   for ix in ds.list_indices()]
            names = [f.name for f in ds.schema]
            scols = [c for c in ("discovered_domain", "normalized_domain", "company_name",
                                 "verification_status", "webset_label") if c in names]
            sample = ds.to_table(columns=scols).slice(0, 5).to_pylist() if scols else []
            out[ds_name] = {"uri": uri, "rows": ds.count_rows(), "indexes": sorted(idx), "sample": sample}
        except Exception as exc:  # noqa: BLE001 — not yet materialized
            out[ds_name] = {"uri": uri, "rows": 0, "error": str(exc)}
        print(f"{ds_name}: {out[ds_name]}")
    return out


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops DDL (HQX). Safe — no Exa calls, no credits."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def verify_only() -> None:
    import json

    print(json.dumps(verify.remote(), indent=2, default=str))
