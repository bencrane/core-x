"""Compute worker — OSHA "Sniper" daily severe-violation triggers (DOL Open Data API).

Part of the ``osha-pipelines`` Modal app. NOT directly exposed — it is spawned by
the Universal Dispatcher (core/modal_dispatcher.py), the only proxy-authed endpoint
in the fleet. This worker has no web endpoint and carries NO embedded schedule
(``modal.Cron`` is forbidden — cadence lives in src/trigger/osha_sniper.ts).

Purpose: a quota-safe daily "sniper" that pulls only the most recent severe OSHA
citations and lands them as an outbound-ready, name-normalized trigger table. The
DOL Open Data API enforces a strict daily usage plan (~20 calls/day), so this
worker is governed by a HARD circuit breaker (MAX_API_CALLS) that aborts before it
can ever overrun the plan.

Two design corrections grounded in what the live DOL feed actually returns:
  1. VIOLATIONS lead, inspections enrich. The goal is "most recent severe
     violations." A citation is issued weeks-to-months AFTER its inspection opens,
     so recent inspections carry no violations yet — pulling inspections first
     yields an empty join. Pulling violations first always joins: every violation
     has exactly one parent inspection (the company/site/NAICS enrichment).
  2. Recency is anchored to the DATA FRONTIER, not wall-clock. The enforcement feed
     loads on a lag (newest issuance_date trails "today" by ~weeks). A literal
     "last 7 days from today" filter is therefore empty. The trailing window is
     instead measured back from max(issuance_date) in the pull — i.e. the 7 most
     recent days of AVAILABLE severe citations. Self-adjusts as DOL loads newer data.

Data plane (clean-room — no Iceberg, no Polaris, Arrow-only interchange):
    DOL /v4/get/OSHA/violation/csv   (Step 1: severe viol_types, newest issuance first)
    DOL /v4/get/OSHA/inspection/csv  (Step 2: activity_nr IN <Step-1 recent ids>)
      → requests (-G/--data-urlencode semantics)  → /tmp CSVs   (Python: I/O only)
      → DuckDB read_csv → frontier-trim + join + normalize        (100% in SQL)
      → Arrow table
      → lance.merge_insert(trigger_uid)  →  s3://data-sink/active/osha_daily_triggers/

Load-bearing design (see the block comments inline):
  • Step 1 — two calls: (A) a 1-row frontier probe (issuance_date DESC) reads the true
    data frontier, then (B) a server-side ``issuance_date > frontier - lookback`` pull
    fetches EXACTLY the window. No full-page scan: the returned row count IS the true
    in-window total, and a full page (page_full) signals a genuine >ROW_LIMIT overflow.
  • Step 2 — up to (MAX_API_CALLS-2) calls: the parent inspections for the recent
    activity_nrs, batched so each request URL stays under the gateway length limit.
  • Dedup — synthetic ``trigger_uid = activity_nr || '-' || citation_id`` is the
    primary key; daily windows overlap, so we UPSERT (merge_insert), never append
    blindly. No (activity_nr, citation_id) pair is ever duplicated.

Control plane (Trigger v4 durable callback): the worker accepts
``trigger_callback_url`` (the pre-signed waitpoint URL). On terminal state — success
OR failure — it (1) writes the run row to ``ops.osha_sniper_runs`` via psycopg and
(2) POSTs the RAW terminal payload to ``trigger_callback_url`` to wake the suspended
Trigger run. No polling, no heartbeat.

    modal run    pipelines/osha/osha_sniper.py   # manual (no callback)
    modal deploy pipelines/osha/osha_sniper.py
"""

from __future__ import annotations

import os

import modal

# Canonical blocking-key macro (single source of truth). KEEPS entity tokens (LLC/INC) — that
# is what makes estab_name's normalized_legal_name match the sos_normalized / SAM spines.
from core.name_norm import name_norm as _name_norm

# ── DOL Open Data API surface (apiprod.dol.gov v4) ─────────────────────────────
# Endpoint template:  <base>/<agency>/<endpoint>/<format>?<params>
# Auth:  X-API-KEY is read from the QUERY STRING (verified: header → 401, query
# param → 200), so it travels in `params` and is redacted from logs. Agency
# abbreviations are uppercase in the catalog (MSHA, ILAB, WB → OSHA). All four
# tokens below are env-overridable so a catalog rename is a one-line fix.
DOL_API_BASE = os.environ.get("DOL_API_BASE", "https://apiprod.dol.gov/v4/get")
OSHA_AGENCY = os.environ.get("OSHA_AGENCY", "OSHA")
INSPECTION_ENDPOINT = os.environ.get("OSHA_INSPECTION_ENDPOINT", "inspection")
VIOLATION_ENDPOINT = os.environ.get("OSHA_VIOLATION_ENDPOINT", "violation")

# DOL filter_object field names.
VIOL_TYPE_FIELD = "viol_type"          # Step-1 severity gate (server-side)
VIOL_DATE_FIELD = "issuance_date"      # Step-1 recency sort + frontier anchor
INSP_ACTIVITY_FIELD = "activity_nr"    # Step-2 join key (inspection IN-list)

# Column projections requested via the `fields` param — lean payloads, predictable
# schema. Exactly the columns the join needs (names live-confirmed against the DOL
# OSHA endpoints). Set OSHA_REQUEST_ALL_FIELDS=1 to drop `fields` (request all).
INSPECTION_FIELDS = [
    "activity_nr", "estab_name", "site_address", "site_city",
    "site_state", "site_zip", "naics_code", "open_date",
]
VIOLATION_FIELDS = [
    "activity_nr", "citation_id", "standard", "viol_type",
    "gravity", "current_penalty", "issuance_date",
]

# ── Quota governor — the hard circuit breaker ─────────────────────────────────
# Every DOL HTTP call is charged against this budget BEFORE it leaves the process;
# the (MAX_API_CALLS+1)th attempt aborts instantly, protecting the ~20-call/day plan.
# Budget split: Step 1 spends 2 (frontier probe + server-side windowed pull), leaving
# (MAX_API_CALLS-2) for Step-2 inspection batches (~90 activity_nrs each → ~900/day at
# 12). A normal day lands in ~7 calls; the rest is headroom for backfill spikes.
MAX_API_CALLS = 12

# Per-DAY plan ceiling. The per-run breaker above cannot see sibling runs, so before any
# DOL call the worker sums today's api_calls_used from ops.osha_sniper_runs and caps this
# run's budget so cumulative daily calls stay under the DOL plan — held at 18 (margin
# under ~20/day). Counts ledgered worker runs only (ad-hoc curls are not tracked).
DAILY_DOL_CALL_BUDGET = int(os.environ.get("OSHA_DAILY_DOL_BUDGET", "18"))

# DOL per-request hard cap (10k rows OR 5 MB, whichever first). Step 1 now filters
# issuance_date server-side, so a FULL page (== ROW_LIMIT) is no longer routine — it
# means the in-window severe set itself overflows one page (page_full → truncated),
# the real early-warning that lookback_days must tighten or Step 1 must paginate.
ROW_LIMIT = 10000

# apiprod.dol.gov sits behind AWS WAF, whose Core Rule Set blocks any QUERY STRING
# over 2048 bytes (SizeRestrictions_QUERYSTRING → 403 Forbidden, NOT 414). The
# Step-2 activity_nr IN-list is the only large query component, so batches are
# packed to keep the encoded query string under this margin (~90 nine-digit ids).
MAX_QUERYSTRING_BYTES = 1900

# "Severe" = everything except Other-than-Serious ('O'). Serious / Willful / Repeat
# / Failure-to-abate / Unclassified all signal a real safety problem worth mailing.
# Applied server-side in Step 1; relax via severe_only=False (keep all types).
SEVERE_VIOL_TYPES = ["S", "W", "R", "F", "U"]

# ── Lance system-of-record (R2) — the directive's outbound-ready sink ──────────
LANCE_URI = os.environ.get(
    "OSHA_TRIGGERS_LANCE_URI", "s3://data-sink/active/osha_daily_triggers/"
)
DATA_STORAGE_VERSION = "2.1"          # current Lance default (02_lancedb_storage.md)
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# trigger_uid is the (activity_nr, citation_id) primary key; normalized_legal_name is
# the cross-spine bridge key; activity_nr joins back to the wider OSHA enforcement
# graph. site_state / viol_type are low-cardinality campaign-routing filters.
BTREE_INDEXES = ["trigger_uid", "normalized_legal_name", "activity_nr"]
BITMAP_INDEXES = ["site_state", "viol_type"]

FEED = "osha_daily_triggers"
SCRATCH_DIR = "/tmp"

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.osha_sniper_runs (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed               text        NOT NULL,
    snapshot_date      date,
    lookback_days      int,
    frontier_date      date,
    api_calls_used     int,
    violations_pulled  bigint,
    activity_nrs       int,
    inspections_pulled bigint,
    rows_upserted      bigint,
    truncated          boolean,
    dropped            int,
    page_full          boolean,
    status             text        NOT NULL,
    error              text,
    started_at         timestamptz,
    completed_at       timestamptz,
    recorded_at        timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE ops.osha_sniper_runs ADD COLUMN IF NOT EXISTS frontier_date date;
ALTER TABLE ops.osha_sniper_runs ADD COLUMN IF NOT EXISTS dropped       int;
ALTER TABLE ops.osha_sniper_runs ADD COLUMN IF NOT EXISTS page_full     boolean;
CREATE INDEX IF NOT EXISTS osha_sniper_runs_status_idx      ON ops.osha_sniper_runs (status);
CREATE INDEX IF NOT EXISTS osha_sniper_runs_recorded_at_idx ON ops.osha_sniper_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    # Lower bounds, not exact pins — freeze with `modal shell` once validated.
    "duckdb>=1.5,<2",        # to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",  # terminal state write to ops.*
).add_local_python_source("core.name_norm")  # ship the canonical blocking-key macro to the container

app = modal.App("osha-pipelines", image=image)


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the r2-credentials secret."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


class QuotaExceeded(RuntimeError):
    """Raised when the worker would exceed MAX_API_CALLS — the hard circuit breaker."""


class _Done(Exception):
    """Internal early-exit sentinel (clean success with nothing to materialize)."""


class _CallBudget:
    """Hard circuit breaker for the DOL daily usage plan. ``charge()`` is called
    BEFORE every HTTP request; once ``used`` reaches the cap, the next charge raises
    and the worker aborts without making the call."""

    def __init__(self, max_calls: int) -> None:
        self.max_calls = max_calls
        self.used = 0

    def charge(self) -> int:
        if self.used >= self.max_calls:
            raise QuotaExceeded(
                f"MAX_API_CALLS={self.max_calls} reached; refusing call "
                f"#{self.used + 1} to protect the DOL daily usage plan."
            )
        self.used += 1
        return self.used

    @property
    def remaining(self) -> int:
        return self.max_calls - self.used


def _dol_csv_get(endpoint: str, params: dict, budget: _CallBudget) -> str:
    """One DOL CSV GET with exact `-G --data-urlencode` semantics (requests `params`
    URL-encodes each field onto the query string). The DOL gateway reads the
    ``X-API-KEY`` from the **query string**, NOT an HTTP header — so the caller
    includes it in ``params`` (verified: header auth → 401, query-param → 200). The
    key is redacted from the log line. Charges the breaker before the request."""
    import requests

    n = budget.charge()
    url = f"{DOL_API_BASE}/{OSHA_AGENCY}/{endpoint}/csv"
    redacted = {k: v for k, v in params.items() if k != "X-API-KEY"}
    fo = redacted.get("filter_object", "")
    if isinstance(fo, str) and len(fo) > 120:
        redacted["filter_object"] = fo[:120] + f"…(+{len(fo) - 120}b)"
    print(f"DOL call #{n}/{budget.max_calls} → {endpoint} :: {redacted}")
    resp = requests.get(url, params=params, headers={"Accept": "text/csv, */*"}, timeout=(15, 180))
    resp.raise_for_status()
    return resp.text


def _filter_object_window(severe_only: bool, cutoff_iso: str | None) -> str | None:
    """Step-1 filter_object: severe viol_types (server-side `in`) AND/OR issuance_date
    `gt` the frontier-anchored cutoff (server-side recency). The {"and": [...]} compound
    is the DOL v4 idiom — live-confirmed against apiprod.dol.gov, and the pre-violations-
    first sniper used the same shape on open_date. Returns None only when neither
    predicate applies (severe_only False AND no cutoff)."""
    import json

    preds: list[dict] = []
    if severe_only:
        preds.append({"field": VIOL_TYPE_FIELD, "operator": "in", "value": SEVERE_VIOL_TYPES})
    if cutoff_iso:
        preds.append({"field": VIOL_DATE_FIELD, "operator": "gt", "value": cutoff_iso})
    if not preds:
        return None
    obj = preds[0] if len(preds) == 1 else {"and": preds}
    return json.dumps(obj, separators=(",", ":"))


def _probe_frontier(api_key: str, severe_only: bool, budget: _CallBudget) -> str | None:
    """Step-1 Call A — the cheapest possible pull (limit=1, issuance_date DESC) to read
    the true data frontier: the single most-recent severe issuance_date (ISO date), or
    None if the severe feed is empty. One DOL call; charges the breaker."""
    import csv
    import io

    params: dict = {
        "limit": 1, "offset": 0, "sort": "desc", "sort_by": VIOL_DATE_FIELD,
        "fields": ",".join(["activity_nr", VIOL_DATE_FIELD]), "X-API-KEY": api_key,
    }
    fo = _filter_object_window(severe_only, None)
    if fo:
        params["filter_object"] = fo
    text = _dol_csv_get(VIOLATION_ENDPOINT, params, budget)
    for row in csv.DictReader(io.StringIO(text)):
        raw = (row.get(VIOL_DATE_FIELD) or "").strip()
        return raw[:10] or None        # DOL returns 'YYYY-MM-DD 00:00:00'
    return None


def _nonempty_csv(path: str) -> bool:
    """A DOL CSV with zero matches comes back as an empty body (no header). DuckDB
    read_csv chokes on that ('column0' binder error), so callers must pre-filter.
    True iff the file holds a header row (a comma in its first non-blank line)."""
    try:
        if os.path.getsize(path) < 2:
            return False
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if line.strip():
                    return "," in line
        return False
    except OSError:
        return False


def _pack_activity_batches(
    activity_nrs: list[str], remaining_calls: int, base_params: dict
) -> tuple[list[list[str]], int]:
    """Greedily pack activity_nrs into Step-2 batches whose encoded QUERY STRING stays
    under MAX_QUERYSTRING_BYTES (the AWS WAF 2048-byte rule), capped at
    ``remaining_calls`` batches. Returns (batches, dropped_count). Inputs are
    recency-ordered, so a truncation drops the OLDEST ids first — surfaced, not
    silently capped."""
    import json
    import urllib.parse

    batches: list[list[str]] = []
    i, n = 0, len(activity_nrs)
    while i < n and len(batches) < remaining_calls:
        batch: list[str] = []
        while i < n:
            fo = json.dumps(
                {"field": INSP_ACTIVITY_FIELD, "operator": "in", "value": batch + [activity_nrs[i]]},
                separators=(",", ":"),
            )
            qslen = len(urllib.parse.urlencode(dict(base_params, filter_object=fo)))
            if qslen > MAX_QUERYSTRING_BYTES and batch:
                break               # close batch; this id starts the next one
            batch.append(activity_nrs[i])
            i += 1
            if qslen > MAX_QUERYSTRING_BYTES:
                break               # single id already at budget — take it alone
        batches.append(batch)
    dropped = n - sum(len(b) for b in batches)
    return batches, dropped


def _recent_activity_nrs_sql(viol_path: str, severe_only: bool, lookback_days: int) -> str:
    """Recency-ordered DISTINCT activity_nrs whose newest severe citation falls within
    ``lookback_days`` of the data frontier (max issuance_date in the pull). Drives the
    Step-2 inspection fetch — a control-flow id list, not the write-path interchange."""
    severe_pred = ""
    if severe_only:
        in_list = ", ".join("'" + t + "'" for t in SEVERE_VIOL_TYPES)
        severe_pred = f"      AND upper(trim(viol_type)) IN ({in_list})\n"
    return f"""
WITH vr AS (
    SELECT nullif(trim(activity_nr), '') AS activity_nr,
           TRY_CAST(issuance_date AS DATE) AS d
    FROM read_csv('{viol_path}', all_varchar = true, header = true,
                  sample_size = -1, ignore_errors = true)
    WHERE nullif(trim(activity_nr), '') IS NOT NULL
{severe_pred}),
fr AS (SELECT max(d) AS frontier FROM vr)
SELECT activity_nr FROM (
    SELECT vr.activity_nr, max(vr.d) AS md
    FROM vr, fr
    WHERE vr.d IS NOT NULL AND vr.d > fr.frontier - {int(lookback_days)}
    GROUP BY vr.activity_nr
)
ORDER BY md DESC NULLS LAST
"""


def _build_transform_sql(viol_path: str, insp_paths: list[str], snapshot_iso: str,
                         severe_only: bool, lookback_days: int,
                         site_states: list[str] | None) -> str:
    """DuckDB does 100% of the transform: read the severe-violation pull + the parent
    inspections (all_varchar + TRY_CAST), trim violations to the frontier window,
    inner-join on activity_nr (every kept violation has a parent inspection),
    normalize estab_name → normalized_legal_name, derive the trigger_uid PK, optional
    state scope, and de-dup the (activity_nr, citation_id) pair within the batch.
    Controlled /tmp paths only — no injection surface."""
    insp_list = "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in insp_paths) + "]"
    severe_pred = ""
    if severe_only:
        in_list = ", ".join("'" + t + "'" for t in SEVERE_VIOL_TYPES)
        severe_pred = f"        AND upper(trim(viol_type)) IN ({in_list})\n"
    state_pred = ""
    if site_states:
        st_list = ", ".join("'" + s.upper().replace("'", "''") + "'" for s in site_states)
        state_pred = f"  AND i.site_state IN ({st_list})\n"
    return f"""
WITH viol_raw AS (
    SELECT
        nullif(trim(activity_nr), '')       AS activity_nr,
        nullif(trim(citation_id), '')       AS citation_id,
        nullif(trim(standard), '')          AS standard,
        nullif(upper(trim(viol_type)), '')  AS viol_type,
        TRY_CAST(gravity AS INTEGER)        AS gravity,
        TRY_CAST(current_penalty AS DOUBLE) AS current_penalty,
        TRY_CAST(issuance_date AS DATE)     AS issuance_date
    FROM read_csv('{viol_path}', all_varchar = true, header = true,
                  sample_size = -1, ignore_errors = true)
    WHERE nullif(trim(citation_id), '') IS NOT NULL
{severe_pred}),
frontier AS (SELECT max(issuance_date) AS f FROM viol_raw),
viol AS (
    SELECT v.* FROM viol_raw v, frontier
    WHERE v.issuance_date IS NOT NULL AND v.issuance_date > frontier.f - {int(lookback_days)}
),
insp AS (
    SELECT
        nullif(trim(activity_nr), '')   AS activity_nr,
        nullif(trim(estab_name), '')    AS estab_name,
        nullif(trim(site_address), '')  AS site_address,
        nullif(trim(site_city), '')     AS site_city,
        nullif(upper(trim(site_state)), '') AS site_state,
        nullif(left(regexp_replace(CAST(site_zip AS VARCHAR), '[^0-9]', '', 'g'), 5), '') AS site_zip,
        nullif(trim(naics_code), '')    AS naics_code,
        TRY_CAST(open_date AS DATE)     AS open_date
    FROM read_csv({insp_list}, all_varchar = true, header = true,
                  sample_size = -1, ignore_errors = true, union_by_name = true)
)
SELECT
    v.activity_nr || '-' || v.citation_id AS trigger_uid,
    v.activity_nr,
    v.citation_id,
    i.estab_name,
    {_name_norm('i.estab_name')}          AS normalized_legal_name,
    i.site_address,
    i.site_city,
    i.site_state,
    i.site_zip,
    i.naics_code,
    i.open_date,
    v.standard,
    v.viol_type,
    v.gravity,
    v.current_penalty,
    v.issuance_date,
    CAST('{snapshot_iso}' AS DATE)        AS snapshot_date,
    CAST(now() AS TIMESTAMP)              AS ingested_at
FROM viol v
JOIN insp i ON i.activity_nr = v.activity_nr
WHERE v.citation_id IS NOT NULL AND v.activity_nr IS NOT NULL
{state_pred}QUALIFY row_number() OVER (
    PARTITION BY v.activity_nr || '-' || v.citation_id
    ORDER BY v.current_penalty DESC NULLS LAST
) = 1
"""


def _materialize(arrow_table, so: dict, mode: str) -> tuple[int, str]:
    """Commit the Arrow batch to Lance. ``merge`` UPSERTs on trigger_uid so overlapping
    daily windows never duplicate an (activity_nr, citation_id) pair; ``overwrite``
    rebuilds from the current window. Creates the dataset on first run. Returns
    (rows_in_batch, action)."""
    import lance

    if arrow_table.num_rows == 0:
        return 0, "noop-empty"

    if mode == "overwrite":
        lance.write_dataset(
            arrow_table, LANCE_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        return arrow_table.num_rows, "overwrite"

    try:
        ds = lance.dataset(LANCE_URI, storage_options=so)
    except Exception as exc:  # noqa: BLE001 — first-ever run: dataset absent → create
        print(f"Dataset not present yet ({exc}); creating fresh via overwrite.")
        lance.write_dataset(
            arrow_table, LANCE_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        return arrow_table.num_rows, "create"

    (
        ds.merge_insert("trigger_uid")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(arrow_table)
    )
    return arrow_table.num_rows, "merge"


def _build_indexes(so: dict) -> None:
    """BTREE on resolution keys, BITMAP on categoricals; replace=True → idempotent
    and covers the rows added this run. An index miss must not fail the ingest."""
    import lance

    ds = lance.dataset(LANCE_URI, storage_options=so)
    for col in BTREE_INDEXES:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in BITMAP_INDEXES:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    try:
        ds.cleanup_old_versions(retain_versions=30)
    except Exception as exc:  # noqa: BLE001
        print(f"  WARN cleanup_old_versions failed: {exc}")


def _daily_calls_used(snapshot_iso: str) -> int:
    """Cumulative DOL calls already charged TODAY by prior osha_sniper runs — sum of
    ops.osha_sniper_runs.api_calls_used for the current UTC date. Drives the per-DAY
    budget cap (the per-run breaker cannot see sibling runs). Best-effort: any ledger
    error returns 0 (fail-open to the per-run breaker — a DB hiccup never blocks ingest;
    a missing table on the first-ever run also lands here and returns 0)."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return 0
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(sum(api_calls_used), 0) FROM ops.osha_sniper_runs "
                "WHERE feed = %s AND (recorded_at AT TIME ZONE 'UTC')::date = %s::date",
                (FEED, snapshot_iso),
            )
            return int(cur.fetchone()[0] or 0)
    except Exception as exc:  # noqa: BLE001 — guard must not block the ingest
        print(f"WARN: daily-budget probe failed ({exc}); per-run breaker only.")
        return 0


def _record_run(metrics: dict, status: str, error, started_at, completed_at) -> None:
    """Terminal run row → ops.osha_sniper_runs (psycopg). Best-effort: an audit-write
    failure must never mask an otherwise-good ingest."""
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
                INSERT INTO ops.osha_sniper_runs
                    (feed, snapshot_date, lookback_days, frontier_date, api_calls_used,
                     violations_pulled, activity_nrs, inspections_pulled,
                     rows_upserted, truncated, dropped, page_full,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    FEED, metrics.get("snapshot_date"), metrics.get("lookback_days"),
                    metrics.get("frontier_date"), metrics.get("api_calls_used"),
                    metrics.get("violations_pulled"), metrics.get("activity_nrs"),
                    metrics.get("inspections_pulled"), metrics.get("rows_upserted"),
                    metrics.get("truncated"), metrics.get("dropped"), metrics.get("page_full"),
                    status, error, started_at, completed_at,
                ),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint URL (the whole body
    becomes result.output; no API key, no {data} wrapper). A few delivery retries."""
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


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
        modal.Secret.from_name("dol-api"),      # exposes DOL_API_KEY
    ],
    timeout=60 * 15,            # tiny job; generous ceiling for API latency + indexing
    memory=4096,
    cpu=2.0,
)
def osha_sniper(
    trigger_callback_url: str | None = None,
    lookback_days: int = 7,
    site_states: list[str] | None = None,
    severe_only: bool = True,
    max_api_calls: int = MAX_API_CALLS,
    mode: str = "merge",
) -> dict:
    """Violations-first quota-safe DOL pull → DuckDB frontier-trim + join/normalize →
    Lance upsert, then record state + wake Trigger.

    Args:
      lookback_days: trailing window measured back from the data frontier
                     (max issuance_date in the pull), NOT wall-clock today.
      site_states:   optional high-value-state scope applied to the joined site_state.
      severe_only:   gate to severe viol_types (exclude Other-than-Serious).
      max_api_calls: per-run circuit-breaker ceiling (default MAX_API_CALLS); the per-day
                     guard caps it further so cumulative daily calls stay within the DOL
                     plan (DAILY_DOL_CALL_BUDGET).
      mode:          "merge" (dedup upsert on trigger_uid) | "overwrite" (rebuild).

    Terminal behaviour (success AND failure): write ops.osha_sniper_runs and POST
    {status, rows, feed, ...} to trigger_callback_url; on failure also re-raise so
    the Modal call is marked failed.
    """
    import datetime as dt

    import duckdb
    import lance  # noqa: F401 — imported for parity / fail-fast if image is wrong

    started_at = dt.datetime.now(dt.timezone.utc)
    snapshot_iso = started_at.date().isoformat()
    viol_path = os.path.join(SCRATCH_DIR, "osha_violation.csv")

    # Per-day guard: cap this run so cumulative ledgered DOL calls today stay under the
    # plan. Fail-open (day_used=0) on any ledger error → degrades to the per-run breaker.
    day_used = _daily_calls_used(snapshot_iso)
    effective_budget = min(max_api_calls, max(0, DAILY_DOL_CALL_BUDGET - day_used))
    if effective_budget < max_api_calls:
        print(f"Per-day guard: {day_used}/{DAILY_DOL_CALL_BUDGET} DOL calls already used "
              f"today; capping this run at {effective_budget} (per-run ceiling {max_api_calls}).")
    budget = _CallBudget(effective_budget)
    metrics: dict = {
        "snapshot_date": snapshot_iso, "lookback_days": lookback_days,
        "frontier_date": None, "api_calls_used": 0, "violations_pulled": 0,
        "activity_nrs": 0, "inspections_pulled": 0, "rows_upserted": 0,
        "truncated": False, "page_full": False, "dropped": 0,
    }
    status, error = "error", None

    try:
        if budget.max_calls <= 0:
            raise RuntimeError(
                f"Per-day DOL budget exhausted: {day_used}/{DAILY_DOL_CALL_BUDGET} calls "
                f"already charged today; deferring this run (idempotent — the window "
                f"resumes on the next dispatch)."
            )
        api_key = os.environ.get("DOL_API_KEY")
        if not api_key:
            raise RuntimeError("DOL_API_KEY not set (Modal secret 'dol-api').")
        so = _r2_storage_options()

        # ── Step 1 — Call A: probe the true data frontier (1 row, issuance_date DESC).
        #    Call B: pull EXACTLY the frontier-anchored window via a server-side
        #    issuance_date filter. No full-page scan, so violations_pulled is the true
        #    in-window count and a full page (page_full) means the window itself
        #    overflows ROW_LIMIT — the real clip signal, not the old pre-trim artifact. ──
        frontier = _probe_frontier(api_key, severe_only, budget)
        if not frontier:
            print("Step 1A: severe violation feed is empty — nothing to materialize.")
            metrics["rows_upserted"] = 0
            status = "success"
            raise _Done()
        metrics["frontier_date"] = frontier
        try:
            frontier_date = dt.date.fromisoformat(frontier)
        except ValueError as exc:
            raise RuntimeError(
                f"DOL frontier date {frontier!r} is not ISO YYYY-MM-DD — the violation "
                f"feed's date format may have changed; refusing to compute a bogus window."
            ) from exc
        cutoff_iso = (frontier_date - dt.timedelta(days=lookback_days)).isoformat()

        v_params: dict = {
            "limit": ROW_LIMIT, "offset": 0, "sort": "desc", "sort_by": VIOL_DATE_FIELD,
            "X-API-KEY": api_key,
        }
        fo = _filter_object_window(severe_only, cutoff_iso)   # severe `in` AND issuance_date `gt` cutoff
        if fo:
            v_params["filter_object"] = fo
        if not os.environ.get("OSHA_REQUEST_ALL_FIELDS"):
            v_params["fields"] = ",".join(VIOLATION_FIELDS)
        with open(viol_path, "w", encoding="utf-8") as fh:
            fh.write(_dol_csv_get(VIOLATION_ENDPOINT, v_params, budget))

        if not _nonempty_csv(viol_path):
            # Frontier row exists but the windowed pull is empty — only reachable on a
            # frontier shift between calls A and B. Clean no-op; self-corrects next run.
            print("Step 1B: windowed severe pull returned no rows — nothing to materialize.")
            metrics["rows_upserted"] = 0
            status = "success"
            raise _Done()

        # Recency-ordered DISTINCT activity_nrs (control-flow list). The pull is already
        # server-side windowed, so the SQL's frontier-trim is a defensive confirmation.
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            metrics["violations_pulled"] = int(con.execute(
                f"SELECT count(*) FROM read_csv('{viol_path}', all_varchar=true, "
                f"header=true, sample_size=-1, ignore_errors=true)"
            ).fetchone()[0] or 0)
            # A full page now means the IN-WINDOW severe set exceeds the DOL row cap — a
            # genuine overflow clipping the oldest in-window citations. Flag it loudly.
            if metrics["violations_pulled"] >= ROW_LIMIT:
                metrics["page_full"] = True
                metrics["truncated"] = True
                print(f"WARN: in-window severe violations hit ROW_LIMIT={ROW_LIMIT}; the "
                      f"{lookback_days}d window overflows one page — oldest in-window "
                      f"citations clipped. Tighten lookback_days or add offset pagination.")
            ids_tbl = con.execute(
                _recent_activity_nrs_sql(viol_path, severe_only, lookback_days)
            ).to_arrow_table()
        finally:
            con.close()
        activity_nrs = ids_tbl.column("activity_nr").to_pylist()
        metrics["activity_nrs"] = len(activity_nrs)
        print(f"Step 1: frontier issuance_date={frontier}; cutoff>{cutoff_iso}; "
              f"{metrics['violations_pulled']} in-window severe violations across "
              f"{len(activity_nrs)} distinct activity_nrs.")

        # ── Step 2 — parent inspections for those activity_nrs, batched under the URL
        #    budget and remaining call budget. No silent cap: log any dropped ids. ──
        insp_paths: list[str] = []
        dropped = 0
        if activity_nrs:
            base_params = {"limit": ROW_LIMIT, "X-API-KEY": api_key}
            if not os.environ.get("OSHA_REQUEST_ALL_FIELDS"):
                base_params["fields"] = ",".join(INSPECTION_FIELDS)
            batches, dropped = _pack_activity_batches(
                activity_nrs, budget.remaining, base_params)
            metrics["dropped"] = dropped
            if dropped:
                metrics["truncated"] = True   # never clear a page_full-set truncation
                print(f"WARN: {dropped}/{len(activity_nrs)} activity_nrs dropped — "
                      f"{budget.remaining} call(s) × query-string budget cannot cover all this run "
                      f"(oldest dropped first; they resurface as the window rolls).")
            for idx, batch in enumerate(batches):
                import json

                ip = os.path.join(SCRATCH_DIR, f"osha_inspection_{idx}.csv")
                i_params = dict(
                    base_params,
                    filter_object=json.dumps(
                        {"field": INSP_ACTIVITY_FIELD, "operator": "in", "value": batch},
                        separators=(",", ":"),
                    ),
                )
                with open(ip, "w", encoding="utf-8") as fh:
                    fh.write(_dol_csv_get(INSPECTION_ENDPOINT, i_params, budget))
                if _nonempty_csv(ip):
                    insp_paths.append(ip)

        # ── Transform — DuckDB frontier-trim + join + normalize + dedup → Arrow ───
        rows = 0
        if insp_paths:
            con = duckdb.connect(":memory:")
            try:
                con.execute("PRAGMA threads=4;")
                insp_list_sql = "[" + ", ".join(
                    "'" + p.replace("'", "''") + "'" for p in insp_paths) + "]"
                metrics["inspections_pulled"] = con.execute(
                    f"SELECT count(*) FROM read_csv({insp_list_sql}, all_varchar=true, "
                    f"header=true, sample_size=-1, ignore_errors=true, union_by_name=true)"
                ).fetchone()[0]
                arrow_table = con.execute(
                    _build_transform_sql(viol_path, insp_paths, snapshot_iso,
                                         severe_only, lookback_days, site_states)
                ).to_arrow_table()
            finally:
                con.close()
            rows, action = _materialize(arrow_table, so, mode)
            print(f"Materialized {rows} trigger rows ({action}) → {LANCE_URI}")
            if rows:
                _build_indexes(so)
        else:
            print("No parent inspections resolved — nothing to materialize.")

        metrics["rows_upserted"] = int(rows)
        status = "success"
    except _Done:
        pass
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        metrics["api_calls_used"] = budget.used
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(metrics, status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {
                "status": status,
                "rows": int(metrics["rows_upserted"]),
                "feed": FEED,
                "api_calls_used": budget.used,
                "truncated": bool(metrics["truncated"]),
                "dropped": int(metrics.get("dropped") or 0),
                "page_full": bool(metrics.get("page_full")),
                "in_window": int(metrics.get("violations_pulled") or 0),
            },
        )

    if status != "success":
        raise RuntimeError(f"osha_sniper failed: {error}")

    return {"feed": FEED, "status": status, **metrics}


@app.local_entrypoint()
def main(
    lookback_days: int = 7,
    severe_only: bool = True,
    mode: str = "merge",
) -> None:
    # Manual run: no callback URL (callback skipped); ops.* write still fires if the
    # hqx-postgres secret is attached. Requires the dol-api secret for DOL_API_KEY.
    print(osha_sniper.remote(
        trigger_callback_url=None,
        lookback_days=lookback_days,
        severe_only=severe_only,
        mode=mode,
    ))
