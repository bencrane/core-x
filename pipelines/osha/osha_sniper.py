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

Data plane (clean-room — no Iceberg, no Polaris, Arrow-only interchange):
    DOL API /v4/get/OSHA/inspection/csv   (Step 1: open_date in the last N days)
    DOL API /v4/get/OSHA/violation/csv    (Step 2: activity_nr IN <Step-1 ids>)
      → requests (-G/--data-urlencode semantics)  → /tmp CSVs   (Python: I/O only)
      → DuckDB read_csv → join + normalize + shape               (100% in SQL)
      → Arrow table
      → lance.merge_insert(trigger_uid)  →  s3://data-sink/active/osha_daily_triggers/

The two-step "delta" pull, the breaker, and the dedup key are the load-bearing
design (see the block comments inline):
  • Step 1 — one call: recent inspections (estab + site + naics), sorted newest-first.
  • Step 2 — up to (MAX_API_CALLS-1) calls: violations for those activity_nrs,
    batched so each request URL stays under the gateway length limit.
  • Dedup — synthetic ``trigger_uid = activity_nr || '-' || citation_id`` is the
    primary key; daily 7-day windows overlap, so we UPSERT (merge_insert), never
    append blindly. No (activity_nr, citation_id) pair is ever duplicated.

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

# ── DOL Open Data API surface (apiprod.dol.gov v4) ─────────────────────────────
# Endpoint template:  <base>/<agency>/<endpoint>/<format>?<params>
# Auth:  X-API-KEY header (kept OUT of the query string so it never lands in logs).
# Agency abbreviations are uppercase in the catalog (MSHA, ILAB, WB → OSHA). All
# four tokens below are env-overridable so a catalog rename is a one-line fix.
DOL_API_BASE = os.environ.get("DOL_API_BASE", "https://apiprod.dol.gov/v4/get")
OSHA_AGENCY = os.environ.get("OSHA_AGENCY", "OSHA")
INSPECTION_ENDPOINT = os.environ.get("OSHA_INSPECTION_ENDPOINT", "inspection")
VIOLATION_ENDPOINT = os.environ.get("OSHA_VIOLATION_ENDPOINT", "violation")

# DOL filter_object field names (Step 1 recency + optional state scope; Step 2 join).
INSP_DATE_FIELD = "open_date"
INSP_STATE_FIELD = "site_state"
VIOL_ACTIVITY_FIELD = "activity_nr"

# Column projections requested via the `fields` param — lean payloads, predictable
# schema. Exactly the columns the join needs (names per the DOL OSHA recon). Set
# OSHA_REQUEST_ALL_FIELDS=1 to drop `fields` and request every column (escape hatch).
INSPECTION_FIELDS = [
    "activity_nr", "estab_name", "site_address", "site_city",
    "site_state", "site_zip", "naics_code", "open_date",
]
VIOLATION_FIELDS = [
    "activity_nr", "citation_id", "standard", "viol_type",
    "gravity", "current_penalty", "issuance_date",
]

# ── Quota governor — the hard circuit breaker (directive: MAX_API_CALLS = 5) ────
# Every DOL HTTP call is charged against this budget BEFORE it leaves the process.
# The 6th attempt aborts instantly, protecting the ~20-call/day usage plan.
MAX_API_CALLS = 5

# DOL per-request hard cap (10k rows OR 5 MB, whichever first). 7-day nationwide
# OSHA inspections sit comfortably under 10k, so Step 1 is a single call.
ROW_LIMIT = 10000

# Conservative URL-length budget for the Step-2 activity_nr IN-list (apiprod runs
# behind a gateway; keep the encoded GET URL well under the common ~8 KB ceiling).
MAX_URL_BYTES = 7000

# "Severe" = everything except Other-than-Serious ('O'). Serious / Willful / Repeat
# / Failure-to-abate / Unclassified all signal a real safety problem worth mailing.
# Tunable via severe_only=False (keep all types) on the worker call.
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
    api_calls_used     int,
    inspections_pulled bigint,
    activity_nrs       int,
    violations_pulled  bigint,
    rows_upserted      bigint,
    truncated          boolean,
    status             text        NOT NULL,
    error              text,
    started_at         timestamptz,
    completed_at       timestamptz,
    recorded_at        timestamptz NOT NULL DEFAULT now()
);
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
)

app = modal.App("osha-pipelines", image=image)


# ── Name normalization — the standard regex protocol (verbatim from ────────────
#    pipelines/sos_normalized/normalize.py). UPPER → strip every non-[A-Z0-9 space]
#    char (punctuation, &, accents, AND entity suffixes' separators) → collapse
#    whitespace → trim. NULL if emptied. Produces the normalized_legal_name bridge.
def _name_norm(col: str) -> str:
    return ("nullif(trim(regexp_replace(regexp_replace(upper(CAST(%s AS VARCHAR)),"
            " '[^A-Z0-9 ]+', '', 'g'), '\\s+', ' ', 'g')), '')") % col


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


def _dol_csv_get(endpoint: str, params: dict, budget: _CallBudget, api_key: str) -> str:
    """One DOL CSV GET with exact `-G --data-urlencode` semantics (requests `params`
    URL-encodes each field; GET puts them on the query string). Charges the breaker
    first. X-API-KEY rides as a header, never in the URL."""
    import requests

    n = budget.charge()
    url = f"{DOL_API_BASE}/{OSHA_AGENCY}/{endpoint}/csv"
    headers = {"X-API-KEY": api_key, "Accept": "text/csv, */*"}
    redacted = {k: v for k, v in params.items()}
    fo = redacted.get("filter_object", "")
    if isinstance(fo, str) and len(fo) > 120:
        redacted["filter_object"] = fo[:120] + f"…(+{len(fo) - 120}b)"
    print(f"DOL call #{n}/{budget.max_calls} → {endpoint} :: {redacted}")
    resp = requests.get(url, params=params, headers=headers, timeout=(15, 180))
    resp.raise_for_status()
    return resp.text


def _filter_object_inspections(cutoff_iso: str, site_states: list[str] | None) -> str:
    """filter_object for Step 1: open_date `gt` cutoff, optionally AND site_state `in`.
    (DOL supports eq/neq/gt/lt/in/not_in/like; `in` takes a JSON array.)"""
    import json

    recency = {"field": INSP_DATE_FIELD, "operator": "gt", "value": cutoff_iso}
    if site_states:
        obj: dict = {"and": [
            recency,
            {"field": INSP_STATE_FIELD, "operator": "in", "value": site_states},
        ]}
    else:
        obj = recency
    return json.dumps(obj, separators=(",", ":"))


def _pack_activity_batches(
    activity_nrs: list[str], remaining_calls: int, base_params: dict
) -> tuple[list[list[str]], int]:
    """Greedily pack activity_nrs into Step-2 batches whose encoded violation-request
    URL stays under MAX_URL_BYTES, capped at ``remaining_calls`` batches. Returns
    (batches, dropped_count). Inputs are recency-ordered, so a truncation drops the
    OLDEST ids first — and we surface the drop rather than silently capping."""
    import json
    import urllib.parse

    prefix = f"{DOL_API_BASE}/{OSHA_AGENCY}/{VIOLATION_ENDPOINT}/csv?"
    batches: list[list[str]] = []
    i, n = 0, len(activity_nrs)
    while i < n and len(batches) < remaining_calls:
        batch: list[str] = []
        while i < n:
            fo = json.dumps(
                {"field": VIOL_ACTIVITY_FIELD, "operator": "in", "value": batch + [activity_nrs[i]]},
                separators=(",", ":"),
            )
            qlen = len(prefix) + len(urllib.parse.urlencode(dict(base_params, filter_object=fo)))
            if qlen > MAX_URL_BYTES and batch:
                break               # close batch; this id starts the next one
            batch.append(activity_nrs[i])
            i += 1
            if qlen > MAX_URL_BYTES:
                break               # single id already at budget — take it alone
        batches.append(batch)
    dropped = n - sum(len(b) for b in batches)
    return batches, dropped


def _build_transform_sql(insp_path: str, viol_paths: list[str], snapshot_iso: str,
                         severe_only: bool) -> str:
    """DuckDB does 100% of the transform: read both CSVs (all_varchar + TRY_CAST),
    inner-join on activity_nr, normalize estab_name → normalized_legal_name, derive
    the trigger_uid PK, optionally gate to severe viol_types, and de-dup the pair
    within the batch. Controlled /tmp paths only — no injection surface."""
    viol_list = "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in viol_paths) + "]"
    severe_pred = ""
    if severe_only:
        in_list = ", ".join("'" + t + "'" for t in SEVERE_VIOL_TYPES)
        severe_pred = f"    WHERE upper(trim(viol_type)) IN ({in_list})\n"
    return f"""
WITH insp AS (
    SELECT
        nullif(trim(activity_nr), '')   AS activity_nr,
        nullif(trim(estab_name), '')    AS estab_name,
        nullif(trim(site_address), '')  AS site_address,
        nullif(trim(site_city), '')     AS site_city,
        nullif(upper(trim(site_state)), '') AS site_state,
        nullif(left(regexp_replace(CAST(site_zip AS VARCHAR), '[^0-9]', '', 'g'), 5), '') AS site_zip,
        nullif(trim(naics_code), '')    AS naics_code,
        TRY_CAST(open_date AS DATE)     AS open_date
    FROM read_csv('{insp_path}', all_varchar = true, header = true,
                  sample_size = -1, ignore_errors = true)
),
viol AS (
    SELECT
        nullif(trim(activity_nr), '')       AS activity_nr,
        nullif(trim(citation_id), '')       AS citation_id,
        nullif(trim(standard), '')          AS standard,
        nullif(upper(trim(viol_type)), '')  AS viol_type,
        TRY_CAST(gravity AS INTEGER)        AS gravity,
        TRY_CAST(current_penalty AS DOUBLE) AS current_penalty,
        TRY_CAST(issuance_date AS DATE)     AS issuance_date
    FROM read_csv({viol_list}, all_varchar = true, header = true,
                  sample_size = -1, ignore_errors = true, union_by_name = true)
{severe_pred})
SELECT
    i.activity_nr || '-' || v.citation_id AS trigger_uid,
    i.activity_nr,
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
WHERE v.citation_id IS NOT NULL AND i.activity_nr IS NOT NULL
QUALIFY row_number() OVER (
    PARTITION BY i.activity_nr || '-' || v.citation_id
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
                    (feed, snapshot_date, lookback_days, api_calls_used,
                     inspections_pulled, activity_nrs, violations_pulled,
                     rows_upserted, truncated, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    FEED, metrics.get("snapshot_date"), metrics.get("lookback_days"),
                    metrics.get("api_calls_used"), metrics.get("inspections_pulled"),
                    metrics.get("activity_nrs"), metrics.get("violations_pulled"),
                    metrics.get("rows_upserted"), metrics.get("truncated"),
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
    """Two-step quota-safe DOL pull → DuckDB join/normalize → Lance upsert, then
    record state + wake Trigger.

    Args:
      lookback_days: Step-1 window — inspections with open_date in the last N days.
      site_states:   optional high-value-state scope (e.g. ["TX","CA"]); None = nationwide.
      severe_only:   keep only severe viol_types (exclude Other-than-Serious).
      max_api_calls: hard circuit-breaker ceiling (default MAX_API_CALLS = 5).
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
    cutoff_iso = (started_at.date() - dt.timedelta(days=lookback_days)).isoformat()
    insp_path = os.path.join(SCRATCH_DIR, "osha_inspection.csv")

    budget = _CallBudget(max_api_calls)
    metrics: dict = {
        "snapshot_date": snapshot_iso, "lookback_days": lookback_days,
        "api_calls_used": 0, "inspections_pulled": 0, "activity_nrs": 0,
        "violations_pulled": 0, "rows_upserted": 0, "truncated": False,
    }
    status, error = "error", None

    try:
        api_key = os.environ.get("DOL_API_KEY")
        if not api_key:
            raise RuntimeError("DOL_API_KEY not set (Modal secret 'dol-api').")
        so = _r2_storage_options()

        # ── Step 1 — recent inspections (one call). Newest-first so any Step-2
        #    truncation drops the oldest ids. fields= keeps the payload lean. ──────
        insp_params: dict = {
            "limit": ROW_LIMIT, "offset": 0, "sort": "desc", "sort_by": INSP_DATE_FIELD,
            "filter_object": _filter_object_inspections(cutoff_iso, site_states),
        }
        if not os.environ.get("OSHA_REQUEST_ALL_FIELDS"):
            insp_params["fields"] = ",".join(INSPECTION_FIELDS)
        insp_csv = _dol_csv_get(INSPECTION_ENDPOINT, insp_params, budget, api_key)
        with open(insp_path, "w", encoding="utf-8") as fh:
            fh.write(insp_csv)

        # Pull the recency-ordered distinct activity_nrs (control-flow list to drive
        # Step 2 — NOT the write-path interchange; DuckDB still does the transform).
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            ids_tbl = con.execute(
                f"""
                SELECT activity_nr FROM (
                    SELECT nullif(trim(activity_nr), '') AS activity_nr,
                           max(TRY_CAST({INSP_DATE_FIELD} AS DATE)) AS od
                    FROM read_csv('{insp_path}', all_varchar = true, header = true,
                                  sample_size = -1, ignore_errors = true)
                    GROUP BY 1
                )
                WHERE activity_nr IS NOT NULL
                ORDER BY od DESC NULLS LAST
                """
            ).to_arrow_table()
        finally:
            con.close()
        activity_nrs = ids_tbl.column("activity_nr").to_pylist()
        metrics["inspections_pulled"] = len(activity_nrs)
        if len(activity_nrs) >= ROW_LIMIT:
            print(f"WARN: Step 1 hit the {ROW_LIMIT}-row cap — inspections truncated; "
                  f"narrow the window or scope site_states.")
        print(f"Step 1: {len(activity_nrs)} distinct activity_nrs (since {cutoff_iso}).")

        # ── Step 2 — violations for those activity_nrs, batched under the URL budget
        #    and the remaining call budget. No silent cap: log any dropped ids. ────
        viol_paths: list[str] = []
        dropped = 0
        if activity_nrs:
            base_params = {"limit": ROW_LIMIT}
            if not os.environ.get("OSHA_REQUEST_ALL_FIELDS"):
                base_params["fields"] = ",".join(VIOLATION_FIELDS)
            batches, dropped = _pack_activity_batches(activity_nrs, budget.remaining, base_params)
            metrics["truncated"] = dropped > 0
            if dropped:
                print(f"WARN: {dropped}/{len(activity_nrs)} activity_nrs dropped — "
                      f"{budget.remaining} call(s) × URL budget cannot cover all this run "
                      f"(oldest dropped first; they resurface as the window rolls).")
            for idx, batch in enumerate(batches):
                import json

                vp = os.path.join(SCRATCH_DIR, f"osha_violation_{idx}.csv")
                v_params = dict(
                    base_params,
                    filter_object=json.dumps(
                        {"field": VIOL_ACTIVITY_FIELD, "operator": "in", "value": batch},
                        separators=(",", ":"),
                    ),
                )
                v_csv = _dol_csv_get(VIOLATION_ENDPOINT, v_params, budget, api_key)
                with open(vp, "w", encoding="utf-8") as fh:
                    fh.write(v_csv)
                viol_paths.append(vp)

        # ── Transform — DuckDB join + normalize + dedup → Arrow (zero-copy) ───────
        rows = 0
        if viol_paths:
            con = duckdb.connect(":memory:")
            try:
                con.execute("PRAGMA threads=4;")
                arrow_table = con.execute(
                    _build_transform_sql(insp_path, viol_paths, snapshot_iso, severe_only)
                ).to_arrow_table()
            finally:
                con.close()
            metrics["violations_pulled"] = arrow_table.num_rows
            rows, action = _materialize(arrow_table, so, mode)
            print(f"Materialized {rows} trigger rows ({action}) → {LANCE_URI}")
            if rows:
                _build_indexes(so)
        else:
            print("No activity_nrs / no violations this run — nothing to materialize.")

        metrics["rows_upserted"] = int(rows)
        status = "success"
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
