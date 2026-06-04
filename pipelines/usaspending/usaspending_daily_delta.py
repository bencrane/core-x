"""Compute worker — USAspending award_search DAILY DELTA (merge_insert freshness).

The ``usaspending-daily-delta`` Modal app. Closes the ~6-week staleness gap between
the monthly 161 GiB bulk dump (snapshot ``2026-05-06``, ingested by
``usaspending_bulk.py``) and "today" by UPSERTing fresh awards directly into the
award-grain system of record:

    s3://data-sink/active/usaspending/award_search/   (Lance, 78.4M rows, 1/award)

merge key ``generated_unique_award_id`` (the API's ``generated_internal_id``,
``CONT_AWD_…``). Spawned only by the Universal Dispatcher (core/modal_dispatcher.py);
no web endpoint. Per Directive 34 / DIRECTIVE_33_USASPENDING_DAILY_DELTA_PORT.md.

TWO MODES, selected from the ops watermark (``max(feed_date) WHERE status='success'``):
  • COLD-START  (ledger empty) — wide ``last_modified_date`` window
    [SNAPSHOT_DATE(2026-05-06) → yesterday] via the async ``POST /bulk_download/awards/``
    server-side CSV job (handles arbitrary volume; the transaction-grain
    ``Contracts_PrimeTransactions`` file is collapsed to award grain on
    ``max(last_modified_date)`` before merge). Closes the whole gap in one run.
  • STEADY-STATE (ledger has a success) — window [last_success+1 → yesterday]
    (normally one day) via ``POST /search/spending_by_award/``, paginated limit=100
    on the ``hasNext`` loop up to MAX_API_CALLS (1000-call safety ceiling).

WHY ``last_modified_date`` (not ``action_date``): USAspending lags 7+ days between a
contract action and warehouse landing, so an ``action_date`` daily window returns
≈0 rows (verified Gen-2 rationale). The delta window is therefore on the warehouse
modification stamp.

ANTI-CORRUPTION (THE load-bearing guarantee, verified on lance 7.0.0): the incoming
Arrow batch carries ONLY the merge key + the columns the API authoritatively
supplies. ``merge_insert(...).when_matched_update_all()`` updates ONLY the columns
PRESENT IN THE SOURCE and leaves every other (bulk-only) column of a matched award
INTACT — it does NOT null them. (Empirically: a column-subset source preserves
non-source columns; an *unmapped* source column is rejected outright, so a bad
projection fails loudly rather than corrupting.) The batch is cast to the live
``award_search`` field types at runtime, so the merge schema matches by construction.
New awards (in the delta, absent from bulk) insert with the API columns set and
bulk-only columns NULL — the gap-closing path.

F5 BotDefense (must port): USAspending throttles by source IP and in-script long
backoff does not recover — a fresh egress IP does. A persistent 429 FAILS FAST out
of the fetch phase so the ``modal.Retries`` policy recycles the container (= a fresh
IP). Transient 5xx get a couple short in-script retries; a single 429 honors
Retry-After once before failing fast.

Data plane (architecture reality — Parquet=transport, DuckDB=compute, Lance=SoR):
  API → verbatim ZSTD Parquet @ s3://dex-raw-landing-zone/usaspending/award_search/
        api-delta/date=YYYY-MM-DD/ (audit, BEFORE compute) → DuckDB project/cast +
        award-grain dedup → Lance merge_insert(award_search) → _optimize_indices.

Control plane (Trigger v4 durable callback): on terminal state writes the run row to
ops.usaspending_award_search_delta_runs (also the watermark) and POSTs the flat
callback to wake the Trigger run.

    modal run    pipelines/usaspending/usaspending_daily_delta.py::init_ops
    modal deploy pipelines/usaspending/usaspending_daily_delta.py
    modal run    pipelines/usaspending/usaspending_daily_delta.py::run            # auto window
    modal run    pipelines/usaspending/usaspending_daily_delta.py::run --dry-run  # fetch+count, no merge
    modal run    pipelines/usaspending/usaspending_daily_delta.py::run --mode steady_state \
                 --window-start 2026-05-20 --window-end 2026-05-20                # explicit window
"""

from __future__ import annotations

import os

import modal

# ─────────────────────────── constants ───────────────────────────

BUCKET = "data-sink"
AWARD_SEARCH_URI = os.environ.get(
    "USASPENDING_AWARD_SEARCH_URI",
    "s3://data-sink/active/usaspending/award_search/",
)
MERGE_KEY = "generated_unique_award_id"
FEED = "usaspending_award_search_delta"

# Bulk snapshot frontier — the cold-start window starts here (one-day overlap with
# the bulk is harmless: merge_insert is idempotent). Kept in sync with usaspending_bulk.py.
SNAPSHOT_DATE = "2026-05-06"

# Verbatim raw transport landing (audit, BEFORE compute) — the dex-raw-landing-zone.
RAW_BUCKET = os.environ.get("USASPENDING_DELTA_RAW_BUCKET", "dex-raw-landing-zone")
RAW_PREFIX = os.environ.get(
    "USASPENDING_DELTA_RAW_PREFIX", "usaspending/award_search/api-delta"
).strip("/")

# USAspending public API (no key; rate-limited by source IP — see F5 note).
SBA_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
BULK_DL_URL = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
PRIME_CONTRACT_TYPES = ["A", "B", "C", "D"]   # definitive / purchase order / delivery / BPA-call
DATE_TYPE = "last_modified_date"
PAGE_LIMIT = 100                              # USAspending hard page cap
MAX_API_CALLS = 1000                          # steady-state safety ceiling (≤100k awards/run)
BULK_POLL_SECONDS = 15                         # async job poll interval
BULK_POLL_CEILING_SECONDS = 60 * 60           # 60 min hard ceiling on the async job

DATA_STORAGE_VERSION = "2.1"
SCRATCH_DIR = "/tmp"

# ── Anti-corruption merge target set ────────────────────────────────────────
# Every column below is one the API authoritatively supplies → safe to update.
# Bulk-only columns (the other ~260 of rpt.award_search) are NEVER in the batch →
# preserved verbatim on matched awards. The obligated figure maps to BOTH
# award_amount and total_obligation: for prime contracts USAspending's matview sets
# them equal (the obligated dollars), and downstream consumers read total_obligation
# (contractor_award_summary) while the GovCon triggers read award_amount via
# GREATEST(award_amount, base_and_all_options_value, total_obligation) — updating both
# keeps every consumer fresh without touching the ceiling columns.
#
# Steady-state (spending_by_award JSON) "fields" — display names. The API also
# auto-returns generated_internal_id / internal_id regardless of this list.
SBA_FIELDS = [
    "Award ID", "Recipient Name", "Recipient UEI", "Award Amount", "Total Outlays",
    "Description", "Contract Award Type", "Awarding Agency", "Awarding Sub Agency",
    "Funding Agency", "Funding Sub Agency", "Start Date", "End Date",
    "Last Modified Date", "Base Obligation Date", "NAICS", "PSC",
]

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",          # provides `import lance`
    "pyarrow>=17",
    "requests>=2.32",      # USAspending API + Trigger callback
    "boto3>=1.35",         # R2 raw-landing put
    "psycopg[binary]>=3.2",  # ops.* watermark + terminal state
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("usaspending-daily-delta", image=image)


class USASpendingDataLagException(RuntimeError):
    """A delta window returned 0 award rows (Directive 34b — lag tolerance).

    Federal award activity is never empty for a real window, so 0 rows means
    USAspending's warehouse has not yet landed this window's ``last_modified_date``
    modifications — a processing lag, NOT a clean sync. Treating it as success and
    advancing the watermark would PERMANENTLY skip the awards that land later under
    that same date stamp (the window would have already moved past it). Raising this
    FREEZES the watermark: the run is recorded ``stalled`` (not ``success``), so
    ``max(feed_date) WHERE status='success'`` is unchanged and the next cron fire
    re-attempts the identical window until the backlog clears and rows return."""


# ─────────────────────────── R2 / S3 plumbing ───────────────────────────

def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2 (Modal secret r2-credentials)."""
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


def _s3_client():
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required`` —
    botocore's default flexible-checksum validation does not match R2."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001
        return False


def _optimize_indices(uri: str, so: dict) -> None:
    """Fold the merge_insert's new fragments into the existing award_search BTREEs."""
    import lance
    try:
        lance.dataset(uri, storage_options=so).optimize.optimize_indices()
        print("  index optimize ✓")
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: optimize_indices failed ({exc}); index still covers pre-merge rows.")


# ─────────────────────────── window / watermark ───────────────────────────

def _yesterday_utc():
    import datetime as dt
    return (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1))


def _watermark_last_success():
    """max(feed_date) of successful runs, or None when the ledger is empty/unreachable."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; cannot read watermark → cold-start.")
        return None
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT max(feed_date) FROM ops.usaspending_award_search_delta_runs "
                "WHERE status = 'success'"
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as exc:  # noqa: BLE001 — missing table on first run → cold-start
        print(f"WARN: watermark read failed ({exc}); treating as cold-start.")
        return None


def _resolve_window(mode, window_start, window_end):
    """Return (mode, window_start_date, window_end_date). Explicit args win; otherwise
    derive from the ops watermark — empty ledger ⇒ cold_start from SNAPSHOT_DATE."""
    import datetime as dt

    def _d(s):
        return dt.date.fromisoformat(s) if isinstance(s, str) else s

    we = _d(window_end) if window_end else _yesterday_utc()

    if window_start:
        ws = _d(window_start)
        resolved_mode = mode or ("cold_start" if (we - ws).days > 7 else "steady_state")
        return resolved_mode, ws, we

    last = _watermark_last_success()
    if last is None:
        return (mode or "cold_start"), _d(SNAPSHOT_DATE), we
    ws = last + dt.timedelta(days=1)
    # A normal day → steady-state pagination. A multi-day backlog (delta was down)
    # auto-promotes to the bulk_download path, which absorbs volume the paginated
    # 1000-call ceiling would truncate. Threshold mirrors the explicit-window branch.
    auto = "cold_start" if (we - ws).days > 7 else "steady_state"
    return (mode or auto), ws, we


# ─────────────────────────── steady-state fetch (spending_by_award) ───────────────────────────

class _ThrottledError(RuntimeError):
    """Persistent 429 — fail fast so modal.Retries recycles the container (fresh IP)."""


def _sba_post(payload: dict) -> dict:
    """POST spending_by_award. Honors one Retry-After on a 429, then FAILS FAST
    (raises _ThrottledError) so the orchestrator-level Modal retry gets a fresh IP.
    Transient 5xx / connection errors get two short in-script retries."""
    import time

    import requests

    seen_429 = False
    last = None
    for attempt in range(3):
        try:
            r = requests.post(SBA_URL, json=payload, timeout=(30, 300))
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                if not seen_429:
                    seen_429 = True
                    ra = r.headers.get("Retry-After")
                    time.sleep(min(int(ra), 60) if (ra and ra.isdigit()) else 5)
                    continue
                raise _ThrottledError("spending_by_award 429 after Retry-After → recycle container")
            if 500 <= r.status_code < 600:
                last = f"status {r.status_code}: {r.text[:200]}"
                time.sleep(5 * (attempt + 1))
                continue
            # 4xx other than 429 = bad request / invalid fields → non-retryable, surface.
            raise RuntimeError(f"spending_by_award {r.status_code}: {r.text[:300]}")
        except _ThrottledError:
            raise
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001 — connection/read error → short retry
            last = str(exc)
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"spending_by_award failed after retries: {last}")


def _fetch_steady_state(window_start, window_end):
    """Paginate spending_by_award over the last_modified_date window. Returns
    (results: list[dict], api_calls: int). Raises on persistent throttle (→ Modal retry)."""
    base = {
        "filters": {
            "award_type_codes": PRIME_CONTRACT_TYPES,
            "time_period": [{
                "start_date": window_start.isoformat(),
                "end_date": window_end.isoformat(),
                "date_type": DATE_TYPE,
            }],
        },
        "fields": SBA_FIELDS,
        "limit": PAGE_LIMIT,
        "sort": "Last Modified Date",
        "order": "desc",
    }
    results: list[dict] = []
    api_calls = 0
    page = 1
    while api_calls < MAX_API_CALLS:
        body = _sba_post({**base, "page": page})
        api_calls += 1
        batch = body.get("results", []) or []
        results.extend(batch)
        if not body.get("page_metadata", {}).get("hasNext"):
            break
        page += 1
    else:
        print(f"WARN: hit MAX_API_CALLS={MAX_API_CALLS} ceiling — window may be truncated; "
              f"next run's watermark will re-cover from this window_start.")
    print(f"steady_state fetched {len(results):,} award rows in {api_calls} API call(s)")
    return results, api_calls


def _num_str(v):
    """Stringify a numeric/None API value for the all-VARCHAR transport parquet."""
    return None if v is None else str(v)


def _flatten_sba(r: dict) -> dict:
    """Flatten one spending_by_award result → stable target-named string columns
    (NAICS/PSC structs expanded) + a verbatim ``raw_payload`` for full audit."""
    import json

    naics = r.get("NAICS") if isinstance(r.get("NAICS"), dict) else {}
    psc = r.get("PSC") if isinstance(r.get("PSC"), dict) else {}
    return {
        "generated_unique_award_id": r.get("generated_internal_id"),
        "piid": r.get("Award ID"),
        "recipient_name": r.get("Recipient Name"),
        "recipient_uei": r.get("Recipient UEI"),
        "obligated_amount": _num_str(r.get("Award Amount")),
        "total_outlays": _num_str(r.get("Total Outlays")),
        "description": r.get("Description"),
        "type_description": r.get("Contract Award Type"),
        "awarding_toptier_agency_name": r.get("Awarding Agency"),
        "awarding_subtier_agency_name": r.get("Awarding Sub Agency"),
        "funding_toptier_agency_name": r.get("Funding Agency"),
        "funding_subtier_agency_name": r.get("Funding Sub Agency"),
        "period_of_performance_start_date": r.get("Start Date"),
        "period_of_performance_current_end_date": r.get("End Date"),
        "last_modified_date": r.get("Last Modified Date"),
        "date_signed": r.get("Base Obligation Date"),
        "naics_code": naics.get("code"),
        "naics_description": naics.get("description"),
        "product_or_service_code": psc.get("code"),
        "product_or_service_description": psc.get("description"),
        "raw_payload": json.dumps(r, separators=(",", ":"), sort_keys=True, default=str),
    }


# steady-state: stringified transport column → typed award_search projection.
_STEADY_PROJECTION = """
    nullif(trim(generated_unique_award_id), '')              AS generated_unique_award_id,
    nullif(trim(piid), '')                                   AS piid,
    nullif(trim(recipient_name), '')                         AS recipient_name,
    nullif(trim(recipient_uei), '')                          AS recipient_uei,
    TRY_CAST(obligated_amount AS DOUBLE)                     AS award_amount,
    TRY_CAST(obligated_amount AS DOUBLE)                     AS total_obligation,
    TRY_CAST(total_outlays AS DOUBLE)                        AS total_outlays,
    nullif(trim(description), '')                            AS description,
    nullif(trim(type_description), '')                       AS type_description,
    nullif(trim(awarding_toptier_agency_name), '')          AS awarding_toptier_agency_name,
    nullif(trim(awarding_subtier_agency_name), '')          AS awarding_subtier_agency_name,
    nullif(trim(funding_toptier_agency_name), '')           AS funding_toptier_agency_name,
    nullif(trim(funding_subtier_agency_name), '')           AS funding_subtier_agency_name,
    TRY_CAST(period_of_performance_start_date AS DATE)       AS period_of_performance_start_date,
    TRY_CAST(period_of_performance_current_end_date AS DATE) AS period_of_performance_current_end_date,
    TRY_CAST(last_modified_date AS TIMESTAMP)               AS last_modified_date,
    TRY_CAST(date_signed AS DATE)                            AS date_signed,
    nullif(trim(naics_code), '')                            AS naics_code,
    nullif(trim(naics_description), '')                     AS naics_description,
    nullif(trim(product_or_service_code), '')              AS product_or_service_code,
    nullif(trim(product_or_service_description), '')       AS product_or_service_description
"""

# cold-start: bulk_download Contracts_PrimeTransactions CSV header → typed projection.
# (Transaction grain; collapsed to award grain downstream on max(last_modified_date).)
_COLD_PROJECTION = """
    nullif(trim(contract_award_unique_key), '')             AS generated_unique_award_id,
    nullif(trim(award_id_piid), '')                         AS piid,
    nullif(trim(recipient_name), '')                        AS recipient_name,
    nullif(trim(recipient_uei), '')                         AS recipient_uei,
    TRY_CAST(total_dollars_obligated AS DOUBLE)             AS award_amount,
    TRY_CAST(total_dollars_obligated AS DOUBLE)             AS total_obligation,
    nullif(trim(prime_award_base_transaction_description), '') AS description,
    nullif(trim(award_type), '')                            AS type_description,
    nullif(trim(awarding_agency_name), '')                  AS awarding_toptier_agency_name,
    nullif(trim(awarding_sub_agency_name), '')              AS awarding_subtier_agency_name,
    nullif(trim(funding_agency_name), '')                   AS funding_toptier_agency_name,
    nullif(trim(funding_sub_agency_name), '')               AS funding_subtier_agency_name,
    TRY_CAST(period_of_performance_start_date AS DATE)       AS period_of_performance_start_date,
    TRY_CAST(period_of_performance_current_end_date AS DATE) AS period_of_performance_current_end_date,
    TRY_CAST(last_modified_date AS TIMESTAMP)               AS last_modified_date,
    TRY_CAST(action_date AS DATE)                            AS action_date,
    nullif(trim(naics_code), '')                            AS naics_code,
    nullif(trim(naics_description), '')                     AS naics_description,
    nullif(trim(product_or_service_code), '')              AS product_or_service_code,
    nullif(trim(product_or_service_code_description), '')  AS product_or_service_description
"""


# ─────────────────────────── cold-start fetch (bulk_download/awards) ───────────────────────────

def _fetch_cold_start(window_start, window_end):
    """Async bulk_download/awards over the wide window. Returns (csv_glob, poll_count).
    The server-side CSV job handles arbitrary volume that would blow the steady-state
    pagination ceiling. Raises on a failed job or the 60-min poll ceiling."""
    import shutil
    import time
    import zipfile

    import requests

    payload = {
        "filters": {
            "prime_award_types": PRIME_CONTRACT_TYPES,
            "date_type": DATE_TYPE,
            "date_range": {
                "start_date": window_start.isoformat(),
                "end_date": window_end.isoformat(),
            },
        },
        "file_format": "csv",
    }
    resp = requests.post(BULK_DL_URL, json=payload, timeout=(30, 120))
    if resp.status_code == 429:
        raise _ThrottledError("bulk_download 429 → recycle container")
    if resp.status_code >= 300:
        raise RuntimeError(f"bulk_download submit {resp.status_code}: {resp.text[:300]}")
    job = resp.json()
    status_url = job["status_url"]
    file_url = job["file_url"]
    print(f"cold_start job submitted: {job.get('file_name')}")

    polls = 0
    deadline = time.time() + BULK_POLL_CEILING_SECONDS
    while time.time() < deadline:
        time.sleep(BULK_POLL_SECONDS)
        polls += 1
        try:
            st = requests.get(status_url, timeout=(30, 120)).json()
        except Exception as exc:  # noqa: BLE001 — transient poll error → keep polling
            print(f"  poll {polls}: transient ({exc})")
            continue
        status = st.get("status")
        print(f"  poll {polls}: status={status} rows={st.get('total_rows')}")
        if status == "finished":
            break
        if status == "failed":
            raise RuntimeError(f"bulk_download job failed: {st.get('message', '')[:300]}")
    else:
        raise RuntimeError(f"bulk_download job did not finish within "
                           f"{BULK_POLL_CEILING_SECONDS}s ({polls} polls)")

    work = os.path.join(SCRATCH_DIR, "cold_start")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work, exist_ok=True)
    zip_path = os.path.join(work, "awards.zip")
    with requests.get(file_url, stream=True, timeout=(30, 600)) as dl:
        dl.raise_for_status()
        with open(zip_path, "wb") as fh:
            for chunk in dl.iter_content(chunk_size=8 * 1024 * 1024):
                fh.write(chunk)
    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        zf.extractall(work, members=members)
    print(f"cold_start downloaded {len(members)} CSV member(s): {members}")
    return os.path.join(work, "*.csv"), polls


# ─────────────────────────── transform + merge ───────────────────────────

def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    os.makedirs(os.path.join(SCRATCH_DIR, "duckdb_spill"), exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{SCRATCH_DIR}/duckdb_spill';")
    con.execute("SET preserve_insertion_order=false;")
    return con


def _land_steady_raw(rows, feed_date, upload: bool = True) -> tuple[str, str | None]:
    """Write the verbatim flattened API rows as ZSTD Parquet locally and (unless this
    is a dry-run plan) upload to the raw-landing zone (audit, BEFORE compute). Returns
    (local_path, s3_uri|None)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    flat = [_flatten_sba(r) for r in rows]
    table = pa.Table.from_pylist(flat)
    local = os.path.join(SCRATCH_DIR, f"steady_{feed_date}.parquet")
    pq.write_table(table, local, compression="zstd")
    if not upload:
        return local, None
    key = f"{RAW_PREFIX}/date={feed_date}/spending_by_award.parquet"
    _s3_client().upload_file(local, RAW_BUCKET, key)
    uri = f"s3://{RAW_BUCKET}/{key}"
    print(f"raw audit landed: {uri} ({table.num_rows:,} rows)")
    return local, uri


def _land_cold_raw(con, csv_glob, feed_date, upload: bool = True) -> tuple[str, str | None]:
    """Read the bulk CSV(s) all-VARCHAR and re-emit as one verbatim ZSTD Parquet (all
    297 source columns) locally; (unless dry-run) upload to the raw-landing zone.
    Returns (local, s3_uri|None)."""
    local = os.path.join(SCRATCH_DIR, f"cold_{feed_date}.parquet")
    con.execute(f"""
        COPY (
            SELECT * FROM read_csv('{csv_glob}', all_varchar=true, header=true,
                                   sample_size=-1, ignore_errors=false)
        ) TO '{local}' (FORMAT parquet, COMPRESSION zstd)
    """)
    if not upload:
        return local, None
    key = f"{RAW_PREFIX}/date={feed_date}/bulk_download_awards.parquet"
    _s3_client().upload_file(local, RAW_BUCKET, key)
    uri = f"s3://{RAW_BUCKET}/{key}"
    print(f"raw audit landed: {uri}")
    return local, uri


def _build_delta_award_grain(con, source_parquet: str, projection: str) -> int:
    """Project the verbatim parquet → typed award_search columns, then collapse to
    ONE row per award on max(last_modified_date) (last-writer-wins). Creates TEMP
    TABLE ``delta_award_grain``; returns its row count."""
    con.execute(f"""
        CREATE TEMP TABLE delta_award_grain AS
        WITH proj AS (
            SELECT {projection}
            FROM read_parquet('{source_parquet}')
        )
        SELECT * FROM proj
        WHERE generated_unique_award_id IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY generated_unique_award_id
            ORDER BY last_modified_date DESC NULLS LAST
        ) = 1
    """)
    return con.execute("SELECT count(*) FROM delta_award_grain").fetchone()[0]


def _merge_delta(con, so) -> int:
    """Cast ``delta_award_grain`` to the LIVE award_search field types (subset) and
    column-scoped merge_insert on the merge key. Returns rows upserted.

    Casting to the target's own field types makes the append schema match by
    construction; an unmapped column would raise here (loud, not silent corruption).
    when_matched_update_all over a column-SUBSET source updates only those columns —
    bulk-only columns of matched awards are preserved (verified, lance 7.0.0)."""
    import lance
    import pyarrow as pa

    if not _dataset_exists(AWARD_SEARCH_URI, so):
        raise RuntimeError(
            f"award_search dataset absent at {AWARD_SEARCH_URI}; the daily delta merges "
            f"into the bulk SoR and must not create it. Run usaspending_bulk first.")

    delta = con.sql("SELECT * FROM delta_award_grain").to_arrow_table()
    ds = lance.dataset(AWARD_SEARCH_URI, storage_options=so)
    target_schema = ds.schema
    # Align every projected column to the committed award_search field (type + name).
    fields = [target_schema.field(name) for name in delta.column_names]  # KeyError ⇒ bad map
    delta = delta.cast(pa.schema(fields))

    (ds.merge_insert(MERGE_KEY)
       .when_matched_update_all()
       .when_not_matched_insert_all()
       .execute(delta))
    _optimize_indices(AWARD_SEARCH_URI, so)
    return delta.num_rows


# ─────────────────────────── ops ledger + callback ───────────────────────────

def _record_run(*, feed_date, window_start, window_end, run_mode, rows_upserted,
                api_calls, raw_landing_uri, status, error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.usaspending_award_search_delta_runs
                    (feed_date, window_start, window_end, run_mode, rows_upserted,
                     api_calls, raw_landing_uri, dataset_uri, status, error_message,
                     started_at, executed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (feed_date, window_start, window_end, run_mode, rows_upserted,
                 api_calls, raw_landing_uri, AWARD_SEARCH_URI, status,
                 (error or "")[:2000] or None, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the flat terminal payload to the Trigger waitpoint url — no API key, no
    {"data": …} envelope; the whole body becomes result.output."""
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


# ─────────────────────────── worker ───────────────────────────

def _run_delta(mode, window_start, window_end, trigger_callback_url, dry_run: bool) -> dict:
    """Shared core. FETCH is retryable (raises bare → modal.Retries recycles the
    container = fresh egress IP per the F5 lesson). TRANSFORM+MERGE is terminal:
    it records the ops row + posts the callback and does NOT re-raise, so a compute
    error (or a lag stall) fails the Trigger run once without triggering a full Modal
    re-run.

    LAG TOLERANCE (Directive 34b): a window that yields 0 award rows is NOT a clean
    sync — it is a federal-warehouse processing lag. It raises
    USASpendingDataLagException → recorded ``stalled`` (never ``success``) → the
    watermark is frozen → the next cron fire re-attempts the identical window."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    run_mode, ws, we = _resolve_window(mode, window_start, window_end)
    feed_date = we
    print(f"[{run_mode}] window last_modified_date ∈ [{ws} … {we}]  (feed_date={feed_date})")

    if ws > we:
        # Watermark already at/after yesterday — nothing to do (idempotent no-op day).
        print("watermark already current; no window to process.")
        if not dry_run:
            completed_at = dt.datetime.now(dt.timezone.utc)
            _record_run(feed_date=feed_date, window_start=ws, window_end=we, run_mode=run_mode,
                        rows_upserted=0, api_calls=0, raw_landing_uri=None, status="success",
                        error=None, started_at=started_at, completed_at=completed_at)
            _post_callback(trigger_callback_url, {
                "status": "success", "rows": 0, "feed": FEED, "dataset_uri": AWARD_SEARCH_URI,
                "run_mode": run_mode, "window_start": ws.isoformat(),
                "window_end": we.isoformat(), "api_calls": 0})
        return {"feed": FEED, "run_mode": run_mode, "rows_upserted": 0, "api_calls": 0,
                "status": "success", "window_start": ws.isoformat(), "window_end": we.isoformat()}

    # ── FETCH (retryable; no ops/callback here so a throttle just recycles) ──
    if run_mode == "cold_start":
        csv_glob, api_calls = _fetch_cold_start(ws, we)
        steady_rows = None
    else:
        steady_rows, api_calls = _fetch_steady_state(ws, we)
        csv_glob = None

    # ── TRANSFORM + MERGE (terminal) ──
    status, error, rows_upserted, raw_uri = "error", None, 0, None
    con = _new_con()
    try:
        if run_mode == "cold_start":
            _local, raw_uri = _land_cold_raw(con, csv_glob, feed_date, upload=not dry_run)
            grain = _build_delta_award_grain(con, _local, _COLD_PROJECTION)
        elif not steady_rows:
            grain, raw_uri = 0, None   # empty fetch → caught by the lag guard below
        else:
            _local, raw_uri = _land_steady_raw(steady_rows, feed_date, upload=not dry_run)
            grain = _build_delta_award_grain(con, _local, _STEADY_PROJECTION)

        print(f"award-grain delta rows: {grain:,}")

        # LAG TOLERANCE (Directive 34b). A 0-row window is a federal-warehouse lag,
        # never a real empty day. HALT — do not record success, do not advance the
        # watermark; the next cron fire re-attempts the identical window. (Raised here,
        # in the terminal phase, so it is recorded as a 'stalled' run rather than
        # re-fetched 5× by modal.Retries — the data is simply not in the warehouse yet.)
        if grain == 0:
            raise USASpendingDataLagException(
                "API returned 0 rows. Assuming federal warehouse lag. Halting watermark.")

        if not dry_run:
            so = _r2_storage_options()
            rows_upserted = _merge_delta(con, so)
            total = lance_count(AWARD_SEARCH_URI, so)
            print(f"merge_insert: {rows_upserted:,} awards upserted; "
                  f"award_search now {total:,} rows")
        else:
            print(f"[dry-run] would upsert {grain:,} award-grain rows (no merge)")
        status = "success"
    except USASpendingDataLagException as exc:
        # Terminal STALL — not a code error and not retryable within this run. Recorded
        # 'stalled' so the success-only watermark stays frozen; surfaced to Trigger as a
        # failure (visible). Not re-raised → no futile modal.Retries storm.
        error = str(exc)
        status = "stalled"
        print(f"STALL (watermark frozen): {error}")
    except Exception as exc:  # noqa: BLE001 — terminal compute error: callback + return
        error = f"{type(exc).__name__}: {exc}"
        status = "error"
    finally:
        con.close()
        completed_at = dt.datetime.now(dt.timezone.utc)
        if not dry_run:
            _record_run(feed_date=feed_date, window_start=ws, window_end=we, run_mode=run_mode,
                        rows_upserted=int(rows_upserted), api_calls=int(api_calls),
                        raw_landing_uri=raw_uri, status=status, error=error,
                        started_at=started_at, completed_at=completed_at)
            # Trigger callback is binary (success|error); a stall is a non-success the
            # cron must see. The 'stalled' flag distinguishes a lag halt from a real error.
            _post_callback(trigger_callback_url, {
                "status": "success" if status == "success" else "error",
                "rows": int(rows_upserted), "feed": FEED,
                "dataset_uri": AWARD_SEARCH_URI, "run_mode": run_mode,
                "window_start": ws.isoformat(), "window_end": we.isoformat(),
                "api_calls": int(api_calls),
                "stalled": status == "stalled",
                "message": error if status != "success" else None})

    return {"feed": FEED, "run_mode": run_mode, "rows_upserted": int(rows_upserted),
            "api_calls": int(api_calls), "status": status, "raw_landing_uri": raw_uri,
            "window_start": ws.isoformat(), "window_end": we.isoformat(), "error": error}


def lance_count(uri: str, so: dict) -> int:
    import lance
    return lance.dataset(uri, storage_options=so).count_rows()


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,
    memory=16384,
    cpu=4.0,
    # F5 BotDefense: a persistent 429 is raised out of the fetch phase so EACH retry
    # is a fresh Modal container = a fresh egress IP. (initial_delay 30s, ×2 backoff.)
    retries=modal.Retries(max_retries=5, backoff_coefficient=2.0, initial_delay=30.0),
)
def ingest_award_search_delta(
    mode: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
    trigger_callback_url: str | None = None,
) -> dict:
    """Daily-delta worker. Auto-selects cold-start vs steady-state from the ops
    watermark unless ``mode``/window are given explicitly."""
    return _run_delta(mode, window_start, window_end, trigger_callback_url, dry_run=False)


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120, memory=16384, cpu=4.0,
)
def plan_award_search_delta(
    mode: str | None = None, window_start: str | None = None, window_end: str | None = None,
) -> dict:
    """Remote review gate — resolve window + fetch + count award-grain delta, merge NOTHING."""
    return _run_delta(mode, window_start, window_end, trigger_callback_url=None, dry_run=True)


# ─────────────────────────── ops bootstrap + verify ───────────────────────────

@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_ops_ddl(sql: str) -> dict:
    """Execute the ops.* DDL (idempotent CREATE … IF NOT EXISTS) via psycopg."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    return {"applied": True}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60)
def watermark() -> dict:
    """Report the current watermark + the window the next run would process."""
    last = _watermark_last_success()
    mode, ws, we = _resolve_window(None, None, None)
    return {"last_success_feed_date": last.isoformat() if last else None,
            "next_mode": mode, "next_window_start": ws.isoformat(),
            "next_window_end": we.isoformat()}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=300)
def verify_award_search_delta(award_id: str | None = None) -> dict:
    """Read-back proof: open award_search from R2 and report rows, indices, and (if a
    generated_unique_award_id is given) the live merged row — independent of the write."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(AWARD_SEARCH_URI, storage_options=so)
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
           for i in ds.list_indices()]
    out = {"uri": AWARD_SEARCH_URI, "rows": ds.count_rows(), "indices": idx,
           "merge_key_indexed": MERGE_KEY in str(idx)}
    if award_id:
        out["row"] = ds.scanner(
            filter=f"{MERGE_KEY} = '{award_id}'",
            columns=[MERGE_KEY, "piid", "recipient_uei", "award_amount", "total_obligation",
                     "naics_code", "product_or_service_code", "last_modified_date",
                     "type_description", "description"],
        ).to_table().to_pylist()
    return out


# ─────────────────────────── local entrypoints ───────────────────────────

@app.local_entrypoint()
def init_ops() -> None:
    """Create ops.usaspending_award_search_delta_runs from the co-located DDL file."""
    from pathlib import Path

    sql = Path(__file__).parent.joinpath("ops_usaspending_award_search_delta_runs.sql").read_text()
    print(apply_ops_ddl.remote(sql))


@app.local_entrypoint()
def run(mode: str = "", window_start: str = "", window_end: str = "", dry_run: bool = False) -> None:
    """Manual run (no Trigger callback). Defaults to the watermark-resolved window.
    ``--dry-run`` fetches + counts but merges nothing."""
    import json

    fn = plan_award_search_delta if dry_run else ingest_award_search_delta
    kwargs = {"mode": mode or None, "window_start": window_start or None,
              "window_end": window_end or None}
    if not dry_run:
        kwargs["trigger_callback_url"] = None
    print(json.dumps(fn.remote(**kwargs), indent=2, default=str))


@app.local_entrypoint()
def show_watermark() -> None:
    import json
    print(json.dumps(watermark.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify(award_id: str = "") -> None:
    import json
    print(json.dumps(verify_award_search_delta.remote(award_id or None), indent=2, default=str))
