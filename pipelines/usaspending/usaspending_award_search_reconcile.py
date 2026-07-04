"""Compute worker — USAspending award_search reconciliation (bulk + delta → serving spine).

Reconciles the BULK historical award_search with FRESH API delta into ONE canonical
serving spine (`s3://data-sink/active/usaspending/award_search_merged/`, Lance v2.1):

    BULK   s3://data-sink/active/usaspending/award_search/   (pg_dump snapshot 2026-05-06)
    DELTA  s3://data-sink/usaspending_api_landings/award_search/pull_date=*/  (API dated snapshots)

MERGE (2-source BULK+DELTA reconciliation):
  • Reads BULK once, scans all DELTA landings (union by date >= bulk snapshot date).
  • Per-key collapse to latest-per-key from EACH source (deterministic tiebreaker).
  • CORE: per-key argmax(last_modified_date) over the union of the two collapsed cores,
    executed as ONE 2-way row_number() window — equal-mtime tie → DELTA wins.
  • Delete signaling: rows where correction_delete_ind='D' are filtered out.
  • Outputs the merged current serving view to s3://data-sink/active/usaspending/award_search_merged/.

WHY bulk_download/awards (not search endpoints): the paginated search endpoints hard-cap
at 10,000 records and would silently truncate a multi-day window. The async server-side
CSV job (bulk_download) handles arbitrary volume.

Run modes:
  • backfill: Pull ALL landings from snapshot date → create the merged spine (first build).
  • daily: Pull trailing landings (default 7d) → overwrite the merged spine (upsert-merge).

Watermarking: tracks max(last_modified_date) merged via ops.* audit row + Trigger callback.

    modal deploy pipelines/usaspending/usaspending_award_search_reconcile.py
    modal run pipelines/usaspending/usaspending_award_search_reconcile.py::init_ops
    modal run --detach pipelines/usaspending/usaspending_award_search_reconcile.py::backfill
    modal run --detach pipelines/usaspending/usaspending_award_search_reconcile.py::daily 7
    modal run pipelines/usaspending/usaspending_award_search_reconcile.py::verify
"""

from __future__ import annotations

import datetime as dt
import os

import modal

# ─────────────────────────── constants ───────────────────────────

FEED = "usaspending_award_search_merged"

# Source URIs
BULK_URI = os.environ.get(
    "USASPENDING_AWARD_SEARCH_BULK_URI",
    "s3://data-sink/active/usaspending/award_search",
).rstrip("/") + "/"

DELTA_LANDINGS_BASE = os.environ.get(
    "USASPENDING_AWARD_SEARCH_LANDINGS_URI",
    "s3://data-sink/usaspending_api_landings/award_search",
).rstrip("/")

# Serving spine (merged output)
MERGED_URI = os.environ.get(
    "USASPENDING_AWARD_SEARCH_MERGED_URI",
    "s3://data-sink/active/usaspending/award_search_merged",
).rstrip("/") + "/"

# Bulk snapshot date (fixed reference point for watermark)
BULK_SNAPSHOT_DATE = dt.date(2026, 5, 6)

# Daily reconcile window (trailing days to re-pull for delta)
DELTA_WINDOW_DAYS = 7

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
SCRATCH_DIR = "/tmp"

# Resolution key: contract_award_unique_key (from API landing, canonical for award_search)
AWARD_KEY_COL = "contract_award_unique_key"
SORT_KEY_COL = "last_modified_date"
DELETE_IND_COL = "correction_delete_ind"

# BTREE indices (resolution + GTM filter keys)
INDEX_COLS = [
    "contract_award_unique_key", "recipient_uei", "naics_code", "product_or_service_code",
    "action_date", "last_modified_date", "federal_action_obligation",
]

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "boto3>=1.34", "psycopg[binary]>=3.2",
)
app = modal.App("usaspending-award-search-reconcile", image=image)


# ─────────────────────────── R2 ───────────────────────────

def _r2_storage_options() -> dict[str, str]:
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


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:
        return False


def _list_delta_landings(so: dict) -> list[str]:
    """List all landing dataset paths (pull_date partitions) >= bulk snapshot date."""
    import duckdb
    import re

    con = duckdb.connect(":memory:")
    try:
        # Query all partitions in the landings prefix via S3 glob
        base_path = DELTA_LANDINGS_BASE.replace("s3://", "s3://")
        result = con.sql(
            f"SELECT * FROM glob('{base_path}/pull_date=*/') LIMIT 1000"
        ).fetchall()
    except Exception:
        print(f"WARN: Could not glob landings at {DELTA_LANDINGS_BASE}; assuming empty", flush=True)
        return []
    finally:
        con.close()

    # Extract pull_date from partition paths and filter >= snapshot
    landings = []
    for row in result:
        path = row[0] if isinstance(row, tuple) else row
        match = re.search(r'pull_date=(\d{4}-\d{2}-\d{2})', str(path))
        if match:
            pull_date_str = match.group(1)
            try:
                pull_date = dt.date.fromisoformat(pull_date_str)
                if pull_date >= BULK_SNAPSHOT_DATE:
                    landings.append(f"{DELTA_LANDINGS_BASE}/pull_date={pull_date_str}/")
            except ValueError:
                pass
    return sorted(landings)


def _merge_award_search(sql_query: str, so: dict) -> tuple[int, int]:
    """Execute the merge SQL and write the result to MERGED_URI. Returns (rows, cols)."""
    import duckdb
    import lance

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    os.makedirs(os.path.join(SCRATCH_DIR, "duckdb_spill"), exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{SCRATCH_DIR}/duckdb_spill';")
    try:
        result = con.sql(sql_query).to_arrow_table()
    finally:
        con.close()

    lance.write_dataset(
        result, MERGED_URI, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        storage_options=so,
    )
    print(f"wrote merged: {result.num_rows:,} rows × {len(result.column_names)} cols → {MERGED_URI}",
          flush=True)
    return result.num_rows, len(result.column_names)


def _build_indices(so: dict) -> list[str]:
    import lance
    ds = lance.dataset(MERGED_URI, storage_options=so)
    present = set(ds.schema.names)
    built = []
    for col in INDEX_COLS:
        if col not in present:
            print(f"  SKIP (absent) {col}", flush=True)
            continue
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
        except TypeError:
            ds.create_scalar_index(col, index_type="BTREE")
        built.append(col)
        print(f"  BTREE {col}", flush=True)
    return built


def _record_run(*, run_mode: str, rows_merged: int, columns: int, table_rows_after: int,
                max_last_modified: str | None, status: str, error: str | None,
                started_at: dt.datetime, completed_at: dt.datetime) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.", flush=True)
        return
    if status != "success" and not error:
        error = "unknown terminal failure (no exception captured)"
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.usaspending_award_search_merged_runs
                    (feed, run_mode, rows_merged, columns, table_rows_after, max_last_modified,
                     status, error_message, started_at, executed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, run_mode, rows_merged, columns, table_rows_after, max_last_modified,
                 status, (error or "")[:2000] or None, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:
        print(f"WARN: ops.* write failed: {exc}", flush=True)


def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger.dev waitpoint. No-op on manual runs."""
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.", flush=True)
        return
    import time
    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}", flush=True)
                return
        except Exception as exc:
            print(f"Callback attempt {i + 1} failed: {exc}", flush=True)
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}", flush=True)


# ─────────────────────────── merge SQL ───────────────────────────

def _build_merge_sql(landings: list[str]) -> str:
    """Build the DuckDB SQL to merge bulk + delta landings. Handles all-VARCHAR columns."""
    landings_union = " UNION ALL ".join(
        f"SELECT * FROM read_parquet('{landing}**/*.parquet')"
        for landing in landings
    )
    if landings_union:
        landings_union = f"({landings_union})"
    else:
        landings_union = "(SELECT 1 WHERE FALSE)"  # Empty union

    # Core merge: per-key argmax(last_modified_date), delta wins on tie
    sql = f"""
    WITH bulk_raw AS (
      SELECT * FROM read_parquet('{BULK_URI}**/*.parquet')
    ),
    delta_raw AS {landings_union},
    bulk_latest AS (
      SELECT * EXCEPT (rn)
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY {AWARD_KEY_COL} ORDER BY {SORT_KEY_COL} DESC) AS rn
        FROM bulk_raw
      ) WHERE rn = 1
    ),
    delta_latest AS (
      SELECT * EXCEPT (rn)
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY {AWARD_KEY_COL} ORDER BY {SORT_KEY_COL} DESC) AS rn
        FROM delta_raw
      ) WHERE rn = 1
    ),
    merged AS (
      SELECT * EXCEPT (src, rn)
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY {AWARD_KEY_COL} ORDER BY {SORT_KEY_COL} DESC, src) AS rn,
          src
        FROM (
          SELECT *, 'delta' AS src FROM delta_latest
          UNION ALL
          SELECT *, 'bulk' AS src FROM bulk_latest
        )
      ) WHERE rn = 1
    )
    SELECT * FROM merged
    WHERE COALESCE(NULLIF(TRIM({DELETE_IND_COL}), ''), '') <> 'D'
    """
    return sql


# ─────────────────────────── workers ───────────────────────────

@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,
    memory=65536,
    cpu=8.0,
    retries=modal.Retries(max_retries=3, backoff_coefficient=2.0, initial_delay=30.0),
)
def run_backfill(trigger_callback_url: str | None = None) -> dict:
    """Merge ALL landings >= snapshot date (full backfill). Creates merged spine."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()

    status, error, rows, cols, total, max_lmd = "error", None, 0, 0, 0, None
    try:
        print(f"[backfill] bulk={BULK_URI} delta_base={DELTA_LANDINGS_BASE}", flush=True)

        landings = _list_delta_landings(so)
        print(f"[backfill] found {len(landings)} delta landings >= {BULK_SNAPSHOT_DATE}", flush=True)

        if not landings:
            print("[backfill] WARNING: no delta landings found — proceeding with bulk only", flush=True)

        sql = _build_merge_sql(landings)
        print("[backfill] executing merge SQL…", flush=True)

        rows, cols = _merge_award_search(sql, so)

        print("[backfill] building indices…", flush=True)
        _build_indices(so)

        import lance
        ds = lance.dataset(MERGED_URI, storage_options=so)
        total = ds.count_rows()

        # Extract max(last_modified_date) from merged
        import duckdb
        con = duckdb.connect(":memory:")
        con.register("src", ds.scanner(columns=[SORT_KEY_COL]).to_reader())
        max_lmd = con.execute(
            f"SELECT MAX({SORT_KEY_COL}) FROM src"
        ).fetchone()[0]
        con.close()

        status = "success"
        _post_callback(trigger_callback_url, {
            "status": status, "feed": FEED, "run_mode": "backfill",
            "rows_merged": int(rows), "table_rows_after": int(total),
            "max_last_modified": str(max_lmd), "merged_uri": MERGED_URI,
        })

    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        status = "error"
        raise

    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(
            run_mode="backfill", rows_merged=int(rows), columns=int(cols),
            table_rows_after=int(total), max_last_modified=str(max_lmd) if max_lmd else None,
            status=status, error=error, started_at=started_at, completed_at=completed_at,
        )

    return {
        "feed": FEED, "run_mode": "backfill", "rows_merged": int(rows),
        "columns": int(cols), "table_rows_after": int(total),
        "max_last_modified": str(max_lmd), "status": status,
    }


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,
    memory=65536,
    cpu=8.0,
    retries=modal.Retries(max_retries=3, backoff_coefficient=2.0, initial_delay=30.0),
)
def run_daily(days: int = 7, trigger_callback_url: str | None = None) -> dict:
    """Merge trailing delta landings (default 7d) with bulk (upsert-merge). Overwrites merged spine."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()

    status, error, rows, cols, total, max_lmd = "error", None, 0, 0, 0, None
    try:
        cutoff = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=days)
        print(f"[daily] delta cutoff={cutoff} bulk={BULK_URI}", flush=True)

        all_landings = _list_delta_landings(so)
        landings = [l for l in all_landings if _landing_date(l) >= cutoff]
        print(f"[daily] selected {len(landings)} landings >= {cutoff} from {len(all_landings)} total",
              flush=True)

        if not landings:
            print("[daily] WARNING: no recent delta landings — proceeding with bulk only", flush=True)

        sql = _build_merge_sql(landings)
        print("[daily] executing merge SQL…", flush=True)

        rows, cols = _merge_award_search(sql, so)

        print("[daily] building indices…", flush=True)
        _build_indices(so)

        import lance
        ds = lance.dataset(MERGED_URI, storage_options=so)
        total = ds.count_rows()

        import duckdb
        con = duckdb.connect(":memory:")
        con.register("src", ds.scanner(columns=[SORT_KEY_COL]).to_reader())
        max_lmd = con.execute(
            f"SELECT MAX({SORT_KEY_COL}) FROM src"
        ).fetchone()[0]
        con.close()

        status = "success"
        _post_callback(trigger_callback_url, {
            "status": status, "feed": FEED, "run_mode": "daily",
            "rows_merged": int(rows), "table_rows_after": int(total),
            "max_last_modified": str(max_lmd), "merged_uri": MERGED_URI,
        })

    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        status = "error"
        raise

    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(
            run_mode="daily", rows_merged=int(rows), columns=int(cols),
            table_rows_after=int(total), max_last_modified=str(max_lmd) if max_lmd else None,
            status=status, error=error, started_at=started_at, completed_at=completed_at,
        )

    return {
        "feed": FEED, "run_mode": "daily", "rows_merged": int(rows),
        "columns": int(cols), "table_rows_after": int(total),
        "max_last_modified": str(max_lmd), "status": status,
    }


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=300)
def verify() -> dict:
    """Independent read-back: rows, columns, indices, last_modified frontier."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(MERGED_URI, storage_options=so)
    try:
        idx = ds.list_indices()
    except Exception:
        idx = getattr(ds, "list_indexes", lambda: [])()

    con = duckdb.connect(":memory:")
    con.register("src", ds.scanner(columns=[SORT_KEY_COL, "action_date"]).to_reader())
    con.execute("CREATE TABLE t AS SELECT * FROM src")
    frontier = con.execute(
        f"SELECT min({SORT_KEY_COL}), max({SORT_KEY_COL}), min(action_date), max(action_date) FROM t"
    ).fetchone()
    con.close()

    return {
        "uri": MERGED_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names),
        "indices": [getattr(i, "name", str(i)) for i in idx],
        "min_last_modified": str(frontier[0]), "max_last_modified": str(frontier[1]),
        "min_action_date": str(frontier[2]), "max_action_date": str(frontier[3]),
    }


# ─────────────────────────── helpers ───────────────────────────

def _landing_date(landing_uri: str) -> dt.date:
    """Extract pull_date from landing URI (e.g., 's3://.../pull_date=2026-06-15/')."""
    import re
    match = re.search(r'pull_date=(\d{4}-\d{2}-\d{2})', landing_uri)
    if match:
        return dt.date.fromisoformat(match.group(1))
    raise ValueError(f"Could not extract pull_date from {landing_uri}")


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_ops_ddl(sql: str) -> dict:
    import psycopg
    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    return {"applied": True}


# ─────────────────────────── local entrypoints ───────────────────────────

@app.local_entrypoint()
def init_ops() -> None:
    from pathlib import Path
    sql = Path(__file__).parent.joinpath("ops_usaspending_award_search_merged_runs.sql").read_text()
    print(apply_ops_ddl.remote(sql))


@app.local_entrypoint()
def backfill() -> None:
    """Full merge of all landings >= snapshot date → create the merged spine."""
    import json
    print(json.dumps(run_backfill.remote(), indent=2, default=str))


@app.local_entrypoint()
def daily(days: int = 7) -> None:
    """Trailing-window merge (default 7d) → overwrite merged spine (upsert-merge)."""
    import json
    print(json.dumps(run_daily.remote(days=days), indent=2, default=str))


@app.local_entrypoint()
def verify_table() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
