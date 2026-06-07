"""USAspending API SUBAWARD FRESH — standalone, accumulating, append-only procurement
subaward table.

The subaward counterpart of ``usaspending_api_fresh.py`` (prime contract+IDV). Pulls
PROCUREMENT subawards from ``bulk_download/awards`` on a ``last_modified_date`` window and
lands them VERBATIM — the exact API download column names, all-VARCHAR, no projection, no
renaming — into ONE standalone Lance table:

    s3://data-sink/active/usaspending_api_fresh/contract_subaward/

APPEND-ONLY. ACCUMULATING. THE TABLE ONLY GROWS.
  • backfill — wide window (default 90d) → write the table (first create).
  • daily    — trailing window (default 7d) → mode="append". NEVER overwrites prior data.

Overlapping windows re-pull rows already present → duplicate rows. This is INTENTIONAL
and harmless: subaward reporting (FFATA/FSRS) lags even more than prime FPDS, so each daily
run deliberately goes back extra days and re-pulls data we already hold. A SEPARATE
downstream mirror table reconciles the duplicates on max(modified) if/when built — NOT here.
No dedup, no merge, no in-place mutate; append-only is the canonical-safe Lance op (new
fragments, no R2 multipart-rewrite hazard).

This table is NOT conformed to the bulk pg_dump schema (rpt.subaward_search) and is NEVER
merged into it. It stands alone, exactly like the prime fresh table.

WHY bulk_download/awards + last_modified_date: uncapped async CSV; last_modified_date
captures late-landing / re-modified subawards when they actually appear in the warehouse
(action_date would miss them — heavy FFATA/FSRS lag). date_type=last_modified_date is a
valid filter for sub_award_types (verified against the upstream API contract).

WHY procurement only: contract subawards (``Contracts_Subawards``, 100 verbatim cols) and
grant subawards (``Assistance_Subawards``, different schema) are DISTINCT shapes. Procurement
is the faithful subaward counterpart of the contract-prime table. Grant subawards belong in a
SEPARATE parallel table, never merged into this one — same discipline that keeps assistance
primes out of the contract-prime table.

    modal deploy pipelines/usaspending/usaspending_api_subaward_fresh.py
    modal run pipelines/usaspending/usaspending_api_subaward_fresh.py::init_ops
    modal run --detach pipelines/usaspending/usaspending_api_subaward_fresh.py::backfill   # past 90d → create
    modal run pipelines/usaspending/usaspending_api_subaward_fresh.py::verify_table
"""

from __future__ import annotations

import os

import modal

# ─────────────────────────── constants ───────────────────────────

FEED = "usaspending_api_fresh_contract_subaward"

FRESH_URI = os.environ.get(
    "USASPENDING_API_SUBAWARD_FRESH_URI",
    "s3://data-sink/active/usaspending_api_fresh/contract_subaward",
).rstrip("/") + "/"

BULK_DL_URL = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
# Procurement (contract) subawards only — they land in ONE Contracts_Subawards member
# (single 100-col schema). Grant subawards (Assistance_Subawards) are a separate schema
# and a separate table; do NOT request them here.
SUB_AWARD_TYPES = ["procurement"]
DATE_TYPE = "last_modified_date"
BULK_POLL_SECONDS = 15
# Subaward files are far smaller than prime-txn files; the prime worker's 150-min ceiling is
# generous headroom here too.
BULK_POLL_CEILING_SECONDS = int(os.environ.get("USASPENDING_SUBAWARD_FRESH_POLL_CEILING", str(150 * 60)))

DATA_STORAGE_VERSION = "2.1"
# Small fragments → uniform R2 multipart parts → R2-safe (the bulk loader's proven path).
MAX_ROWS_PER_FILE = 250_000
SCRATCH_DIR = "/tmp"

# Verbatim API columns to BTREE — all present in Contracts_Subawards (verified against
# upstream download_column_historical_lookups.py: query_paths['subaward_search']['d1']).
# Resolution + GTM filter keys only. NOTE the subaward file carries NO product_or_service_code,
# NO cage_code, and NO plain last_modified_date — its modification frontier is
# subaward_sam_report_last_modified_date, and NAICS is only the prime award's.
INDEX_COLS = [
    "prime_award_unique_key",                # join key → prime award (== prime file's contract_award_unique_key)
    "subaward_number",                       # subaward identity (with prime_award_unique_key)
    "subawardee_uei",                        # the entity that RECEIVED the subaward (sub recipient resolution)
    "prime_awardee_uei",                     # the entity that ISSUED it (prime recipient resolution)
    "prime_award_piid",                      # prime contract PIID (resolution)
    "prime_award_naics_code",                # GTM filter (subawards carry only the prime's NAICS)
    "subaward_action_date",                  # temporal (== prime file's action_date)
    "subaward_sam_report_last_modified_date",  # last-modified frontier (== prime file's last_modified_date)
    "subaward_amount",                       # GTM filter / sort (== prime file's federal_action_obligation)
]

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "requests>=2.32", "psycopg[binary]>=3.2",
)
app = modal.App("usaspending-api-subaward-fresh", image=image)


# ─────────────────────────── R2 ───────────────────────────

def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _dataset_exists(so) -> bool:
    import lance
    try:
        lance.dataset(FRESH_URI, storage_options=so)
        return True
    except Exception:  # noqa: BLE001 — any open failure ⇒ treat as absent
        return False


def _window(days: int):
    import datetime as dt
    we = dt.datetime.now(dt.timezone.utc).date()
    ws = we - dt.timedelta(days=days)
    return ws, we


# ─────────────────────────── fetch (bulk_download/awards) ───────────────────────────

class _ThrottledError(RuntimeError):
    """Persistent 429 → fail fast so modal.Retries recycles the container (fresh IP)."""


def _fetch_window(window_start, window_end):
    """Async bulk_download/awards (sub_award_types) over the window. Returns
    (csv_glob, poll_count). Raises on a failed job or the poll ceiling (retryable → fresh
    Modal container)."""
    import shutil
    import time
    import zipfile

    import requests

    payload = {
        "filters": {
            "sub_award_types": SUB_AWARD_TYPES,
            "date_type": DATE_TYPE,
            "date_range": {"start_date": window_start.isoformat(),
                           "end_date": window_end.isoformat()},
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
    print(f"bulk_download job submitted: {job.get('file_name')}", flush=True)

    polls = 0
    deadline = time.time() + BULK_POLL_CEILING_SECONDS
    while time.time() < deadline:
        time.sleep(BULK_POLL_SECONDS)
        polls += 1
        try:
            st = requests.get(status_url, timeout=(30, 120)).json()
        except Exception as exc:  # noqa: BLE001 — transient poll error → keep polling
            print(f"  poll {polls}: transient ({exc})", flush=True)
            continue
        status = st.get("status")
        print(f"  poll {polls}: status={status} rows={st.get('total_rows')}", flush=True)
        if status == "finished":
            break
        if status == "failed":
            raise RuntimeError(f"bulk_download job failed: {st.get('message', '')[:300]}")
    else:
        raise RuntimeError(f"bulk_download job did not finish within "
                           f"{BULK_POLL_CEILING_SECONDS}s ({polls} polls)")

    work = os.path.join(SCRATCH_DIR, "fresh_pull")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work, exist_ok=True)
    zip_path = os.path.join(work, "awards.zip")
    with requests.get(file_url, stream=True, timeout=(30, 900)) as dl:
        dl.raise_for_status()
        with open(zip_path, "wb") as fh:
            for chunk in dl.iter_content(chunk_size=8 * 1024 * 1024):
                fh.write(chunk)
    with zipfile.ZipFile(zip_path) as zf:
        # Isolate the subaward member(s); never read a stray prime/manifest CSV into the
        # all-VARCHAR union (a different schema would corrupt the read).
        members = [m for m in zf.namelist()
                   if m.lower().endswith(".csv") and "subaward" in m.lower()]
        if not members:
            raise RuntimeError(
                f"download contained no Subawards CSV member: {zf.namelist()[:10]}")
        zf.extractall(work, members=members)
    print(f"downloaded {len(members)} Subawards CSV member(s): {members}", flush=True)
    return os.path.join(work, "*.csv"), polls


# ─────────────────────────── write (verbatim → Lance) ───────────────────────────

def _write(csv_glob, mode, so) -> tuple[int, int]:
    """Read the bulk CSV(s) all-VARCHAR and write VERBATIM (exact API column names) to the
    fresh table. mode='overwrite' on first create, mode='append' thereafter. Returns
    (rows_written, columns)."""
    import duckdb
    import lance

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    os.makedirs(os.path.join(SCRATCH_DIR, "duckdb_spill"), exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{SCRATCH_DIR}/duckdb_spill';")
    try:
        tbl = con.sql(
            f"SELECT * FROM read_csv('{csv_glob}', all_varchar=true, header=true, "
            f"sample_size=-1, ignore_errors=false)"
        ).to_arrow_table()
    finally:
        con.close()

    lance.write_dataset(tbl, FRESH_URI, mode=mode,
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
    print(f"wrote (mode={mode}): {tbl.num_rows:,} rows × {len(tbl.column_names)} cols → {FRESH_URI}",
          flush=True)
    return tbl.num_rows, len(tbl.column_names)


def _build_indices(so) -> list[str]:
    import lance
    ds = lance.dataset(FRESH_URI, storage_options=so)
    present = set(ds.schema.names)
    built = []
    for col in INDEX_COLS:
        if col in present:
            ds.create_scalar_index(col, index_type="BTREE")
            built.append(col)
            print(f"  BTREE {col}", flush=True)
        else:
            print(f"  SKIP (absent) {col}", flush=True)
    return built


# ─────────────────────────── ops ledger (audit) ───────────────────────────

def _record_run(*, run_mode, window_start, window_end, rows_written, columns,
                table_rows_after, api_calls, write_mode, indices_built, status, error,
                started_at, completed_at) -> None:
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
                INSERT INTO ops.usaspending_api_subaward_fresh_runs
                    (feed, run_mode, window_start, window_end, rows_written, columns,
                     table_rows_after, api_calls, write_mode, indices_built, status,
                     error_message, started_at, executed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (FEED, run_mode, window_start, window_end, rows_written, columns,
                 table_rows_after, api_calls, write_mode,
                 ",".join(indices_built) if indices_built else None,
                 status, (error or "")[:2000] or None, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}", flush=True)


# ─────────────────────────── workers ───────────────────────────

@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 200,        # > the 150-min poll ceiling
    memory=32768,            # generous; 90d procurement subawards ≪ the prime-txn volume
    cpu=4.0,
    retries=modal.Retries(max_retries=5, backoff_coefficient=2.0, initial_delay=30.0),
)
def run_backfill(days: int = 90, force: bool = False) -> dict:
    """Pull the past `days` (last_modified_date) of procurement subawards and CREATE the
    fresh table. Refuses if the table already exists unless force=True (so a stray backfill
    can never wipe an accumulating table). Verbatim columns, all-VARCHAR. Builds indices."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    if _dataset_exists(so) and not force:
        raise RuntimeError(
            f"{FRESH_URI} already exists — backfill would OVERWRITE it. Re-run with "
            f"force=True only if you intend to recreate the table from scratch.")

    ws, we = _window(days)
    print(f"[sub-fresh-backfill] last_modified_date ∈ [{ws} … {we}]  ({days}d, "
          f"sub_award_types={SUB_AWARD_TYPES})", flush=True)

    status, error, rows, cols, total, built = "error", None, 0, 0, 0, []
    api_calls = 0
    try:
        csv_glob, api_calls = _fetch_window(ws, we)
        rows, cols = _write(csv_glob, "overwrite", so)
        if rows == 0:
            raise RuntimeError(f"90-day procurement-subaward window [{ws}…{we}] landed 0 rows — hard failure.")
        print("building indices…", flush=True)
        built = _build_indices(so)
        import lance
        total = lance.dataset(FRESH_URI, storage_options=so).count_rows()
        status = "success"
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        status = "error"
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(run_mode="backfill", window_start=ws, window_end=we, rows_written=int(rows),
                    columns=int(cols), table_rows_after=int(total), api_calls=int(api_calls),
                    write_mode="overwrite", indices_built=built, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)

    return {"feed": FEED, "run_mode": "backfill", "window_start": ws.isoformat(),
            "window_end": we.isoformat(), "rows_written": int(rows), "columns": int(cols),
            "table_rows_after": int(total), "api_calls": int(api_calls),
            "indices_built": built, "status": status}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_ops_ddl(sql: str) -> dict:
    import psycopg
    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    return {"applied": True}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=300)
def verify() -> dict:
    """Independent read-back: rows, columns, committed indices, subaward action + SAM-report
    last-modified frontier."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(FRESH_URI, storage_options=so)
    try:
        idx = ds.list_indices()
    except Exception:  # noqa: BLE001 — return-shape drift across lance versions
        idx = getattr(ds, "list_indexes", lambda: [])()
    con = duckdb.connect(":memory:")
    con.register("src", ds.scanner(
        columns=["subaward_action_date", "subaward_sam_report_last_modified_date"]).to_reader())
    con.execute("CREATE TABLE t AS SELECT * FROM src")
    fr = con.execute(
        "SELECT min(subaward_action_date), max(subaward_action_date), "
        "min(subaward_sam_report_last_modified_date), "
        "max(subaward_sam_report_last_modified_date) FROM t").fetchone()
    con.close()
    return {"uri": FRESH_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names),
            "indices": [getattr(i, "name", str(i)) for i in idx],
            "min_subaward_action_date": str(fr[0]), "max_subaward_action_date": str(fr[1]),
            "min_sam_last_modified": str(fr[2]), "max_sam_last_modified": str(fr[3])}


# ─────────────────────────── local entrypoints ───────────────────────────

@app.local_entrypoint()
def init_ops() -> None:
    from pathlib import Path
    sql = Path(__file__).parent.joinpath("ops_usaspending_api_subaward_fresh_runs.sql").read_text()
    print(apply_ops_ddl.remote(sql))


@app.local_entrypoint()
def backfill(days: int = 90, force: bool = False) -> None:
    """Get the past `days` (default 90) of procurement subawards NOW → create the table.
    Use `modal run --detach …::backfill` so the long async pull survives a disconnect."""
    import json
    print(json.dumps(run_backfill.remote(days=days, force=force), indent=2, default=str))


@app.local_entrypoint()
def verify_table() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
