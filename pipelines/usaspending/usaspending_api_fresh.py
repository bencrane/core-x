"""USAspending API FRESH — standalone, accumulating, append-only contract+IDV table.

Pulls prime contract + IDV transactions from ``bulk_download/awards`` on a
``last_modified_date`` window and lands them VERBATIM — the exact API download
column names, all-VARCHAR, no projection, no renaming — into ONE standalone Lance
table:

    s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/

APPEND-ONLY. ACCUMULATING. THE TABLE ONLY GROWS.
  • backfill — wide window (default 90d) → write the table (first create).
  • daily    — trailing window (default 7d) → mode="append". NEVER overwrites prior data.

Overlapping windows re-pull rows already present → duplicate rows. This is INTENTIONAL
and harmless: USAspending publishes on a lag, so each daily run deliberately goes back a
few extra days and re-pulls data we already hold. A SEPARATE downstream mirror table
reconciles the duplicates on max(last_modified_date) if/when built — NOT here. No dedup,
no merge, no in-place mutate; append-only is the canonical-safe Lance op (new fragments,
no R2 multipart-rewrite hazard).

This table is NOT conformed to the bulk pg_dump schema and is NEVER merged into the bulk
SoR (active/usaspending/award_search). It stands alone.

WHY bulk_download/awards + last_modified_date: uncapped async CSV (the paginated search
endpoints truncate at 10k); last_modified_date captures late-landing / re-modified records
when they actually appear in the warehouse (action_date would miss them — DoD/FPDS lag).

WHY contracts + IDVs in one call: A/B/C/D + IDV_* land in a SINGLE 297-col
``Contracts_PrimeTransactions`` member (verified live), so one pull, one schema, one table.

    modal deploy pipelines/usaspending/usaspending_api_fresh.py
    modal run pipelines/usaspending/usaspending_api_fresh.py::init_ops
    modal run --detach pipelines/usaspending/usaspending_api_fresh.py::backfill            # past 90d → create (overwrite)
    modal run --detach pipelines/usaspending/usaspending_api_fresh.py::daily 10            # past 10d → APPEND (never overwrites)
    modal run pipelines/usaspending/usaspending_api_fresh.py::verify
"""

from __future__ import annotations

import os

import modal

# ─────────────────────────── constants ───────────────────────────

FEED = "usaspending_api_fresh_contract_prime_txn"

FRESH_URI = os.environ.get(
    "USASPENDING_API_FRESH_URI",
    "s3://data-sink/active/usaspending_api_fresh/contract_prime_txn",
).rstrip("/") + "/"

BULK_DL_URL = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
# Prime contracts (A–D) + IDV vehicles. Verified: these all land in ONE
# Contracts_PrimeTransactions member (single 297-col schema), no separate IDV file.
AWARD_TYPES = ["A", "B", "C", "D",
               "IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
DATE_TYPE = "last_modified_date"
BULK_POLL_SECONDS = 15
# 90 days is ~2× the 45-day window that polled ~41 min — give the async job generous room.
BULK_POLL_CEILING_SECONDS = int(os.environ.get("USASPENDING_FRESH_POLL_CEILING", str(150 * 60)))

DATA_STORAGE_VERSION = "2.1"
# Small fragments → uniform R2 multipart parts → R2-safe (the bulk loader's proven path).
MAX_ROWS_PER_FILE = 250_000
SCRATCH_DIR = "/tmp"

# Verbatim API columns to BTREE (all present in Contracts_PrimeTransactions; all-VARCHAR —
# pushdown works on the uniform-width ISO strings). Resolution + GTM filter keys only.
INDEX_COLS = [
    "contract_transaction_unique_key", "contract_award_unique_key",
    "recipient_uei", "naics_code", "product_or_service_code", "cage_code",
    "action_date", "last_modified_date", "federal_action_obligation",
]

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "requests>=2.32", "psycopg[binary]>=3.2",
)
app = modal.App("usaspending-api-fresh", image=image)


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
    """Async bulk_download/awards over the window. Returns (csv_glob, poll_count).
    Raises on a failed job or the poll ceiling (retryable → fresh Modal container)."""
    import shutil
    import time
    import zipfile

    import requests

    payload = {
        "filters": {
            "prime_award_types": AWARD_TYPES,
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
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        zf.extractall(work, members=members)
    print(f"downloaded {len(members)} CSV member(s): {members}", flush=True)
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


def _build_indices(so, rebuild: bool = False) -> list[str]:
    import lance
    ds = lance.dataset(FRESH_URI, storage_options=so)
    present = set(ds.schema.names)
    built = []
    for col in INDEX_COLS:
        if col not in present:
            print(f"  SKIP (absent) {col}", flush=True)
            continue
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True) if rebuild \
                else ds.create_scalar_index(col, index_type="BTREE")
        except TypeError:  # older lance has no `replace=` kwarg
            ds.create_scalar_index(col, index_type="BTREE")
        built.append(col)
        print(f"  BTREE {col}", flush=True)
    return built


def _optimize_indices(so) -> None:
    """Extend existing BTREE indices over newly appended fragments (cheap incremental
    train). Full rebuild only if the lance build lacks optimize_indices."""
    import lance
    ds = lance.dataset(FRESH_URI, storage_options=so)
    try:
        ds.optimize.optimize_indices()
        print("optimize_indices: extended over appended fragments", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"optimize_indices unavailable ({e}); rebuilding", flush=True)
        _build_indices(so, rebuild=True)


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
                INSERT INTO ops.usaspending_api_fresh_runs
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


def _post_callback(url, payload, attempts: int = 3) -> None:
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
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}", flush=True)
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}", flush=True)


# ─────────────────────────── workers ───────────────────────────

@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 200,        # > the 150-min poll ceiling
    memory=32768,            # 90d ≈ ~1.3M rows × 297 verbatim cols
    cpu=4.0,
    retries=modal.Retries(max_retries=5, backoff_coefficient=2.0, initial_delay=30.0),
)
def run_backfill(days: int = 90, force: bool = False) -> dict:
    """Pull the past `days` (last_modified_date) of contract+IDV transactions and CREATE the
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
    print(f"[fresh-backfill] last_modified_date ∈ [{ws} … {we}]  ({days}d, award_types={AWARD_TYPES})",
          flush=True)

    status, error, rows, cols, total, built = "error", None, 0, 0, 0, []
    api_calls = 0
    try:
        csv_glob, api_calls = _fetch_window(ws, we)
        rows, cols = _write(csv_glob, "overwrite", so)
        if rows == 0:
            raise RuntimeError(f"90-day federal window [{ws}…{we}] landed 0 rows — hard failure.")
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


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 200,
    memory=32768,            # identical to run_backfill; a 10-day window is far smaller
    cpu=4.0,
    retries=modal.Retries(max_retries=5, backoff_coefficient=2.0, initial_delay=30.0),  # fresh IP/retry
)
def run_daily(days: int = 7, trigger_callback_url: str | None = None) -> dict:
    """Trailing-window APPEND top-up. Pulls the past `days` (last_modified_date) of contract+IDV
    transactions and APPENDS them (mode='append' — NEVER overwrites). Requires the table to exist
    (daily only appends; first-create is backfill's job). Overlapping recent days are re-pulled on
    purpose (publish lag) → harmless duplicate rows reconciled downstream. optimize_indices extends
    BTREE over the new fragments. POSTs terminal metadata to trigger_callback_url on success."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    if not _dataset_exists(so):
        raise RuntimeError(f"{FRESH_URI} does not exist — run backfill first (daily only appends).")

    ws, we = _window(days)
    print(f"[fresh-daily] last_modified_date ∈ [{ws} … {we}]  ({days}d APPEND, award_types={AWARD_TYPES})",
          flush=True)

    status, error, rows, cols, total, built = "error", None, 0, 0, 0, []
    api_calls = 0
    try:
        csv_glob, api_calls = _fetch_window(ws, we)
        rows, cols = _write(csv_glob, "append", so)
        if rows == 0:
            # Soft-block tell: a "finished" job over a multi-day window with 0 rows is the F5/IP-throttle
            # signature, not a normal quiet day. Surface loudly; do NOT hard-fail an append.
            print("WARN: 0 rows appended for a multi-day window — possible soft block / silent throttle.",
                  flush=True)
        else:
            _optimize_indices(so)
        import lance
        total = lance.dataset(FRESH_URI, storage_options=so).count_rows()
        status = "success"
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": int(rows), "feed": FEED, "dataset_uri": FRESH_URI,
                        "window_start": ws.isoformat(), "window_end": we.isoformat(),
                        "table_rows_after": int(total), "run_mode": "daily"})
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        status = "error"
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(run_mode="daily", window_start=ws, window_end=we, rows_written=int(rows),
                    columns=int(cols), table_rows_after=int(total), api_calls=int(api_calls),
                    write_mode="append", indices_built=built, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)

    return {"feed": FEED, "run_mode": "daily", "window_start": ws.isoformat(),
            "window_end": we.isoformat(), "rows_written": int(rows), "columns": int(cols),
            "table_rows_after": int(total), "api_calls": int(api_calls), "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60, memory=32768, cpu=4.0,
    retries=0,        # commit-atomic; re-driven by the next weekly schedule, never auto-retried mid-rewrite
)
def run_compaction(min_fragments: int = 12, target_rows_per_fragment: int = 250_000,
                   force: bool = False, trigger_callback_url: str | None = None) -> dict:
    """Threshold-gated fragment compaction. No-op unless get_fragments() > min_fragments (or force).
    optimize_indices (cheap guard) → compact_files → cleanup_old_versions. Row count invariant (asserted).
    target_rows_per_fragment=250_000 is MANDATORY on R2: the Lance default (~1M) writes a file large enough
    that object_store escalates its multipart part size mid-upload, which R2 rejects (400 InvalidPart,
    "all non-trailing parts must have the same length" — ARCHITECTURE.md:140). 250k keeps each rewritten
    fragment at the proven R2-safe append size AND bounds the count to ~total_rows/250k (it re-merges the
    small daily fragments each pass). compact_files remaps the BTREE indices in-place (verified on live R2)."""
    import datetime as dt
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    ds = lance.dataset(FRESH_URI, storage_options=so)
    n0, rows0 = len(ds.get_fragments()), ds.count_rows()
    if n0 <= min_fragments and not force:
        print(f"[compaction] {n0} fragments ≤ {min_fragments}; no-op.", flush=True)
        _post_callback(trigger_callback_url, {"status": "success", "run_mode": "compaction", "feed": FEED,
                       "compacted": False, "fragments_before": n0, "fragments_after": n0, "table_rows_after": rows0})
        return {"status": "success", "compacted": False, "fragments_before": n0, "fragments_after": n0}

    status, error, n1, rows1, files_removed = "error", None, n0, rows0, None
    try:
        ds.optimize.optimize_indices()                                   # cheap idempotent guard (append already folds tail)
        m = ds.optimize.compact_files(target_rows_per_fragment=target_rows_per_fragment)  # 250k = R2-safe; remaps BTREE in-place
        files_removed = getattr(m, "files_removed", None)
        try:
            ds.cleanup_old_versions(retain_versions=30)                  # count cap; best-effort; never delete_unverified=True
        except Exception as e:  # noqa: BLE001
            print(f"WARN cleanup_old_versions: {e}", flush=True)
        ds2 = lance.dataset(FRESH_URI, storage_options=so)
        n1, rows1 = len(ds2.get_fragments()), ds2.count_rows()
        if rows1 != rows0:
            raise RuntimeError(f"ROW COUNT CHANGED by compaction: {rows0} → {rows1} — abort.")
        status = "success"
        print(f"[compaction] fragments {n0} → {n1}; rows={rows1} (invariant); files_removed={files_removed}", flush=True)
        _post_callback(trigger_callback_url, {"status": status, "run_mode": "compaction", "feed": FEED,
                       "compacted": True, "fragments_before": n0, "fragments_after": n1, "table_rows_after": rows1})
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"; status = "error"; raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(run_mode="compaction", window_start=started_at.date(), window_end=completed_at.date(),
                    rows_written=0, columns=0, table_rows_after=int(rows1), api_calls=0, write_mode="compact",
                    indices_built=[], status=status, error=error,   # error stays None on success
                    started_at=started_at, completed_at=completed_at)
    return {"status": status, "compacted": True, "fragments_before": n0, "fragments_after": n1}


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
    """Independent read-back: rows, columns, committed indices, last_modified frontier."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(FRESH_URI, storage_options=so)
    try:
        idx = ds.list_indices()
    except Exception:  # noqa: BLE001 — return-shape drift across lance versions
        idx = getattr(ds, "list_indexes", lambda: [])()
    con = duckdb.connect(":memory:")
    con.register("src", ds.scanner(columns=["last_modified_date", "action_date"]).to_reader())
    con.execute("CREATE TABLE t AS SELECT * FROM src")
    fr = con.execute("SELECT min(last_modified_date), max(last_modified_date), "
                     "min(action_date), max(action_date) FROM t").fetchone()
    con.close()
    return {"uri": FRESH_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names),
            "indices": [getattr(i, "name", str(i)) for i in idx],
            "min_last_modified": str(fr[0]), "max_last_modified": str(fr[1]),
            "min_action_date": str(fr[2]), "max_action_date": str(fr[3])}


# ─────────────────────────── local entrypoints ───────────────────────────

@app.local_entrypoint()
def init_ops() -> None:
    from pathlib import Path
    sql = Path(__file__).parent.joinpath("ops_usaspending_api_fresh_runs.sql").read_text()
    print(apply_ops_ddl.remote(sql))


@app.local_entrypoint()
def backfill(days: int = 90, force: bool = False) -> None:
    """Get the past `days` (default 90) of contract+IDV transactions NOW → create the table.
    Use `modal run --detach …::backfill` so the long async pull survives a disconnect."""
    import json
    print(json.dumps(run_backfill.remote(days=days, force=force), indent=2, default=str))


@app.local_entrypoint()
def daily(days: int = 7) -> None:
    """Trailing-window APPEND top-up NOW (mode=append, never overwrites). 10-day window:
        modal run --detach pipelines/usaspending/usaspending_api_fresh.py::daily 10
    Launched locally, EXECUTES ON MODAL (fresh-IP-per-retry beats USAspending's IP throttle)."""
    import json
    print(json.dumps(run_daily.remote(days=days), indent=2, default=str))


@app.local_entrypoint()
def compact(min_fragments: int = 12, force: bool = False) -> None:
    """Threshold-gated fragment compaction (no-op unless get_fragments() > min_fragments, or force).
        modal run …::compact --force true            # force, ignore the gate
        modal run …::compact --min-fragments 3       # exercise the GATED fire path"""
    import json
    print(json.dumps(run_compaction.remote(min_fragments=min_fragments, force=force), indent=2, default=str))


@app.local_entrypoint()
def verify_table() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
