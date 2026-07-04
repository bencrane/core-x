"""USAspending API FRESH — standalone, accumulating, append-only contract+IDV AWARD-SUMMARY table.

Pulls prime CONTRACT (A–D) + IDV AWARD SUMMARIES from ``download/awards`` on a
``last_modified_date`` window and lands them VERBATIM — the exact API download column names,
all-VARCHAR, no projection, no renaming — into ONE standalone Lance table:

    s3://data-sink/active/usaspending_api_fresh/contract_prime_award/

This is the AWARD-grain sibling of ``usaspending_api_fresh.py`` (which lands TRANSACTION grain
from ``bulk_download/awards`` → the Contracts_PrimeTransactions member). AWARD grain here is keyed
by ``contract_award_unique_key`` — one row per award summary, the API's rollup of an award's
transactions (obligations, current/potential value, base/latest action dates, PoP, exec-comp).

APPEND-ONLY. ACCUMULATING. THE TABLE ONLY GROWS.
  • backfill — wide window → CREATE the table (first write, mode="overwrite", guarded by force=).
  • daily    — trailing window → mode="append". NEVER overwrites prior data.

Overlapping windows re-pull rows already present → duplicate rows. INTENTIONAL and harmless:
USAspending publishes on a lag, so each daily run deliberately reaches back a few extra days and
re-pulls awards we already hold. A SEPARATE downstream mirror reconciles duplicates on
argmax(last_modified_date) if/when built — NOT here. Append-only is the canonical-safe Lance op
(new fragments, no R2 multipart-rewrite hazard). This table is NEVER merged into the bulk award
SoR (active/usaspending/award_search); it stands alone as the uncapped fresh overlay.

────────────────────────────────────────────────────────────────────────────────────────────────
WHY download/awards (NOT bulk_download/awards) — the endpoint that failed 3× before:
  ``bulk_download/awards`` emits ONLY the *PrimeTransactions* member (txn grain). The award-summary
  member (``Contracts_PrimeAwardSummaries``) lives on ``download/awards``. Switching the award
  fresh leg is a change of ENDPOINT, not a member swap.

THE PAYLOAD — the exact bug that killed the 3 prior clones. ``download/awards`` speaks the
search-family AdvancedFilterObject, which differs from the ``bulk_download/awards`` schema:
  • award types → FLAT top-level ``filters.award_type_codes`` (array[string]).
      NOT ``prime_and_sub_award_types.prime_awards`` — that is the *bulk_download* schema; sent to
      download/awards the server SILENTLY DROPS the restriction and downloads the FULL award
      universe (contracts + assistance + loans).
  • date window → ``filters.time_period`` = ARRAY of {start_date, end_date, date_type} objects.
      NOT a top-level flat ``date_type`` + ``date_range`` (bulk_download schema) — sent to
      download/awards those keys are UNRECOGNIZED and DROPPED, so NO date bound is applied and the
      job enumerates the entire unbounded universe → hangs at status:"running" forever.
  Both failure modes verified live 2026-07-04 by inspecting the server's ``download_request.filters``
  echo: the wrong payload echoed back the full award_type_codes list and NO time_period; the correct
  payload echoed back exactly the 12 codes we sent and the time_period with last_modified_date.

WHY last_modified_date: contract-valid — it is an explicit member of the "Award Search Time Period
Object" date_type enum (usaspending_api/api_contracts/search_filters.md), AND empirically honored
(echoed + bounded the job). It captures late-landing / re-modified awards when they actually appear
in the warehouse; action_date would miss DoD/FPDS-lag records. Watermark sanity via GET
/api/v2/awards/last_updated.

THE REAL OPERATIONAL LIMIT is NOT the 500k row cap (contract-CIV density is ~10k award summaries per
last_modified day, ~2 orders of magnitude under the cap). It is the server-side Elasticsearch
ID-enumeration timeout: ``download/awards`` first scrolls ES to collect award IDs with a hard 90s
read timeout. A window wide enough to enumerate too many IDs at once trips
``opensearchpy ConnectionTimeout`` and the job terminates status:"failed" (verified: a 5-day
A–D+IDV window failed on the ES timeout; a 1-day window finished cleanly, ~7½ min, 9,787 award
rows). So we CHUNK SMALL (default 1 day) and RETRY the transient failed/timeout per chunk. The 500k
cap is kept only as a defensive truncation tripwire.
────────────────────────────────────────────────────────────────────────────────────────────────

Measured 2026-07-04 (live download/awards, 1-day last_modified window 2026-06-23):
  Contracts_PrimeAwardSummaries member = 286 columns, 9,787 award rows; ZIP also carries a
  Contracts_Subawards member (dropped). All 11 index keys present. Full chain (fetch→extract→
  duckdb all_varchar→lance write→BTREE→point-lookup) validated locally.

    modal deploy pipelines/usaspending/usaspending_api_award_fresh.py
    modal run pipelines/usaspending/usaspending_api_award_fresh.py::init_ops
    modal run --detach pipelines/usaspending/usaspending_api_award_fresh.py::backfill            # create
    modal run --detach pipelines/usaspending/usaspending_api_award_fresh.py::daily 10            # APPEND
    modal run pipelines/usaspending/usaspending_api_award_fresh.py::verify
"""

from __future__ import annotations

import os

import modal

# ─────────────────────────── constants ───────────────────────────

FEED = "usaspending_api_fresh_contract_prime_award"

FRESH_URI = os.environ.get(
    "USASPENDING_API_AWARD_FRESH_URI",
    "s3://data-sink/active/usaspending_api_fresh/contract_prime_award",
).rstrip("/") + "/"

DOWNLOAD_URL = "https://api.usaspending.gov/api/v2/download/awards/"
LAST_UPDATED_URL = "https://api.usaspending.gov/api/v2/awards/last_updated/"

# Prime CONTRACT (A–D) + IDV vehicles → the Contracts_PrimeAwardSummaries member. Restricting to
# these codes is what keeps Assistance_* members out of the ZIP entirely.
AWARD_TYPE_CODES = ["A", "B", "C", "D",
                    "IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
# Documented member of the Award Search Time Period Object date_type enum; verified honored live.
DATE_TYPE = "last_modified_date"
PAS_MEMBER = "Contracts_PrimeAwardSummaries"   # the ONE member we keep (286 cols, award grain)

# ── chunking: driven by the ES 90s ID-enumeration timeout, NOT the 500k row cap ──
# 1-day chunks finish (~7½ min / 9,787 rows); a 5-day chunk tripped the ES timeout. Keep it small.
CHUNK_DAYS = int(os.environ.get("USASPENDING_AWARD_FRESH_CHUNK_DAYS", "1"))
# Each finished chunk over the cap would silently truncate — defensive tripwire only (we are ~50×
# under the cap at observed density).
ROW_CAP = 500_000
CAP_GUARD = 490_000
# A chunk that ends status:"failed" (ES ConnectionTimeout) or exceeds its own poll ceiling is
# retried this many times with a fresh submission before the whole run fails.
CHUNK_MAX_ATTEMPTS = int(os.environ.get("USASPENDING_AWARD_FRESH_CHUNK_ATTEMPTS", "4"))

BULK_POLL_SECONDS = 10
# Per-CHUNK ceiling (not whole-run). 1-day chunk finished in ~7½ min; 20 min gives generous slack.
CHUNK_POLL_CEILING_SECONDS = int(os.environ.get("USASPENDING_AWARD_FRESH_POLL_CEILING", str(20 * 60)))

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 250_000              # uniform R2 multipart parts → R2-safe append size
SCRATCH_DIR = "/tmp"

# Verbatim PAS columns to BTREE (presence-filtered at index time; all-VARCHAR). Award-grain
# resolution + GTM filter keys. All 11 verified present in the live 286-col member.
INDEX_COLS = [
    "contract_award_unique_key", "award_id_piid", "recipient_uei", "recipient_name",
    "naics_code", "product_or_service_code", "cage_code",
    "award_base_action_date", "award_latest_action_date", "last_modified_date",
    "total_obligated_amount",
]

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "requests>=2.32", "psycopg[binary]>=3.2",
)
app = modal.App("usaspending-api-award-fresh", image=image)


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


# ─────────────────────────── fetch (download/awards) ───────────────────────────

class _ThrottledError(RuntimeError):
    """Persistent 429 → fail fast so modal.Retries recycles the container (fresh IP)."""


class _ChunkTransientError(RuntimeError):
    """A chunk terminated status:'failed' (ES ConnectionTimeout) or blew its poll ceiling — the job
    handle is per-chunk disposable, so we resubmit the SAME window rather than fail the run."""


def _build_payload(window_start, window_end) -> dict:
    """The correct download/awards AdvancedFilterObject body. FLAT award_type_codes + time_period[]
    array — the exact shape the server echoes back verbatim (proven). Do NOT substitute the
    bulk_download prime_and_sub_award_types / flat date_range shape; the server drops it silently."""
    return {
        "filters": {
            "award_type_codes": AWARD_TYPE_CODES,
            "time_period": [
                {"start_date": window_start.isoformat(),
                 "end_date": window_end.isoformat(),
                 "date_type": DATE_TYPE},
            ],
        },
        "file_format": "csv",
    }


def _fetch_chunk_once(window_start, window_end, work) -> tuple[int, int]:
    """One download/awards job over [start, end] (inclusive). Extracts ONLY the
    Contracts_PrimeAwardSummaries member into `work` (unique per-job filename). Returns
    (polls, total_rows). Raises _ChunkTransientError on failed/ceiling (caller retries),
    RuntimeError on hard errors, _ThrottledError on persistent 429."""
    import time
    import zipfile

    import requests

    payload = _build_payload(window_start, window_end)
    resp = requests.post(DOWNLOAD_URL, json=payload, timeout=(30, 120))
    if resp.status_code == 429:
        raise _ThrottledError("download/awards 429 → recycle container")
    if resp.status_code >= 300:
        raise RuntimeError(f"download/awards submit {resp.status_code}: {resp.text[:300]}")
    job = resp.json()
    status_url = job["status_url"]
    file_url = job["file_url"]

    # Trust-but-verify the server actually parsed our filter (guards against a silent schema drop).
    echoed = (job.get("download_request") or {}).get("filters") or {}
    if "time_period" not in echoed:
        raise RuntimeError(
            f"download/awards did NOT echo a time_period — filter was dropped (unbounded job). "
            f"echoed filters keys={list(echoed)}. Payload schema is wrong; refusing to poll.")
    print(f"download/awards job [{window_start}…{window_end}]: {job.get('file_name')} "
          f"(echoed {len(echoed.get('award_type_codes', []))} award_type_codes, "
          f"time_period ok)", flush=True)

    polls, total_rows = 0, 0
    deadline = time.time() + CHUNK_POLL_CEILING_SECONDS
    while time.time() < deadline:
        time.sleep(BULK_POLL_SECONDS)
        polls += 1
        try:
            st = requests.get(status_url, timeout=(30, 120)).json()
        except Exception as exc:  # noqa: BLE001 — transient poll error → keep polling
            print(f"  poll {polls}: transient ({exc})", flush=True)
            continue
        status = st.get("status")
        total_rows = st.get("total_rows") or 0
        print(f"  poll {polls}: status={status} rows={total_rows}", flush=True)
        if status == "finished":
            break
        if status == "failed":
            # ES ID-enumeration ConnectionTimeout is the dominant cause and is transient.
            raise _ChunkTransientError(
                f"chunk [{window_start}…{window_end}] failed: {str(st.get('message', ''))[:200]}")
    else:
        raise _ChunkTransientError(
            f"chunk [{window_start}…{window_end}] did not finish within "
            f"{CHUNK_POLL_CEILING_SECONDS}s ({polls} polls)")

    # Truncation tripwire: a chunk at/over the cap means the 500k limit clipped rows → shrink chunk.
    if total_rows >= CAP_GUARD:
        raise RuntimeError(
            f"chunk [{window_start}…{window_end}] total_rows={total_rows:,} ≥ {CAP_GUARD:,} "
            f"(cap {ROW_CAP:,}) — window too wide, download would truncate. Reduce "
            f"USASPENDING_AWARD_FRESH_CHUNK_DAYS below {CHUNK_DAYS} and re-run.")

    zip_path = os.path.join(work, f"awards_{window_start.isoformat()}.zip")
    with requests.get(file_url, stream=True, timeout=(30, 900)) as dl:
        dl.raise_for_status()
        with open(zip_path, "wb") as fh:
            for chunk in dl.iter_content(chunk_size=8 * 1024 * 1024):
                fh.write(chunk)
    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist()
                   if PAS_MEMBER in m and m.lower().endswith(".csv")]
        if not members:
            # A truly empty last_modified day yields a header-only or absent PAS member — tolerate 0.
            print(f"  no {PAS_MEMBER} member in ZIP (empty window?): {zf.namelist()}", flush=True)
        else:
            zf.extractall(work, members=members)
            print(f"  extracted {PAS_MEMBER}: {members}", flush=True)
    os.remove(zip_path)
    return polls, total_rows


def _fetch_chunk(window_start, window_end, work) -> tuple[int, int]:
    """Retry wrapper: resubmits a transient (ES-timeout / ceiling) chunk up to CHUNK_MAX_ATTEMPTS."""
    import time

    last_exc = None
    total_polls = 0
    for attempt in range(1, CHUNK_MAX_ATTEMPTS + 1):
        try:
            polls, total_rows = _fetch_chunk_once(window_start, window_end, work)
            return total_polls + polls, total_rows
        except _ChunkTransientError as exc:
            last_exc = exc
            total_polls += CHUNK_POLL_CEILING_SECONDS // BULK_POLL_SECONDS  # conservative poll tally
            backoff = min(30 * attempt, 120)
            print(f"  chunk transient (attempt {attempt}/{CHUNK_MAX_ATTEMPTS}): {exc}; "
                  f"resubmitting in {backoff}s", flush=True)
            time.sleep(backoff)
    raise RuntimeError(f"chunk [{window_start}…{window_end}] exhausted "
                       f"{CHUNK_MAX_ATTEMPTS} attempts: {last_exc}")


# ─────────────────────────── write (verbatim → Lance) ───────────────────────────

def _write(csv_glob, mode, so) -> tuple[int, int]:
    """Read the extracted PrimeAwardSummaries CSV(s) all-VARCHAR and write VERBATIM (exact API
    column names) to the fresh table. mode='overwrite' on first create, 'append' thereafter.
    union_by_name tolerates member-to-member column-order/width drift across chunks."""
    import glob

    import duckdb
    import lance

    if not glob.glob(csv_glob):
        raise RuntimeError(f"no PAS CSV to write (glob {csv_glob} matched nothing) — every chunk "
                           f"was empty. Refusing to write 0 rows.")

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    os.makedirs(os.path.join(SCRATCH_DIR, "duckdb_spill"), exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{SCRATCH_DIR}/duckdb_spill';")
    try:
        tbl = con.sql(
            f"SELECT * FROM read_csv('{csv_glob}', all_varchar=true, header=true, "
            f"sample_size=-1, ignore_errors=false, union_by_name=true)"
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
                INSERT INTO ops.usaspending_api_award_fresh_runs
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

def _run_window(days: int, chunk_days: int, write_mode: str, so) -> tuple[int, int, int, int]:
    """Chunk [now-days … now] into ≤chunk_days sub-windows, fetch each Contracts_PrimeAwardSummaries
    member into a shared workdir (with per-chunk retry), write ONCE. Returns
    (rows, cols, table_rows_after, api_calls)."""
    import datetime as dt
    import shutil

    ws, we = _window(days)
    work = os.path.join(SCRATCH_DIR, "award_fresh_pull")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work, exist_ok=True)
    print(f"last_modified_date ∈ [{ws} … {we}] ({days}d, {chunk_days}d chunks, "
          f"award_type_codes={AWARD_TYPE_CODES})", flush=True)

    api_calls = 0
    cur = ws
    while cur <= we:
        chunk_end = min(cur + dt.timedelta(days=chunk_days - 1), we)
        polls, _ = _fetch_chunk(cur, chunk_end, work)
        api_calls += polls
        cur = chunk_end + dt.timedelta(days=1)

    rows, cols = _write(os.path.join(work, "*.csv"), write_mode, so)
    shutil.rmtree(work, ignore_errors=True)
    import lance
    total = lance.dataset(FRESH_URI, storage_options=so).count_rows()
    return rows, cols, total, api_calls


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 6,     # whole-run ceiling: many 1-day chunks × ~8 min each + retries
    memory=32768,
    cpu=4.0,
    retries=modal.Retries(max_retries=3, backoff_coefficient=2.0, initial_delay=30.0),
)
def run_backfill(days: int = 30, chunk_days: int = CHUNK_DAYS, force: bool = False) -> dict:
    """Pull the past `days` (last_modified_date) of contract+IDV AWARD SUMMARIES, chunked small to
    beat the ES ID-enumeration timeout, and CREATE the fresh table (refuses to overwrite unless
    force=True). Verbatim columns, all-VARCHAR. Builds BTREE indices."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    if _dataset_exists(so) and not force:
        raise RuntimeError(
            f"{FRESH_URI} already exists — backfill would OVERWRITE it. Re-run with force=True "
            f"only if you intend to recreate the table from scratch.")

    ws, we = _window(days)
    status, error, rows, cols, total, built, api_calls = "error", None, 0, 0, 0, [], 0
    try:
        rows, cols, total, api_calls = _run_window(days, chunk_days, "overwrite", so)
        if rows == 0:
            raise RuntimeError(f"{days}-day award window [{ws}…{we}] landed 0 rows — hard failure.")
        print("building indices…", flush=True)
        built = _build_indices(so)
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
    timeout=60 * 60 * 3, memory=32768, cpu=4.0,
    retries=modal.Retries(max_retries=3, backoff_coefficient=2.0, initial_delay=30.0),
)
def run_daily(days: int = 10, chunk_days: int = CHUNK_DAYS) -> dict:
    """Trailing-window APPEND top-up (mode='append', NEVER overwrites). Requires the table to exist."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    if not _dataset_exists(so):
        raise RuntimeError(f"{FRESH_URI} does not exist — run backfill first (daily only appends).")

    ws, we = _window(days)
    status, error, rows, cols, total, api_calls = "error", None, 0, 0, 0, 0
    try:
        rows, cols, total, api_calls = _run_window(days, chunk_days, "append", so)
        if rows == 0:
            print("WARN: 0 rows appended for a multi-day window — possible soft block / throttle.", flush=True)
        else:
            _optimize_indices(so)
        status = "success"
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        status = "error"
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(run_mode="daily", window_start=ws, window_end=we, rows_written=int(rows),
                    columns=int(cols), table_rows_after=int(total), api_calls=int(api_calls),
                    write_mode="append", indices_built=[], status=status, error=error,
                    started_at=started_at, completed_at=completed_at)

    return {"feed": FEED, "run_mode": "daily", "window_start": ws.isoformat(),
            "window_end": we.isoformat(), "rows_written": int(rows), "columns": int(cols),
            "table_rows_after": int(total), "api_calls": int(api_calls), "status": status}


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
    """Independent read-back: rows, columns, committed indices, last_modified frontier, and the
    warehouse high-water date (awards/last_updated) for watermark sanity."""
    import duckdb
    import lance
    import requests

    so = _r2_storage_options()
    ds = lance.dataset(FRESH_URI, storage_options=so)
    try:
        idx = ds.list_indices()
    except Exception:  # noqa: BLE001
        idx = getattr(ds, "list_indexes", lambda: [])()
    con = duckdb.connect(":memory:")
    con.register("src", ds.scanner(columns=["last_modified_date"]).to_reader())
    con.execute("CREATE TABLE t AS SELECT * FROM src")
    fr = con.execute("SELECT min(last_modified_date), max(last_modified_date) FROM t").fetchone()
    con.close()
    try:
        warehouse_hw = requests.get(LAST_UPDATED_URL, timeout=30).json().get("last_updated")
    except Exception:  # noqa: BLE001
        warehouse_hw = None
    return {"uri": FRESH_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names),
            "indices": [getattr(i, "name", i.get("name") if isinstance(i, dict) else str(i))
                        for i in idx],
            "min_last_modified": str(fr[0]), "max_last_modified": str(fr[1]),
            "warehouse_last_updated": warehouse_hw}


# ─────────────────────────── local entrypoints ───────────────────────────

@app.local_entrypoint()
def init_ops() -> None:
    from pathlib import Path
    sql = Path(__file__).parent.joinpath("ops_usaspending_api_award_fresh_runs.sql").read_text()
    print(apply_ops_ddl.remote(sql))


@app.local_entrypoint()
def backfill(days: int = 30, chunk_days: int = CHUNK_DAYS, force: bool = False) -> None:
    """Past `days` (default 30) of contract award summaries → create the table (chunked small)."""
    import json
    print(json.dumps(run_backfill.remote(days=days, chunk_days=chunk_days, force=force), indent=2, default=str))


@app.local_entrypoint()
def daily(days: int = 10, chunk_days: int = CHUNK_DAYS) -> None:
    """Trailing-window APPEND top-up (mode=append, never overwrites)."""
    import json
    print(json.dumps(run_daily.remote(days=days, chunk_days=chunk_days), indent=2, default=str))


@app.local_entrypoint()
def verify_table() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
