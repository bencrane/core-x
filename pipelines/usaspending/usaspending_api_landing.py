"""Compute worker — USAspending award_search RAW API LANDING (Lance-native).

The ``usaspending-api-landing`` Modal app. Pulls a fixed TRAILING WINDOW of prime
contract awards from the USAspending ``bulk_download/awards`` API and lands the
VERBATIM result as an immutable, dated raw Lance dataset. Spawned by the Universal
Dispatcher (core/modal_dispatcher.py) or run directly; no web endpoint.

WHAT THIS IS — and is NOT:
  This is NOT a delta and computes NO delta. It is simply "what the API returned for
  the last N days," landed as-is. Overlap across daily runs is EXPECTED and harmless —
  deduplication into a served table is a SEPARATE materialized-view workstream's job,
  not this one. No merge into any SoR, no scalar index on any SoR, no watermark/state:
  the window is a fixed rolling [today - WINDOW_DAYS … today] on ``last_modified_date``.

WHY ``last_modified_date`` (not action_date): USAspending lags days between a contract
action and warehouse landing; the window is on the warehouse modification stamp so
late-landing and re-modified records are captured when they actually appear. DoD prime
detail is published on a deliberate ~90-day delay at FPDS — those records enter the
window as fresh last_modified events when DoD finally publishes; no pull cadence beats
that floor.

WHY bulk_download (not spending_by_award): the paginated search endpoint hard-caps at
10,000 records and would silently truncate a multi-day window. The async server-side
CSV job handles arbitrary volume.

LANDING TIER (separate from the bulk base and the served SoR, by design):
  s3://data-sink/usaspending_api_landings/award_search/pull_date=YYYY-MM-DD/   (this)
  s3://data-sink/active/usaspending/award_search/    (served SoR — untouched here)
  the monthly 175 GB bulk dump lands under its own prefix — also untouched here.

R2-multipart safety: lands at a small max_rows_per_file so each Lance data fragment
stays well under the size at which object_store escalates its multipart part size mid
-upload (which R2 rejects, 400 InvalidPart). Data fragments at this size are the proven
-safe path (the bulk loader writes 1M-row fragments to R2 routinely).

F5 BotDefense: USAspending throttles by source IP; in-script backoff does not recover,
a fresh egress IP does. A persistent 429 raises out of the fetch phase so the
``modal.Retries`` policy recycles the container (= fresh IP).

Data plane (Lance-native, NO Parquet): bulk_download CSV → DuckDB read all-VARCHAR →
lance.write_dataset(small fragments) → ops audit row + Trigger callback.

    modal deploy pipelines/usaspending/usaspending_api_landing.py
    modal run pipelines/usaspending/usaspending_api_landing.py::init_ops
    modal run pipelines/usaspending/usaspending_api_landing.py::run                         # trailing 45d — spawn-fires on the DEPLOYED app, prints fc-id, returns
    modal run pipelines/usaspending/usaspending_api_landing.py::run \
              --window-start 2026-03-01 --window-end 2026-06-04                             # explicit backfill window
    modal run pipelines/usaspending/usaspending_api_landing.py::verify --pull-date 2026-06-04

Launch durability: ``::run`` deploys-if-stale, SPAWNS land_award_search_window on the
DEPLOYED app (ASYNC input — no client tether) and prints the fc-… id; it does NOT
print the result — the worker's ops ledger row + app logs are the record. Follow with
``modal app logs usaspending-api-landing`` or
``python3 -c "import modal; print(modal.FunctionCall.from_id('fc-…').get(timeout=0))"``.
NEVER ``modal run --detach …::run``: --detach detaches the app, not the SYNC input,
which the server cancels ~74–131 s after client loss.
"""

from __future__ import annotations

import os

import modal

# ─────────────────────────── constants ───────────────────────────

FEED = "usaspending_award_search_api_landing"

# Landing tier — durable, dated, immutable; deliberately SEPARATE from the served SoR
# (active/usaspending/award_search) and from the monthly bulk base.
LANDING_BASE_URI = os.environ.get(
    "USASPENDING_API_LANDING_URI",
    "s3://data-sink/usaspending_api_landings/award_search",
).rstrip("/")

# Fixed rolling re-pull. 45 days >> the civilian warehouse lag; overlap across runs is
# harmless (this job does not dedup — the MV workstream does).
WINDOW_DAYS = int(os.environ.get("USASPENDING_API_LANDING_WINDOW_DAYS", "45"))

# USAspending public API (no key; rate-limited by source IP — see F5 note).
BULK_DL_URL = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
PRIME_CONTRACT_TYPES = ["A", "B", "C", "D"]   # definitive / purchase order / delivery / BPA-call
DATE_TYPE = "last_modified_date"
BULK_POLL_SECONDS = 15                         # async job poll interval
BULK_POLL_CEILING_SECONDS = 60 * 60            # 60 min hard ceiling on the async job

DATA_STORAGE_VERSION = "2.1"
# Small fragments → uniform multipart parts → R2-safe (see module docstring).
LANDING_MAX_ROWS_PER_FILE = 250_000
SCRATCH_DIR = "/tmp"

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",          # provides `import lance`
    "pyarrow>=17",
    "requests>=2.32",      # USAspending API + Trigger callback
    "psycopg[binary]>=3.2",  # ops.* audit ledger
)

app = modal.App("usaspending-api-landing", image=image)


# ─────────────────────────── R2 plumbing ───────────────────────────

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


# ─────────────────────────── trailing window ───────────────────────────

def _today_utc():
    import datetime as dt
    return dt.datetime.now(dt.timezone.utc).date()


def _window(window_start, window_end):
    """Fixed rolling window. Explicit args win (manual backfill); otherwise
    [today - WINDOW_DAYS … today] on last_modified_date."""
    import datetime as dt

    def _d(s):
        return dt.date.fromisoformat(s) if isinstance(s, str) else s

    we = _d(window_end) if window_end else _today_utc()
    ws = _d(window_start) if window_start else (we - dt.timedelta(days=WINDOW_DAYS))
    return ws, we


# ─────────────────────────── fetch (bulk_download/awards) ───────────────────────────

class _ThrottledError(RuntimeError):
    """Persistent 429 — fail fast so modal.Retries recycles the container (fresh IP)."""


def _fetch_window(window_start, window_end):
    """Async bulk_download/awards over the window. Returns (csv_glob, poll_count).
    Raises on a failed job or the poll ceiling (retryable → fresh Modal container)."""
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
    print(f"bulk_download job submitted: {job.get('file_name')}")

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

    work = os.path.join(SCRATCH_DIR, "api_pull")
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
    print(f"downloaded {len(members)} CSV member(s): {members}")
    return os.path.join(work, "*.csv"), polls


# ─────────────────────────── land (verbatim → Lance) ───────────────────────────

def _land(csv_glob, pull_date, so) -> tuple[str, int, int]:
    """Read the bulk CSV(s) all-VARCHAR and land them VERBATIM as a dated raw Lance
    dataset (immutable; one per pull_date; re-run overwrites that date). Returns
    (uri, rows, columns). NO projection, NO dedup — exactly what the API returned."""
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

    uri = f"{LANDING_BASE_URI}/pull_date={pull_date}/"
    lance.write_dataset(
        tbl, uri, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=LANDING_MAX_ROWS_PER_FILE,
        storage_options=so,
    )
    print(f"landed (Lance): {uri} ({tbl.num_rows:,} rows × {len(tbl.column_names)} cols)")
    return uri, tbl.num_rows, len(tbl.column_names)


# ─────────────────────────── ops ledger (audit) + callback ───────────────────────────

def _record_run(*, pull_date, window_start, window_end, rows_landed, columns_landed,
                api_calls, landing_uri, status, error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    # A non-success row must NEVER carry a NULL error_message (the lesson from the
    # prior delta worker, where a BaseException wrote a silent NULL).
    if status != "success" and not error:
        error = "unknown terminal failure (no exception captured)"
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.usaspending_award_search_api_landing_runs
                    (feed, pull_date, window_start, window_end, rows_landed, columns_landed,
                     api_calls, landing_uri, status, error_message, started_at, executed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, pull_date, window_start, window_end, rows_landed, columns_landed,
                 api_calls, landing_uri, status, (error or "")[:2000] or None,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the flat terminal payload to the Trigger waitpoint url (no API key,
    no envelope; the whole body becomes result.output)."""
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

def _run_landing(window_start, window_end, trigger_callback_url) -> dict:
    """FETCH (retryable; bare-raise → modal.Retries recycles for a fresh IP) then LAND
    (terminal: records the ops audit row + posts the callback). The terminal handler
    catches BaseException so a Modal graceful-shutdown signal cannot write a silent
    NULL-error row; SystemExit/KeyboardInterrupt are recorded AND re-raised so Modal
    still sees the kill."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    ws, we = _window(window_start, window_end)
    pull_date = we
    print(f"[api-landing] last_modified_date ∈ [{ws} … {we}]  (window={WINDOW_DAYS}d, pull_date={pull_date})")

    # ── FETCH (retryable) ──
    csv_glob, api_calls = _fetch_window(ws, we)

    # ── LAND (terminal) ──
    status, error, rows_landed, cols_landed, landing_uri = "error", None, 0, 0, None
    try:
        so = _r2_storage_options()
        landing_uri, rows_landed, cols_landed = _land(csv_glob, pull_date, so)
        if rows_landed == 0:
            raise RuntimeError(
                f"window [{ws}…{we}] landed 0 rows — a {WINDOW_DAYS}-day federal window "
                f"is never empty; treating as a hard API/parse failure.")
        status = "success"
    except (SystemExit, KeyboardInterrupt) as exc:
        error = f"{type(exc).__name__}: {exc}".strip() or f"{type(exc).__name__} (no message)"
        status = "error"
        raise
    except BaseException as exc:  # noqa: BLE001 — catch Exception AND non-exit BaseExceptions
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(pull_date=pull_date, window_start=ws, window_end=we,
                    rows_landed=int(rows_landed), columns_landed=int(cols_landed),
                    api_calls=int(api_calls), landing_uri=landing_uri, status=status,
                    error=error, started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url, {
            "status": "success" if status == "success" else "error",
            "rows": int(rows_landed), "feed": FEED, "landing_uri": landing_uri,
            "pull_date": pull_date.isoformat(), "window_start": ws.isoformat(),
            "window_end": we.isoformat(), "api_calls": int(api_calls),
            "message": error if status != "success" else None})

    return {"feed": FEED, "rows_landed": int(rows_landed), "columns_landed": int(cols_landed),
            "api_calls": int(api_calls), "status": status, "landing_uri": landing_uri,
            "pull_date": pull_date.isoformat(), "window_start": ws.isoformat(),
            "window_end": we.isoformat(), "error": error}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,
    memory=16384,
    cpu=4.0,
    # F5 BotDefense: a persistent 429 raises from the fetch phase so each retry is a
    # fresh Modal container = fresh egress IP. (initial_delay 30s, ×2 backoff.)
    retries=modal.Retries(max_retries=5, backoff_coefficient=2.0, initial_delay=30.0),
)
def land_award_search_window(
    window_start: str | None = None,
    window_end: str | None = None,
    trigger_callback_url: str | None = None,
) -> dict:
    """Pull the trailing window (or an explicit one) and land it verbatim as raw Lance."""
    return _run_landing(window_start, window_end, trigger_callback_url)


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


@app.function(timeout=60)
def next_window() -> dict:
    """Report the trailing window the next run would pull (no watermark)."""
    ws, we = _window(None, None)
    return {"window_days": WINDOW_DAYS, "window_start": ws.isoformat(), "window_end": we.isoformat()}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=300)
def verify(pull_date: str | None = None) -> dict:
    """Read-back proof: open a landed pull_date dataset and report rows/cols + the
    last_modified_date frontier it contains — independent of the write."""
    import datetime as dt

    import duckdb
    import lance

    so = _r2_storage_options()
    pd = pull_date or _today_utc().isoformat()
    uri = f"{LANDING_BASE_URI}/pull_date={pd}/"
    ds = lance.dataset(uri, storage_options=so)
    con = duckdb.connect(":memory:")
    con.register("src", ds.scanner(columns=["last_modified_date", "action_date"]).to_reader())
    con.execute("CREATE TABLE t AS SELECT * FROM src")
    frontier = con.execute(
        "SELECT max(TRY_CAST(last_modified_date AS DATE)), max(TRY_CAST(action_date AS DATE)) FROM t"
    ).fetchone()
    con.close()
    return {"uri": uri, "rows": ds.count_rows(), "columns": len(ds.schema),
            "max_last_modified_date": str(frontier[0]), "max_action_date": str(frontier[1])}


# ─────────────────────────── local entrypoints ───────────────────────────

@app.local_entrypoint()
def init_ops() -> None:
    """Create ops.usaspending_award_search_api_landing_runs from the co-located DDL file."""
    from pathlib import Path

    sql = Path(__file__).parent.joinpath(
        "ops_usaspending_award_search_api_landing_runs.sql").read_text()
    print(apply_ops_ddl.remote(sql))


@app.local_entrypoint()
def run(window_start: str = "", window_end: str = "") -> None:
    """Manual run (no Trigger callback). Defaults to the trailing window. Spawns on
    the DEPLOYED app and returns immediately — the fc-id is the durable handle; the
    worker's ledger row (ops.usaspending_award_search_api_landing_runs) and app logs
    are the record, not this entrypoint's stdout."""
    from pipelines._shared.launch import spawn_deployed

    spawn_deployed(
        "usaspending-api-landing", "land_award_search_window", deploy_file=__file__,
        window_start=window_start or None, window_end=window_end or None,
        trigger_callback_url=None)


@app.local_entrypoint()
def show_window() -> None:
    import json
    print(json.dumps(next_window.remote(), indent=2, default=str))
