"""USAspending API SUBAWARD FRESH — standalone, accumulating, append-only procurement
subaward table. CHUNKED. Self-contained (uv-run, no Modal).

The subaward counterpart of ``usaspending_api_fresh.py`` (prime contract+IDV). Pulls
PROCUREMENT subawards from ``bulk_download/awards`` on a ``last_modified_date`` window and
lands them VERBATIM — the exact API download column names, all-VARCHAR, no projection, no
renaming — into ONE standalone Lance table:

    s3://data-sink/active/usaspending_api_fresh/contract_subaward/

APPEND-ONLY. ACCUMULATING. THE TABLE ONLY GROWS.
  • backfill — wide window (default 90d) → CHUNKED, write the table (first create, overwrite).
  • daily    — trailing window (default 14d) → CHUNKED, mode="append". Never overwrites.

═══ WHY CHUNKED (hard-won, do not "simplify" back to a single shot) ═══
USAspending's subaward export runs on the ``elasticsearch_sub_awards`` backend, which is
~100–300× slower than the prime (``prime_awards``) path and has a generation-time ceiling:
a single 90-day procurement-subaward job runs for HOURS and then FAILS (verified repeatedly —
3–5h, status flips to "failed", zero bytes written). Small windows complete fine (a 2-day
job: ~10s–10m). So the backfill MUST split the window into chunks small enough to finish and
assemble them locally. Row density is wildly uneven by last_modified (some 3-day windows have
~50 rows, others >40k and take 45m+), so dense chunks may themselves need sub-splitting; the
backfill tracks any window that never completes as an explicit GAP rather than hanging.

Disjoint chunk date ranges ⇒ no duplicate rows ⇒ one clean combined overwrite on create.
(The daily append path DOES re-pull overlapping recent days on purpose — FFATA/FSRS publish
lag — so daily appends are intentionally duplicative and reconciled downstream, not here.)

WHY last_modified_date: captures late-landing / re-modified subawards when they appear in the
warehouse (action_date would miss them — heavy FFATA/FSRS lag). Valid date_type for
sub_award_types (verified against the upstream API contract + live).

WHY procurement only: contract subawards (``Contracts_Subawards``, 118 verbatim cols) and
grant subawards (``Assistance_Subawards``, different schema) are DISTINCT shapes. Procurement
is the faithful subaward counterpart of the contract-prime table; grants get a separate table.

NOTE the subaward file carries NO product_or_service_code, NO cage_code, and NO plain
last_modified_date column — its modification frontier is subaward_sam_report_last_modified_date,
and NAICS is only the prime award's (prime_award_naics_code).

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'requests>=2.32' --with 'psycopg[binary]>=3.2' \
      python3 pipelines/usaspending/usaspending_api_subaward_fresh.py <init_ops|backfill|daily|verify> [days] [chunk_days]
"""
from __future__ import annotations

import datetime as dt
import os
import shutil
import sys
import time
import zipfile

import requests

# ─────────────────────────── constants ───────────────────────────

FEED = "usaspending_api_fresh_contract_subaward"
FRESH_URI = os.environ.get(
    "USASPENDING_API_SUBAWARD_FRESH_URI",
    "s3://data-sink/active/usaspending_api_fresh/contract_subaward",
).rstrip("/") + "/"

BULK_DL_URL = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
STATUS_URL = "https://api.usaspending.gov/api/v2/download/status?file_name="
FILE_URL = "https://files.usaspending.gov/generated_downloads/"
SUB_AWARD_TYPES = ["procurement"]
DATE_TYPE = "last_modified_date"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 250_000
SCRATCH = "/tmp/usasp_subaward_fresh"

# Chunking. 10-day default keeps most chunks completable; MAX_INFLIGHT throttles concurrent
# generations (high concurrency demonstrably slows the elasticsearch_sub_awards backend).
DEFAULT_BACKFILL_DAYS = 90
DEFAULT_DAILY_DAYS = 14
DEFAULT_CHUNK_DAYS = 10
MAX_INFLIGHT = 4
POLL_SECONDS = 30
CHUNK_CAP_SECONDS = int(os.environ.get("USASPENDING_SUBAWARD_CHUNK_CAP", str(90 * 60)))
RUN_CAP_SECONDS = int(os.environ.get("USASPENDING_SUBAWARD_RUN_CAP", str(6 * 3600)))
# Zombie detection: a `running` job whose API-reported seconds_elapsed exceeds a width-keyed
# budget is treated as stuck. rows/cols stay 0 for the entire healthy generation, so elapsed
# is the only viable signal. Budgets are kept ABOVE the worst observed healthy time at every
# width (1d healthy <=10m; dense 15d observed ~46m) so a slow-but-healthy job is never killed.
# A deduped resubmit reports the OLD stuck job's large elapsed — which correctly trips this.
ZOMBIE_BASE_SECONDS = int(os.environ.get("USASPENDING_SUBAWARD_ZOMBIE_BASE", str(20 * 60)))
ZOMBIE_PER_DAY_SECONDS = int(os.environ.get("USASPENDING_SUBAWARD_ZOMBIE_PER_DAY", str(6 * 60)))

INDEX_COLS = [
    "prime_award_unique_key", "subaward_number", "subawardee_uei", "prime_awardee_uei",
    "prime_award_piid", "prime_award_naics_code", "subaward_action_date",
    "subaward_sam_report_last_modified_date", "subaward_amount",
]


def log(m): print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


# ─────────────────────────── R2 / creds ───────────────────────────

def _r2_so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _dataset_exists(so) -> bool:
    import lance
    try:
        lance.dataset(FRESH_URI, storage_options=so); return True
    except Exception:  # noqa: BLE001
        return False


# ─────────────────────────── USAspending bulk_download ───────────────────────────

def _status(fn):
    try:
        return requests.get(STATUS_URL + fn, timeout=(30, 120)).json()
    except Exception:  # noqa: BLE001 — transient → caller keeps polling
        return {"status": "_transient"}


def _submit(ws, we) -> str:
    payload = {"filters": {"sub_award_types": SUB_AWARD_TYPES, "date_type": DATE_TYPE,
                           "date_range": {"start_date": ws.isoformat(), "end_date": we.isoformat()}},
               "file_format": "csv"}
    for a in range(6):
        r = requests.post(BULK_DL_URL, json=payload, timeout=(30, 120))
        if r.status_code == 429:
            time.sleep(10 * (a + 1)); continue
        r.raise_for_status()
        return r.json()["file_name"]
    raise RuntimeError(f"bulk_download submit throttled for [{ws}..{we}]")


def _download(fn, dest, tries=6) -> bool:
    """Retry-hardened download — a transient DNS/network blip must not nuke an assembly."""
    for a in range(tries):
        try:
            with requests.get(FILE_URL + fn, stream=True, timeout=(30, 1800)) as dl:
                dl.raise_for_status()
                with open(dest, "wb") as fh:
                    for ch in dl.iter_content(8 * 1024 * 1024):
                        fh.write(ch)
            return True
        except Exception as e:  # noqa: BLE001
            log(f"  download {fn} attempt {a+1} failed ({type(e).__name__}); retry")
            time.sleep(20 * (a + 1))
    return False


def _chunk_windows(start, end, chunk_days):
    out, s = [], start
    while s <= end:
        e = min(s + dt.timedelta(days=chunk_days - 1), end)
        out.append((s, e)); s = e + dt.timedelta(days=1)
    return out


def _zombie_budget(ws, we) -> int:
    """Max generation seconds a healthy `running` job of this window width may take before it
    is declared a zombie. Inclusive width in days; super-linear allowance per fact: width
    drives generation time."""
    days = (we - ws).days + 1
    return ZOMBIE_BASE_SECONDS + ZOMBIE_PER_DAY_SECONDS * (days - 1)


def _split_window(ws, we):
    """Split [ws..we] into two DISJOINT, contiguous sub-windows that exactly cover it (fresh
    request signatures ⇒ escape USAspending dedup; disjoint ⇒ no duplicate rows). Returns []
    at the 1-day floor — a single day cannot be split, so a still-zombie 1-day window is a
    bounded GAP (poison day)."""
    days = (we - ws).days + 1
    if days <= 1:
        return []
    mid = ws + dt.timedelta(days=days // 2 - 1)
    return [(ws, mid), (mid + dt.timedelta(days=1), we)]


def _run_chunks(windows, workdir):
    """Submit/poll all windows with an in-flight cap; download finished ones (retry-hardened).
    Returns (n_downloaded, polls, gaps:list[str]).

    Zombie escape: a `running` job whose seconds_elapsed exceeds _zombie_budget (or that blows
    CHUNK_CAP_SECONDS) is abandoned and its window is split into two disjoint sub-windows pushed
    back as FRESH requests (different signature ⇒ escapes USAspending dedup; disjoint ⇒ no dup;
    smaller ⇒ less likely to zombie). A window that still zombies at the 1-day floor is a bounded
    GAP (poison day). `tried` prevents resubmitting a signature; RUN_CAP_SECONDS bounds the run."""
    shutil.rmtree(workdir, ignore_errors=True); os.makedirs(workdir, exist_ok=True)
    pending = list(windows)
    active, done, gaps = [], 0, []   # active: dicts {ws,we,fn,t0,resubs}
    tried = set()                    # (ws,we) signatures already submitted — never resubmit
    polls = 0
    run_t0 = time.time()
    while pending or active:
        while pending and len(active) < MAX_INFLIGHT:
            ws, we = pending.pop(0)
            if (ws, we) in tried:    # defensive: never re-submit a tried signature (would dedup)
                gaps.append(f"[{ws}..{we}]"); log(f"[{ws}..{we}] already tried -> GAP (skip)"); continue
            tried.add((ws, we))
            fn = _submit(ws, we)
            active.append({"ws": ws, "we": we, "fn": fn, "t0": time.time(), "resubs": 0})
            log(f"submitted [{ws}..{we}] -> {fn}")
            time.sleep(2)
        time.sleep(POLL_SECONDS)
        still = []
        for j in active:
            st = _status(j["fn"]); s = st.get("status"); polls += 1
            if s == "finished":
                d = os.path.join(workdir, f"c_{done}"); os.makedirs(d, exist_ok=True)
                if _download(j["fn"], os.path.join(d, "c.zip")):
                    with zipfile.ZipFile(os.path.join(d, "c.zip")) as zf:
                        mem = [m for m in zf.namelist()
                               if m.lower().endswith(".csv") and "subaward" in m.lower()]
                        zf.extractall(d, members=mem)
                    done += 1
                    log(f"[{j['ws']}..{j['we']}] FINISHED rows={st.get('total_rows')} "
                        f"sec={st.get('seconds_elapsed')} -> {mem}")
                else:
                    gaps.append(f"[{j['ws']}..{j['we']}]")
                    log(f"[{j['ws']}..{j['we']}] download failed -> GAP")
            elif s in ("failed", "expired"):
                if j["resubs"] < 1:
                    j["resubs"] += 1; j["fn"] = _submit(j["ws"], j["we"]); j["t0"] = time.time()
                    still.append(j); log(f"[{j['ws']}..{j['we']}] {s} -> resubmitted")
                else:
                    gaps.append(f"[{j['ws']}..{j['we']}]"); log(f"[{j['ws']}..{j['we']}] {s} twice -> GAP")
            else:
                # running / _transient / status=None / cap exceeded → zombie check.
                # elapsed: prefer the API's own generation clock; fall back to local wall-clock
                # when seconds_elapsed is absent (e.g. a `detail`-only unknown-file response).
                elapsed = st.get("seconds_elapsed") or (time.time() - j["t0"])
                hard_cap = (time.time() - j["t0"]) > CHUNK_CAP_SECONDS
                if float(elapsed) > _zombie_budget(j["ws"], j["we"]) or hard_cap:
                    halves = _split_window(j["ws"], j["we"])
                    if halves:
                        pending.extend(halves)   # fresh disjoint signatures; this job is abandoned
                        log(f"[{j['ws']}..{j['we']}] ZOMBIE (elapsed={elapsed}) -> abandon, split {halves}")
                    else:
                        gaps.append(f"[{j['ws']}..{j['we']}]")  # 1-day floor → poison day
                        log(f"[{j['ws']}..{j['we']}] ZOMBIE at 1-day floor -> GAP (poison day)")
                else:
                    still.append(j)
        active = still
        if time.time() - run_t0 > RUN_CAP_SECONDS:
            for j in active:
                gaps.append(f"[{j['ws']}..{j['we']}]")
            for ws, we in pending:
                gaps.append(f"[{ws}..{we}]")
            log(f"RUN CAP {RUN_CAP_SECONDS}s hit; {len(active) + len(pending)} window(s) -> GAP")
            break
    return done, polls, gaps


# ─────────────────────────── write / index / ops ───────────────────────────

def _combine_write(workdir, mode, so):
    import duckdb
    import lance
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=4;")
    os.makedirs(os.path.join(workdir, "spill"), exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{workdir}/spill';")
    glob = os.path.join(workdir, "c_*", "*.csv")
    tbl = con.sql(f"SELECT * FROM read_csv('{glob}', all_varchar=true, header=true, "
                  f"sample_size=-1, ignore_errors=false, union_by_name=true)").to_arrow_table()
    con.close()
    rows, cols = tbl.num_rows, len(tbl.column_names)
    if rows == 0:
        return 0, cols
    lance.write_dataset(tbl, FRESH_URI, mode=mode, data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
    log(f"wrote (mode={mode}): {rows:,} rows × {cols} cols → {FRESH_URI}")
    return rows, cols


def _build_indices(so, rebuild=False):
    import lance
    ds = lance.dataset(FRESH_URI, storage_options=so)
    present = set(ds.schema.names); built = []
    for c in INDEX_COLS:
        if c not in present:
            log(f"  SKIP (absent) {c}"); continue
        try:
            ds.create_scalar_index(c, index_type="BTREE", replace=True) if rebuild \
                else ds.create_scalar_index(c, index_type="BTREE")
        except TypeError:
            ds.create_scalar_index(c, index_type="BTREE")
        built.append(c); log(f"  BTREE {c}")
    return built


def _optimize_indices(so):
    import lance
    ds = lance.dataset(FRESH_URI, storage_options=so)
    try:
        ds.optimize.optimize_indices(); log("optimize_indices: extended over appended fragments")
    except Exception as e:  # noqa: BLE001
        log(f"optimize_indices unavailable ({e}); rebuilding"); _build_indices(so, rebuild=True)


def _record_run(*, run_mode, ws, we, rows, cols, total, polls, write_mode, indices, status,
                error, started, completed):
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL")
    if not dsn:
        log("WARN: no HQX dsn; skipping ops row"); return
    if status != "success" and not error:
        error = "unknown terminal failure"
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute(
                """INSERT INTO ops.usaspending_api_subaward_fresh_runs
                   (feed,run_mode,window_start,window_end,rows_written,columns,table_rows_after,
                    api_calls,write_mode,indices_built,status,error_message,started_at,executed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, run_mode, ws, we, int(rows), int(cols), int(total), int(polls), write_mode,
                 ",".join(indices) if indices else None, status, (error or "")[:2000] or None,
                 started, completed))
            c.commit()
        log(f"ops row: status={status}")
    except Exception as e:  # noqa: BLE001
        log(f"WARN: ops write failed: {e}")


# ─────────────────────────── entrypoints ───────────────────────────

def backfill(days=DEFAULT_BACKFILL_DAYS, chunk_days=DEFAULT_CHUNK_DAYS, force=False):
    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    if _dataset_exists(so) and not force:
        raise RuntimeError(f"{FRESH_URI} exists — refusing to overwrite. Pass force=1 to recreate.")
    we = dt.datetime.now(dt.timezone.utc).date()
    ws = we - dt.timedelta(days=days)
    windows = _chunk_windows(ws, we, chunk_days)
    log(f"[backfill] last_modified [{ws}..{we}] {days}d → {len(windows)} chunks of {chunk_days}d")
    status, error, rows, cols, total, built = "error", None, 0, 0, 0, []
    polls = 0
    try:
        n, polls, gaps = _run_chunks(windows, SCRATCH)
        if n == 0:
            raise RuntimeError("no chunk finished")
        rows, cols = _combine_write(SCRATCH, "overwrite", so)
        if rows == 0:
            raise RuntimeError("0 rows across finished chunks")
        built = _build_indices(so)
        import lance
        total = lance.dataset(FRESH_URI, storage_options=so).count_rows()
        status = "success"
        error = ("GAPS: " + " ".join(gaps)) if gaps else None
        log(f"DONE rows={total:,} cols={cols} gaps={gaps or 'none'} "
            f"subawardee_uei_indexed={'subawardee_uei' in built}")
    finally:
        _record_run(run_mode="backfill", ws=ws, we=we, rows=rows, cols=cols, total=total,
                    polls=polls, write_mode="overwrite", indices=built, status=status,
                    error=error, started=started, completed=dt.datetime.now(dt.timezone.utc))


def daily(days=DEFAULT_DAILY_DAYS, chunk_days=DEFAULT_CHUNK_DAYS):
    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    if not _dataset_exists(so):
        raise RuntimeError(f"{FRESH_URI} does not exist — run backfill first.")
    we = dt.datetime.now(dt.timezone.utc).date()
    ws = we - dt.timedelta(days=days)
    windows = _chunk_windows(ws, we, chunk_days)
    log(f"[daily] last_modified [{ws}..{we}] {days}d → {len(windows)} chunks (append)")
    status, error, rows, cols, total = "error", None, 0, 0, 0
    polls = 0
    try:
        n, polls, gaps = _run_chunks(windows, SCRATCH)
        if n == 0:
            raise RuntimeError("no chunk finished")
        rows, cols = _combine_write(SCRATCH, "append", so)
        if rows > 0:
            _optimize_indices(so)
        import lance
        total = lance.dataset(FRESH_URI, storage_options=so).count_rows()
        status = "success"
        error = ("GAPS: " + " ".join(gaps)) if gaps else None
        log(f"DONE appended={rows:,} table_total={total:,} gaps={gaps or 'none'}")
    finally:
        _record_run(run_mode="daily", ws=ws, we=we, rows=rows, cols=cols, total=total,
                    polls=polls, write_mode="append", indices=INDEX_COLS, status=status,
                    error=error, started=started, completed=dt.datetime.now(dt.timezone.utc))


def verify():
    import duckdb
    import lance
    so = _r2_so()
    ds = lance.dataset(FRESH_URI, storage_options=so)
    try:
        idx = [i.get('name') if isinstance(i, dict) else getattr(i, 'name', str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    con = duckdb.connect(":memory:")
    con.register("s", ds.scanner(columns=["subawardee_uei", "subaward_action_date",
                                          "subaward_sam_report_last_modified_date"]).to_reader())
    con.execute("CREATE TABLE t AS SELECT * FROM s")
    fr = con.execute("SELECT count(*), count(DISTINCT subawardee_uei), "
                     "min(subaward_action_date), max(subaward_action_date), "
                     "min(subaward_sam_report_last_modified_date), "
                     "max(subaward_sam_report_last_modified_date) FROM t").fetchone()
    con.close()
    import json
    print(json.dumps({"uri": FRESH_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names),
                      "indices": idx, "distinct_subawardee_uei": fr[1],
                      "min_action_date": str(fr[2]), "max_action_date": str(fr[3]),
                      "min_sam_last_modified": str(fr[4]), "max_sam_last_modified": str(fr[5])},
                     indent=2, default=str))


def init_ops():
    import psycopg
    from pathlib import Path
    sql = Path(__file__).parent.joinpath("ops_usaspending_api_subaward_fresh_runs.sql").read_text()
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as c, c.cursor() as cur:
        cur.execute(sql); c.commit()
    log("ops DDL applied")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    a2 = int(sys.argv[2]) if len(sys.argv) > 2 else None
    a3 = int(sys.argv[3]) if len(sys.argv) > 3 else None
    if cmd == "backfill":
        backfill(days=a2 or DEFAULT_BACKFILL_DAYS, chunk_days=a3 or DEFAULT_CHUNK_DAYS,
                 force=os.environ.get("FORCE") == "1")
    elif cmd == "daily":
        daily(days=a2 or DEFAULT_DAILY_DAYS, chunk_days=a3 or DEFAULT_CHUNK_DAYS)
    elif cmd == "verify":
        verify()
    elif cmd == "init_ops":
        init_ops()
    else:
        print(f"unknown command: {cmd} (init_ops|backfill|daily|verify)"); sys.exit(2)


if __name__ == "__main__":
    main()
