#!/usr/bin/env python3
"""LOCAL end-to-end runner for the USAspending download/awards AWARD-summary fresh pull.

Runs the whole thing on the operator's machine — NO Modal — because the workload is ~99% idle
retry-waiting on a flaky server-side async queue (USASpending's ES ID-enumeration scroll times out
at 90s under load; failures are fast + clean `status:"failed"`, never the old silent hang).

Architecture: DECOUPLE the flaky fetch from the reliable write.
  • fetch  — concurrent, resumable, retry-heavy. Each 1-day chunk's Contracts_PrimeAwardSummaries CSV
             is persisted to durable local disk; manifest.json records every landed window. A re-run
             skips done windows → the fetch is fully resumable, nothing is ever lost.
  • write  — ONE DuckDB(all_varchar, union_by_name) → lance.write_dataset(overwrite) → R2 → 11 BTREE.
             Deterministic and reliable; no all-or-nothing multi-hour exposure.

The payload is the CORRECT download/awards AdvancedFilterObject (award_type_codes + time_period[]),
NOT the bulk_download schema that hung 3× before. An echo guard refuses to poll any job whose
time_period the server dropped.

Run under doppler (R2 + Postgres creds):
    doppler run -p core-x -c prd -- python3 pipelines/usaspending/usaspending_award_fresh_local.py <mode>
Modes: smoketest | fetch | write | verify | status
"""
from __future__ import annotations
import csv, datetime as dt, glob as globmod, io, json, os, sys, threading, time, urllib.error, urllib.request, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

# ───────────────────────── config ─────────────────────────
DOWNLOAD_URL = "https://api.usaspending.gov/api/v2/download/awards/"
AWARD_TYPE_CODES = ["A", "B", "C", "D", "IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
DATE_TYPE = "last_modified_date"
PAS_MEMBER = "Contracts_PrimeAwardSummaries"

PROD_URI = "s3://data-sink/active/usaspending_api_fresh/contract_prime_award/"
TEST_URI = "s3://data-sink/active/usaspending_api_fresh/_smoketest_contract_prime_award/"
FEED = "usaspending_api_fresh_contract_prime_award"

SP = os.environ.get("AWARD_SCRATCH",
                    "/private/tmp/claude-501/-Users-benjamincrane-core-x/565aef7e-2343-4edb-bb16-eb8ed129681e/scratchpad")
WORK = os.path.join(SP, "award_fetch_csvs")
MANIFEST = os.path.join(SP, "award_fetch_manifest.json")

WINDOW_DAYS = int(os.environ.get("AWARD_WINDOW_DAYS", "40"))
CONCURRENCY = int(os.environ.get("AWARD_CONCURRENCY", "4"))
MAX_ATTEMPTS = int(os.environ.get("AWARD_MAX_ATTEMPTS", "15"))
POLL_SECONDS = 12
POLL_CEILING = 12 * 60          # per-attempt ceiling; success seen ~7.4 min, failures ~20-160s
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 250_000
CAP_GUARD = 490_000             # download/awards limit is 500k/job — defensive tripwire only

INDEX_COLS = [
    "contract_award_unique_key", "award_id_piid", "recipient_uei", "recipient_name",
    "naics_code", "product_or_service_code", "cage_code",
    "award_base_action_date", "award_latest_action_date", "last_modified_date",
    "total_obligated_amount",
]

_manifest_lock = threading.Lock()
_log_lock = threading.Lock()


def log(msg: str):
    with _log_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ───────────────────────── R2 ─────────────────────────
def so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com" if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("R2_ENDPOINT / R2_ACCOUNT_ID missing (run under `doppler run -p core-x -c prd`).")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


# ───────────────────────── fetch ─────────────────────────
def _post(url, payload):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=90) as r:
        return r.status, json.loads(r.read())


def _get(url):
    with urllib.request.urlopen(url, timeout=90) as r:
        return json.loads(r.read())


# Global submit throttle: serialize POSTs to ≥SUBMIT_MIN_INTERVAL apart across ALL workers so
# concurrency drives parallel polling/jobs without bursting the submit endpoint into 5xx/429.
_submit_lock = threading.Lock()
_last_submit = [0.0]
SUBMIT_MIN_INTERVAL = float(os.environ.get("AWARD_SUBMIT_INTERVAL", "1.5"))


def _throttled_submit(payload):
    with _submit_lock:
        wait = SUBMIT_MIN_INTERVAL - (time.time() - _last_submit[0])
        if wait > 0:
            time.sleep(wait)
        _last_submit[0] = time.time()
    return _post(DOWNLOAD_URL, payload)


def build_payload(ws: dt.date, we: dt.date) -> dict:
    return {"filters": {"award_type_codes": AWARD_TYPE_CODES,
                        "time_period": [{"start_date": ws.isoformat(), "end_date": we.isoformat(),
                                         "date_type": DATE_TYPE}]},
            "file_format": "csv"}


def _fetch_once(ws: dt.date, we: dt.date, workdir: str, tag: str):
    """One download/awards job. Returns (rows, [saved_csv_paths]) on finished; raises on transient
    (failed/ceiling) so the caller retries. rows may be 0 (empty window → header-only/absent member)."""
    payload = build_payload(ws, we)
    try:
        st, job = _throttled_submit(payload)
    except urllib.error.HTTPError as e:
        body = e.read()[:120].decode(errors="replace").replace("\n", " ")
        if e.code == 429 or e.code >= 500:   # throttle / gateway / server error → retryable
            raise _Transient(f"submit {e.code} (retryable): {body}")
        raise RuntimeError(f"submit {e.code}: {body}")   # 4xx (bad request) → genuinely hard
    echoed = (job.get("download_request") or {}).get("filters") or {}
    if "time_period" not in echoed:
        raise RuntimeError(f"echo dropped time_period (unbounded job) — schema wrong. keys={list(echoed)}")
    status_url, file_url = job["status_url"], job["file_url"]

    deadline = time.time() + POLL_CEILING
    total_rows = 0
    while time.time() < deadline:
        time.sleep(POLL_SECONDS)
        try:
            s = _get(status_url)
        except Exception as exc:  # noqa: BLE001
            continue
        status = s.get("status")
        total_rows = s.get("total_rows") or 0
        if status == "finished":
            break
        if status == "failed":
            raise _Transient(f"job failed: {str(s.get('message',''))[:120]}")
    else:
        raise _Transient(f"ceiling {POLL_CEILING}s")

    if total_rows >= CAP_GUARD:
        raise RuntimeError(f"total_rows={total_rows} ≥ {CAP_GUARD} — window too wide (unexpected at 1-day)")

    src = file_url if file_url.startswith("http") else "https://api.usaspending.gov" + file_url
    zpath = os.path.join(workdir, f"_{tag}.zip")
    try:
        with urllib.request.urlopen(src, timeout=600) as dl, open(zpath, "wb") as fh:
            while True:
                b = dl.read(8 * 1024 * 1024)
                if not b:
                    break
                fh.write(b)
        saved = []
        with zipfile.ZipFile(zpath) as zf:
            members = [m for m in zf.namelist() if PAS_MEMBER in m and m.lower().endswith(".csv")]
            for i, m in enumerate(members):
                dest = os.path.join(workdir, f"pas_{tag}_{i}.csv")
                with zf.open(m) as fsrc, open(dest, "wb") as fdst:
                    fdst.write(fsrc.read())
                saved.append(dest)
    except Exception as exc:  # noqa: BLE001 — download/unzip hiccup is retryable
        raise _Transient(f"download/extract: {exc}")
    finally:
        if os.path.exists(zpath):
            os.remove(zpath)
    return total_rows, saved


class _Transient(RuntimeError):
    pass


def fetch_window(ws: dt.date, we: dt.date):
    """Retry wrapper for one window. Returns dict manifest entry."""
    tag = ws.isoformat() if ws == we else f"{ws.isoformat()}_{we.isoformat()}"
    last = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            rows, saved = _fetch_once(ws, we, WORK, tag)
            log(f"  ✓ {tag}: {rows} rows, {len(saved)} file(s) [attempt {attempt}]")
            return {"window": tag, "status": "done", "rows": rows, "files": saved, "attempts": attempt}
        except _Transient as exc:
            last = str(exc)
            # short backoff: the global submit throttle governs submit rate, so cycle back fast
            if attempt < MAX_ATTEMPTS:
                time.sleep(min(5 * attempt, 20))
        except RuntimeError as exc:
            log(f"  ✗ {tag}: HARD error: {exc}")
            return {"window": tag, "status": "hard_error", "rows": 0, "files": [], "attempts": attempt, "error": str(exc)}
        except Exception as exc:  # noqa: BLE001 — never let one window crash the pool
            last = f"unexpected: {exc}"
            time.sleep(min(5 * attempt, 20))
    log(f"  ⚠ {tag}: GAP after {MAX_ATTEMPTS} attempts (last: {last})")
    return {"window": tag, "status": "gap", "rows": 0, "files": [], "attempts": MAX_ATTEMPTS, "error": last}


# ───────────────────────── manifest ─────────────────────────
def load_manifest() -> dict:
    if os.path.exists(MANIFEST):
        with open(MANIFEST) as f:
            return json.load(f)
    return {}


def save_manifest(m: dict):
    tmp = MANIFEST + ".tmp"
    with open(tmp, "w") as f:
        json.dump(m, f, indent=2)
    os.replace(tmp, MANIFEST)


def windows(days: int):
    today = dt.date.today()
    ws0 = today - dt.timedelta(days=days)
    out, cur = [], ws0
    while cur <= today:
        out.append((cur, cur))          # 1-day chunks
        cur += dt.timedelta(days=1)
    return out


# ───────────────────────── modes ─────────────────────────
def cmd_fetch():
    os.makedirs(WORK, exist_ok=True)
    man = load_manifest()
    wins = windows(WINDOW_DAYS)
    todo = [(ws, we) for (ws, we) in wins
            if man.get(ws.isoformat() if ws == we else f"{ws.isoformat()}_{we.isoformat()}", {}).get("status") != "done"]
    log(f"FETCH: {len(wins)} windows total, {len(wins)-len(todo)} already done, {len(todo)} to fetch "
        f"(concurrency={CONCURRENCY}, max_attempts={MAX_ATTEMPTS})")
    done_rows = sum(v.get("rows", 0) for v in man.values() if v.get("status") == "done")
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = {ex.submit(fetch_window, ws, we): (ws, we) for (ws, we) in todo}
        for fut in as_completed(futs):
            entry = fut.result()
            with _manifest_lock:
                man[entry["window"]] = entry
                save_manifest(man)
            if entry["status"] == "done":
                done_rows += entry["rows"]
    ok = [v for v in man.values() if v.get("status") == "done"]
    gaps = [v["window"] for v in man.values() if v.get("status") in ("gap", "hard_error")]
    log(f"FETCH DONE: {len(ok)}/{len(wins)} windows landed, {done_rows:,} total rows on disk. GAPS={gaps}")
    if gaps:
        log("  (re-run `fetch` to retry gaps — resumable; only gap/missing windows re-attempted)")
    return gaps


def cmd_write(uri=PROD_URI, mode="overwrite", record=True):
    import duckdb, lance
    csvs = sorted(globmod.glob(os.path.join(WORK, "pas_*.csv")))
    if not csvs:
        raise RuntimeError(f"no CSVs in {WORK} — run fetch first")
    log(f"WRITE: {len(csvs)} CSV file(s) → {uri} (mode={mode})")
    started = dt.datetime.now(dt.timezone.utc)
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    os.makedirs(os.path.join(SP, "duckdb_spill"), exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{os.path.join(SP,'duckdb_spill')}';")
    tbl = con.sql(
        f"SELECT * FROM read_csv({csvs!r}, all_varchar=true, header=true, sample_size=-1, "
        f"ignore_errors=false, union_by_name=true)").to_arrow_table()
    con.close()
    log(f"  parsed {tbl.num_rows:,} rows × {len(tbl.column_names)} cols")
    lance.write_dataset(tbl, uri, mode=mode, data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so())
    log(f"  wrote → {uri}")
    ds = lance.dataset(uri, storage_options=so())
    present = set(ds.schema.names)
    built = []
    for c in INDEX_COLS:
        if c in present:
            ds.create_scalar_index(c, index_type="BTREE")
            built.append(c)
            log(f"  BTREE {c}")
        else:
            log(f"  SKIP (absent) {c}")
    v = cmd_verify(uri)
    if record and uri == PROD_URI:
        wins = windows(WINDOW_DAYS)
        man = load_manifest()
        gaps = [w for w in (x[0].isoformat() for x in wins) if man.get(w, {}).get("status") != "done"]
        _record_ledger(rows=tbl.num_rows, cols=len(tbl.column_names), table_rows_after=v["rows"],
                       built=built, window=(wins[0][0], wins[-1][1]), started=started, gaps=gaps)
    return {"rows": tbl.num_rows, "cols": len(tbl.column_names), "built": built, "verify": v}


def cmd_verify(uri=PROD_URI):
    import duckdb, lance
    ds = lance.dataset(uri, storage_options=so())
    try:
        idx = ds.list_indices()
    except Exception:  # noqa: BLE001
        idx = []
    names = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in idx]
    con = duckdb.connect(":memory:")
    con.register("src", ds.scanner(columns=["last_modified_date"]).to_reader())
    fr = con.execute("SELECT min(last_modified_date), max(last_modified_date), count(*) FROM src").fetchone()
    con.close()
    out = {"uri": uri, "rows": ds.count_rows(), "columns": len(ds.schema.names), "indices": names,
           "min_last_modified": str(fr[0]), "max_last_modified": str(fr[1])}
    log(f"VERIFY {uri}: rows={out['rows']:,} cols={out['columns']} "
        f"lm=[{out['min_last_modified']}..{out['max_last_modified']}] "
        f"idx_has_pk={'contract_award_unique_key' in ' '.join(names)}")
    return out


def _record_ledger(rows, cols, table_rows_after, built, window, started, gaps):
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("  WARN: HQX_DB_URL_POOLED not set; skipping ledger row.")
        return
    err = None if not gaps else f"gap_days={gaps}"
    status = "success" if rows > 0 else "error"
    try:
        try:
            import psycopg
            conn = psycopg.connect(dsn)
        except ImportError:
            import psycopg2 as psycopg  # noqa
            conn = psycopg.connect(dsn)
        with conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO ops.usaspending_api_award_fresh_runs
                   (feed, run_mode, window_start, window_end, rows_written, columns, table_rows_after,
                    api_calls, write_mode, indices_built, status, error_message, started_at, executed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, "backfill_local", window[0], window[1], rows, cols, table_rows_after, 0,
                 "overwrite", ",".join(built) if built else None, status, err, started,
                 dt.datetime.now(dt.timezone.utc)))
        conn.close()
        log(f"  ledger row written (status={status}, gaps={len(gaps)})")
    except Exception as exc:  # noqa: BLE001
        log(f"  WARN: ledger write failed (non-fatal): {exc}")


def _delete_r2_prefix(uri):
    try:
        import boto3
    except ImportError:
        log(f"  boto3 unavailable; leaving smoketest dataset for manual cleanup: {uri}")
        return
    o = so()
    rest = uri[len("s3://"):]
    bucket, _, prefix = rest.partition("/")
    s3 = boto3.client("s3", endpoint_url=o["endpoint"], aws_access_key_id=o["aws_access_key_id"],
                      aws_secret_access_key=o["aws_secret_access_key"], region_name="auto")
    keys = []
    tok = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        keys += [{"Key": o2["Key"]} for o2 in r.get("Contents", [])]
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys[i:i + 1000]})
    log(f"  deleted {len(keys)} smoketest object(s)")


def cmd_smoketest():
    """Prove the local→R2 write path on one known-good day before the long fetch."""
    os.makedirs(WORK + "_smoke", exist_ok=True)
    day = dt.date(2026, 6, 23)   # known non-empty (9,787 rows in prior tests)
    log(f"SMOKETEST: fetch {day} → write {TEST_URI} → verify → delete")
    rows, saved = None, []
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            rows, saved = _fetch_once(day, day, WORK + "_smoke", f"smoke_{day.isoformat()}")
            break
        except _Transient as exc:
            log(f"  smoke fetch attempt {attempt} transient: {exc}")
            time.sleep(min(20 * attempt, 90))
    if not saved:
        log("  SMOKETEST FETCH could not land the day — ES load; retry or wait. (This is fetch-side, not R2.)")
        sys.exit(3)
    import duckdb, lance
    con = duckdb.connect(":memory:")
    tbl = con.sql(f"SELECT * FROM read_csv({saved!r}, all_varchar=true, header=true, sample_size=-1, "
                  f"union_by_name=true)").to_arrow_table()
    con.close()
    log(f"  fetched {tbl.num_rows:,} rows × {len(tbl.column_names)} cols")
    lance.write_dataset(tbl, TEST_URI, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so())
    ds = lance.dataset(TEST_URI, storage_options=so())
    ds.create_scalar_index("contract_award_unique_key", index_type="BTREE")
    ds2 = lance.dataset(TEST_URI, storage_options=so())
    log(f"  R2 WRITE OK: read-back rows={ds2.count_rows():,} cols={len(ds2.schema.names)} "
        f"idx={[ (i.get('name') if isinstance(i,dict) else str(i)) for i in ds2.list_indices()]}")
    _delete_r2_prefix(TEST_URI)
    log("  SMOKETEST PASS ✓ — local→R2 lance write + index + read-back verified, test path deleted.")


def cmd_status():
    man = load_manifest()
    wins = windows(WINDOW_DAYS)
    done = [v for v in man.values() if v.get("status") == "done"]
    gaps = [v["window"] for v in man.values() if v.get("status") in ("gap", "hard_error")]
    rows = sum(v.get("rows", 0) for v in done)
    log(f"STATUS: {len(done)}/{len(wins)} windows done, {rows:,} rows on disk, gaps={gaps}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "status"
    {"smoketest": cmd_smoketest, "fetch": cmd_fetch, "write": cmd_write,
     "verify": cmd_verify, "status": cmd_status}[mode]()
