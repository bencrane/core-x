"""SAM.gov 90-day-winners attachment byte download — concurrent, WAF-compliant, daemonizable.

Stage 3 (isolated motion) for the staffing demand-side GTM. Consumes the local manifest
``s3://data-sink/active/sam_opps_attachment_manifest_winners/`` (155,183 rows; built by
sam_opps_attachment_manifest_90day_winners.py), gates to ``access_level='public'`` distinct
downloadable files, and pulls the actual file bytes into an ISOLATED content-addressed blob
tier + an ENRICHED Lance ledger.

Two deliberate departures from the historical pipelines/sam_gov/sam_attachment_download.py:

  1. CONCURRENT + GLOBALLY RATE-LIMITED.  6 worker threads, but a single global token bucket
     caps *aggregate* request initiation at ~8 req/s — the proven-safe residential envelope
     (SAM_ATTACHMENT_SIZE_GROUND_TRUTH_PROBE.md: conc=6 @ 0.1s pace -> 0 WAF blocks / 1000 req;
     conc>=24 trips the WAF in the first ~100 req). Concurrency hides per-connection S3 latency
     (the byte transfer is the bottleneck, not request rate); the global bucket guarantees the
     WAF never sees more than the envelope regardless of file-size distribution. A circuit
     breaker aborts the run if 429/403 blocks cluster, to protect the residential IP.

  2. ENRICHED, LINEAGE-COMPLETE LEDGER.  The historical ledger stored only resource_id (no
     file_name) — so when its source manifest was overwritten, 86% of one tier's filenames
     became unrecoverable. This ledger persists file_name, declared mime, real Content-Length,
     access_level, notice_id, solicitation_number, naics_code at fetch time. Lineage survives
     any future manifest re-snapshot, and the downstream text-extraction stage can filter by
     mime_type without re-joining the manifest.

OUT-OF-CORE BY DESIGN.  Bytes stream to a SpooledTemporaryFile (RAM up to 16 MB/file, then
NVMe spill) with an incremental sha256 — never a full in-memory bytearray. This is the only
OOM-safe way to pull >=50 MB files at concurrency 6 (the 90-day set has 718 files >=50 MB /
54.7 GB). Upload is single-threaded boto3 upload_fileobj (multipart for large objects).

PURE DETERMINISTIC I/O. No LLM, no extraction, no embedding (downstream stages).

Output (ISOLATED — never merged with the historical landing/ or active/sam_attachment_* sets):
  * bytes  -> s3://data-sink/active/sam_attachment_blobs/<resource_id>   (R2 CAS)
  * ledger -> s3://data-sink/active/sam_attachment_files/                (Lance v2.1, SoR)
  * worklist -> s3://data-sink/active/sam_attachment_worklist/           (Lance v2.1, snapshot)
  * run row -> ops.sam_attachment_download_runs                          (Postgres, per batch)

Full daemonized run:
    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' \
        --with boto3 --with duckdb \
      python pipelines/sam_gov/sam_attachment_download_90day.py --daemon --resume

Calibration smoke (200 files, foreground, throwaway URIs):
    ... python pipelines/sam_gov/sam_attachment_download_90day.py --max-files 200 \
        --ledger-uri  s3://data-sink/active/_smoke_90day_files/ \
        --blob-prefix s3://data-sink/active/_smoke_90day_blobs/ \
        --worklist-uri s3://data-sink/active/_smoke_90day_worklist/
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys

MANIFEST_URI = os.environ.get(
    "SAM90_MANIFEST_URI", "s3://data-sink/active/sam_opps_attachment_manifest_winners/")
LEDGER_URI = os.environ.get("SAM90_LEDGER_URI", "s3://data-sink/active/sam_attachment_files/")
BLOB_PREFIX = os.environ.get("SAM90_BLOB_PREFIX", "s3://data-sink/active/sam_attachment_blobs/")
WORKLIST_URI = os.environ.get("SAM90_WORKLIST_URI", "s3://data-sink/active/sam_attachment_worklist/")
CKPT_PATH = os.environ.get("SAM90_CKPT", "/tmp/sam_90day_ckpt.jsonl")
LOG_PATH = os.environ.get("SAM90_LOG", "/tmp/sam_90day_download.log")
FEED = "sam_attachment_download_90day"

# Gate: strictly public, real downloadable file (named, non-empty), distinct file.
# No export_controlled column on this manifest; no mime/size cap (full 213 GB footprint —
# the enriched ledger tags mime so downstream extraction filters text itself).
_GATE = "access_level = 'public' AND file_name IS NOT NULL AND size_bytes >= 1"

_CLAIM_OK = {
    "pdf": {"pdf"}, "docx": {"zip"}, "xlsx": {"zip"}, "pptx": {"zip"}, "zip": {"zip"},
    "doc": {"ole"}, "xls": {"ole"}, "ppt": {"ole"}, "rtf": {"rtf"}, "txt": {"txt"},
    "jpg": {"jpg"}, "jpeg": {"jpg"}, "png": {"png"}, "gif": {"gif"},
}
_CONTENT_TYPE = {
    "pdf": "application/pdf",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "doc": "application/msword",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "txt": "text/plain", "zip": "application/zip",
}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sam_attachment_download_runs (
    id bigserial PRIMARY KEY, run_id text NOT NULL, scope text,
    concurrency int, req_per_sec numeric,
    attempted int, downloaded int, skipped int, failed int, restricted int, gone int, oversize int,
    waf_blocks_429 int, waf_blocks_403 int,
    bytes_downloaded bigint, sustained_mbps numeric, size_mismatches int, mime_mismatches int,
    status text, error text, started_at timestamptz, completed_at timestamptz
);
"""


# --------------------------------------------------------------------------- daemon
def _daemonize(logpath: str) -> None:
    """Double-fork + setsid so the worker leaves the harness process group and survives a
    session resume. MUST be called BEFORE importing any threaded lib (requests/boto3/lance):
    forking a multi-threaded process on macOS aborts (`+[NSNumber initialize] ... fork()`)."""
    if os.fork() > 0:
        os._exit(0)
    os.setsid()
    if os.fork() > 0:
        os._exit(0)
    f = open(logpath, "a", buffering=1)
    os.dup2(f.fileno(), 1)
    os.dup2(f.fileno(), 2)
    try:
        os.dup2(open(os.devnull).fileno(), 0)
    except OSError:
        pass


# --------------------------------------------------------------------------- helpers
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _s3_client():
    import boto3
    from botocore.config import Config
    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required",
                 max_pool_connections=32, retries={"max_attempts": 3, "mode": "standard"})
    return boto3.client("s3", endpoint_url=so["endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"],
                        region_name="auto", config=cfg)


def _split_s3(uri: str) -> tuple[str, str]:
    body = uri[len("s3://"):] if uri.startswith("s3://") else uri
    bucket, _, key = body.partition("/")
    if key and not key.endswith("/"):
        key += "/"
    return bucket, key


def _headers(notice_id: str | None = None) -> dict:
    h = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
         "Accept": "application/octet-stream, */*", "Origin": "https://sam.gov"}
    if notice_id:
        h["Referer"] = f"https://sam.gov/opp/{notice_id}/view"
    return h


def _norm_mime(m: str | None) -> str:
    return (m or "").lstrip(".").lower()


def _sniff_mime(head: bytes) -> str | None:
    if head[:4] == b"%PDF":
        return "pdf"
    if head[:4] == b"PK\x03\x04":
        return "zip"
    if head[:4] == b"\xd0\xcf\x11\xe0":
        return "ole"
    if head[:5] == b"{\\rtf":
        return "rtf"
    if head[:3] == b"\xff\xd8\xff":
        return "jpg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    s = head.lstrip()[:20].lower()
    if s.startswith(b"<!doctype html") or s.startswith(b"<html"):
        return "html"
    if head and b"\x00" not in head:
        return "txt"
    return None


def _mime_match(claimed: str | None, sniffed: str | None) -> bool:
    if not claimed:
        return sniffed is not None
    ok = _CLAIM_OK.get(claimed)
    if ok is None:
        return sniffed is not None
    return sniffed in ok


# --------------------------------------------------------------------- concurrency
class RateLimiter:
    """Global min-spacing gate. Serializes request *start* times >= 1/rps apart across all
    threads, so aggregate initiation never exceeds the WAF-proven envelope regardless of
    worker count or file-size mix."""

    def __init__(self, rps: float):
        import threading
        self.spacing = 1.0 / rps
        self._lock = threading.Lock()
        self._next = 0.0

    def acquire(self) -> None:
        import time
        with self._lock:
            now = time.monotonic()
            wait = self._next - now
            self._next = (self._next if wait > 0 else now) + self.spacing
        if wait > 0:
            time.sleep(wait)


class Breaker:
    """WAF circuit breaker. Trips on a cluster of 429/403 within a rolling window OR a run of
    consecutive blocks — then the run stops submitting and drains, protecting the residential IP."""

    def __init__(self, window: float = 60.0, max_in_window: int = 15, max_consecutive: int = 25):
        import collections
        import threading
        self.window, self.max_in_window, self.max_consecutive = window, max_in_window, max_consecutive
        self._lock = threading.Lock()
        self._events = collections.deque()
        self._consec = 0
        self.tripped = False
        self.n429 = 0
        self.n403 = 0

    def record(self, blocked: bool, *, is429: bool = False, is403: bool = False) -> bool:
        import time
        with self._lock:
            now = time.monotonic()
            if blocked:
                self._events.append(now)
                self._consec += 1
                self.n429 += int(is429)
                self.n403 += int(is403)
            else:
                self._consec = 0
            while self._events and now - self._events[0] > self.window:
                self._events.popleft()
            if len(self._events) >= self.max_in_window or self._consec >= self.max_consecutive:
                self.tripped = True
            return self.tripped


# ------------------------------------------------------------------------- fetch
def _download_one(session, url, notice_id, *, rate: RateLimiter, breaker: Breaker,
                  ceiling: int, wallclock: float, connect_timeout: float = 15.0,
                  read_timeout: float = 90.0):
    """Stream a single file to a spooled temp file with incremental sha256. Returns
    ``(status, http_status, content_length, size_dl, sha256, mime_sniffed, spool|None, attempts, err)``.
    Rate-limited per network attempt; records WAF blocks to the breaker; bails if it trips.

    status: downloaded | restricted | gone | oversize | failed."""
    import tempfile
    import time

    import requests

    def _short(resp):
        try:
            return resp.text[:300]
        except Exception:  # noqa: BLE001
            return None

    last_err = None
    for attempt in range(6):
        if breaker.tripped:
            return ("failed", 0, None, 0, None, None, None, attempt, "waf_tripped")
        rate.acquire()
        t0 = time.monotonic()
        try:
            resp = session.get(url, headers=_headers(notice_id),
                               timeout=(connect_timeout, read_timeout), stream=True)
        except requests.RequestException as exc:
            last_err = f"net:{type(exc).__name__}"
            time.sleep(min(60, 2 ** attempt))
            continue
        sc = resp.status_code
        if sc == 200:
            clen = resp.headers.get("Content-Length")
            try:
                clen_i = int(clen) if clen is not None else None
            except ValueError:
                clen_i = None
            spool = tempfile.SpooledTemporaryFile(max_size=16 * 1024 * 1024)
            h = hashlib.sha256()
            n = 0
            head = b""
            try:
                for chunk in resp.iter_content(262144):
                    if not chunk:
                        continue
                    if len(head) < 512:
                        head += chunk[:512 - len(head)]
                    spool.write(chunk)
                    h.update(chunk)
                    n += len(chunk)
                    if n > ceiling:
                        spool.close()
                        resp.close()
                        return ("oversize", sc, clen_i, n, None, None, None, attempt + 1, f"stream>{ceiling}")
                    if time.monotonic() - t0 > wallclock:
                        spool.close()
                        resp.close()
                        return ("failed", sc, clen_i, n, None, None, None, attempt + 1, f"wallclock>{wallclock:.0f}s@{n}B")
            except requests.RequestException as exc:
                spool.close()
                last_err = f"stream:{type(exc).__name__}"
                time.sleep(min(60, 2 ** attempt))
                continue
            resp.close()
            spool.seek(0)
            breaker.record(False)
            return ("downloaded", sc, clen_i, n, h.hexdigest(), _sniff_mime(head), spool, attempt + 1, None)
        if sc == 401:
            breaker.record(False)
            return ("restricted", sc, None, 0, None, None, None, attempt + 1, _short(resp))
        if sc == 403:
            body = (_short(resp) or "")
            if "UNAUTHORIZED" in body.upper():
                breaker.record(False)
                return ("restricted", sc, None, 0, None, None, None, attempt + 1, body)
            breaker.record(True, is403=True)        # WAF 403 -> block signal
            time.sleep(min(120, 5 * 2 ** attempt))
            last_err = "waf403"
            continue
        if sc in (400, 410):
            breaker.record(False)
            return ("gone", sc, None, 0, None, None, None, attempt + 1, _short(resp))
        if sc == 429:
            breaker.record(True, is429=True)
            time.sleep(min(120, 5 * 2 ** attempt))
            last_err = "http429"
            continue
        if sc in (503,) or sc >= 500:
            time.sleep(min(120, 5 * 2 ** attempt))
            last_err = f"http{sc}"
            continue
        breaker.record(False)
        return ("failed", sc, None, 0, None, None, None, attempt + 1, _short(resp))
    return ("failed", 0, None, 0, None, None, None, 6, last_err or "retries exhausted")


def _ledger_schema():
    import pyarrow as pa
    return pa.schema([
        ("resource_id", pa.string()), ("file_name", pa.string()),
        ("mime_declared", pa.string()), ("mime_sniffed", pa.string()), ("mime_match", pa.bool_()),
        ("access_level", pa.string()),
        ("size_declared", pa.int64()), ("content_length", pa.int64()),
        ("size_downloaded", pa.int64()), ("size_match", pa.bool_()),
        ("sha256", pa.string()), ("http_status", pa.int32()), ("status", pa.string()),
        ("stored_uri", pa.string()), ("attempts", pa.int32()),
        ("notice_id", pa.string()), ("solicitation_number", pa.string()), ("naics_code", pa.string()),
        ("first_attempt_at", pa.timestamp("us", tz="UTC")),
        ("completed_at", pa.timestamp("us", tz="UTC")),
        ("error", pa.string()), ("run_id", pa.string()),
    ])


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001
        return False


def build_worklist(storage_options: dict, manifest_uri: str, worklist_uri: str):
    """Gate the 90-day manifest to public distinct downloadable files, persist as a Lance v2.1
    snapshot (lineage record — fixes the historical 'no worklist persisted' gap), return rows."""
    import duckdb
    import lance
    cols = ["resource_id", "file_name", "mime_type", "size_bytes", "access_level",
            "download_url", "attachment_order", "notice_id", "solicitation_number", "naics_code"]
    src = lance.dataset(manifest_uri, storage_options=storage_options).to_table(columns=cols)
    con = duckdb.connect()
    con.register("msrc", src)
    wl = con.execute(f"""
        SELECT * EXCLUDE (rn) FROM (
          SELECT *, row_number() OVER (PARTITION BY resource_id ORDER BY attachment_order, notice_id) rn
          FROM msrc WHERE {_GATE}
        ) WHERE rn = 1
    """).to_arrow_table()
    total = con.execute(f"SELECT count(*), coalesce(sum(size_bytes),0) FROM (SELECT size_bytes, "
                        f"row_number() OVER (PARTITION BY resource_id ORDER BY attachment_order, notice_id) rn "
                        f"FROM msrc WHERE {_GATE}) WHERE rn=1").fetchone()
    lance.write_dataset(wl, worklist_uri, mode="overwrite",
                        data_storage_version="2.1", storage_options=storage_options)
    print(f"worklist: distinct_public_files={total[0]:,} declared_bytes={total[1]:,} "
          f"(~{total[1] / 1e9:.1f} GB) -> {worklist_uri}", flush=True)
    return wl.to_pylist()


def _load_done(ledger_uri: str, so: dict, ckpt_path: str) -> dict:
    """Resume set: resource_id -> (status, size_downloaded). Union of the Lance ledger (durable)
    and the line-buffered JSONL checkpoint (covers the window since the last Lance flush)."""
    done: dict = {}
    if _dataset_exists(ledger_uri, so):
        import lance
        for r in lance.dataset(ledger_uri, storage_options=so).to_table(
                columns=["resource_id", "status", "size_downloaded"]).to_pylist():
            done[r["resource_id"]] = (r["status"], r["size_downloaded"])
    if os.path.exists(ckpt_path):
        with open(ckpt_path) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                    done[r["resource_id"]] = (r["status"], r.get("size_downloaded"))
                except Exception:  # noqa: BLE001
                    continue
    return done


def _record_run(stats: dict, dsn: str | None) -> None:
    if not dsn:
        print("WARN: no HQX_DB_URL_POOLED; skipping ops.* write.", flush=True)
        return
    try:
        import psycopg
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute("""
                INSERT INTO ops.sam_attachment_download_runs
                  (run_id, scope, concurrency, req_per_sec, attempted, downloaded, skipped, failed,
                   restricted, gone, oversize, waf_blocks_429, waf_blocks_403, bytes_downloaded,
                   sustained_mbps, size_mismatches, mime_mismatches, status, error, started_at, completed_at)
                VALUES (%(run_id)s,%(scope)s,%(concurrency)s,%(req_per_sec)s,%(attempted)s,%(downloaded)s,
                   %(skipped)s,%(failed)s,%(restricted)s,%(gone)s,%(oversize)s,%(waf_blocks_429)s,
                   %(waf_blocks_403)s,%(bytes_downloaded)s,%(sustained_mbps)s,%(size_mismatches)s,
                   %(mime_mismatches)s,%(status)s,%(error)s,%(started_at)s,%(completed_at)s)
                """, stats)
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}", flush=True)


def run_download(*, storage_options: dict, dsn: str | None, manifest_uri: str = MANIFEST_URI,
                 ledger_uri: str = LEDGER_URI, blob_prefix: str = BLOB_PREFIX,
                 worklist_uri: str = WORKLIST_URI, ckpt_path: str = CKPT_PATH,
                 run_id: str = "adhoc", resume: bool = False, max_files: int = 0,
                 concurrency: int = 6, req_per_sec: float = 8.0, checkpoint_every: int = 500,
                 ceiling: int = 1_073_741_824, wallclock: float = 600.0) -> dict:
    import threading
    import time
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

    import lance
    import pyarrow as pa
    import requests
    from boto3.s3.transfer import TransferConfig

    started_at = dt.datetime.now(dt.timezone.utc)
    schema = _ledger_schema()
    bucket, key_prefix = _split_s3(blob_prefix)
    s3 = _s3_client()
    xfer = TransferConfig(use_threads=False)            # bound connections: worker concurrency only
    rate = RateLimiter(req_per_sec)
    breaker = Breaker()
    tlocal = threading.local()

    def session() -> requests.Session:
        s = getattr(tlocal, "s", None)
        if s is None:
            s = tlocal.s = requests.Session()
        return s

    files = build_worklist(storage_options, manifest_uri, worklist_uri)
    if max_files:
        files = files[:max_files]
    done = _load_done(ledger_uri, storage_options, ckpt_path) if resume else {}
    pending = [f for f in files if done.get(f["resource_id"], ("", 0))[0] not in
               ("downloaded", "restricted", "gone")]
    print(f"run_id={run_id} conc={concurrency} rps={req_per_sec} worklist={len(files):,} "
          f"resume_known={len(done):,} pending={len(pending):,}", flush=True)

    c = {"attempted": 0, "downloaded": 0, "skipped": len(files) - len(pending), "failed": 0,
         "restricted": 0, "gone": 0, "oversize": 0, "bytes": 0, "size_mm": 0, "mime_mm": 0}
    buf: list[dict] = []
    ckpt = open(ckpt_path, "a", buffering=1)
    final_status, error_text = "error", None
    t_first = None

    def flush(tag: str) -> None:
        if not buf:
            return
        at = pa.Table.from_pylist(buf, schema=schema)
        mode = "append" if _dataset_exists(ledger_uri, storage_options) else "create"
        lance.write_dataset(at, ledger_uri, mode=mode,
                            data_storage_version="2.1", storage_options=storage_options)
        print(f"[{tag}] +{len(buf)} ledger rows (dl={c['downloaded']} fail={c['failed']} "
              f"restr={c['restricted']} gone={c['gone']} ovsz={c['oversize']} "
              f"429={breaker.n429} 403={breaker.n403})", flush=True)
        buf.clear()

    def fetch(item: dict) -> dict:
        rid = item["resource_id"]
        first_at = dt.datetime.now(dt.timezone.utc)
        st, sc, clen, n, sha, sniff, spool, attempts, err = _download_one(
            session(), item["download_url"], item.get("notice_id"),
            rate=rate, breaker=breaker, ceiling=ceiling, wallclock=wallclock)
        declared = _norm_mime(item.get("mime_type"))
        size_decl = int(item.get("size_bytes") or 0)
        row = {"resource_id": rid, "file_name": item.get("file_name"),
               "mime_declared": declared or None, "mime_sniffed": sniff, "mime_match": None,
               "access_level": item.get("access_level"),
               "size_declared": size_decl, "content_length": clen,
               "size_downloaded": None, "size_match": None, "sha256": sha,
               "http_status": int(sc), "status": st, "stored_uri": None, "attempts": int(attempts),
               "notice_id": item.get("notice_id"), "solicitation_number": item.get("solicitation_number"),
               "naics_code": item.get("naics_code"), "first_attempt_at": first_at,
               "completed_at": dt.datetime.now(dt.timezone.utc), "error": err, "run_id": run_id}
        if st == "downloaded" and spool is not None:
            try:
                key = f"{key_prefix}{rid}"
                ct = _CONTENT_TYPE.get(declared, "application/octet-stream")
                spool.seek(0)
                s3.upload_fileobj(spool, bucket, key, ExtraArgs={"ContentType": ct}, Config=xfer)
                spool.close()
                row.update({"size_downloaded": n, "size_match": (size_decl == n) if size_decl else None,
                            "mime_match": _mime_match(declared, sniff),
                            "stored_uri": f"s3://{bucket}/{key}"})
            except Exception as exc:  # noqa: BLE001
                try:
                    spool.close()
                except Exception:  # noqa: BLE001
                    pass
                row.update({"status": "failed", "error": f"upload:{type(exc).__name__}:{exc}"})
        return row

    def collect(row: dict) -> None:
        st = row["status"]
        c["attempted"] += 1
        if st == "downloaded":
            c["downloaded"] += 1
            c["bytes"] += int(row["size_downloaded"] or 0)
            c["size_mm"] += int(row["size_match"] is False)
            c["mime_mm"] += int(row["mime_match"] is False)
        else:
            c[st] = c.get(st, 0) + 1
        buf.append(row)
        ckpt.write(json.dumps({"resource_id": row["resource_id"], "status": st,
                               "size_downloaded": row["size_downloaded"]}) + "\n")
        if c["attempted"] % checkpoint_every == 0:
            flush(f"ckpt_{c['attempted']}")
            elapsed = max(1e-9, time.monotonic() - t_first) if t_first else 0.0
            mbps = (c["bytes"] / 1e6) / elapsed if elapsed else 0.0
            print(f"  progress attempted={c['attempted']:,}/{len(pending):,} dl={c['downloaded']:,} "
                  f"~{mbps:.2f} MB/s ~{c['bytes']/1e9:.1f} GB", flush=True)

    try:
        t_first = time.monotonic()
        it = iter(pending)
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            inflight = set()
            for _ in range(concurrency * 3):
                try:
                    inflight.add(pool.submit(fetch, next(it)))
                except StopIteration:
                    break
            while inflight:
                fin, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                for fut in fin:
                    collect(fut.result())
                if breaker.tripped:
                    final_status = "waf_tripped"
                    print("!! WAF circuit breaker TRIPPED — draining in-flight, stopping submission.", flush=True)
                    break
                for _ in range(len(fin)):
                    try:
                        inflight.add(pool.submit(fetch, next(it)))
                    except StopIteration:
                        break
            for fut in inflight:                         # drain whatever was already running
                try:
                    collect(fut.result())
                except Exception:  # noqa: BLE001
                    pass
        flush("final")
        if final_status != "waf_tripped":
            final_status = "success"
    except KeyboardInterrupt:
        final_status = "interrupted"
        flush("interrupt")
    except Exception as exc:  # noqa: BLE001
        error_text, final_status = str(exc), "error"
        print(f"FATAL: {exc}", flush=True)
        try:
            flush("salvage")
        except Exception as e2:  # noqa: BLE001
            print(f"salvage flush failed: {e2}", flush=True)
    finally:
        ckpt.close()
        try:
            lds = lance.dataset(ledger_uri, storage_options=storage_options)
            for col, it_ in [("resource_id", "BTREE"), ("sha256", "BTREE"),
                             ("status", "BITMAP"), ("mime_declared", "BITMAP")]:
                try:
                    lds.create_scalar_index(col, index_type=it_, replace=True)
                except Exception as ie:  # noqa: BLE001
                    print(f"index {col} skipped: {ie}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"index phase skipped: {exc}", flush=True)
        completed_at = dt.datetime.now(dt.timezone.utc)
        elapsed = max(1e-9, time.monotonic() - t_first) if t_first else 0.0
        mbps = (c["bytes"] / 1e6) / elapsed if elapsed else 0.0
        stats = {"run_id": run_id, "scope": f"90day_winners; {_GATE}", "concurrency": concurrency,
                 "req_per_sec": req_per_sec, "attempted": c["attempted"], "downloaded": c["downloaded"],
                 "skipped": c["skipped"], "failed": c["failed"], "restricted": c["restricted"],
                 "gone": c["gone"], "oversize": c["oversize"], "waf_blocks_429": breaker.n429,
                 "waf_blocks_403": breaker.n403, "bytes_downloaded": c["bytes"],
                 "sustained_mbps": round(mbps, 3), "size_mismatches": c["size_mm"],
                 "mime_mismatches": c["mime_mm"], "status": final_status, "error": error_text,
                 "started_at": started_at, "completed_at": completed_at}
        _record_run(stats, dsn)
        print("SUMMARY:", {k: v for k, v in stats.items() if k != "scope"}, flush=True)
        print(f"sustained ~{mbps:.2f} MB/s over {c['bytes'] / 1e9:.2f} GB; "
              f"WAF blocks 429={breaker.n429} 403={breaker.n403}; status={final_status}", flush=True)
    return {"status": final_status, "downloaded": c["downloaded"], "bytes": c["bytes"],
            "waf_429": breaker.n429, "waf_403": breaker.n403, "sustained_mbps": round(mbps, 3)}


def _cli() -> None:
    p = argparse.ArgumentParser(description="SAM.gov 90-day-winners concurrent attachment downloader.")
    p.add_argument("--daemon", action="store_true", help="double-fork + setsid; survive session resume")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--max-files", type=int, default=0, help="cap files (calibration/smoke)")
    p.add_argument("--concurrency", type=int, default=6, help="worker threads (WAF-proven: 6)")
    p.add_argument("--req-per-sec", type=float, default=8.0, help="global request ceiling (WAF-proven: ~8)")
    p.add_argument("--checkpoint-every", type=int, default=500)
    p.add_argument("--ceiling", type=int, default=1_073_741_824, help="per-file byte ceiling (OOM/tarpit backstop)")
    p.add_argument("--wallclock", type=float, default=600.0, help="per-file transfer budget (s)")
    p.add_argument("--blob-prefix", default=BLOB_PREFIX)
    p.add_argument("--ledger-uri", default=LEDGER_URI)
    p.add_argument("--worklist-uri", default=WORKLIST_URI)
    p.add_argument("--manifest-uri", default=MANIFEST_URI)
    p.add_argument("--ckpt", default=CKPT_PATH)
    p.add_argument("--run-id", default=None)
    a = p.parse_args()

    if a.daemon:
        _daemonize(LOG_PATH)                              # BEFORE importing threaded libs
    run_id = a.run_id or f"90day-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
    out = run_download(
        storage_options=_r2_storage_options(), dsn=os.environ.get("HQX_DB_URL_POOLED"),
        manifest_uri=a.manifest_uri, ledger_uri=a.ledger_uri, blob_prefix=a.blob_prefix,
        worklist_uri=a.worklist_uri, ckpt_path=a.ckpt, run_id=run_id, resume=a.resume,
        max_files=a.max_files, concurrency=a.concurrency, req_per_sec=a.req_per_sec,
        checkpoint_every=a.checkpoint_every, ceiling=a.ceiling, wallclock=a.wallclock)
    print("RESULT:", out, flush=True)
    sys.exit(0 if out["status"] in ("success",) else 1)


if __name__ == "__main__":
    _cli()
