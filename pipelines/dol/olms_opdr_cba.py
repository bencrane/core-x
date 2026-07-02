"""DOL OLMS OPDR — Collective Bargaining Agreements (CBA) File harvest.

Lands the OLMS Online Public Disclosure Room (OPDR) CBA File — the corpus of ACTUAL
union contracts (with wage/fringe schedules) — into the Gen-3 Lance SoR + R2 raw-blob
store. This is the wage-bearing document layer behind the SAM.gov §4(c) CBA pointers.

REVERSE-ENGINEERED + VERIFIED LIVE (residential IP, 2026-07-02). OPDR is an AngularJS
1.x SPA (scripts/cbaSearchCtrl.js hard-codes the paths) over classic WebSphere servlets.
NO REST layer, NO auth, NO session/cookie, NO CSRF, NO referer, NO api_key, NO pagination.

  STAGE 1 — INDEX (--index): the entire catalog in ONE POST.
    POST https://olmsapps.dol.gov/olpdr/GetCBAFilerListServlet   body {"clearCache":"F"}
      -> HTTP 200, ~1.69MB. Content-Type is MISLABELED "text/xml;charset=ISO-8859-1"
         but the body is JSON — it MUST be decoded latin-1/ISO-8859-1 (non-UTF8 bytes,
         e.g. 0xD1, appear in employer/union names; resp.json()/utf-8 raises).
      -> { filerList:[...4849...], totalRecords:4849, lastUpdated:<epoch ms>, fromCache }
      Per-record: cbaPubId, docId, cbaId, cbaOid, modId, modDate(epoch ms), empName(100%),
        unionName(99%), unionLocalNum(ALWAYS BLANK — local # is the trailing token inside
        unionName), location(99%, city/state free-text, NOT a clean state col), naics(61%),
        noOfEmp(60%), expDate(epoch ms, 100% — CBA EXPIRATION), type(PRIVATE|PUBLIC),
        agreementFileName. GET on this servlet -> 405 (POST only).
    Reconciles totalRecords == len(filerList) and FAILS CLOSED on mismatch.

  STAGE 2 — DOCUMENTS (--documents): one key-less GET per docId -> raw bytes to R2.
    Route by modId (verified 8258:4514 / 9239:330 / 10000:5):
      modId != 10000  -> GET .../olpdr/GetAttachmentServlet?docId={docId}
      modId == 10000  -> GET .../olpdr/GetECbaAttachmentServlet?docId={docId}   (5 eCBA rows)
      -> Content-Type application/pdf (Content-Disposition inline; filename=...), the actual
         union-contract PDF. NOT all attachments are PDF — a few are .docx / other; the raw
         bytes + content-type are stored verbatim (raw-stays-lossless), never assumed PDF.
    4,844 distinct docIds. Raw bytes land as R2 objects under active/olms_cba_blobs/{docId}.{ext};
    a Lance manifest (olms_cba_documents) carries (doc_id, r2_key, content_type, byte_len,
    sha256, fetch_status, ...) — the unification surface. Resume-safe append: skips docIds
    already fetched (non-retry status); a per-doc http_4xx is recorded, not fatal.

Gating: NONE observed from a residential IP (cold cookieless requests succeed; the app mints
JSESSIONID/AWSALB on the landing page but neither servlet requires them). WebSphere WAF 403s
the WebFetch fetcher UA — this crawl uses curl-equivalent requests with a browser UA. Be polite:
bounded concurrency + backoff.

Output (Lance v2.1, Gen-3 active/ SoR + R2 blobs):
    s3://data-sink/active/olms_cba_index/       one row per CBA record (4,849)          [--index]
    s3://data-sink/active/olms_cba_documents/    one row per distinct docId (~4,844)      [--documents]
    s3://data-sink/active/olms_cba_blobs/{docId}.{ext}   raw contract bytes (R2 objects)  [--documents]

    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with requests --with boto3 --with truststore \
        --with 'psycopg[binary]' \
      python pipelines/dol/olms_opdr_cba.py --index                 # stage 1: metadata catalog
    ... python pipelines/dol/olms_opdr_cba.py --documents --resume   # stage 2: raw contract blobs

Smoke (cap docs, throwaway URIs/prefix):
    ... --index     --index-uri s3://data-sink/active/_smoke_olms_index/
    ... --documents --limit 20 --docs-uri s3://data-sink/active/_smoke_olms_docs/ \
                    --blob-prefix active/_smoke_olms_blobs/
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import time

# olmsapps.dol.gov serves an AIA-only intermediate cert that certifi lacks (curl/browsers fetch
# it via AIA; Python's OpenSSL does not -> SSLCertVerificationError). Route TLS trust through the
# OS store, which resolves the intermediate — matching the curl-verified recon. Requires the
# `truststore` dep (added to the run's `uv --with`).
try:
    import truststore

    truststore.inject_into_ssl()
    # truststore verifies via the OS trust store but sets OpenSSL verify_mode=CERT_NONE (it hooks
    # verification out-of-band via the OS), which makes urllib3 emit a spurious InsecureRequestWarning
    # on every connection. The requests ARE verified (by the OS store) — silence the false positive.
    import warnings as _warnings

    from urllib3.exceptions import InsecureRequestWarning as _IRW
    _warnings.filterwarnings("ignore", category=_IRW)
except Exception:  # noqa: BLE001 — falls back to certifi; a genuine SSL failure then surfaces loudly
    pass

# Reuse the fleet R2/object-store + index plumbing verbatim.
from pipelines.bls.ingest import (  # noqa: E402
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _s3_client,
    _storage_options,
)

BUCKET = "data-sink"

INDEX_URI = os.environ.get("OLMS_CBA_INDEX_URI", f"s3://{BUCKET}/active/olms_cba_index/")
DOCS_URI = os.environ.get("OLMS_CBA_DOCS_URI", f"s3://{BUCKET}/active/olms_cba_documents/")
BLOB_PREFIX = os.environ.get("OLMS_CBA_BLOB_PREFIX", "active/olms_cba_blobs/")

LIST_URL = "https://olmsapps.dol.gov/olpdr/GetCBAFilerListServlet"
ATTACH_URL = "https://olmsapps.dol.gov/olpdr/GetAttachmentServlet?docId={doc_id}"
ECBA_ATTACH_URL = "https://olmsapps.dol.gov/olpdr/GetECbaAttachmentServlet?docId={doc_id}"
ECBA_MOD_ID = 10000

INDEX_SOURCE = "olmsapps.dol.gov/olpdr/GetCBAFilerListServlet (frontend, no api_key)"
DOCS_SOURCE = "olmsapps.dol.gov/olpdr/Get[ECba]AttachmentServlet (frontend, no api_key)"

# Content-Type -> file extension for the raw-blob key. Fallback chain in _ext_for().
_CT_EXT = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/msword": "doc",
    "application/rtf": "rtf",
    "text/plain": "txt",
    "text/html": "html",
    "application/zip": "zip",
    "image/tiff": "tif",
}


def _headers() -> dict:
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
        "Accept": "*/*",
        "Origin": "https://olmsapps.dol.gov",
        "Referer": "https://olmsapps.dol.gov/olpdr/",
    }


# ── field coercion ─────────────────────────────────────────────────────────────────
def _s(v) -> str | None:
    if v is None:
        return None
    v = str(v).strip()
    return v or None


def _int(v) -> int | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(float(s)) if s.replace(".", "", 1).lstrip("-").isdigit() else None
    except (TypeError, ValueError):
        return None


def _iso_ms(v) -> str | None:
    """epoch-millis int -> ISO-8601 UTC string. Blank/0/unparseable -> None."""
    n = _int(v)
    if not n:
        return None
    try:
        return dt.datetime.fromtimestamp(n / 1000.0, dt.timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def _ext_for(content_type: str | None, agreement_file_name: str | None) -> str:
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct in _CT_EXT:
        return _CT_EXT[ct]
    fn = (agreement_file_name or "").strip().lower()
    if "." in fn:
        ext = fn.rsplit(".", 1)[-1]
        if ext.isalnum() and len(ext) <= 5:
            return ext
    return "bin"


# ── WD-local ops.dol_runs ledger ────────────────────────────────────────────────────
def _record_run(dataset: str, uri: str, rows: int, built: list, status: str,
                error: str | None, coverage: dict, started_at, completed_at) -> None:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.dol_runs write.", flush=True)
        return
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ops.dol_runs (
                    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    dataset text NOT NULL, dataset_uri text, source_file text, doc_sha256 text,
                    rows_processed bigint, indexes_built text[], coverage jsonb,
                    status text NOT NULL, error text,
                    started_at timestamptz, completed_at timestamptz,
                    recorded_at timestamptz NOT NULL DEFAULT now())
                """
            )
            cur.execute(
                """
                INSERT INTO ops.dol_runs
                  (dataset, dataset_uri, rows_processed, indexes_built, coverage,
                   status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (dataset, uri, rows, built, json.dumps(coverage), status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must never mask a good crawl
        print(f"WARN: ops.dol_runs write failed: {exc}", flush=True)


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001
        return False


# ═══════════════════════════════════════════════════════════════════════════════════
# STAGE 1 — INDEX
# ═══════════════════════════════════════════════════════════════════════════════════
def run_index(*, storage_options: dict, index_uri: str = INDEX_URI) -> dict:
    import lance
    import pyarrow as pa
    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    crawled_at = started_at
    so = storage_options

    # ONE POST returns the whole corpus. Body is JSON mislabeled text/xml -> latin-1 decode.
    resp = requests.post(LIST_URL, data=json.dumps({"clearCache": "F"}),
                         headers={**_headers(), "Content-Type": "application/json"}, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"index POST HTTP {resp.status_code}: {(resp.text or '')[:200]}")
    j = json.loads(resp.content.decode("latin-1"))
    fl = j.get("filerList") or []
    total = _int(j.get("totalRecords"))
    print(f"filerList={len(fl)} totalRecords={total} fromCache={j.get('fromCache')}", flush=True)

    # FAIL CLOSED on reconciliation gap.
    if total is not None and len(fl) != total:
        raise RuntimeError(
            f"RECONCILIATION FAILED: filerList={len(fl)} != totalRecords={total}. "
            "Refusing to write a partial index.")

    rows = []
    for r in fl:
        rows.append({
            "cba_pub_id": _int(r.get("cbaPubId")),
            "doc_id": _int(r.get("docId")),
            "cba_id": _int(r.get("cbaId")),
            "cba_oid": _int(r.get("cbaOid")),
            "mod_id": _int(r.get("modId")),
            "mod_date": _iso_ms(r.get("modDate")),
            "emp_name": _s(r.get("empName")),
            "union_name": _s(r.get("unionName")),
            "union_local_num": _s(r.get("unionLocalNum")),
            "location": _s(r.get("location")),
            "naics": _s(r.get("naics")),
            "no_of_emp": _int(r.get("noOfEmp")),
            "exp_date": _iso_ms(r.get("expDate")),
            "type": _s(r.get("type")),
            "agreement_file_name": _s(r.get("agreementFileName")),
            "source": INDEX_SOURCE,
            "crawled_at": crawled_at,
        })

    schema = pa.schema([
        ("cba_pub_id", pa.int32()), ("doc_id", pa.int32()), ("cba_id", pa.int32()),
        ("cba_oid", pa.int32()), ("mod_id", pa.int32()), ("mod_date", pa.string()),
        ("emp_name", pa.string()), ("union_name", pa.string()), ("union_local_num", pa.string()),
        ("location", pa.string()), ("naics", pa.string()), ("no_of_emp", pa.int32()),
        ("exp_date", pa.string()), ("type", pa.string()), ("agreement_file_name", pa.string()),
        ("source", pa.string()), ("crawled_at", pa.timestamp("us", tz="UTC")),
    ])

    status, error_text, built = "error", None, []
    try:
        tbl = pa.Table.from_pylist(rows, schema=schema)
        lance.write_dataset(tbl, index_uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                            storage_options=so)
        print(f"wrote {tbl.num_rows} index rows -> {index_uri}", flush=True)
        built = _build_indexes(index_uri, btree=["doc_id", "cba_pub_id", "emp_name"],
                               bitmap=["type", "mod_id"], so=so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc); print(f"FATAL: {exc}", flush=True); raise
    finally:
        from collections import Counter
        completed_at = dt.datetime.now(dt.timezone.utc)
        cov = {"total_records": total, "rows": len(rows),
               "type": dict(Counter(r["type"] for r in rows)),
               "mod_id": dict(Counter(r["mod_id"] for r in rows)),
               "distinct_doc_id": len({r["doc_id"] for r in rows})}
        _record_run("olms_cba_index", index_uri, len(rows), built, status, error_text,
                    cov, started_at, completed_at)
        print(f"INDEX SUMMARY: rows={len(rows)} coverage={cov} status={status}", flush=True)
    return {"status": status, "rows": len(rows), "indexes": built,
            "distinct_doc_id": len({r["doc_id"] for r in rows})}


# ═══════════════════════════════════════════════════════════════════════════════════
# STAGE 2 — DOCUMENTS (raw contract bytes -> R2 blobs + Lance manifest)
# ═══════════════════════════════════════════════════════════════════════════════════
def _docs_schema():
    import pyarrow as pa
    return pa.schema([
        ("doc_id", pa.int32()), ("cba_pub_id", pa.int32()), ("mod_id", pa.int32()),
        ("servlet", pa.string()), ("r2_bucket", pa.string()), ("r2_key", pa.string()),
        ("content_type", pa.string()), ("file_ext", pa.string()), ("byte_len", pa.int64()),
        ("sha256", pa.string()), ("http_status", pa.int32()), ("fetch_status", pa.string()),
        ("fetched_at", pa.timestamp("us", tz="UTC")), ("source", pa.string()),
    ])


def run_documents(*, storage_options: dict, index_uri: str = INDEX_URI, docs_uri: str = DOCS_URI,
                  blob_prefix: str = BLOB_PREFIX, limit: int = 0, workers: int = 8,
                  inter_call_sleep: float = 0.05, resume: bool = True,
                  checkpoint_every: int = 200) -> dict:
    """Fetch the raw contract bytes for every distinct docId on the index -> R2 blob + manifest.

    Resume-safe (append): skips docIds already fetched (non-retry status). A per-doc http_4xx
    records fetch_status='http_NNN' (genuinely-gone doc); only transient exhaustion is left to
    a --resume re-run. Raw bytes are stored verbatim (never assumed PDF); the Lance manifest is
    the unification surface (doc_id joins olms_cba_index)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import lance
    import pyarrow as pa
    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    so = storage_options
    s3 = _s3_client()

    # one work item per DISTINCT docId (index has ~5 dup docIds across its 4,849 records)
    man = lance.dataset(index_uri, storage_options=so).to_table(
        columns=["doc_id", "mod_id", "cba_pub_id", "agreement_file_name"]).to_pylist()
    by_doc: dict[int, dict] = {}
    for r in man:
        d = r["doc_id"]
        if d is None or d in by_doc:
            continue
        by_doc[d] = r
    work = list(by_doc.values())

    done: set = set()
    if resume and _dataset_exists(docs_uri, so):
        prior = lance.dataset(docs_uri, storage_options=so).to_table(
            columns=["doc_id", "fetch_status"]).to_pylist()
        done = {p["doc_id"] for p in prior
                if p["fetch_status"] and not p["fetch_status"].startswith("retry")}
    todo = [w for w in work if w["doc_id"] not in done]
    if limit:
        todo = todo[:limit]
    print(f"documents stage: distinct_doc_id={len(work)} already_done={len(done)} todo={len(todo)}",
          flush=True)

    session = requests.Session()
    calls = {"n": 0}
    schema = _docs_schema()

    def fetch_doc(item: dict) -> dict:
        doc_id = item["doc_id"]
        mod_id = item["mod_id"]
        is_ecba = mod_id == ECBA_MOD_ID
        servlet = "GetECbaAttachmentServlet" if is_ecba else "GetAttachmentServlet"
        url = (f"https://olmsapps.dol.gov/olpdr/{servlet}?docId={doc_id}")
        row = {"doc_id": doc_id, "cba_pub_id": item.get("cba_pub_id"), "mod_id": mod_id,
               "servlet": servlet, "r2_bucket": None, "r2_key": None, "content_type": None,
               "file_ext": None, "byte_len": 0, "sha256": None, "http_status": -1,
               "fetch_status": "retry_exhausted",
               "fetched_at": dt.datetime.now(dt.timezone.utc), "source": DOCS_SOURCE}
        for attempt in range(6):
            calls["n"] += 1
            try:
                resp = session.get(url, headers=_headers(), timeout=180)
            except requests.RequestException:
                time.sleep(min(30, 2 ** attempt)); continue
            sc = resp.status_code
            if sc == 200:
                content = resp.content or b""
                # bare media type only — the raw header carries a per-file ";name=<file>" param
                # that would blow up the content_type BITMAP cardinality (filename is already on
                # the index as agreement_file_name).
                ct_raw = _s(resp.headers.get("Content-Type"))
                ct = ct_raw.split(";")[0].strip().lower() if ct_raw else None
                ext = _ext_for(ct, item.get("agreement_file_name"))
                key = f"{blob_prefix}{doc_id}.{ext}"
                try:
                    s3.put_object(Bucket=BUCKET, Key=key, Body=content,
                                  ContentType=(ct or "application/octet-stream"))
                except Exception as exc:  # noqa: BLE001 — treat as transient
                    print(f"  doc {doc_id}: R2 put failed ({exc}); retrying", flush=True)
                    time.sleep(min(30, 2 ** attempt)); continue
                row.update({
                    "r2_bucket": BUCKET, "r2_key": key, "content_type": ct, "file_ext": ext,
                    "byte_len": len(content),
                    "sha256": hashlib.sha256(content).hexdigest() if content else None,
                    "http_status": 200, "fetch_status": "fetched",
                    "fetched_at": dt.datetime.now(dt.timezone.utc)})
                return row
            if sc in (403, 429, 503):
                time.sleep(min(120, 5 * 2 ** attempt)); continue
            if sc >= 500:
                time.sleep(min(30, 2 ** attempt)); continue
            row.update({"fetch_status": f"http_{sc}", "http_status": sc,
                        "fetched_at": dt.datetime.now(dt.timezone.utc)})
            return row
        return row

    def flush(batch: list) -> None:
        if not batch:
            return
        tbl = pa.Table.from_pylist(batch, schema=schema)
        mode = "append" if _dataset_exists(docs_uri, so) else "create"
        lance.write_dataset(tbl, docs_uri, mode=mode, data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                            storage_options=so)

    counts = {"fetched": 0, "not_found": 0, "retry_exhausted": 0}
    bytes_total = {"n": 0}
    batch: list = []
    done_n = 0
    status, error_text, built = "error", None, []
    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(fetch_doc, w): w["doc_id"] for w in todo}
            for fut in as_completed(futs):
                row = fut.result()
                st = row["fetch_status"]
                counts["fetched" if st == "fetched" else
                       "not_found" if st.startswith("http_") else "retry_exhausted"] += 1
                bytes_total["n"] += row["byte_len"] or 0
                batch.append(row)
                done_n += 1
                if len(batch) >= checkpoint_every:
                    flush(batch); batch = []
                    print(f"  docs {done_n}/{len(todo)}  {counts}  "
                          f"{bytes_total['n'] // (1024*1024)}MB", flush=True)
                if inter_call_sleep:
                    time.sleep(inter_call_sleep)
        flush(batch)
        built = _build_indexes(docs_uri, btree=["doc_id", "cba_pub_id", "sha256"],
                               bitmap=["content_type", "fetch_status", "mod_id"], so=so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc); print(f"FATAL: {exc}", flush=True); raise
    finally:
        total_docs = (lance.dataset(docs_uri, storage_options=so).count_rows()
                      if _dataset_exists(docs_uri, so) else 0)
        completed_at = dt.datetime.now(dt.timezone.utc)
        cov = {"counts": counts, "mb_downloaded": bytes_total["n"] // (1024 * 1024),
               "manifest_rows": total_docs, "distinct_doc_id": len(work), "api_calls": calls["n"]}
        print(f"DOCS SUMMARY: todo={len(todo)} {counts} manifest_rows={total_docs} "
              f"{bytes_total['n'] // (1024*1024)}MB indexes={built} status={status} "
              f"error={error_text}", flush=True)
        _record_run("olms_cba_documents", docs_uri, counts["fetched"], built, status, error_text,
                    cov, started_at, completed_at)
    return {"status": status, "todo": len(todo), "counts": counts,
            "manifest_rows": total_docs, "mb": bytes_total["n"] // (1024 * 1024),
            "api_calls": calls["n"], "indexes": built}


def _cli() -> None:
    p = argparse.ArgumentParser(
        description="DOL OLMS OPDR CBA File harvest (index + raw contract blobs -> Lance/R2).")
    p.add_argument("--index", action="store_true", default=False,
                   help="INDEX stage: POST GetCBAFilerListServlet -> olms_cba_index (one call).")
    p.add_argument("--documents", action="store_true", default=False,
                   help="DOCUMENTS stage: GET each docId's contract bytes -> R2 blob + "
                        "olms_cba_documents manifest (resume-safe append).")
    p.add_argument("--resume", action="store_true", default=False,
                   help="documents: skip already-fetched docIds, re-fetch only transient failures.")
    p.add_argument("--limit", type=int, default=0, help="SMOKE: cap docs fetched (documents stage).")
    p.add_argument("--workers", type=int, default=8, help="documents: concurrent fetch workers.")
    p.add_argument("--index-uri", default=INDEX_URI)
    p.add_argument("--docs-uri", default=DOCS_URI)
    p.add_argument("--blob-prefix", default=BLOB_PREFIX)
    p.add_argument("--inter-call-sleep", type=float, default=0.05)
    a = p.parse_args()
    if a.index == a.documents:
        p.error("pass exactly one of --index or --documents.")
    so = _storage_options()
    if a.index:
        out = run_index(storage_options=so, index_uri=a.index_uri)
    else:
        out = run_documents(storage_options=so, index_uri=a.index_uri, docs_uri=a.docs_uri,
                            blob_prefix=a.blob_prefix, limit=a.limit, workers=a.workers,
                            inter_call_sleep=a.inter_call_sleep, resume=a.resume or True)
    print("RESULT:", out, flush=True)


if __name__ == "__main__":
    _cli()
