"""OPM CBA Database — federal-employee agency⇄union Collective Bargaining Agreements.

Lands the EO-13836 Collective Bargaining Agreements collection — every federal-sector
agency/union CBA OPM is statutorily required to publish — into the Gen-3 Lance SoR + R2
raw-blob store. This is the FEDERAL-EMPLOYEE labor layer; it complements naf_wage_rates
via the NAF slice (exchange/MWR nonappropriated-fund instrumentalities carry negotiated
wage appendices) and is the sibling of the private-sector olms_cba_* corpus. It does NOT
serve the SAM.gov §4(c) service-contractor CBA join (those are contractor CBAs on OLMS).

REVERSE-ENGINEERED + VERIFIED LIVE (residential IP, 2026-07-11). The public app at
opm.gov/policy-data-oversight/labor-relations/collective-bargaining-agreements/ is an
AngularJS SPA over an ASP.NET Web API. Key-less, cookie-less, CSRF-less; a WAF challenges
CLI request bursts, so requests are browser-shaped (UA/Origin/Referer) and politely paced.

  STAGE 1 — INDEX (--index): the catalog, paginated.
    POST https://www.opm.gov/cba/api/documents/published
      Content-Type: application/json; charset=utf-8
      body: {"sortBy":"agencynameAsc","agencyIds":[],"subAgencyNames":[],
             "activityOfficeRegions":[],"laborUnionNames":[],"locals":[],"busCodes":[],
             "currentPage":N,"recsPerPage":20,"searchString":""}
      -> {results:[...], currentPage, pageCount, pageSize, rowCount, firstRowOnPage, lastRowOnPage}
      ⚠ THE TRAP: sortBy MUST be the exact UI casing "agencynameAsc". An invalid value
        silently poisons ASP.NET model binding — the server 200s but IGNORES currentPage/
        filters and returns page 1 of everything. Verified: correct payload yields p1≠p2 and
        rowCount=1248, pageCount=63 at recsPerPage=20 (server caps recsPerPage ~20).
      Per-record: id(UUID), documentType, agencyName, subAgencyOrComponent, activityOfficeRegion,
        laborUnionName, local, busCodes[], expirationDate(ISO), fileUrl(direct public PDF),
        fileName, fileSize(human "1.76 MB"). `highlights` is a search-relevance artifact, dropped.
    Pages are fetched 1..pageCount, deduped on `id`, and reconciled: distinct ids == rowCount
    (re-read live at run time). FAILS CLOSED on mismatch — never writes a partial index.

  STAGE 2 — DOCUMENTS (--documents): one key-less GET per fileUrl -> raw bytes to R2.
    fileUrl is https://www.opm.gov/cba/api/documents/{id}/attachments/{fileName} (the SAME
    www.opm.gov WAF host — not a separate CDN), directly fetchable, no auth (Google indexes
    them). Raw bytes land verbatim under active/opm_cba_blobs/{id}.{ext}; a Lance manifest
    (opm_cba_documents) carries (id, r2_key, content_type, byte_len, sha256, fetch_status, ...)
    — the unification surface (id joins opm_cba_index). Bounded concurrency + exponential
    backoff on 403/429/503; a 200 text/html body is treated as a WAF interstitial (retried,
    never stored as a document). Resume-safe append: skips ids already fetched (non-retry status).

Output (Lance v2.1, Gen-3 active/ SoR + R2 blobs):
    s3://data-sink/active/opm_cba_index/       one row per CBA doc (1,248)             [--index]
    s3://data-sink/active/opm_cba_documents/   one row per distinct id (~1,248)         [--documents]
    s3://data-sink/active/opm_cba_blobs/{id}.{ext}   raw contract bytes (R2 objects)    [--documents]

    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with requests --with boto3 --with truststore \
        --with 'psycopg[binary]' \
      python pipelines/opm/opm_cba.py --index                  # stage 1: metadata catalog
    ... python pipelines/opm/opm_cba.py --documents --resume    # stage 2: raw contract blobs

Smoke (cap docs, throwaway URIs/prefix):
    ... --index     --index-uri s3://data-sink/active/_smoke_opm_index/
    ... --documents --limit 20 --docs-uri s3://data-sink/active/_smoke_opm_docs/ \
                    --blob-prefix active/_smoke_opm_blobs/
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import time

# Route TLS trust through the OS store (matches the OLMS harvest convention); falls back to
# certifi if truststore is absent. www.opm.gov ships a standard CA chain, so this is belt-and-
# suspenders — but keeps the fleet crawl behavior uniform.
try:
    import truststore

    truststore.inject_into_ssl()
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

INDEX_URI = os.environ.get("OPM_CBA_INDEX_URI", f"s3://{BUCKET}/active/opm_cba_index/")
DOCS_URI = os.environ.get("OPM_CBA_DOCS_URI", f"s3://{BUCKET}/active/opm_cba_documents/")
BLOB_PREFIX = os.environ.get("OPM_CBA_BLOB_PREFIX", "active/opm_cba_blobs/")

PUBLISHED_URL = "https://www.opm.gov/cba/api/documents/published"
# EXACT UI casing — an invalid sortBy silently poisons ASP.NET model binding (see module docstring).
SORT_BY = "agencynameAsc"
RECS_PER_PAGE = 20  # server caps ~20; UI default is 10

INDEX_SOURCE = "www.opm.gov/cba/api/documents/published (frontend, no api_key)"
DOCS_SOURCE = "www.opm.gov/cba/api/documents/{id}/attachments (frontend, no api_key)"

# Content-Type -> file extension for the raw-blob key. Fallback chain in _ext_for().
_CT_EXT = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/msword": "doc",
    "application/rtf": "rtf",
    "text/plain": "txt",
    "application/zip": "zip",
    "image/tiff": "tif",
}

_SIZE_UNITS = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}


def _headers() -> dict:
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.opm.gov",
        "Referer": "https://www.opm.gov/cba/",
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


def _str_list(v) -> list[str]:
    """busCodes[] -> clean list of non-empty strings (never None; empty list if absent)."""
    if not isinstance(v, (list, tuple)):
        return []
    out = []
    for x in v:
        s = _s(x)
        if s:
            out.append(s)
    return out


def _size_bytes(v) -> int | None:
    """Human file-size string ("1.76 MB", "800.4 KB") -> integer bytes. Unparseable -> None."""
    s = _s(v)
    if not s:
        return None
    m = re.match(r"^\s*([\d,]+(?:\.\d+)?)\s*([KMGT]?B)\s*$", s, re.I)
    if not m:
        return None
    num = float(m.group(1).replace(",", ""))
    unit = m.group(2).upper()
    mult = _SIZE_UNITS.get(unit)
    return int(round(num * mult)) if mult else None


def _ext_for(content_type: str | None, file_name: str | None) -> str:
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct in _CT_EXT:
        return _CT_EXT[ct]
    fn = (file_name or "").strip().lower()
    if "." in fn:
        ext = fn.rsplit(".", 1)[-1]
        if ext.isalnum() and len(ext) <= 5:
            return ext
    return "bin"


# ── ops.opm_cba_runs ledger ─────────────────────────────────────────────────────────
def _record_run(dataset: str, uri: str, rows: int, built: list, status: str,
                error: str | None, coverage: dict, started_at, completed_at) -> None:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.opm_cba_runs write.", flush=True)
        return
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ops.opm_cba_runs (
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
                INSERT INTO ops.opm_cba_runs
                  (dataset, dataset_uri, rows_processed, indexes_built, coverage,
                   status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (dataset, uri, rows, built, json.dumps(coverage), status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must never mask a good crawl
        print(f"WARN: ops.opm_cba_runs write failed: {exc}", flush=True)


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
def _post_page(session, page: int, recs_per_page: int) -> dict:
    """POST one catalog page. Retries transient/WAF responses with backoff; a persistently
    non-JSON body (WAF challenge) raises after exhaustion so the caller can FAIL CLOSED."""
    import requests

    payload = {
        "sortBy": SORT_BY, "agencyIds": [], "subAgencyNames": [], "activityOfficeRegions": [],
        "laborUnionNames": [], "locals": [], "busCodes": [],
        "currentPage": page, "recsPerPage": recs_per_page, "searchString": "",
    }
    body = json.dumps(payload)
    hdrs = {**_headers(), "Content-Type": "application/json; charset=utf-8"}
    for attempt in range(6):
        try:
            resp = session.post(PUBLISHED_URL, data=body, headers=hdrs, timeout=120)
        except requests.RequestException:
            time.sleep(min(30, 2 ** attempt)); continue
        sc = resp.status_code
        if sc == 200:
            try:
                return resp.json()
            except ValueError:  # non-JSON -> WAF interstitial; back off and retry
                time.sleep(min(120, 5 * 2 ** attempt)); continue
        if sc in (403, 429, 503):
            time.sleep(min(120, 5 * 2 ** attempt)); continue
        if sc >= 500:
            time.sleep(min(30, 2 ** attempt)); continue
        raise RuntimeError(f"published POST page={page} HTTP {sc}: {(resp.text or '')[:200]}")
    raise RuntimeError(
        f"published POST page={page}: retries exhausted (WAF challenge or transient failure).")


def _row_from(r: dict, crawled_at) -> dict:
    return {
        "id": _s(r.get("id")),
        "document_type": _int(r.get("documentType")),
        "agency_name": _s(r.get("agencyName")),
        "sub_agency_or_component": _s(r.get("subAgencyOrComponent")),
        "activity_office_region": _s(r.get("activityOfficeRegion")),
        "labor_union_name": _s(r.get("laborUnionName")),
        "local": _s(r.get("local")),
        "bus_codes": _str_list(r.get("busCodes")),
        "expiration_date": _s(r.get("expirationDate")),  # already ISO-8601; kept verbatim
        "file_url": _s(r.get("fileUrl")),
        "file_name": _s(r.get("fileName")),
        "file_size": _s(r.get("fileSize")),               # human string, lossless
        "file_size_bytes": _size_bytes(r.get("fileSize")),  # derived, for crawl sizing
        "source": INDEX_SOURCE,
        "crawled_at": crawled_at,
    }


def _index_schema():
    import pyarrow as pa
    return pa.schema([
        ("id", pa.string()), ("document_type", pa.int32()), ("agency_name", pa.string()),
        ("sub_agency_or_component", pa.string()), ("activity_office_region", pa.string()),
        ("labor_union_name", pa.string()), ("local", pa.string()),
        ("bus_codes", pa.list_(pa.string())), ("expiration_date", pa.string()),
        ("file_url", pa.string()), ("file_name", pa.string()), ("file_size", pa.string()),
        ("file_size_bytes", pa.int64()), ("source", pa.string()),
        ("crawled_at", pa.timestamp("us", tz="UTC")),
    ])


def run_index(*, storage_options: dict, index_uri: str = INDEX_URI,
              recs_per_page: int = RECS_PER_PAGE, inter_page_sleep: float = 0.25) -> dict:
    import lance
    import pyarrow as pa
    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    crawled_at = started_at
    so = storage_options
    session = requests.Session()

    # Page 1 establishes the live rowCount / pageCount (the fail-closed reconciliation target).
    first = _post_page(session, 1, recs_per_page)
    row_count = _int(first.get("rowCount"))
    page_count = _int(first.get("pageCount"))
    if not row_count or not page_count:
        raise RuntimeError(f"index page 1 missing rowCount/pageCount: keys={list(first.keys())}")
    print(f"live rowCount={row_count} pageCount={page_count} pageSize={first.get('pageSize')}",
          flush=True)

    by_id: dict[str, dict] = {}
    for r in (first.get("results") or []):
        row = _row_from(r, crawled_at)
        if row["id"]:
            by_id[row["id"]] = row

    for page in range(2, page_count + 1):
        if inter_page_sleep:
            time.sleep(inter_page_sleep)
        j = _post_page(session, page, recs_per_page)
        results = j.get("results") or []
        if not results:
            raise RuntimeError(f"index page {page}/{page_count} returned 0 results (unexpected).")
        for r in results:
            row = _row_from(r, crawled_at)
            if row["id"]:
                by_id[row["id"]] = row
        if page % 10 == 0 or page == page_count:
            print(f"  catalog page {page}/{page_count}  distinct_ids={len(by_id)}", flush=True)

    rows = list(by_id.values())
    # FAIL CLOSED on reconciliation gap — never write a partial catalog.
    if len(rows) != row_count:
        raise RuntimeError(
            f"RECONCILIATION FAILED: distinct ids={len(rows)} != live rowCount={row_count}. "
            "Refusing to write a partial index.")

    schema = _index_schema()
    status, error_text, built = "error", None, []
    try:
        tbl = pa.Table.from_pylist(rows, schema=schema)
        lance.write_dataset(tbl, index_uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                            storage_options=so)
        print(f"wrote {tbl.num_rows} index rows -> {index_uri}", flush=True)
        built = _build_indexes(index_uri, btree=["id", "agency_name"],
                               bitmap=["labor_union_name", "document_type"], so=so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc); print(f"FATAL: {exc}", flush=True); raise
    finally:
        from collections import Counter
        completed_at = dt.datetime.now(dt.timezone.utc)
        total_bytes = sum(r["file_size_bytes"] or 0 for r in rows)
        cov = {
            "row_count": row_count, "rows": len(rows), "page_count": page_count,
            "distinct_agencies": len({r["agency_name"] for r in rows}),
            "distinct_unions": len({r["labor_union_name"] for r in rows}),
            "document_type": dict(Counter(r["document_type"] for r in rows)),
            "est_blob_mb": total_bytes // (1024 * 1024),
            "file_size_parsed": sum(1 for r in rows if r["file_size_bytes"] is not None),
        }
        _record_run("opm_cba_index", index_uri, len(rows), built, status, error_text,
                    cov, started_at, completed_at)
        print(f"INDEX SUMMARY: rows={len(rows)} coverage={cov} status={status}", flush=True)
    return {"status": status, "rows": len(rows), "indexes": built,
            "row_count": row_count, "est_blob_mb": total_bytes // (1024 * 1024)}


# ═══════════════════════════════════════════════════════════════════════════════════
# STAGE 2 — DOCUMENTS (raw contract bytes -> R2 blobs + Lance manifest)
# ═══════════════════════════════════════════════════════════════════════════════════
def _docs_schema():
    import pyarrow as pa
    return pa.schema([
        ("id", pa.string()), ("document_type", pa.int32()), ("r2_bucket", pa.string()),
        ("r2_key", pa.string()), ("content_type", pa.string()), ("file_ext", pa.string()),
        ("byte_len", pa.int64()), ("sha256", pa.string()), ("http_status", pa.int32()),
        ("fetch_status", pa.string()), ("fetched_at", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),
    ])


def run_documents(*, storage_options: dict, index_uri: str = INDEX_URI, docs_uri: str = DOCS_URI,
                  blob_prefix: str = BLOB_PREFIX, limit: int = 0, workers: int = 6,
                  inter_call_sleep: float = 0.1, resume: bool = True,
                  checkpoint_every: int = 100) -> dict:
    """Fetch the raw contract bytes for every id on the index -> R2 blob + manifest.

    Resume-safe (append): skips ids already fetched (non-retry status). A per-doc http_4xx
    records fetch_status='http_NNN'; only transient exhaustion is left to a --resume re-run.
    A 200 text/html body is a WAF interstitial — retried, never stored as a document."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import lance
    import pyarrow as pa
    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    so = storage_options
    s3 = _s3_client()

    idx = lance.dataset(index_uri, storage_options=so).to_table(
        columns=["id", "document_type", "file_url", "file_name"]).to_pylist()
    by_id: dict[str, dict] = {}
    for r in idx:
        i = r["id"]
        if i is None or i in by_id or not r["file_url"]:
            continue
        by_id[i] = r
    work = list(by_id.values())

    done: set = set()
    if resume and _dataset_exists(docs_uri, so):
        prior = lance.dataset(docs_uri, storage_options=so).to_table(
            columns=["id", "fetch_status"]).to_pylist()
        done = {p["id"] for p in prior
                if p["fetch_status"] and not p["fetch_status"].startswith("retry")}
    todo = [w for w in work if w["id"] not in done]
    if limit:
        todo = todo[:limit]
    print(f"documents stage: distinct_ids={len(work)} already_done={len(done)} todo={len(todo)}",
          flush=True)

    session = requests.Session()
    calls = {"n": 0}
    schema = _docs_schema()

    def fetch_doc(item: dict) -> dict:
        doc_id = item["id"]
        url = requests.utils.requote_uri(item["file_url"])  # percent-encode spaces in fileName
        row = {"id": doc_id, "document_type": item.get("document_type"),
               "r2_bucket": None, "r2_key": None, "content_type": None, "file_ext": None,
               "byte_len": 0, "sha256": None, "http_status": -1,
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
                ct_raw = _s(resp.headers.get("Content-Type"))
                ct = ct_raw.split(";")[0].strip().lower() if ct_raw else None
                # WAF interstitial guard: the real attachment is a PDF/doc, never text/html.
                if ct == "text/html":
                    time.sleep(min(120, 5 * 2 ** attempt)); continue
                ext = _ext_for(ct, item.get("file_name"))
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
            futs = {ex.submit(fetch_doc, w): w["id"] for w in todo}
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
        built = _build_indexes(docs_uri, btree=["id", "sha256"],
                               bitmap=["content_type", "fetch_status", "document_type"], so=so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc); print(f"FATAL: {exc}", flush=True); raise
    finally:
        total_docs = (lance.dataset(docs_uri, storage_options=so).count_rows()
                      if _dataset_exists(docs_uri, so) else 0)
        completed_at = dt.datetime.now(dt.timezone.utc)
        cov = {"counts": counts, "mb_downloaded": bytes_total["n"] // (1024 * 1024),
               "manifest_rows": total_docs, "distinct_ids": len(work), "api_calls": calls["n"]}
        print(f"DOCS SUMMARY: todo={len(todo)} {counts} manifest_rows={total_docs} "
              f"{bytes_total['n'] // (1024*1024)}MB indexes={built} status={status} "
              f"error={error_text}", flush=True)
        _record_run("opm_cba_documents", docs_uri, counts["fetched"], built, status, error_text,
                    cov, started_at, completed_at)
    return {"status": status, "todo": len(todo), "counts": counts,
            "manifest_rows": total_docs, "mb": bytes_total["n"] // (1024 * 1024),
            "api_calls": calls["n"], "indexes": built}


def _cli() -> None:
    p = argparse.ArgumentParser(
        description="OPM CBA Database harvest (index + raw contract blobs -> Lance/R2).")
    p.add_argument("--index", action="store_true", default=False,
                   help="INDEX stage: paginate published API -> opm_cba_index (fail-closed reconcile).")
    p.add_argument("--documents", action="store_true", default=False,
                   help="DOCUMENTS stage: GET each fileUrl -> R2 blob + opm_cba_documents manifest "
                        "(resume-safe append).")
    p.add_argument("--resume", action="store_true", default=False,
                   help="documents: skip already-fetched ids, re-fetch only transient failures.")
    p.add_argument("--limit", type=int, default=0, help="SMOKE: cap docs fetched (documents stage).")
    p.add_argument("--workers", type=int, default=6, help="documents: concurrent fetch workers.")
    p.add_argument("--index-uri", default=INDEX_URI)
    p.add_argument("--docs-uri", default=DOCS_URI)
    p.add_argument("--blob-prefix", default=BLOB_PREFIX)
    p.add_argument("--inter-call-sleep", type=float, default=0.1)
    p.add_argument("--inter-page-sleep", type=float, default=0.25)
    a = p.parse_args()
    if a.index == a.documents:
        p.error("pass exactly one of --index or --documents.")
    so = _storage_options()
    if a.index:
        out = run_index(storage_options=so, index_uri=a.index_uri,
                        inter_page_sleep=a.inter_page_sleep)
    else:
        out = run_documents(storage_options=so, index_uri=a.index_uri, docs_uri=a.docs_uri,
                            blob_prefix=a.blob_prefix, limit=a.limit, workers=a.workers,
                            inter_call_sleep=a.inter_call_sleep, resume=a.resume or True)
    print("RESULT:", out, flush=True)


if __name__ == "__main__":
    _cli()
