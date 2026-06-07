"""SAM.gov solicitation *attachment byte download* — local/in-session runner.

Stage 2 of the attachment pipeline. Consumes the attachment MANIFEST (the pointer
layer built by ``sam_attachment_manifest.py``) and downloads the actual file bytes
(PDF/DOC/DOCX/TXT) for a prioritized, gated slice — landing them durably in R2 and
recording every physical file in an auditable Lance ledger.

PURE DETERMINISTIC I/O. No LLM, no extraction, no embedding. HTTP GET -> bytes ->
sha256 -> R2 put -> ledger row. Text extraction/embedding are downstream stages and
are explicitly out of scope here.

TWO GRAINS, TWO KEYS (do not conflate):
  * manifest row = one notice-attachment CITATION; key = ``attachment_id``.
  * this ledger  = one physical FILE;             key = ``resource_id``.
The same file is cited by many notices (331,401 citations -> 118,739 files), so we
iterate DISTINCT ``resource_id`` and download each file exactly once. The R2 object
path is identity-addressed (``<prefix>/<resource_id>``) and therefore naturally
idempotent — the repetition lives in citations, not files.

WHY THE sam.gov FRONTEND (not api.sam.gov): the public website backend
``sam.gov/api/prod/opps/v3/opportunities/resources/files/{resource_id}/download``
serves the bytes with NO api_key and NO developer quota (honors HTTP Range). The
developer gateway api.sam.gov caps non-federal keys at a tiny daily quota — useless
for a per-file sweep. The per-row ``download_url`` already encodes this endpoint.

OPERATING CONSTRAINTS (mandatory):
  * Run LOCAL/in-session from a RESIDENTIAL IP — never Modal/datacenter (SAM throttles
    shared datacenter egress with 429 on the first call). Single-threaded, ~4 req/s.
  * Creds via Doppler; deps via uv; network/R2 with the Bash sandbox disabled.
  * New Lance datasets pin ``data_storage_version="2.1"``.
  * Launch the full run DETACHED (Popen ``start_new_session=True``) so a terminal
    interruption can't kill it; monitor via ``pgrep -f sam_attachment_download`` +
    ``/tmp/sam_download.log``; relaunch with ``--resume`` on death (the size-based
    skip makes it idempotent; loss bounded by the last checkpoint window).

Output:
  * bytes  -> s3://data-sink/active/sam_attachment_blobs/<resource_id>   (R2 CAS blobs)
  * ledger -> s3://data-sink/active/sam_attachment_files/                (Lance v2.1, SoR)
  * run row-> ops.sam_attachment_download_runs                          (Postgres, per batch)

    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' \
        --with boto3 --with duckdb \
      python pipelines/sam_gov/sam_attachment_download.py --tier T0+T2 --resume

Smoke (40 files, throwaway URIs):
    ... python pipelines/sam_gov/sam_attachment_download.py --tier T0+T2 --max-files 40 \
        --ledger-uri  s3://data-sink/active/_smoke_attach_files/ \
        --blob-prefix s3://data-sink/active/_smoke_blobs/ \
        --worklist-uri s3://data-sink/active/_smoke_attach_worklist/
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os

MANIFEST_URI = os.environ.get("SAM_ATTACH_MANIFEST_URI", "s3://data-sink/active/sam_opps_attachment_manifest/")
LEDGER_URI = os.environ.get("SAM_ATTACH_LEDGER_URI", "s3://data-sink/active/sam_attachment_files/")
BLOB_PREFIX = os.environ.get("SAM_ATTACH_BLOB_PREFIX", "s3://data-sink/active/sam_attachment_blobs/")
WORKLIST_URI_TMPL = "s3://data-sink/active/sam_attachment_worklist_{tier}/"
FEED = "sam_attachment_download"

# ---- tier gate predicates (exact; SQL fragments evaluated over the manifest) -------
# Universal floor applied to every tier: drops the ~24.7% phantoms, keeps public,
# non-export-controlled files only (guardrail).
_UNIV = ("size_bytes >= 1 AND file_name IS NOT NULL "
         "AND access_level = 'public' AND export_controlled = false")
# DECLARED-size prefilter only — NOT a real-size bound. size_bytes is SAM's
# corrupted size (true mod 10 MB for >=10 MB files; see manifest KNOWN DEFECT), so
# a real >=50 MB file can declare <50 MB and PASS this gate. That is intentional and
# safe: the real 50 MB ceiling is enforced at fetch in _download_one on the
# post-redirect Content-Length AND the running stream length (-> status=oversize).
_SIZECAP = "size_bytes >= 10000 AND size_bytes < 50000000"   # declared band, not real bytes
_TEXT = "mime_type IN ('pdf','docx','doc','txt')"
_HV = "(" + " OR ".join(                                      # high-value filename signal
    f"lower(file_name) LIKE '%{k}%'" for k in
    ("sow", "pws", "statement of work", "performance work", "scope of work",
     "statement of objectives", "specification", "soo")) + ")"
_TRIG = "trigger_relevant = true"                             # NAICS 23 ∪ PSC N063/C1AZ

_TIER_PRED = {
    "T0": f"{_UNIV} AND {_TRIG} AND {_HV} AND {_SIZECAP}",
    "T1": f"{_UNIV} AND {_TRIG} AND {_TEXT} AND {_SIZECAP}",
    "T2": f"{_UNIV} AND {_TRIG} AND attachment_order = 1 AND {_TEXT}",
    "T3": f"{_UNIV} AND {_HV} AND {_SIZECAP}",
    "T4": f"{_UNIV} AND {_TEXT} AND {_SIZECAP}",
}

# Magic-byte sniff -> declared mime families it is consistent with.
_CLAIM_OK = {
    "pdf": {"pdf"},
    "docx": {"zip"}, "xlsx": {"zip"}, "pptx": {"zip"}, "zip": {"zip"},
    "doc": {"ole"}, "xls": {"ole"}, "ppt": {"ole"},
    "rtf": {"rtf"},
    "txt": {"txt"},
    "jpg": {"jpg"}, "jpeg": {"jpg"}, "png": {"png"}, "gif": {"gif"},
}
_CONTENT_TYPE = {
    "pdf": "application/pdf",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "doc": "application/msword",
    "txt": "text/plain",
}


def _tier_predicate(tier: str) -> str:
    """Compose a SQL WHERE fragment for a tier or a '+'-joined union (e.g. ``T0+T2``)."""
    parts = [p.strip() for p in tier.split("+") if p.strip()]
    try:
        preds = [_TIER_PRED[p] for p in parts]
    except KeyError as exc:
        raise SystemExit(f"unknown tier {exc}; valid: {sorted(_TIER_PRED)} or '+'-unions e.g. T0+T2")
    return " OR ".join(f"({p})" for p in preds)


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _s3_client():
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required``;
    botocore's default flexible-checksum validation otherwise raises
    FlexibleChecksumError against R2 on download."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _split_s3(uri: str) -> tuple[str, str]:
    """``s3://bucket/key/prefix/`` -> (``bucket``, ``key/prefix/``)."""
    body = uri[len("s3://"):] if uri.startswith("s3://") else uri
    bucket, _, key = body.partition("/")
    if key and not key.endswith("/"):
        key += "/"
    return bucket, key


def _headers(notice_id: str | None = None) -> dict:
    h = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
        "Accept": "application/octet-stream, */*",
        "Origin": "https://sam.gov",
    }
    if notice_id:
        h["Referer"] = f"https://sam.gov/opp/{notice_id}/view"
    return h


def _sniff_mime(head: bytes) -> str | None:
    """Magic-byte family of the leading bytes. Catches truncations and
    HTML-error-pages-saved-as-PDF (returns ``html``) so they get flagged, not stored silently."""
    if head[:4] == b"%PDF":
        return "pdf"
    if head[:4] == b"PK\x03\x04":
        return "zip"                       # docx/xlsx/pptx/zip (OOXML container)
    if head[:4] == b"\xd0\xcf\x11\xe0":
        return "ole"                       # legacy doc/xls/ppt (OLE2 compound)
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
    if head and b"\x00" not in head:       # last resort: NUL-free => plausible text
        return "txt"
    return None


def _mime_match(claimed: str | None, sniffed: str | None) -> bool:
    if not claimed:
        return False
    ok = _CLAIM_OK.get(claimed.lower())
    if ok is None:                         # unknown declared type: accept any positive sniff
        return sniffed is not None
    return sniffed in ok


def _ledger_schema():
    import pyarrow as pa

    return pa.schema([
        ("resource_id", pa.string()),
        ("status", pa.string()),
        ("http_status", pa.int32()),
        ("sha256", pa.string()),
        ("size_expected", pa.int64()),
        ("size_downloaded", pa.int64()),
        ("size_match", pa.bool_()),
        ("mime_claimed", pa.string()),
        ("mime_sniffed", pa.string()),
        ("mime_match", pa.bool_()),
        ("stored_uri", pa.string()),
        ("attempts", pa.int32()),
        ("first_attempt_at", pa.timestamp("us", tz="UTC")),
        ("completed_at", pa.timestamp("us", tz="UTC")),
        ("error", pa.string()),
        ("run_id", pa.string()),
        ("worklist_tier", pa.string()),
    ])


OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sam_attachment_download_runs (
    id bigserial PRIMARY KEY,
    run_id text NOT NULL,
    worklist_tier text,
    worklist_filter text,
    attempted int, downloaded int, failed int, restricted int, gone int, oversize int,
    bytes_downloaded bigint, sustained_mbps numeric,
    size_mismatches int, mime_mismatches int,
    status text, error text,
    started_at timestamptz, completed_at timestamptz
);
ALTER TABLE ops.sam_attachment_download_runs ADD COLUMN IF NOT EXISTS oversize int;
"""


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance

    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001
        return False


def build_worklist(tier: str, storage_options: dict, manifest_uri: str, worklist_uri: str):
    """Apply the tier gates to the live manifest, collapse to one row per distinct
    ``resource_id`` (§5c), and persist the worklist as a Lance v2.1 snapshot. Returns
    the deduped Arrow table. Re-derived every run — the active set re-snapshots daily."""
    import duckdb
    import lance

    cols = ["notice_id", "solicitation_number", "naics_code", "psc_code", "title",
            "posted_date", "ui_link", "trigger_relevant", "trigger_legs", "attachment_id",
            "resource_id", "attachment_order", "file_name", "mime_type", "size_bytes",
            "access_level", "export_controlled", "download_url", "harvested_at", "snapshot_date"]
    src = lance.dataset(manifest_uri, storage_options=storage_options).to_table(columns=cols)
    con = duckdb.connect()
    con.register("m", src)
    pred = _tier_predicate(tier)
    wl = con.execute(f"""
        SELECT * EXCLUDE (rn) FROM (
          SELECT *, row_number() OVER (PARTITION BY resource_id
                       ORDER BY attachment_order, notice_id) AS rn
          FROM m WHERE {pred}
        ) WHERE rn = 1
    """).to_arrow_table()
    # Declared LOWER BOUND: size_bytes is corrupted (mod 10 MB) for >=10 MB files, so
    # this pre-flight sum undercounts true bytes. Real bytes are summed post-download
    # from the ledger's size_downloaded (ops row bytes_downloaded).
    total_bytes_declared_lb = con.execute(f"""
        SELECT coalesce(sum(size_bytes), 0) FROM (
          SELECT size_bytes, row_number() OVER (PARTITION BY resource_id
                   ORDER BY attachment_order, notice_id) AS rn
          FROM m WHERE {pred}
        ) WHERE rn = 1
    """).fetchone()[0]
    lance.write_dataset(wl, worklist_uri, mode="overwrite",
                        data_storage_version="2.1", storage_options=storage_options)
    print(f"worklist[{tier}] distinct_files={wl.num_rows:,} "
          f"declared_bytes_LOWERBOUND={total_bytes_declared_lb:,} "
          f"(~{total_bytes_declared_lb / 1e9:.1f} GB; >=10 MB undercounted) -> {worklist_uri}", flush=True)
    return wl


def _load_prior(ledger_uri: str, so: dict) -> dict:
    """resume state: ``resource_id -> (terminal_status, size_downloaded)``.
    Terminal row = a ``downloaded`` row if any, else the last appended row."""
    import lance

    if not _dataset_exists(ledger_uri, so):
        return {}
    rows = lance.dataset(ledger_uri, storage_options=so).to_table(
        columns=["resource_id", "status", "size_downloaded"]).to_pylist()
    prior: dict = {}
    for r in rows:
        rid = r["resource_id"]
        cur = prior.get(rid)
        if cur is None or r["status"] == "downloaded" or cur["status"] != "downloaded":
            prior[rid] = r
    return {rid: (r["status"], r["size_downloaded"]) for rid, r in prior.items()}


def _download_one(session, url: str, notice_id: str | None, *,
                  connect_timeout: float = 15.0, read_timeout: float = 60.0,
                  max_bytes: int = 50_000_000, wallclock: float = 240.0):
    """Single file fetch with the exact status map, a REAL-size guard, and a
    wall-clock backstop. Returns ``(status_label, http_status, body|None, attempts, error|None)``.

    The download endpoint 303-redirects to S3; the manifest's declared ``size_bytes``
    is corrupted for >=10 MB files (verified 2026-06-06): SAM reports
    ``((true-1) mod 10_000_000)+1``, a lower bound only — a real 210 MB file declares
    10,000,000; a real 45 MB file declares ~5 MB. So the >50 MB ceiling is enforced on
    the POST-redirect Content-Length AND on the running stream length — NEVER on the
    declared size. ``read_timeout`` bounds a single socket read; ``wallclock`` bounds
    the whole transfer so a slow-roll tarpit cannot hang the run.

    Labels: downloaded | restricted | gone | oversize | failed."""
    import time

    import requests

    def _short_body(resp) -> str | None:
        try:
            return resp.text[:300]
        except Exception:  # noqa: BLE001
            return None

    last_err = None
    for attempt in range(6):
        t0 = time.time()
        try:
            resp = session.get(url, headers=_headers(notice_id),
                               timeout=(connect_timeout, read_timeout), stream=True)
        except requests.RequestException as exc:
            last_err = f"net:{type(exc).__name__}"
            wait = min(60, 2 ** attempt)
            print(f"  net {last_err} attempt{attempt} backoff {wait}s", flush=True)
            time.sleep(wait)
            continue
        sc = resp.status_code
        if sc == 200:
            clen = resp.headers.get("Content-Length")
            try:
                if clen is not None and int(clen) >= max_bytes:   # real size over cap
                    resp.close()
                    return ("oversize", sc, None, attempt + 1, f"content_length={clen}")
            except ValueError:
                pass
            buf = bytearray()
            try:
                for chunk in resp.iter_content(262144):
                    buf += chunk
                    if len(buf) >= max_bytes:                     # lying/absent Content-Length
                        resp.close()
                        return ("oversize", sc, None, attempt + 1, f"streamed>={max_bytes}")
                    if time.time() - t0 > wallclock:              # slow-roll backstop, no retry
                        resp.close()
                        return ("failed", sc, None, attempt + 1, f"wallclock>{wallclock:.0f}s@{len(buf)}B")
            except requests.RequestException as exc:
                last_err = f"stream:{type(exc).__name__}"
                wait = min(60, 2 ** attempt)
                print(f"  stream {last_err} attempt{attempt} backoff {wait}s", flush=True)
                time.sleep(wait)
                continue
            return ("downloaded", sc, bytes(buf), attempt + 1, None)
        if sc == 401:
            return ("restricted", sc, None, attempt + 1, _short_body(resp))
        if sc == 403:                                   # never auto-retry an auth refusal
            body = _short_body(resp) or ""
            if "UNAUTHORIZED" in body.upper():
                return ("restricted", sc, None, attempt + 1, body)
            return ("failed", sc, None, attempt + 1, f"403:{body}")
        if sc in (400, 410):                            # removed/dead resource
            return ("gone", sc, None, attempt + 1, _short_body(resp))
        if sc in (429, 503) or sc >= 500:               # throttle / transient -> back off
            wait = min(120, 5 * 2 ** attempt)
            last_err = f"http{sc}"
            print(f"  {sc} backoff {wait}s", flush=True)
            time.sleep(wait)
            continue
        return ("failed", sc, None, attempt + 1, _short_body(resp))   # other hard 4xx
    return ("failed", 0, None, 6, last_err or "retries exhausted")


def _record_run(stats: dict, dsn: str | None) -> None:
    if not dsn:
        print("WARN: no HQX_DB_URL_POOLED; skipping ops.* write.", flush=True)
        return
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.sam_attachment_download_runs
                  (run_id, worklist_tier, worklist_filter, attempted, downloaded, failed,
                   restricted, gone, oversize, bytes_downloaded, sustained_mbps, size_mismatches,
                   mime_mismatches, status, error, started_at, completed_at)
                VALUES (%(run_id)s,%(worklist_tier)s,%(worklist_filter)s,%(attempted)s,
                   %(downloaded)s,%(failed)s,%(restricted)s,%(gone)s,%(oversize)s,
                   %(bytes_downloaded)s,%(sustained_mbps)s,%(size_mismatches)s,%(mime_mismatches)s,
                   %(status)s,%(error)s,%(started_at)s,%(completed_at)s)
                """,
                stats,
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}", flush=True)


def run_download(
    *,
    storage_options: dict,
    dsn: str | None,
    tier: str,
    manifest_uri: str = MANIFEST_URI,
    ledger_uri: str = LEDGER_URI,
    blob_prefix: str = BLOB_PREFIX,
    worklist_uri: str | None = None,
    run_id: str = "adhoc",
    resume: bool = False,
    max_files: int = 0,
    inter_call_sleep: float = 0.2,
    checkpoint_every: int = 1000,
    read_timeout: float = 60.0,
    connect_timeout: float = 15.0,
    max_bytes: int = 50_000_000,
    wallclock: float = 240.0,
) -> dict:
    import time

    import lance
    import pyarrow as pa
    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    schema = _ledger_schema()
    bucket, key_prefix = _split_s3(blob_prefix)
    worklist_uri = worklist_uri or WORKLIST_URI_TMPL.format(tier=tier.replace("+", "_"))
    s3 = _s3_client()
    session = requests.Session()

    files = build_worklist(tier, storage_options, manifest_uri, worklist_uri).to_pylist()
    if max_files:
        files = files[:max_files]
    prior = _load_prior(ledger_uri, storage_options) if resume else {}
    print(f"run_id={run_id} tier={tier} worklist={len(files):,} resume_known={len(prior):,}", flush=True)

    c = {"attempted": 0, "downloaded": 0, "failed": 0, "restricted": 0, "gone": 0,
         "oversize": 0, "skipped": 0, "bytes": 0, "size_mismatch": 0, "mime_mismatch": 0}
    buf: list[dict] = []
    seen: set[str] = set()
    final_status, error_text, mbps = "error", None, 0.0
    t_dl_start = None

    def flush(tag: str) -> None:
        if not buf:
            return
        at = pa.Table.from_pylist(buf, schema=schema)
        mode = "append" if _dataset_exists(ledger_uri, storage_options) else "create"
        lance.write_dataset(at, ledger_uri, mode=mode,
                            data_storage_version="2.1", storage_options=storage_options)
        print(f"[{tag}] +{len(buf)} ledger rows (dl={c['downloaded']} fail={c['failed']} "
              f"restr={c['restricted']} gone={c['gone']} oversize={c['oversize']} skip={c['skipped']})", flush=True)
        buf.clear()

    try:
        for i, f in enumerate(files):
            rid = f["resource_id"]
            if rid in seen:
                continue
            seen.add(rid)

            # ---- resume skip (size-based; restricted/gone are permanent) -------------
            p = prior.get(rid)
            if p:
                st, sz = p
                if st in ("restricted", "gone"):
                    c["skipped"] += 1
                    continue
                if st == "downloaded":
                    try:
                        h = s3.head_object(Bucket=bucket, Key=f"{key_prefix}{rid}")
                        if h["ContentLength"] == sz:
                            c["skipped"] += 1
                            continue
                    except Exception:  # noqa: BLE001
                        pass   # object vanished -> fall through and re-download

            # ---- fetch --------------------------------------------------------------
            if t_dl_start is None:
                t_dl_start = time.time()
            first_at = dt.datetime.now(dt.timezone.utc)
            label, sc, body, attempts, err = _download_one(
                session, f["download_url"], f.get("notice_id"),
                connect_timeout=connect_timeout, read_timeout=read_timeout,
                max_bytes=max_bytes, wallclock=wallclock)
            c["attempted"] += 1
            row = {
                "resource_id": rid, "status": label, "http_status": int(sc),
                "sha256": None, "size_expected": int(f["size_bytes"] or 0),
                "size_downloaded": None, "size_match": None,
                "mime_claimed": f.get("mime_type"), "mime_sniffed": None, "mime_match": None,
                "stored_uri": None, "attempts": int(attempts),
                "first_attempt_at": first_at, "completed_at": dt.datetime.now(dt.timezone.utc),
                "error": err, "run_id": run_id, "worklist_tier": tier,
            }
            if label == "downloaded" and body is not None:
                size_dl = len(body)
                claimed = (f.get("mime_type") or "").lower()
                sniffed = _sniff_mime(body[:512])
                # CONSISTENCY, not raw equality: SAM corrupts >=10 MB declared sizes to
                # ((true-1) mod 10_000_000)+1 (manifest KNOWN DEFECT), so size_dl==decl
                # false-flags every >=10 MB file. Match iff the declared value is that
                # modulo image of the bytes we actually got; a false then means a REAL
                # anomaly (truncation / wrong file) — what the <0.5% gate exists to catch.
                decl = int(f["size_bytes"] or 0)
                size_match = decl == ((((size_dl - 1) % 10_000_000) + 1) if size_dl >= 1 else 0)
                m_match = _mime_match(claimed, sniffed)
                key = f"{key_prefix}{rid}"
                s3.put_object(Bucket=bucket, Key=key, Body=body,
                              ContentType=_CONTENT_TYPE.get(claimed, "application/octet-stream"))
                row.update({"sha256": hashlib.sha256(body).hexdigest(),
                            "size_downloaded": size_dl, "size_match": size_match,
                            "mime_sniffed": sniffed, "mime_match": m_match,
                            "stored_uri": f"s3://{bucket}/{key}"})
                c["downloaded"] += 1
                c["bytes"] += size_dl
                c["size_mismatch"] += int(not size_match)
                c["mime_mismatch"] += int(not m_match)
            else:
                c[label] = c.get(label, 0) + 1

            buf.append(row)
            if inter_call_sleep:
                time.sleep(inter_call_sleep)
            if checkpoint_every and c["attempted"] and c["attempted"] % checkpoint_every == 0:
                pct = 100 * (i + 1) / len(files)
                print(f"  progress {i + 1}/{len(files)} ({pct:.0f}%) "
                      f"attempted={c['attempted']} dl={c['downloaded']} skip={c['skipped']}", flush=True)
                flush(f"ckpt_{c['attempted']}")
        flush("final")
        final_status = "success"
    except KeyboardInterrupt:
        final_status = "interrupted"
        print("interrupted — checkpointing", flush=True)
        flush("interrupt")
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
        final_status = "error"
        print(f"FATAL: {exc}", flush=True)
        try:
            flush("salvage")
        except Exception as e2:  # noqa: BLE001
            print(f"salvage flush failed: {e2}", flush=True)
    finally:
        # indices ONCE, at the very end — never per checkpoint.
        try:
            lds = lance.dataset(ledger_uri, storage_options=storage_options)
            for col, it in [("resource_id", "BTREE"), ("sha256", "BTREE"),
                            ("status", "BITMAP"), ("worklist_tier", "BITMAP")]:
                try:
                    lds.create_scalar_index(col, index_type=it, replace=True)
                except Exception as ie:  # noqa: BLE001
                    print(f"index {col} skipped: {ie}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"index phase skipped: {exc}", flush=True)

        completed_at = dt.datetime.now(dt.timezone.utc)
        elapsed = max(1e-9, time.time() - t_dl_start) if t_dl_start else 0.0
        mbps = (c["bytes"] / 1e6) / elapsed if elapsed else 0.0
        stats = {
            "run_id": run_id, "worklist_tier": tier,
            "worklist_filter": f"tier={tier}; pred={_tier_predicate(tier)}",
            "attempted": c["attempted"], "downloaded": c["downloaded"], "failed": c["failed"],
            "restricted": c["restricted"], "gone": c["gone"], "oversize": c["oversize"],
            "bytes_downloaded": c["bytes"], "sustained_mbps": round(mbps, 3),
            "size_mismatches": c["size_mismatch"], "mime_mismatches": c["mime_mismatch"],
            "status": final_status, "error": error_text,
            "started_at": started_at, "completed_at": completed_at,
        }
        _record_run(stats, dsn)
        # acceptance ratio carves out the legitimately-undownloadable buckets.
        denom = max(1, len(files) - c["gone"] - c["restricted"] - c["oversize"])
        print("SUMMARY:", {k: v for k, v in stats.items() if k != "worklist_filter"}, flush=True)
        print(f"sustained ~{mbps:.2f} MB/s over {c['bytes'] / 1e9:.2f} GB; skipped={c['skipped']}; "
              f"acceptance dl/(worklist-gone-restricted-oversize) = "
              f"{(c['downloaded'] + c['skipped']) / denom:.4f}", flush=True)

    return {"status": final_status, "sustained_mbps": round(mbps, 3),
            **{k: c[k] for k in ("attempted", "downloaded", "failed", "restricted",
                                 "gone", "oversize", "skipped")}}


def _cli() -> None:
    p = argparse.ArgumentParser(description="SAM.gov attachment byte downloader (distinct-file).")
    p.add_argument("--tier", default="T0+T2", help="T0|T1|T2|T3|T4 or '+'-union e.g. T0+T2")
    p.add_argument("--resume", action="store_true", default=False)
    p.add_argument("--max-files", type=int, default=0, help="cap distinct files (smoke)")
    p.add_argument("--inter-call-sleep", type=float, default=0.2)
    p.add_argument("--checkpoint-every", type=int, default=1000)
    p.add_argument("--blob-prefix", default=BLOB_PREFIX)
    p.add_argument("--ledger-uri", default=LEDGER_URI)
    p.add_argument("--worklist-uri", default=None)
    p.add_argument("--manifest-uri", default=MANIFEST_URI)
    p.add_argument("--run-id", default=None)
    p.add_argument("--read-timeout", type=float, default=60.0, help="per-socket-read timeout (s)")
    p.add_argument("--connect-timeout", type=float, default=15.0)
    p.add_argument("--wallclock", type=float, default=240.0, help="hard per-file transfer budget (s)")
    p.add_argument("--max-bytes", type=int, default=50_000_000, help="real-size ceiling; >= => oversize")
    a = p.parse_args()
    run_id = a.run_id or f"{a.tier}-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
    out = run_download(
        storage_options=_r2_storage_options(), dsn=os.environ.get("HQX_DB_URL_POOLED"),
        tier=a.tier, manifest_uri=a.manifest_uri, ledger_uri=a.ledger_uri,
        blob_prefix=a.blob_prefix, worklist_uri=a.worklist_uri, run_id=run_id,
        resume=a.resume, max_files=a.max_files, inter_call_sleep=a.inter_call_sleep,
        checkpoint_every=a.checkpoint_every, read_timeout=a.read_timeout,
        connect_timeout=a.connect_timeout, max_bytes=a.max_bytes, wallclock=a.wallclock,
    )
    print("RESULT:", out, flush=True)


if __name__ == "__main__":
    _cli()
