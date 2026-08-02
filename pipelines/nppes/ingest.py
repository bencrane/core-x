"""Compute worker — CMS NPPES monthly full-replacement snapshot ingest.

Part of the ``nppes-pipelines`` Modal app. Endpoint-less; spawned by the Universal
Dispatcher (core/modal_dispatcher.py), by the Trigger.dev cron task
(src/trigger/nppes_monthly.ts), or driven by the local entrypoints. Clean-room data
plane: no Iceberg, no Polaris — DuckDB does 100% of the transform, Lance is the system
of record on R2.

THE NPI registry. CMS publishes the National Plan & Provider Enumeration System as a
single monthly FULL REPLACEMENT (no deltas, no history): one ~1 GB ZIP whose core
member ``npidata_pfile_<dates>.csv`` is the ~8.5M-provider, ~330-column registry. The
NPI is the universal primary key for the entire US healthcare provider graph.

Source acquisition is DYNAMIC — the download URL is never hard-coded (CMS rotates the
month + a version suffix):
  1. GET https://download.cms.gov/nppes/NPI_Files.html.
  2. Isolate the CURRENT monthly full replacement, named
     ``NPPES_Data_Dissemination_<Month>_<Year>[_V<n>].zip``. STRICT EXCLUSION of the
     weekly incrementals (``..._<MMDDYY>_<MMDDYY>_Weekly...zip``) and the deactivated
     reports (``NPPES_Deactivated_NPI_Report_...zip``).
  3. Among matches, pick the latest (year, month, version).
  Verified against the live page 2026-06-01: the page listed five .zip hrefs — one
  monthly (``NPPES_Data_Dissemination_May_2026_V2.zip`` — note the ``_V2`` reissue
  suffix a naive ``_<Month>_<Year>.zip`` anchor would MISS), one deactivated report,
  three weeklies — and the selector isolated exactly the one monthly full replacement.
  The weeklies share the ``NPPES_Data_Dissemination_`` prefix, so the prefix alone is
  insufficient: the ``[A-Z][a-z]+`` month-name token rejects their numeric date range,
  and the explicit Weekly/Deactivated exclusions are belt-and-suspenders.

Core member only: ``^npidata_pfile_\\d{8}-\\d{8}\\.csv$`` is extracted; the
``_FileHeader.csv`` sidecars, the ``pl_/othername_/endpoint_`` secondary files, and the
readme PDFs are ignored (directive: "extract the core CSV file").

Encoding (load-bearing — the wrong choice silently corrupts provider names). DuckDB's
core ``read_csv`` ``encoding`` accepts only {utf-8, utf-16, latin-1}, and BOTH the utf-8
AND the latin-1 readers are STRICT — verified: the latin-1 reader rejects C1-control
bytes, and the utf-8 reader hard-fails ("This file is not utf-8 encoded") on a raw
latin-1 file. So neither is a universal "accept any byte" path. We therefore detect and,
only when needed, transcode (the SAM/PPP transcode-on-write lesson):
    stream a strict incremental utf-8 decode over the whole CSV → if it fully validates,
    read it directly as utf-8 (the modern/common case, zero transcode cost); else
    transcode the file latin-1 → utf-8 in Python (latin-1 DECODE never raises on any
    byte; 0xA0-0xFF accented letters — the bulk of real non-ASCII in US provider data —
    map identically to cp1252) and read THAT as utf-8.
The DuckDB read is therefore ALWAYS ``encoding='utf-8'``. The detected source encoding
is recorded in ops.nppes_runs.

Transform (100% DuckDB; ``read_csv`` honoring the directive's ``read_csv_auto`` intent
with the verified NPPES RFC-4180 dialect pinned for deterministic 10 GB parsing):
``all_varchar=true`` + ``normalize_names=true`` turns the official header names into the
directive's exact snake_case columns (verified: "NPI" → ``npi``; "Provider First Line
Business Practice Location Address" → ``provider_first_line_business_practice_location_address``;
"Provider Business Practice Location Address State Name" →
``provider_business_practice_location_address_state_name``). The projection is built from
the runtime DESCRIBE — every one of the ~330 columns is retained losslessly as
``nullif(trim(col), '')`` under its exact normalized name, so a downstream resolver gets
the full record and the build is robust to CMS column drift. NO hand-listed schema (the
"(Legal Business Name)" → ``..._legal_business_name`` normalization drops the trailing
separator — derive names, never guess them). NO VARIANT, NO numeric cast (NPI and every
identifier stay VARCHAR — lexical-join safety / leading-zero preservation).

Scale & write path (the pdl / sam_gov / fmcsa lesson). At 8.5M rows × ~330 cols the
projection is streamed (``to_arrow_reader`` — bounded RSS, never a full materialization)
and the dataset is written to LOCAL disk, indexed locally, then PUBLISHED to R2 with
boto3 — NOT written directly to R2. A direct Lance write to R2 trips R2's multipart rule
("all non-trailing parts must have the same length", 400 InvalidPart) once a scalar-index
``page_data.lance`` file is large enough to force object_store's adaptive multipart to
escalate part size mid-upload — and the BTREE on
``provider_first_line_business_practice_location_address`` (8.5M near-unique ~30-char
strings) is squarely in that class. Local stage + boto3 publish (uniform parts) is the
R2-compliant transport. Lance is still the format, R2 is still the system of record, the
URI is still a plain ``s3://`` string — only the upload transport changes.

Partitioning (directive — CMS gives NO history, so we build our own ledger). Each month
is a DISTINCT immutable Lance dataset at
``s3://data-sink/active/nppes/snapshot=YYYY-MM/``. ``snapshot_month`` is the partition
key (stamped on every row); a re-run of the same month overwrites that month's prefix
(idempotent), while distinct months accrete as the historical ledger. Scheduled runs
pass the execution month (Trigger ``payload.timestamp``); a manual run with no month
falls back to the resolved file's Month_Year (so an off-cycle capture of the May file
correctly lands at snapshot=2026-05, not a misleading execution-month label). A
provided-vs-file month mismatch is logged + recorded, never fatal (CMS publishing a few
days late must not break the pipeline).

Indexing (directive: BTREE the NPI + primary practice-address columns for fast geo joins
against Overture Places). ``npi`` and ``provider_first_line_business_practice_location_address``
are high-cardinality resolution keys → BTREE (as directed). ``provider_business_practice_location_address_state_name``
is a ~60-value categorical → BITMAP, the fleet-correct index type for that cardinality
(02_lancedb_storage.md §6.1 lists ``pop_state`` as the canonical BITMAP example); the
directive grouped it under "BTREE" but BITMAP serves the state-filter/geo-join intent
strictly better at 60 distinct values. Documented reconciliation; flip to BTREE is a
one-line change to INDEX_PLAN.

Cleanup (directive). The ZIP is removed right after extraction, the CSV right after the
Lance write, the local Lance stage after publish — and a finally-guaranteed sweep removes
all scratch (ZIP + CSV + transcode + stage) before the function returns, so a failed run
never leaks the ~20 GB working set onto the ephemeral volume.

Control plane (Trigger v4 durable callback): on terminal state (success OR failure) the
worker (1) writes a run row to ops.nppes_runs via psycopg and (2) POSTs a FLAT JSON body
to trigger_callback_url. No {"data": ...} envelope.

    modal deploy pipelines/nppes/ingest.py
    modal run    pipelines/nppes/ingest.py::init_state                 # create ops.nppes_runs
    modal run    pipelines/nppes/ingest.py::probe                      # resolve current monthly URL (no download)
    modal run    pipelines/nppes/ingest.py::capture                    # initial capture — month zero (auto month)
    modal run    pipelines/nppes/ingest.py::capture --snapshot-month 2026-05
    modal run    pipelines/nppes/ingest.py::verify --snapshot-month 2026-05  # read-back proof
    modal run    pipelines/nppes/ingest.py::reindex --snapshot-month 2026-05
    modal run    pipelines/nppes/ingest.py::show_ledger

Failure paging: any terminal non-success writes the ops.nppes_runs error row and then pages
via ``core.ops_alert.alert()`` -> ``OPS_ALERT_WEBHOOK`` (Modal secret ``ops-alerts``); no-op
when the env var is unset, never masks the original error.
"""

from __future__ import annotations

import os

import modal

from core.ops_alert import alert

BUCKET = "data-sink"
FEED = "nppes"

# Dynamic source — never a hard-coded download URL.
NPI_FILES_URL = "https://download.cms.gov/nppes/NPI_Files.html"
NPPES_BASE_URL = "https://download.cms.gov/nppes/"

# Lance system-of-record tier (env-overridable). Per-month partitions append
# ``/snapshot=YYYY-MM/`` under this prefix → one immutable dataset per month.
ACTIVE_PREFIX = os.environ.get("NPPES_ACTIVE_PREFIX", "active/nppes").strip("/")

SCRATCH_DIR = "/tmp/nppes"
LOCAL_DATASET = os.path.join(SCRATCH_DIR, "lance")  # local Lance staging (pre-publish)

# The single core member of the monthly ZIP. Excludes the 1-row ``_FileHeader.csv``
# sidecar and the pl_/othername_/endpoint_ secondary files + readme PDFs.
CORE_MEMBER_RE = r"^npidata_pfile_\d{8}-\d{8}\.csv$"

# Lance fragment sizing. max_rows_per_file = 1048576 (Lance default; ~9 fragments at
# 8.5M rows). max_bytes_per_file = 90 GiB (Lance's documented default; the fleet
# constant). NEVER the ``90 * 10243`` ≈ 900 KB misread that would shatter the dataset.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new datasets → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"
# Streaming reader batch — 8.5M × ~330 cols never materializes whole (02 §4.2).
READ_BATCH_ROWS = 131072

# Scalar index plan. BTREE = high-cardinality resolution / geo-join keys (directive);
# BITMAP = the ~60-value state categorical (02 §6.1 — the correct type at that
# cardinality; see the module docstring's reconciliation).
INDEX_PLAN: dict[str, list[str]] = {
    "btree": ["npi", "provider_first_line_business_practice_location_address"],
    "bitmap": ["provider_business_practice_location_address_state_name"],
}

# Mirrored verbatim by pipelines/nppes/ops_nppes_runs.sql. Applied by init_state.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.nppes_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,
    snapshot_month  text        NOT NULL,
    dataset_uri     text,
    source_url      text,
    source_file     text,
    source_member   text,
    source_encoding text,
    zip_bytes       bigint,
    csv_bytes       bigint,
    rows_processed  bigint,
    rejected_rows   bigint,
    write_path      text,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS nppes_runs_month_idx       ON ops.nppes_runs (snapshot_month);
CREATE INDEX IF NOT EXISTS nppes_runs_feed_idx        ON ops.nppes_runs (feed);
CREATE INDEX IF NOT EXISTS nppes_runs_status_idx      ON ops.nppes_runs (status);
CREATE INDEX IF NOT EXISTS nppes_runs_recorded_at_idx ON ops.nppes_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_reader; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",           # ZIP download + R2 dataset publish (uniform-part multipart)
    "requests>=2.32",        # HTML scrape + Trigger waitpoint callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    # BTREE training sorts the column; bypass Lance's bounded spill-to-disk sorter
    # (under-sizes its pool, OOMs on high-cardinality string columns). Cheap in-memory
    # sort at 8.5M rows. See lance-format/lance#2650.
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.ops_alert")

app = modal.App("nppes-pipelines", image=image)

MONTHS = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12,
}


# --------------------------------------------------------------------------- #
# R2 / object-store
# --------------------------------------------------------------------------- #
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret."""
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


def _s3_client():
    """boto3 S3 client for R2. checksum behaviour forced to ``when_required`` (R2 rejects
    botocore's default flexible-checksum validation)."""
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


def _month_prefix(snapshot_month: str) -> str:
    """R2 key prefix for one month's Lance dataset."""
    return f"{ACTIVE_PREFIX}/snapshot={snapshot_month}/"


def _month_uri(snapshot_month: str) -> str:
    """Lance dataset URI for one month's snapshot."""
    return f"s3://{BUCKET}/{_month_prefix(snapshot_month)}"


# --------------------------------------------------------------------------- #
# Dynamic source resolution — scrape NPI_Files.html
# --------------------------------------------------------------------------- #
def _parse_file_month(basename: str) -> str | None:
    """``YYYY-MM`` from a monthly basename (``NPPES_Data_Dissemination_<Month>_<Year>``),
    else None. Used by the explicit-``source_url`` path to label the partition."""
    import re

    m = re.match(r"^NPPES_Data_Dissemination_([A-Z][a-z]+)_(\d{4})", basename)
    if m and m.group(1) in MONTHS:
        return f"{int(m.group(2)):04d}-{MONTHS[m.group(1)]:02d}"
    return None


def _resolve_monthly_zip(html: str | None = None) -> dict:
    """Resolve the CURRENT monthly full-replacement ZIP from NPI_Files.html.

    Returns ``{url, basename, month_name, year, version, file_month}`` where
    ``file_month`` is the file's ``YYYY-MM``. Raises if no monthly full replacement is
    found (fail loud — never silently ingest nothing). Weekly + Deactivated files are
    excluded; among versioned monthlies the latest (year, month, version) wins.
    """
    import re
    import urllib.parse

    import requests

    if html is None:
        resp = requests.get(NPI_FILES_URL, timeout=120,
                            headers={"User-Agent": "core-x/nppes-pipelines"})
        resp.raise_for_status()
        html = resp.text

    monthly_re = re.compile(
        r"^NPPES_Data_Dissemination_([A-Z][a-z]+)_(\d{4})(?:_V(\d+))?\.zip$"
    )
    hrefs = re.findall(r'href=["\']([^"\']+?\.zip)["\']', html, flags=re.IGNORECASE)

    candidates: list[tuple] = []
    for href in hrefs:
        base = href.rsplit("/", 1)[-1]
        if "Weekly" in base or "Deactivated" in base:  # STRICT EXCLUSION
            continue
        m = monthly_re.match(base)
        if not m:
            continue
        month_name, year, ver = m.group(1), int(m.group(2)), int(m.group(3) or 0)
        if month_name not in MONTHS:
            continue
        url = urllib.parse.urljoin(NPPES_BASE_URL, href)
        candidates.append((year, MONTHS[month_name], ver, month_name, base, url))

    if not candidates:
        raise RuntimeError(
            f"no monthly full-replacement ZIP found on {NPI_FILES_URL} "
            f"(saw {len(hrefs)} .zip hrefs; all weekly/deactivated/unmatched)"
        )

    year, month_num, ver, month_name, base, url = max(candidates)
    return {
        "url": url, "basename": base, "month_name": month_name,
        "year": year, "version": ver, "file_month": f"{year:04d}-{month_num:02d}",
    }


# --------------------------------------------------------------------------- #
# Download + extract  (Python = I/O only)
# --------------------------------------------------------------------------- #
def _download(url: str, dest: str, attempts: int = 5) -> int:
    """Stream a URL to local scratch, retrying transient connection breaks. Verifies the
    written size against Content-Length when advertised (and not gzip-encoded) so a
    silently truncated body is a failure, not a short success. Returns bytes."""
    import time

    import requests

    backoff = (5, 15, 45, 120)
    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            total = 0
            with requests.get(url, stream=True, timeout=(30, 1800),
                              headers={"User-Agent": "core-x/nppes-pipelines"}) as r:
                r.raise_for_status()
                declared = r.headers.get("Content-Length")
                gzipped = "gzip" in r.headers.get("Content-Encoding", "").lower()
                expected = int(declared) if (declared and not gzipped) else None
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 << 20):
                        if chunk:
                            f.write(chunk)
                            total += len(chunk)
            if expected is not None and total != expected:
                raise OSError(f"truncated download: got {total} bytes, expected {expected}")
            return total
        except Exception as exc:  # noqa: BLE001 — retry transient network failures
            last_exc = exc
            if i < attempts - 1:
                wait = backoff[min(i, len(backoff) - 1)]
                print(f"download attempt {i + 1}/{attempts} failed ({exc}); retrying in {wait}s")
                time.sleep(wait)
    raise RuntimeError(f"download failed after {attempts} attempts: {last_exc}")


def _extract_core_csv(zip_path: str, dest_dir: str) -> tuple[str, str]:
    """Extract ONLY the core ``npidata_pfile_<dates>.csv`` member (streamed, no
    path-traversal) → (local_csv_path, member_name). Raises if absent. Among multiple
    matches the largest wins; ``_FileHeader.csv`` / secondary files are ignored."""
    import re
    import shutil
    import zipfile

    rx = re.compile(CORE_MEMBER_RE)
    with zipfile.ZipFile(zip_path) as zf:
        matches = [n for n in zf.namelist() if rx.match(n.rsplit("/", 1)[-1])]
        if not matches:
            sample = [n.rsplit("/", 1)[-1] for n in zf.namelist()[:8]]
            raise RuntimeError(f"no core npidata_pfile CSV in {zip_path} (members≈{sample})")
        member = max(matches, key=lambda n: zf.getinfo(n).file_size)
        member_base = member.rsplit("/", 1)[-1]
        csv_path = os.path.join(dest_dir, member_base)
        with zf.open(member) as src, open(csv_path, "wb") as dst:
            shutil.copyfileobj(src, dst, length=16 << 20)
    return csv_path, member_base


# --------------------------------------------------------------------------- #
# Encoding — detect utf-8, transcode latin-1 → utf-8 only when needed
# --------------------------------------------------------------------------- #
def _is_utf8(path: str, chunk: int = 1 << 20) -> bool:
    """Strict, streaming utf-8 validation over the whole file (bounded memory)."""
    import codecs

    dec = codecs.getincrementaldecoder("utf-8")()
    with open(path, "rb") as fh:
        while True:
            buf = fh.read(chunk)
            try:
                dec.decode(buf, final=not buf)
            except UnicodeDecodeError:
                return False
            if not buf:
                return True


def _transcode_latin1_to_utf8(src: str, dst: str, chunk: int = 1 << 20) -> int:
    """Rewrite ``src`` (decoded as latin-1 — never raises on any byte) to utf-8 at
    ``dst``. Returns bytes written."""
    written = 0
    with open(src, "rb") as i, open(dst, "wb") as o:
        while (buf := i.read(chunk)):
            out = buf.decode("latin-1").encode("utf-8")
            o.write(out)
            written += len(out)
    return written


def _resolve_read_path(csv_path: str) -> tuple[str, str]:
    """Return (utf8_read_path, detected_source_encoding). If the file is already valid
    utf-8 it is read in place; otherwise it is transcoded latin-1 → utf-8 and the
    transcoded path is returned (the original is removed by the caller). The DuckDB read
    is therefore ALWAYS utf-8."""
    if _is_utf8(csv_path):
        print("encoding: source validated as utf-8 (no transcode)")
        return csv_path, "utf-8"
    utf8_path = csv_path + ".utf8.csv"
    n = _transcode_latin1_to_utf8(csv_path, utf8_path)
    print(f"encoding: source not utf-8 — transcoded latin-1 → utf-8 ({n:,} bytes)")
    return utf8_path, "latin-1"


# --------------------------------------------------------------------------- #
# DuckDB transform — read_csv (RFC-4180 pinned) → trim/nullif passthrough
# --------------------------------------------------------------------------- #
def _read_opts(encoding: str = "utf-8", store_rejects: bool = True) -> str:
    """read_csv options. ``read_csv_auto`` intent with the verified NPPES RFC-4180
    dialect pinned (deterministic at 10 GB — no sniffing). all_varchar + normalize_names
    are the architecture rule + the directive's exact column names. Malformed rows are
    quarantined (store_rejects) rather than aborting the load."""
    opts = (
        f"all_varchar=true, header=true, normalize_names=true, "
        f"delim=',', quote='\"', escape='\"', sample_size=-1, "
        f"ignore_errors=true, null_padding=true, encoding='{encoding}'"
    )
    return opts + ", store_rejects=true" if store_rejects else opts


def _describe_columns(con, csv_path: str, encoding: str) -> list[str]:
    """The normalized column names DuckDB will emit (header-only — derive, never guess)."""
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_csv(?, {_read_opts(encoding, store_rejects=False)})",
        [csv_path],
    ).fetchall()
    return [r[0] for r in rows]


def _build_transform_sql(columns: list[str], encoding: str) -> str:
    """Lossless projection: every source column trimmed + empty→NULL under its exact
    normalized name, plus provenance. The path + provenance values bind positionally via
    ``?``; column identifiers are double-quoted (and normalize_names already strips them
    to ``[a-z0-9_]``, so no injection survives)."""
    def q(c: str) -> str:
        return '"' + c.replace('"', '""') + '"'

    projection = ",\n    ".join(f"nullif(trim({q(c)}), '') AS {q(c)}" for c in columns)
    return (
        f"WITH raw AS (SELECT * FROM read_csv(?, {_read_opts(encoding)}))\n"
        f"SELECT\n    {projection},\n"
        "    ? AS source_file,\n"
        "    ? AS source_member,\n"
        "    ? AS snapshot_month,\n"
        "    now() AS ingested_at\n"
        "FROM raw"
    )


# --------------------------------------------------------------------------- #
# Lance — index + R2 publish (local stage; uniform-part multipart)
# --------------------------------------------------------------------------- #
def _create_indexes(local_path: str) -> list[str]:
    """BTREE + BITMAP scalar indexes on the LOCAL dataset (no storage_options — local
    writes avoid R2's multipart rule). replace=True → idempotent. An index miss is
    logged, never fatal — the Lance data write is the critical artifact."""
    import lance

    ds = lance.dataset(local_path)
    built: list[str] = []
    for col in INDEX_PLAN["btree"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in INDEX_PLAN["bitmap"]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices. Tolerant of pylance return-shape
    drift (dict vs object, list_indices vs list_indexes)."""
    for attr in ("list_indices", "list_indexes"):
        fn = getattr(ds, attr, None)
        if fn is None:
            continue
        try:
            out = []
            for ix in fn():
                if isinstance(ix, dict):
                    out.append({k: ix.get(k) for k in ("name", "type", "fields")})
                else:
                    out.append({"name": getattr(ix, "name", None),
                                "type": str(getattr(ix, "type", None)),
                                "fields": getattr(ix, "fields", None)})
            return out
        except Exception as exc:  # noqa: BLE001
            return [{"error": f"{attr}: {exc}"}]
    return [{"error": "no list_indices/list_indexes method on dataset"}]


def _replace_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Idempotent publish: wipe the month's R2 prefix, then upload the local Lance
    dataset (boto3/s3transfer = uniform-part multipart, R2-compliant). Returns files
    uploaded."""
    to_del: list[dict] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            to_del.append({"Key": o["Key"]})
            if len(to_del) == 1000:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})
                to_del = []
    if to_del:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})

    uploaded = 0
    for root, _, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            uploaded += 1
    return uploaded


def _download_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Stage a committed R2 month back to local disk (for an in-place reindex without
    re-ingesting). Returns files downloaded."""
    import shutil

    shutil.rmtree(local_dir, ignore_errors=True)
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if not rel:
                continue
            lp = os.path.join(local_dir, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(lp), exist_ok=True)
            s3.download_file(BUCKET, o["Key"], lp)
            n += 1
    return n


# --------------------------------------------------------------------------- #
# State + callback + cleanup
# --------------------------------------------------------------------------- #
def _record_run(*, snapshot_month, dataset_uri, source_url, source_file, source_member,
                source_encoding, zip_bytes, csv_bytes, rows, rejected, write_path,
                status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.nppes_runs (psycopg). Best-effort: never let an audit-write
    failure crash an otherwise-good ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.nppes_runs
                    (feed, snapshot_month, dataset_uri, source_url, source_file,
                     source_member, source_encoding, zip_bytes, csv_bytes,
                     rows_processed, rejected_rows, write_path, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, snapshot_month, dataset_uri, source_url, source_file,
                 source_member, source_encoding, zip_bytes, csv_bytes,
                 rows, rejected, write_path, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. FLAT JSON body — no
    {"data": ...} envelope, no API key (the callbackHash in the url is the auth)."""
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


def _cleanup(*paths: str) -> None:
    """Remove scratch files/dirs (directive: rm CSV + ZIP before return). Best-effort."""
    import shutil

    for p in paths:
        if not p:
            continue
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            elif os.path.exists(p):
                os.remove(p)
        except OSError as exc:
            print(f"WARN: cleanup of {p} failed: {exc}")


# --------------------------------------------------------------------------- #
# Workers
# --------------------------------------------------------------------------- #
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    """Apply the idempotent ops.nppes_runs DDL. Run once before the first ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.nppes_runs')")
        present = cur.fetchone()[0]
    print(f"ops.nppes_runs present = {present}")
    return {"status": "success", "table": "ops.nppes_runs", "present": str(present)}


@app.function(image=image, timeout=120)
def resolve_current_url() -> dict:
    """Diagnostic — resolve the current monthly full-replacement URL WITHOUT downloading.
    Backs the ``probe`` entrypoint (verify the scraping mechanism in isolation)."""
    info = _resolve_monthly_zip()
    print(f"current monthly full replacement: {info['basename']}")
    print(f"  url        : {info['url']}")
    print(f"  file month : {info['file_month']}  (version {info['version']})")
    return info


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres"),
             modal.Secret.from_name("ops-alerts")],
    timeout=60 * 60 * 4,    # ~1 GB download + ~10 GB CSV transform + index + publish
    memory=32768,           # ≥ directive's 16 GiB floor; heavy ~330-col feed
    cpu=8.0,
    ephemeral_disk=524288,  # Modal floor 512 GiB ≫ directive's ≥20 GiB (ZIP+CSV+stage ≈ 20 GiB)
)
def ingest_nppes(snapshot_month: str | None = None, source_url: str | None = None,
                 trigger_callback_url: str | None = None) -> dict:
    """Resolve (or accept) the monthly ZIP → download → extract core CSV → detect/transcode
    encoding → DuckDB project/cast (100% transform, streamed) → Lance overwrite on LOCAL
    disk → BTREE/BITMAP index locally → boto3 publish to
    s3://data-sink/active/nppes/snapshot=YYYY-MM/ → cleanup. Records ops.* + wakes the
    Trigger run. Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    os.makedirs(os.path.join(SCRATCH_DIR, "duckdb_spill"), exist_ok=True)

    zip_path = csv_path = read_path = None
    source_file = source_member = source_encoding = None
    zip_bytes = csv_bytes = 0
    rows = rejected = 0
    write_path = "stream-local-publish"
    status, error = "error", None
    built: list[str] = []
    dataset_uri = None

    try:
        # 1) Resolve the source (dynamic scrape unless an explicit URL is pinned).
        if source_url:
            import urllib.parse
            base = urllib.parse.urlparse(source_url).path.rsplit("/", 1)[-1]
            resolved = {"url": source_url, "basename": base,
                        "file_month": _parse_file_month(base)}
        else:
            resolved = _resolve_monthly_zip()
        source_url = resolved["url"]
        source_file = resolved["basename"]
        file_month = resolved.get("file_month")

        # 2) Partition key: explicit arg wins; else the resolved file's month; else clock.
        if not snapshot_month:
            snapshot_month = file_month or started_at.strftime("%Y-%m")
        if file_month and file_month != snapshot_month:
            print(f"WARN: snapshot_month={snapshot_month} != resolved file month "
                  f"{file_month} ({source_file}); proceeding (recorded in ops).")
        dataset_uri = _month_uri(snapshot_month)
        print(f"NPPES capture → snapshot={snapshot_month}  src={source_file}\n  {dataset_uri}")

        # 3) Download ZIP, extract the core CSV, drop the ZIP immediately.
        zip_path = os.path.join(SCRATCH_DIR, source_file)
        zip_bytes = _download(source_url, zip_path)
        print(f"  downloaded {zip_bytes:,} bytes → {zip_path}")
        csv_path, source_member = _extract_core_csv(zip_path, SCRATCH_DIR)
        csv_bytes = os.path.getsize(csv_path)
        print(f"  extracted core member {source_member} ({csv_bytes:,} bytes)")
        _cleanup(zip_path); zip_path = None

        # 4) Encoding: utf-8 in place, or transcode latin-1 → utf-8.
        read_path, source_encoding = _resolve_read_path(csv_path)
        if read_path != csv_path:
            _cleanup(csv_path); csv_path = None

        # 5) Transform (streamed) → Lance overwrite on LOCAL disk.
        _cleanup(LOCAL_DATASET)
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute("SET temp_directory='/tmp/nppes/duckdb_spill';")
            columns = _describe_columns(con, read_path, "utf-8")
            sql = _build_transform_sql(columns, "utf-8")
            reader = con.execute(
                sql, [read_path, source_file, source_member, snapshot_month]
            ).to_arrow_reader(READ_BATCH_ROWS)
            lance.write_dataset(
                reader,
                LOCAL_DATASET,
                schema=reader.schema,   # REQUIRED for a reader source
                mode="overwrite",
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE,
                max_bytes_per_file=MAX_BYTES_PER_FILE,
            )
            try:
                rj = con.execute("SELECT count(*) FROM reject_errors").fetchone()
                rejected = int(rj[0]) if rj else 0
            except Exception:  # noqa: BLE001 — table absent ⇒ zero rejects
                rejected = 0
        finally:
            con.close()
        rows = lance.dataset(LOCAL_DATASET).count_rows()
        print(f"NPPES: wrote {rows:,} rows ({rejected:,} rejected) → local stage")

        # 6) Drop the CSV (free ~10 GB) BEFORE indexing — indexing reads Lance, not the CSV.
        _cleanup(read_path); read_path = csv_path = None

        # 7) Index locally, then publish to the month's R2 prefix (wipe + uniform upload).
        built = _create_indexes(LOCAL_DATASET)
        s3 = _s3_client()
        uploaded = _replace_r2_prefix(s3, _month_prefix(snapshot_month), LOCAL_DATASET)
        print(f"Published {uploaded} files → {dataset_uri}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        _cleanup(zip_path, csv_path, read_path, LOCAL_DATASET)  # directive: rm before return
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(
            snapshot_month=snapshot_month, dataset_uri=dataset_uri, source_url=source_url,
            source_file=source_file, source_member=source_member,
            source_encoding=source_encoding, zip_bytes=int(zip_bytes), csv_bytes=int(csv_bytes),
            rows=int(rows), rejected=int(rejected), write_path=write_path,
            status=status, error=error, started_at=started_at, completed_at=completed_at,
        )
        if status != "success":
            # Terminal-failure page: core.ops_alert.alert -> OPS_ALERT_WEBHOOK (Modal secret
            # ops-alerts). No-op when unset; alert() never raises, so the original error
            # (re-raised below) is never masked.
            alert(f"[nppes_ingest] snapshot={snapshot_month} ingest {status}: {str(error)[:300]}")
        _post_callback(trigger_callback_url, {
            "status": status, "feed": FEED, "snapshot_month": snapshot_month,
            "rows": int(rows), "rejected_rows": int(rejected),
            "dataset_uri": dataset_uri, "source_file": source_file,
        })

    if status != "success":
        raise RuntimeError(f"nppes ingest failed for snapshot={snapshot_month}: {error}")
    return {"feed": FEED, "snapshot_month": snapshot_month, "rows_processed": int(rows),
            "rejected_rows": int(rejected), "indices": built, "dataset_uri": dataset_uri,
            "source_file": source_file, "source_member": source_member,
            "source_encoding": source_encoding, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 60, memory=32768, cpu=8.0, ephemeral_disk=524288,
)
def reindex_nppes(snapshot_month: str) -> dict:
    """Rebuild the scalar indexes on an existing month (no re-ingest): stage the committed
    R2 dataset to local disk, index locally (no R2 multipart), publish back via boto3."""
    import lance

    s3 = _s3_client()
    prefix = _month_prefix(snapshot_month)
    staged = _download_r2_prefix(s3, prefix, LOCAL_DATASET)
    if staged == 0:
        raise RuntimeError(f"no dataset at {_month_uri(snapshot_month)} to reindex")
    print(f"Staged {staged} files from {_month_uri(snapshot_month)} → {LOCAL_DATASET}")
    try:
        rows = lance.dataset(LOCAL_DATASET).count_rows()
        built = _create_indexes(LOCAL_DATASET)
        uploaded = _replace_r2_prefix(s3, prefix, LOCAL_DATASET)
        print(f"Published {uploaded} files → {_month_uri(snapshot_month)}")
    finally:
        _cleanup(LOCAL_DATASET)
    return {"snapshot_month": snapshot_month, "dataset_uri": _month_uri(snapshot_month),
            "rows": rows, "indices": built}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10)
def verify_snapshot(snapshot_month: str) -> dict:
    """Read-back proof: open the month's Lance dataset from R2 and report row count,
    non-null counts on the indexed keys, and committed indices. Authoritative success
    check — reads what actually landed, independent of the write path's return value."""
    import lance

    uri = _month_uri(snapshot_month)
    ds = lance.dataset(uri, storage_options=_r2_storage_options())
    out: dict = {"snapshot_month": snapshot_month, "dataset_uri": uri, "rows": ds.count_rows()}
    for col in INDEX_PLAN["btree"] + INDEX_PLAN["bitmap"]:
        try:
            out[f"{col}__non_null"] = ds.count_rows(filter=f"{col} IS NOT NULL")
        except Exception as exc:  # noqa: BLE001
            out[f"{col}__non_null"] = f"err: {str(exc)[:80]}"
    out["indices"] = _list_committed_indices(ds)
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 10) -> list:
    """Read the most recent ops.nppes_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, snapshot_month, dataset_uri, source_file, source_encoding, "
            "rows_processed, rejected_rows, status, error, started_at, completed_at "
            "FROM ops.nppes_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# --------------------------------------------------------------------------- #
# Manual ops entrypoints (local — no Trigger callback). ops.* write still fires.
# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def init_state() -> None:
    """Create ops.nppes_runs (idempotent)."""
    import json

    print(json.dumps(apply_state_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def probe() -> None:
    """Resolve + print the current monthly full-replacement URL (no download)."""
    import json

    print(json.dumps(resolve_current_url.remote(), indent=2, default=str))


@app.local_entrypoint()
def capture(snapshot_month: str = "", source_url: str = "") -> None:
    """Initial / ad-hoc capture → Lance month partition (no Trigger callback). With no
    --snapshot-month the partition is derived from the resolved file's month."""
    import json

    print(json.dumps(
        ingest_nppes.remote(snapshot_month=(snapshot_month or None),
                            source_url=(source_url or None), trigger_callback_url=None),
        indent=2, default=str))


@app.local_entrypoint()
def reindex(snapshot_month: str) -> None:
    """Rebuild scalar indexes on an existing month's dataset (no re-ingest)."""
    import json

    print(json.dumps(reindex_nppes.remote(snapshot_month), indent=2, default=str))


@app.local_entrypoint()
def verify(snapshot_month: str) -> None:
    """Read-back proof of a committed month's Lance dataset (rows, key non-nulls, indices)."""
    import json

    print(json.dumps(verify_snapshot.remote(snapshot_month), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 10) -> None:
    """Print the most recent ops.nppes_runs rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
