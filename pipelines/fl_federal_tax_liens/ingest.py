"""Compute worker — Florida Federal Lien Registrations (FLR / federal tax liens).

Part of the ``fl-federal-tax-liens`` Modal app. One endpoint-less worker spawned by
the Universal Dispatcher (core/modal_dispatcher.py) — no web endpoint. Clean-room data
plane: Python does I/O ONLY (download + unzip); DuckDB does 100% of the transform;
Arrow is the only interchange; Lance is the system of record written straight to R2.

Source shape (fixed-width COBOL copybook, NOT delimited — established by the read-only
probe). The operator lands a single Quarterly FLR bulk export to
s3://data-sink/landing/fl_federal_tax_liens/ as four ZIP members:
    flrf.zip → FLRF.TXT  Filings        (LIEN_DATA_FILE,      82 bytes/record)
    flrd.zip → FLRD.TXT  Debtors        (LIEN_DEB_DATA_FILE, 206 bytes/record)
    flrs.zip → FLRS.TXT  Secured        (LIEN_SEC_DATA_FILE, 206 bytes/record)
    flre.zip → FLRE.TXT  Events         (EXCLUDED — physical offsets diverge from the
                                         copybook past the two leading doc-number fields,
                                         and its keys are a cross-format historical tail:
                                         0% exact / 4.72% normalized into the active snapshot)

Data plane (one Modal invocation; one Lance dataset):
    R2 landing ZIPs (flrf/flrd/flrs)
      → boto3 download → /tmp/*.zip                          (Python: I/O only)
      → zipfile extract single member → /tmp/fl_flr/*.TXT     (Python: I/O only)
      → DuckDB single-column line read + substr() byte-offset parse  (100% in SQL)
      → debtor-grain unified view: FLRD ⨝ FLRF (1:1 on doc_number, 100% integrity),
        FLRS folded as a nested LIST<STRUCT> per filing
      → quarantines: drop sentinel doc '26FLR0000999'; corporate debtors only (name_format='C')
      → bridge keys: normalized_legal_name + zip5 (EXACT sos_normalized_master standard)
      → Arrow table → con.execute(...).to_arrow_table()
      → lance.write_dataset(s3://data-sink/active/fl_federal_tax_liens/, v2.1, overwrite)
      → BTREE scalar indexes on normalized_legal_name, zip5, doc_number
         (direct-to-R2; escalates to the /tmp local round-trip on R2 multipart InvalidPart)

Bridge target: s3://data-sink/active/sos_normalized_master/. normalized_legal_name and
zip5 are derived with the byte-identical normalization the spine uses (pipelines/
sos_normalized/normalize.py _name_norm / _zip5), so the two BTREE blocking keys align.

Control plane (Trigger v4 durable callback): the worker accepts ``trigger_callback_url``
and, on terminal state (success OR failure), (1) writes the run row to ``ops.fl_flr_runs``
via psycopg and (2) POSTs a FLAT JSON body to that url to wake the suspended Trigger run.

    modal run    pipelines/fl_federal_tax_liens/ingest.py::setup     # create ops.fl_flr_runs
    modal run    pipelines/fl_federal_tax_liens/ingest.py::run       # execute the ingest (manual)
    modal run    pipelines/fl_federal_tax_liens/ingest.py::reindex   # rebuild scalar indexes only
    modal deploy pipelines/fl_federal_tax_liens/ingest.py            # publish for the dispatcher
"""

from __future__ import annotations

import os

import modal

from core.name_norm import name_norm as _name_norm

BUCKET = "data-sink"
FEED = "fl_federal_tax_liens"

# Lance system-of-record dataset (env-overridable). One debtor-grain dataset.
DATASET_URI = os.environ.get("FL_FLR_LANCE_URI", "s3://data-sink/active/fl_federal_tax_liens/")
DS_PREFIX = "active/fl_federal_tax_liens/"  # R2 key prefix (local round-trip index publish)

# Landing ZIP members (role → R2 key). FLRE (events) is deliberately excluded.
LANDING_ZIPS = {
    "filing":  "landing/fl_federal_tax_liens/flrf.zip",
    "debtor":  "landing/fl_federal_tax_liens/flrd.zip",
    "secured": "landing/fl_federal_tax_liens/flrs.zip",
}

SCRATCH_DIR = "/tmp"
WORKDIR = "/tmp/fl_flr"

# Dummy/test filing quarantined on ingest (probe finding: 5 (doc,seq) collisions, all here).
SENTINEL_DOC = "26FLR0000999"

# BTREE resolution keys (directive). normalized_legal_name + zip5 bridge to the SoS spine;
# doc_number is the internal lien/filing join key.
INDEX_COLS = ["normalized_legal_name", "zip5", "doc_number"]

# as_of: the quarterly FLR export fulfillment date (operator-overridable).
AS_OF_DEFAULT = "2026-05-31"

# Lance fragment sizing — fleet constants (rows-per-file binds first; 90 GiB byte ceiling).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"  # net-new dataset → current Lance default (02_lancedb_storage.md §2.3)

# Folded secured-party struct (FLRS, one filing → N secured parties; here uniformly the IRS).
SEC_STRUCT = (
    "STRUCT(name VARCHAR, name_format VARCHAR, address1 VARCHAR, city VARCHAR, "
    "state VARCHAR, zip_code VARCHAR, zip5 VARCHAR, country VARCHAR, seq INTEGER)"
)

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.fl_flr_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase            text        NOT NULL,     -- 'ingest' | 'reindex'
    dataset_uri      text,
    as_of            date,
    source_zips      jsonb,                    -- {filing,debtor,secured: byte sizes}
    filing_rows      bigint,                   -- parsed filings (FLRF)
    secured_rows     bigint,                   -- parsed secured parties (FLRS)
    dropped_sentinel bigint,                   -- debtor rows dropped on the sentinel doc
    rows_processed   bigint,                   -- committed Lance rows (debtor grain)
    indexes          jsonb,                    -- ["normalized_legal_name","zip5","doc_number"]
    index_mode       text,                     -- 'direct-r2' | 'local-roundtrip'
    status           text        NOT NULL,     -- 'success' | 'error'
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fl_flr_runs_phase_idx       ON ops.fl_flr_runs (phase);
CREATE INDEX IF NOT EXISTS fl_flr_runs_status_idx      ON ops.fl_flr_runs (status);
CREATE INDEX IF NOT EXISTS fl_flr_runs_recorded_at_idx ON ops.fl_flr_runs (recorded_at DESC);
"""

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
        "lancedb>=0.15",
        "pylance>=7",            # provides `import lance`; lancedb does not re-export it
        "pyarrow>=17",
        "boto3>=1.35",           # R2 landing read + local round-trip publish
        "requests>=2.32",        # Trigger callback
        "psycopg[binary]>=3.2",  # terminal state → ops.*
    )
    .env(
        # BTREE scalar-index builds sort the column; force the in-memory sort path so the
        # index always builds (fleet convention; lance-format/lance#2650). Trivial at this scale.
        {"LANCE_BYPASS_SPILLING": "true"}
    )
    .add_local_python_source("core.name_norm")  # ship the canonical blocking-key macro to the container
)

app = modal.App("fl-federal-tax-liens", image=image)


# ── R2 / object-store plumbing (mirrors the SBA/CA-UCC/HMDA workers verbatim) ──
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the r2-credentials Modal secret."""
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
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required`` — botocore's
    default flexible-checksum validation does not match R2's semantics and otherwise raises."""
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


def _extract_zip_member(s3, key: str, dest_txt: str) -> tuple[str, int]:
    """Download a landing ZIP and extract its single member (Deflate — stdlib-capable) to
    dest_txt. Python I/O only (no parse). Returns (member_name, extracted_bytes)."""
    import zipfile

    os.makedirs(WORKDIR, exist_ok=True)
    local_zip = os.path.join(SCRATCH_DIR, os.path.basename(key))
    s3.download_file(BUCKET, key, local_zip)
    with zipfile.ZipFile(local_zip) as zf:
        members = [n for n in zf.namelist() if not n.endswith("/")]
        if not members:
            raise RuntimeError(f"no members in s3://{BUCKET}/{key}")
        member = members[0]
        with zf.open(member) as src, open(dest_txt, "wb") as dst:
            while True:
                chunk = src.read(1 << 22)  # 4 MiB streamed copy — bounded memory
                if not chunk:
                    break
                dst.write(chunk)
    size = os.path.getsize(dest_txt)
    print(f"extracted {member} from {key} -> {dest_txt} ({size:,} bytes)")
    return member, size


# ── DuckDB fixed-width read + byte-offset transform (100% of the work in SQL) ──
def _lit(s: str) -> str:
    return s.replace("'", "''")


def _fw(path: str) -> str:
    """Single-column line read of a fixed-width file: delim is a byte that never occurs, so
    each physical line becomes one VARCHAR column; CR is stripped for clean trailing fields."""
    return (
        "(SELECT replace(line, chr(13), '') AS L FROM read_csv('"
        f"{_lit(path)}', columns={{'line': 'VARCHAR'}}, delim='\\x1F', header=false, "
        "quote='', escape='', strict_mode=false))"
    )


def _date8(off: int) -> str:
    """MMDDYYYY at byte offset → DATE (NULL on blank/'00000000'/unparseable)."""
    return f"try_strptime(nullif(trim(substr(L,{off},8)), ''), '%m%d%Y')::DATE"


def _build_transform_sql(paths: dict[str, str], as_of: str) -> str:
    """Debtor-grain unified view. Offsets are the exact copybook positions validated by the
    probe (FLRF 82 / FLRD-FLRS 206). normalized_legal_name comes from the canonical macro
    (core/name_norm.py, byte-identical to sos_normalized_master); zip5 uses the directive's
    exact regex standard. \\\\s / \\\\x1F survive Python → emit \\s / \\x1F."""
    name_norm = _name_norm("d.debtor_name")
    zip5_debtor = "nullif(left(regexp_replace(d.debtor_zip,'[^0-9]','','g'),5),'')"
    zip5_secured = "nullif(left(regexp_replace(secured_zip,'[^0-9]','','g'),5),'')"
    return f"""
WITH filing AS (
    SELECT
        trim(substr(L,1,12))                  AS doc_number,
        {_date8(13)}                          AS filing_date,
        TRY_CAST(substr(L,21,5) AS INTEGER)   AS pages,
        TRY_CAST(substr(L,26,5) AS INTEGER)   AS total_pages,
        nullif(trim(substr(L,31,1)),'')       AS filing_status,
        nullif(trim(substr(L,32,1)),'')       AS filing_type,
        {_date8(33)}                          AS assessment_date,
        {_date8(41)}                          AS cancellation_date,
        {_date8(49)}                          AS expiration_date,
        nullif(trim(substr(L,57,1)),'')       AS trans_utility,
        TRY_CAST(substr(L,58,5) AS INTEGER)   AS filing_event_count,
        TRY_CAST(substr(L,63,5) AS INTEGER)   AS filing_total_deb_ctr,
        TRY_CAST(substr(L,68,5) AS INTEGER)   AS filing_total_sec_ctr,
        TRY_CAST(substr(L,73,5) AS INTEGER)   AS filing_cur_deb_ctr,
        TRY_CAST(substr(L,78,5) AS INTEGER)   AS filing_cur_sec_ctr
    FROM {_fw(paths['filing'])}
),
debtor_raw AS (
    SELECT
        nullif(trim(substr(L,1,1)),'')        AS rec_filing_type,
        trim(substr(L,2,12))                  AS doc_number,
        nullif(trim(substr(L,14,55)),'')      AS debtor_name,
        substr(L,69,1)                        AS name_format,
        nullif(trim(substr(L,70,44)),'')      AS debtor_address1,
        nullif(trim(substr(L,114,44)),'')     AS debtor_address2,
        nullif(trim(substr(L,158,28)),'')     AS debtor_city,
        nullif(trim(substr(L,186,2)),'')      AS debtor_state,
        nullif(trim(substr(L,188,9)),'')      AS debtor_zip,
        nullif(trim(substr(L,197,2)),'')      AS debtor_country,
        TRY_CAST(substr(L,199,5) AS INTEGER)  AS debtor_seq,
        nullif(trim(substr(L,204,1)),'')      AS rel_to_filing,
        nullif(trim(substr(L,205,1)),'')      AS orig_party,
        nullif(trim(substr(L,206,1)),'')      AS debtor_filing_status
    FROM {_fw(paths['debtor'])}
),
debtor AS (
    SELECT * FROM debtor_raw
    WHERE name_format = 'C'                    -- corporate debtors only (directive)
      AND doc_number <> '{SENTINEL_DOC}'       -- quarantine the dummy test filing (directive)
),
secured AS (
    SELECT
        trim(substr(L,2,12))                  AS doc_number,
        nullif(trim(substr(L,14,55)),'')      AS secured_name,
        nullif(substr(L,69,1),' ')            AS secured_name_format,
        nullif(trim(substr(L,70,44)),'')      AS secured_address1,
        nullif(trim(substr(L,158,28)),'')     AS secured_city,
        nullif(trim(substr(L,186,2)),'')      AS secured_state,
        nullif(trim(substr(L,188,9)),'')      AS secured_zip,
        nullif(trim(substr(L,197,2)),'')      AS secured_country,
        TRY_CAST(substr(L,199,5) AS INTEGER)  AS secured_seq
    FROM {_fw(paths['secured'])}
    WHERE trim(substr(L,2,12)) <> '{SENTINEL_DOC}'
),
sec_agg AS (
    SELECT
        doc_number,
        list(struct_pack(
            name := secured_name,
            name_format := secured_name_format,
            address1 := secured_address1,
            city := secured_city,
            state := secured_state,
            zip_code := secured_zip,
            zip5 := {zip5_secured},
            country := secured_country,
            seq := secured_seq
        )) AS secured_parties,
        count(*) AS secured_count
    FROM secured
    GROUP BY doc_number
)
SELECT
    d.doc_number,
    d.debtor_seq,
    d.debtor_name                             AS source_entity_name,
    {name_norm}                               AS normalized_legal_name,
    d.name_format,
    d.debtor_address1,
    d.debtor_address2,
    d.debtor_city,
    d.debtor_state,
    d.debtor_zip,
    {zip5_debtor}                             AS zip5,
    d.debtor_country,
    d.rel_to_filing,
    d.orig_party,
    d.debtor_filing_status,
    f.filing_date,
    f.filing_status,
    f.filing_type,
    f.assessment_date,
    f.cancellation_date,
    f.expiration_date,
    f.filing_event_count,
    f.filing_total_deb_ctr,
    f.filing_total_sec_ctr,
    f.filing_cur_deb_ctr,
    f.filing_cur_sec_ctr,
    coalesce(sa.secured_parties, CAST([] AS {SEC_STRUCT}[])) AS secured_parties,
    coalesce(sa.secured_count, 0)             AS secured_count,
    CAST('{_lit(as_of)}' AS DATE)             AS as_of,
    now()                                     AS ingested_at
FROM debtor d
JOIN filing f      ON d.doc_number = f.doc_number          -- 100% referential integrity (probe)
LEFT JOIN sec_agg sa ON d.doc_number = sa.doc_number
"""


def _count_sql(path_role: str, paths: dict[str, str], where: str = "") -> str:
    return f"SELECT count(*) FROM {_fw(paths[path_role])}" + (f" WHERE {where}" if where else "")


# ── Lance write + R2-safe BTREE indexing ───────────────────────────────────────
def _write_lance(table, so: dict) -> None:
    import lance

    lance.write_dataset(
        table,
        DATASET_URI,
        mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )


def _index_direct(so: dict) -> list[str]:
    """Build the BTREE indexes directly on the R2 dataset. Fine at this scale (single small
    fragment, well under R2's multipart-escalation threshold)."""
    import lance

    ds = lance.dataset(DATASET_URI, storage_options=so)
    cols = set(ds.schema.names)
    built: list[str] = []
    for col in INDEX_COLS:
        if col not in cols:
            print(f"  WARN index column {col!r} not in schema; skipping")
            continue
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(col)
        print(f"  BTREE ✓ {col} (direct R2)")
    return built


def _download_r2_prefix(s3, prefix: str, local_dir: str) -> set[str]:
    """Stage every object under an R2 prefix to local disk. Returns the relative keys already
    in R2 (so the publish step skips re-uploading unchanged data files)."""
    import shutil

    shutil.rmtree(local_dir, ignore_errors=True)
    existing: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if not rel:
                continue
            lp = os.path.join(local_dir, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(lp), exist_ok=True)
            s3.download_file(BUCKET, o["Key"], lp)
            existing.add(rel)
    return existing


def _upload_new_files(s3, prefix: str, local_dir: str, existing: set[str]) -> int:
    """Upload local files whose relative key is NOT already in R2 (the freshly-built index
    files + the new manifest version). boto3/s3transfer uses uniform multipart parts → R2-safe.
    Manifest/version files upload LAST so the new version is resolvable only once every index
    file it references is present — an interrupt-safe publish."""
    new: list[tuple[str, str]] = []
    for root, _, files in os.walk(local_dir):
        for f in files:
            lp = os.path.join(root, f)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            if rel not in existing:
                new.append((rel, lp))
    new.sort(key=lambda t: ("_versions/" in t[0] or t[0].endswith(".manifest"), t[0]))
    for rel, lp in new:
        s3.upload_file(lp, BUCKET, prefix + rel)
    return len(new)


def _index_local_roundtrip(so: dict) -> list[str]:
    """R2-safe escalation: stage the dataset → local disk, create_scalar_index LOCALLY (no R2
    multipart), then publish only the new index + manifest files via boto3. Works at any size."""
    import lance

    s3 = _s3_client()
    local = f"{SCRATCH_DIR}/fl_flr_local"
    existing = _download_r2_prefix(s3, DS_PREFIX, local)
    print(f"staged {len(existing)} files from s3://{BUCKET}/{DS_PREFIX} → {local}")

    ds = lance.dataset(local)  # LOCAL — index writes go to disk, never R2 multipart
    cols = set(ds.schema.names)
    built: list[str] = []
    for col in INDEX_COLS:
        if col not in cols:
            print(f"  WARN index column {col!r} not in schema; skipping")
            continue
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(col)
        print(f"  BTREE ✓ {col} (local)")

    published = _upload_new_files(s3, DS_PREFIX, local, existing)
    print(f"published {published} new files (index + manifest) → s3://{BUCKET}/{DS_PREFIX}")
    return built


def _build_indexes(so: dict) -> tuple[list[str], str]:
    """Direct-to-R2 first (the directive's primary path); escalate to the /tmp local
    round-trip on R2's multipart 'InvalidPart' / part-size error. Returns (cols, mode)."""
    try:
        return _index_direct(so), "direct-r2"
    except Exception as exc:  # noqa: BLE001 — multipart escalation or transient index error
        print(f"WARN direct-to-R2 index build failed ({exc}); escalating to local round-trip")
        return _index_local_roundtrip(so), "local-roundtrip"


# ── Terminal state + callback ───────────────────────────────────────────────────
def _record_run(phase, as_of, source_zips, filing_rows, secured_rows, dropped_sentinel,
                rows_processed, indexes, index_mode, status, error,
                started_at, completed_at) -> None:
    """Terminal run row → ops.fl_flr_runs (psycopg). Best-effort: never let an audit-write
    failure crash an otherwise-good ingest."""
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.fl_flr_runs
                    (phase, dataset_uri, as_of, source_zips, filing_rows, secured_rows,
                     dropped_sentinel, rows_processed, indexes, index_mode, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (phase, DATASET_URI, as_of, Jsonb(source_zips), filing_rows, secured_rows,
                 dropped_sentinel, rows_processed, Jsonb(indexes), index_mode, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint url. FLAT JSON body — no envelope."""
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


def _duck():
    """In-memory DuckDB connection (spill to NVMe scratch; trivial at this scale)."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    con.execute(f"PRAGMA temp_directory='{SCRATCH_DIR}/duck_spill';")
    return con


# ── Worker functions ────────────────────────────────────────────────────────────
@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 45,
    memory=16384,
    cpu=4.0,
)
def ingest_fl_flr(
    as_of: str = AS_OF_DEFAULT,
    trigger_callback_url: str | None = None,
) -> dict:
    """Land flrf/flrd/flrs ZIPs → fixed-width parse → debtor-grain unified view (FLRE excluded)
    → quarantine sentinel + corporate-only → bridge keys → Lance overwrite → BTREE indexes,
    then record ops.* state and wake Trigger. Re-raises on failure (Modal call marked failed)."""
    import datetime as dt
    import re

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", as_of or ""):
        raise ValueError(f"as_of must be YYYY-MM-DD, got {as_of!r}")

    started_at = dt.datetime.now(dt.timezone.utc)
    source_zips: dict[str, int] = {}
    filing_rows = secured_rows = dropped_sentinel = rows_processed = 0
    indexes: list[str] = []
    index_mode = None
    status, error = "error", None

    try:
        so = _r2_storage_options()
        s3 = _s3_client()

        # PHASE 1 — Python I/O only: download + extract the three members.
        paths: dict[str, str] = {}
        for role, key in LANDING_ZIPS.items():
            dest = os.path.join(WORKDIR, f"{role}.TXT")
            _member, size = _extract_zip_member(s3, key, dest)
            paths[role] = dest
            source_zips[role] = size

        # PHASE 2 — DuckDB: 100% of the transform, zero-copy → Arrow.
        con = _duck()
        try:
            filing_rows = con.execute(_count_sql("filing", paths)).fetchone()[0]
            secured_rows = con.execute(_count_sql("secured", paths)).fetchone()[0]
            dropped_sentinel = con.execute(
                _count_sql("debtor", paths,
                           f"substr(L,69,1)='C' AND trim(substr(L,2,12))='{SENTINEL_DOC}'")
            ).fetchone()[0]
            arrow_table = con.execute(_build_transform_sql(paths, as_of)).to_arrow_table()
        finally:
            con.close()
        rows_processed = arrow_table.num_rows
        print(f"parsed: filings={filing_rows:,} secured={secured_rows:,} "
              f"debtor-grain rows={rows_processed:,} (dropped sentinel debtors={dropped_sentinel})")

        # PHASE 3 — Python I/O only: commit Arrow buffer → Lance on R2, then index.
        _write_lance(arrow_table, so)
        print(f"wrote Lance dataset → {DATASET_URI}")
        del arrow_table
        indexes, index_mode = _build_indexes(so)

        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("ingest", as_of, source_zips, filing_rows, secured_rows, dropped_sentinel,
                    rows_processed, indexes, index_mode, status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "dataset_uri": DATASET_URI, "as_of": as_of,
             "rows_processed": rows_processed, "filing_rows": filing_rows,
             "secured_rows": secured_rows, "dropped_sentinel": dropped_sentinel,
             "indexes": indexes, "index_mode": index_mode},
        )

    if status != "success":
        raise RuntimeError(f"fl_federal_tax_liens ingest failed: {error}")
    return {"feed": FEED, "dataset_uri": DATASET_URI, "as_of": as_of,
            "rows_processed": rows_processed, "filing_rows": filing_rows,
            "secured_rows": secured_rows, "dropped_sentinel": dropped_sentinel,
            "indexes": indexes, "index_mode": index_mode, "status": status}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_db() -> dict:
    """Create ops schema + ops.fl_flr_runs (idempotent)."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    return {"created": "ops.fl_flr_runs"}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15,
              memory=8192, cpu=2.0)
def verify(sample: int = 3) -> dict:
    """Read the committed dataset back: row count, schema, committed scalar indices, and an
    indexed lookup proving the BTREE blocking key resolves. Read-only; mutates nothing."""
    import json

    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    total = ds.count_rows()
    schema = [(f.name, str(f.type)) for f in ds.schema]

    committed = []
    for ix in ds.list_indices():
        committed.append({"name": ix.get("name") if isinstance(ix, dict) else getattr(ix, "name", None),
                          "type": str(ix.get("type") if isinstance(ix, dict) else getattr(ix, "type", None)),
                          "fields": ix.get("fields") if isinstance(ix, dict) else getattr(ix, "fields", None)})

    # Indexed lookup on a BTREE blocking key (proves the index resolves a predicate).
    probe = ds.to_table(filter="zip5 = '32202'",
                        columns=["doc_number", "normalized_legal_name", "zip5", "debtor_state"],
                        limit=sample).to_pylist()
    head = ds.to_table(columns=["doc_number", "normalized_legal_name", "zip5", "secured_count"],
                       limit=sample).to_pylist()

    print(f"=== {FEED} ===")
    print(f"uri:        {DATASET_URI}")
    print(f"total rows: {total:,}")
    print(f"schema:     {json.dumps(schema)}")
    print(f"indices:    {json.dumps(committed, default=str)}")
    print(f"sample:     {json.dumps(head, default=str)}")
    print(f"zip5=32202: {json.dumps(probe, default=str)}")
    return {"uri": DATASET_URI, "total_rows": total, "schema": schema,
            "committed_indices": committed, "sample": head, "indexed_probe": probe}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 30,
              memory=16384, cpu=4.0)
def reindex_local(trigger_callback_url: str | None = None) -> dict:
    """(Re)build the BTREE scalar indexes on the existing dataset via the R2-safe local
    round-trip (no re-ingest). Idempotent (replace=True)."""
    so = _r2_storage_options()
    built = _index_local_roundtrip(so)
    _post_callback(trigger_callback_url,
                   {"status": "success", "feed": FEED, "indexes": built, "index_mode": "local-roundtrip"})
    return {"feed": FEED, "dataset_uri": DATASET_URI, "indexes": built, "index_mode": "local-roundtrip"}


# ── Local entrypoints (modal run) ───────────────────────────────────────────────
@app.local_entrypoint()
def setup() -> None:
    """Create ops.fl_flr_runs."""
    print(init_db.remote())


@app.local_entrypoint()
def run(as_of: str = AS_OF_DEFAULT) -> None:
    """Execute the ingest (manual; no Trigger callback)."""
    import json

    print(json.dumps(ingest_fl_flr.remote(as_of=as_of, trigger_callback_url=None),
                     indent=2, default=str))


@app.local_entrypoint()
def reindex() -> None:
    """Rebuild scalar indexes only (R2-safe local round-trip)."""
    import json

    print(json.dumps(reindex_local.remote(), indent=2, default=str))


@app.local_entrypoint()
def run_verify(sample: int = 3) -> None:
    """Read-back verification — row count, schema, committed indices, indexed lookup."""
    import json

    print(json.dumps(verify.remote(sample=sample), indent=2, default=str))
