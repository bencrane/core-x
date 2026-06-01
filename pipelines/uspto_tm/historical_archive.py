"""Compute worker — USPTO Trademark Assignment HISTORICAL archive (pre-2024 / OCE).

Part of the ``uspto-trademarks-historical`` Modal app. Endpoint-less; spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — Python does ZIP I/O only, DuckDB does
100% of the transform/cast/nesting, Lance is the system of record on R2.

THE SOURCE. This is the USPTO Office-of-the-Chief-Economist (OCE) *Trademark Assignment
Dataset* — the flattened, research-grade relational rendering of every recorded TM
assignment (recordation 1952-03-26 → 2024-01-31). It is a ONE-SHOT historical baseline,
NOT the live daily XML feed (that is pipelines/uspto_tm/ingest.py → uspto-trademarks).
The two share the same source field vocabulary; this worker establishes the historical
counterpart that structurally mirrors the live ``uspto_tm_assignments`` dataset.

ARCHIVE SHAPE (reconnaissance-verified). The landing object is a zip-of-zips:
``s3://data-sink/landing/uspto/trademarks-prior/dta.zip`` → 10 inner zips. The complete
table set is CSV (7 tables); a partial .dta (Stata) subset (3 tables) is IGNORED entirely
— DuckDB has no native Stata reader and the CSV is the superset. This build flattens the
FIVE CSVs the directive enumerates — tm_assignment, tm_assignor, tm_assignee, tm_cf_no,
tm_convey — keyed on rf_id (the Reel-Frame Identification, verified =
lpad(reel_no,4,'0')||lpad(frame_no,4,'0') for all 1,380,594 rows). tm_docid is excluded
by the directive's table list; its only unique column (intl_reg_no) is therefore NOT
carried — properties come from the VALIDATED tm_cf_no linkage.

TYPE SAFETY (load-bearing). read_csv runs with all_varchar=true so DuckDB cannot silently
drop leading zeros on identifiers (249 serials are <8 digits; reg/cf-reg run to 7). Dates
are integer YYYYMMDD strings → TRY_CAST(TRY_STRPTIME(.,'%Y%m%d') AS DATE). Trademark
serials are canonicalized to 8 wide via lpad(serial,8,'0'). Every other field is held as
the LITERAL source string (nullif(trim(.),'')) — NO normalization: state names stay full
text (e.g. 'PENNSYLVANIA', not 'PA'), sparse assignor addresses are preserved, nothing is
filtered. Downstream resolvers own standardization (directive guardrail).

TARGET SCHEMA (assignment-grain, nested; mirrors the approved reconnaissance DDL). One row
per rf_id. Parties and properties are nested LIST<STRUCT>; each party carries a 6-part
address STRUCT(address_1, address_2, city, state, postcode, country) preserved verbatim.
The correspondent (the USPTO mailing contact — not a party) is its own STRUCT(name,
address_lines VARCHAR[]) holding the raw 4-line block faithfully. See SCHEMA_DDL below.

WRITE PATH (the nppes / pdl / sam_gov R2-multipart lesson). The dataset is written to LOCAL
disk, indexed locally, then PUBLISHED to R2 with boto3 (uniform-part multipart) — NOT
written straight to R2. A direct Lance→R2 write trips R2's "all non-trailing parts equal
length" rule (400 InvalidPart) once a scalar-index page_data.lance grows enough to escalate
object_store's adaptive multipart mid-upload. Lance is still the format, R2 still the
system of record, the URI still a plain s3:// string — only the upload transport changes.

INDEXING. BTREE on the resolution keys rf_id / reel_no / frame_no (as directed). The
flattened property_serial_numbers is a LIST<VARCHAR>; Lance BTREE is scalar-only, so the
fleet-correct index for list membership / instant resolution against other property graphs
is LABEL_LIST (the directive grouped it under "BTREE" — documented reconciliation, same
class as nppes's BITMAP-for-a-categorical reconciliation). Flip is a one-line change.

Control plane (Trigger v4 durable callback): on terminal state (success OR failure) the
worker (1) writes a run row to ops.uspto_tm_runs (dataset='assignments_historical') via
psycopg and (2) POSTs a FLAT JSON body to trigger_callback_url. No {"data": ...} envelope.

    modal deploy pipelines/uspto_tm/historical_archive.py
    modal run    pipelines/uspto_tm/historical_archive.py::migrate   # ensure ops.uspto_tm_runs
    modal run    pipelines/uspto_tm/historical_archive.py::run       # download → build → publish
    modal run    pipelines/uspto_tm/historical_archive.py::verify    # read-back proof from R2
"""

from __future__ import annotations

import os

import modal

# ── Source (landing) + target (active) ─────────────────────────────────────────
BUCKET = "data-sink"
LANDING_PREFIX = "landing/uspto/trademarks-prior/"
ZIP_NAME = "dta.zip"
TARGET_PREFIX = os.environ.get(
    "USPTO_TM_HIST_PREFIX", "active/uspto_tm_assignments_historical"
).strip("/") + "/"
TARGET_URI = f"s3://{BUCKET}/{TARGET_PREFIX}"

FEED = "uspto_tm_assignments_historical"
DATASET = "assignments_historical"
# Archive internal stamp (members dated 2024-03-29; data through 2024-01-31). Stamped on
# every row so the historical baseline is self-describing alongside the live feed.
SOURCE_RELEASE = os.environ.get("USPTO_TM_HIST_RELEASE", "2024-01")

SCRATCH_DIR = "/tmp/uspto_tm_hist"
LOCAL_DATASET = os.path.join(SCRATCH_DIR, "lance")

# The five CSV members the directive flattens (inner-zip basenames → member CSV).
CSV_MEMBERS = {
    "assignment": "tm_assignment.csv",
    "assignor": "tm_assignor.csv",
    "assignee": "tm_assignee.csv",
    "cf_no": "tm_cf_no.csv",
    "convey": "tm_convey.csv",
}

# Lance fragment sizing — fleet constants. max_rows 1,048,576 (Lance default; the whole
# dataset is ~1.38M rows ⇒ ~2 fragments). max_bytes 90 GiB (Lance documented default).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new dataset → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"
READ_BATCH_ROWS = 65536

# Scalar index plan (directive). BTREE = scalar resolution keys; LABEL_LIST = the
# List<VARCHAR> membership column (Lance BTREE is scalar-only — see module docstring).
INDEX_BTREE = ["rf_id", "reel_no", "frame_no"]
INDEX_LABEL_LIST = ["property_serial_numbers"]

# Human-readable record of the exact nested Lance schema this worker materializes. The
# authoritative schema is the Arrow schema DuckDB exports from TRANSFORM_SQL; this mirrors
# it for review/documentation.
SCHEMA_DDL = """
rf_id                    VARCHAR  NOT NULL              -- PK, Reel-Frame ID (BTREE)
reel_no                  INTEGER                        -- ordinal reel (BTREE)
frame_no                 VARCHAR                        -- zero-padded frame (BTREE)
file_id                  VARCHAR
conveyance_group         VARCHAR                        -- tm_convey.conv_group
conveyance_text          VARCHAR                        -- tm_assignment.convey_text
page_count               INTEGER
purge_indicator          VARCHAR                        -- 'N' across this snapshot
record_date              DATE                           -- tm_assignment.record_dt (YYYYMMDD)
last_update_date         DATE                           -- tm_assignment.last_update_dt
correspondent            STRUCT(name VARCHAR, address_lines VARCHAR[])   -- cname + caddress_1..4 (verbatim block)
assignors  LIST<STRUCT(
    person_or_organization_name VARCHAR, legal_entity_text VARCHAR, nationality VARCHAR,
    formerly_statement VARCHAR, composed_of_statement VARCHAR, dba_aka_ta_statement VARCHAR,
    execution_date DATE, date_acknowledged DATE, count_in_xml INTEGER,
    address STRUCT(address_1 VARCHAR, address_2 VARCHAR, city VARCHAR, state VARCHAR, postcode VARCHAR, country VARCHAR))>
assignees  LIST<STRUCT(
    person_or_organization_name VARCHAR, legal_entity_text VARCHAR, nationality VARCHAR,
    formerly_statement VARCHAR, composed_of_statement VARCHAR, dba_aka_ta_statement VARCHAR,
    count_in_xml INTEGER,
    address STRUCT(address_1 VARCHAR, address_2 VARCHAR, city VARCHAR, state VARCHAR, postcode VARCHAR, country VARCHAR))>
properties LIST<STRUCT(
    serial_number VARCHAR, registration_number VARCHAR,
    cf_serial_number VARCHAR, cf_registration_number VARCHAR, match_error VARCHAR)>   -- from tm_cf_no
property_serial_numbers  VARCHAR[]                       -- distinct coalesce(cf_serial, serial), 8-wide (LABEL_LIST)
source_release           VARCHAR                         -- '2024-01'
ingested_at              TIMESTAMP
"""

# Idempotent ops.* DDL — mirror of pipelines/uspto_tm/ops_uspto_tm_runs.sql (source of
# truth). Reused as-is: the historical run records as dataset='assignments_historical'.
_OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.uspto_tm_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset         text        NOT NULL,
    feed            text        NOT NULL,
    run_mode        text        NOT NULL,
    write_mode      text,
    dataset_uri     text,
    as_of           text,
    source_files    jsonb,
    parts_processed integer,
    rows_processed  bigint,
    rows_upserted   bigint,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_dataset_idx     ON ops.uspto_tm_runs (dataset);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_feed_idx        ON ops.uspto_tm_runs (feed);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_status_idx      ON ops.uspto_tm_runs (status);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_as_of_idx       ON ops.uspto_tm_runs (as_of DESC);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_recorded_at_idx ON ops.uspto_tm_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_reader; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance` (+ LABEL_LIST scalar index); lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",           # R2 landing read + dataset publish (uniform-part multipart)
    "requests>=2.32",        # Trigger waitpoint callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    # BTREE training sorts the column; bypass Lance's bounded spill-to-disk sorter (OOMs on
    # high-cardinality string columns). Cheap in-memory sort at ~1.38M rows.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("uspto-trademarks-historical", image=image)


# ──────────────────────────────────────────────────────────────────────────────
# R2 / object-store
# ──────────────────────────────────────────────────────────────────────────────
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret / env."""
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
    """boto3 S3 client for R2 — checksum behaviour forced to ``when_required`` (R2 rejects
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


# ──────────────────────────────────────────────────────────────────────────────
# Download + extract  (Python = I/O only)
# ──────────────────────────────────────────────────────────────────────────────
def _download_zip(s3, dest: str) -> int:
    """Download the landing dta.zip to local scratch. Returns bytes."""
    key = LANDING_PREFIX + ZIP_NAME
    s3.download_file(BUCKET, key, dest)
    return os.path.getsize(dest)


def _extract_csvs(zip_path: str, dest_dir: str) -> dict[str, str]:
    """Unpack the zip-of-zips: extract the five needed inner *.csv.zip members, then the
    single CSV inside each. Returns {logical_name: local_csv_path}. The .dta subset and
    tm_docid/tm_subparty are never touched (directive's five-table flatten)."""
    import zipfile

    os.makedirs(dest_dir, exist_ok=True)
    wanted_inner = {f"{csv}.zip": (name, csv) for name, csv in CSV_MEMBERS.items()}
    paths: dict[str, str] = {}

    with zipfile.ZipFile(zip_path) as outer:
        outer_names = set(outer.namelist())
        missing = [z for z in wanted_inner if z not in outer_names]
        if missing:
            raise RuntimeError(f"dta.zip missing expected inner zips: {missing} "
                               f"(saw {sorted(outer_names)})")
        for inner_zip_name, (logical, csv_base) in wanted_inner.items():
            inner_bytes = outer.read(inner_zip_name)
            inner_path = os.path.join(dest_dir, inner_zip_name)
            with open(inner_path, "wb") as fh:
                fh.write(inner_bytes)
            with zipfile.ZipFile(inner_path) as inner:
                members = [n for n in inner.namelist() if n.rsplit("/", 1)[-1] == csv_base]
                if not members:
                    raise RuntimeError(f"{inner_zip_name} has no member {csv_base} "
                                       f"(members={inner.namelist()[:5]})")
                inner.extract(members[0], dest_dir)
                extracted = os.path.join(dest_dir, members[0])
            paths[logical] = extracted
            try:
                os.remove(inner_path)
            except OSError:
                pass
    return paths


# ──────────────────────────────────────────────────────────────────────────────
# DuckDB transform (100%): five all-varchar CSVs → one nested assignment-grain Arrow
# ──────────────────────────────────────────────────────────────────────────────
# read_csv dialect: all_varchar (no leading-zero loss), RFC-4180 quoting pinned (the OCE
# CSVs quote embedded commas, e.g. individual names "LAST, FIRST" / "CO., INC."). Fail loud
# on malformed rows (no ignore_errors) — this is a fixed historical archive, not a feed.
_CSV_OPTS = "all_varchar=true, header=true, delim=',', quote='\"', escape='\"', null_padding=true"

# Reusable cast fragments.
_DATE = "TRY_CAST(TRY_STRPTIME(nullif(trim({c}),''), '%Y%m%d') AS DATE)"   # YYYYMMDD → DATE
_SER8 = "lpad(nullif(trim({c}),''), 8, '0')"                               # serial → 8-wide VARCHAR


def _csv(opt_path: str) -> str:
    return f"read_csv('{opt_path}', {_CSV_OPTS})"


def transform_sql(paths: dict[str, str]) -> str:
    """Build the full nested-assembly SQL. Each CSV is read all-varchar; parties/properties
    are grouped per rf_id into ordered LIST<STRUCT> (no source ordinal exists — `count` is a
    per-rf cardinality, NOT a row key — so arrays order deterministically by name/serial).
    Every string is the literal source value (nullif/trim only); only dates are cast and
    serials are lpad'd to 8. The assignment table is the spine (1 row per rf_id); convey is
    1:1; parties/properties LEFT JOIN (coalesce to empty list)."""
    d = _DATE.format
    s8 = _SER8.format
    return f"""
WITH asg AS (
    SELECT
        nullif(trim(rf_id),'')                              AS rf_id,
        TRY_CAST(nullif(trim(reel_no),'') AS INTEGER)       AS reel_no,
        nullif(trim(frame_no),'')                           AS frame_no,
        nullif(trim(file_id),'')                            AS file_id,
        nullif(trim(convey_text),'')                        AS conveyance_text,
        TRY_CAST(nullif(trim(page_count),'') AS INTEGER)    AS page_count,
        nullif(trim(purge_in),'')                           AS purge_indicator,
        {d(c='record_dt')}                                  AS record_date,
        {d(c='last_update_dt')}                             AS last_update_date,
        nullif(trim(cname),'')                              AS corr_name,
        list_filter(
            [nullif(trim(caddress_1),''), nullif(trim(caddress_2),''),
             nullif(trim(caddress_3),''), nullif(trim(caddress_4),'')],
            x -> x IS NOT NULL)                             AS corr_lines
    FROM {_csv(paths['assignment'])}
    WHERE nullif(trim(rf_id),'') IS NOT NULL
),
conv AS (
    SELECT nullif(trim(rf_id),'') AS rf_id, nullif(trim(conv_group),'') AS conveyance_group
    FROM {_csv(paths['convey'])}
    WHERE nullif(trim(rf_id),'') IS NOT NULL
),
asgnor_rows AS (
    SELECT
        nullif(trim(rf_id),'')   AS rf_id,
        nullif(trim(or_name),'') AS _ord,
        struct_pack(
            person_or_organization_name := nullif(trim(or_name),''),
            legal_entity_text           := nullif(trim(or_legal_entity_text),''),
            nationality                 := nullif(trim(or_natlty),''),
            formerly_statement          := nullif(trim(or_former_stm),''),
            composed_of_statement       := nullif(trim(or_comp_stm),''),
            dba_aka_ta_statement        := nullif(trim(or_dba_stm),''),
            execution_date              := {d(c='exec_dt')},
            date_acknowledged           := {d(c='ack_dt')},
            count_in_xml                := TRY_CAST(nullif(trim("count"),'') AS INTEGER),
            address := struct_pack(
                address_1 := nullif(trim(or_address_1),''),
                address_2 := nullif(trim(or_address_2),''),
                city      := nullif(trim(or_city),''),
                state     := nullif(trim(or_state),''),
                postcode  := nullif(trim(or_postcode),''),
                country   := nullif(trim(or_country),'')
            )
        ) AS party
    FROM {_csv(paths['assignor'])}
    WHERE nullif(trim(rf_id),'') IS NOT NULL
),
asgnor AS (
    SELECT rf_id, list(party ORDER BY _ord NULLS LAST) AS assignors
    FROM asgnor_rows GROUP BY rf_id
),
asgnee_rows AS (
    SELECT
        nullif(trim(rf_id),'')   AS rf_id,
        nullif(trim(ee_name),'') AS _ord,
        struct_pack(
            person_or_organization_name := nullif(trim(ee_name),''),
            legal_entity_text           := nullif(trim(ee_legal_entity_text),''),
            nationality                 := nullif(trim(ee_natlty),''),
            formerly_statement          := nullif(trim(ee_former_stm),''),
            composed_of_statement       := nullif(trim(ee_comp_stm),''),
            dba_aka_ta_statement        := nullif(trim(ee_dba_stm),''),
            count_in_xml                := TRY_CAST(nullif(trim("count"),'') AS INTEGER),
            address := struct_pack(
                address_1 := nullif(trim(ee_address_1),''),
                address_2 := nullif(trim(ee_address_2),''),
                city      := nullif(trim(ee_city),''),
                state     := nullif(trim(ee_state),''),
                postcode  := nullif(trim(ee_postcode),''),
                country   := nullif(trim(ee_country),'')
            )
        ) AS party
    FROM {_csv(paths['assignee'])}
    WHERE nullif(trim(rf_id),'') IS NOT NULL
),
asgnee AS (
    SELECT rf_id, list(party ORDER BY _ord NULLS LAST) AS assignees
    FROM asgnee_rows GROUP BY rf_id
),
prop_rows AS (
    SELECT
        nullif(trim(rf_id),'') AS rf_id,
        {s8(c='serial')}       AS _serial8,
        {s8(c='cf_serial_no')} AS _cf_serial8,
        struct_pack(
            serial_number          := {s8(c='serial')},
            registration_number    := nullif(trim(reg_no),''),
            cf_serial_number       := {s8(c='cf_serial_no')},
            cf_registration_number := nullif(trim(cf_registration_no),''),
            match_error            := nullif(trim(error),'')
        ) AS prop
    FROM {_csv(paths['cf_no'])}
    WHERE nullif(trim(rf_id),'') IS NOT NULL
),
props AS (
    SELECT
        rf_id,
        list(prop ORDER BY prop.serial_number NULLS LAST)              AS properties,
        list_distinct(list(coalesce(_cf_serial8, _serial8)))          AS property_serial_numbers
    FROM prop_rows GROUP BY rf_id
)
SELECT
    asg.rf_id,
    asg.reel_no,
    asg.frame_no,
    asg.file_id,
    conv.conveyance_group,
    asg.conveyance_text,
    asg.page_count,
    asg.purge_indicator,
    asg.record_date,
    asg.last_update_date,
    struct_pack(name := asg.corr_name, address_lines := asg.corr_lines)  AS correspondent,
    coalesce(asgnor.assignors, [])                                       AS assignors,
    coalesce(asgnee.assignees, [])                                       AS assignees,
    coalesce(props.properties, [])                                       AS properties,
    coalesce(props.property_serial_numbers, [])                          AS property_serial_numbers,
    '{SOURCE_RELEASE}'                                                   AS source_release,
    now()                                                                AS ingested_at
FROM asg
LEFT JOIN conv   USING (rf_id)
LEFT JOIN asgnor USING (rf_id)
LEFT JOIN asgnee USING (rf_id)
LEFT JOIN props  USING (rf_id)
"""


def build_local_dataset(paths: dict[str, str], local_lance_dir: str, threads: int = 4) -> int:
    """Run TRANSFORM_SQL and stream the nested Arrow result into a LOCAL Lance dataset
    (overwrite). Streamed via to_arrow_reader (bounded RSS). Returns rows written."""
    import shutil

    import duckdb
    import lance

    shutil.rmtree(local_lance_dir, ignore_errors=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"PRAGMA threads={threads};")
        con.execute("SET enable_progress_bar=false;")
        con.execute(f"SET temp_directory='{os.path.join(SCRATCH_DIR, 'duckdb_spill')}';")
        reader = con.sql(transform_sql(paths)).to_arrow_reader(batch_size=READ_BATCH_ROWS)
        lance.write_dataset(
            reader,
            local_lance_dir,
            schema=reader.schema,          # REQUIRED for a reader source
            mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
        )
    finally:
        con.close()
    return lance.dataset(local_lance_dir).count_rows()


# ──────────────────────────────────────────────────────────────────────────────
# Lance index (local) + R2 publish (uniform-part multipart)
# ──────────────────────────────────────────────────────────────────────────────
def create_indexes(local_lance_dir: str) -> list[str]:
    """BTREE on scalar resolution keys; LABEL_LIST on the List<VARCHAR> serial column
    (Lance BTREE is scalar-only — see module docstring). Local build avoids R2's multipart
    rule. An index miss is logged, never fatal — the data write is the critical artifact."""
    import lance

    ds = lance.dataset(local_lance_dir)
    built: list[str] = []
    for col in INDEX_BTREE:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE      ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in INDEX_LABEL_LIST:
        try:
            ds.create_scalar_index(col, index_type="LABEL_LIST", replace=True)
            built.append(f"LABEL_LIST:{col}")
            print(f"  LABEL_LIST ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN LABEL_LIST {col} failed: {exc}")
    return built


def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices (tolerant of pylance return-shape drift)."""
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


def publish_to_r2(s3, prefix: str, local_dir: str) -> int:
    """Idempotent publish: wipe the target R2 prefix, then upload the local Lance dataset
    (boto3/s3transfer = uniform-part multipart, R2-compliant). Returns files uploaded."""
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


# ──────────────────────────────────────────────────────────────────────────────
# State + callback + cleanup
# ──────────────────────────────────────────────────────────────────────────────
def _record_run(*, write_mode, dataset_uri, source_files, rows, status, error,
                started_at, completed_at) -> None:
    """Terminal run row → ops.uspto_tm_runs (psycopg). Best-effort: an audit-write failure
    never masks an otherwise-good ingest."""
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.uspto_tm_runs
                    (dataset, feed, run_mode, write_mode, dataset_uri, as_of, source_files,
                     parts_processed, rows_processed, rows_upserted, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (DATASET, FEED, "backfill", write_mode, dataset_uri, SOURCE_RELEASE,
                 Jsonb(source_files), 1, rows, rows, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. FLAT JSON body — NO
    {"data": ...} envelope, NO API key (the callbackHash in the url is the auth)."""
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
    """Remove scratch files/dirs. Best-effort."""
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


# ──────────────────────────────────────────────────────────────────────────────
# Orchestration — Modal-independent (driven by the @app.function OR a local harness)
# ──────────────────────────────────────────────────────────────────────────────
def run_pipeline(trigger_callback_url: str | None = None, threads: int = 4) -> dict:
    """download dta.zip → extract 5 CSVs → DuckDB nested transform → Lance overwrite on LOCAL
    disk → BTREE/LABEL_LIST index locally → boto3 publish to TARGET_URI → ops.* + callback.
    Returns the terminal payload. Re-raises on failure."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    os.makedirs(os.path.join(SCRATCH_DIR, "duckdb_spill"), exist_ok=True)

    zip_path = os.path.join(SCRATCH_DIR, ZIP_NAME)
    csv_paths: dict[str, str] = {}
    rows = 0
    built: list[str] = []
    status, error = "error", None

    try:
        so = _r2_storage_options()  # noqa: F841 — validates creds present before heavy work
        s3 = _s3_client()

        zip_bytes = _download_zip(s3, zip_path)
        print(f"downloaded s3://{BUCKET}/{LANDING_PREFIX}{ZIP_NAME} ({zip_bytes:,} bytes)")
        csv_paths = _extract_csvs(zip_path, SCRATCH_DIR)
        print(f"extracted CSVs: {[os.path.basename(p) for p in csv_paths.values()]}")
        _cleanup(zip_path)

        rows = build_local_dataset(csv_paths, LOCAL_DATASET, threads=threads)
        print(f"built local Lance dataset — {rows:,} assignment-grain rows")
        for p in csv_paths.values():
            _cleanup(p)
        csv_paths = {}

        built = create_indexes(LOCAL_DATASET)
        s3 = _s3_client()
        uploaded = publish_to_r2(s3, TARGET_PREFIX, LOCAL_DATASET)
        print(f"published {uploaded} files → {TARGET_URI}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        _cleanup(zip_path, *csv_paths.values(), LOCAL_DATASET)
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(
            write_mode="overwrite", dataset_uri=TARGET_URI,
            source_files=[ZIP_NAME] + list(CSV_MEMBERS.values()),
            rows=int(rows), status=status, error=error,
            started_at=started_at, completed_at=completed_at,
        )
        _post_callback(trigger_callback_url, {
            "status": status, "rows": int(rows), "feed": FEED, "dataset": DATASET,
            "run_mode": "backfill", "dataset_uri": TARGET_URI, "as_of": SOURCE_RELEASE,
        })

    if status != "success":
        raise RuntimeError(f"uspto_tm historical backfill failed: {error}")
    return {"feed": FEED, "dataset": DATASET, "run_mode": "backfill",
            "rows": int(rows), "indices": built, "dataset_uri": TARGET_URI,
            "source_release": SOURCE_RELEASE, "status": status}


# ──────────────────────────────────────────────────────────────────────────────
# Workers
# ──────────────────────────────────────────────────────────────────────────────
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 2,
    memory=16384,
    cpu=4.0,
)
def run(trigger_callback_url: str | None = None) -> dict:
    """One-shot historical backfill → TARGET_URI. Dispatcher-compatible (accepts
    trigger_callback_url); a manual run passes None and skips the callback."""
    return run_pipeline(trigger_callback_url=trigger_callback_url, threads=4)


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 20,
    memory=8192,
    cpu=2.0,
)
def verify() -> dict:
    """Read-back proof: open the materialized dataset from R2 and report row count,
    non-null counts on the indexed keys, and committed indices. Authoritative success check
    — reads what actually landed, independent of the write path's return value."""
    import lance

    ds = lance.dataset(TARGET_URI, storage_options=_r2_storage_options())
    out: dict = {"dataset_uri": TARGET_URI, "rows": ds.count_rows()}
    for col in INDEX_BTREE:
        try:
            out[f"{col}__non_null"] = ds.count_rows(filter=f"{col} IS NOT NULL")
        except Exception as exc:  # noqa: BLE001
            out[f"{col}__non_null"] = f"err: {str(exc)[:80]}"
    out["indices"] = _list_committed_indices(ds)
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_migration() -> dict:
    """Ensure ops.uspto_tm_runs exists (idempotent). Mirrors ops_uspto_tm_runs.sql."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.uspto_tm_runs')")
        present = cur.fetchone()[0]
    print(f"ops.uspto_tm_runs present = {present}")
    return {"table": "ops.uspto_tm_runs", "present": str(present)}


# ──────────────────────────────────────────────────────────────────────────────
# Manual ops entrypoints (local — no Trigger callback). ops.* write still fires.
# ──────────────────────────────────────────────────────────────────────────────
@app.local_entrypoint()
def migrate() -> None:
    import json
    print(json.dumps(apply_migration.remote(), indent=2, default=str))


@app.local_entrypoint()
def run_backfill() -> None:
    import json
    print(json.dumps(run.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def verify_dataset() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
