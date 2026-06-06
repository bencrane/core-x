"""Compute worker — NPPES derived analytical serving layer (canonical directive).

Part of the ``nppes-analytical`` Modal app — a NEW app, separate from the raw ``nppes``
ingest (pipelines/nppes/ingest.py) for blast-radius containment: this worker reads the raw
SoR READ-ONLY and writes ONLY the three derived prefixes, so a failure here can never
corrupt the raw monthly capture. Clean-room data plane: DuckDB does 100% of the transform,
Lance is the system of record on R2.

WHY THIS LAYER EXISTS (docs/nppes_structural_diagnostic.md §7). The raw NPPES snapshot at
``s3://data-sink/active/nppes/snapshot=YYYY-MM/`` is physically pristine but stored in raw
CMS dissemination shape, not analytical shape: dates are ``MM/DD/YYYY`` strings (naive range
filters silently return 0 — diag §6.3), specialty is shattered across ``taxonomy_code_1..15``
with no indexable form (15-col OR = 6.65 s, primary-slot-only undercounts 12% — diag §6.4),
the analytical axes carry no index (scan floor ≈97 MiB/s — diag §6.2), and ``npi`` is
unclustered so batch joins fan out to all 10 fragments (diag §1.1). This worker builds the
derived serving layer that reverses every one of those; the raw stays the immutable archive.

WHAT IT BUILDS — three append-only, per-snapshot Lance datasets (directive §2), each a pure
function of one raw month, rebuildable + idempotent (overwrite the month prefix):

    nppes_provider              1 row / NPI                 (9,551,447 @ 2026-05)  ORDER BY npi
    nppes_provider_taxonomy     1 row / (NPI, taxonomy slot) (11,952,809)          ORDER BY taxonomy_code, npi
    nppes_provider_identifier   1 row / (NPI, identifier slot) (2,759,800)          ORDER BY npi

LOAD-BEARING DECISIONS (directive §1):
  D2  Taxonomy is a LONG CHILD TABLE with a scalar ``BITMAP(taxonomy_code)`` — NOT
      ``list<struct>`` (a list element cannot carry a Lance scalar index) and NOT slot-1-only
      (1,106,232 providers — ~12% — hold their primary specialty in a slot whose code differs
      from code_1). This is the single change that makes specialty market-mapping possible.
  D3  Dates → ``date32`` via ``try_strptime(...,'%m/%d/%Y')`` (zero parse failures @ 2026-05).
  D5  Deactivated providers are KEPT, flagged ``is_active=false`` (the 343,321 stub cohort),
      never dropped.
  D8  Read raw ONCE → local out-of-core ``rawstage`` table → derive all three locally (no
      triple R2 scan). Local Lance stage → boto3 publish (the R2 multipart rule, diag §6.6).
  D9  Drop the dead column + 3 ``'<UNAVAIL>'`` redaction sentinels + per-row provenance
      (carried as schema metadata instead).

OUT-OF-CORE (directive §5, diag §4-C). 32 GiB / 8 vCPU / 512 GiB ephemeral disk. DuckDB
``memory_limit='20GB'``, ``temp_directory`` + the staging ``.duckdb`` under ``SCRATCH_DIR``
on the Modal ephemeral disk (``/tmp`` — there is NO ``/mnt/nvme`` mount). ``ORDER BY`` sorts
spill the decoded payload; ``max_temp_directory_size='128GB'``. ``LANCE_BYPASS_SPILLING=true``
for the high-card string BTREE trains (last_name, practice_address_line1, identifier_value) —
Lance's bounded spill sorter OOMs on these; in-RAM sort is <1 GiB each ≪ 32 GiB.

WRITE TRANSPORT (D8, diag §6.6 — non-negotiable). Build the Lance dataset on LOCAL disk,
set provenance metadata explicitly (the streaming ``to_arrow_reader`` write drops Arrow
schema KV metadata — verified — so ``update_schema_metadata`` is required AFTER the write),
build the scalar indices locally, then boto3 publish (uniform parts). NEVER write indices
straight to R2 (``400 InvalidPart`` once a BTREE ``page_data.lance`` escalates part size).

ACCEPTANCE GATE (directive §8). ``verify`` runs G1–G12 against the published layer.
Correctness gates (G1–G5, G8–G12) are ABSOLUTE build-fail gates; latency is warm-asserted
(G3 warm < 250 ms, G6 warm < 600 ms after a per-index warm-up) with the cold figure recorded
to the ledger, never gated (cold R2 round-trips alone exceed sub-second on a correct build).

    modal run    pipelines/nppes/materialize_analytical.py::init_state
    modal run    pipelines/nppes/materialize_analytical.py::materialize --snapshot-month 2026-05
    modal run    pipelines/nppes/materialize_analytical.py::verify --snapshot-month 2026-05
    modal run    pipelines/nppes/materialize_analytical.py::show_ledger
    modal deploy pipelines/nppes/materialize_analytical.py

The core build (``build_all``) and gate (``run_gate``) are plain module-level functions so the
directive §9.3 local dry-run can call them in-process (uv venv + ``doppler run``) and gate
locally BEFORE any R2 write; the Modal functions below are thin wrappers around them.
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
FEED = "nppes_analytical"

# Raw SoR (read-only) + the three derived tiers. All env-overridable.
RAW_ACTIVE_PREFIX = os.environ.get("NPPES_ACTIVE_PREFIX", "active/nppes").strip("/")
PROVIDER_PREFIX = os.environ.get("NPPES_PROVIDER_PREFIX", "active/nppes_provider").strip("/")
TAXONOMY_PREFIX = os.environ.get("NPPES_TAXONOMY_PREFIX", "active/nppes_provider_taxonomy").strip("/")
IDENTIFIER_PREFIX = os.environ.get("NPPES_IDENTIFIER_PREFIX", "active/nppes_provider_identifier").strip("/")

PREFIXES = {
    "nppes_provider": PROVIDER_PREFIX,
    "nppes_provider_taxonomy": TAXONOMY_PREFIX,
    "nppes_provider_identifier": IDENTIFIER_PREFIX,
}
TABLE_ORDER = ["nppes_provider", "nppes_provider_taxonomy", "nppes_provider_identifier"]

# All scratch/spill/stage on the Modal ephemeral disk — NEVER /mnt/nvme (no such mount).
SCRATCH_DIR = os.environ.get("NPPES_SCRATCH_DIR", "/tmp/nppes_analytical")
SPILL_DIR = os.path.join(SCRATCH_DIR, "duck_spill")

# Lance fragment sizing → directive §2 fragment counts (provider 10, taxonomy 12, identifier 3).
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"          # net-new datasets → current Lance default
READ_BATCH_ROWS = 131_072             # bounded write RSS (streaming to_arrow_reader)

# DuckDB out-of-core config (directive §5, diag §4-C).
DUCKDB_THREADS = 8
DUCKDB_MEMORY_LIMIT = "20GB"
DUCKDB_MAX_TEMP = "128GB"

# Scalar index plan (directive §4). BTREE = high-card resolution/range keys; BITMAP = the
# low/medium-card categoricals (taxonomy_code NDV 873, practice_state 59, entity_type_code 2,
# identifier_type_code 2, enumeration_year ~22; booleans is_active/is_primary).
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "nppes_provider": {
        "btree": ["npi", "last_name", "practice_address_line1", "practice_zip5",
                  "enumeration_date", "last_update_date"],
        "bitmap": ["entity_type_code", "is_active", "primary_taxonomy_code",
                   "practice_state", "enumeration_year"],
    },
    "nppes_provider_taxonomy": {
        # npi BTREE dropped per diagnostic §E.4 (docs/cms_nppes_relational_diagnostic.md):
        # the table is (taxonomy_code, npi)-clustered, so the npi BTREE delivered row
        # selection but ZERO fragment pruning (12/12 frags, 320 IOPs) at 147.43 MiB — 90%
        # of this table's index budget for a non-pruning path. Batch npi→taxonomy now routes
        # through the npi-clustered nppes_provider table (no live single-NPI reverse-lookup
        # consumer). The taxonomy_code BITMAP remains the load-bearing prune index.
        "btree": [],
        "bitmap": ["taxonomy_code", "is_primary", "license_state"],
    },
    "nppes_provider_identifier": {
        "btree": ["npi", "identifier_value"],
        "bitmap": ["identifier_type_code", "identifier_state"],
    },
}

# Clustering sort per table (directive §2) — clusters fragments by the hot predicate.
SORT_BY = {
    "nppes_provider": "npi",
    "nppes_provider_taxonomy": "taxonomy_code, npi",
    "nppes_provider_identifier": "npi",
}

# Gate representatives (directive §8): highest-volume primary taxonomy code + a hot state.
GATE_TAXONOMY_CODE = "106S00000X"     # G3 == 582,200
GATE_STATE = "TX"
TAXONOMY_SLOTS = 15
IDENTIFIER_SLOTS = 50

# Mirrored verbatim by pipelines/nppes/ops_nppes_analytical_runs.sql. Applied by init_state.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.nppes_analytical_runs (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                    text        NOT NULL,
    snapshot_month          text        NOT NULL,
    source_dataset_uri      text,
    source_version          bigint,
    provider_rows           bigint,
    taxonomy_rows           bigint,
    identifier_rows         bigint,
    date_parse_failures     bigint,
    dirty_state_nulled      bigint,
    provider_dataset_uri    text,
    taxonomy_dataset_uri    text,
    identifier_dataset_uri  text,
    indices_built           text,
    datasets_published      text,
    g3_cold_ms              double precision,
    g6_cold_ms              double precision,
    gate                    jsonb,
    status                  text        NOT NULL,
    error                   text,
    started_at              timestamptz,
    completed_at            timestamptz,
    recorded_at             timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS nppes_analytical_runs_month_idx    ON ops.nppes_analytical_runs (snapshot_month);
CREATE INDEX IF NOT EXISTS nppes_analytical_runs_feed_idx     ON ops.nppes_analytical_runs (feed);
CREATE INDEX IF NOT EXISTS nppes_analytical_runs_status_idx   ON ops.nppes_analytical_runs (status);
CREATE INDEX IF NOT EXISTS nppes_analytical_runs_recorded_idx ON ops.nppes_analytical_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`
    "pyarrow>=17",
    "boto3>=1.35",           # R2 dataset publish (uniform-part multipart)
    "requests>=2.32",        # Trigger waitpoint callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    # High-card string BTREE trains (last_name, practice_address_line1, identifier_value)
    # sort the column; bypass Lance's bounded spill sorter (OOMs on these; lance#2650).
    # In-RAM sort is <1 GiB each ≪ 32 GiB; trains run sequentially.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("nppes-analytical", image=image)


# --------------------------------------------------------------------------- #
# R2 / object-store  (proven helpers, mirrored from pipelines/nppes/ingest.py)
# --------------------------------------------------------------------------- #
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from env (Modal secret or doppler)."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret / doppler config.")
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
                 response_checksum_validation="when_required",
                 retries={"max_attempts": 5, "mode": "standard"})
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _month_prefix(prefix: str, snapshot_month: str) -> str:
    return f"{prefix}/snapshot={snapshot_month}/"


def _month_uri(prefix: str, snapshot_month: str) -> str:
    return f"s3://{BUCKET}/{_month_prefix(prefix, snapshot_month)}"


def _raw_month_uri(snapshot_month: str) -> str:
    return f"s3://{BUCKET}/{RAW_ACTIVE_PREFIX}/snapshot={snapshot_month}/"


def _local_stage(scratch_dir: str, name: str) -> str:
    return os.path.join(scratch_dir, f"{name}_lance")


# --------------------------------------------------------------------------- #
# Source projection + SQL builders (directive §3)
# --------------------------------------------------------------------------- #
def projected_cols() -> list[str]:
    """The 308 raw source columns the §2 output needs (verified to resolve against the live
    raw schema). rawstage = SELECT these FROM raw — one R2 read (D8). Excludes the §D9 noise
    (dead column, 3 redaction sentinels, per-row provenance) and the 19 unused secondary
    fields not carried by §2."""
    scalar = [
        "npi", "entity_type_code", "npi_deactivation_date", "npi_reactivation_date",
        "provider_organization_name_legal_business_name", "provider_last_name_legal_name",
        "provider_first_name", "provider_middle_name", "provider_name_prefix_text",
        "provider_name_suffix_text", "provider_credential_text", "provider_sex_code",
        "is_sole_proprietor", "is_organization_subpart",
        "provider_first_line_business_practice_location_address",
        "provider_second_line_business_practice_location_address",
        "provider_business_practice_location_address_city_name",
        "provider_business_practice_location_address_state_name",
        "provider_business_practice_location_address_postal_code",
        "provider_business_practice_location_address_country_code_if_outside_us",
        "provider_business_practice_location_address_telephone_number",
        "provider_business_practice_location_address_fax_number",
        "provider_business_mailing_address_city_name",
        "provider_business_mailing_address_state_name",
        "provider_business_mailing_address_postal_code",
        "provider_enumeration_date", "last_update_date", "certification_date",
        "authorized_official_last_name", "authorized_official_first_name",
        "authorized_official_title_or_position", "parent_organization_lbn", "snapshot_month",
    ]
    tax: list[str] = []
    for i in range(1, TAXONOMY_SLOTS + 1):
        tax += [f"healthcare_provider_taxonomy_code_{i}",
                f"healthcare_provider_primary_taxonomy_switch_{i}",
                f"provider_license_number_{i}",
                f"provider_license_number_state_code_{i}",
                f"healthcare_provider_taxonomy_group_{i}"]
    ident: list[str] = []
    for i in range(1, IDENTIFIER_SLOTS + 1):
        ident += [f"other_provider_identifier_{i}",
                  f"other_provider_identifier_type_code_{i}",
                  f"other_provider_identifier_state_{i}",
                  f"other_provider_identifier_issuer_{i}"]
    return scalar + tax + ident


# USPS-valid 2-letter state set (directive §3.2): 50 states + DC + territories
# (AS GU MP PR VI UM) + freely-associated states (FM MH PW) + military (AA AE AP) = 63.
# Everything else 2-letter (BC/ON/QC/MX/UK/JP/…) is foreign → correctly NULLed.
USPS_STATES = (
    "['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS',"
    "'KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY',"
    "'NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV',"
    "'WI','WY','DC','AS','GU','MP','PR','VI','UM','FM','MH','PW','AA','AE','AP']"
)

MACROS_SQL = f"""
CREATE OR REPLACE MACRO clean_state(s) AS (
  CASE WHEN list_contains({USPS_STATES}, upper(trim(s))) THEN upper(trim(s)) ELSE NULL END);
CREATE OR REPLACE MACRO zip5(z) AS nullif(regexp_extract(z, '^\\s*(\\d{{5}})', 1), '');
CREATE OR REPLACE MACRO d(x) AS try_strptime(x, '%m/%d/%Y')::DATE;
"""

DATE_COLS = ["provider_enumeration_date", "last_update_date", "npi_deactivation_date",
             "npi_reactivation_date", "certification_date"]


def provider_select() -> str:
    """The §3.3 1-row-per-NPI projection FROM rawstage (no ORDER BY — applied at stream)."""
    primary = "coalesce(\n" + ",\n".join(
        f"    CASE WHEN healthcare_provider_primary_taxonomy_switch_{i}='Y' "
        f"THEN healthcare_provider_taxonomy_code_{i} END"
        for i in range(1, TAXONOMY_SLOTS + 1)
    ) + ",\n    healthcare_provider_taxonomy_code_1\n  ) AS primary_taxonomy_code"
    return f"""
SELECT
  npi,
  entity_type_code,
  CASE entity_type_code WHEN '1' THEN 'individual' WHEN '2' THEN 'organization' END AS entity_type,
  (d(npi_deactivation_date) IS NULL
    OR (d(npi_reactivation_date) IS NOT NULL
        AND d(npi_reactivation_date) >= d(npi_deactivation_date)
        AND entity_type_code IS NOT NULL)) AS is_active,
  CASE WHEN entity_type_code='2' THEN provider_organization_name_legal_business_name
       WHEN entity_type_code='1' THEN concat_ws(', ', provider_last_name_legal_name,
                                       trim(concat_ws(' ', provider_first_name, provider_middle_name)))
       ELSE coalesce(provider_organization_name_legal_business_name, provider_last_name_legal_name) END AS provider_name,
  provider_organization_name_legal_business_name AS organization_name,
  provider_last_name_legal_name AS last_name,
  provider_first_name AS first_name,
  provider_middle_name AS middle_name,
  provider_name_prefix_text AS name_prefix,
  provider_name_suffix_text AS name_suffix,
  provider_credential_text AS credential,
  provider_sex_code AS sex_code,
  is_sole_proprietor,
  is_organization_subpart,
  {primary},
  provider_first_line_business_practice_location_address AS practice_address_line1,
  provider_second_line_business_practice_location_address AS practice_address_line2,
  provider_business_practice_location_address_city_name AS practice_city,
  clean_state(provider_business_practice_location_address_state_name) AS practice_state,
  zip5(provider_business_practice_location_address_postal_code) AS practice_zip5,
  provider_business_practice_location_address_postal_code AS practice_zip,
  provider_business_practice_location_address_country_code_if_outside_us AS practice_country,
  provider_business_practice_location_address_telephone_number AS practice_phone,
  provider_business_practice_location_address_fax_number AS practice_fax,
  provider_business_mailing_address_city_name AS mailing_city,
  clean_state(provider_business_mailing_address_state_name) AS mailing_state,
  zip5(provider_business_mailing_address_postal_code) AS mailing_zip5,
  d(provider_enumeration_date) AS enumeration_date,
  year(d(provider_enumeration_date))::SMALLINT AS enumeration_year,
  d(last_update_date) AS last_update_date,
  d(npi_deactivation_date) AS deactivation_date,
  d(npi_reactivation_date) AS reactivation_date,
  d(certification_date) AS certification_date,
  authorized_official_last_name,
  authorized_official_first_name,
  authorized_official_title_or_position AS authorized_official_title,
  parent_organization_lbn,
  snapshot_month
FROM rawstage
""".strip()


def taxonomy_select(n: int = TAXONOMY_SLOTS) -> str:
    """§3.4 — NULL-filtered UNION ALL over 15 slots, each arm carrying its parallel
    switch/license/group (a blind UNPIVOT would orphan the switch from its code)."""
    parts = [f"""
      SELECT npi, {i}::TINYINT AS taxonomy_rank,
             healthcare_provider_taxonomy_code_{i} AS taxonomy_code,
             (healthcare_provider_primary_taxonomy_switch_{i} = 'Y') AS is_primary,
             provider_license_number_{i} AS license_number,
             clean_state(provider_license_number_state_code_{i}) AS license_state,
             healthcare_provider_taxonomy_group_{i} AS taxonomy_group,
             snapshot_month
      FROM rawstage WHERE healthcare_provider_taxonomy_code_{i} IS NOT NULL""" for i in range(1, n + 1)]
    return " UNION ALL ".join(parts)


def identifier_select(n: int = IDENTIFIER_SLOTS) -> str:
    """§3.4 — NULL-filtered UNION ALL over 50 other-identifier slots."""
    parts = [f"""
      SELECT npi, {i}::TINYINT AS identifier_rank,
             other_provider_identifier_{i} AS identifier_value,
             other_provider_identifier_type_code_{i} AS identifier_type_code,
             clean_state(other_provider_identifier_state_{i}) AS identifier_state,
             other_provider_identifier_issuer_{i} AS identifier_issuer,
             snapshot_month
      FROM rawstage WHERE other_provider_identifier_{i} IS NOT NULL""" for i in range(1, n + 1)]
    return " UNION ALL ".join(parts)


def _select_for(name: str) -> str:
    if name == "nppes_provider":
        return provider_select()
    if name == "nppes_provider_taxonomy":
        return taxonomy_select()
    if name == "nppes_provider_identifier":
        return identifier_select()
    raise KeyError(name)


# --------------------------------------------------------------------------- #
# DuckDB connection + Lance write/metadata/index helpers
# --------------------------------------------------------------------------- #
def _connect(scratch_dir: str):
    """Out-of-core DuckDB on the ephemeral disk (directive §3.1 / §5)."""
    import duckdb

    os.makedirs(SPILL_DIR if scratch_dir == SCRATCH_DIR else os.path.join(scratch_dir, "duck_spill"),
                exist_ok=True)
    spill = os.path.join(scratch_dir, "duck_spill")
    con = duckdb.connect(os.path.join(scratch_dir, "nppes_build.duckdb"))
    con.execute(f"PRAGMA threads={DUCKDB_THREADS};")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"SET temp_directory='{spill}';")
    con.execute(f"SET max_temp_directory_size='{DUCKDB_MAX_TEMP}';")
    con.execute("SET preserve_insertion_order=true;")  # carry the ORDER BY into the Lance write
    con.execute("SET enable_progress_bar=false;")
    return con


def _stream_to_lance(con, select_sql: str, sort_by: str, local_path: str) -> int:
    """Stream a sorted DuckDB result → local Lance (bounded RSS). The ORDER BY is applied on
    the streaming read so the Lance fragments are physically clustered by the hot predicate
    (this is what makes fragment pruning work — directive §2)."""
    import shutil

    import lance

    shutil.rmtree(local_path, ignore_errors=True)
    reader = con.execute(f"SELECT * FROM ({select_sql}) ORDER BY {sort_by}").to_arrow_reader(READ_BATCH_ROWS)
    lance.write_dataset(
        reader, local_path,
        schema=reader.schema,           # REQUIRED for a reader source
        mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
    )
    return lance.dataset(local_path).count_rows()


def _set_metadata(local_path: str, *, raw_uri: str, source_member: str | None,
                  snapshot_month: str) -> None:
    """Set provenance as schema KV metadata on the committed dataset. REQUIRED: the streaming
    to_arrow_reader write drops Arrow schema metadata (verified, pylance 7.0.0), so this must
    run AFTER the write and BEFORE publish (gated by G12)."""
    import lance

    lance.dataset(local_path).update_schema_metadata({
        "source_snapshot_uri": raw_uri,
        "source_member": source_member or "",
        "pipeline": "materialize_analytical",
        "snapshot_month": snapshot_month,
    }, replace=True)


def _create_indexes(local_path: str, btree: list[str], bitmap: list[str]) -> list[str]:
    """BTREE + BITMAP scalar indexes on the LOCAL dataset (no storage_options — local writes
    avoid R2's multipart rule). replace=True → idempotent. An index miss is logged, never
    fatal — the Lance data write is the critical artifact."""
    import lance

    ds = lance.dataset(local_path)
    built: list[str] = []
    for col in btree:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in bitmap:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


def _replace_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Idempotent publish: wipe the month's R2 prefix, then upload the local Lance dataset
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


def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices (tolerant of pylance shape drift)."""
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
    return [{"error": "no list_indices/list_indexes method"}]


def _meta_get(ds, key: str) -> str | None:
    """Read a schema-metadata value tolerant of str|bytes keys (pylance returns bytes)."""
    md = ds.schema.metadata or {}
    for k, v in md.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else k
        if kk == key:
            return v.decode() if isinstance(v, (bytes, bytearray)) else v
    return None


# --------------------------------------------------------------------------- #
# Core build — read raw once → rawstage → derive + stage + index all three (D8)
# --------------------------------------------------------------------------- #
def build_all(snapshot_month: str, *, scratch_dir: str = SCRATCH_DIR,
              raw_uri: str | None = None) -> dict:
    """Build the three derived datasets to LOCAL Lance stages with provenance metadata + scalar
    indices. Does NOT publish (the caller gates locally first, then publishes). Pure function
    of one raw month; callable in-process by the §9.3 dry-run and by the Modal worker."""
    import lance

    os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")
    os.makedirs(scratch_dir, exist_ok=True)
    raw_uri = raw_uri or _raw_month_uri(snapshot_month)
    so = _r2_storage_options()
    raw_ds = lance.dataset(raw_uri, storage_options=so)
    source_version = int(raw_ds.version)

    # source_member: constant within the partition (NDV=1) — one cheap limit-1 Lance read.
    sm_tbl = raw_ds.scanner(columns=["source_member"], limit=1).to_table()
    source_member = sm_tbl.column(0)[0].as_py() if sm_tbl.num_rows else None

    con = _connect(scratch_dir)
    result: dict = {"snapshot_month": snapshot_month, "raw_uri": raw_uri,
                    "source_version": source_version, "source_member": source_member,
                    "local": {}, "rows": {}, "indices": {}}
    try:
        con.register("raw", raw_ds)
        con.execute(MACROS_SQL)
        # D8 — single R2 read of the projected raw into a local out-of-core table.
        cols = ", ".join(projected_cols())
        print(f"[build] staging projected raw ({len(projected_cols())} cols) → rawstage (1 R2 read)")
        con.execute(f"CREATE OR REPLACE TABLE rawstage AS SELECT {cols} FROM raw")
        con.unregister("raw")
        staged = con.execute("SELECT count(*) FROM rawstage").fetchone()[0]
        print(f"[build] rawstage rows = {staged:,}")

        # Date-parse quality (gate G8) + dirty-state count (ledger context for G9).
        total_fail = 0
        max_ratio = 0.0
        for c in DATE_COLS:
            nonnull, fails = con.execute(
                f"SELECT count({c}), count(*) FILTER (WHERE {c} IS NOT NULL AND d({c}) IS NULL) "
                f"FROM rawstage").fetchone()
            ratio = (fails / nonnull) if nonnull else 0.0
            total_fail += int(fails)
            max_ratio = max(max_ratio, ratio)
            print(f"[build] date {c}: non_null={nonnull:,} parse_fail={fails:,} ratio={ratio:.8f}")
        dirty = con.execute(
            "SELECT count(*) FROM rawstage WHERE provider_business_practice_location_address_state_name "
            "IS NOT NULL AND clean_state(provider_business_practice_location_address_state_name) IS NULL"
        ).fetchone()[0]
        result["date_parse_failures"] = total_fail
        result["date_parse_max_ratio"] = max_ratio
        result["dirty_state_nulled"] = int(dirty)
        print(f"[build] date_parse_failures={total_fail} max_ratio={max_ratio:.8f} dirty_state_nulled={dirty:,}")

        # Derive + stage + meta + index each table.
        for name in TABLE_ORDER:
            local = _local_stage(scratch_dir, name)
            print(f"[build] {name}: stream → {local}  (ORDER BY {SORT_BY[name]})")
            rows = _stream_to_lance(con, _select_for(name), SORT_BY[name], local)
            _set_metadata(local, raw_uri=raw_uri, source_member=source_member,
                          snapshot_month=snapshot_month)
            built = _create_indexes(local, INDEX_PLAN[name]["btree"], INDEX_PLAN[name]["bitmap"])
            frags = len(lance.dataset(local).get_fragments())
            result["local"][name] = local
            result["rows"][name] = rows
            result["indices"][name] = built
            print(f"[build] {name}: rows={rows:,} fragments={frags} indices={built}")
    finally:
        con.close()
    return result


def publish_table(name: str, snapshot_month: str, local_path: str) -> str:
    """boto3 publish one local Lance stage → its month R2 prefix (wipe + uniform upload)."""
    s3 = _s3_client()
    prefix = _month_prefix(PREFIXES[name], snapshot_month)
    uploaded = _replace_r2_prefix(s3, prefix, local_path)
    uri = _month_uri(PREFIXES[name], snapshot_month)
    print(f"[publish] {name}: {uploaded} files → {uri}")
    return uri


# --------------------------------------------------------------------------- #
# Acceptance gate (directive §8) — run against local stages OR the published R2 layer
# --------------------------------------------------------------------------- #
def _open(name: str, snapshot_month: str, *, local: dict | None, so: dict | None):
    """Open a derived dataset — local stage (dry-run) or R2 prefix (verify)."""
    import lance

    if local is not None:
        return lance.dataset(local[name])
    return lance.dataset(_month_uri(PREFIXES[name], snapshot_month), storage_options=so)


def _frags_scanned(ds, filt: str) -> tuple[int, int]:
    """(fragments_scanned, num_fragments) from analyze_plan() for a filtered scan (G7)."""
    import re

    txt = str(ds.scanner(filter=filt).analyze_plan())
    sc = re.search(r"fragments_scanned=(\d+)", txt)
    nf = re.search(r"num_fragments=(\d+)", txt)
    total = int(nf.group(1)) if nf else len(ds.get_fragments())
    return (int(sc.group(1)) if sc else total), total


def run_gate(snapshot_month: str, *, local: dict | None = None,
             raw_uri: str | None = None, build_metrics: dict | None = None) -> dict:
    """Run the §8 acceptance gate against the three datasets (local stages if ``local`` given,
    else the published R2 prefixes). Correctness gates G1–G5/G8–G12 are absolute; G3/G6 assert
    a WARM threshold after a per-index warm-up and RECORD the cold figure. Returns a dict with
    a per-gate verdict, ``passed`` (all absolute gates), and the recorded latencies."""
    import time

    import duckdb
    import lance

    so = None if local is not None else _r2_storage_options()
    raw_uri = raw_uri or _raw_month_uri(snapshot_month)
    prov = _open("nppes_provider", snapshot_month, local=local, so=so)
    tax = _open("nppes_provider_taxonomy", snapshot_month, local=local, so=so)
    ident = _open("nppes_provider_identifier", snapshot_month, local=local, so=so)

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8; SET enable_progress_bar=false;")
    con.register("prov", prov)
    con.register("tax", tax)
    con.register("ident", ident)

    # Latency assertion policy: assert the warm thresholds only against warm/local stages (the
    # build-time gate), where the measurement is meaningful (NVMe / in-process). On the R2
    # read-back, RECORD latency but never absolute-fail — R2-remote reads are egress-bound and
    # environment-relative (directive §8 / review M3): a metadata-only count (G3) still warms to
    # milliseconds, but a data-scan join (G6) re-egresses provider columns and cannot warm
    # sub-second from a remote vantage. Correctness assertions stay absolute in BOTH modes.
    assert_latency = local is not None
    gates: dict[str, dict] = {}

    def record(gid, desc, ok, **extra):
        gates[gid] = {"desc": desc, "pass": bool(ok), **extra}
        print(f"  {gid} {'PASS' if ok else 'FAIL'} — {desc} {extra if extra else ''}")

    # G1 — provider rows == raw distinct npi (9,551,447)
    p_rows = prov.count_rows()
    record("G1", "provider rows == 9,551,447", p_rows == 9_551_447, value=p_rows, absolute=True)

    # G2 — npi unique (PK preserved)
    n_all, n_dist = con.execute("SELECT count(*), count(DISTINCT npi) FROM prov").fetchone()
    record("G2", "count(DISTINCT npi) == rows", n_all == n_dist, rows=n_all, distinct=n_dist, absolute=True)

    # G3 — taxonomy long reproduces the raw any-of-15 PROVIDER count (582,200) via
    # count(DISTINCT npi); BITMAP filter used; WARM < 250 ms (cold recorded). Reconciliation:
    # the long grain is (npi, populated slot), so count(*) WHERE code=X = 586,363 includes
    # 4,163 rows from 4,068 providers who list this code in >1 slot (each slot carrying its
    # own license/group — faithfully preserved, not deduped). The diagnostic's "any-of-15"
    # baseline (582,200) is a distinct-PROVIDER count, so the long table reproduces it as
    # count(DISTINCT npi) — NOT count(*). count(*) is recorded alongside for transparency.
    g3_cold0 = time.perf_counter()
    g3_filter_rows = tax.count_rows(filter=f"taxonomy_code = '{GATE_TAXONOMY_CODE}'")
    g3_cold_ms = (time.perf_counter() - g3_cold0) * 1000
    g3_w0 = time.perf_counter()
    tax.count_rows(filter=f"taxonomy_code = '{GATE_TAXONOMY_CODE}'")
    g3_warm_ms = (time.perf_counter() - g3_w0) * 1000
    g3_providers = con.execute(
        f"SELECT count(DISTINCT npi) FROM tax WHERE taxonomy_code = '{GATE_TAXONOMY_CODE}'").fetchone()[0]
    tax_idx = _list_committed_indices(tax)
    bitmap_ok = any((list(i.get("fields") or []) == ["taxonomy_code"]
                     and "bitmap" in str(i.get("type", "")).lower()) for i in tax_idx)
    g3_correct = g3_providers == 582_200 and bitmap_ok
    g3_lat_ok = g3_warm_ms < 250
    record("G3", f"taxonomy_code='{GATE_TAXONOMY_CODE}' distinct providers == 582,200, BITMAP"
           + (", warm<250ms" if assert_latency else " (latency recorded)"),
           g3_correct and (g3_lat_ok or not assert_latency),
           providers=g3_providers, rows=g3_filter_rows, bitmap=bitmap_ok,
           warm_ms=round(g3_warm_ms, 1), cold_ms=round(g3_cold_ms, 1),
           latency_ok=g3_lat_ok, latency_gated=assert_latency)

    # G4 — taxonomy is_primary count == 9,208,126; ≤ 1 primary per npi
    g4_count, g4_max = con.execute(
        "SELECT (SELECT count(*) FILTER (WHERE is_primary) FROM tax), "
        "(SELECT max(p) FROM (SELECT npi, count(*) FILTER (WHERE is_primary) p FROM tax GROUP BY npi))"
    ).fetchone()
    record("G4", "is_primary == 9,208,126 and ≤1 primary/npi",
           g4_count == 9_208_126 and (g4_max or 0) <= 1,
           primaries=g4_count, max_per_npi=g4_max, absolute=True)

    # G5 — date range via date32 == 3,292,670
    g5 = prov.count_rows(filter="enumeration_date >= date '2020-01-01'")
    record("G5", "enumeration_date >= 2020-01-01 == 3,292,670", g5 == 3_292_670, value=g5, absolute=True)

    # G6 — specialty×geo join correct; WARM < 600 ms (cold recorded). Mechanism (M2): taxonomy
    # BITMAP push + provider dynamic-range prune (NOT a two-sided npi-BTREE take).
    join_sql = (f"SELECT count(DISTINCT p.npi) FROM tax t JOIN prov p ON t.npi = p.npi "
                f"WHERE t.taxonomy_code = '{GATE_TAXONOMY_CODE}' AND p.practice_state = '{GATE_STATE}'")
    g6_c0 = time.perf_counter()
    g6_join = con.execute(join_sql).fetchone()[0]
    g6_cold_ms = (time.perf_counter() - g6_c0) * 1000
    g6_w0 = time.perf_counter()
    con.execute(join_sql).fetchone()
    g6_warm_ms = (time.perf_counter() - g6_w0) * 1000
    # correctness cross-check: same set via independent semi-joins
    g6_ref = con.execute(
        f"SELECT count(*) FROM (SELECT DISTINCT npi FROM tax WHERE taxonomy_code='{GATE_TAXONOMY_CODE}') t "
        f"WHERE EXISTS (SELECT 1 FROM prov p WHERE p.npi=t.npi AND p.practice_state='{GATE_STATE}')"
    ).fetchone()[0]
    g6_correct = g6_join == g6_ref and g6_join > 0
    g6_lat_ok = g6_warm_ms < 600
    record("G6", f"join code×{GATE_STATE} correct"
           + (", warm<600ms" if assert_latency else " (latency recorded)"),
           g6_correct and (g6_lat_ok or not assert_latency),
           value=g6_join, ref=g6_ref, warm_ms=round(g6_warm_ms, 1),
           cold_ms=round(g6_cold_ms, 1), latency_ok=g6_lat_ok, latency_gated=assert_latency)

    # G7 — batch-npi fragment pruning on the npi-sorted provider table (Lance scanner prefilter)
    ids = [r[0] for r in con.execute("SELECT npi FROM prov ORDER BY npi LIMIT 1000").fetchall()]
    in_list = ",".join("'" + i.replace("'", "''") + "'" for i in ids)
    fscan, ftot = _frags_scanned(prov, f"npi IN ({in_list})")
    record("G7", "batch-npi prunes fragments (scanned < total)", fscan < ftot,
           fragments_scanned=fscan, num_fragments=ftot)

    # G8 — date parse failures < 0.0001 (from build metrics, else recompute from raw)
    if build_metrics and "date_parse_max_ratio" in build_metrics:
        g8_ratio = build_metrics["date_parse_max_ratio"]
        g8_fail = build_metrics.get("date_parse_failures")
    else:
        con.register("raw", lance.dataset(raw_uri, storage_options=_r2_storage_options()))
        con.execute(MACROS_SQL)
        g8_ratio, g8_fail = 0.0, 0
        for c in DATE_COLS:
            nn, ff = con.execute(
                f"SELECT count({c}), count(*) FILTER (WHERE {c} IS NOT NULL AND d({c}) IS NULL) FROM raw"
            ).fetchone()
            g8_ratio = max(g8_ratio, (ff / nn) if nn else 0.0)
            g8_fail += int(ff)
        con.unregister("raw")
    record("G8", "max date parse-fail ratio < 0.0001", (g8_ratio or 0) < 0.0001,
           max_ratio=round(float(g8_ratio or 0), 10), failures=g8_fail, absolute=True)

    # G9 — cleaned practice_state ∈ USPS ∪ {NULL}; ≤ 63 distinct
    g9_distinct = con.execute("SELECT count(DISTINCT practice_state) FROM prov WHERE practice_state IS NOT NULL").fetchone()[0]
    g9_bad = con.execute(
        f"SELECT count(*) FROM (SELECT DISTINCT practice_state FROM prov WHERE practice_state IS NOT NULL) "
        f"WHERE practice_state NOT IN (SELECT unnest({USPS_STATES}))"
    ).fetchone()[0]
    record("G9", "practice_state ⊆ USPS, ≤63 distinct", g9_distinct <= 63 and g9_bad == 0,
           distinct=g9_distinct, out_of_set=g9_bad, absolute=True)

    # G10 — cross-dataset integrity: shared snapshot_month; child npis ⊆ provider npis
    months = {nm: con.execute(f"SELECT DISTINCT snapshot_month FROM {al}").fetchall()
              for nm, al in (("prov", "prov"), ("tax", "tax"), ("ident", "ident"))}
    one_month = all(len(v) == 1 for v in months.values()) and \
        len({v[0][0] for v in months.values()}) == 1
    tax_orphan = con.execute("SELECT count(*) FROM (SELECT DISTINCT npi FROM tax) t "
                             "WHERE NOT EXISTS (SELECT 1 FROM prov p WHERE p.npi=t.npi)").fetchone()[0]
    id_orphan = con.execute("SELECT count(*) FROM (SELECT DISTINCT npi FROM ident) t "
                            "WHERE NOT EXISTS (SELECT 1 FROM prov p WHERE p.npi=t.npi)").fetchone()[0]
    record("G10", "shared snapshot_month; children ⊆ provider", one_month and tax_orphan == 0 and id_orphan == 0,
           one_month=one_month, tax_orphan=tax_orphan, id_orphan=id_orphan, absolute=True)

    # G11 — is_active invariant: NOT is_active count == entity_type_code NULL count == 343,321
    g11_inactive, g11_null_etc = con.execute(
        "SELECT count(*) FILTER (WHERE NOT is_active), count(*) FILTER (WHERE entity_type_code IS NULL) FROM prov"
    ).fetchone()
    record("G11", "NOT is_active == entity_type_code NULL == 343,321",
           g11_inactive == 343_321 and g11_null_etc == 343_321,
           inactive=g11_inactive, null_entity_type=g11_null_etc, absolute=True)

    # G12 — provenance round-trip: each dataset's source_snapshot_uri non-empty
    g12 = {n: bool(_meta_get(ds, "source_snapshot_uri"))
           for n, ds in (("nppes_provider", prov), ("nppes_provider_taxonomy", tax),
                         ("nppes_provider_identifier", ident))}
    record("G12", "source_snapshot_uri metadata non-empty per dataset", all(g12.values()),
           per_dataset=g12, absolute=True)

    con.close()

    # Each gate's "pass" already encodes the policy (G3/G6 latency is part of the verdict only
    # when assert_latency=True, i.e. against warm/local stages); correctness is absolute always.
    passed = all(v["pass"] for v in gates.values())
    return {"passed": passed, "gates": gates,
            "g3_cold_ms": gates["G3"]["cold_ms"], "g6_cold_ms": gates["G6"]["cold_ms"],
            "g3_warm_ms": gates["G3"]["warm_ms"], "g6_warm_ms": gates["G6"]["warm_ms"]}


# --------------------------------------------------------------------------- #
# ops ledger
# --------------------------------------------------------------------------- #
def _record_run(*, snapshot_month, source_dataset_uri, source_version, rows, date_parse_failures,
                dirty_state_nulled, dataset_uris, indices_built, datasets_published, g3_cold_ms,
                g6_cold_ms, gate, status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.nppes_analytical_runs (psycopg). Best-effort — never mask a
    good build with an audit-write failure."""
    import psycopg
    from psycopg.types.json import Jsonb

    # Prefer the pooled DSN (correct for the fleet's many concurrent workers); fall back to the
    # direct DSN if the pooler is saturated (observed: supabase session-pool EMAXCONNSESSION at
    # pool_size 15). Best-effort throughout — a failed audit write never masks a good build.
    dsns = [d for d in (os.environ.get("HQX_DB_URL_POOLED"), os.environ.get("HQX_DB_URL_DIRECT")) if d]
    if not dsns:
        print("WARN: no HQX_DB_URL_POOLED/DIRECT set; skipping ops.* state write.")
        return
    params = (
        FEED, snapshot_month, source_dataset_uri, source_version,
        rows.get("nppes_provider"), rows.get("nppes_provider_taxonomy"),
        rows.get("nppes_provider_identifier"), date_parse_failures, dirty_state_nulled,
        dataset_uris.get("nppes_provider"), dataset_uris.get("nppes_provider_taxonomy"),
        dataset_uris.get("nppes_provider_identifier"), indices_built, datasets_published,
        g3_cold_ms, g6_cold_ms, Jsonb(gate) if gate is not None else None,
        status, error, started_at, completed_at,
    )
    insert = """
        INSERT INTO ops.nppes_analytical_runs
            (feed, snapshot_month, source_dataset_uri, source_version,
             provider_rows, taxonomy_rows, identifier_rows, date_parse_failures,
             dirty_state_nulled, provider_dataset_uri, taxonomy_dataset_uri,
             identifier_dataset_uri, indices_built, datasets_published,
             g3_cold_ms, g6_cold_ms, gate, status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    last_exc = None
    for dsn in dsns:
        try:
            with psycopg.connect(dsn, connect_timeout=15) as conn, conn.cursor() as cur:
                cur.execute(OPS_DDL)
                cur.execute(insert, params)
                conn.commit()
            return
        except Exception as exc:  # noqa: BLE001 — audit must not mask the build
            last_exc = exc
            print(f"WARN: ops.* write via {dsn.rsplit('@', 1)[-1][:32]} failed: {str(exc)[:120]}")
    print(f"WARN: ops.nppes_analytical_runs write failed on all DSNs: {str(last_exc)[:160]}")


# --------------------------------------------------------------------------- #
# Modal workers
# --------------------------------------------------------------------------- #
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    """Apply the idempotent ops.nppes_analytical_runs DDL. Run once before the first build."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.nppes_analytical_runs')")
        present = cur.fetchone()[0]
    print(f"ops.nppes_analytical_runs present = {present}")
    return {"status": "success", "table": "ops.nppes_analytical_runs", "present": str(present)}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 4,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=524288,
)
def materialize(snapshot_month: str, publish: bool = True,
                trigger_callback_url: str | None = None) -> dict:
    """Build the three derived datasets from the raw month → gate locally → publish to R2 →
    gate the published layer → record ops. Re-raises on a correctness-gate failure so the Modal
    call is marked failed. Partial publish (crash after N<3 prefixes) is recorded status=partial
    and rejected by G10 on the next verify."""
    import datetime as dt

    started_at = dt.datetime.now(dt.timezone.utc)
    status, error = "error", None
    datasets_published: list[str] = []
    gate_result: dict | None = None
    b: dict = {}
    dataset_uris: dict[str, str] = {}
    try:
        b = build_all(snapshot_month)

        # Gate the LOCAL stages before any R2 write (blast-radius containment).
        print("[gate] local stages")
        local_gate = run_gate(snapshot_month, local=b["local"], raw_uri=b["raw_uri"],
                              build_metrics=b)
        if not local_gate["passed"]:
            raise RuntimeError(f"local acceptance gate failed: "
                               f"{[g for g, v in local_gate['gates'].items() if not v['pass']]}")

        if publish:
            for name in TABLE_ORDER:
                dataset_uris[name] = publish_table(name, snapshot_month, b["local"][name])
                datasets_published.append(PREFIXES[name])
            # Authoritative read-back gate against R2.
            print("[gate] published R2 layer")
            gate_result = run_gate(snapshot_month, local=None, raw_uri=b["raw_uri"])
            if not gate_result["passed"]:
                raise RuntimeError(f"published acceptance gate failed: "
                                   f"{[g for g, v in gate_result['gates'].items() if not v['pass']]}")
            status = "success"
        else:
            gate_result = local_gate
            dataset_uris = {n: _month_uri(PREFIXES[n], snapshot_month) for n in TABLE_ORDER}
            status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "partial" if datasets_published and len(datasets_published) < len(TABLE_ORDER) else "error"
        print(f"[materialize] {status.upper()}: {error}")
    finally:
        import shutil
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(
            snapshot_month=snapshot_month, source_dataset_uri=b.get("raw_uri"),
            source_version=b.get("source_version"), rows=b.get("rows", {}),
            date_parse_failures=b.get("date_parse_failures"),
            dirty_state_nulled=b.get("dirty_state_nulled"),
            dataset_uris=dataset_uris,
            indices_built=";".join(f"{n}:{','.join(b.get('indices', {}).get(n, []))}" for n in TABLE_ORDER),
            datasets_published=",".join(datasets_published),
            g3_cold_ms=(gate_result or {}).get("g3_cold_ms"),
            g6_cold_ms=(gate_result or {}).get("g6_cold_ms"),
            gate=(gate_result or {}).get("gates"),
            status=status, error=error, started_at=started_at, completed_at=completed_at,
        )
        if trigger_callback_url:
            _post_callback(trigger_callback_url, {"status": status, "feed": FEED,
                                                  "snapshot_month": snapshot_month,
                                                  "rows": b.get("rows", {})})
        shutil.rmtree(SCRATCH_DIR, ignore_errors=True)

    if status != "success":
        raise RuntimeError(f"nppes analytical build {status} for snapshot={snapshot_month}: {error}")
    return {"feed": FEED, "snapshot_month": snapshot_month, "rows": b.get("rows"),
            "dataset_uris": dataset_uris, "indices": b.get("indices"),
            "gate_passed": (gate_result or {}).get("passed"), "status": status}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 30)
def verify_snapshot(snapshot_month: str) -> dict:
    """Read-back proof: run the §8 gate against the published R2 layer. Authoritative success
    check — reads what actually landed, independent of the build's return value."""
    g = run_gate(snapshot_month, local=None)
    out = {"snapshot_month": snapshot_month, "passed": g["passed"],
           "g3_cold_ms": g["g3_cold_ms"], "g6_cold_ms": g["g6_cold_ms"],
           "gates": {k: {"pass": v["pass"], **{kk: vv for kk, vv in v.items()
                                               if kk not in ("pass", "desc")}}
                     for k, v in g["gates"].items()}}
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 10) -> list:
    """Read the most recent ops.nppes_analytical_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, snapshot_month, provider_rows, taxonomy_rows, identifier_rows, "
            "date_parse_failures, datasets_published, g3_cold_ms, g6_cold_ms, status, error, "
            "started_at, completed_at FROM ops.nppes_analytical_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the terminal payload to the Trigger waitpoint url (flat JSON, no envelope)."""
    if not url:
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


# --------------------------------------------------------------------------- #
# Local entrypoints (manual ops). ops.* write still fires.
# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def init_state() -> None:
    """Create ops.nppes_analytical_runs (idempotent)."""
    import json

    print(json.dumps(apply_state_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def materialize_month(snapshot_month: str, publish: bool = True) -> None:
    """Build + gate + publish a month's analytical layer (no Trigger callback)."""
    import json

    print(json.dumps(materialize.remote(snapshot_month=snapshot_month, publish=publish,
                                        trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def verify(snapshot_month: str) -> None:
    """Run the §8 gate against the published month."""
    import json

    print(json.dumps(verify_snapshot.remote(snapshot_month), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 10) -> None:
    """Print the most recent ops.nppes_analytical_runs rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
