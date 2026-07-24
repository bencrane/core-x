"""Compute worker — California Secretary of State bulk business-records ingest.

Part of the ``ca-sos-businesses`` Modal app. Endpoint-less functions, spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — DuckDB does 100% of the transform,
Lance is written straight to R2.

Topology (proven by the diagnostic probe): the FOIA payload is a SINGLE ZIP holding
THREE normalized relational tables keyed on ENTITY_NUM — NOT a Corp/LLC/LP split.
Corp/LLC/LP is the ``entity_type`` column inside the master. Three distinct Lance
datasets, one per member, joined on ``entity_num``:

    Filings.csv    (9.39M rows, 36 cols, ~1/entity)  -> s3://data-sink/active/ca_sos_entities/
    Agents.csv     (8.56M rows, 14 cols, ~1/entity)  -> s3://data-sink/active/ca_sos_agents/
    Principals.csv (18.67M rows, 14 cols, N/entity)  -> s3://data-sink/active/ca_sos_principals/

Source dialect (uniform across members): field delimiter ``*|*`` (multi-char),
record terminator CRLF, NO quoting (quote=''/escape=''), UTF-8 (no transcode).
ENTITY_NUM is alphanumeric with real leading zeros (7/8/12-char, 'B'-prefixed) →
projected as VARCHAR, never cast. Three date formats in Filings each get their own
parser. Ragged rows are quarantined via store_rejects (never silently dropped).

Two-phase ingest (mirrors the PPP precedent):
  Phase 1 — explode_zip (Python I/O only):
      R2 landing ZIP → /tmp → extract each member → recompress .csv.zst
        → boto3 upload → s3://data-sink/landing/ca_sos/members/<member>.csv.zst
  Phase 2 — ingest_member (DuckDB does 100% of the transform; runs 3-way parallel):
      landed .csv.zst → /tmp (zstd decompress) → DuckDB read_csv(delim='*|*',
        quote='', store_rejects=true) → project/cast → con.execute(...).to_arrow_table()
        → lance.write_dataset(active/ca_sos_<member>/, v2.1, overwrite)
        → BTREE + BITMAP scalar indexes (LANCE_BYPASS_SPILLING baked into the image)

Control plane (Trigger v4 durable callback): each function accepts
``trigger_callback_url`` and, on terminal state, (1) writes a run row to
``ops.ca_sos_runs`` via psycopg and (2) POSTs a FLAT JSON body to that url. No
``{"data": ...}`` envelope.

    modal deploy pipelines/ca_sos/entities_bulk.py                          # ALWAYS first — run_all spawns DEPLOYED code
    modal run    pipelines/ca_sos/entities_bulk.py::init_state              # create ops.ca_sos_runs
    modal run    pipelines/ca_sos/entities_bulk.py::run_all                 # spawn-fires run_all_remote on the deployed
                                                                            #   app; prints fc-... and returns in seconds
    modal run    pipelines/ca_sos/entities_bulk.py::explode                 # Phase 1 only
    modal run    pipelines/ca_sos/entities_bulk.py::ingest --member agents  # Phase 2, one member
    modal run    pipelines/ca_sos/entities_bulk.py::reindex --member entities

run_all no longer blocks or prints row counts — the fc-id is the durable handle; the
record is app logs + ops.ca_sos_runs (terminal = 1 phase='explode' + 3 phase='ingest'
rows with status='success' for the as_of). Follow:
    modal app logs ca-sos-businesses
    python3 -c "import modal; print(modal.FunctionCall.from_id('fc-...').get(timeout=0))"
Sequencing lives INSIDE run_all_remote: explode_zip.remote() gates the fan-out (no
ingest spawns after a failed Phase 1); a failed member re-raises in the coordinator
while sibling ingests finish on the deployed app — per-member truth is the ledger.
Recovery: ::explode re-runs Phase 1 alone; ::ingest --member <m> re-runs one member
(idempotent overwrite).
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
# Source ZIP landing zone (operator-uploaded) and per-member transcoded landing.
LANDING_PREFIX = "landing/ca_sos/"
LANDING_MEMBERS_PREFIX = "landing/ca_sos/members/"

SCRATCH_DIR = "/tmp"
AS_OF_DEFAULT = "2026-05-31"

# member registry: logical name -> ZIP member, Lance dataset URI (env-overridable),
# durable per-member landing key, provenance source_file label.
MEMBERS: dict[str, dict[str, str]] = {
    "entities": {
        "zip_member": "Filings.csv",
        "uri": os.environ.get("CA_SOS_ENTITIES_LANCE_URI", "s3://data-sink/active/ca_sos_entities/"),
    },
    "agents": {
        "zip_member": "Agents.csv",
        "uri": os.environ.get("CA_SOS_AGENTS_LANCE_URI", "s3://data-sink/active/ca_sos_agents/"),
    },
    "principals": {
        "zip_member": "Principals.csv",
        "uri": os.environ.get("CA_SOS_PRINCIPALS_LANCE_URI", "s3://data-sink/active/ca_sos_principals/"),
    },
}


def _landing_member_key(member: str) -> str:
    return f"{LANDING_MEMBERS_PREFIX}{member}.csv.zst"


# Lance fragment sizing (directive constraints).
#   max_rows_per_file = 1048576 — exact.
#   max_bytes_per_file: the directive wrote `90 * 10243` and annotated it "(90 GiB)".
#   The only reading that equals 90 GiB is `90 * 1024**3` (= 96,636,764,160), which is
#   also Lance's documented default and the existing SBA-worker constant. A literal
#   `90 * 10243` (~900 KB) would shatter each member (up to 18.67M rows) into hundreds
#   of fragments — not the intent. Honor the stated "90 GiB". With max_rows_per_file
#   binding first, rows-per-file caps fragment count (≤ ~18 for principals).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new datasets → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan, built post-write inside ingest_member. BTREE for high-cardinality
# load-bearing resolution keys (equality + range); BITMAP for low-cardinality
# categoricals filtered frequently. NOTE: the resolution key is ``entity_name_clean``
# (the quote/apostrophe-stripped variant) per the approved name-normalization decision —
# raw ``entity_name`` carries export-quoting artifacts that make equality unreliable.
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "entities": {
        "btree": ["entity_num", "entity_name_clean", "last_si_file_number"],
        "bitmap": ["entity_type", "entity_status", "jurisdiction", "filing_type",
                   "standing_sos", "standing_ftb", "principal_state", "mailing_state"],
    },
    "agents": {
        "btree": ["entity_num", "entity_name_clean"],
        "bitmap": ["agent_type", "physical_state"],
    },
    "principals": {
        "btree": ["entity_num", "entity_name_clean", "last_name"],
        "bitmap": ["position_type", "state"],
    },
}

# ── DuckDB transforms (100% of the shaping). Parameterized: ? placeholders bind, in
# textual order, [csv_path, source_file, as_of] — keeping the controlled /tmp path and
# literals out of string interpolation (mirrors the SAM reference). read_csv options:
#   delim='*|*'      multi-char CA SoS field separator (verified)
#   quote='' escape=''  NO quoting — values carry literal " and '; *|* never collides
#   all_varchar=true    every cast is explicit + defensive
#   null_padding=true   pad short rows with NULL
#   store_rejects=true  quarantine ragged rows into reject_errors (do NOT silently drop)
# entity_name_clean strips wrapping {", ', space}; internal apostrophes (O'BRIEN) survive.
_READ = (
    "read_csv(?, all_varchar=true, header=true, delim='*|*', quote='', escape='', "
    "strict_mode=false, null_padding=true, max_line_size=40000000, store_rejects=true)"
)

TRANSFORMS: dict[str, str] = {
    "entities": (
        "WITH raw AS (SELECT * FROM " + _READ + ")\n"
        "SELECT\n"
        "    nullif(trim(\"ENTITY_NAME\"), '') AS entity_name,\n"
        "    nullif(trim(BOTH '\"'' ' FROM trim(\"ENTITY_NAME\")), '') AS entity_name_clean,\n"
        "    nullif(trim(\"ENTITY_NUM\"), '') AS entity_num,\n"
        "    TRY_STRPTIME(nullif(trim(\"INITIAL_FILING_DATE\"), ''), '%m/%d/%Y')::DATE AS initial_filing_date,\n"
        "    nullif(trim(\"JURISDICTION\"), '') AS jurisdiction,\n"
        "    nullif(trim(\"ENTITY_STATUS\"), '') AS entity_status,\n"
        "    nullif(trim(\"STANDING_SOS\"), '') AS standing_sos,\n"
        "    nullif(trim(\"ENTITY_TYPE\"), '') AS entity_type,\n"
        "    nullif(trim(\"FILING_TYPE\"), '') AS filing_type,\n"
        "    nullif(trim(\"FOREIGN_NAME\"), '') AS foreign_name,\n"
        "    nullif(trim(BOTH '\"'' ' FROM trim(\"FOREIGN_NAME\")), '') AS foreign_name_clean,\n"
        "    nullif(trim(\"STANDING_FTB\"), '') AS standing_ftb,\n"
        "    nullif(trim(\"STANDING_VCFCF\"), '') AS standing_vcfcf,\n"
        "    nullif(trim(\"STANDING_AGENT\"), '') AS standing_agent,\n"
        "    TRY_CAST(nullif(trim(\"SUSPENSION_DATE\"), '') AS DATE) AS suspension_date,\n"
        "    nullif(trim(\"LAST_SI_FILE_NUMBER\"), '') AS last_si_file_number,\n"
        "    TRY_STRPTIME(nullif(regexp_replace(trim(\"LAST_SI_FILE_DATE\"), ' +', ' ', 'g'), ''), '%b %-d %Y %I:%M%p')::DATE AS last_si_file_date,\n"
        "    nullif(trim(\"PRINCIPAL_ADDRESS\"), '') AS principal_address,\n"
        "    nullif(trim(\"PRINCIPAL_ADDRESS2\"), '') AS principal_address2,\n"
        "    nullif(trim(\"PRINCIPAL_CITY\"), '') AS principal_city,\n"
        "    nullif(trim(\"PRINCIPAL_STATE\"), '') AS principal_state,\n"
        "    nullif(trim(\"PRINCIPAL_COUNTRY\"), '') AS principal_country,\n"
        "    nullif(trim(\"PRINCIPAL_POSTAL_CODE\"), '') AS principal_postal_code,\n"
        "    nullif(trim(\"MAILING_ADDRESS\"), '') AS mailing_address,\n"
        "    nullif(trim(\"MAILING_ADDRESS2\"), '') AS mailing_address2,\n"
        "    nullif(trim(\"MAILING_ADDRESS3\"), '') AS mailing_address3,\n"
        "    nullif(trim(\"MAILING_CITY\"), '') AS mailing_city,\n"
        "    nullif(trim(\"MAILING_STATE\"), '') AS mailing_state,\n"
        "    nullif(trim(\"MAILING_COUNTRY\"), '') AS mailing_country,\n"
        "    nullif(trim(\"MAILING_POSTAL_CODE\"), '') AS mailing_postal_code,\n"
        "    nullif(trim(\"PRINCIPAL_ADDRESS_IN_CA\"), '') AS principal_address_in_ca,\n"
        "    nullif(trim(\"PRINCIPAL_ADDRESS2_IN_CA\"), '') AS principal_address2_in_ca,\n"
        "    nullif(trim(\"PRINCIPAL_CITY_IN_CA\"), '') AS principal_city_in_ca,\n"
        "    nullif(trim(\"PRINCIPAL_STATE_IN_CA\"), '') AS principal_state_in_ca,\n"
        "    nullif(trim(\"PRINCIPAL_COUNTRY_IN_CA\"), '') AS principal_country_in_ca,\n"
        "    nullif(trim(\"PRINCIPAL_POSTAL_CODE_IN_CA\"), '') AS principal_postal_code_in_ca,\n"
        "    nullif(trim(\"LLC_MANAGEMENT_STRUCTURE\"), '') AS llc_management_structure,\n"
        "    nullif(trim(\"TYPE_OF_BUSINESS\"), '') AS type_of_business,\n"
        "    ? AS source_file,\n"
        "    CAST(? AS DATE) AS snapshot_date,\n"
        "    now() AS ingested_at\n"
        "FROM raw\n"
    ),
    "agents": (
        "WITH raw AS (SELECT * FROM " + _READ + ")\n"
        "SELECT\n"
        "    nullif(trim(\"ENTITY_NAME\"), '') AS entity_name,\n"
        "    nullif(trim(BOTH '\"'' ' FROM trim(\"ENTITY_NAME\")), '') AS entity_name_clean,\n"
        "    nullif(trim(\"ENTITY_NUM\"), '') AS entity_num,\n"
        "    nullif(trim(\"ORG_NAME\"), '') AS org_name,\n"
        "    nullif(trim(\"FIRST_NAME\"), '') AS first_name,\n"
        "    nullif(trim(\"MIDDLE_NAME\"), '') AS middle_name,\n"
        "    nullif(trim(\"LAST_NAME\"), '') AS last_name,\n"
        "    nullif(trim(\"PHYSICAL_ADDRESS1\"), '') AS physical_address1,\n"
        "    nullif(trim(\"PHYSICAL_ADDRESS2\"), '') AS physical_address2,\n"
        "    nullif(trim(\"PHYSICAL_ADDRESS3\"), '') AS physical_address3,\n"
        "    nullif(trim(\"PHYSICAL_CITY\"), '') AS physical_city,\n"
        "    nullif(trim(\"PHYSICAL_STATE\"), '') AS physical_state,\n"
        "    nullif(trim(\"PHYSICAL_COUNTRY\"), '') AS physical_country,\n"
        "    nullif(trim(\"PHYSICAL_POSTAL_CODE\"), '') AS physical_postal_code,\n"
        "    nullif(trim(\"AGENT_TYPE\"), '') AS agent_type,\n"
        "    ? AS source_file,\n"
        "    CAST(? AS DATE) AS snapshot_date,\n"
        "    now() AS ingested_at\n"
        "FROM raw\n"
    ),
    "principals": (
        "WITH raw AS (SELECT * FROM " + _READ + ")\n"
        "SELECT\n"
        "    nullif(trim(\"ENTITY_NAME\"), '') AS entity_name,\n"
        "    nullif(trim(BOTH '\"'' ' FROM trim(\"ENTITY_NAME\")), '') AS entity_name_clean,\n"
        "    nullif(trim(\"ENTITY_NUM\"), '') AS entity_num,\n"
        "    nullif(trim(\"ORG_NAME\"), '') AS org_name,\n"
        "    nullif(trim(\"FIRST_NAME\"), '') AS first_name,\n"
        "    nullif(trim(\"MIDDLE_NAME\"), '') AS middle_name,\n"
        "    nullif(trim(\"LAST_NAME\"), '') AS last_name,\n"
        "    nullif(trim(\"POSITION_TYPE\"), '') AS position_type,\n"
        "    nullif(trim(\"ADDRESS1\"), '') AS address1,\n"
        "    nullif(trim(\"ADDRESS2\"), '') AS address2,\n"
        "    nullif(trim(\"ADDRESS3\"), '') AS address3,\n"
        "    nullif(trim(\"CITY\"), '') AS city,\n"
        "    nullif(trim(\"STATE\"), '') AS state,\n"
        "    nullif(trim(\"COUNTRY\"), '') AS country,\n"
        "    nullif(trim(\"POSTAL_CODE\"), '') AS postal_code,\n"
        "    ? AS source_file,\n"
        "    CAST(? AS DATE) AS snapshot_date,\n"
        "    now() AS ingested_at\n"
        "FROM raw\n"
    ),
}

# Mirrored verbatim by pipelines/ca_sos/ops_ca_sos_runs.sql. Applied by init_state.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.ca_sos_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase          text        NOT NULL,
    member         text,
    dataset_uri    text,
    as_of          date,
    source_zip     text,
    landing_key    text,
    rows_processed bigint,
    rejected_rows  bigint,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ca_sos_runs_member_idx      ON ops.ca_sos_runs (member);
CREATE INDEX IF NOT EXISTS ca_sos_runs_phase_idx       ON ops.ca_sos_runs (phase);
CREATE INDEX IF NOT EXISTS ca_sos_runs_status_idx      ON ops.ca_sos_runs (status);
CREATE INDEX IF NOT EXISTS ca_sos_runs_recorded_at_idx ON ops.ca_sos_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",       # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",           # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",          # R2 landing read/write
    "zstandard>=0.22",      # .csv.zst per-member landing compression
    "psycopg[binary]>=3.2",  # terminal state write to ops.*
).env(
    # BTREE scalar-index builds sort the column. Lance's spill-to-disk sorter uses a
    # bounded DataFusion pool that OOMs on high-cardinality string columns at this scale
    # (entity_num/entity_name_clean over 9–18M rows). Force the in-memory sort path
    # (uses the container's RAM) so every index builds. (lance-format/lance#2650)
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("ca-sos-businesses", image=image)


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret.
    AWS-style creds + explicit endpoint and region ("auto" for R2)."""
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
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required``:
    botocore's default flexible-checksum validation does not match R2's semantics
    and otherwise raises on get/put."""
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


def _discover_zip_key(s3) -> str:
    """Find the single source ZIP under the landing prefix (filename carries a request
    hash, not a stable name). Picks the largest .zip if more than one is present."""
    paginator = s3.get_paginator("list_objects_v2")
    zips: list[tuple[str, int]] = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=LANDING_PREFIX):
        for o in page.get("Contents", []):
            if o["Key"].lower().endswith(".zip"):
                zips.append((o["Key"], o["Size"]))
    if not zips:
        raise RuntimeError(f"No .zip found under s3://{BUCKET}/{LANDING_PREFIX}")
    zips.sort(key=lambda t: t[1], reverse=True)
    return zips[0][0]


def _build_indexes(member: str, so: dict) -> list[str]:
    """Build the BTREE + BITMAP scalar indexes for one dataset. create_scalar_index
    defaults to replace=True → idempotent. An index miss must not fail the load."""
    import lance

    ds = lance.dataset(MEMBERS[member]["uri"], storage_options=so)
    built: list[str] = []
    for col in INDEX_PLAN[member]["btree"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in INDEX_PLAN[member]["bitmap"]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


def _record_run(phase, member, dataset_uri, as_of, source_zip, landing_key, rows,
                rejected, status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.ca_sos_runs (psycopg). Best-effort: never let an
    audit-write failure crash an otherwise-good ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.ca_sos_runs
                    (phase, member, dataset_uri, as_of, source_zip, landing_key,
                     rows_processed, rejected_rows, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (phase, member, dataset_uri, as_of, source_zip, landing_key,
                 rows, rejected, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint URL. FLAT JSON body — no
    ``{"data": ...}`` envelope. A few retries for delivery reliability."""
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


@app.function(
    secrets=[modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 5,
)
def apply_state_schema() -> dict:
    """Apply the idempotent ops.ca_sos_runs DDL. Run once before the first ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.ca_sos_runs schema.")
    return {"status": "success", "table": "ops.ca_sos_runs"}


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 45,
    memory=8192,
    cpu=4.0,
)
def explode_zip(
    as_of: str = AS_OF_DEFAULT,
    zip_key: str | None = None,
    trigger_callback_url: str | None = None,
) -> dict:
    """Phase 1 — Python I/O ONLY. Download the source ZIP from R2 landing, extract
    each member, recompress as .csv.zst, and re-land per-member to R2. No parsing."""
    import datetime as dt
    import os.path
    import zipfile

    import zstandard as zstd

    started_at = dt.datetime.now(dt.timezone.utc)
    status, error = "error", None
    landed: dict[str, str] = {}
    src_zip = zip_key
    try:
        s3 = _s3_client()
        if not src_zip:
            src_zip = _discover_zip_key(s3)
        local_zip = os.path.join(SCRATCH_DIR, "ca_sos_source.zip")
        print(f"Downloading s3://{BUCKET}/{src_zip} -> {local_zip}")
        s3.download_file(BUCKET, src_zip, local_zip)

        zf = zipfile.ZipFile(local_zip)
        names = {zi.filename: zi for zi in zf.infolist()}
        for member, meta in MEMBERS.items():
            zip_member = meta["zip_member"]
            if zip_member not in names:
                raise RuntimeError(f"ZIP missing expected member {zip_member!r} "
                                   f"(have: {sorted(names)})")
            raw_csv = os.path.join(SCRATCH_DIR, zip_member)
            zpath = raw_csv + ".zst"
            # stream-extract member -> /tmp
            with zf.open(zip_member) as src, open(raw_csv, "wb") as dst:
                while True:
                    chunk = src.read(16 << 20)
                    if not chunk:
                        break
                    dst.write(chunk)
            # stream-compress -> .csv.zst (level 10, matches PPP/SBA landing)
            with open(raw_csv, "rb") as fi, open(zpath, "wb") as fo:
                zstd.ZstdCompressor(level=10).copy_stream(fi, fo)
            key = _landing_member_key(member)
            s3.upload_file(zpath, BUCKET, key)
            landed[member] = key
            size = os.path.getsize(zpath)
            print(f"landed {member}: {zip_member} -> s3://{BUCKET}/{key} ({size/1024**2:.1f} MiB zst)")
            for p in (raw_csv, zpath):
                try:
                    os.remove(p)
                except OSError:
                    pass
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("explode", None, None, as_of, src_zip, None, None, None,
                    status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "phase": "explode", "as_of": as_of,
             "source_zip": src_zip, "members": landed},
        )

    if status != "success":
        raise RuntimeError(f"ca_sos explode failed: {error}")
    return {"status": status, "phase": "explode", "as_of": as_of,
            "source_zip": src_zip, "members": landed}


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 60,
    memory=32768,
    cpu=8.0,
)
def ingest_member(
    member: str,
    as_of: str = AS_OF_DEFAULT,
    trigger_callback_url: str | None = None,
) -> dict:
    """Phase 2 — download the landed .csv.zst → DuckDB project/cast → Arrow → Lance
    overwrite → BTREE/BITMAP indexes, then record ops.* state and wake Trigger.
    Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import os.path

    import duckdb
    import lance
    import zstandard as zstd

    member = member.strip().lower()
    if member not in MEMBERS:
        raise ValueError(f"member must be one of {sorted(MEMBERS)}, got {member!r}")

    dataset_uri = MEMBERS[member]["uri"]
    source_file = MEMBERS[member]["zip_member"]
    landing_key = _landing_member_key(member)
    started_at = dt.datetime.now(dt.timezone.utc)
    rows, rejected = 0, 0
    status, error = "error", None

    try:
        so = _r2_storage_options()
        s3 = _s3_client()

        zpath = os.path.join(SCRATCH_DIR, f"{member}.csv.zst")
        csv_path = os.path.join(SCRATCH_DIR, f"{member}.csv")
        print(f"Downloading s3://{BUCKET}/{landing_key} -> {zpath}")
        s3.download_file(BUCKET, landing_key, zpath)
        with open(zpath, "rb") as fi, open(csv_path, "wb") as fo:
            zstd.ZstdDecompressor().copy_stream(fi, fo)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET memory_limit='26GB';")
            con.execute("SET temp_directory='/tmp/duckdb_spill';")
            table = con.execute(
                TRANSFORMS[member], [csv_path, source_file, as_of]
            ).to_arrow_table()
            rows = table.num_rows
            # ragged rows quarantined by store_rejects (scalar count via Arrow)
            try:
                rj = con.execute("SELECT count(*) AS n FROM reject_errors").to_arrow_table()
                rejected = int(rj.to_pylist()[0]["n"])
            except Exception:  # noqa: BLE001 — table absent ⇒ zero rejects
                rejected = 0
        finally:
            con.close()
        print(f"{member}: parsed {rows:,} rows, {rejected:,} rejected")

        lance.write_dataset(
            table,
            dataset_uri,
            mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        del table  # free the Arrow buffer before the index sort
        print(f"{member}: wrote Lance dataset -> {dataset_uri}")
        _build_indexes(member, so)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("ingest", member, dataset_uri, as_of, None, landing_key,
                    int(rows), int(rejected), status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "phase": "ingest", "member": member,
             "rows": int(rows), "rejected_rows": int(rejected),
             "dataset_uri": dataset_uri, "as_of": as_of},
        )

    if status != "success":
        raise RuntimeError(f"ca_sos ingest failed for member={member}: {error}")
    return {"status": status, "phase": "ingest", "member": member,
            "rows_processed": int(rows), "rejected_rows": int(rejected),
            "dataset_uri": dataset_uri, "as_of": as_of}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 45,
    memory=32768,
    cpu=8.0,
)
def reindex_member(member: str) -> dict:
    """(Re)build the scalar indexes on an already-written dataset (no re-ingest)."""
    member = member.strip().lower()
    if member not in MEMBERS:
        raise ValueError(f"member must be one of {sorted(MEMBERS)}, got {member!r}")
    built = _build_indexes(member, _r2_storage_options())
    return {"member": member, "dataset_uri": MEMBERS[member]["uri"],
            "indexes": built, "index_count": len(built)}


@app.function(timeout=60 * 110)
def run_all_remote(
    as_of: str = AS_OF_DEFAULT,
    zip_key: str | None = None,
    trigger_callback_url: str | None = None,
) -> dict:
    """Thin coordinator — sequences the full run in ITS OWN container (no client
    tether). explode_zip.remote() gates Phase 1 (it re-raises on failure, so no
    ingest spawns after a failed explode); then ingest_member fans out 3-way
    parallel via .spawn(). Phase workers receive trigger_callback_url=None and
    write their own terminal ops.ca_sos_runs rows; this function POSTs the flat
    aggregate on terminal state. .get() re-raises on the first failed member while
    sibling ingests run to completion on the deployed app — per-member truth is
    the ledger, not this result alone."""
    status, error = "error", None
    res_explode: dict = {}
    results: dict[str, dict] = {}
    try:
        print("=== Phase 1 — explode ===")
        res_explode = explode_zip.remote(as_of, zip_key, trigger_callback_url=None)

        print("=== Phase 2 — parallel ingest ===")
        calls = {m: ingest_member.spawn(m, as_of=as_of, trigger_callback_url=None)
                 for m in MEMBERS}
        results = {m: c.get() for m, c in calls.items()}
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        total = sum(int(r.get("rows_processed", 0)) for r in results.values())
        print("=== FINAL ROW COUNTS ===")
        for m, r in results.items():
            print(f"  {m:12s} rows={r.get('rows_processed', 0):>12,}  "
                  f"rejected={r.get('rejected_rows', 0):>6,}  -> {r.get('dataset_uri')}")
        print(f"  {'TOTAL':12s} rows={total:>12,}")
        _post_callback(
            trigger_callback_url,
            {"status": status, "phase": "run_all", "as_of": as_of,
             "explode": res_explode, "members": results,
             "total_rows": total, "error": error},
        )

    if status != "success":
        raise RuntimeError(f"ca_sos run_all failed: {error}")
    return {"status": status, "phase": "run_all", "as_of": as_of,
            "explode": res_explode, "members": results, "total_rows": total}


@app.local_entrypoint()
def init_state() -> None:
    """Create ops.ca_sos_runs (idempotent)."""
    print(apply_state_schema.remote())


@app.local_entrypoint()
def explode(as_of: str = AS_OF_DEFAULT, zip_key: str = "") -> None:
    """Phase 1 — explode the source ZIP into per-member .csv.zst landing artifacts."""
    print(explode_zip.remote(as_of, zip_key or None, trigger_callback_url=None))


@app.local_entrypoint()
def ingest(member: str, as_of: str = AS_OF_DEFAULT) -> None:
    """Phase 2 — ingest a single member into its Lance dataset."""
    print(ingest_member.remote(member, as_of=as_of, trigger_callback_url=None))


@app.local_entrypoint()
def run_all(as_of: str = AS_OF_DEFAULT, zip_key: str = "") -> None:
    """End-to-end run, durable: spawn run_all_remote on the DEPLOYED app and return.
    Sequencing (explode gate → 3-way parallel ingest) runs in the coordinator's own
    container; the printed fc-id is the durable handle. Row counts land in app logs
    and ops.ca_sos_runs, not here."""
    from pipelines._shared.launch import spawn_deployed

    spawn_deployed("ca-sos-businesses", "run_all_remote", deploy_file=__file__,
                   as_of=as_of, zip_key=zip_key or None, trigger_callback_url=None)


@app.local_entrypoint()
def reindex(member: str = "") -> None:
    """Rebuild scalar indexes on existing dataset(s) (no re-ingest). Default: all."""
    import json

    for m in ([member] if member else list(MEMBERS)):
        print(json.dumps(reindex_member.remote(m), default=str))
