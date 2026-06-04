"""Compute worker — SAM.gov Entity Master (clean registrant universe).

The ``sam-gov-entity-master-pipelines`` Modal app. Promotes the raw, stacked-snapshot
``entity_registrations`` (NAICS trapped in the positional ``pipe_fields`` array) into a
clean, deduped, indexed entity master: one row per ACTIVE UEI from the latest v2
monthly snapshot, with NAICS/PSC lifted out of ``pipe_fields`` into real columns.

Source-of-truth input (read-only; never mutated):
  s3://data-sink/active/entity_registrations/   (Lance, 19.3M rows, stacked monthly
  snapshots). uei/cage_code/legal_business_name/registration_status/dates are typed
  columns; everything past dba_name — including NAICS — is retained losslessly in the
  positional ``pipe_fields`` array (the ingest deferred it; see entity_registrations_bulk).

v2 142-wide ``pipe_fields`` layout (1-indexed, verified against live records):
  [33] primary NAICS    [35] NAICS list (each code + Y/N flag, ~-delimited)
  [37] PSC list         [18]/[19]/[20] physical city/state/zip5    [32] business types

Grain & keys:
  1 row per active (``registration_status='A'``) UEI, scoped to the latest v2
  ``extract_label`` (the current universe — every active entity is in it), defensively
  deduped latest-per-uei by (last_update_date, registration_date). uei-keyed → joins the
  SAM × USAspending crosswalk and contractor_award_summary; NAICS-indexed for cohorts.

Data plane (clean-room — DuckDB does 100% of the transform):
  Lance(entity_registrations, latest active v2) → DuckDB project + positional NAICS/PSC
  lift → Arrow → lance.write_dataset(R2 active, v2.1, overwrite) → BTREE(uei, primary_naics)
  on the R2 dataset. LANCE_BYPASS_SPILLING=true keeps the high-cardinality uei BTREE
  in-memory (lance#2650).

    modal run    pipelines/sam_gov/sam_entity_master.py            # build
    modal run    pipelines/sam_gov/sam_entity_master.py --dry-run  # plan + counts, no write

NOTE: control-plane (Universal Dispatcher registration + Trigger durable callback) is a
follow-up; the ops.sam_entity_master_runs ledger row is written for observability.
"""
from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
SAM_SRC_URI = "s3://data-sink/active/entity_registrations/"
DATASET_URI = os.environ.get("SAM_ENTITY_MASTER_LANCE_URI",
                             "s3://data-sink/active/sam_entity_master/")
FEED = "sam_entity_master"

# Net-new dataset → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576

# uei = entity spine key (equality + prefix point-lookup); primary_naics =
# cohort-selection code (equality + sector-prefix range). Both BTREE so industry
# cohorts over the full registrant universe are index lookups, not ~780k full scans.
BTREE_INDEXES = ["uei", "primary_naics"]

# pipe_fields is wide; spill is mandatory for the uei BTREE sort.
DUCKDB_MEMORY_LIMIT = "24GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

# v2 142-wide pipe_fields positions (1-indexed), verified against live records.
PRIMARY_NAICS_POS = 33
NAICS_LIST_POS = 35
PSC_LIST_POS = 37
BUSINESS_TYPES_POS = 32
CITY_POS, STATE_POS, ZIP5_POS = 18, 19, 20

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sam_entity_master_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    dataset_uri   text        NOT NULL,
    sam_label     text,
    rows_written  bigint,
    distinct_uei  bigint,
    naics_nonnull bigint,
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sam_entity_master_runs_feed_idx        ON ops.sam_entity_master_runs (feed);
CREATE INDEX IF NOT EXISTS sam_entity_master_runs_recorded_at_idx ON ops.sam_entity_master_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("sam-gov-entity-master-pipelines", image=image)


# --------------------------------------------------------------------------- #
# R2 / S3
# --------------------------------------------------------------------------- #
def _r2_storage_options() -> dict[str, str]:
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


# --------------------------------------------------------------------------- #
# DuckDB transform — pure SQL builder (importable without modal/auth)
# --------------------------------------------------------------------------- #
def build_entity_master_sql() -> str:
    """Project + dedup the `reg` relation into the entity master.

    Reads a `reg` relation PRE-FILTERED (by the scanner) to format_family='v2', the
    latest extract_label, and registration_status='A'. Lifts NAICS/PSC out of the
    1-indexed `pipe_fields` array into real columns; QUALIFY keeps 1 row/uei (latest
    by update/registration date). The NAICS list strips each code's trailing Y/N flag.
    """
    def naics_array(pos: int) -> str:   # strip each code's trailing Y/N flag → digits only
        return (f"list_filter(list_transform(string_split(coalesce(pipe_fields[{pos}], ''), '~'),"
                f" x -> regexp_extract(trim(x), '^[0-9]+')), c -> c <> '')")

    def psc_array(pos: int) -> str:     # PSC codes are alphanumeric (e.g. S203), no flag → trim only
        return (f"list_filter(list_transform(string_split(coalesce(pipe_fields[{pos}], ''), '~'),"
                f" x -> trim(x)), c -> c <> '')")

    return f"""
SELECT
    nullif(trim(uei), '')                          AS uei,
    nullif(trim(legal_business_name), '')          AS legal_business_name,
    nullif(trim(cage_code), '')                    AS cage_code,
    registration_status,
    nullif(trim(purpose_of_registration), '')      AS purpose_of_registration,
    registration_date, expiration_date, last_update_date, activation_date,
    nullif(trim(pipe_fields[{PRIMARY_NAICS_POS}]), '')   AS primary_naics,
    {naics_array(NAICS_LIST_POS)}                        AS naics_codes,
    {psc_array(PSC_LIST_POS)}                            AS psc_codes,
    nullif(trim(pipe_fields[{BUSINESS_TYPES_POS}]), '')  AS business_types,
    nullif(trim(pipe_fields[{CITY_POS}]), '')            AS physical_city,
    nullif(trim(pipe_fields[{STATE_POS}]), '')           AS physical_state,
    nullif(trim(pipe_fields[{ZIP5_POS}]), '')            AS physical_zip5,
    extract_label                                        AS sam_extract_label
FROM reg
WHERE nullif(trim(uei), '') IS NOT NULL
QUALIFY row_number() OVER (
    PARTITION BY nullif(trim(uei), '')
    ORDER BY last_update_date DESC NULLS LAST, registration_date DESC NULLS LAST
) = 1
"""


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _materialize(con):
    """Register the latest-active-v2 slice of entity_registrations, run the
    transform, return (arrow_table, metrics, sam_label). One build scan."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SAM_SRC_URI, storage_options=so)

    # Latest snapshot provenance — a cheap one-column scan (the build reader below is a
    # one-shot stream consumed exactly once).
    con.register("lbl", ds.scanner(
        columns=["extract_label", "format_family"], filter="format_family = 'v2'"
    ).to_reader())
    sam_label = con.execute("SELECT max(extract_label) FROM lbl").fetchone()[0]
    con.unregister("lbl")

    con.register("reg", ds.scanner(
        columns=["uei", "legal_business_name", "cage_code", "registration_status",
                 "purpose_of_registration", "registration_date", "expiration_date",
                 "last_update_date", "activation_date", "pipe_fields", "extract_label"],
        filter=(f"format_family = 'v2' AND extract_label = '{sam_label}' "
                f"AND registration_status = 'A'"),
    ).to_reader())
    con.execute(f"CREATE TEMP TABLE em AS {build_entity_master_sql()}")
    con.unregister("reg")

    rows, d_uei, nn = con.execute(
        "SELECT count(*), count(DISTINCT uei), count(primary_naics) FROM em"
    ).fetchone()
    table = con.sql("SELECT * FROM em").to_arrow_table()
    return table, {"rows": int(rows), "distinct_uei": int(d_uei), "naics_nonnull": int(nn)}, sam_label


# --------------------------------------------------------------------------- #
# ops ledger
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, sam_label, metrics, status, error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.sam_entity_master_runs
                    (feed, dataset_uri, sam_label, rows_written, distinct_uei,
                     naics_nonnull, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, sam_label, metrics.get("rows"),
                 metrics.get("distinct_uei"), metrics.get("naics_nonnull"),
                 status, error, started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops ledger write failed: {exc}")


# --------------------------------------------------------------------------- #
# Modal build
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
        modal.Secret.from_dict({"LANCE_BYPASS_SPILLING": "true"}),
    ],
    timeout=60 * 30,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=64 * 1024,
)
def build_sam_entity_master(dry_run: bool = False) -> dict:
    """Phase — materialize the SAM entity master + build BTREE indexes on R2."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    status, error, sam_label, metrics = "error", None, None, {}
    try:
        con = _new_con()
        table, metrics, sam_label = _materialize(con)
        print(f"materialized {metrics} (label={sam_label})")
        if metrics["rows"] < 500_000:
            raise RuntimeError(f"row floor breached: {metrics['rows']} < 500000")

        if dry_run:
            status = "dry_run"
            return {"feed": FEED, "label": sam_label, "dry_run": True, **metrics}

        so = _r2_storage_options()
        lance.write_dataset(table, DATASET_URI, storage_options=so, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE)
        ds = lance.dataset(DATASET_URI, storage_options=so)
        present = set(ds.schema.names)
        for col in BTREE_INDEXES:
            if col in present:
                ds.create_scalar_index(col, index_type="BTREE")
                print(f"  BTREE ✓ {col}")
        status = "success"
        return {"feed": FEED, "dataset": DATASET_URI, "label": sam_label, **metrics}
    except Exception as exc:  # noqa: BLE001
        error = str(exc)[:500]
        raise
    finally:
        _record_run(sam_label=sam_label, metrics=metrics, status=status, error=error,
                    started_at=started_at, completed_at=dt.datetime.now(dt.timezone.utc))


@app.local_entrypoint()
def build(dry_run: bool = False):
    print(build_sam_entity_master.remote(dry_run=dry_run))
