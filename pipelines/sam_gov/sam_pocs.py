"""Compute worker — SAM.gov Points of Contact (human layer, Pattern-A derived).

The ``sam-gov-pocs-pipelines`` Modal app. Reshapes the POC blocks embedded in the
positional ``pipe_fields`` array of the published SAM entity registry into a
long, indexed human-contact dataset that attaches to the SAM × USAspending
crosswalk spine. Spawned only by the Universal Dispatcher; no web endpoint.

Source-of-truth input (read-only; never mutated):
  s3://data-sink/active/entity_registrations/   (Lance, 19.3M rows)
    POCs live in pipe_fields (the lossless positional array the ingest deferred),
    NOT in a separate dataset. Layout is width-determined (confirmed empirically):
      - v2            (width 142): uei@pos0, cage@pos3, 6 POC slots base pos46
      - legacy_v1     (width 120): no uei, cage@pos2, 6 POC slots base pos44
      - legacy_v1     (width 142): MIS-CLASSIFIED v2-layout — the ingest nulls the
                       uei (real uei is at pos0) and reads cage from the wrong
                       position. EXCLUDED here; recovery requires fixing
                       pipelines/sam_gov/entity_registrations_bulk.py, not a
                       work-around on top of corrupt projections.

POC slot = 11 contiguous fields: first, middle, last, title, address_line_1,
address_line_2, city, zip5, zip4, country, state. Six slots per entity:
  1 government_business (mandatory)   4 past_performance_alt
  2 government_business_alt           5 electronic_business (mandatory)
  3 past_performance                  6 electronic_business_alt
Slots 1 & 5 are near-always populated (and frequently the same individual); the
four alternates are optional. Labels follow the SAM public-extract convention +
the empirical fill pattern (mandatory pair at slots 1 & 5).

Grain & keys:
  v2     → 1 row/uei latest (QUALIFY by last_update_date, registration_date),
           uei-keyed → joins crosswalk.uei.
  legacy → 1 row/cage_code latest, uei NULL, cage-keyed → joins crosswalk.cage_code
           (the defense-tail bridge). ALL distinct legacy cages retained (max spine).
  Output = 1 row per (entity, populated POC slot). Empty slots dropped.

ZERO-ALTERATION NAME POLICY (operator mandate): human name strings are NEVER run
through nameparser or any splitting library. SAM already delivers discrete
first/middle/last positional fields — they are copied through with whitespace
hygiene only (trim / '' → NULL), structure untouched, no component dropped, no
suffix stripped. `full_name` is a lossless concat of the present parts. `name_key`
( upper(trim(full_name)) ) is an ADDED, non-authoritative lookup accelerator — the
verbatim parts remain system-of-record.

Data plane (clean-room — DuckDB does 100% of the transform):
  Lance(entity_registrations) → DuckDB positional unpivot → Arrow →
  lance.write_dataset(R2 active, v2.1, overwrite) → BTREE(uei, cage_code, name_key,
  last_name) + BITMAP(poc_type, source_family) on the R2 dataset.
  LANCE_BYPASS_SPILLING=true forces the in-memory sort so the high-cardinality
  string BTREEs (name_key, uei) build without OOM (lance#2650).

Control plane (Trigger v4 durable callback): on terminal state writes the run row
to ops.sam_pocs_runs and POSTs the flat callback to wake the suspended Trigger run.

    modal run    pipelines/sam_gov/sam_pocs.py::init_ops    # create ops table
    modal deploy pipelines/sam_gov/sam_pocs.py              # dispatcher-resolvable
    modal run    pipelines/sam_gov/sam_pocs.py              # build (local entrypoint)
    modal run    pipelines/sam_gov/sam_pocs.py --dry-run    # plan + counts, no write
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
SAM_SRC_URI = "s3://data-sink/active/entity_registrations/"
DATASET_URI = os.environ.get("SAM_POCS_LANCE_URI", "s3://data-sink/active/sam_pocs/")
FEED = "sam_pocs"

# Net-new dataset → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# Resolution keys → BTREE (equality + prefix point-lookup). uei = v2 spine key;
# cage_code = legacy defense-tail bridge; name_key = reverse human-name lookup;
# last_name = surname reverse lookup (free from SAM's pre-split names). Low-card
# categoricals → BITMAP.
BTREE_INDEXES = ["uei", "cage_code", "name_key", "last_name"]
BITMAP_INDEXES = ["poc_type", "source_family"]

# Validated compute envelope. The pipe_fields scan is wide; spill is mandatory.
DUCKDB_MEMORY_LIMIT = "24GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sam_pocs_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,
    dataset_uri      text        NOT NULL,
    sam_label        text,
    rows_written     bigint,
    distinct_uei     bigint,
    distinct_cage    bigint,
    poc_rows_v2      bigint,
    poc_rows_legacy  bigint,
    status           text        NOT NULL,
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sam_pocs_runs_feed_idx        ON ops.sam_pocs_runs (feed);
CREATE INDEX IF NOT EXISTS sam_pocs_runs_status_idx      ON ops.sam_pocs_runs (status);
CREATE INDEX IF NOT EXISTS sam_pocs_runs_recorded_at_idx ON ops.sam_pocs_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("sam-gov-pocs-pipelines", image=image)


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
# 1-indexed (DuckDB list_extract) first-name position of POC slot 1, by width.
# pos46 → list index 47 (v2 / legacy-142); pos44 → 45 (legacy-120). Eleven fields
# per slot; six slots stride by 11.
_POC_FIELD_NAMES = [
    "first_name", "middle_name", "last_name", "title",
    "address_line_1", "address_line_2", "city", "zip5", "zip4", "country", "state",
]
_POC_TYPE_BY_SLOT = {
    1: "government_business", 2: "government_business_alt",
    3: "past_performance", 4: "past_performance_alt",
    5: "electronic_business", 6: "electronic_business_alt",
}


def build_pocs_sql() -> str:
    """Positional unpivot of the SAM POC blocks → one row per (entity, slot).

    Reads a `reg` relation (the entity_registrations scan) with columns
    uei, cage_code, format_family, field_count, pipe_fields, last_update_date,
    registration_date, extract_label. Keys come from the projected (and
    crosswalk-aligned) uei/cage_code columns; POC content from pipe_fields.
    """
    # Per-slot field projections referencing the row's 1-indexed slot-1 base `b`
    # and slot offset. fn = b + (slot-1)*11 is the slot's first-name index.
    def field_expr(offset: int) -> str:
        return f"nullif(trim(pf[fn + {offset}]), '')"

    field_projs = ",\n      ".join(
        f"{field_expr(i)} AS {name}" for i, name in enumerate(_POC_FIELD_NAMES)
    )
    poc_type_case = " ".join(
        f"WHEN {slot} THEN '{label}'" for slot, label in _POC_TYPE_BY_SLOT.items()
    )
    # Lossless verbatim name: concat of the present pre-split parts (NULLs skipped
    # by concat_ws); nothing parsed, nothing dropped.
    full_name_expr = (
        "trim(concat_ws(' ', "
        f"{field_expr(0)}, {field_expr(1)}, {field_expr(2)}))"
    )
    return f"""
WITH extracted AS (
    SELECT
        CASE WHEN format_family = 'v2' THEN nullif(trim(uei), '') END AS uei,
        nullif(trim(cage_code), '')                                  AS cage_code,
        format_family                                                AS source_family,
        last_update_date, registration_date, extract_label,
        CASE WHEN field_count = 142 THEN 47
             WHEN field_count = 120 THEN 45 END                      AS b,
        pipe_fields                                                  AS pf
    FROM reg
    -- v2 (uei-native) + clean 120-wide legacy (cage-native). 142-wide legacy is a
    -- mis-classified v2 layout (uei nulled by the ingest) → excluded by design.
    WHERE (format_family = 'v2')
       OR (format_family = 'legacy_v1' AND field_count = 120)
),
keyed AS (
    SELECT *
    FROM extracted
    WHERE b IS NOT NULL AND (uei IS NOT NULL OR cage_code IS NOT NULL)
    QUALIFY row_number() OVER (
        PARTITION BY coalesce(uei, 'CAGE:' || cage_code)
        ORDER BY last_update_date DESC NULLS LAST, registration_date DESC NULLS LAST
    ) = 1
),
slotted AS (
    SELECT
        uei, cage_code, source_family, extract_label,
        s.slot_no,
        b + (s.slot_no - 1) * 11 AS fn,
        pf
    FROM keyed
    CROSS JOIN (SELECT unnest([1, 2, 3, 4, 5, 6]) AS slot_no) s
),
unpacked AS (
    SELECT
        uei,
        cage_code,
        source_family,
        slot_no AS poc_slot_no,
        CASE slot_no {poc_type_case} END AS poc_type,
        {field_projs},
        {full_name_expr}            AS full_name,
        upper({full_name_expr})     AS name_key,
        extract_label               AS sam_extract_label
    FROM slotted
)
SELECT *
FROM unpacked
WHERE first_name IS NOT NULL OR last_name IS NOT NULL
"""


def _materialize(con):
    """Register the SAM source, run the positional unpivot, return
    (arrow_table, metrics, sam_label). One scan of entity_registrations."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SAM_SRC_URI, storage_options=so)

    # sam_label provenance — a cheap one-column scan (a registered Arrow reader is a
    # one-shot stream, so the build reader below must be consumed exactly once).
    con.register("lbl", ds.scanner(
        columns=["extract_label", "format_family"], filter="format_family = 'v2'"
    ).to_reader())
    sam_label = con.execute("SELECT max(extract_label) FROM lbl").fetchone()[0]
    con.unregister("lbl")

    # Build scan — single consumption straight into the pocs temp table.
    con.register("reg", ds.scanner(
        columns=["uei", "cage_code", "format_family", "field_count", "pipe_fields",
                 "last_update_date", "registration_date", "extract_label"],
        filter="format_family = 'v2' OR (format_family = 'legacy_v1' AND field_count = 120)",
    ).to_reader())
    con.execute(f"CREATE TEMP TABLE pocs AS {build_pocs_sql()}")
    con.unregister("reg")

    rows, d_uei, d_cage, v2_rows, lg_rows = con.execute("""
        SELECT count(*),
               count(DISTINCT uei),
               count(DISTINCT cage_code) FILTER (WHERE uei IS NULL),
               count(*) FILTER (WHERE source_family = 'v2'),
               count(*) FILTER (WHERE source_family = 'legacy_v1')
        FROM pocs
    """).fetchone()
    table = con.sql("SELECT * FROM pocs").to_arrow_table()
    metrics = {
        "rows": int(rows), "distinct_uei": int(d_uei), "distinct_cage": int(d_cage),
        "poc_rows_v2": int(v2_rows), "poc_rows_legacy": int(lg_rows),
    }
    return table, metrics, sam_label


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
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
                INSERT INTO ops.sam_pocs_runs
                    (feed, dataset_uri, sam_label, rows_written, distinct_uei,
                     distinct_cage, poc_rows_v2, poc_rows_legacy, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, sam_label, metrics.get("rows"),
                 metrics.get("distinct_uei"), metrics.get("distinct_cage"),
                 metrics.get("poc_rows_v2"), metrics.get("poc_rows_legacy"),
                 status, error, started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


def _post_callback(url, payload, attempts: int = 3) -> None:
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
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


# --------------------------------------------------------------------------- #
# Core build
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=32768,
    cpu=8.0,
)
def build_sam_pocs(trigger_callback_url: str | None = None) -> dict:
    """Rebuild the SAM POC human layer and publish it to R2 active. Idempotent full
    overwrite; BTREE + BITMAP indexes rebuilt on the R2 dataset each run."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error, sam_label = "error", None, None
    metrics = {"rows": 0, "distinct_uei": 0, "distinct_cage": 0,
               "poc_rows_v2": 0, "poc_rows_legacy": 0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics, sam_label = _materialize(con)
        finally:
            con.close()
        print(f"Built sam_pocs: {metrics} sam_label={sam_label}")

        lance.write_dataset(
            table, DATASET_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        print(f"Wrote Lance dataset (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")

        ds = lance.dataset(DATASET_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE ✓ {col}")
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP")
            print(f"  BITMAP ✓ {col}")
        committed = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        print(f"Committed rows: {committed:,}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(sam_label=sam_label, metrics=metrics, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": metrics["rows"], "feed": FEED,
                        "dataset_uri": DATASET_URI, "sam_label": sam_label,
                        "distinct_uei": metrics["distinct_uei"],
                        "distinct_cage": metrics["distinct_cage"]})

    if status != "success":
        raise RuntimeError(f"sam_pocs build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, "sam_label": sam_label, **metrics}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_sam_pocs() -> dict:
    """Read-back proof: open the published dataset from R2 and report counts,
    schema, indices, slot distribution — independent of the write path."""
    import lance
    import duckdb

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
           for i in ds.list_indices()]
    con = duckdb.connect()
    con.register("d", ds.scanner(
        columns=["uei", "cage_code", "poc_type", "source_family", "name_key"]).to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    n, d_uei, d_cage, d_name = con.execute(
        "SELECT count(*), count(DISTINCT uei), "
        "count(DISTINCT cage_code) FILTER (WHERE uei IS NULL), "
        "count(DISTINCT name_key) FROM d2").fetchone()
    by_type = dict(con.execute("SELECT poc_type, count(*) FROM d2 GROUP BY 1").fetchall())
    by_fam = dict(con.execute("SELECT source_family, count(*) FROM d2 GROUP BY 1").fetchall())
    con.close()
    # Independent name-correctness sample (catches any positional drift).
    sample = ds.scanner(
        columns=["uei", "cage_code", "source_family", "poc_type", "first_name",
                 "middle_name", "last_name", "full_name", "title", "city", "state"],
        limit=6).to_table().to_pylist()
    return {"rows": n, "distinct_uei": d_uei, "distinct_cage": d_cage,
            "distinct_name_key": d_name, "by_poc_type": by_type,
            "by_source_family": by_fam, "indices": idx, "uri": DATASET_URI,
            "schema": [f"{f.name}:{f.type}" for f in ds.schema], "sample": sample}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.sam_pocs_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.sam_pocs_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 60, memory=32768, cpu=8.0,
)
def plan_sam_pocs() -> dict:
    """Remote review gate — materialize + count, write NOTHING."""
    os.makedirs(SPILL_DIR, exist_ok=True)
    con = _new_con()
    try:
        _table, metrics, sam_label = _materialize(con)
    finally:
        con.close()
    return {"sam_label": sam_label, **metrics}


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    if dry_run:
        print(plan_sam_pocs.remote())
        return
    print(build_sam_pocs.remote(trigger_callback_url=None))
    print(verify_sam_pocs.remote())
