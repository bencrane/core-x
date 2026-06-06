"""Compute worker — SAM.gov Points of Contact (human layer, Pattern-A derived).

The ``sam-gov-pocs-pipelines`` Modal app. Reshapes the POC blocks embedded in the
positional ``pipe_fields`` array of the published SAM entity registry into a
long, indexed human-contact dataset that attaches to the SAM × USAspending
crosswalk spine. Spawned only by the Universal Dispatcher; no web endpoint.

Source-of-truth input (read-only; never mutated):
  s3://data-sink/active/entity_registrations/   (Lance, 19.3M rows)
    POCs live in pipe_fields (the lossless positional array the ingest deferred),
    NOT in a separate dataset. Layout is width-determined — the loader classifies
    `format_family` by width (142 ⇒ v2, 120 ⇒ legacy_v1):
      - v2            (width 142): uei@pos0, cage@pos3, 6 POC slots base pos46
      - legacy_v1     (width 120): no uei, cage@pos2, 6 POC slots base pos44
    The loader previously filename-classified some 142-wide v2 extracts (the
    `MONTHLY_*_MODIFIED` set) as legacy_v1 and nulled their uei; that is fixed in
    entity_registrations_bulk.py, so every 142-wide record — including those
    recovered extracts — is uei-native v2 here and attaches at base pos46.

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

from core.ops_alert import alert

BUCKET = "data-sink"
SAM_SRC_URI = "s3://data-sink/active/entity_registrations/"
_PROD_URI = "s3://data-sink/active/sam_pocs/"
DATASET_URI = os.environ.get("SAM_POCS_LANCE_URI", _PROD_URI)


def _feed_for(uri: str) -> str:
    """Ledger feed tag for a write target: prod URI → 'sam_pocs'; any other URI
    (scratch/validation) → 'sam_pocs_scratch' so validation runs never poison the prod
    Δ-baseline or pollute prod metrics. The effective URI is threaded per-call (the
    container's module-level DATASET_URI cannot see a locally-set SAM_POCS_LANCE_URI)."""
    return "sam_pocs" if uri == _PROD_URI else "sam_pocs_scratch"


FEED = _feed_for(DATASET_URI)

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

# ── gate constants (baselined from ops.sam_pocs_runs healthy runs 2026-06-02..05) ──
# Floors = catastrophic-collapse catchers (well below live). The per-family Δ-guards
# are the sensitive regression check and auto-track month-over-month drift. Re-baseline
# the absolute floors on any sustained ±20% shift in the SAM universe (SAM also *purges*
# inactive registrations — a legitimate shrink must not false-trip a floor).
# Live healthy: rows ~8.07M · distinct_uei ~1.541M · distinct_cage ~1.168M ·
#               poc_rows_v2 ~4.373M · poc_rows_legacy ~3.692M.
POCS_ROW_FLOOR = 6_000_000       # abort floor
DISTINCT_UEI_FLOOR = 1_300_000   # v2 entities w/ ≥1 POC
DISTINCT_CAGE_FLOOR = 900_000    # legacy cage entities w/ ≥1 POC
BASELINE_MIN_ROWS = 7_000_000    # a success must clear this to qualify as a Δ baseline
DELTA_GUARD = 0.25               # ±25% per-family vs prior healthy success
NAME_FILL_MIN = 0.999            # name_key fill — invariant tripwire (NOT a content check)
NAME_ALPHA_MIN = 0.95            # first_name alpha-char fraction (positional-offset defense)
ZIP_NUMERIC_MIN = 0.95           # zip5 numeric fraction (positional-offset defense)
MANDATORY_SLOTS = ("government_business", "electronic_business")  # always-populated pair
SEEK_CEILING_MS = 2000           # post-write seek WARN threshold (observability only — a cold
                                 # R2 first-seek of a freshly-written index can exceed this)

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
).env({"LANCE_BYPASS_SPILLING": "true"}).add_local_python_source("core.ops_alert")

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
# SAM extract-label sort key
# --------------------------------------------------------------------------- #
def _snap_key_sql(col: str = "extract_label") -> str:
    """Canonical SAM extract-label sort key — verbatim copy of sam_master.py:_snap_key_sql.
    Normalizes both ``^[0-9]{8}$`` and ``YYYY_MMM`` label forms to a numeric sort key so
    ``max`` over mixed forms is correct (a naive lexical max picks '2026_MAY' over a later
    '20260601' because '_' > digit). One accepted duplication (frozen calendar map, zero
    divergence risk); §10 of the build plan consolidates this into a shared module."""
    months = ("WHEN 'JAN' THEN '01' WHEN 'FEB' THEN '02' WHEN 'MAR' THEN '03' WHEN 'APR' THEN '04' "
              "WHEN 'MAY' THEN '05' WHEN 'JUN' THEN '06' WHEN 'JUL' THEN '07' WHEN 'AUG' THEN '08' "
              "WHEN 'SEP' THEN '09' WHEN 'OCT' THEN '10' WHEN 'NOV' THEN '11' WHEN 'DEC' THEN '12'")
    return (f"CASE WHEN regexp_matches({col}, '^[0-9]{{8}}$') THEN CAST({col} AS BIGINT) "
            f"ELSE CAST(substr({col},1,4) || CASE upper(substr({col},6,3)) {months} ELSE '00' END "
            f"|| '00' AS BIGINT) END")


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
    registration_date, extract_label, source_file. Keys come from the projected
    (and crosswalk-aligned) uei/cage_code columns; POC content from pipe_fields.
    The dedup QUALIFY tiebreaks deterministically (a uei is unique within one
    source_file) so rebuilds over a fixed source are bit-stable.
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
        last_update_date, registration_date, extract_label, source_file,
        CASE WHEN field_count = 142 THEN 47
             WHEN field_count = 120 THEN 45 END                      AS b,
        pipe_fields                                                  AS pf
    FROM reg
    -- v2 (uei-native, width 142) + legacy_v1 (cage-native, width 120). The loader
    -- now classifies by width, so 142-wide records are always v2 here; the
    -- field_count guard only drops any future width-anomalous legacy_v1 row.
    WHERE (format_family = 'v2')
       OR (format_family = 'legacy_v1' AND field_count = 120)
),
keyed AS (
    SELECT *
    FROM extracted
    WHERE b IS NOT NULL AND (uei IS NOT NULL OR cage_code IS NOT NULL)
    QUALIFY row_number() OVER (
        PARTITION BY coalesce(uei, 'CAGE:' || cage_code)
        ORDER BY last_update_date DESC NULLS LAST, registration_date DESC NULLS LAST,
                 extract_label DESC NULLS LAST, source_file DESC NULLS LAST
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
    sam_label = con.execute(
        f"SELECT extract_label FROM lbl ORDER BY {_snap_key_sql()} DESC LIMIT 1"
    ).fetchone()[0]
    con.unregister("lbl")

    # Build scan — single consumption straight into the pocs temp table. source_file is
    # carried for the deterministic dedup tiebreak (a uei is unique within one source_file).
    con.register("reg", ds.scanner(
        columns=["uei", "cage_code", "format_family", "field_count", "pipe_fields",
                 "last_update_date", "registration_date", "extract_label", "source_file"],
        filter="format_family = 'v2' OR (format_family = 'legacy_v1' AND field_count = 120)",
    ).to_reader())
    con.execute(f"CREATE TEMP TABLE pocs AS {build_pocs_sql()}")
    con.unregister("reg")

    (rows, d_uei, d_cage, v2_rows, lg_rows, name_present, unkeyed, null_pt,
     fn_present, fn_alpha, zip_present, zip_num, present_types) = con.execute("""
        SELECT count(*),
               count(DISTINCT uei),
               count(DISTINCT cage_code) FILTER (WHERE uei IS NULL),
               count(*) FILTER (WHERE source_family = 'v2'),
               count(*) FILTER (WHERE source_family = 'legacy_v1'),
               count(*) FILTER (WHERE name_key IS NOT NULL),
               count(*) FILTER (WHERE uei IS NULL AND cage_code IS NULL),
               count(*) FILTER (WHERE poc_type IS NULL),
               count(*) FILTER (WHERE first_name IS NOT NULL),
               count(*) FILTER (WHERE first_name IS NOT NULL AND regexp_matches(first_name, '[A-Za-z]')),
               count(*) FILTER (WHERE zip5 IS NOT NULL),
               count(*) FILTER (WHERE zip5 IS NOT NULL AND regexp_matches(zip5, '^[0-9]{3,5}$')),
               array_agg(DISTINCT poc_type)
        FROM pocs
    """).fetchone()
    # Probe: a uei guaranteed to carry a POC with a non-null name_key (most POC rows;
    # uei tiebreak → stable across runs). Drives the post-write round-trip gate (16).
    probe_row = con.execute("""
        SELECT uei FROM pocs WHERE uei IS NOT NULL AND name_key IS NOT NULL
        GROUP BY uei ORDER BY count(*) DESC, uei LIMIT 1
    """).fetchone()
    table = con.sql("SELECT * FROM pocs").to_arrow_table()
    metrics = {
        "rows": int(rows), "distinct_uei": int(d_uei), "distinct_cage": int(d_cage),
        "poc_rows_v2": int(v2_rows), "poc_rows_legacy": int(lg_rows),
        "name_present_frac": (name_present / rows) if rows else 0.0,
        "unkeyed_rows": int(unkeyed), "null_poc_type": int(null_pt),
        "name_alpha_frac": (fn_alpha / fn_present) if fn_present else 0.0,
        "zip_numeric_frac": (zip_num / zip_present) if zip_present else 0.0,
        "present_poc_types": set(present_types or []),
        "probe_uei": probe_row[0] if probe_row else None,
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
# pre-write gates (pure — no R2/Modal/PG; the unit-tested core of the safety net)
# --------------------------------------------------------------------------- #
def _within(value: int, target: int, tol: float) -> bool:
    return abs(value - target) <= target * tol


def assert_pre_write_gates(metrics: dict, baseline: dict | None) -> list[str]:
    """Gates 1-13 on in-memory metrics. Raises RuntimeError on the first hard failure;
    returns the human-readable check log on success. Protection lives here — pre-write,
    at zero exposure window. `baseline` is the floor-qualified, prod-URI prior success
    (or None on the first build / no qualified prior)."""
    rows = metrics["rows"]
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    # — floors (catastrophic-collapse catchers) —
    gate(rows >= POCS_ROW_FLOOR, f"1 row floor: {rows:,} >= {POCS_ROW_FLOOR:,}")
    gate(metrics["distinct_uei"] >= DISTINCT_UEI_FLOOR,
         f"2 uei floor: {metrics['distinct_uei']:,} >= {DISTINCT_UEI_FLOOR:,}")
    gate(metrics["distinct_cage"] >= DISTINCT_CAGE_FLOOR,
         f"3 cage floor: {metrics['distinct_cage']:,} >= {DISTINCT_CAGE_FLOOR:,}")
    gate(metrics["poc_rows_v2"] > 0 and metrics["poc_rows_legacy"] > 0,
         f"4 both families: v2={metrics['poc_rows_v2']:,} legacy={metrics['poc_rows_legacy']:,}")
    # — per-family Δ-guards (the sensitive regression check) —
    if baseline:
        gate(_within(rows, baseline["rows_written"], DELTA_GUARD),
             f"5 rows Δ: {rows:,} ~ ±{DELTA_GUARD:.0%} of {baseline['rows_written']:,}")
        gate(_within(metrics["poc_rows_v2"], baseline["poc_rows_v2"], DELTA_GUARD),
             f"6 v2 Δ: {metrics['poc_rows_v2']:,} ~ ±{DELTA_GUARD:.0%} of {baseline['poc_rows_v2']:,}")
        gate(_within(metrics["distinct_uei"], baseline["distinct_uei"], DELTA_GUARD),
             f"7 uei Δ: {metrics['distinct_uei']:,} ~ ±{DELTA_GUARD:.0%} of {baseline['distinct_uei']:,}")
    else:
        checks.append("SKIP  5-7 Δ-guards: no floor-qualified prior success")
    # — structural invariants (defense-in-depth on the SQL) —
    gate(metrics["unkeyed_rows"] == 0, f"8 every row keyed (uei|cage): unkeyed={metrics['unkeyed_rows']}")
    gate(metrics["null_poc_type"] == 0, f"9 no null poc_type: {metrics['null_poc_type']}")
    gate(set(MANDATORY_SLOTS).issubset(metrics["present_poc_types"]),
         f"10 mandatory slots present: have {sorted(metrics['present_poc_types'])}")
    gate(metrics["name_present_frac"] >= NAME_FILL_MIN,
         f"11 name-fill tripwire: {metrics['name_present_frac']:.4%} >= {NAME_FILL_MIN:.2%}")
    # — content plausibility (positional-offset defense) —
    gate(metrics["name_alpha_frac"] >= NAME_ALPHA_MIN,
         f"12 name-alpha: {metrics['name_alpha_frac']:.4%} >= {NAME_ALPHA_MIN:.0%}")
    gate(metrics["zip_numeric_frac"] >= ZIP_NUMERIC_MIN,
         f"13 zip-numeric: {metrics['zip_numeric_frac']:.4%} >= {ZIP_NUMERIC_MIN:.0%}")
    return checks


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


def _prior_success_baseline(dataset_uri: str) -> dict | None:
    """Latest success at `dataset_uri` that cleared BASELINE_MIN_ROWS — the per-family Δ
    baseline. Floor-qualified (a degraded success can't become the baseline → no ratchet)
    and dataset_uri-scoped (scratch/validation runs can't poison the prod baseline)."""
    conn = _pg_connect()
    if conn is None:
        return None
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "SELECT rows_written, distinct_uei, poc_rows_v2, poc_rows_legacy "
                "FROM ops.sam_pocs_runs "
                "WHERE status='success' AND dataset_uri = %s AND rows_written >= %s "
                "ORDER BY recorded_at DESC LIMIT 1",
                (dataset_uri, BASELINE_MIN_ROWS),
            )
            r = cur.fetchone()
            return None if not r else {
                "rows_written": int(r[0]), "distinct_uei": int(r[1]),
                "poc_rows_v2": int(r[2]), "poc_rows_legacy": int(r[3]),
            }
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: baseline lookup failed: {exc}")
        return None
    finally:
        conn.close()


def _record_run(*, feed, dataset_uri, sam_label, metrics, status, error,
                started_at, completed_at) -> None:
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
                (feed, dataset_uri, sam_label, metrics.get("rows"),
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
def build_sam_pocs(trigger_callback_url: str | None = None,
                   dataset_uri: str | None = None) -> dict:
    """Rebuild the SAM POC human layer and publish it to R2 active — fail-safe.
    Pre-write gates 1-13 abort before any R2 write; write + indexing + post-write
    gates 14-17 run under one rollback guard that restores the prior version on any
    failure. Idempotent overwrite; terminal failure emits a best-effort alert.

    dataset_uri overrides the write target (the local entrypoint passes
    SAM_POCS_LANCE_URI for scratch validation); None → the module default (prod).
    The dispatcher spawns with kwargs={}, so the daily run always targets prod."""
    import datetime as dt
    import time

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    uri = dataset_uri or DATASET_URI
    feed = _feed_for(uri)
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
        baseline = _prior_success_baseline(uri)
        printable = {k: v for k, v in metrics.items() if k != "present_poc_types"}
        print(f"target={uri} feed={feed}")
        print(f"materialized: {printable} sam_label={sam_label} baseline={baseline}")

        # ── PRE-WRITE GATES — abort before any R2 write ──
        for line in assert_pre_write_gates(metrics, baseline):
            print("  ", line)

        # ── rollback target (sam_pocs is live → resolves; None only on a net-new URI) ──
        try:
            v_before = lance.dataset(uri, storage_options=so).version
        except Exception:  # noqa: BLE001
            v_before = None
        print(f"v_before = {v_before}")

        # ── write + index + post-write gates, ALL under one rollback guard ──
        try:
            lance.write_dataset(
                table, uri, mode="overwrite",
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE,
                max_bytes_per_file=MAX_BYTES_PER_FILE,
                storage_options=so,
            )
            print(f"Wrote Lance dataset (overwrite, v{DATA_STORAGE_VERSION}) → {uri}")
            ds = lance.dataset(uri, storage_options=so)
            for col in BTREE_INDEXES:
                ds.create_scalar_index(col, index_type="BTREE")
                print(f"  BTREE ✓ {col}")
            for col in BITMAP_INDEXES:
                ds.create_scalar_index(col, index_type="BITMAP")
                print(f"  BITMAP ✓ {col}")

            ds = lance.dataset(uri, storage_options=so)
            committed = ds.count_rows()
            if committed != metrics["rows"]:
                raise RuntimeError(f"gate 14 write-integrity: committed {committed:,} != materialized {metrics['rows']:,}")
            idx = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                   for i in ds.list_indices()}
            expect = {f"{c}_idx" for c in BTREE_INDEXES + BITMAP_INDEXES}
            if not expect.issubset(idx):
                raise RuntimeError(f"gate 15 indices: missing {sorted(expect - idx)} (have {sorted(idx)})")
            pr = metrics["probe_uei"]
            rt = ds.scanner(columns=["uei", "name_key"], filter=f"uei = '{pr}'").to_table().to_pylist()
            probe_name = next((r["name_key"] for r in rt if r["name_key"]), None)
            if not probe_name:
                raise RuntimeError(f"gate 16 round-trip: probe {pr} → {len(rt)} rows / no name_key")
            # Gate 17 — the name_key reverse index returns the known-present probe (correctness).
            # Seek LATENCY is logged, not gated: a cold first-seek of a freshly-written R2 index
            # is slow (~4s on prod) and not representative of whether the index is used. Gate 15
            # already proves the index exists; gating on cold-seek time was a false-positive that
            # rolled back a good build, so timing is observability only (WARN above the ceiling).
            _esc = probe_name.replace(chr(39), chr(39) * 2)
            t0 = time.monotonic()
            hit = ds.scanner(columns=["uei"], filter=f"name_key = '{_esc}'").to_table().num_rows
            seek_ms = (time.monotonic() - t0) * 1000
            if hit < 1:
                raise RuntimeError(f"gate 17 name_key index: lookup of a known-present name returned 0 rows")
            _slow = "" if seek_ms <= SEEK_CEILING_MS else f"  [WARN cold seek >{SEEK_CEILING_MS}ms]"
            print(f"post-write gates PASS — committed={committed:,} idx={sorted(idx)} "
                  f"probe={pr} name_key_hits={hit} seek={seek_ms:.0f}ms{_slow}")
        except Exception as werr:  # noqa: BLE001
            if v_before is not None:
                try:
                    lance.dataset(uri, storage_options=so, version=v_before).restore()
                except Exception as rerr:  # noqa: BLE001
                    raise RuntimeError(f"ROLLBACK FAILED to v{v_before}: {rerr}; original: {werr}")
                raise RuntimeError(f"write/index/gate failed → rolled back to v{v_before}: {werr}")
            raise RuntimeError(f"failed on net-new dataset (inspect/drop {uri}): {werr}")

        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(feed=feed, dataset_uri=uri, sam_label=sam_label, metrics=metrics,
                    status=status, error=error, started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": metrics["rows"], "feed": feed,
                        "dataset_uri": uri, "sam_label": sam_label,
                        "distinct_uei": metrics["distinct_uei"],
                        "distinct_cage": metrics["distinct_cage"]})
        if status != "success":
            alert(f"[sam_pocs] {feed} build {status}: {str(error)[:300]}")

    if status != "success":
        raise RuntimeError(f"sam_pocs build failed: {error}")
    return {"feed": feed, "dataset": uri, "sam_label": sam_label,
            "rows": metrics["rows"], "distinct_uei": metrics["distinct_uei"],
            "distinct_cage": metrics["distinct_cage"], "poc_rows_v2": metrics["poc_rows_v2"],
            "poc_rows_legacy": metrics["poc_rows_legacy"]}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_sam_pocs(dataset_uri: str | None = None) -> dict:
    """Read-back proof: open the published dataset from R2 and report counts,
    schema, indices, slot distribution — independent of the write path."""
    import lance
    import duckdb

    so = _r2_storage_options()
    uri = dataset_uri or DATASET_URI
    ds = lance.dataset(uri, storage_options=so)
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
            "by_source_family": by_fam, "indices": idx, "uri": uri,
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
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60, memory=32768, cpu=8.0,
)
def plan_sam_pocs(dataset_uri: str | None = None) -> dict:
    """Remote review gate — materialize, run the SAME pre-write gates as the build
    (incl. the floor-qualified Δ baseline scoped to the effective URI), write NOTHING.
    Raises if any gate fails."""
    os.makedirs(SPILL_DIR, exist_ok=True)
    uri = dataset_uri or DATASET_URI
    con = _new_con()
    try:
        _table, metrics, sam_label = _materialize(con)
    finally:
        con.close()
    baseline = _prior_success_baseline(uri)
    checks = assert_pre_write_gates(metrics, baseline)
    out = {k: v for k, v in metrics.items() if k != "present_poc_types"}
    return {"sam_label": sam_label, "target": uri, "feed": _feed_for(uri),
            "baseline": baseline, "gates": checks, **out}


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    # Local override → passed explicitly to the remote (the container's module-level
    # DATASET_URI cannot see this env var). None → remote uses the prod default.
    uri = os.environ.get("SAM_POCS_LANCE_URI")
    if dry_run:
        print(plan_sam_pocs.remote(dataset_uri=uri))
        return
    print(build_sam_pocs.remote(trigger_callback_url=None, dataset_uri=uri))
    print(verify_sam_pocs.remote(dataset_uri=uri))
