"""Compute worker — SAM.gov normalized-name → UEI resolution sidecar.

The ``sam-gov-normalized-entities-pipelines`` Modal app. Projects the faithful golden
mirror ``sam_master_entities`` into a thin, BTREE-indexed resolution surface: one row per
UEI carrying the canonical ``core.name_norm`` blocking keys (``normalized_legal_name`` +
``legal_name_base``) precomputed and indexed, plus inline geo for the false-positive
tiebreak. THE reusable, consumer-agnostic right side for any ``name → UEI`` bridge.

Plan of record: docs/plans/SAM_NORMALIZED_ENTITIES_BUILD_PLAN.md (+ its adversarial review).
The blocking key is byte-identical to sos_normalized_master / crosswalk_sam_usaspending —
``core.name_norm`` is imported, never re-inlined.

Source-of-truth input (read-only; never mutated):
  s3://data-sink/active/sam_master_entities/   (Lance, 1,541,566 rows, 1/uei)

Why a sidecar, not columns on the mirror: name_norm is an evolving key policy; isolating it
in a derived projection keeps the faithful mirror's contract clean, bounds a macro-change
rebuild to this narrow table, and keeps it unionable with sos_normalized_master (matching
source_state/zip_code column names). Full data stays once in the mirror; this is an index.

Data plane (clean-room — DuckDB does 100% of the transform):
  Lance(sam_master_entities) → DuckDB project (name_norm + legal_name_base + geo reshape) →
  Arrow → [pre-write gates on the Arrow table] → lance.write_dataset(R2 active, v2.1,
  overwrite) → BTREE(uei, normalized_legal_name, legal_name_base, cage_code, primary_naics)
  + BITMAP(is_active) → [post-write gates; restore-to-v_before on failure].
  LANCE_BYPASS_SPILLING=true forces the in-memory sort so the high-cardinality
  normalized_legal_name BTREE (~1.47M distinct) builds without OOM (lance#2650).

Control plane: on terminal state writes ops.sam_normalized_entities_runs and POSTs the flat
callback to wake a suspended Trigger run (if invoked with one).

    modal run    pipelines/sam_gov/sam_normalized_entities.py::init_ops   # create ops table
    modal run    pipelines/sam_gov/sam_normalized_entities.py --dry-run   # gates 1-7, no write
    modal run    pipelines/sam_gov/sam_normalized_entities.py             # build + verify
    modal deploy pipelines/sam_gov/sam_normalized_entities.py             # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

from core.name_norm import legal_name_base, name_norm
from core.ops_alert import alert

BUCKET = "data-sink"
SRC_URI = "s3://data-sink/active/sam_master_entities/"
DATASET_URI = os.environ.get(
    "SAM_NORMALIZED_ENTITIES_URI", "s3://data-sink/active/sam_normalized_entities/"
)
FEED = "sam_normalized_entities"

# Net-new dataset → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576

# Resolution keys → BTREE. normalized_legal_name = exact blocking key; legal_name_base =
# suffix-peeled blocking key; uei = spine; cage_code = defense-tail secondary; primary_naics
# = sector-scoped resolution. is_active → BITMAP (active-only filter).
BTREE_INDEXES = ["uei", "normalized_legal_name", "legal_name_base", "cage_code", "primary_naics"]
BITMAP_INDEXES = ["is_active"]

# The pure projection is narrow (8 source cols, no pipe_fields unnest); the cost is the
# high-cardinality string BTREE sort, kept in-memory by LANCE_BYPASS_SPILLING.
DUCKDB_MEMORY_LIMIT = "12GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

# ── §7 gate constants (probe baselines, 2026-06-05) ──────────────────────────
ROW_FLOOR = 1_400_000
NORM_DISTINCT_TARGET = 1_466_764
BASE_DISTINCT_TARGET = 1_450_598
CARDINALITY_TOL = 0.05          # ±5%
NORM_FILL_MIN = 0.999           # normalized_legal_name non-null floor
GEO_COFILL_MIN = 0.95           # name ∧ source_state ∧ zip_code floor
DELTA_GUARD = 0.25              # ±25% row delta vs prior success
KIPPER_UEI = "DD1BCRF2QQG8"     # canonical round-trip probe

SCAN_COLS = [
    "uei", "legal_business_name", "cage_code", "physical_address_province_or_state",
    "physical_address_zip_postal_code", "is_active", "primary_naics", "sam_extract_label",
]

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sam_normalized_entities_runs (
    id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                     text        NOT NULL,
    dataset_uri              text        NOT NULL,
    source_uri               text,
    sam_extract_label        text,
    rows_written             bigint,
    distinct_uei             bigint,
    distinct_normalized_name bigint,
    distinct_legal_name_base bigint,
    status                   text        NOT NULL,
    error                    text,
    started_at               timestamptz,
    completed_at             timestamptz,
    recorded_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sam_normalized_entities_runs_feed_idx        ON ops.sam_normalized_entities_runs (feed);
CREATE INDEX IF NOT EXISTS sam_normalized_entities_runs_status_idx      ON ops.sam_normalized_entities_runs (status);
CREATE INDEX IF NOT EXISTS sam_normalized_entities_runs_recorded_at_idx ON ops.sam_normalized_entities_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"}).add_local_python_source("core.name_norm").add_local_python_source("core.ops_alert")

app = modal.App("sam-gov-normalized-entities-pipelines", image=image)


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
def build_normalized_entities_sql() -> str:
    """Project sam_master_entities → the normalized resolution sidecar. Reads a `src`
    relation (the scanned mirror). 1 row/uei passthrough; uei is already unique in source.

    name_norm / legal_name_base are the canonical core.name_norm builders — byte-identical
    to the fleet. legal_name_base references the normalized_legal_name SELECT alias (DuckDB
    resolves SELECT-list aliases left-to-right; precedent: sos_normalized/normalize.py).
    """
    return f"""
    SELECT
        nullif(trim(uei), '')                                       AS uei,
        {name_norm("legal_business_name")}                          AS normalized_legal_name,
        {legal_name_base("normalized_legal_name")}                  AS legal_name_base,
        nullif(trim(legal_business_name), '')                       AS legal_business_name,
        nullif(trim(cage_code), '')                                 AS cage_code,
        nullif(trim(physical_address_province_or_state), '')        AS source_state,
        left(nullif(trim(physical_address_zip_postal_code), ''), 5) AS zip_code,
        nullif(trim(primary_naics), '')                             AS primary_naics,
        is_active,
        sam_extract_label,
        'sam_master_entities'                                       AS source_dataset
    FROM src
    WHERE nullif(trim(uei), '') IS NOT NULL
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
    """Register sam_master_entities, run the transform, return (arrow_table, metrics,
    sam_label). One build scan. metrics carry everything gates 1-7 need."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SRC_URI, storage_options=so)

    con.register("src", ds.scanner(columns=SCAN_COLS).to_reader())
    con.execute(f"CREATE TEMP TABLE sne AS {build_normalized_entities_sql()}")
    con.unregister("src")

    rows, d_uei, d_norm, d_base, norm_nn, geo_cofill = con.execute("""
        SELECT
            count(*),
            count(DISTINCT uei),
            count(DISTINCT normalized_legal_name),
            count(DISTINCT legal_name_base),
            count(*) FILTER (WHERE normalized_legal_name IS NOT NULL),
            count(*) FILTER (WHERE normalized_legal_name IS NOT NULL
                               AND source_state IS NOT NULL
                               AND zip_code IS NOT NULL)
        FROM sne
    """).fetchone()
    sam_label = con.execute("SELECT max(sam_extract_label) FROM sne").fetchone()[0]
    table = con.sql("SELECT * FROM sne").to_arrow_table()
    metrics = {
        "rows": int(rows), "distinct_uei": int(d_uei),
        "distinct_normalized_name": int(d_norm), "distinct_legal_name_base": int(d_base),
        "normalized_nonnull": int(norm_nn), "geo_cofill": int(geo_cofill),
    }
    return table, metrics, sam_label


# --------------------------------------------------------------------------- #
# §7 gates
# --------------------------------------------------------------------------- #
def _within(value: int, target: int, tol: float) -> bool:
    return abs(value - target) <= target * tol


def assert_pre_write_gates(metrics: dict, src_count: int, prior_rows: int | None) -> list[str]:
    """Gates 1-7 on the in-memory metrics. Raises RuntimeError on the first hard failure;
    returns the human-readable check log on success."""
    rows = metrics["rows"]
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    gate(rows >= ROW_FLOOR, f"1 row floor: {rows:,} >= {ROW_FLOOR:,}")
    gate(rows == src_count, f"2 1:1 passthrough: rows {rows:,} == source {src_count:,}")
    gate(metrics["distinct_uei"] == rows,
         f"3 uei uniqueness: distinct_uei {metrics['distinct_uei']:,} == rows {rows:,}")
    norm_fill = metrics["normalized_nonnull"] / rows
    gate(norm_fill >= NORM_FILL_MIN,
         f"4 normalized_legal_name fill: {norm_fill:.4%} >= {NORM_FILL_MIN:.2%}")
    gate(_within(metrics["distinct_normalized_name"], NORM_DISTINCT_TARGET, CARDINALITY_TOL)
         and _within(metrics["distinct_legal_name_base"], BASE_DISTINCT_TARGET, CARDINALITY_TOL),
         f"5 cardinality: norm {metrics['distinct_normalized_name']:,} (~{NORM_DISTINCT_TARGET:,}) "
         f"& base {metrics['distinct_legal_name_base']:,} (~{BASE_DISTINCT_TARGET:,}) within ±{CARDINALITY_TOL:.0%}")
    geo_fill = metrics["geo_cofill"] / rows
    gate(geo_fill >= GEO_COFILL_MIN,
         f"6 geo co-fill (name∧state∧zip): {geo_fill:.4%} >= {GEO_COFILL_MIN:.0%}")
    if prior_rows:
        gate(_within(rows, prior_rows, DELTA_GUARD),
             f"7 Δ-guard: {rows:,} within ±{DELTA_GUARD:.0%} of prior {prior_rows:,}")
    else:
        checks.append("SKIP  7 Δ-guard: no prior success (first build)")
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


def _prior_success_rows() -> int | None:
    """Latest successful rows_written from the ops ledger (for the Δ-guard). None if no
    prior success or no DB."""
    conn = _pg_connect()
    if conn is None:
        return None
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "SELECT rows_written FROM ops.sam_normalized_entities_runs "
                "WHERE status = 'success' AND rows_written IS NOT NULL "
                "ORDER BY recorded_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: prior-rows lookup failed: {exc}")
        return None
    finally:
        conn.close()


def _record_run(*, sam_label, metrics, status, error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.sam_normalized_entities_runs
                    (feed, dataset_uri, source_uri, sam_extract_label, rows_written,
                     distinct_uei, distinct_normalized_name, distinct_legal_name_base,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, SRC_URI, sam_label, metrics.get("rows"),
                 metrics.get("distinct_uei"), metrics.get("distinct_normalized_name"),
                 metrics.get("distinct_legal_name_base"), status, error,
                 started_at, completed_at),
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
def build_sam_normalized_entities(trigger_callback_url: str | None = None) -> dict:
    """Materialize the sidecar, run gates 1-7 BEFORE the overwrite, write + index, then run
    post-write gates 8-10 and restore the prior version on failure. Idempotent overwrite."""
    import datetime as dt
    import time

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error, sam_label = "error", None, None
    metrics = {"rows": 0, "distinct_uei": 0, "distinct_normalized_name": 0,
               "distinct_legal_name_base": 0, "normalized_nonnull": 0, "geo_cofill": 0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)

        # ── materialize + pre-write gates (on the Arrow table, before any overwrite) ──
        con = _new_con()
        try:
            table, metrics, sam_label = _materialize(con)
        finally:
            con.close()
        src_count = lance.dataset(SRC_URI, storage_options=so).count_rows()
        prior_rows = _prior_success_rows()
        print(f"materialized: {metrics} sam_label={sam_label} src_count={src_count:,}")
        for line in assert_pre_write_gates(metrics, src_count, prior_rows):
            print("  ", line)

        # ── capture the pre-write version for rollback (None for a net-new dataset) ──
        try:
            v_before = lance.dataset(DATASET_URI, storage_options=so).version
        except Exception:
            v_before = None
        print(f"v_before = {v_before}")

        # ── write + index ──
        lance.write_dataset(
            table, DATASET_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            storage_options=so,
        )
        print(f"wrote dataset (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")
        ds = lance.dataset(DATASET_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE ✓ {col}")
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP")
            print(f"  BITMAP ✓ {col}")

        # ── post-write gates 8-10; restore-to-v_before on failure ──
        try:
            ds = lance.dataset(DATASET_URI, storage_options=so)
            committed = ds.count_rows()
            idx_names = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                         for i in ds.list_indices()}
            expect_idx = {f"{c}_idx" for c in BTREE_INDEXES + BITMAP_INDEXES}
            if not expect_idx.issubset(idx_names):
                raise RuntimeError(f"gate 8 indices: missing {sorted(expect_idx - idx_names)} "
                                   f"(have {sorted(idx_names)})")
            kip = ds.scanner(columns=["uei", "normalized_legal_name"],
                             filter=f"uei = '{KIPPER_UEI}'").to_table().to_pylist()
            if not (len(kip) == 1 and kip[0]["normalized_legal_name"]):
                raise RuntimeError(f"gate 9 KIPPER round-trip: {KIPPER_UEI} → {kip}")
            probe_name = kip[0]["normalized_legal_name"]
            t0 = time.monotonic()
            hit = ds.scanner(columns=["uei"],
                             filter=f"normalized_legal_name = '{probe_name}'").to_table().num_rows
            seek_ms = (time.monotonic() - t0) * 1000
            # Gate 10 intent: prove a BTREE point-seek, not a full scan. <100ms is the warm
            # local target; a remote R2 seek carries network RTT, so the hard ceiling is set
            # generously (2s) to catch a missing-index full scan while tolerating RTT. Actual
            # ms is logged. (Engineering call — full plan §7.)
            if hit < 1 or seek_ms > 2000:
                raise RuntimeError(f"gate 10 point-lookup: {hit} rows in {seek_ms:.0f}ms (>2000ms ⇒ no index)")
            print(f"post-write gates PASS — committed={committed:,} indices={sorted(idx_names)} "
                  f"KIPPER='{probe_name}' seek={seek_ms:.0f}ms ({hit} rows) "
                  f"[target <100ms warm; <2000ms ceiling for R2 RTT]")
        except Exception as gate_exc:  # noqa: BLE001
            if v_before is not None:
                lance.dataset(DATASET_URI, storage_options=so, version=v_before).restore()
                raise RuntimeError(f"post-write gate failed → rolled back to v{v_before}: {gate_exc}")
            raise RuntimeError(f"post-write gate failed on net-new dataset (no rollback target; "
                               f"inspect/drop {DATASET_URI}): {gate_exc}")

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
                        "distinct_uei": metrics["distinct_uei"]})

    if status != "success":
        alert(f"[sam_normalized_entities] {FEED} build {status}: {str(error)[:300]}")
        raise RuntimeError(f"sam_normalized_entities build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, "sam_label": sam_label, **metrics}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=900)
def verify_sam_normalized_entities() -> dict:
    """Read-back proof from R2 — independent of the write path. Reports counts, schema,
    indices, a sample, and the §B8 observability (legal_name_base collision rate; CO-peel)."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = sorted((i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                 for i in ds.list_indices())

    con = duckdb.connect()
    con.register("d", ds.scanner(
        columns=["uei", "normalized_legal_name", "legal_name_base", "source_state",
                 "zip_code", "is_active"]).to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    con.unregister("d")
    rows, d_uei, d_norm, d_base = con.execute(
        "SELECT count(*), count(DISTINCT uei), count(DISTINCT normalized_legal_name), "
        "count(DISTINCT legal_name_base) FROM d2").fetchone()
    active = con.execute("SELECT count(*) FILTER (WHERE is_active) FROM d2").fetchone()[0]
    # §B8 observability: base keys mapping to >1 uei (collision), and bare-trailing-CO peels.
    base_multi, base_total = con.execute("""
        SELECT count(*) FILTER (WHERE n > 1), count(*) FROM (
            SELECT legal_name_base, count(DISTINCT uei) n FROM d2
            WHERE legal_name_base IS NOT NULL GROUP BY 1)
    """).fetchone()
    co_peel = con.execute(
        "SELECT count(*) FROM d2 WHERE normalized_legal_name LIKE '% CO' "
        "AND legal_name_base IS DISTINCT FROM normalized_legal_name").fetchone()[0]
    con.close()

    sample = ds.scanner(
        columns=["uei", "normalized_legal_name", "legal_name_base", "source_state",
                 "zip_code", "is_active", "primary_naics"], limit=6).to_table().to_pylist()
    return {
        "uri": DATASET_URI, "rows": rows, "distinct_uei": d_uei,
        "distinct_normalized_name": d_norm, "distinct_legal_name_base": d_base,
        "active_rows": active, "indices": idx,
        "schema": [f"{f.name}:{f.type}" for f in ds.schema],
        "obs_base_multi_uei_rate": round(base_multi / base_total, 5) if base_total else None,
        "obs_base_multi_uei_keys": base_multi,
        "obs_bare_co_peel_rows": co_peel,
        "sample": sample,
    }


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.sam_normalized_entities_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.sam_normalized_entities_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 30, memory=32768, cpu=8.0,
)
def plan_sam_normalized_entities() -> dict:
    """Dry-run gate — materialize + run gates 1-7, write NOTHING."""
    import lance

    os.makedirs(SPILL_DIR, exist_ok=True)
    so = _r2_storage_options()
    con = _new_con()
    try:
        _table, metrics, sam_label = _materialize(con)
    finally:
        con.close()
    src_count = lance.dataset(SRC_URI, storage_options=so).count_rows()
    prior_rows = _prior_success_rows()
    checks = assert_pre_write_gates(metrics, src_count, prior_rows)
    return {"feed": FEED, "sam_label": sam_label, "src_count": src_count,
            "prior_rows": prior_rows, "gates": checks, **metrics}


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    import json

    if dry_run:
        print(json.dumps(plan_sam_normalized_entities.remote(), indent=2, default=str))
        return
    print(json.dumps(build_sam_normalized_entities.remote(trigger_callback_url=None), indent=2, default=str))
    print(json.dumps(verify_sam_normalized_entities.remote(), indent=2, default=str))
