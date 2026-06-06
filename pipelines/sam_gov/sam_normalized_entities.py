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
_PROD_URI = "s3://data-sink/active/sam_normalized_entities/"
DATASET_URI = os.environ.get("SAM_NORMALIZED_ENTITIES_URI", _PROD_URI)


def _feed_for(uri: str) -> str:
    """Ledger feed tag: prod URI → 'sam_normalized_entities'; any override →
    'sam_normalized_entities_scratch' so validation never poisons the prod baseline."""
    return "sam_normalized_entities" if uri == _PROD_URI else "sam_normalized_entities_scratch"


FEED = "sam_normalized_entities"  # prod default; effective feed derived per-run from the uri

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

# ── gate constants (live: distinct_normalized_name ~1.467M · distinct_legal_name_base
# ~1.451M; coarse floors below the ±25% Δ-band so the per-family Δ is the sensitive check) ──
ROW_FLOOR = 1_400_000           # tight floor on the critical 1:1 row/uei count
NORM_FLOOR = 1_050_000          # distinct_normalized_name floor BELOW the ±25% Δ-band (Δ-lower
BASE_FLOOR = 1_050_000          # ~1.10M) so the per-family Δ is the binding sensitive check —
                                # a distinct-key collapse is caught by gate 10/11, not shadowed
BASELINE_MIN_ROWS = 1_450_000   # a success must clear this to qualify as a Δ baseline
NORM_FILL_MIN = 0.999           # normalized_legal_name non-null floor
NAME_ALPHA_MIN = 0.95           # normalized_legal_name alpha-frac (normalization-regression defense)
GEO_COFILL_MIN = 0.95           # name ∧ source_state ∧ zip_code floor
DELTA_GUARD = 0.25              # ±25% per-family vs prior success
SEEK_WARN_MS = 2000             # post-write seek WARN threshold (observability only — not gated)

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
).env({"LANCE_BYPASS_SPILLING": "true"}).add_local_python_source(
    "core.name_norm", "core.ops_alert", "pipelines.sam_gov.reference.sam_labels")

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

    (rows, d_uei, d_norm, d_base, norm_nn, geo_cofill, name_alpha) = con.execute("""
        SELECT
            count(*),
            count(DISTINCT uei),
            count(DISTINCT normalized_legal_name),
            count(DISTINCT legal_name_base),
            count(*) FILTER (WHERE normalized_legal_name IS NOT NULL),
            count(*) FILTER (WHERE normalized_legal_name IS NOT NULL
                               AND source_state IS NOT NULL
                               AND zip_code IS NOT NULL),
            count(*) FILTER (WHERE normalized_legal_name IS NOT NULL
                               AND regexp_matches(normalized_legal_name, '[A-Za-z]'))
        FROM sne
    """).fetchone()
    sam_label = con.execute("SELECT max(sam_extract_label) FROM sne").fetchone()[0]
    # population probe (D9) — a uei with a non-null key, deterministic via uei order.
    probe = con.execute(
        "SELECT uei FROM sne WHERE uei IS NOT NULL AND normalized_legal_name IS NOT NULL "
        "ORDER BY uei LIMIT 1").fetchone()
    table = con.sql("SELECT * FROM sne").to_arrow_table()
    metrics = {
        "rows": int(rows), "distinct_uei": int(d_uei),
        "distinct_normalized_name": int(d_norm), "distinct_legal_name_base": int(d_base),
        "normalized_nonnull": int(norm_nn), "geo_cofill": int(geo_cofill),
        "name_alpha_frac": (name_alpha / norm_nn) if norm_nn else 0.0,
        "probe_uei": probe[0] if probe else None,
    }
    return table, metrics, sam_label


# --------------------------------------------------------------------------- #
# §7 gates
# --------------------------------------------------------------------------- #
def _within(value: int, target: int, tol: float) -> bool:
    return abs(value - target) <= target * tol


def assert_pre_write_gates(metrics: dict, src_count: int, baseline: dict | None) -> list[str]:
    """Gates on the in-memory metrics, BEFORE the overwrite. Raises on first hard failure;
    returns the check log on success. `baseline` = the floor-qualified, URI-scoped prior
    ({rows_written, distinct_normalized_name, distinct_legal_name_base}) or None."""
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
    gate(metrics["distinct_normalized_name"] >= NORM_FLOOR,
         f"5 norm-distinct floor: {metrics['distinct_normalized_name']:,} >= {NORM_FLOOR:,}")
    gate(metrics["distinct_legal_name_base"] >= BASE_FLOOR,
         f"6 base-distinct floor: {metrics['distinct_legal_name_base']:,} >= {BASE_FLOOR:,}")
    geo_fill = metrics["geo_cofill"] / rows
    gate(geo_fill >= GEO_COFILL_MIN,
         f"7 geo co-fill (name∧state∧zip): {geo_fill:.4%} >= {GEO_COFILL_MIN:.0%}")
    gate(metrics["name_alpha_frac"] >= NAME_ALPHA_MIN,
         f"8 name-alpha: {metrics['name_alpha_frac']:.4%} >= {NAME_ALPHA_MIN:.0%}")
    # per-family Δ-guards (the sensitive check) — rows, norm-distinct, AND base-distinct
    # (the latter is what the retired absolute BASE_DISTINCT_TARGET used to cover).
    if baseline:
        gate(_within(rows, baseline["rows_written"], DELTA_GUARD),
             f"9 rows Δ: {rows:,} within ±{DELTA_GUARD:.0%} of {baseline['rows_written']:,}")
        gate(_within(metrics["distinct_normalized_name"], baseline["distinct_normalized_name"], DELTA_GUARD),
             f"10 norm-distinct Δ: {metrics['distinct_normalized_name']:,} ~ ±{DELTA_GUARD:.0%} of {baseline['distinct_normalized_name']:,}")
        gate(_within(metrics["distinct_legal_name_base"], baseline["distinct_legal_name_base"], DELTA_GUARD),
             f"11 base-distinct Δ: {metrics['distinct_legal_name_base']:,} ~ ±{DELTA_GUARD:.0%} of {baseline['distinct_legal_name_base']:,}")
    else:
        checks.append("SKIP  9-11 Δ-guards: no floor-qualified prior success")
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
    """Latest success at `dataset_uri` clearing BASELINE_MIN_ROWS — the per-family Δ baseline
    ({rows_written, distinct_normalized_name, distinct_legal_name_base}). Floor-qualified (no
    ratchet) and dataset_uri-scoped (no scratch poison)."""
    conn = _pg_connect()
    if conn is None:
        return None
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "SELECT rows_written, distinct_normalized_name, distinct_legal_name_base "
                "FROM ops.sam_normalized_entities_runs "
                "WHERE status='success' AND dataset_uri = %s AND rows_written >= %s "
                "ORDER BY recorded_at DESC LIMIT 1",
                (dataset_uri, BASELINE_MIN_ROWS),
            )
            r = cur.fetchone()
            return None if not r else {
                "rows_written": int(r[0]), "distinct_normalized_name": int(r[1]),
                "distinct_legal_name_base": int(r[2])}
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
                INSERT INTO ops.sam_normalized_entities_runs
                    (feed, dataset_uri, source_uri, sam_extract_label, rows_written,
                     distinct_uei, distinct_normalized_name, distinct_legal_name_base,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (feed, dataset_uri, SRC_URI, sam_label, metrics.get("rows"),
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
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres"),
             modal.Secret.from_name("ops-alerts")],
    timeout=60 * 60,
    memory=32768,
    cpu=8.0,
)
def build_sam_normalized_entities(trigger_callback_url: str | None = None,
                                  dataset_uri: str | None = None,
                                  skip_if_current: bool = True) -> dict:
    """Materialize the sidecar, gate BEFORE the overwrite, then write + index + post-write
    gates under ONE rollback guard (restore prior version on any failure). Idempotent.
    dataset_uri overrides the write target (scratch); skip_if_current no-ops when the sidecar
    already reflects the latest sam_master_entities snapshot (snap-key compared)."""
    import datetime as dt
    import time

    import lance

    from pipelines.sam_gov.reference.sam_labels import snap_key_sql

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    uri = dataset_uri or DATASET_URI
    feed = _feed_for(uri)
    status, error, sam_label = "error", None, None
    metrics = {"rows": 0, "distinct_uei": 0, "distinct_normalized_name": 0,
               "distinct_legal_name_base": 0, "normalized_nonnull": 0, "geo_cofill": 0,
               "name_alpha_frac": 0.0, "probe_uei": None}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)

        # ── skip_if_current (D12, snap-key) — cheap narrow-column label compare before scan ──
        if skip_if_current:
            try:
                con0 = _new_con()
                con0.register("s", lance.dataset(SRC_URI, storage_options=so).scanner(
                    columns=["sam_extract_label"]).to_reader())
                src_key = con0.execute(f"SELECT max({snap_key_sql('sam_extract_label')}) FROM s").fetchone()[0]
                con0.unregister("s")
                cur_key = None
                try:
                    con0.register("c", lance.dataset(uri, storage_options=so).scanner(
                        columns=["sam_extract_label"]).to_reader())
                    cur_key = con0.execute(f"SELECT max({snap_key_sql('sam_extract_label')}) FROM c").fetchone()[0]
                    con0.unregister("c")
                except Exception:  # noqa: BLE001 — net-new/missing target → not current
                    cur_key = None
                con0.close()
                if cur_key is not None and src_key == cur_key:
                    status = "skipped"
                    print(f"skip_if_current: sidecar already at source snap-key {cur_key}")
                    return {"feed": feed, "dataset": uri, "status": "skipped", "sam_label": None}
            except Exception as exc:  # noqa: BLE001 — never let the skip check block a build
                print(f"WARN: skip_if_current check failed ({exc}); proceeding with build.")

        # ── materialize + pre-write gates (on the Arrow table, before any overwrite) ──
        con = _new_con()
        try:
            table, metrics, sam_label = _materialize(con)
        finally:
            con.close()
        src_count = lance.dataset(SRC_URI, storage_options=so).count_rows()
        baseline = _prior_success_baseline(uri)
        printable = {k: v for k, v in metrics.items() if k != "probe_uei"}
        print(f"materialized: {printable} sam_label={sam_label} src_count={src_count:,} baseline={baseline}")
        for line in assert_pre_write_gates(metrics, src_count, baseline):
            print("  ", line)

        # ── rollback target (None for a net-new dataset) ──
        try:
            v_before = lance.dataset(uri, storage_options=so).version
        except Exception:  # noqa: BLE001
            v_before = None
        print(f"v_before = {v_before}")

        # ── write + index + post-write gates, ALL under one rollback guard (D10) ──
        try:
            lance.write_dataset(table, uri, mode="overwrite",
                                data_storage_version=DATA_STORAGE_VERSION,
                                max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
            print(f"wrote dataset (overwrite, v{DATA_STORAGE_VERSION}) → {uri}")
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
                raise RuntimeError(f"write-integrity: committed {committed:,} != materialized {metrics['rows']:,}")
            idx_names = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                         for i in ds.list_indices()}
            expect_idx = {f"{c}_idx" for c in BTREE_INDEXES + BITMAP_INDEXES}
            if not expect_idx.issubset(idx_names):
                raise RuntimeError(f"indices: missing {sorted(expect_idx - idx_names)} (have {sorted(idx_names)})")
            pr = metrics["probe_uei"]
            rt = ds.scanner(columns=["uei", "normalized_legal_name"],
                            filter=f"uei = '{pr}'").to_table().to_pylist()
            probe_name = next((r["normalized_legal_name"] for r in rt if r["normalized_legal_name"]), None)
            if not probe_name:
                raise RuntimeError(f"round-trip: probe {pr} → {len(rt)} rows / no normalized_legal_name")
            t0 = time.monotonic()
            hit = ds.scanner(columns=["uei"],
                             filter=f"normalized_legal_name = '{probe_name.replace(chr(39), chr(39) * 2)}'").to_table().num_rows
            seek_ms = (time.monotonic() - t0) * 1000
            if hit < 1:  # D1 — correctness only; cold-seek latency is logged, NOT gated
                raise RuntimeError("name index: lookup of a known-present name returned 0 rows")
            _slow = "" if seek_ms <= SEEK_WARN_MS else f"  [WARN cold seek >{SEEK_WARN_MS}ms]"
            print(f"post-write gates PASS — committed={committed:,} indices={sorted(idx_names)} "
                  f"probe={pr} seek={seek_ms:.0f}ms{_slow}")
        except Exception as gate_exc:  # noqa: BLE001
            if v_before is not None:
                try:
                    lance.dataset(uri, storage_options=so, version=v_before).restore()
                except Exception as rerr:  # noqa: BLE001
                    raise RuntimeError(f"ROLLBACK FAILED to v{v_before}: {rerr}; original: {gate_exc}")
                raise RuntimeError(f"write/index/gate failed → rolled back to v{v_before}: {gate_exc}")
            raise RuntimeError(f"write/index/gate failed on net-new dataset (inspect/drop {uri}): {gate_exc}")

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
                        "distinct_uei": metrics["distinct_uei"]})

    if status != "success":
        alert(f"[sam_normalized_entities] {feed} build {status}: {str(error)[:300]}")
        raise RuntimeError(f"sam_normalized_entities build failed: {error}")
    return {"feed": feed, "dataset": uri, "sam_label": sam_label,
            "rows": metrics["rows"], "distinct_uei": metrics["distinct_uei"],
            "distinct_normalized_name": metrics["distinct_normalized_name"],
            "distinct_legal_name_base": metrics["distinct_legal_name_base"]}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=900)
def verify_sam_normalized_entities(dataset_uri: str | None = None) -> dict:
    """Read-back proof from R2 — independent of the write path. Reports counts, schema,
    indices, a sample, and the §B8 observability (legal_name_base collision rate; CO-peel)."""
    import duckdb
    import lance

    so = _r2_storage_options()
    uri = dataset_uri or DATASET_URI
    ds = lance.dataset(uri, storage_options=so)
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
        "uri": uri, "rows": rows, "distinct_uei": d_uei,
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
def plan_sam_normalized_entities(dataset_uri: str | None = None) -> dict:
    """Dry-run gate — materialize + run the SAME pre-write gates (incl. the floor-qualified
    Δ baseline scoped to the effective URI), write NOTHING."""
    import lance

    os.makedirs(SPILL_DIR, exist_ok=True)
    so = _r2_storage_options()
    uri = dataset_uri or DATASET_URI
    con = _new_con()
    try:
        _table, metrics, sam_label = _materialize(con)
    finally:
        con.close()
    src_count = lance.dataset(SRC_URI, storage_options=so).count_rows()
    baseline = _prior_success_baseline(uri)
    checks = assert_pre_write_gates(metrics, src_count, baseline)
    out = {k: v for k, v in metrics.items() if k != "probe_uei"}
    return {"feed": _feed_for(uri), "sam_label": sam_label, "src_count": src_count,
            "baseline": baseline, "gates": checks, **out}


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    import json

    uri = os.environ.get("SAM_NORMALIZED_ENTITIES_URI")  # None → prod
    if dry_run:
        print(json.dumps(plan_sam_normalized_entities.remote(dataset_uri=uri), indent=2, default=str))
        return
    # skip_if_current=False: a manual run always rebuilds (the orchestrator path uses the default True).
    print(json.dumps(build_sam_normalized_entities.remote(
        trigger_callback_url=None, dataset_uri=uri, skip_if_current=False), indent=2, default=str))
    print(json.dumps(verify_sam_normalized_entities.remote(dataset_uri=uri), indent=2, default=str))
