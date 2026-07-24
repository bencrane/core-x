"""Compute worker — SoS → SAM (name → UEI) crosswalk.

The ``resolution-sos-sam-pipelines`` Modal app. Attaches a federal **UEI** (from
``sam_normalized_entities``) to every Secretary-of-State registrant in
``sos_normalized_master`` that name-matches a SAM entity — the first real consumer of the
name→UEI sidecar, and the only SoS-registration → federal-UEI path in the fleet.

Plan of record (v9-canonical): docs/plans/CROSSWALK_SOS_SAM_BUILD_PLAN.md (+ its adversarial
review, folded). Build MUST read the remediated v9 SoS master.

Source-of-truth inputs (read-only; never mutated):
  s3://data-sink/active/sos_normalized_master/   (Lance v9, 17.93M rows; current-macro
    normalized_legal_name + MATERIALIZED BTREE legal_name_base; geo: source_state=JURISDICTION,
    state/zip_code=PHYSICAL). Entity key = (source_state, original_entity_id).
  s3://data-sink/active/sam_normalized_entities/  (Lance v7, 1.54M rows, 1/uei; BTREE
    normalized_legal_name + legal_name_base; physical source_state/zip_code; is_active).

Match design (plan §4): join on the MATERIALIZED legal_name_base column BOTH sides (the
superset — equal-normalized ⟹ equal-base), label match_key by exact-normalized equality, score
a 5-tier name+geo ladder (1=exact+state+zip … 5=base-no-geo), and mark exactly one is_canonical
per SoS entity over tiers 1–4 (tier 5 is recall-only, never canonical). Geo is the PHYSICAL
locus (sos.state ↔ sam.source_state), a score never a gate. The base join reads the v9
materialized columns — core.name_norm is NOT called to recompute a base (the v4 mechanic).

Data plane (clean-room — DuckDB does 100% of the transform):
  Lance(sos, sam) → DuckDB join + 5-tier score + canonical window → Arrow → [pre-write gates on
  the Arrow table, §7 1-7+5b] → lance.write_dataset(R2 active, v2.1, overwrite) → BTREE(uei,
  sos_entity_key, sos_normalized_legal_name) + BITMAP(match_tier, is_canonical, match_key) →
  [post-write gates 8-10; restore-to-v_before on failure]. LANCE_BYPASS_SPILLING=true for the
  high-cardinality index sort (lance#2650).

Control plane: on terminal state writes ops.crosswalk_sos_sam_runs and POSTs the flat callback.

Launch (spawn-on-deployed): the entrypoint deploy-if-stale gates via pipelines._shared.launch
(spawn executes the DEPLOYED snapshot, never worktree code), spawns on the deployed app,
prints the fc-id, and exits — the run survives client death. The entrypoint no longer prints
the result JSON; the worker's ops.crosswalk_sos_sam_runs row and app logs are the record.

    modal deploy pipelines/resolution/crosswalk_sos_sam.py             # publish snapshot
    modal run    pipelines/resolution/crosswalk_sos_sam.py::init_ops   # create ops table (sync idempotent DDL, seconds)
    modal run    pipelines/resolution/crosswalk_sos_sam.py --dry-run   # spawn plan_crosswalk: gates 1-7+5b, no write
    modal run    pipelines/resolution/crosswalk_sos_sam.py             # PHASE 1: spawn build_crosswalk

Record the printed fc-id. Follow: ``modal app logs resolution-sos-sam-pipelines``; result:
``modal.FunctionCall.from_id("fc-...").get(timeout=0)`` (TimeoutError while running); ledger:
SELECT status FROM ops.crosswalk_sos_sam_runs ORDER BY recorded_at DESC LIMIT 1.
PHASE 2 — spawn verify_crosswalk ONLY after phase-1 terminal SUCCESS (a spawned build that
fails no longer auto-suppresses verify via exception propagation; this gate replaces it):
``modal.Function.from_name("resolution-sos-sam-pipelines", "verify_crosswalk").spawn()``, then
fetch its dict via FunctionCall.get(). verify_crosswalk and plan_crosswalk write no ledger
rows — their results live only in the function-call result/logs, so fetch promptly.
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
SOS_URI = "s3://data-sink/active/sos_normalized_master/"
SAM_URI = "s3://data-sink/active/sam_normalized_entities/"
DATASET_URI = os.environ.get("CROSSWALK_SOS_SAM_URI", "s3://data-sink/active/crosswalk_sos_sam/")
FEED = "crosswalk_sos_sam"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576

# Forward (uei) + reverse (sos_entity_key) + audit (sos_normalized_legal_name) lookups.
BTREE_INDEXES = ["uei", "sos_entity_key", "sos_normalized_legal_name"]
BITMAP_INDEXES = ["match_tier", "is_canonical", "match_key"]

# The 17.9M-row SoS scan + dedup window is the heavy input; spill mandatory.
DUCKDB_MEMORY_LIMIT = "24GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

# ── §7 gate baselines (v9 live, 2026-06-06) ──────────────────────────────────
ROW_FLOOR = 500_000
COVERAGE_UEI = 443_473          # all-tier union distinct UEI
COVERAGE_SOS = 742_795          # all-tier distinct SoS entities
COVERAGE_TOL = 0.10             # ±10%
STATE_CONFIRM_BAND = (0.41, 0.51)   # exact-name state_confirms rate (C1 fix; v9 46.03%)
TIER1_FLOOR = 120_000           # exact+state+zip pairs (v9 ~207k)
TIER5_SHARE_MAX = 0.40          # tier-5 (base-no-geo) share of output (v9 33.2%)

# Source scan projections.
SOS_COLS = ["source_state", "original_entity_id", "normalized_legal_name", "legal_name_base",
            "source_entity_name", "state", "zip_code", "entity_status", "snapshot_date"]
SAM_COLS = ["uei", "normalized_legal_name", "legal_name_base", "legal_business_name",
            "source_state", "zip_code", "is_active", "primary_naics", "sam_extract_label"]

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.crosswalk_sos_sam_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,
    dataset_uri         text        NOT NULL,
    rows_written        bigint,
    canonical_rows      bigint,
    distinct_uei        bigint,
    distinct_sos_entity bigint,
    tier1_rows          bigint,
    tier5_rows          bigint,
    status              text        NOT NULL,
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crosswalk_sos_sam_runs_feed_idx        ON ops.crosswalk_sos_sam_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_sos_sam_runs_status_idx      ON ops.crosswalk_sos_sam_runs (status);
CREATE INDEX IF NOT EXISTS crosswalk_sos_sam_runs_recorded_at_idx ON ops.crosswalk_sos_sam_runs (recorded_at DESC);
"""

# add_local_python_source("core.name_norm") kept for skeleton parity (plan §6); the base join
# reads the materialized v9 columns, so the transform makes NO macro call.
image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"}).add_local_python_source("core.name_norm")

app = modal.App("resolution-sos-sam-pipelines", image=image)


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
# DuckDB transform — pure SQL builder (plan §5; no core.name_norm call)
# --------------------------------------------------------------------------- #
def build_crosswalk_sql() -> str:
    """Join sos (1 row/entity, materialized v9 legal_name_base) to sam on the materialized
    legal_name_base column both sides; label, score (5-tier), rank (recency+status), mark
    canonical over tiers 1-4. Reads `sos_src` and `sam_src` relations (the scanned layers)."""
    return """
    WITH sos AS (
        SELECT
            source_state || ':' || original_entity_id        AS sos_entity_key,
            source_state                                     AS sos_source_state,
            original_entity_id                               AS sos_original_entity_id,
            normalized_legal_name                            AS sos_normalized_legal_name,
            source_entity_name                               AS sos_source_entity_name,
            state                                            AS sos_state,
            left(nullif(trim(zip_code), ''), 5)              AS sos_zip_code,
            entity_status                                    AS sos_entity_status,
            (entity_status IN ('ACTIVE','GOOD STANDING'))    AS status_is_active,
            legal_name_base                                  AS sos_legal_name_base
        FROM sos_src
        WHERE normalized_legal_name IS NOT NULL
          AND nullif(trim(source_state), '') IS NOT NULL          -- key components must exist:
          AND nullif(trim(original_entity_id), '') IS NOT NULL    -- sos_entity_key is unusable if null (gate 3)
        QUALIFY row_number() OVER (
            PARTITION BY source_state, original_entity_id
            ORDER BY snapshot_date DESC NULLS LAST) = 1
    ),
    pairs AS (
        SELECT
            s.sos_entity_key, s.sos_source_state, s.sos_original_entity_id,
            s.sos_normalized_legal_name, s.sos_source_entity_name, s.sos_state,
            s.sos_zip_code, s.sos_entity_status, s.status_is_active,
            m.uei, m.normalized_legal_name AS sam_normalized_legal_name,
            m.legal_business_name AS sam_legal_business_name,
            m.source_state AS sam_source_state, m.zip_code AS sam_zip_code,
            m.is_active AS sam_is_active, m.primary_naics AS sam_primary_naics,
            m.sam_extract_label,
            (m.normalized_legal_name = s.sos_normalized_legal_name) AS exact_name,
            (upper(trim(s.sos_state)) = upper(trim(m.source_state))) AS state_confirms,
            (s.sos_zip_code IS NOT NULL
             AND s.sos_zip_code = left(nullif(trim(m.zip_code), ''), 5)) AS zip_confirms
        FROM sos s
        JOIN sam_src m ON m.legal_name_base = s.sos_legal_name_base
    ),
    scored AS (
        SELECT *,
            CASE WHEN exact_name THEN 'normalized_legal_name' ELSE 'legal_name_base' END AS match_key,
            CASE WHEN exact_name AND state_confirms AND zip_confirms THEN 1
                 WHEN exact_name AND state_confirms                  THEN 2
                 WHEN exact_name                                     THEN 3
                 WHEN state_confirms                                 THEN 4
                 ELSE 5 END                                       AS match_tier
        FROM pairs
    ),
    ranked AS (
        SELECT *,
            row_number() OVER (
                PARTITION BY sos_entity_key
                ORDER BY match_tier ASC, state_confirms DESC, zip_confirms DESC,
                         status_is_active DESC, sam_is_active DESC,
                         sam_extract_label DESC, uei ASC)             AS rn
        FROM scored
    )
    SELECT
        * EXCLUDE (exact_name, rn),
        CASE match_tier WHEN 1 THEN 'high' WHEN 2 THEN 'medium_high'
                        WHEN 3 THEN 'medium' WHEN 4 THEN 'low'
                        ELSE 'unsafe' END                          AS match_confidence,
        (rn = 1 AND match_tier <= 4)                               AS is_canonical,
        'crosswalk_sos_sam'                                        AS source_dataset
    FROM ranked
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
    """Register both layers, run the join/score/rank, return (arrow_table, metrics).
    metrics carry everything gates 1-7+5b need."""
    import lance

    so = _r2_storage_options()
    con.register("sos_src", lance.dataset(SOS_URI, storage_options=so).scanner(columns=SOS_COLS).to_reader())
    con.register("sam_src", lance.dataset(SAM_URI, storage_options=so).scanner(columns=SAM_COLS).to_reader())
    con.execute(f"CREATE TEMP TABLE xwalk AS {build_crosswalk_sql()}")
    con.unregister("sos_src")
    con.unregister("sam_src")

    row = con.execute("""
        SELECT
            count(*)                                                          AS rows,
            count(*) FILTER (WHERE is_canonical)                             AS canonical_rows,
            count(DISTINCT uei)                                              AS distinct_uei,
            count(DISTINCT sos_entity_key)                                   AS distinct_sos_entity,
            count(DISTINCT sos_entity_key) FILTER (WHERE match_tier <= 4)    AS distinct_sos_tier14,
            count(*) FILTER (WHERE match_tier = 1)                           AS tier1_rows,
            count(*) FILTER (WHERE match_tier = 5)                           AS tier5_rows,
            count(*) FILTER (WHERE uei IS NULL OR sos_entity_key IS NULL)    AS orphans,
            count(*) FILTER (WHERE match_key = 'normalized_legal_name')      AS exact_pairs,
            count(*) FILTER (WHERE match_key = 'normalized_legal_name' AND state_confirms) AS exact_state,
            count(*) FILTER (WHERE is_canonical AND match_tier = 5)          AS tier5_canonical,
            count(*) FILTER (WHERE match_tier NOT BETWEEN 1 AND 5)           AS tier_violations,
            count(*) FILTER (WHERE (match_tier <= 3) <> (match_key = 'normalized_legal_name')) AS key_violations
        FROM xwalk
    """).fetchone()
    keys = ["rows", "canonical_rows", "distinct_uei", "distinct_sos_entity", "distinct_sos_tier14",
            "tier1_rows", "tier5_rows", "orphans", "exact_pairs", "exact_state",
            "tier5_canonical", "tier_violations", "key_violations"]
    metrics = {k: int(v) for k, v in zip(keys, row)}
    table = con.sql("SELECT * FROM xwalk").to_arrow_table()
    return table, metrics


# --------------------------------------------------------------------------- #
# §7 gates
# --------------------------------------------------------------------------- #
def _within(value: int, target: int, tol: float) -> bool:
    return abs(value - target) <= target * tol


def assert_pre_write_gates(m: dict, prior_rows: int | None) -> list[str]:
    """Gates 1-7 + 5b on the in-memory metrics. Raises on first hard failure; returns the log."""
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    rows = m["rows"]
    gate(rows >= ROW_FLOOR, f"1 row floor: {rows:,} >= {ROW_FLOOR:,}")
    gate(m["canonical_rows"] == m["distinct_sos_tier14"],
         f"2 canonical uniqueness: is_canonical {m['canonical_rows']:,} == distinct sos w/ tier1-4 {m['distinct_sos_tier14']:,}")
    gate(m["orphans"] == 0, f"3 no orphans: {m['orphans']} null uei/sos_entity_key")
    gate(_within(m["distinct_uei"], COVERAGE_UEI, COVERAGE_TOL),
         f"4a coverage uei: {m['distinct_uei']:,} within ±{COVERAGE_TOL:.0%} of {COVERAGE_UEI:,}")
    gate(_within(m["distinct_sos_entity"], COVERAGE_SOS, COVERAGE_TOL),
         f"4b coverage sos: {m['distinct_sos_entity']:,} within ±{COVERAGE_TOL:.0%} of {COVERAGE_SOS:,}")
    rate = m["exact_state"] / m["exact_pairs"] if m["exact_pairs"] else 0.0
    lo, hi = STATE_CONFIRM_BAND
    gate(lo <= rate <= hi,
         f"5 locus guard: exact-name state_confirms {rate:.4f} in [{lo},{hi}] (v9 ~0.46)")
    gate(m["tier1_rows"] >= TIER1_FLOOR,
         f"5 tier1 floor: {m['tier1_rows']:,} >= {TIER1_FLOOR:,} (exact+state+zip)")
    gate(m["tier5_canonical"] == 0,
         f"5b tier-5 never canonical: {m['tier5_canonical']} canonical tier-5 rows")
    gate(m["tier5_rows"] / rows <= TIER5_SHARE_MAX,
         f"5b tier-5 share: {m['tier5_rows']/rows:.4f} <= {TIER5_SHARE_MAX} (v9 ~0.33)")
    gate(m["tier_violations"] == 0 and m["key_violations"] == 0,
         f"6 tier monotonicity: {m['tier_violations']} tier + {m['key_violations']} key violations")
    if prior_rows:
        gate(_within(rows, prior_rows, 0.25), f"7 Δ-guard: {rows:,} within ±25% of prior {prior_rows:,}")
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
    conn = _pg_connect()
    if conn is None:
        return None
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute("SELECT rows_written FROM ops.crosswalk_sos_sam_runs "
                        "WHERE status='success' AND rows_written IS NOT NULL "
                        "ORDER BY recorded_at DESC LIMIT 1")
            r = cur.fetchone()
            return int(r[0]) if r and r[0] is not None else None
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: prior-rows lookup failed: {exc}")
        return None
    finally:
        conn.close()


def _record_run(*, metrics, status, error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.crosswalk_sos_sam_runs
                    (feed, dataset_uri, rows_written, canonical_rows, distinct_uei,
                     distinct_sos_entity, tier1_rows, tier5_rows, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, metrics.get("rows"), metrics.get("canonical_rows"),
                 metrics.get("distinct_uei"), metrics.get("distinct_sos_entity"),
                 metrics.get("tier1_rows"), metrics.get("tier5_rows"),
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
    memory=49152,
    cpu=8.0,
)
def build_crosswalk(trigger_callback_url: str | None = None) -> dict:
    """Materialize → pre-write gates → write + index → post-write gates (restore on failure)."""
    import datetime as dt
    import time

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    metrics = {"rows": 0, "canonical_rows": 0, "distinct_uei": 0, "distinct_sos_entity": 0,
               "tier1_rows": 0, "tier5_rows": 0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics = _materialize(con)
        finally:
            con.close()
        prior_rows = _prior_success_rows()
        print(f"materialized: {metrics}")
        for line in assert_pre_write_gates(metrics, prior_rows):
            print("  ", line)

        try:
            v_before = lance.dataset(DATASET_URI, storage_options=so).version
        except Exception:
            v_before = None
        print(f"v_before = {v_before}")

        lance.write_dataset(table, DATASET_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
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
                raise RuntimeError(f"gate 8 indices: missing {sorted(expect_idx - idx_names)} (have {sorted(idx_names)})")
            t1 = ds.scanner(columns=["uei", "sos_entity_key", "match_tier", "is_canonical"],
                            filter="match_tier = 1 AND is_canonical").to_table().slice(0, 1).to_pylist()
            if not t1:
                raise RuntimeError("gate 9 round-trip: no tier-1 canonical row found")
            probe_uei, probe_key = t1[0]["uei"], t1[0]["sos_entity_key"]
            sam_hit = lance.dataset(SAM_URI, storage_options=so).scanner(
                columns=["uei"], filter=f"uei = '{probe_uei}'").to_table().num_rows
            ss, oid = probe_key.split(":", 1)
            sos_hit = lance.dataset(SOS_URI, storage_options=so).scanner(
                columns=["original_entity_id"],
                filter=f"source_state = '{ss}' AND original_entity_id = '{oid}'").to_table().num_rows
            if sam_hit != 1 or sos_hit < 1:
                raise RuntimeError(f"gate 9 round-trip: uei={probe_uei}→sam {sam_hit}, key={probe_key}→sos {sos_hit}")
            t0 = time.monotonic()
            n_cand = ds.scanner(columns=["sos_entity_key"], filter=f"uei = '{probe_uei}'").to_table().num_rows
            seek_ms = (time.monotonic() - t0) * 1000
            if n_cand < 1 or seek_ms > 2000:
                raise RuntimeError(f"gate 10 point-lookup: {n_cand} rows in {seek_ms:.0f}ms (>2000ms ⇒ no index)")
            print(f"post-write gates PASS — committed={committed:,} indices={sorted(idx_names)} "
                  f"round-trip uei={probe_uei} (sam {sam_hit}, sos {sos_hit}) seek={seek_ms:.0f}ms/{n_cand} rows "
                  f"[target <100ms warm; <2000ms R2 ceiling]")
        except Exception as gate_exc:  # noqa: BLE001
            if v_before is not None:
                lance.dataset(DATASET_URI, storage_options=so, version=v_before).restore()
                raise RuntimeError(f"post-write gate failed → rolled back to v{v_before}: {gate_exc}")
            raise RuntimeError(f"post-write gate failed on net-new dataset (inspect/drop {DATASET_URI}): {gate_exc}")

        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(metrics=metrics, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": metrics["rows"], "feed": FEED,
                        "dataset_uri": DATASET_URI, "distinct_uei": metrics["distinct_uei"],
                        "canonical_rows": metrics["canonical_rows"]})

    if status != "success":
        raise RuntimeError(f"crosswalk_sos_sam build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, **metrics}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=900)
def verify_crosswalk() -> dict:
    """Read-back proof from R2 — independent of the write path. Counts, 5-tier distribution,
    canonical reach, observability (status/recency tiebreak, top fan-out)."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = sorted((i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                 for i in ds.list_indices())
    con = duckdb.connect()
    con.register("d", ds.scanner(columns=[
        "uei", "sos_entity_key", "match_tier", "match_confidence", "is_canonical",
        "state_confirms", "zip_confirms", "status_is_active", "match_key"]).to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    con.unregister("d")
    rows, d_uei, d_sos, canon = con.execute(
        "SELECT count(*), count(DISTINCT uei), count(DISTINCT sos_entity_key), "
        "count(*) FILTER (WHERE is_canonical) FROM d2").fetchone()
    canon_uei = con.execute("SELECT count(DISTINCT uei) FILTER (WHERE is_canonical) FROM d2").fetchone()[0]
    by_tier = dict(con.execute("SELECT match_tier, count(*) FROM d2 GROUP BY 1 ORDER BY 1").fetchall())
    canon_by_conf = dict(con.execute(
        "SELECT match_confidence, count(*) FROM d2 WHERE is_canonical GROUP BY 1").fetchall())
    nonactive_canon = con.execute(
        "SELECT count(*) FILTER (WHERE is_canonical AND NOT status_is_active) FROM d2").fetchone()[0]
    con.close()
    sample = ds.scanner(columns=[
        "sos_entity_key", "sos_normalized_legal_name", "uei", "sam_legal_business_name",
        "match_tier", "match_confidence", "state_confirms", "zip_confirms", "is_canonical"],
        filter="is_canonical", limit=6).to_table().to_pylist()
    return {"uri": DATASET_URI, "rows": rows, "distinct_uei": d_uei, "distinct_sos_entity": d_sos,
            "canonical_rows": canon, "canonical_uei_reach": canon_uei, "indices": idx,
            "by_match_tier": by_tier, "canonical_by_confidence": canon_by_conf,
            "nonactive_canonical": nonactive_canon,
            "schema": [f"{f.name}:{f.type}" for f in ds.schema], "sample": sample}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.crosswalk_sos_sam_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.crosswalk_sos_sam_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 45, memory=49152, cpu=8.0,
)
def plan_crosswalk() -> dict:
    """Dry-run gate — materialize + run gates 1-7+5b, write NOTHING."""
    os.makedirs(SPILL_DIR, exist_ok=True)
    con = _new_con()
    try:
        _table, metrics = _materialize(con)
    finally:
        con.close()
    prior_rows = _prior_success_rows()
    checks = assert_pre_write_gates(metrics, prior_rows)
    return {"feed": FEED, "prior_rows": prior_rows, "gates": checks, **metrics}


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    from pipelines._shared.launch import spawn_deployed

    if dry_run:
        print("PHASE plan (dry-run): spawning plan_crosswalk — gates 1-7+5b, no write")
        spawn_deployed("resolution-sos-sam-pipelines", "plan_crosswalk", deploy_file=__file__)
        return
    print("PHASE 1: spawning build_crosswalk — spawn verify_crosswalk only after terminal "
          "success (module docstring, PHASE 2)")
    spawn_deployed("resolution-sos-sam-pipelines", "build_crosswalk", deploy_file=__file__,
                   trigger_callback_url=None)
