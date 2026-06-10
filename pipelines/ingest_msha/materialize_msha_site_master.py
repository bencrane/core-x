"""Compute worker — msha_site_master (Phase 5 of the MSHA ID spine execution plan).

Part of the ``msha-site-master`` Modal app. A DERIVED dataset: it reads the active MSHA Lance
spine datasets (NOT a landing zip), joins them in DuckDB, and writes one deterministic row per
MINE_ID — the market-map anchor. Same clean-room write path as the curated ingest workers
(DuckDB transform → lance.write_dataset overwrite → scalar indices → ops ledger).

WHAT IT BUILDS
    s3://data-sink/active/msha_site_master/   (1 row per MINE_ID == msha_mines grain, 91,803)

    Per mine: descriptive attrs (status, geo, commodity, address) + the SCD-resolved current
    controller / operator + pre-computed GTM signal rollups. The SCD non-determinism (a mine
    with >1 open controller window) is resolved by the plan's R1 decision = **Option D**:

      current controller = the controller with the LATEST CONTROLLER_START_DT among open
      (CONTROLLER_END_DT IS NULL) windows; if >1 distinct controller shares that latest start
      (a genuine same-day tie, 778 mines) → NULL (zero fabrication). `multi_controller_flag`
      is true for any mine that had >1 distinct open controller (so a consumer can tell a
      recency-disambiguated pick from a clean single one, and a NULL tie from a no-window NULL).

    Operator resolution is identical over OPERATOR_*_DT. Contractor is intentionally NOT carried
    (contractor↔mine is M:N — that belongs to the contractor-grain fast-follow, not the 1:1 anchor).
    MSHA's own denormalized current IDs are carried verbatim as MSHA_REPORTED_* for audit.

    modal run    pipelines/ingest_msha/materialize_msha_site_master.py::run        # build
    modal run    pipelines/ingest_msha/materialize_msha_site_master.py::verify     # read-back proof
    modal run    pipelines/ingest_msha/materialize_msha_site_master.py::reindex_only
    modal deploy pipelines/ingest_msha/materialize_msha_site_master.py
"""

from __future__ import annotations

import os

import modal

FEED = "msha"
_ACTIVE = "s3://data-sink/active"
URI = os.environ.get("MSHA_SITE_MASTER_URI", f"{_ACTIVE}/msha_site_master/")
DATA_STORAGE_VERSION = "2.1"

INDEX_PLAN = {
    "BTREE": ["MINE_ID", "CURRENT_CONTROLLER_ID", "CURRENT_OPERATOR_ID",
              "MSHA_REPORTED_CONTROLLER_ID", "CURRENT_CONTROLLER_NAME"],
    "BITMAP": ["CURRENT_MINE_STATUS", "COAL_METAL_IND", "STATE",
               "multi_controller_flag", "multi_operator_flag", "silica_overexposure"],
}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.msha_ingest_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,
    source_bucket    text        NOT NULL,
    source_prefix    text        NOT NULL,
    datasets         jsonb       NOT NULL,
    rows_total       bigint      NOT NULL DEFAULT 0,
    bytes_downloaded bigint      NOT NULL DEFAULT 0,
    status           text        NOT NULL,
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS msha_ingest_runs_feed_idx        ON ops.msha_ingest_runs (feed);
CREATE INDEX IF NOT EXISTS msha_ingest_runs_status_idx      ON ops.msha_ingest_runs (status);
CREATE INDEX IF NOT EXISTS msha_ingest_runs_recorded_at_idx ON ops.msha_ingest_runs (recorded_at DESC);
"""

# The assembly. Spine = msha_mines (the grain anchor). Current controller/operator via the
# Option-D SCD resolver; rollups LEFT-JOINed per MINE_ID. Every LEFT join is N:1 → grain holds.
BUILD_SQL = """
WITH
-- ── Option-D current-controller resolver ───────────────────────────────────
open_ctrl AS (
  SELECT MINE_ID, CONTROLLER_ID, CONTROLLER_NAME, CONTROLLER_START_DT
  FROM ch WHERE CONTROLLER_END_DT IS NULL),
mx_ctrl AS (
  SELECT MINE_ID, max(CONTROLLER_START_DT) AS mx, count(DISTINCT CONTROLLER_ID) AS n_open
  FROM open_ctrl GROUP BY MINE_ID),
ctrl_latest AS (
  SELECT o.MINE_ID, count(DISTINCT o.CONTROLLER_ID) AS n_at_latest,
         any_value(o.CONTROLLER_ID) AS pid, any_value(o.CONTROLLER_NAME) AS pnm
  FROM open_ctrl o JOIN mx_ctrl m ON o.MINE_ID = m.MINE_ID AND o.CONTROLLER_START_DT = m.mx
  GROUP BY o.MINE_ID),
controller AS (
  SELECT m.MINE_ID,
         CASE WHEN l.n_at_latest = 1 THEN l.pid END AS CURRENT_CONTROLLER_ID,
         CASE WHEN l.n_at_latest = 1 THEN l.pnm END AS CURRENT_CONTROLLER_NAME,
         (m.n_open > 1) AS multi_controller_flag
  FROM mx_ctrl m JOIN ctrl_latest l USING (MINE_ID)),
-- ── Option-D current-operator resolver (identical predicate) ────────────────
open_op AS (
  SELECT MINE_ID, OPERATOR_ID, OPERATOR_NAME, OPERATOR_START_DT
  FROM ch WHERE OPERATOR_END_DT IS NULL),
mx_op AS (
  SELECT MINE_ID, max(OPERATOR_START_DT) AS mx, count(DISTINCT OPERATOR_ID) AS n_open
  FROM open_op GROUP BY MINE_ID),
op_latest AS (
  SELECT o.MINE_ID, count(DISTINCT o.OPERATOR_ID) AS n_at_latest,
         any_value(o.OPERATOR_ID) AS pid, any_value(o.OPERATOR_NAME) AS pnm
  FROM open_op o JOIN mx_op m ON o.MINE_ID = m.MINE_ID AND o.OPERATOR_START_DT = m.mx
  GROUP BY o.MINE_ID),
oper AS (
  SELECT m.MINE_ID,
         CASE WHEN l.n_at_latest = 1 THEN l.pid END AS CURRENT_OPERATOR_ID,
         CASE WHEN l.n_at_latest = 1 THEN l.pnm END AS CURRENT_OPERATOR_NAME,
         (m.n_open > 1) AS multi_operator_flag
  FROM mx_op m JOIN op_latest l USING (MINE_ID)),
-- ── GTM signal rollups (typed once here; mirrors stay passthrough) ──────────
enf_roll AS (
  SELECT MINE_ID,
         count(*) AS violation_count,
         count(*) FILTER (WHERE SIG_SUB = 'Y') AS ss_count,
         count(*) FILTER (WHERE SIG_SUB = 'Y' AND VIOLATION_ISSUE_DT >= DATE '2025-01-01') AS ss_count_since_2025,
         count(*) FILTER (WHERE CIT_ORD_SAFE = 'Order') AS order_count,
         round(sum(PROPOSED_PENALTY_AMT)) AS proposed_penalty_sum,
         max(VIOLATION_ISSUE_DT) AS last_violation_dt
  FROM enf GROUP BY MINE_ID),
acc_roll AS (
  SELECT MINE_ID,
         count(*) AS accident_count,
         count(*) FILTER (WHERE DEGREE_INJURY_CD = '01') AS fatality_count,
         max(ACCIDENT_DT) AS last_accident_dt
  FROM acc GROUP BY MINE_ID),
qz_roll AS (
  SELECT MINE_ID, (max(try_cast(QUARTZ_PCT AS DOUBLE)) > 5) AS silica_overexposure
  FROM qz GROUP BY MINE_ID)
SELECT
  m.MINE_ID, m.CURRENT_MINE_NAME, m.CURRENT_MINE_STATUS, m.CURRENT_MINE_TYPE,
  m.COAL_METAL_IND, m.PRIMARY_SIC, m.PRIMARY_CANVASS, m.STATE, m.FIPS_CNTY_NM,
  m.LATITUDE, m.LONGITUDE, m.NO_EMPLOYEES, m.CITY, m.STATE_ABBR, m.ZIP_CD,
  -- MSHA's own denormalized current pick (audit / fallback)
  m.CURRENT_CONTROLLER_ID AS MSHA_REPORTED_CONTROLLER_ID,
  m.CURRENT_OPERATOR_ID   AS MSHA_REPORTED_OPERATOR_ID,
  -- deterministic SCD-resolved current entity (Option D: NULL on genuine tie)
  c.CURRENT_CONTROLLER_ID, c.CURRENT_CONTROLLER_NAME, coalesce(c.multi_controller_flag, false) AS multi_controller_flag,
  o.CURRENT_OPERATOR_ID,   o.CURRENT_OPERATOR_NAME,   coalesce(o.multi_operator_flag, false)   AS multi_operator_flag,
  -- GTM signal rollups
  coalesce(e.violation_count, 0)       AS violation_count,
  coalesce(e.ss_count, 0)              AS ss_count,
  coalesce(e.ss_count_since_2025, 0)   AS ss_count_since_2025,
  coalesce(e.order_count, 0)           AS order_count,
  coalesce(e.proposed_penalty_sum, 0)  AS proposed_penalty_sum,
  e.last_violation_dt,
  coalesce(a.accident_count, 0)        AS accident_count,
  coalesce(a.fatality_count, 0)        AS fatality_count,
  a.last_accident_dt,
  coalesce(q.silica_overexposure, false) AS silica_overexposure,
  'msha_site_master (derived)' AS source_file, now() AS ingested_at
FROM mines m
LEFT JOIN controller c USING (MINE_ID)
LEFT JOIN oper o       USING (MINE_ID)
LEFT JOIN enf_roll e   USING (MINE_ID)
LEFT JOIN acc_roll a   USING (MINE_ID)
LEFT JOIN qz_roll q    USING (MINE_ID)
"""

# Source projections: (active dataset, alias, [columns]).
SOURCES = [
    ("msha_mines", "mines", ["MINE_ID", "CURRENT_MINE_NAME", "CURRENT_MINE_STATUS", "CURRENT_MINE_TYPE",
                             "COAL_METAL_IND", "PRIMARY_SIC", "PRIMARY_CANVASS", "STATE", "FIPS_CNTY_NM",
                             "LATITUDE", "LONGITUDE", "NO_EMPLOYEES", "CITY", "STATE_ABBR", "ZIP_CD",
                             "CURRENT_CONTROLLER_ID", "CURRENT_OPERATOR_ID"]),
    ("msha_corporate_history", "ch", ["MINE_ID", "CONTROLLER_ID", "CONTROLLER_NAME", "CONTROLLER_START_DT",
                                      "CONTROLLER_END_DT", "OPERATOR_ID", "OPERATOR_NAME",
                                      "OPERATOR_START_DT", "OPERATOR_END_DT"]),
    ("msha_enforcement_ledger", "enf", ["MINE_ID", "SIG_SUB", "CIT_ORD_SAFE", "VIOLATION_ISSUE_DT",
                                        "PROPOSED_PENALTY_AMT"]),
    ("msha_accidents", "acc", ["MINE_ID", "DEGREE_INJURY_CD", "ACCIDENT_DT"]),
    ("msha_quartz_samples", "qz", ["MINE_ID", "QUARTZ_PCT"]),
]

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "boto3>=1.35", "requests>=2.32", "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})
app = modal.App("msha-site-master", image=image)


def _so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _build_indexes(uri: str, so: dict) -> list[dict]:
    import lance
    ds = lance.dataset(uri, storage_options=so)
    out = []
    for itype in ("BTREE", "BITMAP"):
        for col in INDEX_PLAN.get(itype, []):
            try:
                ds.create_scalar_index(col, index_type=itype, replace=True)
                print(f"    {itype:6s} ✓ {col}")
                out.append({"col": col, "type": itype, "ok": True})
            except Exception as exc:  # noqa: BLE001
                print(f"    {itype:6s} ✗ {col}: {exc}")
                out.append({"col": col, "type": itype, "ok": False, "error": str(exc)[:200]})
    return out


def _record_run(*, datasets, rows_total, status, error, started_at, completed_at) -> None:
    """Terminal ledger row → ops.msha_ingest_runs. to_regclass-guarded DDL + INSERT retry
    (the #377 deadlock-safe pattern); best-effort, never masks the build."""
    import time

    import psycopg
    from psycopg import errors as pg_errors
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            with conn.transaction():
                cur.execute("SELECT to_regclass('ops.msha_ingest_runs')")
                if cur.fetchone()[0] is None:
                    cur.execute(OPS_DDL)
            params = ("msha", "data-sink", "active/ (derived)", Jsonb(datasets),
                      rows_total, 0, status, error, started_at, completed_at)
            for attempt in range(3):
                try:
                    with conn.transaction():
                        cur.execute(
                            "INSERT INTO ops.msha_ingest_runs (feed, source_bucket, source_prefix, "
                            "datasets, rows_total, bytes_downloaded, status, error, started_at, "
                            "completed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", params)
                    break
                except (pg_errors.DeadlockDetected, pg_errors.SerializationFailure):
                    if attempt == 2:
                        raise
                    time.sleep(0.25 * (attempt + 1))
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 40, memory=32768, cpu=8.0,
)
def build() -> dict:
    import datetime as dt

    import duckdb
    import lance

    so = _so()
    started = dt.datetime.now(dt.timezone.utc)
    status, error, rows, indexes, stats = "error", None, 0, [], {}
    try:
        con = duckdb.connect()
        con.execute("PRAGMA threads=8")
        con.execute("SET preserve_insertion_order=false")
        for name, alias, cols in SOURCES:
            ds = lance.dataset(f"{_ACTIVE}/{name}/", storage_options=so)
            con.register(alias, ds.scanner(columns=cols).to_table())
            print(f"    loaded {name} ({ds.count_rows():,} rows, {len(cols)} cols) as {alias}")

        tbl = con.execute(BUILD_SQL).fetch_arrow_table()
        rows = tbl.num_rows
        mines_n = con.execute("SELECT count(*) FROM mines").fetchone()[0]
        grain_ok = rows == mines_n
        print(f"    assembled {rows:,} rows × {tbl.num_columns} cols (mines spine {mines_n:,}) "
              f"→ {'OK' if grain_ok else '!!!! GRAIN MISMATCH'}")
        if not grain_ok:
            raise RuntimeError(f"grain mismatch: site_master {rows} != mines {mines_n}")

        # Option-D resolution audit (the numbers the operator signed off on)
        con.register("sm", tbl)
        stats = con.execute("""
            SELECT count(*) total,
                   count(*) FILTER (WHERE CURRENT_CONTROLLER_ID IS NOT NULL) ctrl_resolved,
                   count(*) FILTER (WHERE CURRENT_CONTROLLER_ID IS NULL AND multi_controller_flag) ctrl_tie_null,
                   count(*) FILTER (WHERE CURRENT_CONTROLLER_ID IS NULL AND NOT multi_controller_flag) ctrl_no_window,
                   count(*) FILTER (WHERE multi_controller_flag) had_multi_ctrl
            FROM sm""").fetchone()
        stats = dict(zip(["total", "ctrl_resolved", "ctrl_tie_null", "ctrl_no_window", "had_multi_ctrl"], stats))
        print(f"    Option-D controller: resolved={stats['ctrl_resolved']:,} "
              f"tie_null={stats['ctrl_tie_null']:,} no_window={stats['ctrl_no_window']:,} "
              f"(had_ambiguity={stats['had_multi_ctrl']:,})")
        con.close()

        lance.write_dataset(tbl, URI, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                            storage_options=so)
        rows = lance.dataset(URI, storage_options=so).count_rows()
        print(f"    wrote {rows:,} rows → {URI}")
        indexes = _build_indexes(URI, so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(datasets={"msha_site_master": {"uri": URI, "kind": "derived",
                    "rows": int(rows), "option_d": stats, "indexes": indexes}},
                    rows_total=int(rows), status=status, error=error,
                    started_at=started, completed_at=completed)
    if status != "success":
        raise RuntimeError(f"site_master build failed: {error}")
    return {"rows": int(rows), "option_d": stats, "indexes": indexes, "status": status}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15)
def verify_dataset() -> dict:
    import lance
    so = _so()
    ds = lance.dataset(URI, storage_options=so)
    idx = sorted((i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                 for i in ds.list_indices())
    info = {"uri": URI, "rows": ds.count_rows(), "cols": len(ds.schema.names), "indices": idx}
    for col in ("MINE_ID", "CURRENT_CONTROLLER_ID", "CURRENT_OPERATOR_ID"):
        info[f"{col}__non_null"] = ds.count_rows(filter=f"{col} IS NOT NULL")
    info["multi_controller_flag__true"] = ds.count_rows(filter="multi_controller_flag IS true")
    info["active_with_ss_since_2025"] = ds.count_rows(
        filter="CURRENT_MINE_STATUS IN ('Active','Intermittent','NonProducing','New Mine','Temporarily Idled') "
               "AND ss_count_since_2025 > 0")
    return info


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 20)
def reindex() -> list:
    return _build_indexes(URI, _so())


@app.local_entrypoint()
def run() -> None:
    import json
    print(json.dumps(build.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify() -> None:
    import json
    print(json.dumps(verify_dataset.remote(), indent=2, default=str))


@app.local_entrypoint()
def reindex_only() -> None:
    import json
    print(json.dumps(reindex.remote(), indent=2, default=str))
