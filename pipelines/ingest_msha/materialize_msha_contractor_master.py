"""Compute worker — msha_contractor_master (the contractor third-spine anchor).

Part of the ``msha-contractor-master`` Modal app. Sibling to materialize_msha_site_master.py:
a DERIVED dataset (reads the active spine datasets, not landing) that materializes one
deterministic row per CONTRACTOR_ID — the contractor population made a first-class, queryable
anchor. Same clean-room write path as the curated workers (DuckDB → lance overwrite → indices →
deadlock-safe ledger).

WHY
    CONTRACTOR_ID is the deterministic THIRD spine (alpha-prefixed, ~38.7K in the registry).
    The operator's requirement: pivot on a single CONTRACTOR_ID across all its activity —
    production registry, violations, accidents, exposure samples — deterministically and
    index-backed. This anchor bakes that cross-spine footprint into one indexed row, the
    contractor-grain analog of msha_site_master. Whether a contractor later resolves to a
    controller/operator is post-spine business logic, intentionally NOT decided here.

GRAIN — the FULL contractor population, not just the registry: the distinct union of
    CONTRACTOR_ID across {msha_contractors (production registry), msha_enforcement_ledger,
    msha_accidents, the three exposure-sample sets}. ``in_production_registry`` flags the ~89%
    that filed production vs. the cited-but-unregistered tail. All keys are the indexed
    CONTRACTOR_ID spine.

    modal run pipelines/ingest_msha/materialize_msha_contractor_master.py::run
    modal run pipelines/ingest_msha/materialize_msha_contractor_master.py::verify
    modal deploy pipelines/ingest_msha/materialize_msha_contractor_master.py
"""

from __future__ import annotations

import os

import modal

FEED = "msha"
_ACTIVE = "s3://data-sink/active"
URI = os.environ.get("MSHA_CONTRACTOR_MASTER_URI", f"{_ACTIVE}/msha_contractor_master/")
DATA_STORAGE_VERSION = "2.1"

INDEX_PLAN = {
    "BTREE": ["CONTRACTOR_ID", "CONTRACTOR_NAME"],
    "BITMAP": ["in_production_registry", "primary_coal_metal"],
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

# Assembly. Grain = distinct CONTRACTOR_ID across all sources; every per-source rollup is a
# 1-row-per-contractor LEFT JOIN, so the grain holds. Enforcement/accidents/samples all key on
# the indexed CONTRACTOR_ID spine.
BUILD_SQL = """
WITH
reg AS (
  SELECT CONTRACTOR_ID,
         any_value(CONTRACTOR_NAME) AS CONTRACTOR_NAME,
         any_value(COAL_METAL_IND)  AS primary_coal_metal,
         count(DISTINCT CAL_YR)     AS registry_years,
         max(CAL_YR)                AS registry_latest_year,
         round(sum(HOURS_WORKED))   AS total_hours_worked,
         round(sum(COAL_PRODUCTION)) AS total_coal_production,
         max(AVG_EMPLOYEE_CNT)      AS max_employee_cnt
  FROM ctr WHERE CONTRACTOR_ID IS NOT NULL GROUP BY CONTRACTOR_ID),
enf_c AS (
  SELECT CONTRACTOR_ID,
         count(*) AS violation_count,
         count(*) FILTER (WHERE SIG_SUB = 'Y') AS ss_count,
         count(*) FILTER (WHERE CIT_ORD_SAFE = 'Order') AS order_count,
         round(sum(PROPOSED_PENALTY_AMT)) AS proposed_penalty_sum,
         count(DISTINCT MINE_ID) AS n_mines_cited,
         max(VIOLATION_ISSUE_DT) AS last_violation_dt
  FROM enf WHERE CONTRACTOR_ID IS NOT NULL GROUP BY CONTRACTOR_ID),
acc_c AS (
  SELECT CONTRACTOR_ID,
         count(*) AS accident_count,
         count(*) FILTER (WHERE DEGREE_INJURY_CD = '01') AS fatality_count,
         count(DISTINCT MINE_ID) AS n_mines_accidents,
         max(ACCIDENT_DT) AS last_accident_dt
  FROM acc WHERE CONTRACTOR_ID IS NOT NULL GROUP BY CONTRACTOR_ID),
smp AS (
  SELECT CONTRACTOR_ID, count(*) AS exposure_sample_count, count(DISTINCT MINE_ID) AS n_mines_sampled
  FROM (SELECT CONTRACTOR_ID, MINE_ID FROM ph WHERE CONTRACTOR_ID IS NOT NULL
        UNION ALL SELECT CONTRACTOR_ID, MINE_ID FROM nz WHERE CONTRACTOR_ID IS NOT NULL
        UNION ALL SELECT CONTRACTOR_ID, MINE_ID FROM ar WHERE CONTRACTOR_ID IS NOT NULL)
  GROUP BY CONTRACTOR_ID),
ids AS (
  SELECT CONTRACTOR_ID FROM reg
  UNION SELECT CONTRACTOR_ID FROM enf_c
  UNION SELECT CONTRACTOR_ID FROM acc_c
  UNION SELECT CONTRACTOR_ID FROM smp)
SELECT
  i.CONTRACTOR_ID,
  r.CONTRACTOR_NAME,
  (r.CONTRACTOR_ID IS NOT NULL) AS in_production_registry,
  r.primary_coal_metal,
  coalesce(r.registry_years, 0)        AS registry_years,
  r.registry_latest_year,
  coalesce(r.total_hours_worked, 0)    AS total_hours_worked,
  coalesce(r.total_coal_production, 0) AS total_coal_production,
  r.max_employee_cnt,
  coalesce(e.violation_count, 0)       AS violation_count,
  coalesce(e.ss_count, 0)              AS ss_count,
  coalesce(e.order_count, 0)           AS order_count,
  coalesce(e.proposed_penalty_sum, 0)  AS proposed_penalty_sum,
  coalesce(e.n_mines_cited, 0)         AS n_mines_cited,
  e.last_violation_dt,
  coalesce(a.accident_count, 0)        AS accident_count,
  coalesce(a.fatality_count, 0)        AS fatality_count,
  coalesce(a.n_mines_accidents, 0)     AS n_mines_accidents,
  a.last_accident_dt,
  coalesce(s.exposure_sample_count, 0) AS exposure_sample_count,
  coalesce(s.n_mines_sampled, 0)       AS n_mines_sampled,
  'msha_contractor_master (derived)' AS source_file, now() AS ingested_at
FROM ids i
LEFT JOIN reg r   USING (CONTRACTOR_ID)
LEFT JOIN enf_c e USING (CONTRACTOR_ID)
LEFT JOIN acc_c a USING (CONTRACTOR_ID)
LEFT JOIN smp s   USING (CONTRACTOR_ID)
"""

SOURCES = [
    ("msha_contractors", "ctr", ["CONTRACTOR_ID", "CONTRACTOR_NAME", "CAL_YR", "AVG_EMPLOYEE_CNT",
                                 "HOURS_WORKED", "COAL_PRODUCTION", "COAL_METAL_IND"]),
    ("msha_enforcement_ledger", "enf", ["CONTRACTOR_ID", "SIG_SUB", "CIT_ORD_SAFE",
                                        "PROPOSED_PENALTY_AMT", "MINE_ID", "VIOLATION_ISSUE_DT"]),
    ("msha_accidents", "acc", ["CONTRACTOR_ID", "DEGREE_INJURY_CD", "MINE_ID", "ACCIDENT_DT"]),
    ("msha_personal_health_samples", "ph", ["CONTRACTOR_ID", "MINE_ID"]),
    ("msha_noise_samples", "nz", ["CONTRACTOR_ID", "MINE_ID"]),
    ("msha_area_samples", "ar", ["CONTRACTOR_ID", "MINE_ID"]),
]

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "boto3>=1.35", "requests>=2.32", "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})
app = modal.App("msha-contractor-master", image=image)


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
            print(f"    loaded {name} ({ds.count_rows():,} rows) as {alias}")

        tbl = con.execute(BUILD_SQL).fetch_arrow_table()
        rows = tbl.num_rows
        con.register("cm", tbl)
        stats = con.execute("""
            SELECT count(*) total,
                   count(*) FILTER (WHERE in_production_registry) in_registry,
                   count(*) FILTER (WHERE NOT in_production_registry) cited_unregistered,
                   count(*) FILTER (WHERE violation_count > 0) with_violations,
                   count(*) FILTER (WHERE accident_count > 0) with_accidents
            FROM cm""").fetchone()
        stats = dict(zip(["total", "in_registry", "cited_unregistered", "with_violations",
                          "with_accidents"], stats))
        print(f"    assembled {rows:,} contractors × {tbl.num_columns} cols — "
              f"registry={stats['in_registry']:,} cited_unregistered={stats['cited_unregistered']:,}")
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
        _record_run(datasets={"msha_contractor_master": {"uri": URI, "kind": "derived",
                    "rows": int(rows), "population": stats, "indexes": indexes}},
                    rows_total=int(rows), status=status, error=error,
                    started_at=started, completed_at=completed)
    if status != "success":
        raise RuntimeError(f"contractor_master build failed: {error}")
    return {"rows": int(rows), "population": stats, "indexes": indexes, "status": status}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15)
def verify_dataset() -> dict:
    import lance
    so = _so()
    ds = lance.dataset(URI, storage_options=so)
    idx = sorted((i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                 for i in ds.list_indices())
    info = {"uri": URI, "rows": ds.count_rows(), "cols": len(ds.schema.names), "indices": idx}
    info["CONTRACTOR_ID__non_null"] = ds.count_rows(filter="CONTRACTOR_ID IS NOT NULL")
    info["in_production_registry__true"] = ds.count_rows(filter="in_production_registry IS true")
    info["with_violations"] = ds.count_rows(filter="violation_count > 0")
    info["with_fatality"] = ds.count_rows(filter="fatality_count > 0")
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
