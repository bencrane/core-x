"""gtm_fpds_entity_signal_events — day-precision entity-signal mart from FPDS.

The FPDS adjacency layer for the SAM profile-delta mart (sam_master_profile_deltas
answers "a CAGE appeared / a NAICS was added" at SAM vintage cadence; this refines the
demonstrated side to the DAY). One narrow scan of transaction_search_fpds → a compact
event mart the sidecar serves warm:

  gtm_fpds_entity_signal_events   1 row / (uei, signal_type, signal_value)
      cage_txn         first/last day each (uei, cage_code) pair transacts — cross-ref a
                       net-new CAGE from the delta mart to pinpoint the exact day the entity
                       moves into that CAGE's space (Miller-Act / defense-space signal).
      jv_8a_certified  sba_certified_8_a_joint_ve  ┐ verified, action-dated JV / 8(a)
      jv_econ_disadv   joint_venture_economically  │ structured flags — the CLEAN JV signal
      jv_women_owned   joint_venture_women_owned    │ that replaces the polluted SAM
      c8a_participant  c8a_program_participant     ┘ name-pattern heuristic entirely.
      Each row: first_action_date, last_action_date, action_ct, obl_sum.

Bridges Gap B (docs/sidecar_gaps/SIDECAR_GAP_REPORT_2026-07-12-*) FPDS-adjacency half.
Source: s3://data-sink/active/usaspending/transaction_search_fpds/ (~108M actions;
recipient_uei, cage_code, action_date day-grain, federal_action_obligation, the bool flags).

Rebuild: full snapshot, Lance overwrite (new version), rollback guard. Ledger:
ops.gtm_profile_delta_runs (shared feed table; feed='gtm_fpds_entity_signal_events').

Run:
    modal run --detach pipelines/gtm/gtm_fpds_entity_signal_events.py    # full build
    modal run pipelines/gtm/gtm_fpds_entity_signal_events.py --smoke     # bounded smoke
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid

SRC_FPDS = "s3://data-sink/active/usaspending/transaction_search_fpds/"
SIGNALS_URI = os.environ.get(
    "GTM_FPDS_SIGNAL_EVENTS_URI", "s3://data-sink/active/gtm_fpds_entity_signal_events/")

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
SIGNALS_BTREE = ["uei", "first_action_date"]
SIGNALS_BITMAP = ["signal_type"]
SIGNALS_ROW_FLOOR = 200_000        # measured live ≈ 285k ((uei,cage) pairs + flag events)

DUCKDB_MEMORY_LIMIT = os.environ.get("GTM_DUCKDB_MEM", "48GB")
DUCKDB_THREADS = 16
SPILL_DIR = os.environ.get("GTM_DUCKDB_SPILL", "/tmp/duckdb_spill")

_FLAG_SIGNALS = [
    ("jv_8a_certified", "sba_certified_8_a_joint_ve"),
    ("jv_econ_disadv", "joint_venture_economically"),
    ("jv_women_owned", "joint_venture_women_owned"),
    ("c8a_participant", "c8a_program_participant"),
]

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.gtm_profile_delta_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,
    dataset_uri     text        NOT NULL,
    build_id        text,
    rows_written    bigint,
    metrics         jsonb,
    inputs          jsonb,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
"""


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _alert(msg: str) -> None:
    try:
        from core.ops_alert import alert
        alert(msg)
    except Exception:  # noqa: BLE001 — alerting must never mask the build
        print(f"ALERT (no channel): {msg}")


def _pg_connect():
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, feed, dataset_uri, build_id, rows, metrics, lineage, status,
                error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "INSERT INTO ops.gtm_profile_delta_runs (feed, dataset_uri, build_id,"
                " rows_written, metrics, inputs, status, error, started_at,"
                " completed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (feed, dataset_uri, build_id, rows, json.dumps(metrics),
                 json.dumps(lineage), status, error, started_at, completed_at))
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


def _new_con():
    import duckdb
    os.makedirs(SPILL_DIR, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads={DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _materialize_signals(con, so: dict, smoke: bool):
    import lance

    ds = lance.dataset(SRC_FPDS, storage_options=so)
    lineage = [{"name": "transaction_search_fpds", "uri": SRC_FPDS,
                "version": ds.version, "rows_at_read": ds.count_rows()}]
    smoke_flt = " AND recipient_uei LIKE 'C1%'" if smoke else ""

    # pass 1 — (uei, cage) grain (full scan, narrow projection)
    con.register("fpds_cage", ds.scanner(
        columns=["recipient_uei", "cage_code", "action_date",
                 "federal_action_obligation"],
        filter=("recipient_uei IS NOT NULL AND cage_code IS NOT NULL" + smoke_flt),
    ).to_reader())
    con.execute("""
        CREATE TEMP TABLE sig_cage AS
        SELECT recipient_uei AS uei, 'cage_txn' AS signal_type, cage_code AS signal_value,
               min(action_date) AS first_action_date, max(action_date) AS last_action_date,
               count(*) AS action_ct,
               sum(coalesce(federal_action_obligation, 0)) AS obl_sum
        FROM fpds_cage GROUP BY 1, 2, 3
    """)
    con.unregister("fpds_cage")

    # pass 2 — structured-flag grains (pushdown filter → tiny subset lands)
    flag_cols = [c for _, c in _FLAG_SIGNALS]
    con.register("fpds_flags", ds.scanner(
        columns=["recipient_uei", "action_date", "federal_action_obligation", *flag_cols],
        filter=("recipient_uei IS NOT NULL AND ("
                + " OR ".join(f"{c} = true" for c in flag_cols) + ")" + smoke_flt),
    ).to_reader())
    flag_selects = [
        f"""SELECT recipient_uei AS uei, '{sig}' AS signal_type,
                   CAST(NULL AS VARCHAR) AS signal_value,
                   min(action_date) AS first_action_date,
                   max(action_date) AS last_action_date, count(*) AS action_ct,
                   sum(coalesce(federal_action_obligation, 0)) AS obl_sum
            FROM fpds_flags WHERE {col} = true GROUP BY 1, 2, 3"""
        for sig, col in _FLAG_SIGNALS]
    con.execute(f"CREATE TEMP TABLE sig_flags AS {' UNION ALL '.join(flag_selects)}")
    con.unregister("fpds_flags")

    table = con.sql("""
        SELECT * FROM (SELECT * FROM sig_cage UNION ALL SELECT * FROM sig_flags)
        ORDER BY uei, signal_type, signal_value
    """).to_arrow_table()
    by_type = dict(con.sql(
        "SELECT signal_type, count(*) FROM (SELECT signal_type FROM sig_cage "
        "UNION ALL SELECT signal_type FROM sig_flags) GROUP BY 1").fetchall())
    metrics = {"rows": table.num_rows, "by_signal_type": by_type}
    con.execute("DROP TABLE sig_cage; DROP TABLE sig_flags")
    return table, metrics, lineage


def _write_dataset(table, uri: str, so: dict, row_floor: int) -> int:
    import lance

    if table.num_rows < row_floor:
        raise RuntimeError(f"pre-write gate: {table.num_rows:,} < floor {row_floor:,}")
    try:
        v_before = lance.dataset(uri, storage_options=so).version
    except Exception:  # noqa: BLE001
        v_before = None
    print(f"{uri} v_before={v_before} rows_in={table.num_rows:,}")
    try:
        lance.write_dataset(table, uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        ds = lance.dataset(uri, storage_options=so)
        for col in SIGNALS_BTREE:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE ✓ {col}")
        for col in SIGNALS_BITMAP:
            ds.create_scalar_index(col, index_type="BITMAP")
            print(f"  BITMAP ✓ {col}")
        ds = lance.dataset(uri, storage_options=so)
        committed = ds.count_rows()
        if committed != table.num_rows:
            raise RuntimeError(f"write-integrity: {committed:,} != {table.num_rows:,}")
        print(f"  committed={committed:,}")
        return committed
    except Exception as werr:  # noqa: BLE001
        if v_before is not None:
            try:
                lance.dataset(uri, storage_options=so, version=v_before).restore()
            except Exception as rerr:  # noqa: BLE001
                raise RuntimeError(f"ROLLBACK FAILED to v{v_before}: {rerr}; original: {werr}")
            raise RuntimeError(f"write/index failed → rolled back: {werr}")
        raise RuntimeError(f"failed on net-new dataset (inspect/drop {uri}): {werr}")


def run_build(smoke: bool = False) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    build_id = f"{started_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    feed = "gtm_fpds_entity_signal_events"
    status, error, rows, metrics, lineage = "error", None, 0, {}, []
    try:
        con = _new_con()
        try:
            table, metrics, lineage = _materialize_signals(con, so, smoke)
        finally:
            con.close()
        print(f"[{feed}] materialized: {metrics}")
        if smoke:
            status, rows = "smoke_ok", table.num_rows
            print(f"[{feed}] SMOKE — no write. rows={rows:,}")
        else:
            rows = _write_dataset(table, SIGNALS_URI, so,
                                  0 if smoke else SIGNALS_ROW_FLOOR)
            status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        _alert(f"[gtm_fpds_entity_signal_events] build error: {error[:300]}")
        raise
    finally:
        if not smoke:
            _record_run(feed=feed, dataset_uri=SIGNALS_URI, build_id=build_id, rows=rows,
                        metrics=metrics, lineage=lineage, status=status, error=error,
                        started_at=started_at,
                        completed_at=dt.datetime.now(dt.timezone.utc))
    return {"feed": feed, "status": status, "rows": rows, **metrics}


try:
    import modal

    image = modal.Image.debian_slim(python_version="3.12").pip_install(
        "duckdb>=1.5,<2", "pylance>=8", "pyarrow>=17", "psycopg[binary]>=3.2",
    ).env({"LANCE_BYPASS_SPILLING": "true"})
    app = modal.App("gtm-fpds-entity-signal-events", image=image)

    @app.function(
        secrets=[modal.Secret.from_name("r2-credentials"),
                 modal.Secret.from_name("hqx-postgres")],
        timeout=2 * 60 * 60, memory=65536, cpu=16.0)
    def build_signal_events(smoke: bool = False) -> dict:
        return run_build(smoke)

    @app.local_entrypoint()
    def main(smoke: bool = False):
        print(build_signal_events.remote(smoke))
except ImportError:  # local run without modal installed
    modal = None


if __name__ == "__main__":
    print(run_build())
