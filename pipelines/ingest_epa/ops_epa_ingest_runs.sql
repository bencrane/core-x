-- Canonical DDL for ops.epa_ingest_runs — the EPA multi-media ingest terminal-state ledger.
-- Verbatim mirror of OPS_DDL in pipelines/ingest_epa/materialize_epa.py (keep the two in
-- sync). Applied idempotently by every worker run and by the `init` entrypoint:
--   modal run pipelines/ingest_epa/materialize_epa.py::init
--
-- One row per (run_id, dataset): each materialize_one writes its dataset's terminal row,
-- build_bridge writes the bridge row (with match metrics in `metrics`), and the
-- orchestrator writes a `__run__` summary row. Lance/R2 remains the system of record;
-- this table is operational state only (per the Substrate Split).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.epa_ingest_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          text        NOT NULL,
    feed            text        NOT NULL,
    dataset         text        NOT NULL,
    dataset_uri     text,
    source_archives text,
    rows_written    bigint,
    indexes_built   text,
    status          text        NOT NULL,
    error           text,
    metrics         jsonb,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS epa_ingest_runs_run_idx      ON ops.epa_ingest_runs (run_id);
CREATE INDEX IF NOT EXISTS epa_ingest_runs_dataset_idx  ON ops.epa_ingest_runs (dataset);
CREATE INDEX IF NOT EXISTS epa_ingest_runs_status_idx   ON ops.epa_ingest_runs (status);
CREATE INDEX IF NOT EXISTS epa_ingest_runs_recorded_idx ON ops.epa_ingest_runs (recorded_at DESC);
