-- Ops ledger for the USAspending Award Data Archive (monthly Full + Delta) ingest.
-- DERIVED landing: each monthly file is read VERBATIM (all-VARCHAR, every column/row) and
-- APPENDED to its kind's Lance table, stamped with the snapshot. One row per terminal state.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.usaspending_archive_runs (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind              text        NOT NULL,   -- 'full' | 'delta'
    snapshot_stamp    text        NOT NULL,   -- e.g. '20260606'
    zip_key           text,                   -- R2 landing key ingested
    csv_members       int,                    -- # CSV members in the zip
    columns           int,                    -- column count written (verbatim + provenance)
    rows_written      bigint,                 -- rows appended this run
    table_rows_after  bigint,                 -- table total after the append
    write_mode        text,                   -- 'overwrite' (first create) | 'append'
    indices_built     text,                   -- comma list
    status            text        NOT NULL,   -- 'success' | 'error'
    error_message     text,
    started_at        timestamptz,
    completed_at      timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS usaspending_archive_runs_kind_stamp_idx
    ON ops.usaspending_archive_runs (kind, snapshot_stamp);
CREATE INDEX IF NOT EXISTS usaspending_archive_runs_status_idx
    ON ops.usaspending_archive_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_archive_runs_recorded_at_idx
    ON ops.usaspending_archive_runs (recorded_at DESC);
