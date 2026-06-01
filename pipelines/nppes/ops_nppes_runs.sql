-- Terminal-state ledger for the CMS NPPES monthly full-replacement ingest.
-- Mirrored verbatim by OPS_DDL in pipelines/nppes/ingest.py (applied by the
-- apply_state_schema function / `modal run ...::init_state`). This file is the
-- reviewable source. One row per capture run (full ledger — never upserted), so the
-- run history of every monthly snapshot is auditable. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.nppes_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,    -- 'nppes'
    snapshot_month  text        NOT NULL,    -- 'YYYY-MM' partition key (active/nppes/snapshot=YYYY-MM/)
    dataset_uri     text,                    -- s3://data-sink/active/nppes/snapshot=YYYY-MM/
    source_url      text,                    -- resolved CMS monthly full-replacement ZIP URL
    source_file     text,                    -- NPPES_Data_Dissemination_<Month>_<Year>[_V<n>].zip
    source_member   text,                    -- npidata_pfile_<dates>.csv (the extracted core member)
    source_encoding text,                    -- detected source encoding ('utf-8' | 'latin-1')
    zip_bytes       bigint,                  -- downloaded ZIP size
    csv_bytes       bigint,                  -- extracted core CSV size
    rows_processed  bigint,                  -- committed Lance row count
    rejected_rows   bigint,                  -- malformed rows quarantined by store_rejects
    write_path      text,                    -- 'stream-local-publish' (local stage → boto3 publish)
    status          text        NOT NULL,    -- 'success' | 'error'
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS nppes_runs_month_idx       ON ops.nppes_runs (snapshot_month);
CREATE INDEX IF NOT EXISTS nppes_runs_feed_idx        ON ops.nppes_runs (feed);
CREATE INDEX IF NOT EXISTS nppes_runs_status_idx      ON ops.nppes_runs (status);
CREATE INDEX IF NOT EXISTS nppes_runs_recorded_at_idx ON ops.nppes_runs (recorded_at DESC);
