-- Terminal-state ledger for the SEC Form ADV ingest. Mirrored verbatim by _OPS_DDL in
-- pipelines/sec_adv/ingest.py (applied by the apply_migration function). One row per dataset
-- per run: dataset ∈ {part1, advw}; feed ∈ {sec_adv_part1, sec_adv_w}.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sec_adv_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset         text        NOT NULL,
    feed            text        NOT NULL,
    source_url      text,
    dataset_uri     text,
    as_of           text,
    source_files    jsonb,
    rows_processed  bigint,
    rows_written    bigint,
    rejected_rows   bigint,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sec_adv_runs_dataset_idx     ON ops.sec_adv_runs (dataset);
CREATE INDEX IF NOT EXISTS sec_adv_runs_feed_idx        ON ops.sec_adv_runs (feed);
CREATE INDEX IF NOT EXISTS sec_adv_runs_status_idx      ON ops.sec_adv_runs (status);
CREATE INDEX IF NOT EXISTS sec_adv_runs_as_of_idx       ON ops.sec_adv_runs (as_of DESC);
CREATE INDEX IF NOT EXISTS sec_adv_runs_recorded_at_idx ON ops.sec_adv_runs (recorded_at DESC);
