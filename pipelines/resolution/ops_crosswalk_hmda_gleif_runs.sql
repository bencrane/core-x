-- Terminal-state table for the HMDA × GLEIF corporate-identity crosswalk worker.
-- Written by pipelines/resolution/crosswalk_hmda_gleif.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the ops.*
-- contract used by the SAM / GLEIF / co_ucc feeds (ARCHITECTURE.md §5). One row per
-- crosswalk rebuild; the match-rate columns make every run's resolution quality auditable.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and applies it
-- via `modal run pipelines/resolution/crosswalk_hmda_gleif.py::init_ops`. Keep the two in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.crosswalk_hmda_gleif_runs (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                    text        NOT NULL,   -- 'crosswalk_hmda_gleif'
    dataset_uri             text        NOT NULL,   -- s3://data-sink/active/crosswalk_hmda_gleif/
    gleif_publish_date      text,                   -- GLEIF golden-copy snapshot date (provenance)
    hmda_panel_leis         bigint,                 -- distinct LEI in hmda_panels (match denominator)
    gleif_total_leis        bigint,                 -- distinct LEI in gleif_l1_entities (context)
    matched_leis            bigint,                 -- inner-join hits == rows_written (numerator)
    unmatched_leis          bigint,                 -- hmda_panel_leis - matched_leis
    match_rate              double precision,       -- matched_leis / hmda_panel_leis
    normalized_name_nonnull bigint,                 -- rows with a non-null normalized_legal_name
    rows_written            bigint,                 -- crosswalk rows == matched distinct LEI
    status                  text        NOT NULL,   -- 'success' | 'error'
    error                   text,
    started_at              timestamptz,
    completed_at            timestamptz,
    recorded_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crosswalk_hmda_gleif_runs_feed_idx
    ON ops.crosswalk_hmda_gleif_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_hmda_gleif_runs_status_idx
    ON ops.crosswalk_hmda_gleif_runs (status);
CREATE INDEX IF NOT EXISTS crosswalk_hmda_gleif_runs_recorded_at_idx
    ON ops.crosswalk_hmda_gleif_runs (recorded_at DESC);
