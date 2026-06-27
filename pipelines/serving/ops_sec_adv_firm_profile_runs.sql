-- Audit ledger for the SEC ADV firm-profile serving materializer.
-- Written by pipelines/serving/materialize_sec_adv_firm_profile.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- DERIVED / rebuildable: sec_adv_firm_profile is a read model (snapshot-overwrite each run) that
-- recovers the 252-column Form ADV Part 1A base — locked in sec_adv_part1.raw_filing (JSON) — into
-- typed, BTREE/BITMAP-indexed columns, 1 row per CRD. Pure function of the source snapshot.
-- Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sec_adv_firm_profile_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,   -- 'sec_adv_firm_profile'
    dataset_uri         text,                   -- s3://data-sink/active/sec_adv_firm_profile/
    source_uri          text,                   -- s3://data-sink/active/sec_adv_part1/
    source_snapshot_date date,                  -- which ADV snapshot this profile was projected from
    rows_written        bigint,                 -- total rows (1 / CRD)
    ia_rows             bigint,                 -- IA subset (economics-bearing)
    era_rows            bigint,                 -- ERA subset (economics NULL by design)
    write_mode          text,                   -- 'overwrite'
    indices_built       text,                   -- comma list of BTREE:col / BITMAP:col actually built
    status              text        NOT NULL,   -- 'success' | 'error'
    error_message       text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sec_adv_firm_profile_runs_status_idx
    ON ops.sec_adv_firm_profile_runs (status);
CREATE INDEX IF NOT EXISTS sec_adv_firm_profile_runs_recorded_at_idx
    ON ops.sec_adv_firm_profile_runs (recorded_at DESC);
