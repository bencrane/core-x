-- Operational ledger for the CMS Medicare full-archive ingest
-- (pipelines/cms_medicare/ingest.py). Key: (dataset, program_year, source_object_etag).
-- Mirrored verbatim by the module's OPS_DDL; applied by ::init_state.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.cms_medicare_runs (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  feed text NOT NULL DEFAULT 'cms_medicare',
  phase text NOT NULL,                 -- ingest | verify
  dataset text, program_year smallint,
  source_archive text, source_member text, source_object_etag text,
  candidate_key_dups bigint,           -- §9 #1 grain proof, recorded per dataset
  decimal_overflow_nulls bigint,       -- §9 #2 / §1 #5 numeric-width assertion, recorded per unit
  rows_processed bigint, rejected_rows bigint,
  status text NOT NULL, error text,
  started_at timestamptz, completed_at timestamptz,
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cms_medicare_runs_dataset_idx  ON ops.cms_medicare_runs (dataset);
CREATE INDEX IF NOT EXISTS cms_medicare_runs_phase_idx    ON ops.cms_medicare_runs (phase);
CREATE INDEX IF NOT EXISTS cms_medicare_runs_status_idx   ON ops.cms_medicare_runs (status);
CREATE INDEX IF NOT EXISTS cms_medicare_runs_recorded_idx ON ops.cms_medicare_runs (recorded_at DESC);

-- Idempotency keys (UPSERT targets): one ingest row per (dataset, member), one verify row per dataset.
-- Re-runs / Modal retries UPSERT these rows rather than appending duplicates, so the §4 idempotency
-- query ("is this unit landed+verified?") stays exact.
CREATE UNIQUE INDEX IF NOT EXISTS cms_medicare_ingest_uq ON ops.cms_medicare_runs (dataset, source_member) WHERE phase = 'ingest';
CREATE UNIQUE INDEX IF NOT EXISTS cms_medicare_verify_uq ON ops.cms_medicare_runs (dataset)                WHERE phase = 'verify';
