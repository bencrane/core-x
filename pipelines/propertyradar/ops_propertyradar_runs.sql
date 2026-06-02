-- Terminal-state ledger for the PropertyRadar quota-governed ingest.
-- Mirrored verbatim by OPS_DDL in pipelines/propertyradar/properties.py (applied by the
-- init_db function / `modal run ...::setup`, and re-asserted idempotently on every run by
-- _record_run). This file is the reviewable source. One row per phase per run:
-- phase ∈ {ingest, reindex}. Idempotent DDL.
--
-- Quota-governor telemetry (the directive's mandate — parameters used, matches found, exact
-- credits consumed): criteria (the Criteria sent), max_allowed_spend (the authorized ceiling),
-- preview_count (totalResultCount from the free Purchase=0 preview = matches found),
-- credits_consumed (rows actually retrieved under Purchase=1 = exact spend), and
-- governor_decision (which branch the governor took).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.propertyradar_runs (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed              text        NOT NULL,
    phase             text        NOT NULL,     -- 'ingest' | 'reindex'
    property_uri      text,                     -- s3://data-sink/active/propertyradar_property_lance/
    person_uri        text,                     -- s3://data-sink/active/propertyradar_person_lance/
    criteria          jsonb,                    -- parameters used: the PropertyRadar Criteria array
    max_allowed_spend bigint,                   -- the operator's --max-allowed-spend ceiling
    page_limit        integer,                  -- pagination Limit used for billable retrieval
    preview_count     bigint,                   -- matches found: envelope totalResultCount (free preview)
    credits_consumed  bigint,                   -- exact credits spent = rows retrieved under Purchase=1
    governor_decision text,                     -- 'preview_only' | 'aborted_over_budget' | 'authorized'
    pages             bigint,                   -- billable pages paginated
    property_rows     bigint,                   -- committed property-grain Lance rows
    person_rows       bigint,                   -- committed person-grain Lance rows (exploded Persons)
    property_indexes  jsonb,                    -- ["RadarID","parcel_key","fips5"]
    person_indexes    jsonb,                    -- ["RadarID","parcel_key","person_key"]
    status            text        NOT NULL,     -- 'success' | 'aborted_over_budget' | 'error'
    error             text,
    started_at        timestamptz,
    completed_at      timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS propertyradar_runs_phase_idx       ON ops.propertyradar_runs (phase);
CREATE INDEX IF NOT EXISTS propertyradar_runs_status_idx      ON ops.propertyradar_runs (status);
CREATE INDEX IF NOT EXISTS propertyradar_runs_recorded_at_idx ON ops.propertyradar_runs (recorded_at DESC);
