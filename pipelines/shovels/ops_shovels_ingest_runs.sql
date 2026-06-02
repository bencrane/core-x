-- Terminal-state table for the Shovels.ai ingestion engine.
-- Written by pipelines/shovels/ingest.py:_record_run via psycopg (HQX_DB_URL_POOLED)
-- on every terminal state, success or failure — mirrors the ops.* contract used by the
-- edgar / cms_open_payments / sam feeds (ARCHITECTURE.md §5). One row per entity ingest run.
-- Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the _OPS_DDL constant and applies it
-- via `modal run pipelines/shovels/ingest.py::migrate`. Keep the two in sync.
--
-- Telemetry captured (the directive's "parameters used, rows fetched, credits spent"):
--   * parameters used  → geo_id, permit_from, permit_to, page_size, max_pages, query_spec(jsonb)
--   * rows fetched     → rows_fetched (records returned by the API, pre-dedup)
--   * rows written     → rows_written (Lance dataset row count after merge/dedup)
--   * credits spent    → credits_spent (summed X-Credits-Request header; §3 — 1 credit/record)

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.shovels_ingest_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id           text        NOT NULL,   -- uuid; also stamped into the provenance source_run_id
    feed             text        NOT NULL,   -- 'shovels'
    entity           text        NOT NULL,   -- permits|contractors|employees|residents|geo|tags
    source_endpoint  text,                   -- e.g. 'permits/search', 'contractors/search', 'list/tags'
    dataset_uri      text,                   -- s3://data-sink/active/shovels_<entity>/
    -- parameters used --
    geo_id           text,                   -- state code (CA) | ZIP | Shovels geo_id (§18)
    permit_from      text,                   -- YYYY-MM-DD (inclusive, on permit start_date; §7.1)
    permit_to        text,                   -- YYYY-MM-DD (inclusive)
    page_size        integer,                -- == credits per page (§3)
    max_pages        integer,                -- credit guardrail (<=0 means exhaust cursor)
    query_spec       jsonb,                  -- full parameterization (geo+window+filters+knobs)
    write_mode       text,                   -- create | merge_insert | noop
    -- volume + spend --
    api_calls        integer,                -- HTTP GETs issued this run
    rows_fetched     bigint,                 -- records returned by the API (pre-dedup)
    rows_written     bigint,                 -- Lance dataset row count after merge/dedup
    credits_spent    integer,                -- summed X-Credits-Request (§3)
    indexes_built    jsonb,                  -- e.g. ["BTREE:id"]
    -- lifecycle --
    status           text        NOT NULL,   -- running | success | no_data | error
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    duration_seconds double precision,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS shovels_ingest_runs_feed_idx        ON ops.shovels_ingest_runs (feed);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_entity_idx      ON ops.shovels_ingest_runs (entity);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_status_idx      ON ops.shovels_ingest_runs (status);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_geo_idx         ON ops.shovels_ingest_runs (geo_id);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_run_id_idx      ON ops.shovels_ingest_runs (run_id);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_recorded_at_idx ON ops.shovels_ingest_runs (recorded_at DESC);
