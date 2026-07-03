-- ops.jsearch_capture_postings + ops.jsearch_capture_runs
--
-- Mirror of the inline OPS_DDL in pipelines/jsearch/harvest_capture_roles.py. The INLINE copy is
-- AUTHORITATIVE (it is what the worker actually executes at runtime, travelling with the Modal
-- function). This file exists for repo grep/parity with the other pipelines/<domain>/ops_*.sql
-- ledgers. Keep the two in sync; if they diverge, the Python inline copy wins.
--
-- ops.jsearch_capture_postings — raw JSearch posting SoR (landed FIRST, before Lance hydration).
--   Grain: one row per job_id (the natural per-publisher-posting key = dedup + resume guard).
--   Land EVERYTHING: recruiter/staffing + "Confidential" employers are flagged, never filtered.
-- ops.jsearch_capture_runs — per-run terminal ledger (harvest + materialize).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.jsearch_capture_postings (
    job_id                  text        PRIMARY KEY,
    query_variant           text,
    job_title               text,
    employer_name           text,
    employer_website        text,
    employer_domain         text,
    employer_is_confidential boolean    NOT NULL DEFAULT false,
    employer_is_staffing    boolean     NOT NULL DEFAULT false,
    publisher               text,
    apply_publishers        jsonb,
    n_apply_options         integer     NOT NULL DEFAULT 0,
    employment_type         text,
    job_is_remote           boolean,
    job_city                text,
    job_state               text,
    job_country             text,
    job_location            text,
    posted_at_ts            bigint,
    posted_at               timestamptz,
    job_min_salary          numeric,
    job_max_salary          numeric,
    salary_period           text,
    job_apply_link          text,
    onet_soc                text,
    raw_json                jsonb,
    harvest_run_root        text,
    first_seen_at           timestamptz NOT NULL DEFAULT now(),
    last_seen_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jsearch_capture_postings_domain_idx    ON ops.jsearch_capture_postings (employer_domain);
CREATE INDEX IF NOT EXISTS jsearch_capture_postings_employer_idx  ON ops.jsearch_capture_postings (employer_name);
CREATE INDEX IF NOT EXISTS jsearch_capture_postings_state_idx     ON ops.jsearch_capture_postings (job_state);
CREATE INDEX IF NOT EXISTS jsearch_capture_postings_publisher_idx ON ops.jsearch_capture_postings (publisher);
CREATE INDEX IF NOT EXISTS jsearch_capture_postings_seen_idx      ON ops.jsearch_capture_postings (last_seen_at DESC);

CREATE TABLE IF NOT EXISTS ops.jsearch_capture_runs (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed               text        NOT NULL,
    mode               text        NOT NULL,
    run_root           text,
    date_posted        text,
    queries_run        integer     NOT NULL DEFAULT 0,
    pages_fetched      integer     NOT NULL DEFAULT 0,
    credits_spent      integer     NOT NULL DEFAULT 0,
    jobs_seen          integer     NOT NULL DEFAULT 0,
    jobs_new           integer     NOT NULL DEFAULT 0,
    jobs_updated       integer     NOT NULL DEFAULT 0,
    employers_distinct integer     NOT NULL DEFAULT 0,
    rows_materialized  integer     NOT NULL DEFAULT 0,
    status             text        NOT NULL,
    error              text,
    started_at         timestamptz,
    completed_at       timestamptz,
    recorded_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jsearch_capture_runs_status_chk CHECK (status IN ('success', 'error')),
    CONSTRAINT jsearch_capture_runs_mode_chk   CHECK (mode IN ('backfill', 'incremental', 'materialize'))
);
CREATE INDEX IF NOT EXISTS jsearch_capture_runs_feed_idx        ON ops.jsearch_capture_runs (feed);
CREATE INDEX IF NOT EXISTS jsearch_capture_runs_recorded_at_idx ON ops.jsearch_capture_runs (recorded_at DESC);
