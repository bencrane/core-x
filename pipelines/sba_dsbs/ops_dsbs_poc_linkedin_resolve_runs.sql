-- ops.dsbs_poc_linkedin_resolve_runs — per-run terminal ledger for the DSBS-POC → LinkedIn
-- resolver (pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py). One row per resolve pass. Run-
-- state + credit accounting only; NOT the system of record (resolutions are Lance on R2,
-- per-subject spend is ops.dsbs_poc_linkedin_spend). Idempotent DDL.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.dsbs_poc_linkedin_resolve_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text NOT NULL,
    pass_name      text NOT NULL,                 -- 'primary' | 'fallback'
    budget         integer,                       -- account credit budget for this account
    reserve        integer,                       -- credits held back (never spent)
    ceiling        integer,                       -- budget - reserve (global hard cap)
    prior_spent    integer,                       -- credits spent before this run (ledger sum)
    attempted      integer,                       -- subjects queried this run
    credits_spent  integer,                       -- credits consumed this run
    resolved       integer,                       -- validated /in/ resolutions this run
    unresolved     integer,                       -- queried-but-not-validated this run
    status         text NOT NULL,                 -- success | error
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dsbs_poc_linkedin_resolve_runs_feed_idx        ON ops.dsbs_poc_linkedin_resolve_runs (feed);
CREATE INDEX IF NOT EXISTS dsbs_poc_linkedin_resolve_runs_recorded_at_idx ON ops.dsbs_poc_linkedin_resolve_runs (recorded_at DESC);
