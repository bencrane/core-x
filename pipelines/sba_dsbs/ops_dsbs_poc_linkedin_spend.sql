-- ops.dsbs_poc_linkedin_spend — per-subject serper spend + resolution ledger for the
-- DSBS-POC → LinkedIn resolver (pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py).
--
-- THE CREDIT DOUBLE-SPEND GUARD. One row per subject (uei|name_key). A subject already
-- present here is NEVER re-queried, so the resolver is safe to retry/resume and the account
-- credit budget is enforced globally as sum(credits_spent). NOT the system of record for the
-- resolutions themselves — the validated /in/ URLs are materialized to Lance
-- (s3://data-sink/active/dsbs_poc_linkedin/) from resolved=true rows. This ledger retains
-- EVERY attempt (hit and miss) for credit accounting and never-retry semantics. Idempotent DDL.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.dsbs_poc_linkedin_spend (
    subject_id        text PRIMARY KEY,            -- uei || '|' || name_key
    uei               text  NOT NULL,
    name_key          text  NOT NULL,
    first_name        text,
    last_name         text,
    full_name         text,
    poc_type          text,                        -- current_principal | contact_person | both
    company_term      text,                        -- the query company string actually used
    company_source    text,                        -- 'pdl' (brand) | 'legal' (SAM/DSBS legal name)
    domain            text,                        -- disambiguator / fallback-query root
    name_consistent   boolean,                     -- crosswalk_dsbs_sam.matched_name_consistent
    obligated_usd     double precision,            -- firmographics entity_active_obligated_usd (won-$)
    priority_tier     smallint,                    -- 0 won-$, 1 has-awards, 2 rest
    priority_rank     integer,                     -- global rank at spend time (1 = highest value)
    query_variant     text,                        -- 'primary' | 'primary+fallback'
    serper_query      text,
    fallback_query    text,
    fallback_done     boolean NOT NULL DEFAULT false,
    credits_spent     smallint NOT NULL DEFAULT 0, -- 1 primary, 2 if a domain fallback also ran
    http_status       integer,
    n_organic         integer,
    resolved          boolean NOT NULL DEFAULT false,
    linkedin_url      text,
    match_title       text,
    match_snippet     text,
    validation_reason text,                        -- validated | no_confident_match | no_in_link | error
    raw_json          jsonb,                       -- serper organic payload (provider truth, verbatim)
    queried_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dsbs_poc_linkedin_spend_uei_idx      ON ops.dsbs_poc_linkedin_spend (uei);
CREATE INDEX IF NOT EXISTS dsbs_poc_linkedin_spend_resolved_idx ON ops.dsbs_poc_linkedin_spend (resolved);
CREATE INDEX IF NOT EXISTS dsbs_poc_linkedin_spend_rank_idx     ON ops.dsbs_poc_linkedin_spend (priority_rank);
CREATE INDEX IF NOT EXISTS dsbs_poc_linkedin_spend_fallback_idx ON ops.dsbs_poc_linkedin_spend (resolved, fallback_done);
