-- Clay Find People — raw landing grain (append-only). Applied to the hqx control-plane
-- Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run; the
-- ALTER ADD COLUMN IF NOT EXISTS block upgrades an already-deployed table in place).
--
-- CONTRACT. Each Clay record is stored TWO ways, both faithful to source:
--   1. raw_payload (jsonb) — the object EXACTLY as sent. Immutable source of truth; drift-proof.
--   2. flat typed columns — a LOSSLESS structural projection of the known fields, stored AS-IS.
--      This is NOT normalization. Semantic normalization (job_title -> job_level enum) is a
--      SEPARATE downstream stage; matched_job_title et al. hold the verbatim Clay strings.
-- The only computed columns are lossless identity keys read server-side from the payload:
--   raw_payload.url    -> linkedin_url_raw (feeds blitz /v2/enrichment/email) + linkedin_url_norm + person_id
--   raw_payload.domain -> domain_norm (FK -> firmographics_blitz.domain_norm)
--
-- GRAIN: (person x company). PK = sha256(linkedin_url_norm | company_key), where company_key
-- degrades  domain_norm -> company_record_id -> hash(full payload)  so a missing company never
-- silently collapses distinct observations. person_id (indexed) is the per-person email-rail key.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.clay_find_people (
    -- identity / lineage
    record_id         text        PRIMARY KEY,             -- sha256(linkedin_url_norm | company_key)
    person_id         text        NOT NULL,                -- sha256(linkedin_url_norm) — email-rail dedup key
    linkedin_url_raw  text        NOT NULL,                -- raw_payload.url, verbatim -> blitz email finder
    linkedin_url_norm text        NOT NULL,
    domain_norm       text,                                -- normalized raw_payload.domain -> FK firmographics_blitz
    -- exploded projection of the Clay payload (verbatim values; NOT normalized)
    domain_raw        text,                                -- raw_payload.domain (pre-norm)
    full_name         text,                                -- raw_payload.name
    first_name        text,
    last_name         text,
    location_name     text,
    follower_count    bigint,
    company_table_id  text,                                -- Clay table id
    company_record_id text,                                -- Clay company-record id (company_key fallback)
    matched_job_title    text,                             -- matched_experience.job_title (verbatim)
    matched_company_name text,                             -- matched_experience.company_name
    matched_start_date   text,                             -- matched_experience.start_date (kept as text)
    loc_city          text,                                -- structured_location.city
    loc_state         text,
    loc_region        text,
    loc_country       text,
    loc_country_iso   text,
    latest_experience_title      text,
    latest_experience_company    text,
    latest_experience_start_date text,
    -- raw source of truth + lineage
    source            text        NOT NULL DEFAULT 'clay_find_people',
    raw_payload       jsonb       NOT NULL,                -- the Clay object, EXACTLY as sent
    landed_at         timestamptz NOT NULL DEFAULT now()
);

-- Upgrade an already-deployed table (created before the exploded columns existed). No-op on fresh installs.
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS domain_raw        text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS full_name         text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS first_name        text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS last_name         text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS location_name     text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS follower_count    bigint;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS company_table_id  text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS company_record_id text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS matched_job_title    text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS matched_company_name text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS matched_start_date   text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS loc_city          text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS loc_state         text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS loc_region        text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS loc_country       text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS loc_country_iso   text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS latest_experience_title      text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS latest_experience_company    text;
ALTER TABLE gtm.clay_find_people ADD COLUMN IF NOT EXISTS latest_experience_start_date text;

-- batch_id removed from the contract (Clay fires one row per request — there is no batch grain).
-- Drop it idempotently so an already-deployed table converges to the new shape. It was pure lineage
-- metadata, never an identity key, so dropping it cannot collapse or corrupt the (person x company) grain.
DROP INDEX  IF EXISTS gtm.clay_find_people_batch_idx;
ALTER TABLE gtm.clay_find_people DROP COLUMN IF EXISTS batch_id;

CREATE INDEX IF NOT EXISTS clay_find_people_person_idx ON gtm.clay_find_people (person_id);
CREATE INDEX IF NOT EXISTS clay_find_people_domain_idx ON gtm.clay_find_people (domain_norm);
CREATE INDEX IF NOT EXISTS clay_find_people_landed_idx ON gtm.clay_find_people (landed_at DESC);
