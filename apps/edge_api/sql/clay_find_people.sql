-- Clay Find People — raw landing grain (append-only). Applied to the hqx control-plane
-- Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL.
--
-- CONTRACT. Each Clay find-people record lands VERBATIM in raw_payload (jsonb). Nothing is
-- exploded/flattened. The only derived columns are lossless identity keys read server-side
-- out of the payload — the caller sends the object as-is and does NOT pre-extract a
-- person_linkedin_url:
--     raw_payload.url     -> linkedin_url_raw (fed to blitz /v2/enrichment/email)
--                            + linkedin_url_norm + person_id
--     raw_payload.domain  -> domain_norm  (FK -> firmographics_blitz.domain_norm)
-- Title/role normalization and email enrichment are SEPARATE downstream stages off this table.
--
-- GRAIN: (person x company-record). The same LinkedIn URL can attach to multiple domains
-- (Clay match noise — e.g. a person whose matched_experience.company_name != the attached
-- domain), so the PK preserves every (person, domain) attachment rather than collapsing on
-- person. person_id stays as a non-unique, indexed column = the per-person email-rail key.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.clay_find_people (
    record_id         text        PRIMARY KEY,             -- sha256(linkedin_url_norm | domain_norm)
    person_id         text        NOT NULL,                -- sha256(linkedin_url_norm) — email-rail dedup key
    linkedin_url_raw  text        NOT NULL,                -- raw_payload.url, verbatim -> blitz email finder
    linkedin_url_norm text        NOT NULL,
    domain_norm       text,                                -- normalized raw_payload.domain -> FK firmographics_blitz
    source            text        NOT NULL DEFAULT 'clay_find_people',
    batch_id          text,
    raw_payload       jsonb       NOT NULL,                -- the Clay object, EXACTLY as sent
    landed_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS clay_find_people_person_idx ON gtm.clay_find_people (person_id);
CREATE INDEX IF NOT EXISTS clay_find_people_domain_idx ON gtm.clay_find_people (domain_norm);
CREATE INDEX IF NOT EXISTS clay_find_people_batch_idx  ON gtm.clay_find_people (batch_id);
CREATE INDEX IF NOT EXISTS clay_find_people_landed_idx ON gtm.clay_find_people (landed_at DESC);
