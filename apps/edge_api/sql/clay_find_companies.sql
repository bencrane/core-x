-- Clay Find Companies — raw landing grain (append-only). Applied to the hqx control-plane
-- Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run; the
-- ALTER ADD COLUMN IF NOT EXISTS block upgrades an already-deployed table in place).
--
-- CONTRACT. Each Clay record is stored TWO ways, both faithful to source:
--   1. raw_payload (jsonb) — the object EXACTLY as sent. Immutable source of truth; drift-proof.
--   2. flat typed columns — a LOSSLESS structural projection of the known fields, stored AS-IS.
--      This is NOT normalization. Semantic normalization (annual_revenue band -> numeric,
--      size band -> employee_count) is a SEPARATE downstream stage; annual_revenue, size,
--      total_funding_amount_range_usd hold the verbatim Clay bands.
--      Array/object fields (industries, structured_locations, derived_datapoints) are projected
--      verbatim into their own jsonb columns; the scalar leaves of resolved_domain, the HQ
--      structured_locations entry, and derived_datapoints are additionally exploded into typed
--      columns for pushdown filtering.
-- The only computed columns are lossless identity keys read server-side from the payload:
--   raw_payload.linkedin_url -> linkedin_url_raw (verbatim) + linkedin_url_norm + company_id
--   raw_payload.domain       -> domain_norm (FK -> firmographics_blitz.domain_norm)
--
-- GRAIN: one row per company. PK = record_id = sha256(company_key), where company_key degrades
--   linkedin_url_norm -> linkedin_company_id -> clay_company_id -> domain_norm
-- so a record lacking the LinkedIn handle still lands under the next-strongest stable id; a record
-- with NONE of these (unidentifiable) is rejected (422) rather than landed keyless. company_id
-- (indexed, sha256(linkedin_url_norm)) is the LinkedIn-canonical cross-source join key.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.clay_find_companies (
    -- identity / lineage
    record_id         text        PRIMARY KEY,             -- sha256(company_key)
    company_id        text,                                -- sha256(linkedin_url_norm) — LinkedIn-canonical join key (NULL when no linkedin_url)
    linkedin_url_raw  text,                                -- raw_payload.linkedin_url, verbatim
    linkedin_url_norm text,
    domain_norm       text,                                -- normalized raw_payload.domain -> FK firmographics_blitz
    domain_raw        text,                                -- raw_payload.domain (pre-norm)
    clay_company_id   bigint,                              -- Clay stable company id
    linkedin_company_id bigint,                            -- LinkedIn numeric company id (vanity-rename-proof)
    -- exploded projection of the Clay payload (verbatim values; NOT normalized)
    name              text,                                -- raw_payload.name
    size              text,                                -- verbatim band, e.g. "10,001+ employees"
    company_type      text,                                -- raw_payload.type, e.g. "Public Company"
    industry          text,                                -- primary industry (verbatim)
    country           text,
    location          text,                                -- freeform, e.g. "Melbourne, Florida"
    description       text,
    annual_revenue    text,                                -- verbatim band, e.g. "10B-100B"
    total_funding_amount_range_usd text,                   -- verbatim band, e.g. "$250M+"
    -- resolved_domain.* scalar leaves
    domain_resolved   text,                                -- resolved_domain.resolved_domain
    domain_is_live    boolean,                             -- resolved_domain.is_live
    domain_redirects  boolean,                             -- resolved_domain.redirects_to_another_domain
    -- headquarters structured_locations[] entry (is_headquarters=true, else first) scalar leaves
    hq_city           text,
    hq_state          text,
    hq_region         text,
    hq_country_iso    text,
    hq_postal_code    text,
    -- derived_datapoints.* scalar leaves (LLM enrichment; absent on some records)
    derived_business_stage text,                           -- derived_datapoints.business_stage
    derived_scale_scope    text,                           -- derived_datapoints.scale_scope
    derived_pattern_tags   text,                           -- derived_datapoints.pattern_tags
    derived_description    text,                           -- derived_datapoints.description
    -- structural array/object projections (verbatim; named jsonb columns over digging raw_payload)
    industries        jsonb,                               -- raw_payload.industries[]
    structured_locations jsonb,                            -- raw_payload.structured_locations[]
    derived_datapoints   jsonb,                            -- raw_payload.derived_datapoints (whole sub-object)
    -- raw source of truth + lineage
    source            text        NOT NULL DEFAULT 'clay_find_companies',
    raw_payload       jsonb       NOT NULL,                -- the Clay object, EXACTLY as sent
    landed_at         timestamptz NOT NULL DEFAULT now()
);

-- Upgrade an already-deployed table (created before later columns existed). No-op on fresh installs.
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS company_id        text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS linkedin_url_raw  text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS linkedin_url_norm text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS domain_norm       text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS domain_raw        text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS clay_company_id   bigint;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS linkedin_company_id bigint;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS name              text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS size              text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS company_type      text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS industry          text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS country           text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS location          text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS description       text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS annual_revenue    text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS total_funding_amount_range_usd text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS domain_resolved   text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS domain_is_live    boolean;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS domain_redirects  boolean;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS hq_city           text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS hq_state          text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS hq_region         text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS hq_country_iso    text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS hq_postal_code    text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS derived_business_stage text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS derived_scale_scope    text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS derived_pattern_tags   text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS derived_description    text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS industries        jsonb;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS structured_locations jsonb;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS derived_datapoints   jsonb;

CREATE INDEX IF NOT EXISTS clay_find_companies_company_idx ON gtm.clay_find_companies (company_id);
CREATE INDEX IF NOT EXISTS clay_find_companies_domain_idx  ON gtm.clay_find_companies (domain_norm);
CREATE INDEX IF NOT EXISTS clay_find_companies_clay_id_idx ON gtm.clay_find_companies (clay_company_id);
CREATE INDEX IF NOT EXISTS clay_find_companies_li_id_idx   ON gtm.clay_find_companies (linkedin_company_id);
CREATE INDEX IF NOT EXISTS clay_find_companies_landed_idx  ON gtm.clay_find_companies (landed_at DESC);
