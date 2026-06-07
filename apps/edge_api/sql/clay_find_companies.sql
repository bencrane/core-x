-- Clay Find Companies — raw landing grain (append-only). Applied to the hqx control-plane
-- Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run; the
-- ALTER block converges an already-deployed table in place).
--
-- CONTRACT. Each Clay record is stored TWO ways, both faithful to source:
--   1. raw_payload (jsonb) — the object EXACTLY as sent. Immutable source of truth; drift-proof.
--   2. flat typed columns — a LOSSLESS structural projection of the known fields, stored AS-IS.
--      This surface does ZERO normalization. linkedin_url and domain are stored VERBATIM — no
--      scheme/www/sub-domain/case stripping. Bands (annual_revenue, size,
--      total_funding_amount_range_usd) are kept as-sent. Semantic normalization (URL
--      canonicalization, band -> numeric, domain -> firmographics FK) is a SEPARATE downstream
--      stage. Array/object fields (industries, structured_locations, derived_datapoints) land
--      verbatim in their own jsonb columns; the scalar leaves of resolved_domain, the HQ
--      structured_locations entry, and derived_datapoints are exploded into typed columns.
--
-- GRAIN: one row per company. PK = record_id = sha256(company_key), where company_key is the
-- VERBATIM identity, degrading  linkedin_url -> domain -> linkedin_company_id -> clay_company_id.
-- A record with none of these (unidentifiable) is rejected (422) rather than landed keyless.
-- Because the key is the raw string, www/trailing-slash/case variants of the same URL land as
-- DISTINCT rows by design — deduping them is a downstream normalization concern.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.clay_find_companies (
    -- identity / lineage
    record_id         text        PRIMARY KEY,             -- sha256(company_key), verbatim key
    linkedin_url      text,                                -- raw_payload.linkedin_url, VERBATIM (no normalization)
    domain            text,                                -- raw_payload.domain, VERBATIM (no normalization)
    clay_company_id   bigint,                              -- Clay stable company id
    linkedin_company_id bigint,                            -- LinkedIn numeric company id
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

-- Upgrade an already-deployed table. ADD the verbatim identity columns; no-op on fresh installs.
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS linkedin_url text;
ALTER TABLE gtm.clay_find_companies ADD COLUMN IF NOT EXISTS domain       text;

-- Converge a table created with the earlier NORMALIZED shape: drop the derived/normalized columns
-- (and their indexes) so the surface lands strictly verbatim. Idempotent; no-op on fresh installs.
-- Safe on data: these were pure derived keys, recomputable downstream from linkedin_url / domain.
DROP INDEX  IF EXISTS gtm.clay_find_companies_company_idx;
DROP INDEX  IF EXISTS gtm.clay_find_companies_domain_idx;   -- was on domain_norm; recreated on domain below
ALTER TABLE gtm.clay_find_companies DROP COLUMN IF EXISTS company_id;
ALTER TABLE gtm.clay_find_companies DROP COLUMN IF EXISTS linkedin_url_norm;
ALTER TABLE gtm.clay_find_companies DROP COLUMN IF EXISTS domain_norm;
ALTER TABLE gtm.clay_find_companies DROP COLUMN IF EXISTS linkedin_url_raw;
ALTER TABLE gtm.clay_find_companies DROP COLUMN IF EXISTS domain_raw;

CREATE INDEX IF NOT EXISTS clay_find_companies_linkedin_idx ON gtm.clay_find_companies (linkedin_url);
CREATE INDEX IF NOT EXISTS clay_find_companies_domain_idx   ON gtm.clay_find_companies (domain);
CREATE INDEX IF NOT EXISTS clay_find_companies_clay_id_idx  ON gtm.clay_find_companies (clay_company_id);
CREATE INDEX IF NOT EXISTS clay_find_companies_li_id_idx    ON gtm.clay_find_companies (linkedin_company_id);
CREATE INDEX IF NOT EXISTS clay_find_companies_landed_idx   ON gtm.clay_find_companies (landed_at DESC);
