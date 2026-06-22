-- equipment_finance_candidates — flat cohort landing for curated equipment-finance lender candidates
-- (append-only). Applied to the hqx control-plane Postgres (HQX_DB_URL_POOLED). Idempotent DDL.
--
-- CONTRACT. Curated list of equipment-finance / lender candidates (NOT a research payload — flat
-- name + domain + linkedin_url + verdict). Each record stored TWO ways:
--   1. raw_payload (jsonb) — the row as sent. Source of truth.
--   2. flat typed columns — company_name, company_domain/domain_norm (bridge), linkedin_url/_norm,
--      verdict. company_domain/linkedin_url verbatim; *_norm columns are the canonical bridge keys.
--
-- GRAIN: one row per (domain_norm or linkedin_url_norm) × verdict. PK = record_id =
-- sha256(domain_norm '|' linkedin_url_norm '|' verdict). Byte-identical resends idempotent via
-- ON CONFLICT DO NOTHING. A record without either a usable domain or LinkedIn URL is rejected.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.equipment_finance_candidates (
    record_id           text        PRIMARY KEY,    -- sha256(domain_norm|linkedin_url_norm|verdict)
    company_name        text        NOT NULL,       -- as-sent
    company_domain      text,                       -- verbatim as sent (may be NULL)
    domain_norm         text,                       -- canonical bridge → firmographics_blitz.domain_norm (may be NULL)
    linkedin_url        text,                       -- verbatim as sent (may be NULL)
    linkedin_url_norm   text,                       -- normalized: lower, strip scheme/www/trailing slash
    verdict             text,                       -- 'SKIP' or NULL (operator-marked at curation time)
    source              text        NOT NULL DEFAULT 'equipment_finance_candidates',
    raw_payload         jsonb       NOT NULL,
    landed_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS equip_fin_cand_domain_norm_idx     ON gtm.equipment_finance_candidates (domain_norm);
CREATE INDEX IF NOT EXISTS equip_fin_cand_linkedin_norm_idx   ON gtm.equipment_finance_candidates (linkedin_url_norm);
CREATE INDEX IF NOT EXISTS equip_fin_cand_company_name_idx    ON gtm.equipment_finance_candidates (company_name);
CREATE INDEX IF NOT EXISTS equip_fin_cand_verdict_idx         ON gtm.equipment_finance_candidates (verdict);
CREATE INDEX IF NOT EXISTS equip_fin_cand_landed_at_idx       ON gtm.equipment_finance_candidates (landed_at DESC);
