-- company_capital_providers — raw landing grain for the capital-provider classification
-- enrichment (append-only). Applied to the hqx control-plane Postgres (HQX_DB_URL_POOLED)
-- that edge_api writes. Idempotent DDL (safe to re-run).
--
-- PROVENANCE. capitalType / providesCapital are NOT a prospeo.io vendor field — they are the
-- output of an IN-HOUSE LLM classification pass run over the prospeo-export company set. The
-- business fields (company_name / domain / company_linkedin_url / description) ride alongside on
-- the wire; raw_payload is the verbatim LLM object.
--
-- CONTRACT. raw_payload shape (EXACTLY as the LLM tool emits it):
--   { reasoning, confidence, sourceUrls[], stepsTaken[], capitalType, evidencePhrases[],
--     providesCapital (bool), totalInputTokens, totalOutputTokens, timeTakenInSeconds,
--     totalCostToAIProvider, forcedToFinishEarlyBecauseOfCost }
-- Stored TWO ways:
--   1. raw_payload (jsonb) — the object EXACTLY as sent. Immutable source of truth (so re-running
--      the same enrichment on new entities just appends; the full object is always recoverable).
--   2. flat typed columns — lossless projection. domain stored verbatim; domain_norm is the
--      canonical bridge key (mirrors firmographics_blitz._normalized_domain).
--
-- GRAIN: one row per (domain_norm × canonical-raw_payload). PK = record_id = sha256(domain_norm
-- "|" sha256(canonical_json(raw_payload))). Byte-identical resends are idempotent via
-- ON CONFLICT DO NOTHING. A different payload for the same domain lands as a DISTINCT row —
-- the enrichment is append-only history.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.company_capital_providers (
    record_id                       text        PRIMARY KEY,  -- sha256(domain_norm|sha256(raw_payload))
    company_name                    text,                     -- verbatim as sent (nullable)
    domain                          text        NOT NULL,     -- verbatim as sent
    domain_norm                     text        NOT NULL,     -- canonical bridge → firmographics_blitz.domain_norm
    company_linkedin_url            text,                     -- nullable (if known)
    description                     text,                     -- prospeo company description (nullable)
    capital_provider_type_category  text,                     -- raw_payload->>'capitalType' (e.g. equipmentFinancing)
    provides_capital                boolean,                  -- raw_payload->>'providesCapital'
    confidence                      text,                     -- "low"/"medium"/"high" — verbatim
    reasoning                       text,                     -- LLM justification, free text
    source_urls                     jsonb,                    -- text[] — sourceUrls[] from payload
    evidence_phrases                jsonb,                    -- text[] — evidencePhrases[] from payload
    steps_taken                     jsonb,                    -- text[] — stepsTaken[] (preserves order)
    source                          text        NOT NULL DEFAULT 'prospeo_capital_llm',
    raw_payload                     jsonb       NOT NULL,
    landed_at                       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS company_capital_providers_domain_norm_idx ON gtm.company_capital_providers (domain_norm);
CREATE INDEX IF NOT EXISTS company_capital_providers_category_idx    ON gtm.company_capital_providers (capital_provider_type_category);
CREATE INDEX IF NOT EXISTS company_capital_providers_provides_idx    ON gtm.company_capital_providers (provides_capital);
CREATE INDEX IF NOT EXISTS company_capital_providers_landed_at_idx   ON gtm.company_capital_providers (landed_at DESC);

-- clay_table_url — optional provenance: which Clay workbook table produced the payload.
ALTER TABLE gtm.company_capital_providers ADD COLUMN IF NOT EXISTS clay_table_url text;
