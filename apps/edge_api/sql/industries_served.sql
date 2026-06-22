-- industries_served — raw landing grain for company-industries research payloads (append-only).
-- Applied to the hqx control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent
-- DDL (safe to re-run; the ALTER block converges an already-deployed table in place).
--
-- CONTRACT. One shape only — the industries-served research output:
--    { sources[], reasoning, confidence, stepsTaken[], industriesServed[] }
-- Each record is stored TWO ways:
--   1. raw_payload (jsonb) — the object EXACTLY as sent. Immutable source of truth.
--   2. flat typed columns — lossless structural projection. company_domain stored verbatim;
--      domain_norm is the canonical bridge key (mirrors firmographics_blitz._normalized_domain).
--
-- GRAIN: one row per (domain_norm × canonical-raw_payload). PK = record_id = sha256(domain_norm
-- "|" sha256(canonical_json(raw_payload))). Byte-identical resends are idempotent via
-- ON CONFLICT DO NOTHING. A different payload for the same domain lands as a DISTINCT row —
-- research outputs are append-only history.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.industries_served (
    record_id              text        PRIMARY KEY,    -- sha256(domain_norm|sha256(raw_payload))
    company_domain         text        NOT NULL,       -- verbatim as sent
    domain_norm            text        NOT NULL,       -- canonical bridge → firmographics_blitz.domain_norm
    confidence             text,                       -- "low"/"medium"/"high" — verbatim
    reasoning              text,                       -- research justification, free text
    sources                jsonb,                      -- text[] — explicit sources[] from payload
    steps_taken            jsonb,                      -- text[] — "Visited <url>" lines (preserves order)
    industries_served      jsonb,                      -- text[] — the industry labels
    industries_served_count integer,                   -- derived: len(industries_served)
    source                 text        NOT NULL DEFAULT 'industries_served',
    raw_payload            jsonb       NOT NULL,
    landed_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS industries_served_domain_norm_idx    ON gtm.industries_served (domain_norm);
CREATE INDEX IF NOT EXISTS industries_served_company_domain_idx ON gtm.industries_served (company_domain);
CREATE INDEX IF NOT EXISTS industries_served_confidence_idx     ON gtm.industries_served (confidence);
CREATE INDEX IF NOT EXISTS industries_served_landed_at_idx      ON gtm.industries_served (landed_at DESC);
