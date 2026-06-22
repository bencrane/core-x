-- equipment_provider — raw landing grain for company-is-equipment-provider classification payloads
-- (append-only). Applied to the hqx control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes.
-- Idempotent DDL (safe to re-run; the ALTER block converges an already-deployed table in place).
--
-- CONTRACT. One shape — the binary classification "is this company a qualifying equipment provider?"
--    { mode, reasoning, confidence, stepsTaken[], evidenceUrl, evidenceSnippet, isEquipmentProvider }
-- Each record is stored TWO ways:
--   1. raw_payload (jsonb) — the object EXACTLY as sent. Immutable source of truth.
--   2. flat typed columns — lossless structural projection. company_domain stored verbatim;
--      domain_norm is the canonical bridge key (mirrors firmographics_blitz._normalized_domain).
--
-- GRAIN: one row per (domain_norm × canonical-raw_payload). PK = record_id = sha256(domain_norm
-- "|" sha256(canonical_json(raw_payload))). Byte-identical resends idempotent via
-- ON CONFLICT DO NOTHING. A different payload for the same domain lands as a DISTINCT row —
-- research outputs are append-only history.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.equipment_provider (
    record_id              text        PRIMARY KEY,    -- sha256(domain_norm|sha256(raw_payload))
    company_domain         text        NOT NULL,       -- verbatim as sent
    domain_norm            text        NOT NULL,       -- canonical bridge → firmographics_blitz.domain_norm
    is_equipment_provider  boolean,                    -- the verdict
    mode                   text,                       -- 'rental'/'sale'/'lease'/'unclear'/... — verbatim
    confidence             text,                       -- 'low'/'medium'/'high'
    reasoning              text,                       -- research justification, free text
    steps_taken            jsonb,                      -- text[] — "Visited <url>" lines
    evidence_url           text,                       -- single supporting URL
    evidence_snippet       text,                       -- the supporting quote/snippet
    source                 text        NOT NULL DEFAULT 'equipment_provider',
    raw_payload            jsonb       NOT NULL,
    landed_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS equipment_provider_domain_norm_idx           ON gtm.equipment_provider (domain_norm);
CREATE INDEX IF NOT EXISTS equipment_provider_company_domain_idx        ON gtm.equipment_provider (company_domain);
CREATE INDEX IF NOT EXISTS equipment_provider_is_equipment_provider_idx ON gtm.equipment_provider (is_equipment_provider);
CREATE INDEX IF NOT EXISTS equipment_provider_mode_idx                  ON gtm.equipment_provider (mode);
CREATE INDEX IF NOT EXISTS equipment_provider_confidence_idx            ON gtm.equipment_provider (confidence);
CREATE INDEX IF NOT EXISTS equipment_provider_landed_at_idx             ON gtm.equipment_provider (landed_at DESC);
