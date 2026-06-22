-- equipment_catalog — raw landing grain for company-offerings research payloads (append-only).
-- Applied to the hqx control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent
-- DDL (safe to re-run; the ALTER block converges an already-deployed table in place).
--
-- CONTRACT. Each research record is stored TWO ways, both faithful to source:
--   1. raw_payload (jsonb) — the object EXACTLY as sent. Immutable source of truth; drift-proof.
--   2. flat typed columns — a LOSSLESS structural projection of the known fields, stored AS-IS.
--      The surface accepts heterogeneous payload shapes — "industries served" research outputs
--      (Ex 1) carry industriesServed[]; "equipment offerings" research outputs (Ex 2) carry
--      providerModes[], categories[], equipmentItems[], evidence[]. Sparse flat columns cover
--      both — null where a shape does not carry that field. payload_kind discriminates.
--      Verbatim — no scheme/www/case stripping on company_domain; domain_norm is the only
--      derived bridge key. Endpoint code introspects the payload to fill what is there.
--
-- BRIDGE. domain_norm = lower/trim → strip scheme → strip www → strip path → strip trailing dots
-- — the SAME canonical normalization as firmographics_blitz.domain_norm. BTREE-indexed for
-- pushdown joins from the companies Lance system-of-record.
--
-- GRAIN: one row per (domain_norm × canonical-raw_payload). PK = record_id = sha256(domain_norm
-- "|" sha256(canonical_json(raw_payload))). Byte-identical resends are idempotent (first-write-
-- wins via ON CONFLICT DO NOTHING; raw_payload is immutable). A different payload for the same
-- domain lands as a DISTINCT row by design — research outputs are append-only history, not a
-- most-recent-wins overwrite. A record without a resolvable domain_norm is rejected (422) rather
-- than landed keyless.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.equipment_catalog (
    -- identity / bridge
    record_id            text        PRIMARY KEY,    -- sha256(domain_norm|sha256(raw_payload))
    company_domain       text        NOT NULL,       -- verbatim as sent (no normalization)
    domain_norm          text        NOT NULL,       -- canonical bridge → firmographics_blitz.domain_norm
    payload_kind         text,                       -- inferred: industries_served|equipment_offerings|mixed|unknown
    -- common projection (both shapes)
    confidence           text,                       -- "low"/"medium"/"high" — verbatim
    reasoning            text,                       -- free text — research justification
    steps_taken          jsonb,                      -- text[] — "Visited <url>" lines (preserves order)
    sources              jsonb,                      -- text[] — explicit sources[] (Ex 1) or derived from evidence[].url (Ex 2)
    evidence             jsonb,                      -- Ex 2: evidence[] objects {url, note}
    -- "industries served" shape (Ex 1)
    industries_served    jsonb,                      -- text[] — explicit industry labels
    -- "equipment offerings" shape (Ex 2)
    provider_modes       jsonb,                      -- text[] — sell/rent/lease/...
    categories           jsonb,                      -- categories[] — full objects {name, evidenceUrl, evidenceSnippet}
    category_names       jsonb,                      -- text[] — derived from categories[].name (pushdown helper)
    equipment_items      jsonb,                      -- equipmentItems[] — full objects {name, evidenceUrl, categoryName, evidenceSnippet}
    equipment_item_names jsonb,                      -- text[] — derived from equipmentItems[].name (pushdown helper)
    equipment_item_count integer,                    -- derived: len(equipmentItems)
    -- raw source of truth + lineage
    source               text        NOT NULL DEFAULT 'equipment_catalog',
    raw_payload          jsonb       NOT NULL,       -- the object, EXACTLY as sent
    landed_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS equipment_catalog_domain_norm_idx    ON gtm.equipment_catalog (domain_norm);
CREATE INDEX IF NOT EXISTS equipment_catalog_company_domain_idx ON gtm.equipment_catalog (company_domain);
CREATE INDEX IF NOT EXISTS equipment_catalog_payload_kind_idx   ON gtm.equipment_catalog (payload_kind);
CREATE INDEX IF NOT EXISTS equipment_catalog_landed_at_idx      ON gtm.equipment_catalog (landed_at DESC);
