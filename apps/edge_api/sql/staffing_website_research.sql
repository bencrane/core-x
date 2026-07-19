-- Staffing-agency website research — raw landing grain (append-only). Applied to the hqx
-- control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run).
--
-- The research payload for one SAM-matched staffing agency (rolesPlaced / placementModel /
-- workCategories / geographiesServed / clearanceAndFederalIntent / reasoning / confidence /
-- stepsTaken), produced against the operator's outbound CSV
-- (staffing_agencies_sam_matched_1-500_*.csv) and sent back one row per request.
--
-- CONTRACT — RAW ONLY, NO EXPLODE. raw_payload (jsonb) lands EXACTLY as sent — the immutable,
-- drift-proof source of truth. No typed unfurl here; normalization (geo → FIPS, roles → SOC via
-- occupation_alias_lookup) is a downstream Lance-materializer concern, never this surface's.
-- The connect key is the UEI the operator sent out and is POSTed back as a TOP-LEVEL field
-- (the payload itself does not carry it), so it is a plain column set by the lander.
--
-- GRAIN: one row per landed research payload. PK = record_id = sha256(uei + canonical
-- raw_payload) — the same payload under two UEIs stays distinct; a byte-identical re-send
-- collapses (ON CONFLICT DO NOTHING, first-write-wins). Append-only: a re-research of the same
-- agency hashes differently and lands as a new immutable row.

CREATE TABLE IF NOT EXISTS gtm.staffing_website_research (
    record_id   text PRIMARY KEY,
    uei         text NOT NULL,
    source      text NOT NULL,
    raw_payload jsonb NOT NULL,
    landed_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS staffing_website_research_uei_idx
    ON gtm.staffing_website_research (uei);

-- 2026-07-18b: domain-keyed sibling landings (the non-SAM staffing population has no UEI;
-- normalized domain is its connect key). Exactly one of (uei, domain) per row — enforced by
-- the lander, not a constraint, to keep the DDL idempotent and additive.
ALTER TABLE gtm.staffing_website_research ALTER COLUMN uei DROP NOT NULL;
ALTER TABLE gtm.staffing_website_research ADD COLUMN IF NOT EXISTS domain text;
CREATE INDEX IF NOT EXISTS staffing_website_research_domain_idx
    ON gtm.staffing_website_research (domain);
