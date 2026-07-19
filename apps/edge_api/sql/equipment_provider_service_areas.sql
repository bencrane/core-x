-- Equipment-yard industries-served research — raw landing grain (append-only). Applied to
-- the hqx control-plane Postgres that edge_api writes. Idempotent DDL (safe to re-run).
--
-- The website service-area/geography payload for one equipment provider (summary / reasoning /
-- confidence / stepsTaken / serviceAreas[] with type/value/parsed/sourceUrl), produced against the operator's provider
-- roster (hq/rosters/2026-07-18-equipment-providers-885.csv) and sent back one row per
-- request. DOMAIN-keyed: the beyond-SAM provider plane (roster
-- hq/rosters/2026-07-18-equipment-providers-beyond-sam-1785.csv) has no UEI;
-- company_domain is the identity and domain_norm the canonical bridge.
--
-- CONTRACT — RAW ONLY, NO EXPLODE. raw_payload (jsonb) lands EXACTLY as sent — the
-- immutable, drift-proof source of truth. Normalization (serviceAreas → states/centroids/radii
--  is a downstream Lance-materializer concern, never this surface's.
-- The connect key is the UEI the operator sent out and is POSTed back as a TOP-LEVEL
-- field (the payload itself does not carry it), so it is a plain column set by the lander.
--
-- GRAIN: one row per landed payload. PK = record_id = sha256(uei + canonical raw_payload) —
-- byte-identical re-sends collapse (ON CONFLICT DO NOTHING, first-write-wins); a
-- re-research of the same yard hashes differently and lands as a new immutable row.

CREATE TABLE IF NOT EXISTS gtm.equipment_provider_service_areas (
    record_id      text PRIMARY KEY,
    company_domain text NOT NULL,
    domain_norm    text NOT NULL,
    source      text NOT NULL,
    raw_payload jsonb NOT NULL,
    landed_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS equipment_provider_service_areas_domain_idx
    ON gtm.equipment_provider_service_areas (domain_norm);
