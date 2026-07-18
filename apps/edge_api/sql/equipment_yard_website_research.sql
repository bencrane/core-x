-- Equipment-yard website research — raw landing grain (append-only). Applied to the hqx
-- control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run).
--
-- The research payload for one candidate equipment-rental yard (evidence / reasoning /
-- categories / confidence / stepsTaken / providerModes / equipmentItems — including explicit
-- "not an equipment provider" verdicts), produced against the operator's outbound roster
-- (hq/rosters/2026-07-18-equipment-yards-clay-roster.csv) and sent back one row per request.
--
-- CONTRACT — RAW ONLY, NO EXPLODE. raw_payload (jsonb) lands EXACTLY as sent — the immutable,
-- drift-proof source of truth. No typed unfurl here; normalization (equipmentItems → PSC
-- buckets, geographies → centroids) is a downstream Lance-materializer concern, never this
-- surface's. The connect key is the UEI the operator sent out and is POSTed back as a
-- TOP-LEVEL field (the payload itself does not carry it), so it is a plain column set by
-- the lander.
--
-- GRAIN: one row per landed research payload. PK = record_id = sha256(uei + canonical
-- raw_payload) — the same payload under two UEIs stays distinct; a byte-identical re-send
-- collapses (ON CONFLICT DO NOTHING, first-write-wins). Append-only: a re-research of the
-- same yard hashes differently and lands as a new immutable row.

CREATE TABLE IF NOT EXISTS gtm.equipment_yard_website_research (
    record_id   text PRIMARY KEY,
    uei         text NOT NULL,
    source      text NOT NULL,
    raw_payload jsonb NOT NULL,
    landed_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS equipment_yard_website_research_uei_idx
    ON gtm.equipment_yard_website_research (uei);
