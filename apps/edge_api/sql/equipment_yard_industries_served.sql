-- Equipment-yard industries-served research — raw landing grain (append-only). Applied to
-- the hqx control-plane Postgres that edge_api writes. Idempotent DDL (safe to re-run).
--
-- The industries/verticals-served payload for one equipment provider (sources / reasoning /
-- confidence / stepsTaken / industriesServed[]), produced against the operator's provider
-- roster (hq/rosters/2026-07-18-equipment-providers-885.csv) and sent back one row per
-- request.
--
-- CONTRACT — RAW ONLY, NO EXPLODE. raw_payload (jsonb) lands EXACTLY as sent — the
-- immutable, drift-proof source of truth. Normalization (industriesServed → a canonical
-- vertical taxonomy) is a downstream Lance-materializer concern, never this surface's.
-- The connect key is the UEI the operator sent out and is POSTed back as a TOP-LEVEL
-- field (the payload itself does not carry it), so it is a plain column set by the lander.
--
-- GRAIN: one row per landed payload. PK = record_id = sha256(uei + canonical raw_payload) —
-- byte-identical re-sends collapse (ON CONFLICT DO NOTHING, first-write-wins); a
-- re-research of the same yard hashes differently and lands as a new immutable row.

CREATE TABLE IF NOT EXISTS gtm.equipment_yard_industries_served (
    record_id   text PRIMARY KEY,
    uei         text NOT NULL,
    source      text NOT NULL,
    raw_payload jsonb NOT NULL,
    landed_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS equipment_yard_industries_served_uei_idx
    ON gtm.equipment_yard_industries_served (uei);
