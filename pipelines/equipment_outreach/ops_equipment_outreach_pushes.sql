-- ops.equipment_outreach_pushes — the outreach suppression ledger.
-- One row per (person, campaign) push into Clay. Written ONLY by edge_api
-- POST /internal/equipment-outreach/pushes (trigger.dev equipment_outreach_push
-- task calls it after a successful Clay-webhook delivery). Read by catalyst_api
-- POST /api/v1/equipment-audience/select as the anti-join.
CREATE TABLE IF NOT EXISTS ops.equipment_outreach_pushes (
    person_key    text        NOT NULL,
    campaign_id   text        NOT NULL,
    macro_region  text,
    batch_label   text,
    pushed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (person_key, campaign_id)
);
CREATE INDEX IF NOT EXISTS idx_equipment_outreach_pushes_campaign
    ON ops.equipment_outreach_pushes (campaign_id);
