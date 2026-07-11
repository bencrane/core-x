-- combo_work_summary_equipment_needs — raw landing sink for LLM equipment-needs
-- verdicts at the NAICS x PSC combo grain. Applied to the hqx control-plane
-- Postgres that edge_api writes. Idempotent DDL (safe to re-run).
--
-- Each row = one LLM verdict ("what equipment does performing this combo's work
-- require") produced upstream in Clay (GPT) from the combo's work_summary /
-- deliverable context. raw_payload is stored EXACTLY as sent — no projection of
-- response/reasoning, no comma-splitting of the equipment list, no taxonomy
-- normalization. Unfurling into equipment rows and Lance materialization are
-- downstream concerns.
--
-- GRAIN: one row per (naics_code, psc_code, model_id). Re-ingest UPSERTs
-- (overwrites prior payload for the same combo+model). A different model_id
-- lands as a DISTINCT row, so re-runs on a new model accumulate rather than
-- clobber a prior model's verdict.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.combo_work_summary_equipment_needs (
    naics_code   text        NOT NULL,
    psc_code     text        NOT NULL,
    model_id     text        NOT NULL DEFAULT 'gpt-5.4-nano',
    source       text        NOT NULL DEFAULT 'clay',
    raw_payload  jsonb       NOT NULL,               -- verbatim LLM object — no projection
    landed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (naics_code, psc_code, model_id)
);
-- PK btree only. No secondary indexes by design — deferred-processing sink.
