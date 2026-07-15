-- combo_job_to_be_done — raw landing sink for LLM "to: <verb> <object>" job
-- sentences at the NAICS x PSC combo grain. Applied to the hqx control-plane
-- Postgres that edge_api writes. Idempotent DDL (safe to re-run).
--
-- Each row = one LLM rewrite of the combo's work_summary into the canonical
-- typeable job-to-be-done sentence ("to: build fixed-wing aircraft") produced
-- upstream (GPT batch; a later Opus pass may land under a different model_id).
-- output_sentence is stored EXACTLY as sent — no trimming, no normalization.
-- Vocabulary normalization into the phrase grammar is a downstream concern.
--
-- GRAIN: one row per (naics_code, psc_code, model_id). Re-ingest UPSERTs
-- (overwrites prior sentence for the same combo+model). A different model_id
-- lands as a DISTINCT row, so re-runs on a new model accumulate rather than
-- clobber a prior model's output.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.combo_job_to_be_done (
    naics_code       text        NOT NULL,
    psc_code         text        NOT NULL,
    model_id         text        NOT NULL DEFAULT 'gpt-5.4',
    source           text        NOT NULL DEFAULT 'clay',
    output_sentence  text        NOT NULL,            -- verbatim LLM sentence — no trimming
    landed_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (naics_code, psc_code, model_id)
);
-- PK btree only. No secondary indexes by design — deferred-processing sink.
