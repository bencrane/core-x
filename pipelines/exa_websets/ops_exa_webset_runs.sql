-- Terminal-state + credit-ledger tables for the Exa Webset ingestion worker (Directive 22).
-- Written by pipelines/exa_websets/ingest.py via psycopg (HQX_DB_URL_POOLED) on every
-- terminal state — mirrors the ops.* contract used by every other feed (ARCHITECTURE.md §5).
-- Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and self-applies it
-- defensively before each terminal write (and via `modal run …::init_ops`). Keep the two in sync.
--
-- Lives in the HQX control-plane DB (where every other ops.*_runs table lives), NOT in any
-- source DB. Two tables:
--   ops.exa_webset_runs    — one row per ingest run (upsert on run_id), full credit + count audit.
--   ops.exa_credit_ledger  — one row per calendar month; the budget guard. The worker RESERVES
--                            projected credits at webset-create and RECONCILES to actual at
--                            completion. month_remaining = month_cap - max(reserved, actual).
--                            §11 ratified: month_cap = 100000 (D1, lower-tier plan).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.exa_webset_runs (
    run_id            text PRIMARY KEY,          -- == Trigger ctx.run.id (externalId = exa-webset-<run_id>)
    exa_webset_id     text,                       -- Exa's native webset id ('harvest-<run_id>' for Tier B)
    webset_identifier text NOT NULL,              -- slug, e.g. 'osha_defense_firms'
    webset_label      text NOT NULL,              -- origin flag, e.g. 'osha_defense_firms_2026'
    search_prompt     text NOT NULL,
    tier              text NOT NULL DEFAULT 'precision',  -- 'precision' (Websets) | 'harvest' (findSimilar/search)
    status            text NOT NULL,              -- success | timeout_partial | rejected | dry_run | failed
    requested         integer,                    -- clamped count (≤ 1000, D4)
    returned          integer,                    -- items pulled from Exa
    new_count         integer,                    -- routed to discovered_websets
    known_count       integer,                    -- routed to webset_membership
    credits_estimated integer,                    -- pre-flight projection (reserved)
    credits_actual    integer,                    -- returned × per-item rate (Tier A)
    usd_actual        numeric(12,4),              -- Tier A: credits×rate · Tier B: Σ costDollars.total
    rejected_reason   text,
    started_at        timestamptz,
    finished_at       timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS exa_webset_runs_status_idx   ON ops.exa_webset_runs (status);
CREATE INDEX IF NOT EXISTS exa_webset_runs_label_idx    ON ops.exa_webset_runs (webset_label);
CREATE INDEX IF NOT EXISTS exa_webset_runs_recorded_idx ON ops.exa_webset_runs (recorded_at DESC);

CREATE TABLE IF NOT EXISTS ops.exa_credit_ledger (
    month            date   PRIMARY KEY,          -- first-of-month bucket
    credits_reserved bigint NOT NULL DEFAULT 0,   -- held at create, released on clean terminal
    credits_actual   bigint NOT NULL DEFAULT 0,   -- accumulated real spend
    month_cap        bigint NOT NULL              -- §11 D1 = 100000
);
