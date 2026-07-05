-- ops.company_scrape_* — Icypeas company-scrape submit ledger + run-state (hq-x control plane).
--
-- Canonical source-of-truth mirror of the OPS_DDL string in company_scrape.py (the worker applies it
-- defensively before every terminal write). Kept here as the reviewable schema artifact.
--
--   ops.company_scrape_submissions  PK company_url — the idempotency ledger. A url already
--       'submitted' is skipped on re-run (never re-spend a scrape credit) unless force=True; a prior
--       'submit_failed' is retryable. file_id correlates the url to its Icypeas bulk + to the landed
--       rows in business.icypeas_webhook_events (edge_api SoR).
--   ops.company_scrape_runs         per-run terminal counts (requested/skipped/submitted/batches/failed).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.company_scrape_submissions (
    company_url   text        PRIMARY KEY,           -- the LinkedIn company URL (dedup / idempotency key)
    file_id       text,                               -- Icypeas bulk file id this url was submitted in
    external_id   text,                               -- what we stamped at submit (== company_url)
    batch_label   text,
    run_root      text,
    status        text        NOT NULL,               -- 'submitted' | 'submit_failed'
    submitted_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT company_scrape_submissions_status_chk CHECK (status IN ('submitted', 'submit_failed'))
);
CREATE INDEX IF NOT EXISTS company_scrape_submissions_file_idx      ON ops.company_scrape_submissions (file_id);
CREATE INDEX IF NOT EXISTS company_scrape_submissions_submitted_idx ON ops.company_scrape_submissions (submitted_at DESC);

CREATE TABLE IF NOT EXISTS ops.company_scrape_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    batch_label   text,
    run_root      text,
    requested     bigint      NOT NULL DEFAULT 0,
    skipped       bigint      NOT NULL DEFAULT 0,
    submitted     bigint      NOT NULL DEFAULT 0,
    batches       bigint      NOT NULL DEFAULT 0,
    failed        bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT company_scrape_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS company_scrape_runs_feed_idx     ON ops.company_scrape_runs (feed);
CREATE INDEX IF NOT EXISTS company_scrape_runs_recorded_idx ON ops.company_scrape_runs (recorded_at DESC);
