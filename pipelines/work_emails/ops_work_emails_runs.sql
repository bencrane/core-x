-- ops.work_emails_runs — shared per-run terminal-state ledger for the work-email Lance
-- trinity (Directive 21/28 follow-on). ONE ledger, three feeds: the three materializers
--   work_emails                  (master, latest-wins per contact_id)
--   work_email_vendor_responses  (provider raw — icypeas | leadmagic | blitz)
--   work_email_mv_validations    (MillionVerifier responses, verbatim)
-- each write a row here distinguished by `feed`. Mirrors ops.email_verifications_runs /
-- ops.firmographics_blitz_runs. Lives in the HQX control-plane DB (HQX_DB_URL_POOLED).
--
-- Each materializer embeds this DDL verbatim and applies it idempotently before every
-- terminal write, so the ledger self-bootstraps. Forward-only; IF NOT EXISTS throughout.
--
--   modal run pipelines/work_emails/materialize_work_emails.py::init_ops

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.work_emails_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,   -- work_emails | work_email_vendor_responses | work_email_mv_validations
    source_db      text        NOT NULL,   -- provenance label (hqx:ops.email_resolutions[+email_verifications])
    datasets       jsonb       NOT NULL,   -- {<dataset>: rows_total}
    mode           text        NOT NULL DEFAULT 'overwrite',   -- overwrite | append
    rows_total     bigint      NOT NULL DEFAULT 0,
    rows_source    bigint      NOT NULL DEFAULT 0,
    rows_added     bigint      NOT NULL DEFAULT 0,
    watermark      timestamptz,
    status         text        NOT NULL,   -- success | error
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS work_emails_runs_feed_idx        ON ops.work_emails_runs (feed);
CREATE INDEX IF NOT EXISTS work_emails_runs_status_idx      ON ops.work_emails_runs (status);
CREATE INDEX IF NOT EXISTS work_emails_runs_recorded_at_idx ON ops.work_emails_runs (recorded_at DESC);
