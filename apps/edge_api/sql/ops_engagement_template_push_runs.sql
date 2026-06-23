-- QA ledger for the engagement-template render+push lane (content source -> DocRaptor PDF -> Documenso
-- TEMPLATE). Written by apps/edge_api/src/engagement_templates/push.py on EVERY terminal state
-- (success | error), via the edge_api async pool (HQX_DB_URL_POOLED). Fire-and-forget: a ledger error
-- never blocks or fails the render-push response.
--
-- One row per render+push attempt: which content source was pulled (brand/path/archetype/version/
-- source_kind), the style rendered, the Documenso template handle minted, and the failure reason on
-- error — the audit stamp that makes "send this through" traceable. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.engagement_template_push_runs (
    id                    bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts                    timestamptz NOT NULL DEFAULT now(),
    run_id                text,                   -- Trigger.dev run id (ctx.run.id); NULL for a direct/manual call
    brand                 text        NOT NULL,   -- 'active-operators' | 'rare-structure'
    path                  text        NOT NULL,   -- catalog template-family segment
    archetype             text        NOT NULL,
    version               text        NOT NULL,
    style                 text,                   -- 'plain' | 'branded' (resolved)
    source_kind           text        NOT NULL,   -- 'repo-html' | 'db-markdown'
    status                text        NOT NULL,   -- 'success' | 'error'
    documenso_template_id text,                   -- envelope/template handle returned by Documenso
    documenso_numeric_id  bigint,                 -- secondary numeric id
    pdf_r2_key            text,                   -- audit copy of the rendered PDF in R2 (NULL if not stored)
    pdf_bytes             integer,
    error                 text
);

CREATE INDEX IF NOT EXISTS engagement_template_push_runs_ts_idx
    ON ops.engagement_template_push_runs (ts DESC);
CREATE INDEX IF NOT EXISTS engagement_template_push_runs_status_idx
    ON ops.engagement_template_push_runs (status);
CREATE INDEX IF NOT EXISTS engagement_template_push_runs_path_idx
    ON ops.engagement_template_push_runs (brand, path, archetype, version);
