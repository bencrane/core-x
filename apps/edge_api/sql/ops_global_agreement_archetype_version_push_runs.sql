-- QA ledger for the archetype-version render+push lane (repo content -> DocRaptor PDF -> Documenso
-- TEMPLATE). Written by apps/edge_api/src/engagement_templates/push.py record_run() on EVERY
-- terminal state (success | error) from BOTH lanes (operator service-token route and the internal
-- Trigger.dev route). Fire-and-forget: a ledger error never blocks or fails a push.
--
-- ONTOLOGY (ruled 2026-07-21): a push run is of a GLOBAL AGREEMENT ARCHETYPE VERSION — the
-- (brand, path, archetype, version) columns are the version's content_path segments, matching
-- gc.global_agreement_archetype_versions.content_path. This table supersedes the retired
-- ops.engagement_template_push_runs (pre-archetype nomenclature); the migration block below
-- copies its rows once and drops it.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.global_agreement_archetype_version_push_runs (
    id                    bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts                    timestamptz NOT NULL DEFAULT now(),
    run_id                text,                   -- Trigger.dev run id (ctx.run.id); NULL for a direct/manual call
    brand                 text        NOT NULL,   -- content-root subtree (content_path segment 1)
    path                  text        NOT NULL,   -- catalog template-family segment (segment 2)
    archetype             text        NOT NULL,   -- archetype directory segment (segment 3)
    version               text        NOT NULL,   -- version segment (segment 4)
    style                 text,                   -- 'plain' | 'branded' (resolved)
    source_kind           text        NOT NULL,   -- 'repo-html' | 'db-markdown'
    status                text        NOT NULL,   -- 'success' | 'error'
    documenso_template_id text,                   -- envelope/template handle returned by Documenso
    documenso_numeric_id  bigint,                 -- secondary numeric id
    pdf_r2_key            text,                   -- audit copy of the rendered PDF in R2 (NULL if not stored)
    pdf_bytes             integer,
    error                 text
);

CREATE INDEX IF NOT EXISTS gaav_push_runs_ts_idx
    ON ops.global_agreement_archetype_version_push_runs (ts DESC);
CREATE INDEX IF NOT EXISTS gaav_push_runs_status_idx
    ON ops.global_agreement_archetype_version_push_runs (status);
CREATE INDEX IF NOT EXISTS gaav_push_runs_path_idx
    ON ops.global_agreement_archetype_version_push_runs (brand, path, archetype, version);

-- One-time migration from the retired pre-archetype table: copy rows, then drop it. Idempotent —
-- after the drop, to_regclass is NULL and the block no-ops forever.
DO $$
BEGIN
    IF to_regclass('ops.engagement_template_push_runs') IS NOT NULL THEN
        INSERT INTO ops.global_agreement_archetype_version_push_runs
            (ts, run_id, brand, path, archetype, version, style, source_kind, status,
             documenso_template_id, documenso_numeric_id, pdf_r2_key, pdf_bytes, error)
        SELECT ts, run_id, brand, path, archetype, version, style, source_kind, status,
               documenso_template_id, documenso_numeric_id, pdf_r2_key, pdf_bytes, error
          FROM ops.engagement_template_push_runs
         ORDER BY id;
        DROP TABLE ops.engagement_template_push_runs;
    END IF;
END $$;
