-- business.deal_document_configs — the deal's DOCUMENT configuration (which MIRROR template is attached
-- + the per-field prefill OVERRIDES), APPEND-ONLY with one ACTIVE row per deal. Applied to the hq-x
-- control-plane Postgres (HQX_DB_URL_POOLED) at boot by src/migrate.py. Idempotent (safe to re-run).
--
-- Supersedes business.deal_details (the 1:1 mutable satellite). A deal corresponds to a single Documenso
-- template at a time, so each row pins {template_documenso_id, field_values} TOGETHER — the prefill values
-- are keyed by THAT template's field labels and are only meaningful next to their template.
--
-- WRITE SEMANTICS (see deals/queries.upsert_document_config):
--   • editing values on the SAME template  → UPDATE the active row in place.
--   • attaching a DIFFERENT template        → ARCHIVE the active row, INSERT a new active row.
-- The partial unique index pins at most one active row per deal; archive-before-insert avoids a
-- transient violation. "Which template is attached" = the ACTIVE row's template_documenso_id.
--
-- field_values holds OPERATOR OVERRIDES ONLY (label -> value). Template defaults + prospect facts are
-- resolved at read/originate (Model B: value = field_values[label] ?? config default) — NEVER copied in,
-- so a later change to a template default can't leave a stale baked copy on the deal.
--
-- No FK to the projector-owned mirror (business.documenso_envelopes) — validated at write. FK to
-- business.deals (ON DELETE CASCADE): the config dies with its deal.

CREATE TABLE IF NOT EXISTS business.deal_document_configs (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id               uuid        NOT NULL REFERENCES business.deals (id) ON DELETE CASCADE,
    template_documenso_id bigint,                                  -- attached MIRROR template (documenso_envelopes.documenso_id); NULL = none attached
    field_values          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status                text        NOT NULL DEFAULT 'active',   -- 'active' | 'archived'
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS deal_document_configs_deal_id_idx
    ON business.deal_document_configs (deal_id);
-- at most ONE active config per deal
CREATE UNIQUE INDEX IF NOT EXISTS deal_document_configs_one_active_per_deal_uidx
    ON business.deal_document_configs (deal_id) WHERE status = 'active';
