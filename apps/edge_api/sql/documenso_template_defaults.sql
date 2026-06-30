-- Documenso MIRROR-template DEFAULT store. Applied to the hq-x control-plane Postgres
-- (HQX_DB_URL_POOLED) at boot by src/migrate.py. Idempotent DDL (safe to re-run).
--
-- WHY THIS TABLE EXISTS. The "Confirm & Originate default" for a MIRROR template
-- (business.documenso_envelopes) has nowhere else to live: the mirror is projector-owned and stored
-- VERBATIM (it must never carry an operator flag), and the legacy business.documenso_templates registry
-- does NOT contain mirror-path templates (e.g. 14503). So the operator's choice of default is recorded
-- HERE, keyed by the mirror's numeric documenso_id — the same operator-owned boundary as
-- business.documenso_template_document_prefill_configs.
--
-- OWNERSHIP. OPERATOR/app-owned. The async projector / on-demand re-grab NEVER write this table; the
-- sole writer is the Set-Template-as-Default picker (POST /api/v1/documenso-template-defaults).
--
-- INVARIANT. At most ONE default across all mirror templates (single-operator plane). The partial
-- unique index pins it; set-default is clear-then-set (mirroring business.documenso_templates) so the
-- partial unique index is never transiently violated.

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.documenso_template_defaults (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    documenso_id bigint      NOT NULL,   -- the MIRROR template's numeric documenso_id (the upsert key)
    is_default   boolean     NOT NULL DEFAULT true,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- one row per mirror template (the ON CONFLICT upsert target)
CREATE UNIQUE INDEX IF NOT EXISTS documenso_template_defaults_documenso_id_uidx
    ON business.documenso_template_defaults (documenso_id);
-- at most ONE default across all mirror templates: every indexed row has is_default = true, so the
-- unique constraint admits a single such row.
CREATE UNIQUE INDEX IF NOT EXISTS documenso_template_defaults_one_default_uidx
    ON business.documenso_template_defaults (is_default) WHERE is_default;
