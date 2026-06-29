-- Documenso ENVELOPE mirror + template-config. Applied to the hq-x control-plane Postgres
-- (HQX_DB_URL_POOLED) at boot by src/migrate.py. Idempotent DDL (safe to re-run).
--
-- GREENFIELD. This is a NEW mirror, independent of the legacy business.documenso_templates table and
-- its readers (deals/queries.py, etc.) — those are untouched.
--
-- ── business.documenso_envelopes ──────────────────────────────────────────────────────────────────
-- A queryable MIRROR of Documenso envelopes (templates AND documents), projected from webhook events
-- by the async projector (src/documenso_projection/). On every non-DELETE event the projector pulls
-- the FULL live envelope (GET /api/v2/envelope/{id}) and upserts it here. The mirror is read-side
-- convenience over business.documenso_webhook_events (which remains the raw append-only SoR).
--
-- VERBATIM CONTRACT. documenso_response holds the FULL get_envelope response EXACTLY as Documenso
-- returns it (no key rename, no value rewrite, no snake_casing). The scalar columns (type/status) are
-- LOWERCASED-ONLY projections of Documenso's own terms — never remapped (Documenso 'CANCELLED' stores
-- as 'cancelled', never 'voided'); no derived/normalized states. Re-derive anything from
-- documenso_response if a scalar extract is ever wrong.
--
-- COLUMNS.
--   documenso_id          — the envelope's numeric Documenso id (the upsert key). UNIQUE.
--   envelope_id           — the prefixed v2 envelope handle (envelope_…). UNIQUE.
--   secondary_id          — Documenso secondaryId (e.g. document_<n> / template_<n>), if present.
--   type                  — lowercased Documenso type/source: 'template' | 'document'. VERBATIM term.
--   template_documenso_id — documents: the SOURCE template numeric id; NULL for templates.
--   external_id           — documents: Documenso externalId (the deal handle stamped at originate).
--   title                 — envelope title, verbatim.
--   status                — lowercased Documenso status, VERBATIM (NEVER remapped).
--   documenso_response    — full GET /api/v2/envelope/{id} response (jsonb), verbatim. System of mirror.
--   deleted_at            — soft-delete stamp set on a *_DELETED event (no API pull on delete).
--
-- ── business.documenso_template_document_prefill_configs ────────────────────────────────────────────
-- Per-template DOCUMENT-prefill config (per-field default value + read-only). OPERATOR/app-owned —
-- the projector/sync NEVER writes this table. Keyed UNIQUE on template_documenso_id.

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.documenso_envelopes (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    documenso_id          bigint      NOT NULL,
    envelope_id           text        NOT NULL,
    secondary_id          text,
    type                  text        NOT NULL,   -- lowercased Documenso type: 'template' | 'document'
    template_documenso_id bigint,                 -- documents: source template numeric id; null for templates
    external_id           text,                   -- documents: Documenso externalId (deal handle)
    title                 text,
    status                text,                   -- lowercased Documenso status, VERBATIM (no remap)
    documenso_response    jsonb       NOT NULL,   -- full GET /api/v2/envelope/{id} response, verbatim
    deleted_at            timestamptz,
    synced_at             timestamptz NOT NULL DEFAULT now(),
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS documenso_envelopes_documenso_id_uidx
    ON business.documenso_envelopes (documenso_id);
CREATE UNIQUE INDEX IF NOT EXISTS documenso_envelopes_envelope_id_uidx
    ON business.documenso_envelopes (envelope_id);
CREATE INDEX IF NOT EXISTS documenso_envelopes_template_documenso_id_idx
    ON business.documenso_envelopes (template_documenso_id);
CREATE INDEX IF NOT EXISTS documenso_envelopes_external_id_idx
    ON business.documenso_envelopes (external_id);
CREATE INDEX IF NOT EXISTS documenso_envelopes_type_idx
    ON business.documenso_envelopes (type);

-- Per-template DOCUMENT-PREFILL config. Operator-authored (the projector/sync NEVER writes this).
-- Dictates what happens when a document is instantiated + prefilled off this template, per field label:
--   field_settings = { "<field label>": { default_document_field_value: <str>, read_only: <bool>,
--                                          source?: <deal-fact key, Phase 2> } }
-- Resolution at originate (model B): value = deal_details.field_values[label] (explicit override)
--   ELSE default_document_field_value (live fallback); read_only fields are locked on the document.
CREATE TABLE IF NOT EXISTS business.documenso_template_document_prefill_configs (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    template_documenso_id bigint      NOT NULL,
    field_settings        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS documenso_template_document_prefill_configs_template_uidx
    ON business.documenso_template_document_prefill_configs (template_documenso_id);

-- Superseded by the table above (renamed + reshaped). Empty in prod; dropped manually. Kept absent
-- here so a fresh DB never creates the old name.
