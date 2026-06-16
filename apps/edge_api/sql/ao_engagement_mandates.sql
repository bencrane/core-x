-- AO Engagement Mandates — the PARALLEL engagement-document pathway's own ledger. Applied to the
-- hq-x control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run).
--
-- This pathway is DELIBERATELY SEPARATE from the existing proposal/agreement machinery
-- (business.engagement_proposals + the markdown→HTML proposal_templates render). It takes the
-- repo-resident STATIC HTML engagement content (apps/edge_api/content/global_engagement_content/),
-- binds an opportunity's values + a locked price/term package into it, and renders a plain PDF via
-- DocRaptor. One row per opportunity: the operator's "lock the price + term" selection + the rendered
-- artifact pointer. It owns its own state — it never writes engagement_proposals or the proposal SoR.
--
-- opportunity_id references business.opportunities(id) by VALUE (no FK): that table is owned by the
-- hq-x migration tool, and this parallel pathway does not reshape it. Integrity is held by the app +
-- the partial-unique index (one live mandate per opportunity).

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.ao_engagement_mandates (
    -- identity
    id                 text        PRIMARY KEY,                 -- mand_… (minted at stage time)
    opportunity_id     uuid        NOT NULL,                    -- business.opportunities.id (by value, no FK)
    -- locked commercial terms (resolved SERVER-SIDE from a preset package key; client never sets money)
    package_key        text        NOT NULL,                    -- preset selector (engagement_docs.packages)
    term_fee_cents     bigint      NOT NULL,                    -- {{term_fee}} — Engagement Fee for the Initial Term
    duration_months    integer     NOT NULL,                    -- {{duration_in_months}} — the Initial Term
    -- which engagement-content document + style was rendered
    document_slug      text        NOT NULL DEFAULT 'active_operators_term_only',
    style              text        NOT NULL DEFAULT 'plain'     -- plain | branded (manifest flag at render)
                       CHECK (style IN ('plain','branded')),
    -- lifecycle
    status             text        NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending','rendering','rendered','failed')),
    -- rendered artifact (R2 — segregated engagement-mandates/ namespace, never the SoR)
    pdf_r2_key         text,                                    -- stable R2 object key
    pdf_url            text,                                    -- last presigned GET url (short-lived; informational)
    pdf_bytes          integer,                                 -- rendered size (sanity)
    -- Documenso v2 linkage — a SIGNABLE DOCUMENT (type=DOCUMENT) created from the rendered PDF:
    -- SIGNATURE/DATE placed by [[anchor]] (findText), distributed NONE so the signing tokens exist
    -- WITHOUT Documenso emailing anyone (the app controls delivery). Provider = RS signatory.
    documenso_envelope_id      text,                            -- envelope_… (the v2 document id)
    documenso_document_id      integer,                         -- numeric secondaryId (for download)
    participant_signing_token  text,                            -- the prospect's signing capability
    provider_signing_token     text,                            -- the RS signatory's signing capability
    -- provenance
    field_values       jsonb       NOT NULL DEFAULT '{}'::jsonb,-- the merge values bound into the document
    trigger_run_id     text,                                    -- the Trigger.dev run that produced it
    error              text,                                    -- last failure detail (status='failed')
    created_by         text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

-- One live mandate per opportunity — re-staging/re-rendering UPDATES in place (the upsert target).
CREATE UNIQUE INDEX IF NOT EXISTS ao_engagement_mandates_opportunity_uidx
    ON business.ao_engagement_mandates (opportunity_id);
CREATE INDEX IF NOT EXISTS ao_engagement_mandates_status_idx  ON business.ao_engagement_mandates (status);
CREATE INDEX IF NOT EXISTS ao_engagement_mandates_created_idx ON business.ao_engagement_mandates (created_at DESC);

-- Documenso linkage — additive + idempotent for an already-provisioned table (the CREATE TABLE above
-- only fires on a fresh DB).
ALTER TABLE business.ao_engagement_mandates ADD COLUMN IF NOT EXISTS documenso_envelope_id     text;
ALTER TABLE business.ao_engagement_mandates ADD COLUMN IF NOT EXISTS documenso_document_id     integer;
ALTER TABLE business.ao_engagement_mandates ADD COLUMN IF NOT EXISTS participant_signing_token text;
ALTER TABLE business.ao_engagement_mandates ADD COLUMN IF NOT EXISTS provider_signing_token    text;
