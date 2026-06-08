-- Engagement Proposals — engagement-agreement / e-signature grain. Applied to the hq-x
-- control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run).
--
-- NOTE ON NAMING: ``business.proposals`` is ALREADY TAKEN by the DMaaS data-transfer
-- proposal→checkout subsystem (partner data-engine audience sales). This is a DIFFERENT domain:
-- Rare Structure engagement-mandate e-signature. Hence ``business.engagement_proposals``.
--
-- One row per engagement proposal. ``ref`` is an unguessable capability token: it is BOTH the
-- primary key and the credential that grants public read of the proposal shell + the signing
-- token (no auth header — the ref is the bearer). The operator mints a proposal (service-token
-- gated), the engine renders the legal PDF (DocRaptor, live mode) and creates a Documenso v2
-- envelope with the Client as the sole e-sign recipient; status advances by Documenso webhook,
-- never by the client-side embed callback.
--
-- MONEY is stored in integer minor units (cents). The success-fee schedule (5/4/3/2/1.5%) is
-- FIXED in the agreement body and is NOT stored per-row — only the variable Infrastructure Fee
-- (monthly + quarterly) and the counterparty identity vary per deal.

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.engagement_proposals (
    -- identity / capability
    ref                    text        PRIMARY KEY,                 -- unguessable token; the URL slug + the credential
    template_id            text        NOT NULL DEFAULT 'strategic_origination_mandate',
    -- counterparty (the Client / institutional fund partner)
    client_name            text        NOT NULL,                    -- <<clientName>> — the institutional entity
    client_signer_name     text        NOT NULL,                    -- <<clientSignerName>> — the person who signs
    client_title           text,                                    -- <<clientTitle>>
    client_email           text        NOT NULL,                    -- the Documenso recipient
    -- commercial terms (the variable values; the % success schedule is fixed in the body)
    effective_date         date        NOT NULL,                    -- <<effectiveDate>>
    monthly_fee_cents      bigint      NOT NULL,                    -- <<monthlyFee>> infrastructure fee / month
    quarterly_total_cents  bigint      NOT NULL,                    -- <<quarterlyTotal>> billed 3 months in advance
    rs_signer_name         text        NOT NULL,                    -- <<rsName>> — Rare Structure signatory (pre-signed)
    -- lifecycle
    status                 text        NOT NULL DEFAULT 'draft'
                           CHECK (status IN ('draft','sent','opened','signed','completed','rejected','voided')),
    -- Documenso v2 linkage (populated when the envelope is created at send time)
    documenso_envelope_id  text,                                    -- v2 envelope/document id
    documenso_client_token text,                                    -- the Client recipient's signing token (drives the embed)
    signed_pdf_url         text,                                    -- our stored URL of the sealed PDF (post-completion)
    -- provenance + snapshot
    field_values           jsonb       NOT NULL DEFAULT '{}'::jsonb,-- merge values bound into the PDF at create time
    created_by             text,                                    -- operator user id (optional)
    -- timeline
    created_at             timestamptz NOT NULL DEFAULT now(),
    sent_at                timestamptz,
    opened_at              timestamptz,
    signed_at              timestamptz,
    completed_at           timestamptz,
    updated_at             timestamptz NOT NULL DEFAULT now()
);

-- Webhook + operator lookups. The envelope index is PARTIAL UNIQUE: exactly one proposal per
-- Documenso envelope (so the webhook UPDATE always hits ≤1 row, and any accidental rebind of an
-- envelope id onto a second proposal fails loudly instead of corrupting state). Converges an
-- already-deployed non-unique index in place.
DROP INDEX IF EXISTS business.engagement_proposals_envelope_idx;
CREATE UNIQUE INDEX IF NOT EXISTS engagement_proposals_envelope_uidx
    ON business.engagement_proposals (documenso_envelope_id) WHERE documenso_envelope_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS engagement_proposals_status_idx   ON business.engagement_proposals (status);
CREATE INDEX IF NOT EXISTS engagement_proposals_created_idx  ON business.engagement_proposals (created_at DESC);
