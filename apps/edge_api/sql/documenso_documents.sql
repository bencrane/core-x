-- business.documenso_documents — the per-instantiated-document SYSTEM OF RECORD, sourced from the
-- actual Documenso envelope PAYLOAD (not our render inputs), linked to the opportunity it serves.
--
-- One row per Documenso document instantiated off a template (the direct-to-documenso "Confirm &
-- Originate" path). Stamped at instantiation from the live GET /api/v2/envelope/{id} response —
-- recipients (each signer's token = the provider + participant links), fields (the values actually
-- baked into the document), and the full raw payload — and kept current by the Documenso webhook.
--
-- amount_cents is FROZEN at stamp time from the template's variant
-- (documenso_templates.global_input_content_variant_id -> global_input_content_variants.params
-- ->> 'system_fee_cents'): the single source of truth for the printed AND charged figure, so the
-- payment page reads the variant price (no $35k constant, no PDF/charge drift).
--
-- Supersedes business.documenso_envelopes (a payload mirror with no opportunity link, never written
-- at instantiation). NOT the business.ao_engagement_mandates lane (active-operators, render-input-
-- sourced, webhook-status-only).
--
-- Idempotent (CREATE ... IF NOT EXISTS); applied to HQX_DB_URL_POOLED at edge_api boot.

CREATE TABLE IF NOT EXISTS business.documenso_documents (
  envelope_id                      text PRIMARY KEY,                    -- Documenso envelope id (stable handle)
  document_id                      bigint,                              -- numeric Documenso document id (payment key + signed-PDF download)
  external_id                      text,                                -- stamped on the envelope at create -> deterministic webhook match (= draft id)
  opportunity_id                   uuid NOT NULL
                                     REFERENCES business.opportunities (id) ON DELETE CASCADE,
  documenso_template_id            text NOT NULL,                       -- template instantiated from (numeric id as text; not FK-able — PK is on id)
  global_input_content_variant_id  uuid
                                     REFERENCES business.global_input_content_variants (id) ON DELETE SET NULL,
  amount_cents                     integer,                             -- frozen from variant.params->>'system_fee_cents' at stamp time
  currency                         text NOT NULL DEFAULT 'usd',
  recipients                       jsonb NOT NULL DEFAULT '[]'::jsonb,  -- payload recipients (provider + participant, each with signing token)
  fields                           jsonb NOT NULL DEFAULT '[]'::jsonb,  -- the field values on the instantiated document
  raw                              jsonb NOT NULL DEFAULT '{}'::jsonb,  -- the entire envelope payload (lossless)
  events                           jsonb NOT NULL DEFAULT '[]'::jsonb,  -- appended Documenso webhook events (audit trail)
  status                           text,                                -- Documenso lifecycle status (webhook-advanced)
  signed_pdf_url                   text,
  signed_pdf_r2_key                text,
  created_at                       timestamptz NOT NULL DEFAULT now(),
  updated_at                       timestamptz NOT NULL DEFAULT now()
);

-- Resolution key: serve a document (and its amount) by the opportunity it belongs to.
CREATE INDEX IF NOT EXISTS documenso_documents_opportunity_idx
  ON business.documenso_documents (opportunity_id);

-- Payment-page lookup by the numeric Documenso document id (one document per numeric id).
CREATE UNIQUE INDEX IF NOT EXISTS documenso_documents_document_id_uidx
  ON business.documenso_documents (document_id)
  WHERE document_id IS NOT NULL;

-- Deterministic webhook match by the external id we stamp on the envelope.
CREATE INDEX IF NOT EXISTS documenso_documents_external_id_idx
  ON business.documenso_documents (external_id)
  WHERE external_id IS NOT NULL;

-- Variant fan-in (which documents were minted from a given price/term variant).
CREATE INDEX IF NOT EXISTS documenso_documents_variant_idx
  ON business.documenso_documents (global_input_content_variant_id)
  WHERE global_input_content_variant_id IS NOT NULL;
