-- business.agreements — the contractual instrument (child of a deal).
--
-- Ontology (operator ruling 2026-07-19, ~/Desktop/hq/plans/2026-07-19-deal-agreement-application-ontology.md):
-- the Agreement owns signatory, template binding, field_values, the generated Documenso
-- document, and the document lifecycle. The Deal owns none of that. History = rows;
-- "latest" = newest by created_at. `agreement_handle` (LEFT(id,8), same pattern as
-- deals.deal_handle) is the PUBLIC capability — it is stamped as the Documenso externalId
-- at generate and keys the first-party sign link (/sign/{agreement_handle}); the uuid id
-- stays internal (PK/FK only).
--
-- The table was first applied ad-hoc in prd (2026-07-19); this file is the schema-as-code
-- home (migrate.py applies sql/*.sql on every boot). The ALTER covers the pre-existing
-- table; both paths are idempotent.

CREATE TABLE IF NOT EXISTS business.agreements (
    id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id                uuid NOT NULL REFERENCES business.deals(id),
    agreement_handle       text GENERATED ALWAYS AS (left(id::text, 8)) STORED,
    template_documenso_id  text,
    field_values           jsonb NOT NULL DEFAULT '{}'::jsonb,
    signatory_contact_id   uuid REFERENCES business.contacts(id),
    status                 text NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'generated', 'sent', 'signed', 'void')),
    documenso_envelope_id  text,
    documenso_document_id  integer,
    signing_token          text,
    created_at             timestamptz NOT NULL DEFAULT now(),
    generated_at           timestamptz,
    sent_at                timestamptz,
    signed_at              timestamptz,
    voided_at              timestamptz
);

ALTER TABLE business.agreements
    ADD COLUMN IF NOT EXISTS agreement_handle text GENERATED ALWAYS AS (left(id::text, 8)) STORED;

CREATE INDEX IF NOT EXISTS agreements_deal_idx
    ON business.agreements (deal_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS agreements_handle_uidx
    ON business.agreements (agreement_handle);
