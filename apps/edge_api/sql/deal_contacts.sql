-- business.deal_contacts — the deal<->contact junction (which contacts are on a deal + who signs).
--
-- A deal's people are a per-deal selection over the account's contacts (the eligible pool =
-- contacts WHERE account_id = deal.account_id). This junction records the SELECTED set + the per-deal
-- is_signatory flag — state that cannot live on the account-scoped business.contacts row (a contact is
-- shared across ALL of an account's deals) nor with integrity in a jsonb blob.
--
--   is_signatory — whether this contact signs the deal's document. Default true (all deal contacts are
--                  signatories until toggled off). Originate's recipient is the signatory contact.
--
-- business.contacts stays the source of truth for the PERSON (name/email/title); this table only
-- references it (contact_id FK) + carries the deal-scoped attribute. Supersedes deal_details.contacts
-- (the denormalized jsonb), which is dropped once nothing reads it.
--
-- business.deals / business.contacts are upstream-owned (exist in prod); CREATE-only + idempotent.
CREATE TABLE IF NOT EXISTS business.deal_contacts (
    deal_id      uuid        NOT NULL REFERENCES business.deals (id) ON DELETE CASCADE,
    contact_id   uuid        NOT NULL REFERENCES business.contacts (id),
    is_signatory boolean     NOT NULL DEFAULT true,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (deal_id, contact_id)
);

-- Reverse lookup (which deals is a given contact on) — BTREE scalar index on the FK column.
CREATE INDEX IF NOT EXISTS idx_deal_contacts_contact ON business.deal_contacts (contact_id);
