-- Contacts live on the deal_details satellite as a first-class set; deals stays company-only.
--
-- A deal maps to exactly ONE company (deals: account_id + company_name/company_domain). Its PEOPLE are
-- per-deal and plural — seeded from the booking, editable — so they hang off the deal_details satellite,
-- not the deals spine. Which contact is the SIGNATORY is a later selection over this set (operator-owned;
-- not modeled here yet).
--
--   business.deal_details.contacts — the deal's contacts. JSONB array; each entry expected to carry
--                                    contact_id (business.contacts.id), first_name, last_name, email,
--                                    title. Signatory selection is layered on later.
--   business.deals.contact_id      — DROPPED: a single contact on the company spine contradicts the
--                                    plural per-deal contact set above (and was NULL/unused).
--
-- Upstream-owned tables exist in prod; idempotent (ADD / DROP COLUMN IF [NOT] EXISTS), safe to re-apply.
ALTER TABLE business.deal_details
    ADD COLUMN IF NOT EXISTS contacts jsonb NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE business.deals
    DROP COLUMN IF EXISTS contact_id;
