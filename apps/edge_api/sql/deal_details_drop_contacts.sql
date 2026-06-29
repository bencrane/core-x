-- business.deal_details.contacts is superseded by the business.deal_contacts junction (#804): the
-- junction is the system of record for a deal's contacts (membership + is_signatory), read by the
-- /details GET and reconciled by the PUT. The deal_details.contacts jsonb is now a dead mirror — no
-- code path reads it. Drop it. Idempotent and fail-safe to re-apply (runs every boot via migrate.py).
ALTER TABLE business.deal_details DROP COLUMN IF EXISTS contacts;
