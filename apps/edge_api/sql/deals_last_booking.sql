-- business.deals: associate a deal with its most-recent booking, and enforce one-deal-per-account.
--
-- GOING-FORWARD MODEL (replaces the old booking -> opportunity projection): a new corex.bookings row
-- creates a deal — UNLESS a deal already exists for that booking's account, in which case the existing
-- deal's last_booking_id is advanced to the new booking. These two objects instrument that rule:
--
--   last_booking_id     — the deal's most-recent booking (FK corex.bookings.booking_id). A deal can
--                         front many bookings on the same account over time; this points at the latest.
--   uq_deals_account    — UNIQUE(account_id): one deal per account. Makes "unless we already have a deal
--                         on that account" STRUCTURAL (not advisory) and lets the booking->deal upsert
--                         collapse on account_id (ON CONFLICT).
--
-- corex.bookings / business.deals are upstream-owned (exist in prod); idempotent (ADD COLUMN / CREATE
-- INDEX IF NOT EXISTS), safe to re-apply at boot.
ALTER TABLE business.deals
    ADD COLUMN IF NOT EXISTS last_booking_id uuid REFERENCES corex.bookings (booking_id);

-- Resolution key (deal -> its latest booking) — BTREE scalar index.
CREATE INDEX IF NOT EXISTS idx_deals_last_booking_id
    ON business.deals (last_booking_id);

-- One deal per account — the structural form of the dedupe rule.
CREATE UNIQUE INDEX IF NOT EXISTS uq_deals_account
    ON business.deals (account_id);
