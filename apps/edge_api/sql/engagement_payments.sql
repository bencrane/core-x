-- Engagement Payments — Stripe ACH (us_bank_account) PaymentIntent state on the engagement grain,
-- plus an APPEND-ONLY lifecycle event ledger. Applied to the hq-x control-plane Postgres
-- (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run).
--
-- WHY ACH-ONLY: the engagement infrastructure fee is billed QUARTERLY IN ADVANCE at five figures;
-- ACH (us_bank_account) avoids card interchange on that size of debit. No card method is offered.
--
-- AUTHORITATIVE PAID STATE: payment_status advances ONLY by the Stripe webhook
-- (payment_intent.processing|succeeded|payment_failed|canceled), NEVER by the browser confirm
-- result — ACH settles asynchronously (1-3 business days), so the client-side result is
-- informational only.
--
-- MONEY in integer minor units (cents). The charge amount is RESOLVED from the proposal content
-- (quarterly_total_cents) at intent-creation time and snapshotted into amount_charged_cents — it is
-- never taken from the browser.

-- ── Payment columns on the engagement row (additive; the signing path is untouched) ──────────────
ALTER TABLE business.engagement_proposals
    ADD COLUMN IF NOT EXISTS stripe_customer_id        text,
    ADD COLUMN IF NOT EXISTS stripe_payment_intent_id  text,
    ADD COLUMN IF NOT EXISTS payment_status            text NOT NULL DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS amount_charged_cents      bigint,
    ADD COLUMN IF NOT EXISTS payment_currency          text NOT NULL DEFAULT 'usd',
    ADD COLUMN IF NOT EXISTS payment_initiated_at      timestamptz,
    ADD COLUMN IF NOT EXISTS paid_at                   timestamptz;

-- Constrain payment_status to the known lifecycle. Dropped/recreated so a re-run converges it.
ALTER TABLE business.engagement_proposals DROP CONSTRAINT IF EXISTS engagement_proposals_payment_status_chk;
ALTER TABLE business.engagement_proposals
    ADD CONSTRAINT engagement_proposals_payment_status_chk
    CHECK (payment_status IN ('none','requires_payment','processing','succeeded','failed','canceled'));

-- Exactly one proposal per PaymentIntent: the webhook UPDATE always hits <=1 row, and an accidental
-- rebind of an intent id onto a second proposal fails loudly instead of corrupting state.
CREATE UNIQUE INDEX IF NOT EXISTS engagement_proposals_pi_uidx
    ON business.engagement_proposals (stripe_payment_intent_id) WHERE stripe_payment_intent_id IS NOT NULL;

-- ── Append-only lifecycle event ledger — the business-state audit trail ───────────────────────────
-- Every Documenso and Stripe webhook event appends ONE immutable row. This is the queryable
-- lifecycle history (created->sent->opened->signed->completed->processing->paid) AND the
-- idempotency guard for webhook redelivery. It is distinct from Stripe's own ledger (the SoR for the
-- money) and from Trigger.dev run history (execution auditability). Never updated, only inserted.
CREATE TABLE IF NOT EXISTS business.engagement_events (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    ref              text        NOT NULL REFERENCES business.engagement_proposals(ref) ON DELETE CASCADE,
    source           text        NOT NULL CHECK (source IN ('documenso','stripe','system')),
    event_type       text        NOT NULL,                  -- 'payment_intent.succeeded', 'DOCUMENT_COMPLETED', …
    idempotency_key  text,                                   -- provider event id (stripe evt_…, documenso event id)
    payload          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at       timestamptz NOT NULL DEFAULT now()
);

-- Idempotent ingestion: a redelivered provider event (same source + event id) is a no-op insert.
CREATE UNIQUE INDEX IF NOT EXISTS engagement_events_idem_uidx
    ON business.engagement_events (source, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS engagement_events_ref_idx ON business.engagement_events (ref, created_at DESC);
