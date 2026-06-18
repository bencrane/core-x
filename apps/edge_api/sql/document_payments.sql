-- Document payments — the direct-to-documenso engagement-fee collection (Stripe ACH).
--
-- Keyed by the Documenso numeric ``document_id`` (the unique pin per signed document). The 8-char
-- ``opportunity_id`` handle is carried alongside so the (opportunity, document) PAIR — the same pair
-- the prospect signing link uses — gates every read/write. This is a STANDALONE payment record: it
-- has no relationship to the legacy proposal-``ref`` payment path (engagement_payments) — the
-- direct-to-documenso flow has no proposal row.
--
-- The charge amount is resolved server-side from ``opportunity_specific_content.field_values
-- ['fee_amount']`` at intent-mint time and frozen here in minor units (cents); it is NEVER taken from
-- the browser. ``payment_status`` is advanced authoritatively ONLY by the Stripe webhook, never by the
-- mint/read endpoints.
--
-- Applied to the hq-x control-plane Postgres (HQX_DB_URL_POOLED). Idempotent (safe to re-run).

CREATE TABLE IF NOT EXISTS business.document_payments (
    document_id              text        PRIMARY KEY,                 -- Documenso numeric document id (the unique pin)
    opportunity_id           text        NOT NULL,                    -- 8-char opportunity handle (the pair capability)
    amount_cents             integer     NOT NULL,                    -- frozen charge, minor units (resolved from fee_amount)
    currency                 text        NOT NULL DEFAULT 'usd',      -- ACH (us_bank_account) is USD-only
    stripe_customer_id       text,
    stripe_payment_intent_id text,
    -- none | requires_payment | processing | succeeded | failed | canceled
    payment_status           text        NOT NULL DEFAULT 'none',
    rail                     text,                                    -- 'card' | 'us_bank_account' — settled rail, stamped ONCE by the webhook
    paid_at                  timestamptz,                             -- set ONCE by the webhook on succeeded
    created_at               timestamptz NOT NULL DEFAULT now(),
    updated_at               timestamptz NOT NULL DEFAULT now()
);

-- Backfill for records created before the card rail (idempotent — safe to re-run on existing tables).
ALTER TABLE business.document_payments ADD COLUMN IF NOT EXISTS rail text;

-- Pair lookups (by opportunity) and webhook lookups (by intent id) — both load-bearing resolution keys.
CREATE INDEX IF NOT EXISTS idx_document_payments_opportunity_id
    ON business.document_payments (opportunity_id);
CREATE INDEX IF NOT EXISTS idx_document_payments_intent_id
    ON business.document_payments (stripe_payment_intent_id);

-- Append-only Stripe webhook audit + idempotency ledger. The UNIQUE ``stripe_event_id`` makes the
-- webhook re-entrant: a redelivered event inserts nothing (ON CONFLICT DO NOTHING) and is skipped.
CREATE TABLE IF NOT EXISTS business.document_payment_events (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_id     text,                                            -- the pair, best-effort from the event metadata
    opportunity_id  text,
    stripe_event_id text        NOT NULL UNIQUE,                     -- Stripe's event id — the idempotency key
    event_type      text,                                            -- e.g. payment_intent.succeeded
    payload         jsonb       NOT NULL,                            -- the raw Stripe event (system of record)
    received_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_document_payment_events_document_id
    ON business.document_payment_events (document_id);
