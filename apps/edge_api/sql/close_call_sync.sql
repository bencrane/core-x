-- close_call_sync.sql — RAW Close.com webhook capture + operator map (auto-applied on boot).
--
-- Mirrors business.documenso_webhook_events: append-only verbatim landing of every Close
-- webhook delivery. `payload` is the system of record; scalar columns are best-effort lookup
-- extracts. The "now dialing" briefing is DERIVED at read time from these rows (the offline
-- /api/v1/close/active-call read), exactly like documenso /sign-state — no projection table.
--
-- The pointer (public.active_call_signal) + lead->domain resolution (public.close_crosswalk)
-- are owned by rare-structure-hq migration 0005 and are NOT created here.

CREATE TABLE IF NOT EXISTS business.close_webhook_events (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    received_at      timestamptz NOT NULL DEFAULT now(),
    event_id         text,                 -- Close event id (ev_…) — for idempotency/dedup at read time
    object_type      text,                 -- 'activity.call', etc.
    action           text,                 -- 'created' | 'updated' | 'deleted'
    close_user_id    text,                 -- the Close user who placed the call (-> operator map)
    close_lead_id    text,
    close_contact_id text,
    direction        text,                 -- 'outbound' | 'inbound'
    status           text,                 -- call status (verbatim from Close)
    remote_phone     text,
    payload          jsonb       NOT NULL  -- THE verbatim webhook body — system of record
);

CREATE INDEX IF NOT EXISTS close_webhook_events_user_received_idx
    ON business.close_webhook_events (close_user_id, received_at DESC);
CREATE INDEX IF NOT EXISTS close_webhook_events_lead_idx
    ON business.close_webhook_events (close_lead_id);
CREATE INDEX IF NOT EXISTS close_webhook_events_received_idx
    ON business.close_webhook_events (received_at DESC);

-- close user -> operator (auth_user_id). Maps the Close user who placed a call to the platform
-- operator whose Insights tab should surface the briefing. Seeded per operator.
CREATE TABLE IF NOT EXISTS business.close_operator_map (
    close_user_id text        PRIMARY KEY,
    auth_user_id  uuid        NOT NULL,
    email         text,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
