-- OUTBOUND cal.com booking ledger — the create path's idempotency + terminal-state audit. Written by
-- apps/edge_api/src/routers/internal_cal_v1.py (the cal-book Trigger.dev task's callback) via the
-- edge_api async pool (HQX_DB_URL_POOLED).
--
-- One row per Close custom activity that requested a booking, keyed UNIQUE on close_activity_id — the
-- DOUBLE-BOOK GUARD: the route CLAIMS the row (INSERT … ON CONFLICT DO NOTHING) BEFORE the cal.com
-- create, so a duplicate webhook delivery / re-fire never mints a second booking. Mirrors the inbound
-- shape (corex.bookings) but for the outbound direction. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.cal_booking_runs (
    id                bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    close_activity_id text        NOT NULL UNIQUE,  -- Close custom-activity id (the idempotency anchor)
    status            text        NOT NULL,          -- 'pending' | 'success' | 'error'
    event_type_slug   text,                          -- resolved cal.com event-type slug (15min/30min/…)
    cal_booking_id    text,                          -- cal.com v2 booking id (numeric, stored as text)
    cal_booking_uid   text,                          -- cal.com v2 booking uid
    ical_uid          text,                          -- iCalUID if returned (links to corex.bookings)
    close_lead_id     text,                          -- Close lead the activity hangs off
    close_contact_id  text,                          -- Close contact resolved as the booking attendee
    error             text,                          -- failure reason when status='error'
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- close_activity_id already carries a UNIQUE btree (the idempotency key). Secondary resolution keys:
CREATE INDEX IF NOT EXISTS cal_booking_runs_status_idx
    ON ops.cal_booking_runs (status);
CREATE INDEX IF NOT EXISTS cal_booking_runs_created_at_idx
    ON ops.cal_booking_runs (created_at DESC);
