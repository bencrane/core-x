-- Documenso webhook RAW landing. Applied to the hq-x control-plane Postgres (HQX_DB_URL_POOLED).
-- Idempotent DDL (safe to re-run).
--
-- PURPOSE. Capture the raw Documenso webhook body, verbatim, as the immutable system-of-record —
-- BEFORE any normalization/projection. Documenso POSTs every configured event (document.sent /
-- opened / signed / completed / rejected / cancelled) to POST /api/v1/documenso/webhook; the route
-- verifies X-Documenso-Secret (DOCUMENSO_WEBHOOK_SECRET) and append-inserts one row here per
-- delivery. Projection into engagement/signing state is a SEPARATE, revisable step decided against
-- the real captured payloads — never the other way around. This table is the source of record; the
-- extracted columns below are best-effort lookup keys only.
--
-- This is NOT the legacy `/api/v1/proposals/webhook` path (engagement_proposals); that endpoint is
-- untouched. Documenso is repointed to /api/v1/documenso/webhook.
--
-- COLUMNS.
--   payload       — the RAW verbatim webhook body (jsonb). The system of record.
--   event         — best-effort extract of the event string (e.g. 'document.completed'), for filtering.
--   envelope_id   — best-effort extract of the envelope handle (payload id/envelopeId), for lookup.
--   external_id   — best-effort extract of externalId (= opportunity_id, stamped at originate), for lookup.
--   received_at   — capture time.
-- The extracts are NOT authoritative — re-derive anything from `payload` if extraction was wrong.
-- No dedup here (append-only): redelivery handling is a projection-time concern.

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.documenso_webhook_events (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    received_at   timestamptz NOT NULL DEFAULT now(),
    event         text,                  -- raw event string, verbatim
    envelope_id   text,                  -- best-effort lookup extract (NOT source of truth)
    external_id   text,                  -- best-effort lookup extract (= opportunity_id at originate)
    payload       jsonb       NOT NULL   -- THE raw verbatim webhook body — system of record
);

CREATE INDEX IF NOT EXISTS documenso_webhook_events_envelope_idx ON business.documenso_webhook_events (envelope_id);
CREATE INDEX IF NOT EXISTS documenso_webhook_events_external_idx ON business.documenso_webhook_events (external_id);
CREATE INDEX IF NOT EXISTS documenso_webhook_events_received_idx ON business.documenso_webhook_events (received_at DESC);
