-- business.icypeas_webhook_events — RAW Icypeas webhook capture (append-only, system of record).
--
-- Icypeas pushes company-scrape results here: one delivery per scraped company
-- (webhookUrlItem, kind='scrape_item') and one per finished bulk (webhookUrlBulkDone,
-- kind='bulk_done'). The FULL webhook body is stored verbatim in `payload` (the SoR); the scalar
-- columns are best-effort lookup extracts ONLY — never re-derive truth from them, re-read `payload`
-- (Directive 28 raw-first doctrine). Projection into a company dimension is a SEPARATE, revisable
-- step decided against the captured payloads — never inferred ahead of real landed data.
--
-- Applied to the hq-x control-plane Postgres (HQX_DB_URL_POOLED) by edge_api's boot migration
-- (src/migrate.py applies every sql/*.sql in filename order under an advisory lock). Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.icypeas_webhook_events (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    received_at   timestamptz NOT NULL DEFAULT now(),
    kind          text,                  -- 'scrape_item' | 'bulk_done' — which webhook fired
    item_id       text,                  -- Icypeas search-item _id (scrape_item deliveries)
    file_id       text,                  -- Icypeas bulk file id — correlation key back to the submit
    status        text,                  -- item status verbatim (FOUND / DEBITED / NOT_FOUND / …)
    external_id   text,                  -- the externalId we stamped at submit = the requested company URL
    company_url   text,                  -- best-effort: the scraped LinkedIn company URL
    signature_ts  text,                  -- the timestamp Icypeas signed the delivery with (audit)
    payload       jsonb       NOT NULL   -- THE raw verbatim webhook body — system of record
);

CREATE INDEX IF NOT EXISTS icypeas_webhook_events_file_idx     ON business.icypeas_webhook_events (file_id);
CREATE INDEX IF NOT EXISTS icypeas_webhook_events_item_idx     ON business.icypeas_webhook_events (item_id);
CREATE INDEX IF NOT EXISTS icypeas_webhook_events_external_idx ON business.icypeas_webhook_events (external_id);
CREATE INDEX IF NOT EXISTS icypeas_webhook_events_received_idx ON business.icypeas_webhook_events (received_at DESC);
