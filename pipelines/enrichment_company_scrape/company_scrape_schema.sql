-- Company-scrape schema — Icypeas /api/scrape rail (hq-x control plane).
--
-- Canonical source-of-truth mirror of the DDL string in company_scrape.py (the worker applies it
-- defensively before every write). Kept here as the reviewable schema artifact.
--
--   gtm.icypeas_company_scrapes  append-only raw SoR for scraped companies. raw_result holds the
--       Icypeas data[] item VERBATIM (result{} + status + searchId); the flat columns are a
--       best-effort projection ON TOP of it, never a replacement (Directive 28). Idempotency +
--       bridging key: company_url_norm (a url already landed FOUND is skipped on re-run). domain_norm
--       bridges to firmographics; linkedin_url is the canonical LinkedIn company url (result.url).
--   ops.company_scrape_runs      per-run terminal counts (requested/skipped/found/not_found/batches/failed).

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.icypeas_company_scrapes (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_url       text        NOT NULL,          -- the requested LinkedIn company URL
    company_url_norm  text,                           -- normalized (idempotency / bridge key)
    search_id         text,                           -- Icypeas searchId (provenance)
    status            text,                           -- FOUND / NOT_FOUND / … verbatim
    -- flat projection (best-effort, from result{}) — convenience OVER raw_result, never a replacement
    company_name      text,
    linkedin_url      text,                           -- result.url (canonical LinkedIn company url)
    website           text,
    domain_norm       text,                           -- normalized website domain — bridge to firmographics
    industry          text,
    headcount_range   text,
    employee_count    int,
    country           text,
    raw_result        jsonb       NOT NULL,           -- the Icypeas data[] item VERBATIM — system of record
    batch_label       text,
    run_root          text,
    scraped_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS icypeas_company_scrapes_url_norm_idx ON gtm.icypeas_company_scrapes (company_url_norm);
CREATE INDEX IF NOT EXISTS icypeas_company_scrapes_domain_idx   ON gtm.icypeas_company_scrapes (domain_norm);
CREATE INDEX IF NOT EXISTS icypeas_company_scrapes_linkedin_idx ON gtm.icypeas_company_scrapes (linkedin_url);
CREATE INDEX IF NOT EXISTS icypeas_company_scrapes_scraped_idx  ON gtm.icypeas_company_scrapes (scraped_at DESC);

CREATE TABLE IF NOT EXISTS ops.company_scrape_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    batch_label   text,
    run_root      text,
    requested     bigint      NOT NULL DEFAULT 0,
    skipped       bigint      NOT NULL DEFAULT 0,
    found         bigint      NOT NULL DEFAULT 0,
    not_found     bigint      NOT NULL DEFAULT 0,
    batches       bigint      NOT NULL DEFAULT 0,
    failed        bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT company_scrape_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS company_scrape_runs_feed_idx     ON ops.company_scrape_runs (feed);
CREATE INDEX IF NOT EXISTS company_scrape_runs_recorded_idx ON ops.company_scrape_runs (recorded_at DESC);
