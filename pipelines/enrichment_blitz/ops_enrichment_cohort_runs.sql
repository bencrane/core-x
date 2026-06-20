-- ops.enrichment_cohort_runs — per-run provenance ledger for cohort assembly that
-- feeds the rate-governed Blitz enrichment plane. One row per cohort build (one
-- pipelines/enrichment_blitz/cohort_*.py invocation → one transport Parquet drop).
-- Separate from ops.enrichment_blitz_runs (which records the Workflow A/B/C
-- enrichment runs the cohort is fed INTO): this table records HOW the cohort was
-- resolved — supply size and the per-hop match attrition (domain → PDL → LinkedIn)
-- — so the funnel is observable across re-enrollment cycles. Lives in the hq-x
-- control-plane DB (HQX_DB_URL_POOLED), alongside every other ops.*_runs table.
--
-- The cohort builder (pipelines/enrichment_blitz/cohort_equipment_rental.py)
-- applies this DDL defensively before each terminal write, so it re-applies
-- cleanly. Forward-only. IF NOT EXISTS throughout.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.enrichment_cohort_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,                  -- 'enrichment_cohort_rental'
    cohort_name         text        NOT NULL,                  -- 'equipment_rental_firms_linkedin'
    firms_total         bigint      NOT NULL DEFAULT 0,        -- active US equip-rental supply (NAICS bundle)
    firms_with_domain   bigint      NOT NULL DEFAULT 0,        -- ∩ sam_master_domains (canonical normalized domain)
    firms_pdl_matched   bigint      NOT NULL DEFAULT 0,        -- ∩ pdl_normalized_companies (non-generic domain → pid)
    firms_with_linkedin bigint      NOT NULL DEFAULT 0,        -- ∩ pdl_companies.linkedin_url (the enrolled firms)
    distinct_urls       bigint      NOT NULL DEFAULT 0,        -- DISTINCT company_linkedin_url written (Workflow B cohort)
    r2_key              text,                                  -- transport Parquet key under data-sink
    column_name         text,                                  -- cohort column ('company_linkedin_url')
    status              text        NOT NULL,                  -- 'success' | 'error'
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT enrichment_cohort_runs_status_chk CHECK (status IN ('success', 'error'))
);

CREATE INDEX IF NOT EXISTS enrichment_cohort_runs_feed_idx        ON ops.enrichment_cohort_runs (feed);
CREATE INDEX IF NOT EXISTS enrichment_cohort_runs_cohort_idx      ON ops.enrichment_cohort_runs (cohort_name);
CREATE INDEX IF NOT EXISTS enrichment_cohort_runs_recorded_at_idx ON ops.enrichment_cohort_runs (recorded_at DESC);
