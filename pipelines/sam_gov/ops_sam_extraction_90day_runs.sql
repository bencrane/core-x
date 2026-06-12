-- Per-run roll-up table for the SAM.gov 90-day attachment TEXT/STRUCTURED EXTRACTION pipeline
-- (Stage 4). Written by pipelines/sam_gov/sam_attachment_extract_90day.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, one row per (run_id, phase, lane). ISOLATED from the
-- Stage-3 ops.sam_attachment_download_90day_runs by design (separate motion: download is byte
-- transport, this is text extraction off the CAS at s3://data-sink/active/sam_attachment_blobs_90day/).
-- Idempotent DDL. Canonical column set = SAM_90DAY_EXTRACTION_PIPELINE_SPEC_V2.md §3.8.
--
-- CANONICAL COPY. The extraction worker mirrors this verbatim as OPS_DDL and applies it
-- (CREATE ... IF NOT EXISTS) before each insert. Keep in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sam_extraction_90day_runs (
    id                    bigserial PRIMARY KEY,
    run_id                text        NOT NULL,   -- '90day-extract-<UTC ISO>' batch identifier
    phase                 text,                   -- 'phase0' | 'route' | 'expand' | 'extract'
    lane                  text,                   -- L1_scope | L4_structured | L3_triage | container | <multi>
    files_in              int,                    -- files claimed this (run, phase, lane) (excludes resume-skips)
    extracted_scope       int,                    -- terminal extracted_scope     -> govcon_scope_vectors_90day
    extracted_pricing     int,                    -- terminal extracted_pricing    -> govcon_pricing_90day
    extracted_spreadsheet int,                    -- L4 spreadsheets extracted (re-routed to scope/pricing/unknown, §7.5)
    extracted_unknown     int,                    -- terminal extracted_unknown    -> govcon_unknown_90day
    dropped_boilerplate   int,                    -- filename DROP_RX match (terminal, §5.2)
    dropped_duplicate     int,                    -- non-canonical sha256 (terminal, §5.1 dedup pre-pass)
    dropped_content_noise int,                    -- content-truth boilerplate header (terminal, §7.4)
    content_marked        int,                    -- count of files w/ >=1 detected content marking (§7.4/C10; was cui_tagged)
    expanded_container    int,                    -- zip parents expanded (terminal, §6/D10)
    requires_ocr          int,                    -- low text-yield PDFs deferred to Phase 3 (re-attemptable, §7.3)
    extract_failed        int,                    -- per-file exception / converter miss (re-attemptable, §13)
    by_extractor          jsonb,                  -- per-engine success/fail roll-up {extractor:{ok,fail}} (#27)
    total_chars           bigint,                 -- sum of extracted text_chars this (run, phase, lane)
    total_chunks          bigint,                 -- sum of chunks written to the vector/pricing/unknown sinks
    sustained_files_per_s numeric,                -- measured throughput (files / wall-clock), feeds §10/§16 model
    sustained_mbps        numeric,                -- measured throughput (blob bytes read / wall-clock)
    cpu_wait_ratio        numeric,                -- CPU-extraction time vs R2-read wait (binding-term signal, §16)
    status                text,                   -- 'success' | 'interrupted' | 'error'
    error                 text,
    started_at            timestamptz,
    completed_at          timestamptz
);

-- Forward-rename for tables provisioned before the rename: cui_tagged -> content_marked. Idempotent.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ops'
             AND table_name='sam_extraction_90day_runs' AND column_name='cui_tagged') THEN
    ALTER TABLE ops.sam_extraction_90day_runs RENAME COLUMN cui_tagged TO content_marked;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS sam_extraction_90day_runs_run_id_idx
    ON ops.sam_extraction_90day_runs (run_id);
CREATE INDEX IF NOT EXISTS sam_extraction_90day_runs_started_at_idx
    ON ops.sam_extraction_90day_runs (started_at DESC);
