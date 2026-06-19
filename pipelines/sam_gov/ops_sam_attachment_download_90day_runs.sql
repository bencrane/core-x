-- Terminal-state table for the SAM.gov 90-day-winners attachment byte-download worker.
-- Written by pipelines/sam_gov/sam_attachment_download_90day.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state. ISOLATED from the historical
-- ops.sam_attachment_download_runs by design (separate Stage-3 motion, separate CAS
-- prefix sam_attachment_blobs/). One row per download batch. Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as OPS_DDL and applies it
-- (CREATE ... IF NOT EXISTS) before each insert. Keep in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sam_attachment_download_runs (
    id               bigserial PRIMARY KEY,
    run_id           text        NOT NULL,   -- '90day-<UTC ISO>' batch identifier
    scope            text,                   -- composed gate predicate (provenance)
    concurrency      int,                    -- worker pool size (WAF envelope)
    req_per_sec      numeric,                -- global token-bucket ceiling
    attempted        int,                    -- files fetched this run (excludes resume-skips)
    downloaded       int,                    -- HTTP 200, bytes stored
    skipped          int,                    -- resume-skips (already in ledger + object present)
    failed           int,                    -- hard 4xx / retries-exhausted / wall-clock backstop
    restricted       int,                    -- 401 / 403 UNAUTHORIZED (no retry)
    gone             int,                    -- 400 / 410 removed resource / link-type (no retry)
    oversize         int,                    -- real stream length >= ceiling (no store)
    waf_blocks_429   int,                    -- 429 throttle events observed (circuit-breaker input)
    waf_blocks_403   int,                    -- 403 WAF events observed (non-UNAUTHORIZED)
    bytes_downloaded bigint,                 -- sum of stored object sizes this run
    sustained_mbps   numeric,                -- measured throughput (bytes / transfer wall-clock)
    size_mismatches  int,                    -- downloaded rows where size_downloaded != size_declared (real anomaly)
    mime_mismatches  int,                    -- downloaded rows where magic-byte sniff != declared mime
    status           text,                   -- 'success' | 'interrupted' | 'waf_tripped' | 'error'
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz
);

CREATE INDEX IF NOT EXISTS sam_attachment_download_90day_runs_run_id_idx
    ON ops.sam_attachment_download_runs (run_id);
CREATE INDEX IF NOT EXISTS sam_attachment_download_90day_runs_started_at_idx
    ON ops.sam_attachment_download_runs (started_at DESC);
