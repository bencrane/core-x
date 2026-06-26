-- Terminal-state table for the SAM.gov attachment byte-download worker.
-- Written by pipelines/sam_gov/sam_attachment_download.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SAM manifest / POC / crosswalk feeds. One row per
-- download batch (a single run_id invocation). Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and
-- applies it (CREATE ... IF NOT EXISTS) before each insert. Keep in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sam_attachment_download_runs (
    id               bigserial PRIMARY KEY,
    run_id           text        NOT NULL,   -- '<tier>-<UTC ISO>' batch identifier
    worklist_tier    text,                   -- T0 | T1 | T2 | T3 | T4 | unions e.g. 'T0+T2'
    worklist_filter  text,                   -- exact composed gate predicate (provenance)
    attempted        int,                    -- files fetched this run (excludes resume-skips)
    downloaded       int,                    -- HTTP 200, bytes stored
    failed           int,                    -- hard 4xx / retries-exhausted / wall-clock backstop
    restricted       int,                    -- 401 / 403 UNAUTHORIZED (no retry)
    gone             int,                    -- 400 / 410 removed resource (no retry)
    oversize         int,                    -- real Content-Length >= 50 MB cap (declared size lies; no store)
    bytes_downloaded bigint,                 -- sum of stored object sizes this run
    sustained_mbps   numeric,                -- measured throughput (bytes / transfer wall-clock)
    size_mismatches  int,                    -- downloaded rows whose bytes are NOT modulo-10MB-consistent with declared size_expected (real anomaly; SAM's >=10 MB mod-10MB corruption is excluded so it doesn't false-flag)
    mime_mismatches  int,                    -- downloaded rows where magic-byte sniff != declared mime
    status           text,                   -- 'success' | 'interrupted' | 'error'
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz
);

CREATE INDEX IF NOT EXISTS sam_attachment_download_runs_run_id_idx
    ON ops.sam_attachment_download_runs (run_id);
CREATE INDEX IF NOT EXISTS sam_attachment_download_runs_tier_idx
    ON ops.sam_attachment_download_runs (worklist_tier);
CREATE INDEX IF NOT EXISTS sam_attachment_download_runs_started_at_idx
    ON ops.sam_attachment_download_runs (started_at DESC);
