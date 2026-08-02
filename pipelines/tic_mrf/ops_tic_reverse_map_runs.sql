-- Terminal-state + idempotency ledger for the TiC reverse-mapper.
-- Written by pipelines/tic_mrf/reverse_map.py:record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state. Read by already_ingested() to
-- guard duplicate appends on retry / resharded nationwide reruns. Mirrors the
-- ops.* contract used by the SAM / provider_360 / form5500 feeds. Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and
-- applies it (CREATE ... IF NOT EXISTS) before each read/insert. Keep in sync.
--
-- Idempotency key: (payer, source_file_url, file_version).
--   * source_file_url is TOKEN-STRIPPED (query string / SAS sig removed by
--     strip_url_token) — UHC re-mints the SAS `sig` on every master-index fetch,
--     so the raw downloadUrl is unstable; the blob path is the identity.
--   * file_version is NOT NULL — derive_file_version guarantees a surrogate
--     (ETag > Last-Modified > date-slug from the filename > content-length).
--     A NULL here would be distinct to the unique index, silently disabling both
--     the skip and the ON CONFLICT upsert.
-- A monthly drop publishes a new file_version, so the same employer/plan file
-- re-ingests once per real publish, never twice for the same bytes.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.tic_reverse_map_runs (
    id                 bigserial PRIMARY KEY,
    run_id             text NOT NULL,        -- '<payer>-<UTC ISO>' batch identifier
    payer              text NOT NULL,        -- aetna | uhc | ...
    source_file_url    text NOT NULL,        -- the in-network file reverse-mapped (TOKEN-STRIPPED)
    file_version       text NOT NULL,        -- ETag > Last-Modified > date-slug > bytes surrogate; never NULL
    cohort_size        int,                  -- target NPIs searched this run
    matched_groups     int,                  -- provider_reference groups intersecting cohort
    rows_emitted       int,                  -- flat (npi x billing_code x price) rows appended
    compressed_bytes   bigint,               -- bytes pulled off the wire
    uncompressed_bytes bigint,               -- bytes after gunzip
    throughput_mb_s    numeric,              -- uncompressed MB / parse wall-clock
    peak_rss_mb        numeric,              -- resident-set ceiling of the worker
    parse_ms           numeric,
    status             text NOT NULL,        -- success | error | skipped
    error              text,
    started_at         timestamptz,
    completed_at       timestamptz,
    recorded_at        timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS tic_reverse_map_runs_uq
    ON ops.tic_reverse_map_runs (payer, source_file_url, file_version);
CREATE INDEX IF NOT EXISTS tic_reverse_map_runs_recorded_idx
    ON ops.tic_reverse_map_runs (recorded_at DESC);
CREATE INDEX IF NOT EXISTS tic_reverse_map_runs_payer_idx
    ON ops.tic_reverse_map_runs (payer);
