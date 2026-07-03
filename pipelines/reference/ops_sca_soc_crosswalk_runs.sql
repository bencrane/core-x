-- Operational ledger for the SCA↔SOC occupation crosswalk build.
-- Best-effort observability (never a correctness gate): the materialize is fail-closed
-- on its own guards; this table records what ran, when, and the tier distribution.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sca_soc_crosswalk_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stage               text        NOT NULL,          -- 'manifest' | 'retrieve'
    dataset             text        NOT NULL,          -- 'sca_soc_crosswalk' | 'sca_soc_candidates' | '_sca_soc_crosswalk_manifest'
    dataset_uri         text,
    prompt_version      text,
    model_id            text,
    embed_model_version text,
    manifest_sha256     text,
    agent_results_sha256 text,
    sca_universe        int,
    t1_rows             int,
    t2_rows             int,
    unmatched_rows      int,
    register_only_rows  int,
    guard_failures      int,
    indexes_built       text[],
    metrics             jsonb,
    status              text        NOT NULL,          -- 'success' | 'gap' | 'error'
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sca_soc_crosswalk_runs_dataset_idx  ON ops.sca_soc_crosswalk_runs (dataset);
CREATE INDEX IF NOT EXISTS sca_soc_crosswalk_runs_status_idx   ON ops.sca_soc_crosswalk_runs (status);
CREATE INDEX IF NOT EXISTS sca_soc_crosswalk_runs_recorded_idx ON ops.sca_soc_crosswalk_runs (recorded_at DESC);
