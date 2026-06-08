-- Operations ledger for subawardee_solicitations (Tier 1/2) build runs.
-- Tracks bridge construction and resources probing phases separately.

CREATE TABLE IF NOT EXISTS ops.subawardee_solicitations_runs (
    id bigserial PRIMARY KEY,
    feed text NOT NULL,
    phase text,                        -- 'build_bridge' or 'probe_resources'
    row_count int,
    status text NOT NULL,               -- 'bridge_built', 'bridge_failed', 'resources_probed', 'resources_failed'
    error_message text,
    started_at timestamptz,
    executed_at timestamptz,
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS subawardee_solicitations_runs_feed
    ON ops.subawardee_solicitations_runs(feed, created_at DESC);

CREATE INDEX IF NOT EXISTS subawardee_solicitations_runs_phase
    ON ops.subawardee_solicitations_runs(phase, status, created_at DESC);
