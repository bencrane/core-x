CREATE TABLE IF NOT EXISTS ops.usaspending_award_search_merged_runs (
  id SERIAL PRIMARY KEY,
  feed TEXT NOT NULL DEFAULT 'usaspending_award_search_merged',
  run_mode TEXT NOT NULL, -- 'backfill' | 'daily'
  rows_merged INT,
  columns INT,
  table_rows_after INT,
  max_last_modified TEXT,
  status TEXT NOT NULL, -- 'success' | 'error'
  error_message TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_usaspending_award_search_merged_runs_feed_mode
  ON ops.usaspending_award_search_merged_runs (feed, run_mode);
