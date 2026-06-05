-- Confidence-gate review queue for the Parallel.ai enrichment workflow (Directive 24 §4.3).
-- The VALUE always lands in Lance regardless; this queue flags per-field cells whose Basis
-- confidence is null / low / medium for human review. Operational state only — no analytical
-- data here (that lives in the per-spec Lance dataset). Mirrored by OPS_DDL in enrich.py.
-- Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.parallel_review (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spec_id     text NOT NULL,
    run_id      text,                          -- Parallel run_id of the offending cell
    company_id  text NOT NULL,
    field       text NOT NULL,                 -- the output_schema field under review
    confidence  text,                          -- low | medium | NULL (the review trigger)
    resolved    boolean NOT NULL DEFAULT false,
    recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS parallel_review_spec_idx       ON ops.parallel_review (spec_id);
CREATE INDEX IF NOT EXISTS parallel_review_company_idx    ON ops.parallel_review (company_id);
CREATE INDEX IF NOT EXISTS parallel_review_unresolved_idx ON ops.parallel_review (resolved) WHERE NOT resolved;
