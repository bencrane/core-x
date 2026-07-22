-- ops.crosswalk_sec_adv_ucc_runs — run ledger for the SEC-ADV × UCC private-credit
-- book crosswalk (pipelines/resolution/crosswalk_sec_adv_ucc.py). Canonical copy; the
-- pipeline embeds a byte-identical OPS_DDL string and applies it via `init_ops`.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.crosswalk_sec_adv_ucc_runs (
    id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                     text        NOT NULL,
    dataset_uri              text        NOT NULL,
    gleif_publish_date       text,
    advisers_total           bigint,
    advisers_pc              bigint,
    advisers_anchored        bigint,
    ucc_party_rows           bigint,
    ucc_party_resolved       bigint,
    link_rows                bigint,
    link_rows_llm            bigint,
    distinct_managers        bigint,
    distinct_pc_managers     bigint,
    distinct_debtor_filings  bigint,
    rows_written             bigint,
    status                   text        NOT NULL,
    error                    text,
    started_at               timestamptz,
    completed_at             timestamptz,
    recorded_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crosswalk_sec_adv_ucc_runs_feed_idx
    ON ops.crosswalk_sec_adv_ucc_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_sec_adv_ucc_runs_status_idx
    ON ops.crosswalk_sec_adv_ucc_runs (status);
CREATE INDEX IF NOT EXISTS crosswalk_sec_adv_ucc_runs_recorded_at_idx
    ON ops.crosswalk_sec_adv_ucc_runs (recorded_at DESC);
