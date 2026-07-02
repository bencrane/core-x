-- ops.people_from_contacts_runs — ledger for the contact→canonical identity backfill
-- (pipelines/gtm/backfill_people_from_contacts.py). One row per (source, build). Run-state
-- only; the canonical people + sidecar are the SoR. Idempotent DDL.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.people_from_contacts_runs (
    id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                 text        NOT NULL,
    source_platform      text        NOT NULL,   -- work_emails | phone_resolutions
    source_uri           text        NOT NULL,
    contacts_url_bearing bigint,                 -- distinct (person_id,url) fed
    people_candidates    bigint,                 -- pre-merge people rows (net-new land idempotently)
    sidecar_candidates   bigint,                 -- pre-merge sidecar rows
    people_before        bigint,
    people_after         bigint,
    status               text        NOT NULL,
    error                text,
    started_at           timestamptz,
    completed_at         timestamptz,
    recorded_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS people_from_contacts_runs_feed_idx  ON ops.people_from_contacts_runs (feed);
CREATE INDEX IF NOT EXISTS people_from_contacts_runs_rec_idx   ON ops.people_from_contacts_runs (recorded_at DESC);
