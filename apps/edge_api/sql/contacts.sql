-- gtm.contacts — curated GTM contact intake (append-only). Applied to the hqx control-plane
-- Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run).
--
-- CONTRACT. Operator/agent POSTs ONE contact per request as FLAT singular fields (no nested
-- raw_payload object on the wire). Each record is stored TWO ways, both faithful to source:
--   1. raw_payload (jsonb) — the body EXACTLY as sent. Immutable source of truth; drift-proof.
--   2. flat typed columns — verbatim values + canonical bridge keys (*_norm) computed server-side.
-- The full_name is split into first/middle/last server-side; the verbatim full_name is retained.
--
-- IDENTITY. work_email is the person identity key (this grain always carries a work email).
--   person_id  = sha256(work_email_norm)                     — stable cross-company person key (email-rail)
--   contact_key= sha256(work_email_norm | domain_norm)       — stable (person x company) identity (group → latest)
--
-- GRAIN: (person x company), APPEND-ONLY HISTORY. PK record_id = sha256 over identity + every
-- mutable field, so a byte-identical resend is a no-op (ON CONFLICT DO NOTHING) while ANY change
-- (title, is_main_contact, location, name, company bridge) lands a NEW historical row. A reader
-- takes the latest by landed_at per contact_key — operator corrections are history, never in-place
-- mutation (the Lance SoR downstream stays strictly append-only).

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.contacts (
    -- identity / lineage
    record_id              text        PRIMARY KEY,            -- sha256(identity | all mutable fields) — append-only history key
    contact_key            text        NOT NULL,               -- sha256(work_email_norm | domain_norm) — stable person×company key
    person_id              text        NOT NULL,               -- sha256(work_email_norm) — stable cross-company person key
    -- person
    full_name              text        NOT NULL,               -- verbatim as sent
    first_name             text,                               -- parsed from full_name
    middle_name            text,                               -- parsed (middle name / initial; may be NULL)
    last_name              text,                               -- parsed (NULL for single-token names)
    work_email             text        NOT NULL,               -- verbatim as sent
    work_email_norm        text        NOT NULL,               -- lower(trim(work_email)) — identity / dedup key
    job_title              text,                               -- verbatim (NOT normalized; job_level enum is a downstream stage)
    is_main_contact        boolean,                            -- coerced from "true"/"false" (NULL if absent/unparseable)
    -- location (verbatim)
    city                   text,
    state                  text,
    country                text,
    -- company (verbatim + canonical bridge keys)
    company_name           text        NOT NULL,               -- verbatim as sent
    company_domain         text,                               -- verbatim as sent (may be NULL)
    domain_norm            text,                               -- canonical bridge → firmographics_blitz.domain_norm (may be NULL)
    company_linkedin_url   text,                               -- verbatim as sent (may be NULL)
    company_linkedin_url_norm text,                            -- normalized: lower, strip scheme/www/locale/trailing slash
    -- raw source of truth + lineage
    source                 text        NOT NULL DEFAULT 'contacts',
    raw_payload            jsonb       NOT NULL,                -- the flat body, EXACTLY as sent
    landed_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS contacts_contact_key_idx     ON gtm.contacts (contact_key);
CREATE INDEX IF NOT EXISTS contacts_person_id_idx       ON gtm.contacts (person_id);
CREATE INDEX IF NOT EXISTS contacts_work_email_norm_idx ON gtm.contacts (work_email_norm);
CREATE INDEX IF NOT EXISTS contacts_domain_norm_idx     ON gtm.contacts (domain_norm);
CREATE INDEX IF NOT EXISTS contacts_company_li_norm_idx ON gtm.contacts (company_linkedin_url_norm);
CREATE INDEX IF NOT EXISTS contacts_company_name_idx    ON gtm.contacts (company_name);
CREATE INDEX IF NOT EXISTS contacts_landed_at_idx       ON gtm.contacts (landed_at DESC);
