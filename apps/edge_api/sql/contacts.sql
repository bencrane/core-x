-- gtm.contacts — curated GTM contact intake (append-only). Applied to the hqx control-plane
-- Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run).
--
-- CONTRACT. Operator/agent POSTs ONE contact per request as FLAT singular fields (no nested
-- raw_payload object on the wire). Each record is stored TWO ways, both faithful to source:
--   1. raw_payload (jsonb) — the body EXACTLY as sent. Immutable source of truth; drift-proof.
--   2. flat typed columns — verbatim values + canonical bridge keys (*_norm) computed server-side.
-- The full_name is split into first/middle/last server-side; the verbatim full_name is retained.
--
-- IDENTITY (work_email is OPTIONAL — degrades gracefully when absent):
--   person_disc = 'email:'<work_email_norm>  if a work_email is present,
--                 'name:'<name_norm>          otherwise (lower/collapsed full_name).
--   person_id   = sha256(work_email_norm)     — email-rail key; NULL when no work_email.
--   contact_key = sha256(person_disc | company_bridge)  — stable (person × company) key; ALWAYS set.
--                 company_bridge = domain_norm or company_linkedin_url_norm or ''.
--
-- GRAIN: (person × company), APPEND-ONLY HISTORY. PK record_id = sha256(person_disc | every mutable
-- field), so a byte-identical resend is a no-op (ON CONFLICT DO NOTHING) while ANY change (title,
-- is_main_contact, location, name, company bridge) lands a NEW historical row. A reader takes the
-- latest by landed_at per contact_key — operator corrections are history, never in-place mutation
-- (the Lance SoR downstream stays strictly append-only).
--
-- NOTE on email-less identity: without a work_email (and no per-person LinkedIn on this grain), two
-- distinct people sharing a name at the same company collapse to one contact_key. That is inherent to
-- lacking a unique person key; supply work_email whenever available to keep them distinct.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.contacts (
    -- identity / lineage
    record_id              text        PRIMARY KEY,            -- sha256(person_disc | all mutable fields) — append-only history key
    contact_key            text        NOT NULL,               -- sha256(person_disc | company_bridge) — stable person×company key
    person_id              text,                               -- sha256(work_email_norm) — email-rail key; NULL when no work_email
    -- person
    full_name              text        NOT NULL,               -- verbatim as sent
    first_name             text,                               -- parsed from full_name
    middle_name            text,                               -- parsed (middle name / initial; may be NULL)
    last_name              text,                               -- parsed (NULL for single-token names)
    person_linkedin_url       text,                            -- the PERSON's LinkedIn URL (verbatim; feeds blitz /v2/enrichment/email). NULLABLE
    person_linkedin_url_norm  text,                            -- normalized person LinkedIn (lower, strip scheme/www/locale/trailing slash) — bridge/dedup key. NULLABLE
    work_email             text,                               -- verbatim as sent (OPTIONAL — may be NULL)
    work_email_norm        text,                               -- lower(trim(work_email)) — email-rail key (NULL when absent)
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

-- Converge an already-deployed table to the OPTIONAL-work_email contract (no-op on a fresh create
-- and idempotent on re-run): work_email / work_email_norm / person_id are nullable.
ALTER TABLE gtm.contacts ALTER COLUMN work_email      DROP NOT NULL;
ALTER TABLE gtm.contacts ALTER COLUMN work_email_norm DROP NOT NULL;
ALTER TABLE gtm.contacts ALTER COLUMN person_id       DROP NOT NULL;

-- Person LinkedIn URL (added post-deploy; no-op on fresh create, idempotent on re-run). The person
-- LinkedIn URL is the single input blitz /v2/enrichment/email requires, so the *_norm form is indexed.
ALTER TABLE gtm.contacts ADD COLUMN IF NOT EXISTS person_linkedin_url      text;
ALTER TABLE gtm.contacts ADD COLUMN IF NOT EXISTS person_linkedin_url_norm text;

CREATE INDEX IF NOT EXISTS contacts_contact_key_idx     ON gtm.contacts (contact_key);
CREATE INDEX IF NOT EXISTS contacts_person_id_idx       ON gtm.contacts (person_id);
CREATE INDEX IF NOT EXISTS contacts_work_email_norm_idx ON gtm.contacts (work_email_norm);
CREATE INDEX IF NOT EXISTS contacts_domain_norm_idx     ON gtm.contacts (domain_norm);
CREATE INDEX IF NOT EXISTS contacts_company_li_norm_idx ON gtm.contacts (company_linkedin_url_norm);
CREATE INDEX IF NOT EXISTS contacts_person_li_norm_idx  ON gtm.contacts (person_linkedin_url_norm);
CREATE INDEX IF NOT EXISTS contacts_company_name_idx    ON gtm.contacts (company_name);
CREATE INDEX IF NOT EXISTS contacts_landed_at_idx       ON gtm.contacts (landed_at DESC);
