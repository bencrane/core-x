-- Company Profile Snapshots — the APPEND-ONLY history behind the Dossier's "Save Profile".
-- Applied to the hq-x control-plane Postgres (HQX_DB_URL_POOLED) that edge_api reads/writes.
-- Idempotent DDL (safe to re-run).
--
-- business.company_profiles is the canonical CURRENT dossier — ONE row per domain (its PK), the
-- hand-seeded / enrichment-refreshed source the page falls back to. THIS table is the operator's
-- verified history: every "Save Profile" click appends one IMMUTABLE row (never an update), so the
-- same domain accumulates many timestamped snapshots. The Dossier loads the LATEST snapshot for a
-- domain when one exists, else the company_profiles seed.
--
-- It is a SUPERSET of company_profiles: it also carries the Main Contact (signer/title/email) and
-- the per-section Verified map, which the canonical table does not hold (contact lives on the cal
-- booking). The tag arrays are jsonb, matching company_profiles.

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.company_profile_snapshots (
    id                 bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    domain             text        NOT NULL,                     -- resolution key (lowercased); NOT unique — many snapshots per domain
    company            text,                                     -- canonical company name
    signer_name        text,                                     -- Main Contact — name (from the cal booking, operator-editable)
    title              text,                                     -- Main Contact — title
    email              text,                                     -- Main Contact — email
    hq                 text,                                     -- headquarters
    headcount          text,                                     -- range string
    est_revenue_range  text,                                     -- estimated revenue range
    overview           text,                                     -- on-the-record firm description
    focus              jsonb       NOT NULL DEFAULT '[]'::jsonb,  -- string[] — focus areas
    industries         jsonb       NOT NULL DEFAULT '[]'::jsonb,  -- string[]
    geographies        jsonb       NOT NULL DEFAULT '[]'::jsonb,  -- string[]
    verified           jsonb       NOT NULL DEFAULT '{}'::jsonb,  -- per-section verify map {identity,signer,overview,focus,industries,geographies}
    saved_by           text,                                     -- operator identity (provenance; optional)
    created_at         timestamptz NOT NULL DEFAULT now()        -- snapshot instant; never updated
);

-- Latest-snapshot-by-domain is the hot read (Dossier load): domain equality + created_at DESC.
CREATE INDEX IF NOT EXISTS company_profile_snapshots_domain_created_idx
    ON business.company_profile_snapshots (domain, created_at DESC);
