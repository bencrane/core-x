-- Company Profiles — the domain-keyed dossier payload the Dossier / booking-profile page reads.
-- Applied to the hq-x control-plane Postgres (HQX_DB_URL_POOLED) that edge_api reads.
-- Idempotent DDL (safe to re-run).
--
-- The booking-profile page is ADDRESSED by booking_id (corex.bookings); the CONTENT under it
-- (firmographics, overview, focus/industries/geographies) is resolved by DOMAIN — this table.
-- One company → one row, keyed by domain, shared across every booking from that company.
--
-- HAND-SEEDED for now (one row per current booking domain) — a deliberate throwaway stand-in so
-- the page has a real source to pull from. Later a projection from the parallel.ai research
-- (which lands in Lance) — or a cal.com coalesce — populates/refreshes these rows; `source`
-- distinguishes a hand-authored seed from a real enrichment. The page contract (resolve by domain)
-- stays stable regardless of where the values originate.

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.company_profiles (
    domain             text        PRIMARY KEY,                  -- canonical resolution key (lowercased); matches corex.bookings.domain
    company            text,                                     -- canonical company name
    hq                 text,                                     -- headquarters, e.g. "Houston, TX"
    headcount          text,                                     -- range string, e.g. "50–150"
    est_revenue_range  text,                                     -- estimated revenue range, e.g. "$10M–$25M"
    overview           text,                                     -- on-the-record firm description
    focus              jsonb       NOT NULL DEFAULT '[]'::jsonb,  -- string[] — focus areas
    industries         jsonb       NOT NULL DEFAULT '[]'::jsonb,  -- string[]
    geographies        jsonb       NOT NULL DEFAULT '[]'::jsonb,  -- string[]
    source             text        NOT NULL DEFAULT 'seed',       -- 'seed' = hand-authored stand-in; later 'parallel'/'cal' etc.
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);
