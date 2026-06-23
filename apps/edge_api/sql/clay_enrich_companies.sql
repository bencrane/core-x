-- Clay Enrich Company — raw landing grain (append-only). Applied to the hqx control-plane
-- Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run).
--
-- DISTINCT from gtm.clay_find_companies. That is the Clay *Find Companies* discovery grain.
-- THIS is the Clay *Enrich Company* dossier — a strict superset payload carrying org_id /
-- founded / website / logo_url / specialties / last_refresh / employee_count / follower_count and
-- locations[].inferred_location (geocoded), none of which the find-companies surface holds.
--
-- CONTRACT — RAW ONLY, NO EXPLODE. The Clay object lands EXACTLY as sent in raw_payload (jsonb),
-- the immutable, drift-proof source of truth. This surface deliberately does NOT unfurl the payload
-- into typed firmographic columns. The ONLY derived column is the resolution key the dataset is
-- connected on:
--   company_linkedin_url = raw_payload->>'url'  (e.g. "https://www.linkedin.com/company/bank-ozk")
-- materialized as a STORED generated column so it can NEVER drift from the payload. clay_company_id
-- is deliberately NOT the connect key — LinkedIn URL is. URL canonicalization → linkedin_slug (the
-- lake's canonical company join key) is a downstream concern, done in the Lance materializer, never
-- here.
--
-- GRAIN: one row per landed enrichment payload. PK = record_id = sha256(canonical raw_payload),
-- computed by the lander. APPEND-ONLY HISTORY: a refreshed Clay payload for the same company (new
-- last_refresh / changed fields) hashes differently and lands as a NEW immutable row; a byte-
-- identical re-send collapses to the same record_id and is a no-op under ON CONFLICT DO NOTHING.
-- company_linkedin_url is therefore NON-unique by design — the same company accumulates a timestamped
-- history. Current-state for a company is the latest landed_at:
--   SELECT DISTINCT ON (company_linkedin_url) * FROM gtm.clay_enrich_companies
--   ORDER BY company_linkedin_url, landed_at DESC;

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.clay_enrich_companies (
    record_id            text        PRIMARY KEY,                                       -- sha256(canonical raw_payload); exact-payload idempotency
    company_linkedin_url text        GENERATED ALWAYS AS (raw_payload->>'url') STORED,  -- the connect key; drift-proof, NON-unique (history)
    source               text        NOT NULL DEFAULT 'clay_enrich_companies',
    raw_payload          jsonb       NOT NULL,                                          -- the Clay enrich-company object, EXACTLY as sent
    landed_at            timestamptz NOT NULL DEFAULT now()
);

-- Hot reads: resolve a company by its LinkedIn URL; pull the newest landings.
CREATE INDEX IF NOT EXISTS clay_enrich_companies_li_url_idx ON gtm.clay_enrich_companies (company_linkedin_url);
CREATE INDEX IF NOT EXISTS clay_enrich_companies_landed_idx ON gtm.clay_enrich_companies (landed_at DESC);
