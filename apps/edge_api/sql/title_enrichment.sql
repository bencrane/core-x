-- gtm.title_enrichment — landing grain for job-title enrichment records (append-only). Applied to the
-- hqx control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run).
--
-- This is the PERSISTENCE sibling of the stateless /api/v1/titles/normalize gate (title_normalize_v1):
-- that route compiles ONE raw scraped title into the strict 6×22 taxonomy in-flight and returns it; this
-- table is where a caller LANDS an enriched title so the result is reusable (front-runs re-enrichment).
--
-- CONTRACT. Operator/agent POSTs ONE record per request as FLAT singular fields (no nested raw_payload
-- object on the wire). The ONLY required value is raw_job_title; EVERY other field is OPTIONAL and may be
-- sent null/omitted, so a bare {"raw_job_title": "..."} is a valid landing. Each record is stored TWO ways:
--   1. raw_payload (jsonb) — the body EXACTLY as sent. Immutable source of truth; drift-proof.
--   2. flat typed columns — verbatim values + the canonical bridge key (title_norm) computed server-side.
--
-- GRAIN: APPEND-ONLY HISTORY. PK record_id = sha256(title_norm | normalized_level | normalized_function |
-- confidence | model), so a byte-identical resend is a no-op (ON CONFLICT DO NOTHING) while a change to the
-- classification lands a NEW historical row. reasoning is stored but EXCLUDED from record_id (free-text,
-- non-deterministic). A reader takes the latest by landed_at.
--
-- NOTE on the taxonomy: normalized_level / normalized_function are stored as verbatim nullable text with NO
-- enum CHECK — this landing surface is intentionally permissive. Coercion to the closed 6×22 taxonomy is
-- the title_normalize_v1 concern, kept decoupled from persistence.
--
-- NOTE on passthrough columns: normalized_job_title and person_linkedin_url are optional, plain nullable
-- text — stored VERBATIM, and DELIBERATELY NOT indexed, NOT part of record_id, and NOT a cross-table
-- bridge. Do not "promote" them to *_norm / indexed / bridge keys.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.title_enrichment (
    -- identity / lineage
    record_id            text        PRIMARY KEY,                    -- sha256(title_norm | level | function | confidence | model) — append-only history key
    -- title (the ONLY required value) + canonical bridge key
    raw_job_title        text        NOT NULL,                       -- verbatim as sent — the single required field
    title_norm           text        NOT NULL,                       -- lower + whitespace-collapsed raw_job_title — canonical bridge/dedup key
    -- enrichment attributes (ALL nullable — a bare raw_job_title is a valid landing)
    normalized_job_title text,                                       -- caller-supplied normalized/cleaned title — verbatim; plain passthrough
    normalized_level     text,                                       -- taxonomy seniority (C-Team/VP/Director/Manager/Staff/Other) — verbatim, NOT enum-checked
    normalized_function  text,                                       -- taxonomy function (22-way) — verbatim, NOT enum-checked
    confidence           text,                                       -- 'low'/'medium'/'high' (or whatever the enricher emits) — verbatim
    model                text,                                       -- model id that produced the enrichment (lineage)
    reasoning            text,                                       -- free-text justification — STORED, but excluded from record_id
    person_linkedin_url  text,                                       -- optional person LinkedIn URL — verbatim; plain passthrough (NOT indexed, NOT a bridge)
    -- raw source of truth + lineage
    source               text        NOT NULL DEFAULT 'title_enrichment',
    raw_payload          jsonb       NOT NULL,                       -- the flat body, EXACTLY as sent
    landed_at            timestamptz NOT NULL DEFAULT now()
);

-- Converge an already-deployed table (an interim deploy may have created it before these columns existed).
-- Both are additive, nullable, NOT indexed and NOT part of record_id. No-op on a fresh create; idempotent.
ALTER TABLE gtm.title_enrichment ADD COLUMN IF NOT EXISTS normalized_job_title text;
ALTER TABLE gtm.title_enrichment ADD COLUMN IF NOT EXISTS person_linkedin_url  text;

CREATE INDEX IF NOT EXISTS title_enrichment_title_norm_idx ON gtm.title_enrichment (title_norm);
CREATE INDEX IF NOT EXISTS title_enrichment_level_idx      ON gtm.title_enrichment (normalized_level);
CREATE INDEX IF NOT EXISTS title_enrichment_function_idx   ON gtm.title_enrichment (normalized_function);
CREATE INDEX IF NOT EXISTS title_enrichment_landed_at_idx  ON gtm.title_enrichment (landed_at DESC);
