-- gtm.title_enrichment — landing grain for job-title enrichment records (append-only). Applied to the
-- hqx control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent DDL (safe to re-run).
--
-- This is the PERSISTENCE sibling of the stateless /api/v1/titles/normalize gate (title_normalize_v1):
-- that route compiles ONE raw scraped title into the strict 6×22 taxonomy in-flight and returns it; this
-- table is where a caller LANDS an enriched title so the result is reusable (front-runs re-enrichment) and
-- joinable to title-bearing records (gtm.contacts.job_title) via the normalized bridge key.
--
-- CONTRACT. Operator/agent POSTs ONE record per request as FLAT singular fields (no nested raw_payload
-- object on the wire). The ONLY required value is raw_job_title; every enrichment attribute is OPTIONAL,
-- so a bare {"raw_job_title": "..."} is a valid landing (an as-yet-unenriched title). Each record is stored
-- TWO ways, both faithful to source:
--   1. raw_payload (jsonb) — the body EXACTLY as sent. Immutable source of truth; drift-proof. A richer
--      enricher can send extra keys with no schema change — they survive verbatim here.
--   2. flat typed columns — verbatim values + the canonical bridge key (title_norm) computed server-side.
--
-- BRIDGE / GRAIN: title_norm = lower + whitespace-collapsed raw_job_title — the deterministic, caller-
-- reproducible key downstream records resolve against (normalize(contact.job_title) = title_norm). The
-- grain is APPEND-ONLY HISTORY: PK record_id = sha256(title_norm | normalized_level | normalized_function |
-- confidence | model), so a byte-identical resend is a no-op (ON CONFLICT DO NOTHING) while ANY change to
-- the classification lands a NEW historical row. reasoning is STORED but deliberately EXCLUDED from
-- record_id — it is free-text, non-deterministic model output, and folding it into the identity hash would
-- spawn a new row on every re-run even when the (level, function) verdict is unchanged, defeating dedup.
-- A reader takes the latest enrichment per title by landed_at — corrections are history, never in-place
-- mutation (the Lance SoR downstream stays strictly append-only).
--
-- NOTE on the taxonomy: normalized_level / normalized_function are stored as verbatim nullable text with NO
-- enum CHECK — this landing surface is intentionally permissive (it accepts whatever a caller sends, incl.
-- as-yet-unclassified nulls). Coercion to the closed 6×22 taxonomy is the title_normalize_v1 concern, kept
-- decoupled from persistence so the two never drift through a shared constraint.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.title_enrichment (
    -- identity / lineage
    record_id            text        PRIMARY KEY,                    -- sha256(title_norm | level | function | confidence | model) — append-only history key
    -- title (the ONLY required value) + canonical bridge key
    raw_job_title        text        NOT NULL,                       -- verbatim as sent — the single required field
    title_norm           text        NOT NULL,                       -- lower + whitespace-collapsed raw_job_title — canonical bridge/dedup key
    -- enrichment attributes (ALL nullable — a bare raw_job_title is a valid landing)
    normalized_level     text,                                       -- taxonomy seniority (C-Team/VP/Director/Manager/Staff/Other) — verbatim, NOT enum-checked
    normalized_function  text,                                       -- taxonomy function (22-way) — verbatim, NOT enum-checked
    confidence           text,                                       -- 'low'/'medium'/'high' (or whatever the enricher emits) — verbatim
    model                text,                                       -- model id that produced the enrichment (lineage)
    reasoning            text,                                       -- free-text justification — STORED, but excluded from record_id
    -- raw source of truth + lineage
    source               text        NOT NULL DEFAULT 'title_enrichment',
    raw_payload          jsonb       NOT NULL,                       -- the flat body, EXACTLY as sent
    landed_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS title_enrichment_title_norm_idx ON gtm.title_enrichment (title_norm);
CREATE INDEX IF NOT EXISTS title_enrichment_level_idx      ON gtm.title_enrichment (normalized_level);
CREATE INDEX IF NOT EXISTS title_enrichment_function_idx   ON gtm.title_enrichment (normalized_function);
CREATE INDEX IF NOT EXISTS title_enrichment_landed_at_idx  ON gtm.title_enrichment (landed_at DESC);
