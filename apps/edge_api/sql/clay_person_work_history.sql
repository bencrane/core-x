-- clay_person_work_history — raw landing sink for the FULL Clay person profile / work-history
-- payload (append-only). Applied to the hqx control-plane Postgres (HQX_DB_URL_POOLED) that
-- edge_api writes. Idempotent DDL (safe to re-run).
--
-- WHY THIS TABLE. The sibling gtm.clay_find_people lands the (person × company) find-people grain
-- and EXPLODES known scalars into typed columns. This payload is different: the whole LinkedIn-style
-- profile — the full multi-position `experience[]` array plus education / publications /
-- certifications / volunteering / structured_location — sent as ONE object. It is stored VERBATIM as
-- a single jsonb blob with NO projection and NO typed attribute columns (cf. existing_claygent_payloads).
-- The only computed columns are lossless identity keys read server-side from the payload, so the blob
-- stays the immutable source of truth and downstream parsing (experience[] → a typed work-history
-- serving table) is a SEPARATE stage, never done here.
--
-- CONTRACT. Wire body: { "raw_payload": { "url": "...", "experience": [...], ... } } — one profile per
-- request. A bare object with no raw_payload wrapper is also accepted (Clay-misconfig tolerant).
-- raw_payload.url is REQUIRED (it is the per-person identity); a payload without it is rejected (422).
--
-- IDENTITY KEYS (computed server-side; the blob is never mutated):
--   raw_payload.url        -> linkedin_url_raw (verbatim) -> linkedin_url_norm
--                             -> person_id = sha256(linkedin_url_norm)   -- SAME derivation as
--                                gtm.clay_find_people.person_id, so the two tables JOIN on person_id.
--   raw_payload.profile_id -> profile_id (bigint)         -- stable LinkedIn numeric id, alternate key.
--   raw_payload.last_refresh -> last_refresh (timestamptz) -- enrichment recency, for latest-per-person.
--
-- GRAIN: one row per DISTINCT profile snapshot. PK = record_id = sha256(canonical_json(raw_payload)).
-- Byte-identical resends are idempotent via ON CONFLICT DO NOTHING (first-write-wins; the blob is
-- immutable). A RE-ENRICHMENT of the same person (any field changed, incl. last_refresh) lands as a
-- DISTINCT row — append-only snapshot history, by design. The current profile for a person is then
-- the latest snapshot:  SELECT DISTINCT ON (person_id) * ... ORDER BY person_id, last_refresh DESC.
--
-- FOLLOW-ON (deferred): a GIN index on (raw_payload -> 'experience') jsonb_path_ops would enable
-- containment pushdown for work-history analytics (e.g. "everyone who worked at coreweave.com"). It is
-- intentionally NOT created here: this is a high-throughput write surface (Clay fires one row per
-- request) and the analytic read tier is a separate parsed/indexed serving table. Add it there.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.clay_person_work_history (
    -- identity / lineage (lossless keys read from the payload — NOT an attribute projection)
    record_id         text        PRIMARY KEY,                         -- sha256(canonical_json(raw_payload))
    person_id         text        NOT NULL,                            -- sha256(linkedin_url_norm) — joins gtm.clay_find_people
    linkedin_url_raw  text        NOT NULL,                            -- raw_payload.url, verbatim
    linkedin_url_norm text        NOT NULL,                            -- scheme/www/query/trailing-slash stripped, lowercased
    profile_id        bigint,                                          -- raw_payload.profile_id — stable LinkedIn numeric id
    last_refresh      timestamptz,                                     -- raw_payload.last_refresh — enrichment recency
    -- raw source of truth + lineage
    source            text        NOT NULL DEFAULT 'clay_person_work_history',
    raw_payload       jsonb       NOT NULL,                            -- the Clay profile object, EXACTLY as sent
    landed_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS clay_person_work_history_person_idx       ON gtm.clay_person_work_history (person_id);
CREATE INDEX IF NOT EXISTS clay_person_work_history_li_norm_idx      ON gtm.clay_person_work_history (linkedin_url_norm);
CREATE INDEX IF NOT EXISTS clay_person_work_history_profile_idx      ON gtm.clay_person_work_history (profile_id) WHERE profile_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS clay_person_work_history_last_refresh_idx ON gtm.clay_person_work_history (last_refresh DESC);
CREATE INDEX IF NOT EXISTS clay_person_work_history_landed_idx       ON gtm.clay_person_work_history (landed_at DESC);
