-- business.opportunities.opportunity_id — the opportunity's PUBLIC 8-char handle.
--
-- The UUID `id` stays the internal primary key (and the FK every other table references). This adds a
-- SEPARATE, public-facing `opportunity_id` = the first 8 chars of that UUID, DERIVED automatically by
-- the database on insert (GENERATED ... STORED) — zero application logic, nothing to keep in sync.
--
-- This 8-char value is what edge_api stamps as the Documenso envelope's `externalId` at originate and
-- what the prospect signing link carries: `/p/m/{opportunity_id}/{document_id}`. The signing gate then
-- matches on this exact value (no prefix logic). The full row UUID is NO LONGER the externally-visible
-- opportunity id.
--
-- SECURITY NOTE: as the prospect-link access capability, this is 8 hex chars = 32 bits of entropy
-- (down from the full UUID). The `document_id` is the unique lookup pin; this handle is the gate.
-- Adequate for the performative signing link at current volume; widen the prefix length here (and in
-- the originate stamp) if that ever changes.
--
-- Applied to the hq-x control-plane Postgres (HQX_DB_URL_POOLED). Idempotent (safe to re-run).

ALTER TABLE business.opportunities
    ADD COLUMN IF NOT EXISTS opportunity_id text
    GENERATED ALWAYS AS (LEFT(id::text, 8)) STORED;

-- The public handle is a resolution key — give it a BTREE scalar index. NON-unique by design: a
-- unique constraint on a derived 8-char prefix could (astronomically rarely) HARD-FAIL an insert on a
-- first-8 collision; the signing flow never resolves an opportunity BY this column (document_id is the
-- unique pin), so non-unique is the safe, correct choice.
CREATE INDEX IF NOT EXISTS idx_opportunities_opportunity_id
    ON business.opportunities (opportunity_id);
