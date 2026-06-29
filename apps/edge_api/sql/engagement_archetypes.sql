-- Engagement Archetypes — the economic SHAPE of an engagement (which pricing variables exist and how
-- they combine), the low-cardinality classifier ABOVE business.documenso_templates. Applied to the
-- hq-x control-plane Postgres (HQX_DB_URL_POOLED) that edge_api reads. Idempotent DDL (safe to re-run).
--
-- An archetype is a TYPE, not a value: the specific numbers (term length, fee %, flat amount) are a
-- parameterization WITHIN an archetype and never mint a new one. A new archetype appears only when the
-- STRUCTURE changes — a variable is added/removed, or the fee combination rule changes. Each
-- documenso_template belongs to exactly one archetype (its body either carries the perf-fee clause or
-- it does not); one archetype maps to many templates (the per-term / per-fee matrix).
--
--   engagement_archetypes (1) ──< documenso_templates (N) ──< engagement_documenso_template_mappings
--
-- Seeded with the two live archetypes:
--   term_only             — a fixed term / retainer only; no per-deal performance fee.
--   term_plus_greater_of  — a term PLUS a per-deal performance fee = greater_of(percentage, flat).

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.engagement_archetypes (
    -- identity
    id                     uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    key                    text        NOT NULL UNIQUE,    -- stable code selector ('term_only', 'term_plus_greater_of')
    name                   text        NOT NULL,           -- operator-facing label
    description            text,
    -- economic shape (the classifier — STRUCTURE only, never per-deal values). NULL basis = term-only
    -- (no per-deal performance fee); a non-null basis names how the per-deal fee is computed.
    performance_fee_basis  text        CHECK (performance_fee_basis IN ('greater_of','lesser_of','sum','percentage_only','flat_only')),
    -- timeline
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);

-- The two live archetypes. Idempotent: re-running leaves existing rows (and any later edits) untouched.
INSERT INTO business.engagement_archetypes (key, name, description, performance_fee_basis)
VALUES
    ('term_only',
     'Term only',
     'Priced on a fixed term / retainer only — no per-deal performance fee.',
     NULL),
    ('term_plus_greater_of',
     'Term + performance fee (greater of % or flat)',
     'A term / retainer PLUS a per-deal performance fee taken as the greater of a percentage or a flat amount.',
     'greater_of')
ON CONFLICT (key) DO NOTHING;

-- documenso_templates.archetype_id — additive + idempotent (the table predates this file and is not
-- defined here). FK added guardedly (Postgres has no ADD CONSTRAINT IF NOT EXISTS); ON DELETE RESTRICT
-- so an archetype cannot be dropped out from under the templates that belong to it.
ALTER TABLE business.documenso_templates ADD COLUMN IF NOT EXISTS archetype_id uuid;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = 'business'
          AND table_name = 'documenso_templates'
          AND constraint_name = 'documenso_templates_archetype_id_fkey'
    ) THEN
        ALTER TABLE business.documenso_templates
            ADD CONSTRAINT documenso_templates_archetype_id_fkey
            FOREIGN KEY (archetype_id) REFERENCES business.engagement_archetypes (id) ON DELETE RESTRICT;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS documenso_templates_archetype_idx
    ON business.documenso_templates (archetype_id);

-- Backfill existing templates from their declared merge fields: a template whose documenso_response.text_fields
-- carry the performance-fee tokens is term_plus_greater_of, else term_only. Idempotent (fills NULLs
-- only), data-driven (no hardcoded template ids).
UPDATE business.documenso_templates dt
   SET archetype_id = (
       SELECT id FROM business.engagement_archetypes
        WHERE key = CASE
            WHEN (dt.documenso_response->'text_fields') ?| array['percentage_deal_fee','flat_deal_fee_amount','term_fee']
                THEN 'term_plus_greater_of'
            ELSE 'term_only'
        END
   )
 WHERE dt.archetype_id IS NULL;
