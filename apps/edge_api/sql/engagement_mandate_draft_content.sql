-- Engagement mandate draft — staging columns for the direct-to-documenso "Originate Mandate" prep
-- flow. Applied to the hq-x control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes.
-- Idempotent DDL (safe to re-run).
--
-- The base table (business.engagement_mandate_draft_content) predates this file and is not defined
-- here; this adds the columns the per-opportunity staging page needs:
--   prefill_values — the operator-entered per-deal values (term, fee %, flat …), keyed by the
--                    template's text_field name. Stamped into the document at originate via
--                    Documenso prefillFields. The agreement TEXT comes from the template; only
--                    these values vary per deal.
--   archetype_id   — the economic-shape archetype this draft's template belongs to (denormalized
--                    from documenso_templates so the staging form knows its shape without a join).
-- `status` already exists on the base table (default 'draft'); the staging lifecycle reuses it.

CREATE SCHEMA IF NOT EXISTS business;

-- prefill_values — additive + idempotent. The staged per-deal values, empty until the prep form is
-- filled. jsonb object keyed by text_field: e.g. {"term_fee":"50000","duration_in_months":"12"}.
ALTER TABLE business.engagement_mandate_draft_content
    ADD COLUMN IF NOT EXISTS prefill_values jsonb NOT NULL DEFAULT '{}'::jsonb;

-- archetype_id — additive + idempotent. FK added guardedly (no ADD CONSTRAINT IF NOT EXISTS in PG);
-- ON DELETE RESTRICT so an archetype can't be dropped out from under a staged draft.
ALTER TABLE business.engagement_mandate_draft_content
    ADD COLUMN IF NOT EXISTS archetype_id uuid;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = 'business'
          AND table_name = 'engagement_mandate_draft_content'
          AND constraint_name = 'engagement_mandate_draft_content_archetype_id_fkey'
    ) THEN
        ALTER TABLE business.engagement_mandate_draft_content
            ADD CONSTRAINT engagement_mandate_draft_content_archetype_id_fkey
            FOREIGN KEY (archetype_id) REFERENCES business.engagement_archetypes (id) ON DELETE RESTRICT;
    END IF;
END $$;

-- The staging page loads the latest draft for an opportunity (then PATCHes it), so index the lookup.
CREATE INDEX IF NOT EXISTS engagement_mandate_draft_content_opportunity_idx
    ON business.engagement_mandate_draft_content (opportunity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS engagement_mandate_draft_content_archetype_idx
    ON business.engagement_mandate_draft_content (archetype_id);

-- Backfill archetype_id on existing drafts from their template (the draft stores the numeric
-- documenso_template_id as text, matching documenso_templates.documenso_template_id). Idempotent.
UPDATE business.engagement_mandate_draft_content d
   SET archetype_id = dt.archetype_id
  FROM business.documenso_templates dt
 WHERE dt.documenso_template_id = d.documenso_template_id
   AND d.archetype_id IS NULL;
