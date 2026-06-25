-- Rename the misleadingly-named FK column on business.engagement_documenso_template_mappings.
--
-- The column holds documenso_templates.id (the uuid PK), NOT
-- documenso_templates.documenso_template_id (the TEXT external Documenso id / originate value).
-- Across this codebase `documenso_template_id` is an established name for that TEXT id; this lone
-- uuid violator is renamed to `documenso_template_uuid` so the join reads self-evidently
-- (`= dt.id`) and a future name-collision join (`= dt.documenso_template_id`, text) reads as
-- obviously wrong.
--
-- This table is upstream-owned (no CREATE TABLE in this repo); edge_api only reads it. The single
-- in-repo reader is engagement_mappings/queries.py, shipped in the SAME artifact as this rename.
--
-- Idempotent: renames only when the old column exists AND the new one does not, so it is safe to
-- re-run on every boot (and a no-op once applied).
DO $$
BEGIN
    IF EXISTS (
            SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'business'
               AND table_name   = 'engagement_documenso_template_mappings'
               AND column_name  = 'documenso_template_id'
       ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'business'
               AND table_name   = 'engagement_documenso_template_mappings'
               AND column_name  = 'documenso_template_uuid'
       )
    THEN
        ALTER TABLE business.engagement_documenso_template_mappings
            RENAME COLUMN documenso_template_id TO documenso_template_uuid;
    END IF;
END $$;
