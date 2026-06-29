-- Rename documenso_templates.recipients → documenso_templates.documenso_response
-- Store the full Documenso API response + custom metadata.
--
-- business.documenso_templates is upstream-owned; this is ALTER-only + idempotent.
-- The DO block handles the rename safely: if the column already exists with the new name,
-- the ALTER is skipped; if it still has the old name, the rename executes. Idempotent
-- across concurrent replica boots (each guarded by pg_advisory_xact_lock in migrate.py).

DO $$
BEGIN
    -- Check if the old column name exists
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'business'
          AND table_name = 'documenso_templates'
          AND column_name = 'recipients'
    ) THEN
        -- Old column exists; rename it
        ALTER TABLE business.documenso_templates RENAME COLUMN recipients TO documenso_response;
    ELSIF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'business'
          AND table_name = 'documenso_templates'
          AND column_name = 'documenso_response'
    ) THEN
        -- Neither old nor new column exists; add the new one (first-time setup on a fresh DB)
        ALTER TABLE business.documenso_templates ADD COLUMN documenso_response jsonb;
    END IF;
END $$;
