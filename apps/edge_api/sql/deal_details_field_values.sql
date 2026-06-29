-- Rename deal_details.content -> field_values for clarity. The column holds ONLY the document's
-- prefilled form-field values (the deal-native analog of the old opportunity_specific_content.field_values)
-- — NOT a catch-all (that ambiguity is what made people want to stuff contacts into it; contacts now
-- live in business.deal_contacts). Guarded DO block: idempotent, safe to re-apply on every boot.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'business' AND table_name = 'deal_details' AND column_name = 'content'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'business' AND table_name = 'deal_details' AND column_name = 'field_values'
    ) THEN
        ALTER TABLE business.deal_details RENAME COLUMN content TO field_values;
    END IF;
END $$;
