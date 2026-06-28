-- Mark ONE documenso_template per org as the DEFAULT for Confirm & Originate.
--
-- Org-scoped via the template's organization_id; the partial unique index enforces at-most-one
-- default per org. The set path (edge_api documenso_templates.set_default) clears the org's current
-- default THEN sets the new one in a transaction, so the index is never transiently violated.
--
-- business.documenso_templates is upstream-owned; this is ALTER-only + idempotent, safe to re-run on
-- every boot (cf. opportunities_opportunity_id.sql, engagement_mappings_*.sql).
ALTER TABLE business.documenso_templates
    ADD COLUMN IF NOT EXISTS is_default boolean NOT NULL DEFAULT false;

CREATE UNIQUE INDEX IF NOT EXISTS documenso_templates_one_default_per_org
    ON business.documenso_templates (organization_id)
    WHERE is_default;
