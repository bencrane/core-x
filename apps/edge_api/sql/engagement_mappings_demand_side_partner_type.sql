-- Add business.engagement_documenso_template_mappings.demand_side_partner_type
--
-- Classifies each engagement mapping by the kind of demand-side partner the originating org is:
--   Rare Structure   → 'capital-provider'   (provides capital)
--   Active Operators → 'services-partner'   (provides services)
-- The value is an attribute of the originating organization, so it is backfilled by
-- organization_id; any future mapping row for these orgs inherits the right type on next boot.
--
-- This table is upstream-owned (no CREATE TABLE in this repo); edge_api only reads it and applies
-- incremental, idempotent ALTERs here (cf. engagement_mappings_template_uuid_rename.sql).
--
-- Idempotent / self-healing, safe to re-run on every boot:
--   * ADD COLUMN IF NOT EXISTS — a no-op once the column is present.
--   * Each backfill is guarded WHERE demand_side_partner_type IS NULL, so it fills only unset rows
--     and NEVER clobbers an explicit operator override. The org is resolved by slug; an unknown
--     slug yields a NULL subselect that matches no rows (safe no-op).
-- Nullable on purpose: other/future partner types are unconstrained (no CHECK), and rows
-- provisioned upstream may legitimately carry no type yet.
ALTER TABLE business.engagement_documenso_template_mappings
    ADD COLUMN IF NOT EXISTS demand_side_partner_type text;

UPDATE business.engagement_documenso_template_mappings
   SET demand_side_partner_type = 'capital-provider'
 WHERE demand_side_partner_type IS NULL
   AND organization_id = (SELECT id FROM business.organizations WHERE slug = 'rare-structure');

UPDATE business.engagement_documenso_template_mappings
   SET demand_side_partner_type = 'services-partner'
 WHERE demand_side_partner_type IS NULL
   AND organization_id = (SELECT id FROM business.organizations WHERE slug = 'active-operators');
