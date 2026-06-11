-- organizations_theme.sql
--
-- Additive, idempotent. `business.organizations` is an UPSTREAM/shared table (edge_api only
-- FK-references it); this column backs the proposal render's brand identity so the agreement
-- shell is data-driven per organization instead of hardcoding "RARE STRUCTURE".
--
-- theme_config (jsonb) — the org's visual identity pulled into the DocRaptor brand shell:
--   { wordmark, legal_name, surface, ink, ink_strong, accent, muted, rule }
-- Any missing key falls back to the built-in Rare Structure default at render time, so a NULL /
-- partial theme_config never breaks a render.

ALTER TABLE business.organizations ADD COLUMN IF NOT EXISTS theme_config jsonb;

-- Seed the visual identity for the active orgs (shared Rare Structure palette today; only the
-- footer legal_name differs — Active Operators / Engineered Demand are DBAs, Rare Structure is the
-- LLC). Idempotent: a no-op where the named org is absent. The wordmark itself comes from
-- organizations.name at render time, so it is not duplicated here.
UPDATE business.organizations SET theme_config = jsonb_build_object(
  'surface', '#0a0e1a', 'ink', '#e4e4e7', 'ink_strong', '#fafafa',
  'accent', '#7b9fd4', 'muted', '#82828c', 'rule', '#2d3548',
  'legal_name', CASE name WHEN 'Rare Structure' THEN 'Rare Structure LLC' ELSE name END
) WHERE name IN ('Active Operators', 'Rare Structure', 'Engineered Demand');
