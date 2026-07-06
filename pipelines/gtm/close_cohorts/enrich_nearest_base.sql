-- enrich_nearest_base — nearest MAJOR military installation per pushed lead.
--
-- nearest_major_base (site name) + nearest_major_base_miles from
-- gtm_entity_nearby_bases (FIRRMA-designated installations only; the any-site
-- answer also lives there but is not sent — operator-decided 2026-07-06).
-- Scope: all pushed leads (ledger join in the engine drops the rest). Idempotent.
-- Run: --enrich pipelines/gtm/close_cohorts/enrich_nearest_base.sql [--live]
SELECT uei, nearest_major_base, nearest_major_base_miles
FROM gtm_entity_nearby_bases
WHERE nearest_major_base IS NOT NULL
