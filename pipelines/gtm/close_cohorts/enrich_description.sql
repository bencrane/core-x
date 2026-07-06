-- enrich_description — company description onto the native Close lead description.
--
-- Waterfall (provider text verbatim): firmographics_blitz.about (LinkedIn) →
-- sba_dsbs_certified_firms.capabilities_narrative → web_homepage_meta description.
-- Cohort coverage at authoring: 308 / 75 / 14 of 347 → ~320 leads land a value.
-- Run: --enrich pipelines/gtm/close_cohorts/enrich_description.sql [--live]
WITH fb AS (
    SELECT e.uei, any_value(f.about) AS about
    FROM gtm_audience_entities e
    JOIN firmographics_blitz f ON f.domain_norm = e.normalized_domain
    WHERE f.about IS NOT NULL AND length(f.about) > 30
    GROUP BY 1
),
wh AS (
    SELECT e.uei, any_value(coalesce(w.meta_description, w.og_description)) AS meta_desc
    FROM gtm_audience_entities e
    JOIN web_homepage_meta w ON w.normalized_domain = e.normalized_domain
    WHERE coalesce(w.meta_description, w.og_description) IS NOT NULL
    GROUP BY 1
)
SELECT e.uei,
       coalesce(fb.about, ds.capabilities_narrative, wh.meta_desc) AS description
FROM gtm_audience_entities e
LEFT JOIN fb USING (uei)
LEFT JOIN (SELECT uei, any_value(capabilities_narrative) AS capabilities_narrative
           FROM sba_dsbs_certified_firms
           WHERE capabilities_narrative IS NOT NULL GROUP BY 1) ds USING (uei)
LEFT JOIN wh USING (uei)
WHERE coalesce(fb.about, ds.capabilities_narrative, wh.meta_desc) IS NOT NULL
