-- enrich_officer_comp_display — human-readable officer compensation on contacts.
--
-- officer_comp_display: "$1.3M" / "$330K" for FFATA-disclosed officers with comp > 0.
-- The raw number stays in officer_comp (filter on that); this is display-only.
-- Scope: all pushed contacts (ledger join in the engine drops the rest). Idempotent.
-- Run: --enrich pipelines/gtm/close_cohorts/enrich_officer_comp_display.sql [--live]
SELECT sam_person_id,
       CASE
         WHEN max_officer_amount >= 1e6 THEN
           '$' || replace(format('{:.1f}', max_officer_amount / 1e6), '.0', '') || 'M'
         WHEN max_officer_amount >= 1e3 THEN
           '$' || CAST(ROUND(max_officer_amount / 1e3) AS INT) || 'K'
         ELSE '$' || CAST(ROUND(max_officer_amount) AS INT)
       END AS officer_comp_display
FROM gtm_audience_people
WHERE (coalesce(is_exec_officer_prime, false) OR coalesce(is_exec_officer_sub, false))
  AND max_officer_amount > 0
