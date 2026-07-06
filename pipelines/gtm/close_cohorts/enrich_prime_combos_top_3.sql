-- enrich_prime_combos_top_3 — the top-3 prime combos as one at-a-glance list field.
--
-- prime_combos_top_3: ' | '-separated (combo labels contain commas — PSC titles like
-- "GUNS, THROUGH 30MM" — so comma can't be the list separator), ordered by 24mo $.
-- Same pure categorical labels as prime_combo_1..3.
-- Run: --enrich pipelines/gtm/close_cohorts/enrich_prime_combos_top_3.sql [--live]
WITH ranked AS (
    SELECT uei, naics_code, psc_code,
           coalesce(naics_title, '?') AS nt, coalesce(psc_title, '?') AS pt,
           SUM(prime_obl_24mo) AS amt,
           ROW_NUMBER() OVER (PARTITION BY uei
                              ORDER BY SUM(prime_obl_24mo) DESC NULLS LAST) AS rk
    FROM gtm_prime_combo_lanes
    WHERE prime_obl_24mo > 0 AND naics_code IS NOT NULL AND psc_code IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
)
SELECT uei,
       string_agg(naics_code || ' ' || nt || ' x ' || psc_code || ' ' || pt, ' | '
                  ORDER BY rk) AS prime_combos_top_3
FROM ranked
WHERE rk <= 3
GROUP BY 1
