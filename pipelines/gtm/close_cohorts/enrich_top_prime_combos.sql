-- enrich_top_prime_combos — stamp each pushed lead's top-3 prime code combos (24mo $ rank).
--
-- prime_combo_1..3: pure categorical label "NAICS code title x PSC code title" — exactly
-- the codes+titles as they appear in the lanes, NO dollar suffix (dollars live in their
-- own fields; identical combos must be identical strings so Close exact-match filters work).
-- Scope: every uei in the ledger (targets whatever has been pushed; ledger join in the
-- engine drops anything unpushed). Idempotent; re-run after mart rebuilds to refresh.
-- Run: --enrich pipelines/gtm/close_cohorts/enrich_top_prime_combos.sql [--live]
WITH ranked AS (
    SELECT uei, naics_code, psc_code,
           coalesce(naics_title, '?') AS nt, coalesce(psc_title, '?') AS pt,
           SUM(prime_obl_24mo) AS amt,
           ROW_NUMBER() OVER (PARTITION BY uei
                              ORDER BY SUM(prime_obl_24mo) DESC NULLS LAST) AS rk
    FROM gtm_prime_combo_lanes
    WHERE prime_obl_24mo > 0 AND naics_code IS NOT NULL AND psc_code IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),
lbl AS (
    SELECT uei, rk,
           naics_code || ' ' || nt || ' x ' || psc_code || ' ' || pt AS combo_label
    FROM ranked WHERE rk <= 3
)
SELECT uei,
       any_value(combo_label) FILTER (rk = 1) AS prime_combo_1,
       any_value(combo_label) FILTER (rk = 2) AS prime_combo_2,
       any_value(combo_label) FILTER (rk = 3) AS prime_combo_3
FROM lbl
GROUP BY 1
