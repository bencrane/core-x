-- enrich_qual_combo_dualhat — stamp the qualifying prime combo onto the dual-hat leads.
--
-- Backfills qual_combo / qual_naics / qual_psc / qual_prime_amt_24m(_band) for the
-- subk_dualhat_eng_r425 cohort (541330 x R425): the combo that made each entity
-- eligible, with the combo-specific 24mo prime $ (not the all-codes total).
-- Run: --enrich pipelines/gtm/close_cohorts/enrich_qual_combo_dualhat.sql [--live]
WITH subs AS (
    SELECT uei FROM gtm_sub_combo_lanes
    GROUP BY 1
    HAVING SUM(sub_amt_24mo) >= 1e6 AND SUM(sub_amt_24mo) < 100e6
),
combo AS (
    SELECT uei,
           any_value(naics_title) AS naics_title,
           any_value(psc_title) AS psc_title,
           SUM(prime_obl_24mo) AS combo_amt
    FROM gtm_prime_combo_lanes
    WHERE naics_code = '541330' AND psc_code = 'R425'
    GROUP BY 1
    HAVING SUM(prime_obl_24mo) >= 1e6
)
SELECT c.uei,
       '541330 Engineering Services x R425 Support-Professional: Engineering/Technical'
           AS qual_combo,
       '541330 - ' || coalesce(c.naics_title, 'Engineering Services') AS qual_naics,
       'R425 - '   || coalesce(c.psc_title, 'SUPPORT- PROFESSIONAL: ENGINEERING/TECHNICAL') AS qual_psc,
       c.combo_amt AS qual_prime_amt_24m,
       CASE
         WHEN c.combo_amt < 5e6   THEN '$1M-$5M'
         WHEN c.combo_amt < 10e6  THEN '$5M-$10M'
         WHEN c.combo_amt < 25e6  THEN '$10M-$25M'
         WHEN c.combo_amt < 100e6 THEN '$25M-$100M'
         WHEN c.combo_amt < 500e6 THEN '$100M-$500M'
         ELSE '$500M+'
       END AS qual_prime_amt_24m_band
FROM combo c
JOIN subs USING (uei)
