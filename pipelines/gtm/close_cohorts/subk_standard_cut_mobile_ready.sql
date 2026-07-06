-- subk_standard_cut_mobile_ready — active subawardees, standard cut, mobile in hand.
--
-- WHO: people at entities in the subk standard cut (2026-07-06 audience investigation)
-- that already have a found mobile — dialable today, zero credit spend.
-- Reference size at authoring: 400 people / 310 entities (live re-derivation;
-- the 2026-07-06 CSV snapshot read 406/316 before contactability settled).
--
-- Standard cut (playbook: hq/directives/2026-07-06-subk-audience-cuts-playbook.md):
--   * context family on the PSC of the PRIME award subbed under (eng_labor /
--     facilities / hardware_parts), per-(uei, family) 24mo sub income >= $1M AND < $100M,
--   * employee band 1-500.
-- Runs off the audience marts (build: scripts/build_gtm_audience_marts.py). Their 24mo
-- window is anchored at mart as_of (2026-07-06 build == the playbook's frozen
-- DATE '2024-07-06' anchor); rebuilding the marts rolls the window — move deliberately.
-- Counts are FSRS floors (>=$30K subs on covered awards).
WITH fam_amt AS (
    SELECT uei,
           CASE
             WHEN psc_code LIKE 'R4%' OR psc_code LIKE 'R6%' OR psc_code LIKE 'R7%'
               OR psc_code IN ('D301','D302','D307','D308','D316','D399') THEN 'eng_labor'
             WHEN psc_code LIKE 'M%' OR psc_code LIKE 'S2%' THEN 'facilities'
             WHEN regexp_matches(psc_code, '^[1-5]') THEN 'hardware_parts'
             ELSE NULL
           END AS fam,
           SUM(sub_amt_24mo) AS sub_amt
    FROM gtm_sub_combo_lanes
    GROUP BY 1, 2
    HAVING fam IS NOT NULL AND sub_amt >= 1e6 AND sub_amt < 100e6
),
cut_ueis AS (
    SELECT DISTINCT f.uei
    FROM fam_amt f
    JOIN gtm_audience_entities e USING (uei)
    WHERE e.employee_size_band IN ('1-10','11-50','51-200','201-500')
)
SELECT p.sam_person_id
FROM gtm_audience_people p
JOIN cut_ueis USING (uei)
WHERE p.enrichment_state = 'dialable'
