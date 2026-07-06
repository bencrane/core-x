-- subk_dualhat_eng_r425 — dual-hat engineering subawardees (operator cut, 2026-07-06).
--
-- WHO: entities with 24mo subaward income $1M-$100M (all codes) that ALSO primed
-- 541330 x R425 (Engineering Services x Support-Professional: Engineering/Technical)
-- for >=$1M aggregate obligations in the same 24mo window. No employee-band filter,
-- no prime cap — includes the majors, per instruction. Reference: 347 entities.
--
-- CONTACTS: every dialable person at those entities, plus the best person per entity
-- that has no dialable (priority: enrichment_state, then dm, then source count) so
-- every entity lands as a Close lead.
--
-- Ride-along columns match Close custom fields created 2026-07-06 (lead: $ windows +
-- text bands, primary PoP, FFATA/DSBS/FSRS designations, NAICS 2-6 rollups;
-- contact: enrichment_state, is_ffata_officer, officer_comp).
WITH subs AS (
    SELECT uei FROM gtm_sub_combo_lanes
    GROUP BY 1
    HAVING SUM(sub_amt_24mo) >= 1e6 AND SUM(sub_amt_24mo) < 100e6
),
primes AS (
    SELECT uei FROM gtm_prime_combo_lanes
    WHERE naics_code = '541330' AND psc_code = 'R425'
    GROUP BY 1
    HAVING SUM(prime_obl_24mo) >= 1e6
),
cut AS (SELECT uei FROM subs INTERSECT SELECT uei FROM primes),
ranked AS (
    SELECT p.*,
           ROW_NUMBER() OVER (PARTITION BY p.uei ORDER BY
               CASE p.enrichment_state WHEN 'dialable' THEN 0 WHEN 'emailable' THEN 1
                    WHEN 'bridged_no_contacts' THEN 2 ELSE 3 END,
               CASE WHEN p.dm_class = 'dm' THEN 0 ELSE 1 END,
               p.n_sources DESC, p.sam_person_id) AS rn
    FROM gtm_audience_people p
    JOIN cut USING (uei)
)
SELECT b.sam_person_id,
       -- contact custom fields
       b.enrichment_state,
       CASE WHEN coalesce(b.is_exec_officer_prime, false)
              OR coalesce(b.is_exec_officer_sub, false) THEN 'true' ELSE 'false' END
           AS is_ffata_officer,
       CASE WHEN (coalesce(b.is_exec_officer_prime, false)
              OR coalesce(b.is_exec_officer_sub, false))
             AND b.max_officer_amount > 0 THEN b.max_officer_amount END AS officer_comp,
       -- lead custom fields
       e.employee_size_band AS size_band,
       e.sub_amt_12mo AS sub_amt_12m, e.sub_amt_24mo AS sub_amt_24m,
       e.sub_amt_60mo AS sub_amt_5y,  e.sub_amt_lifetime AS sub_amt_total,
       e.prime_obl_12mo AS prime_amt_12m, e.prime_obl_24mo AS prime_amt_24m,
       e.prime_obl_60mo AS prime_amt_5y,  e.prime_obl_lifetime AS prime_amt_total,
       e.sub_amt_12mo_band AS sub_amt_12m_band, e.sub_amt_24mo_band AS sub_amt_24m_band,
       e.sub_amt_60mo_band AS sub_amt_5y_band,  e.sub_amt_lifetime_band AS sub_amt_total_band,
       e.prime_obl_12mo_band AS prime_amt_12m_band, e.prime_obl_24mo_band AS prime_amt_24m_band,
       e.prime_obl_60mo_band AS prime_amt_5y_band,  e.prime_obl_lifetime_band AS prime_amt_total_band,
       e.n_subawards_24mo AS n_subawards,
       CAST(e.sub_last_action_date AS VARCHAR) AS last_subaward_date,
       e.primary_pop_state AS pop_state_primary,
       e.primary_pop_county AS pop_county_primary,
       CASE WHEN e.is_ffata_disclosing THEN 'true' ELSE 'false' END AS is_ffata,
       CASE WHEN coalesce(e.in_dsbs, false) THEN 'true' ELSE 'false' END AS in_dsbs,
       NULLIF(concat_ws(', ',
           CASE WHEN e.dsbs_8a THEN '8(a)' END,
           CASE WHEN e.dsbs_hubzone THEN 'HUBZone' END,
           CASE WHEN e.dsbs_wosb THEN 'WOSB' END,
           CASE WHEN e.dsbs_edwosb THEN 'EDWOSB' END,
           CASE WHEN e.dsbs_sdvosb THEN 'SDVOSB' END,
           CASE WHEN e.dsbs_vosb THEN 'VOSB' END), '') AS dsbs_certs,
       NULLIF(concat_ws(', ',
           CASE WHEN e.fsrs_sdvosb THEN 'SDVOSB' END,
           CASE WHEN e.fsrs_vosb THEN 'VOSB' END,
           CASE WHEN e.fsrs_wosb THEN 'WOSB' END,
           CASE WHEN e.fsrs_edwosb THEN 'EDWOSB' END,
           CASE WHEN e.fsrs_woman_owned THEN 'Woman-Owned' END,
           CASE WHEN e.fsrs_hubzone THEN 'HUBZone' END,
           CASE WHEN e.fsrs_8a THEN '8(a)' END,
           CASE WHEN e.fsrs_sdb THEN 'SDB' END,
           CASE WHEN e.fsrs_self_sdb THEN 'Self-Cert SDB' END,
           CASE WHEN e.fsrs_minority THEN 'Minority-Owned' END), '') AS fsrs_designations,
       e.naics_2, e.naics_3, e.naics_4, e.naics_5, e.naics_6
FROM ranked b
JOIN gtm_audience_entities e USING (uei)
WHERE b.enrichment_state = 'dialable' OR b.rn = 1
