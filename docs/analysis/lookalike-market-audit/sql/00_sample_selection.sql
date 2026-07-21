-- Sample selection (run once; the resulting UEIs are hard-coded in lib_sidecar.SAMPLE).
-- FY23-25 subawardees with >= $1M subaward dollars, self-pairs excluded, ranked by
-- distinct primes (breadth) then dollars. The 8 chosen span reseller / electronics /
-- staffing / big-integrator shapes.
WITH s AS (
  SELECT subawardee_uei AS uei,
         SUM(subaward_amount_num) AS sub_amt,
         COUNT(*) AS n,
         COUNT(DISTINCT prime_awardee_uei) AS n_primes
  FROM subaward_canonical_slim_by_sub
  WHERE subaward_action_date_fiscal_year IN (2023, 2024, 2025)
    AND subawardee_uei <> prime_awardee_uei
  GROUP BY 1
  HAVING SUM(subaward_amount_num) >= 1000000)
SELECT s.uei, e.legal_business_name, s.sub_amt, s.n, s.n_primes
FROM s JOIN gtm_sam_entities e USING(uei)
ORDER BY s.n_primes DESC, s.sub_amt DESC
LIMIT 40;
