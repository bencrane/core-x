-- Absolute ceilings that bound the gate-OFF candidate set.
-- firms_10m_floor is the hard upper bound on c_raw at the DEFAULT market_prime_floor:
-- every candidate passes `JOIN gtm_entity_behavior_rollup ... WHERE prime_obl_60mo>=1e7`,
-- so c_raw <= firms_10m_floor. Measured 14,482 < 25,000 => the fetch ceiling cannot
-- bite at default dials. At floor=0 the bound rises to distinct_primes_sig (194,043).
SELECT
  (SELECT COUNT(*) FROM gtm_entity_behavior_rollup WHERE prime_obl_60mo >= 1e7) AS firms_10m_floor,
  (SELECT COUNT(*) FROM gtm_entity_behavior_rollup WHERE prime_obl_60mo > 0)    AS firms_any_prime60,
  (SELECT COUNT(DISTINCT uei) FROM gtm_prime_code_signature)                    AS distinct_primes_sig,
  (SELECT COUNT(*) FROM gtm_entity_behavior_rollup)                             AS rollup_rows;
