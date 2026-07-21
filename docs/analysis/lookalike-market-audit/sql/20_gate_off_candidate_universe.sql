-- THE load-bearing query: the gate-OFF (default) base lookalike candidate set.
-- Verbatim mirror of sub_dossier_v1.sql_market() with require_subout=False,
-- reduced to candidate identity + wt. Substitute {UEI} and the four dial values
-- (defaults: sig_rank=5, sig_share=0.05, min_lane_hits=2, market_prime_floor=1e7).
--
-- Why the fo / sub_out LEFT JOINs from the engine are omitted here: under gate
-- OFF, subout_where is '' and nothing in the SELECT/WHERE references so.* or
-- fo.*, so those LEFT JOINs cannot change the row set. The engine keeps them only
-- to decorate each row with evidence. This query returns the identical candidate
-- set the engine fetches (confirmed row-exact in 02_reconstruct_total.py).
--
-- COUNT(*) over this = c_raw (pre Python family/JV trim). The engine's
-- market.total is this set after: exclude target family, exclude JV, collapse
-- corporate families to one row (see 02_reconstruct_total.py for the exact trim).
WITH seeds AS (
  SELECT DISTINCT prime_uei AS uei FROM gtm_prime_sub_pairs_by_sub
  WHERE sub_uei = '{UEI}' AND edge_dollars_5y > 0 AND prime_uei <> '{UEI}'),
seed_sig AS (
  SELECT s.code_type, s.code, COUNT(DISTINCT s.uei) AS seed_ct
  FROM gtm_prime_code_signature s JOIN seeds ON seeds.uei = s.uei
  WHERE s.rank_lifetime <= 5 AND s.share_lifetime >= 0.05
  GROUP BY 1, 2),
lane_freq AS (
  SELECT s.code_type, s.code, COUNT(DISTINCT s.uei) AS n_with_lane
  FROM gtm_prime_code_signature s
  JOIN seed_sig g ON g.code_type = s.code_type AND g.code = s.code
  WHERE s.rank_lifetime <= 5 AND s.share_lifetime >= 0.05
  GROUP BY 1, 2),
cand AS (
  SELECT s.uei, COUNT(*) AS lane_hits,
         SUM(g.seed_ct * s.share_lifetime / LN(1 + f.n_with_lane)) AS wt
  FROM gtm_prime_code_signature s
  JOIN seed_sig g ON g.code_type = s.code_type AND g.code = s.code
  JOIN lane_freq f ON f.code_type = s.code_type AND f.code = s.code
  WHERE s.rank_lifetime <= 5 AND s.share_lifetime >= 0.05
    AND s.uei <> '{UEI}' AND s.uei NOT IN (SELECT uei FROM seeds)
  GROUP BY 1 HAVING COUNT(*) >= 2)
SELECT c.uei, c.wt, c.lane_hits, b.prime_obl_60mo, b.top_naics
FROM cand c JOIN gtm_entity_behavior_rollup b USING(uei)
WHERE b.prime_obl_60mo >= 1e7
ORDER BY c.wt DESC, c.uei;
