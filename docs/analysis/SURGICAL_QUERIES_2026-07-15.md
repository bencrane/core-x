# Surgical Queries — 2026-07-15 session (durable, re-runnable recipes)

Three ad-hoc GTM screens run at session start, preserved here as executable recipes so any
agent can re-run or augment them. All run against the **query-sidecar** (no Lance scan):

```bash
TOKEN=$(doppler secrets get QUERY_SIDECAR_TOKEN -p core-x -c prd --plain)
curl -s -X POST https://query-sidecar-api.onrender.com/api/v1/sql \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"sql": "<ONE statement>", "limit": 50000}'
```

Dates are relative to the run date — adjust the literals. Result CSVs from the original
runs sit in `~/Desktop/hq/*_2026-07-15.csv`. Afterlife of each shape is noted — prefer the
typed canonical query where one exists.

---

## 1. CAGE step-growth screen (TTM ≥ 4× prior 24 months)

**Superseded for the common case** by canonical Q4 (`<industry> companies whose prime
obligations grew {N}x in the last 12 months vs the prior 24 months`, edge
`mode:"growth"`). Re-run this SQL form when you need NAICS-set control (Q4 industry ≠
arbitrary NAICS list), CAGE codes, or a non-standard multiplier/window.

```sql
WITH agg AS (
  SELECT uei,
         SUM(CASE WHEN action_date >  DATE '2025-07-15' THEN obligation ELSE 0 END) AS ttm_obl,
         SUM(CASE WHEN action_date <= DATE '2025-07-15' THEN obligation ELSE 0 END) AS prior24_obl
  FROM gtm_txn_events_slim
  WHERE action_date > DATE '2023-07-15' AND action_date <= DATE '2026-07-15'
    AND (naics_code LIKE '23%' OR naics_code = '561210' OR naics_code = '561612')
  GROUP BY uei
), hits AS (
  SELECT * FROM agg WHERE prior24_obl > 0 AND ttm_obl >= 4 * prior24_obl
)
SELECT e.legal_business_name, e.cage_code, h.uei,
       ROUND(h.ttm_obl,0)  AS ttm_obligations,
       ROUND(h.prior24_obl,0) AS prior_24mo_obligations,
       ROUND(h.ttm_obl - h.prior24_obl,0) AS delta_obligations
FROM hits h LEFT JOIN gtm_sam_entities e USING (uei)
WHERE e.cage_code IS NOT NULL
ORDER BY delta_obligations DESC
```
Semantics: "300% higher" was ruled = **4×** (this ambiguity is why the canonical language
bans percent talk — `grew Nx` only). Net obligations (deobligations included); prior > 0
excludes zero-history entrants. Original run: 427 rows, 5.4 s.

## 2. Sub→prime graduates (first prime in 18mo, ≥3yr strictly-sub history)

**No canonical shape exists** (sub-only companies side-parked 2026-07-15) — this recipe is
the only way to run it. ~98 s (full-history MIN per uei).

```sql
WITH prime AS (
  SELECT uei, MIN(action_date) AS first_prime_date, SUM(obligation) AS total_prime_obl,
         COUNT(DISTINCT award_key) AS prime_award_ct
  FROM gtm_txn_events_slim GROUP BY uei
), new_primes AS (
  SELECT * FROM prime WHERE first_prime_date > DATE '2025-01-15'
), subs AS (
  SELECT subawardee_uei AS uei, MIN(subaward_action_date) AS first_sub_date,
         MAX(subaward_action_date) AS last_sub_date,
         SUM(subaward_amount_num) AS sub_amt_lifetime, COUNT(*) AS sub_ct
  FROM subaward_canonical_slim_by_sub GROUP BY subawardee_uei
), hits AS (
  SELECT p.uei, p.first_prime_date, p.total_prime_obl, p.prime_award_ct,
         s.first_sub_date, s.last_sub_date, s.sub_amt_lifetime, s.sub_ct
  FROM new_primes p JOIN subs s USING (uei)
  WHERE s.first_sub_date <= p.first_prime_date - INTERVAL 3 YEAR
    AND s.last_sub_date  <  p.first_prime_date
)
SELECT e.legal_business_name, e.cage_code, h.*
FROM hits h JOIN gtm_sam_entities e USING (uei)
WHERE e.cage_code IS NOT NULL
ORDER BY h.total_prime_obl DESC
```
Caveats: "3 years of history" = first-sub-to-first-prime span (a single old sub qualifies);
corporate-family UEI spins (e.g. Kiewit US Contractors) look like graduates; pre-UEI-backfill
subaward rows undercount. Original run: 230 rows.

## 3. Outsized 2026 award vs 2025 revenue (single award > 2× prior-year gross)

**Partially covered** by canonical Q2 `single awards … in the last N` — but the
ratio-to-prior-revenue comparison is NOT expressible in the language (candidate future
card: "won a single award ≥ Nx their prior-year revenue"). Use this recipe meanwhile.

```sql
WITH rev25 AS (
  SELECT uei, SUM(obligation) AS rev_2025 FROM gtm_txn_events_slim
  WHERE action_date >= DATE '2025-01-01' AND action_date < DATE '2026-01-01' GROUP BY uei
), awd26 AS (
  SELECT recipient_uei AS uei, contract_award_unique_key, award_id_piid,
         first_action_date, life_to_date_obligated AS award_obl, awarding_agency_code
  FROM usaspending_fpds_prime_award_state
  WHERE contract_award_unique_key LIKE 'CONT_AWD%'          -- task orders + definitive; IDV wrappers excluded
    AND first_action_date >= DATE '2026-01-01' AND life_to_date_obligated > 0
), hits AS (
  SELECT a.*, r.rev_2025 FROM awd26 a JOIN rev25 r USING (uei)
  WHERE r.rev_2025 > 0 AND a.award_obl > 2 * r.rev_2025
)
SELECT e.legal_business_name, e.cage_code, h.uei, h.award_id_piid, h.first_action_date,
       ROUND(h.award_obl,0) AS award_value, ROUND(h.rev_2025,0) AS rev_2025,
       ROUND(h.award_obl/h.rev_2025,1) AS ratio, v.name AS awarding_agency
FROM hits h
JOIN gtm_sam_entities e USING (uei)
LEFT JOIN agency_vocab v ON v.code = h.awarding_agency_code
LEFT JOIN gtm_entity_behavior_rollup b ON b.uei = h.uei
WHERE e.sam_is_active
  AND (list_contains(e.business_types,'LJ') OR list_contains(e.business_types,'2L')
       OR list_contains(e.business_types,'8H'))              -- LLC / corporate entity
  AND COALESCE(b.prime_obl_lifetime,0) < 2000000000          -- behemoth cut, per-UEI
ORDER BY award_value DESC
```
Notes: do NOT re-aggregate awards from the 108M txn table (OOMs the sidecar — that path
failed; the award-grain table is the correct source). Per-UEI behemoth cut passes large
subsidiaries (e.g. GardaWorld Federal) — drop manually if org-family scale is intended.
Original run: 587 rows, 5.5 s.
