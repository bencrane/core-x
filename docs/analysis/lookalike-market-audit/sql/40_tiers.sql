-- Tier membership, evaluated over the Tier0 representative UEIs (the gate-OFF
-- family-deduped set produced by 20_gate_off_candidate_universe.sql + the Python
-- trim). {REPS} = comma-quoted rep UEIs; {SHAPE_NAICS} = target work-shape NAICS
-- (sub-side lanes, obl_lifetime>=250k, top 12; see below).

-- Target work-shape NAICS (the recipient_code filter for Tier 2):
SELECT code FROM gtm_entity_code_lanes
WHERE uei = '{UEI}' AND side = 'sub' AND code_type = 'naics'
  AND obl_lifetime >= 250000
ORDER BY obl_lifetime DESC LIMIT 12;

-- Tier 1 (WRONG source — do not use for nesting): any 5y sub-out edge in the
-- prime_sub_pairs mart. This does NOT contain Tier 2 (different mart/window).
SELECT DISTINCT prime_uei FROM gtm_prime_sub_pairs
WHERE prime_uei IN ({REPS}) AND edge_dollars_5y > 0;

-- Tier 1 (CORRECT source — same cube as Tier 2): subs out to ANY recipient shape.
SELECT DISTINCT prime_awardee_uei FROM gtm_prime_subout_by_recipient_code
WHERE prime_awardee_uei IN ({REPS})
  AND recipient_code_source = 'awarded_prime_contracts_in_code'
  AND recipient_code_type = 'naics' AND context_code_type = 'naics'
  AND subaward_amt_total > 0;

-- Tier 2 (strongest signal): subs out to recipients whose OWN PRIME HISTORY sits
-- in the target's work-shape NAICS. Subset of the correct Tier 1 by construction.
SELECT DISTINCT prime_awardee_uei FROM gtm_prime_subout_by_recipient_code
WHERE prime_awardee_uei IN ({REPS})
  AND recipient_code_source = 'awarded_prime_contracts_in_code'
  AND recipient_code_type = 'naics' AND context_code_type = 'naics'
  AND recipient_code IN ({SHAPE_NAICS})
  AND subaward_amt_total > 0;
