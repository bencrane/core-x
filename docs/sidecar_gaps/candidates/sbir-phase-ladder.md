# sbir-phase-ladder

**Status:** `parked` — feasibility CONFIRMED, build spec ready; deferred to its own cycle

## Capability

SBIR/STTR phase ladder: `research`/`sbir_phase` label on `txn_events_combo` + a
`gtm_sbir_phase_ladder` entity mart (last_phase2_date, first_phase3_date,
first_nonsbir_prime_date, crossover_flag) — "who crossed from SBIR to real primes."

## Evidence trail

- 2026-07-20 — [processed/SIDECAR_GAP_REPORT_2026-07-20-sbir-phase3-crossover.md](../processed/SIDECAR_GAP_REPORT_2026-07-20-sbir-phase3-crossover.md)
  disposition: PARK (structural), rank HIGH — question "not answerable at any cost" today;
  parked only because it touches the 108M-row combo fact and "two clean thoughts beat one
  risky combined build." Spec written: extend `_COMBO_SRC_COLS`/`_COMBO_FACT_SQL` with a
  CASE off `research`, plus the uei-grain ladder mart.

## Proposed shape

Column on the combo fact (rides its rebuild) + small entity mart. Delta: ~0.5 GiB
(combo-fact column across copies) + negligible mart.

## Adjacency candidates

Agency-of-first-Phase-3 attribution columns on the ladder mart.

## Notes

Ready-to-execute: highest-leverage parked candidate whenever a rebuild already touches
`txn_events_combo`.
