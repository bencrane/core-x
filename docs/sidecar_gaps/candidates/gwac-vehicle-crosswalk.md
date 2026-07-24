# gwac-vehicle-crosswalk

**Status:** `parked` — needs NEW external reference data a rebuild cannot synthesize

## Capability

PIID-prefix → named GWAC program crosswalk (`gwac_vehicle_reference`: program, owner,
ceiling, ordering period) + a `vehicle_program` label on the order grain, resolving the GSA
GWAC family (all `A`+`047`) that the coarse heuristic cannot split.

## Evidence trail

- 2026-07-20 — [processed/SIDECAR_GAP_REPORT_2026-07-20-gwac-vehicle-crosswalk.md](../processed/SIDECAR_GAP_REPORT_2026-07-20-gwac-vehicle-crosswalk.md)
  disposition: PARK (structural); rank HIGH; "kept as active demand; no build fired."
  Routing note shipped the coarse `(idv_type_code, awarding_agency)` heuristic
  (MAS=C+047, SEWP=A+080, NITAAC=A+075).

## Proposed shape

Small curated Lance reference dataset (piid_prefix, program_name, owner, ceiling,
ordering_period) — an ingest/curation task; then the `vehicle_program` CASE label "rides
the `parent_window` build of award_state for free." Delta: negligible.

## Adjacency candidates

Vehicle on-ramp dates for expiring-vehicle plays once the reference exists.

## Notes

Blocked on sourcing the reference data (GSA program lists), not on build capacity.
