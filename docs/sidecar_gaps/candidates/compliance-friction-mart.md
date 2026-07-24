# compliance-friction-mart

**Status:** `parked` — application-layer scoring; all inputs now warm, buildable on demand

## Capability

`gtm_compliance_friction` uei-grain composed mart: n_active_primes, first_dod_touch_date,
DoD-obligation ramp, requires_cmmc/clearance, is_commercial_software, domain, POC, friction
score — the SecurityPal-class target list as one read.

## Evidence trail

- 2026-07-20 — [processed/SIDECAR_GAP_REPORT_2026-07-20-compliance-friction-securitypal.md](../processed/SIDECAR_GAP_REPORT_2026-07-20-compliance-friction-securitypal.md)
  disposition: PROMOTE (partial) — `us_software_companies` shipped warm + routing fix;
  the composed mart PARKED ("application-layer w/ opinionated scoring; all inputs now warm
  post-this-build"). Second-order lever also parked: scale `govcon_scope_vectors`
  extraction (`requires_cmmc` at 17% coverage — correctly-upstream).

## Proposed shape

Entity-grain composed mart over already-warm inputs; sub-1M rows. Delta: negligible.
The scoring formula is the real gate — freeze it before baking.

## Adjacency candidates

`requires_clearance`/`fedramp` siblings from scope vectors once extraction coverage rises.

## Notes

Same anti-pattern class as [staffing-absorption-mart.md](staffing-absorption-mart.md):
don't materialize a moving formula.
