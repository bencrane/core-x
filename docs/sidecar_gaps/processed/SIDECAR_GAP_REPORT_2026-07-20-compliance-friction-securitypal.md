# Sidecar Gap Report — compliance-friction segment mart (SecurityPal recon)

- **Date:** 2026-07-20
- **Artifact stamp:** `query-sidecar/query_sidecar_20260720T025249Z.duckdb` (106 tables)
- **Topic:** Isolating commercial software vendors under acute federal security-compliance friction
  (DFARS 252.204-7012 / NIST 800-171 / CMMC L2 / prime TPRM) for a commercial-client targeting motion.

---

## Gap entry

1. **Intent** — Find commercial software vendors "slamming into the compliance wall right now":
   multi-prime subcontractors (N concurrent supplier security questionnaires), civilian→DoD
   first-crossers (DFARS trigger), and scope-verified CMMC/clearance-gated subs — sized, and
   linked to commercial footprint (domain) + contactable POC.

2. **Why not the sidecar** — **cross-system hand-join + coverage-limited signal.** No single mart
   expresses "compliance friction." Answer required joining, in-session: `govcon_subawardee_profiles`
   (teaming-prime counts, extracted `requires_cmmc`/`requires_clearance`, POC) + `gtm_txn_events_slim`
   (first-DoD-touch) + `gtm_entity_behavior_rollup` (DoD $ ramp) + `gtm_sam_entities` (uei↔domain) +
   the `us_software_companies` Lance (commercial-software membership — NOT in the sidecar). Two hard
   limits: (a) uei→domain bridge resolves ~73%; (b) the `requires_cmmc` flag is extracted for only
   4,220/25,450 subs (17% scope coverage) → 55 total / 4 software = a floor, not prevalence.

3. **What I ran instead** — sidecar pulls of the three candidate sets, then Python classification
   against the commercial-software domain set + NAICS(5415/5132/511210/5182/5191), + crossover/POC
   slicing. Not warm/native.

4. **Cost** — 6–7 s per scan (first-DoD-touch and behavior joins are off-sort-key on 108M-row
   `gtm_txn_events_slim`). Repeated per segment. Feasible one-off; not productionizable as-is.

5. **Recurrence** — **recurring.** Compliance-friction targeting is a durable commercial-GTM
   product (re-run per client ICP, per window, per vehicle). High reuse.

---

## Footer — rank (recurrence × cost)

**HIGH.** Demand: a **compliance-friction entity mart** keyed on `uei`, joining the subaward graph +
`us_software_companies` + `gtm_sam_entities` + `gtm_entity_behavior_rollup` + `govcon_subawardee_profiles`,
and **calculating** per vendor: `n_active_primes` (concurrent-questionnaire load), `first_dod_touch_date`
(DFARS onset), `dod_obl_12mo/24mo_ramp`, `requires_cmmc`/`requires_clearance`, `is_commercial_software`,
`normalized_domain`, `poc_*`. Depends on the already-flagged promotions (warm `us_software_companies`
+ `uei↔domain` crosswalk). Second-order lever: **scale the scope-vector extraction** (currently 17% of
subs) to make `requires_cmmc` a reliable — not floor — signal. Report demand only; disposition via the
`sidecar-gaps` promotion cycle.

---

## Disposition (2026-07-20 build cycle · artifact `query_sidecar_20260721T020734Z.duckdb`, 107 tables)

**Verdict: PROMOTE (partial) + ROUTING FIX. The friction mart itself: PARKED (structural, application-layer).**

| Claim | Gate | Action |
|---|---|---|
| `us_software_companies` not warm (Lance-only) | **PROMOTE** — demand-evidenced (3 analyses hand-joined it), cheap (173k-row generic CTAS, exact parity) | **SHIPPED** as tier-D mart, sorted `domain`. |
| "need a `uei↔domain` crosswalk" | **ROUTING FIX** — the bridge was already warm: `gtm_sam_entities.normalized_domain` | No build. Guide §4 pattern (n) + catalog row now document the join explicitly. |
| `requires_cmmc` coverage-limited (17% scope extraction) | **Correctly-upstream** — depends on scaling the `govcon_scope_vectors` extraction, not a sidecar projection | Parked; noted as second-order lever. |
| `gtm_compliance_friction` entity mart (n_active_primes, first_dod_touch, dod_ramp, friction_score) | **PARK (structural)** — application-layer w/ opinionated scoring; all inputs now warm post-this-build | Buildable on demand next cycle; no speculative structural growth. |

**Adjacency sweep (executed before build):** `us_software_companies` shipped `SELECT *` → all 27 firmographic cols ride (industry/size/funding/country/specialties). Join-side (`gtm_sam_entities`: uei+normalized_domain+primary_naics) already warm — no crosswalk resort needed. Next-question sim (software ∩ DoD; software multi-prime subs; software GWAC entrants) all answerable post-build with no further rebuild.

**Measured (serving):** `gtm_sam_entities ⋈ us_software_companies` on `normalized_domain=domain` → **19,054** commercial-software federal entities in **162 ms** (before: sidecar pull + Lance domain-set load + Python intersect, multi-second cross-system). Mart parity exact (173,119 = 173,119). Build: ledger #41 success, 107 marts, 1.382B rows.
