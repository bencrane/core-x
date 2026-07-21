# Sidecar Gap Report — named-GWAC / vehicle identity crosswalk

- **Date:** 2026-07-20
- **Artifact stamp:** `query-sidecar/query_sidecar_20260720T025249Z.duckdb` (106 tables, built 2026-07-20T02:52:49Z)
- **Topic:** Resolving task/delivery orders to NAMED contract vehicles (GSA MAS, NASA SEWP, CIO-SP3, Alliant, OASIS, VETS, 8(a) STARS, Polaris) for GWAC-activation tracking

---

## Gap entry

1. **Intent** — Count software vendors who secured their FIRST active delivery order under a
   major GWAC in the last 12 months, broken down by which named vehicle they are penetrating.

2. **Why not the sidecar** — **missing table (vehicle reference crosswalk).** The sidecar carries
   parent-IDV *keys* (`usaspending_fpds_prime_award_state.parent_award_id_piid`,
   `parent_idv_type_code`, `parent_awarding_agency_code`, `parent_award_key_resolved`) and a
   dollars-only `gtm_prime_vehicle_lanes` keyed on `parent_piid` — but NOTHING maps a parent PIID
   (or PIID prefix) to a named GWAC program. A `(idv_type_code, awarding_agency)` heuristic
   resolves the 1:1-to-agency vehicles cleanly (GSA MAS = FSS `C`+GSA `047`; NASA SEWP = GWAC
   `A`+NASA `080`, validated as all `NNG15S*`; NITAAC = `A`+HHS `075`) — but every GSA GWAC
   collapses into one indistinguishable `A`+`047` signature. PIID prefixes carry the identity
   (`47QTCK18*`=Alliant 2, `47QTCB22*`=Polaris, `47QTCH18*`=VETS/STARS) yet there is no
   prefix→program crosswalk to decode them. 21 newly-activated vendors landed in an
   undifferentiated "GSA_GWAC_family" bucket that cannot be split into Alliant / OASIS / VETS /
   STARS / Polaris.

3. **What I ran instead** — sidecar for the counts at the coarse `(idv_type, agency)` grain, then
   a hand-authored CASE bucket + manual PIID-prefix inspection to confirm which programs hide
   inside `A`+`047`. Vendor→software classification via the `us_software_companies` Lance
   (domain match) + `gtm_sam_entities` UEI→domain bridge — a cross-system join, not warm/native.

4. **Cost** — order aggregates 4–7 s each (topology filter is off the `current_end_date` sort
   key → near-full scan of 82.9M rows). The named-vehicle breakdown was **not answerable** below
   the agency×type grain at any cost — hard structural miss for the GSA GWAC family.

5. **Recurrence** — **recurring.** "Who is newly penetrating vehicle X / which vehicles is firm Y
   on" is a core GTM motion (vehicle-access is a primeability gate). Will be re-asked per vehicle,
   per vendor, per window.

---

## Footer — rank (recurrence × cost)

**HIGH** (recurring GTM motion × un-answerable below agency×type). Demand: a **GWAC/vehicle
reference crosswalk** — parent IDV PIID (and PIID-prefix) → program name / owner / ceiling /
ordering-period — plus a derived `vehicle_program` label folded onto the order grain
(`usaspending_fpds_prime_award_state` vehicle_orders / `gtm_position_orders`). Adjacent: a warm
`us_software_companies` mart + `uei↔domain` crosswalk (already flagged in the
2026-07-20-sbir/subaward reports) so vendor commercial-footprint linking stops being a
three-system hand-join. Not a bespoke new sidecar service — a reference dataset + label column on
existing marts. Report demand only — disposition set by the `sidecar-gaps` promotion cycle.

---

## Disposition (2026-07-20 build cycle · artifact `query_sidecar_20260721T020734Z.duckdb`, 107 tables)

**Verdict: PARK (structural — needs NEW reference data a rebuild cannot synthesize).**

The parent-IDV keys exist on `usaspending_fpds_prime_award_state` (`parent_award_id_piid`,
`parent_idv_type_code`, `parent_awarding_agency_code`, verified). But the missing asset — a
PIID-prefix → named-GWAC-program crosswalk (`47QTCK18*`→Alliant 2, `NNG15S*`→SEWP V, etc.) — is
**external curated reference data that is not anywhere in the R2 plane**; a sidecar rebuild only
re-projects existing Lance, so it cannot manufacture this mapping. Not promotable until the
crosswalk is sourced/authored as a Lance reference dataset.

- The vendor-footprint adjacency this report also cited is now **resolved** by the sibling
  compliance-friction disposition (`us_software_companies` shipped warm this cycle).
- The coarse `(idv_type_code, awarding_agency)` heuristic (GSA MAS = C+047, SEWP = A+080,
  NITAAC = A+075) is already query-expressible with **no build** — usable today for the marquee
  vehicles; only the GSA GWAC family (all A+047) stays unresolvable without the crosswalk.

**Next step (blocked on data, not compute):** land a `gwac_vehicle_reference` Lance dataset
(piid_prefix, program_name, owner, ceiling, ordering_period), THEN a `vehicle_program` CASE label
rides the `parent_window` build of award_state for free. Kept as active demand; no build fired.
