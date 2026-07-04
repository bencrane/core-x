# FPDS Ontology Doctrine — constitution, invariant registry, compliance ledger

**Status:** binding for every derived column in the core-x FPDS/USAspending plane.
**Committed:** 2026-07-04, closing the Layer 0→2 ontology rebuild (PRs #975 dec_code_domain_ref · #978 L2 corrections · #986 doc sweep; production rebuild live-verified).
**Why this exists:** two shipped defects shared one root cause — derived taxonomies that reused or contradicted government lexicography (`action_type='Y'` guessed as `nonstandard` when the DEC documents `Y = "ADD SUBCONTRACT PLAN"`; `award_kind` values `definitive`/`order` colliding with government award-type descriptions they didn't match). This doc makes the cure durable.

---

## 1. The constitution — three clauses, every derived column

1. **Disjoint namespace.** A derived value may not reuse any government domain's value *or its description*. If a word appears in a DEC `domain_values` block anywhere, it is government property. (Killed: `definitive`, `order`, `nonstandard`.)
2. **Name ≤ computation.** A derived column asserts only what its derivation evaluates. No importing a richer semantic onto a structural test — a parent-presence check may be called *topology*, never *market access*. (Killed: `open_market`/`closed_pool` as names for a parent-IDV test.)
3. **No fuzzy shadows.** Where the government already models a concept precisely (`fair_opportunity_limited_sources`, `multiple_or_single_award_idv`, `extent_competed`, `parent_award`'s `direct`/`rollup` measures), do not synthesize a lossy near-duplicate. Surface the real field, or build an explicit composite that *references* it.

**Enforcement:** clause 1 is checkable — the authoritative value list is `dec_code_domain_ref` (Layer 0). A proposed derived value that matches any `(code, description)` row is a violation. Clauses 2–3 are review discipline; every new derived column ships with a compliance row in §5.

## 2. The layer model

| layer | asset | role |
|---|---|---|
| **0 — bedrock** | `s3://data-sink/active/dec_code_domain_ref/` (625 rows; see `DEC_CODE_DOMAIN_REF.md`) | normalized `(db_element, sub_domain, code) → verbatim description`. Every code resolves here; no derivation ever guesses a letter. |
| **1 — invariants** | §3 registry (this doc) | the whitepaper/DEC *structural rules* the enumerations can't express — mutual exclusivity, absence semantics, derivation branches. Constrain how Layer 2 columns are shaped. |
| **2 — derived** | L2 satellite columns (§5) | validate codes against Layer 0; derive structure per Layer 1; named per §1. |

Government codes are **namespace-scoped**: bare `A` = "BPA Call" (`contract_award_type`) / "GWAC" (`idv_type`) / "Fixed Price Redetermination" (`type_of_contract_pricing`) / "Additional Work" (`action_type`·Contracts) / "New" (`action_type`·Assistance). `(domain, code)` is data; a bare code is not. The `action_type` element is additionally **dual-named across communities** — "type of action" (assistance) vs "reason for modification" (procurement) — one DEC element, two sub-domains, same letters, different meanings.

## 3. Layer 1 invariant registry — load-bearing (certified against source)

Source: https://fedspendingtransparency.github.io/whitepapers/types/ (whitepaper) and the DEC (`usaspending_data_dictionary`, DAIMS Data Element Crosswalk). Each verified verbatim; "constrains" names the derived column/gate that depends on the rule being true.

| id | rule | verbatim anchor | constrains |
|---|---|---|---|
| **INV-FEED-XOR** | a procurement record carries an IDV-Type value **XOR** an Award-Type value | *"Each procurement action is identified with an Indefinite Delivery Vehicle (IDV) Type value or an Award Type value; they are mutually exclusive."* | `award_topology`: the `vehicle` branch (idv_type presence test) is sound only because no record carries both |
| **INV-TYPE-ELEMENT-SPLIT** | Award Type and IDV Type are distinct data elements / columns | *"They are collected as separate data elements in FPDS using the codes and descriptions shown below"* | `award_topology` reads `idv_type_code` and `award_type_code` as separate columns |
| **INV-ORDER-UNDER-IDV** | an order against an IDV is an *award-side* record (`award_type='C'`), never IDV-side | *"C = Delivery Order – delivery order or task order under an Indefinite Delivery Vehicle"* | `award_topology` `vehicle_order` branch (parent present ∧ idv_type absent); precedence of the vehicle test |
| **INV-PARENTPIID-PROCUREMENT** | `parent_award_id_piid` applies to procurement actions only | DEC: *"The identifier of the procurement award under which the specific award is issued… currently applies to procurement actions only."* | `vehicle_order` branch + parent resolution + vehicle rollup |
| **INV-MOD-ABSENCE** | an **absent** Reason-for-Modification code = a new/base procurement award, not a mod | *"Note, the absence of a 'Reason for Modification' code indicates that the action is a new procurement award."* | `mod_delta`'s modification gate (`WHERE action_type_code IS NOT NULL AND <> ''`); entity-dim `new_award_observation` change class |
| **INV-BAO-VALUE-BRANCH** | Base-And-All-Options additionally embeds estimated potential-order value **for IDVs** | *"For IDVs the value is the… total contract value including all options (if any) AND the estimated value of all potential orders."* | `potential_ceiling`: `vehicle` rows take `base_and_all_options_value` directly |
| **INV-IDV-LASTORDER-SCOPE** | `ordering_period_end_date` (last date to order) is an IDV-scope element | *"the date on which… no additional orders referring to it may be placed"* | `current_end_date`: `vehicle` rows use `ord_end`, others `pop_cur_end` |

**Advisory (verified, constrain nothing computed):** INV-TYPEOFIDC-SCOPE (`type_of_idc` sub-classifies the IDC family only), INV-MSIDV-SCOPE (`multiple_or_single_award_idv` is IDV-scoped; BPA bases don't inherit from the referenced FSS), INV-MAJORPROG-IDV-MEANING (IDV `major_program` may be a GWAC name), INV-ACTIONTYPE-COMMUNITY-SPLIT (the "new" value is assistance-only), INV-ACTIONTYPE-DESC-PAIR (`…DescriptionTag` is a paired serialization, never independent).

**Coverage caveat:** the advisory tail came from a rate-limit-degraded verification run; the load-bearing set above was independently re-certified against the source. Treat the advisory list as verified-but-possibly-incomplete.

## 4. Known cross-dataset crack — the 30× IDV scope gap

| dataset | "vehicle" population | basis |
|---|--:|---|
| `usaspending/award_search` `category='idv'` | **32,341** | IDVs present as award records in the (FPDS-scoped) bulk |
| `usaspending/parent_award` | **987,705** | all historical IDV parents |
| `usaspending_fpds_prime_award_state` `award_topology='vehicle'` | **990,041** | every spine award key with `idv_type_code` present |

**Rule: never reconcile the vehicle population to `award_search`.** The FPDS spine and `parent_award` agree (99.7%); `award_search` under-represents vehicles ~30×. Reconciliation target for the vehicle universe is `parent_award` / the spine, full stop. Corollary: `parent_award`'s `direct_*`/`rollup_*` measures are the canonical vocabulary for subtree aggregation — L2 adopted it (`rollup_obligated`, `rollup_order_count`).

## 5. Derived-column compliance ledger

| column (table) | values | clause 1 | clause 2 | clause 3 | note |
|---|---|---|---|---|---|
| `award_topology` (both) | `vehicle` / `vehicle_order` / `standalone` | ✓ disjoint | ✓ pure topology (idv_type / parent presence) | ✓ gov semantics stay in `award_type_code` | replaced `award_kind` (#978) |
| `action_type_klass` (delta) | option_exercise / scope_change / funding_only / termination / identity_boundary / admin / unclassified | ✓ | ✓ advisory grouping of `action_type_code`; codes resolve via Layer 0 | ✓ raw code carried alongside | `Y`→`admin` (#978); `nonstandard` retired |
| `award_pool` (delta) | `parent` / `child` | ✓ | ✓ (= topology projection) | ✓ | calibration axis (Cycle 2) |
| `parent_match_flag` (state) | `self` / `resolved` / `dangling` | ✓ | ✓ names the join outcome exactly | ✓ | construct-and-validate resolution |
| `is_scope_increase`, `is_termination_event`, `identity_changed`, `is_terminated`, `is_expired_no_followon`, `potential_ceiling_is_fallback`, `has_nested_vehicle` | booleans | ✓ | ✓ name = predicate computed | ✓ | — |
| `rollup_obligated`, `rollup_order_count` (state) | measures | ✓ | ✓ | ✓ adopts `parent_award` canonical vocabulary | renamed from `idv_child_*` (#978) |

**Retired for violations:** `award_kind` (`definitive`/`order`/`idv` — clause 1), klass value `nonstandard` for `Y` (clause 2 — asserted "undocumented" against a documented code).

## 6. Deferred / open

1. **`pop_county_fips` + `primary_place_of_performance_state_code` onto `prime_award_state`** — known x+1 miss of the 2026-07-04 rebuild (enrichment, not ontology). Batch into the next state-table rebuild; collapses the wage-locality join hop.
2. **`fpds_action_type_ref` dataset removal** — loader deprecated (do-not-run banner); live dataset frozen. Remove after the `dec_code_domain_ref` reconciliation gate is repointed to a static expectation.
3. **Advisory-invariant exhaustive pass** — optional; load-bearing set is certified.
4. **Layer 1 additions** must land in §3 with a verbatim anchor — no uncited invariants.
