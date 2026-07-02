# FPDS Canonical Spine — Adversarial Review of Omissions & Proposed Adds

_Adversarial critique (Opus 4.8) of the curation in `FPDS_CANONICAL_FIELD_DICTIONARY.md`. Challenges both directions: the ~121 proposed adds (over-proposing / redundancy / spine-bloat) and the ~183 dismissed `🔒 internal` columns (wrongly left out). Generated 2026-07-02._

> **Correction to the review's "#0" finding.** The reviewer flagged a `subcontracting_plan` doc-vs-code discrepancy (dictionary says ✅ IN; live `COLUMN_SPEC` on `origin/main` has 87 entries and no `subcontract`). This is **not a regression** — `subcontracting_plan` lives on the **unmerged PR #873** (`feat/fpds-subcontracting-plan`, 6 occurrences); the manifest/dictionary were generated from that branch, and the review read the `origin/main` base. **Resolution: merge #873 + rebuild.** The reviewer's priority — "make sure `subcontracting_plan` actually lands before adding anything" — stands.

---

## Bottom line

**The 121-column proposal is over-proposed by ~2.4×. The defensible high-value add-set is ~50 columns + ~6 rescues.** The redundancy the curation missed: the socioeconomic axis is already a first-class, UEI-keyed, *decoded* serving surface, and `business_types` (the point-in-time self-cert code string) is already on the spine.

| Decision | Δ | Rationale |
|---|---|---|
| **Final recommended add-set** | **~50** (not 121) | Real transaction-grain facts with a GTM/analytics query pattern; **codes not `_desc` twins** |
| Cut: socioeconomic flag booleans | −35 of 39 | Redundant with `govcon_subawardee_designations` + DSBS crosswalk (UEI-keyed, already decoded) and `business_types` (already IN); keep only WOSB / SDVOSB / HUBZone / 8(a) |
| Cut: `_desc` description twins | −~20 | Static code→label legend; **violates the spine's own code-only convention** |
| Cut: geography name / FIPS / population derivations | −~18 | Derivable from codes on-spine; populations are award-time-frozen SCD errors; BULK-only NULL cliff |
| Cut: funding abbreviation / raw / id twins | −~6 | Pre-normalization dupes of columns already carried |
| **Rescues** from `🔒`/`⬜` | +~6 | `total_obligated_amount`, `base_exercised_options_val`, `number_of_actions`, `transaction_number`, `solicitation_date` (+consider `award_date_signed`) |

---

## 1. The redundancy teardown of the ~121 proposed adds

### 1a. Socioeconomic / business-type flags — REJECT ~35 of 39 (largest single cut)
Every one is redundant with UEI-keyed SAM data the platform already serves, and the spine already carries indexed `recipient_uei`:
- `sam_entity_master` emits `business_types`, `naics_codes[]`, `psc_codes[]`, `primary_naics` per UEI (`pipelines/sam_gov/sam_entity_master.py:147-150`).
- `govcon_subawardee_designations` **already decodes the 12 canonical socioeconomic designations** verbatim, built as a "zero-join socioeconomic" surface (`pipelines/serving/materialize_subawardee_designations.py:7-9, 50-66`): SDVOSB, VOSB, WOSB/EDWOSB/JV-WOSB, HUBZone, 8(a), SDB, self-cert SDB, minority.
- `crosswalk_dsbs_sam.py` carries live SBA-administered cert booleans: `active_8a/hz/wosb/edwosb/sdvosb/vosb`.
- The awards map already filters by socioeconomic **set-aside** (`apps/catalyst_api/src/map_decoders.py:435, 478-484`).
- **Point-in-time doesn't save them:** the FPDS bulk flags are documented as "derived from the SAM data element 'Business Types'" — and `business_types` (that code string) is **already IN** as enrichment. 39 near-duplicate booleans re-encode it at 108M-row cost.
- **Keep at most ~4 discrete point-in-time flags:** `women_owned_small_business`, `service_disabled_veteran_o[wned]`, `historically_underutilized` (HUBZone), `c8a_program_participant` (8(a)).

### 1b. Geography population / name / FIPS — REJECT ~18 of ~27
- `pop_state_name/fips`, `pop_county_name/code`, `pop_country_name`, `recipient_location_*_name` are 1:1 lookups from codes already carried → reference dim, not 108M-row replication.
- **`*_population` columns — reject all.** Census population is a slowly-changing geography attribute; freezing it per-transaction at award time is an SCD error with no query pattern.
- **Keep net-new resolution (~5):** `place_of_performance_code`, `place_of_performance_scope`, `place_of_performance_zip4a` (true zip4 — the spine's `primary_place_of_performance_zip_4` is documented **lossy zip5-only**, `usaspending_fpds_canonical.py:~227`), `place_of_performance_forei`, `pop_city_name`.

### 1c. The `_desc` / description twins — REJECT the redundant half (~20)
The proposal adds **both** code and `_desc` for many fields (`cost_or_pricing_data` + `_desc`, `consolidated_contract` + `_desc`, `multi_year_contract` + `_desc`, `type_set_aside_description`, `extent_compete_description`, `idv_type_description`, …). This **violates the spine's own convention** — `extent_competed` is IN but `extent_compete_description` is ⬜ out; `type_of_set_aside_code` IN but `type_set_aside_description` out. The code→label expansion is a static FPDS legend (published in this dictionary's domain-values column). **Ship the code; resolve the label in a reference dim / the app.**

### 1d. Funding abbreviation / raw / id twins — REJECT ~6
`funding_toptier_agency_abbreviation/id`, `*_name_raw`, `parent_recipient_name_raw`, `parent_recipient_unique_id` are pre-normalization dupes of columns already carried. **Keep** `funding_office_code/name`, `funding_sub_tier_agency_co`, `funding_subtier_agency_name` (office-level granularity is a real axis).

### 1e. The tight core worth adding — KEEP ~50
Real transaction-grain facts, not in SAM/bridges, with GTM/analytics patterns:
- **Competition / vehicle:** `solicitation_procedures`, `other_than_full_and_open_c`, `fair_opportunity_limited_s`, `number_of_offers_received`, `commercial_item_acquisitio`, `multiple_or_single_award_i`, `referenced_idv_agency_iden`, `referenced_idv_type`, `referenced_idv_modificatio`, `ordering_period_end_date`, `major_program`, `program_acronym`.
- **Contract characteristics (codes):** `contract_bundling`, `consolidated_contract`, `performance_based_service`, `undefinitized_action`, `multi_year_contract`, `contract_financing`, `cost_or_pricing_data`, `dod_claimant_program_code`, `inherently_government_func`, `purchase_card_as_payment_m`, `clinger_cohen_act_planning`, `national_interest_action`, `domestic_or_foreign_entity`, `price_evaluation_adjustmen`.
- **Geography (net-new):** `place_of_performance_code/scope/zip4a/forei`, `pop_city_name`.
- **Funding granularity:** `funding_office_code/name`, `funding_sub_tier_agency_co`, `funding_subtier_agency_name`.
- **Socioeconomic (thin):** `women_owned_small_business`, `service_disabled_veteran_o`, `historically_underutilized`, `c8a_program_participant`.

---

## 2. Rescues from the ~183 dismissed `🔒 internal`
Most dismissals are correct (ETL timestamps, surrogate ids, hashes). These are **wrongly dismissed**:
- **`total_obligated_amount`** (⬜ out) — FPDS system-generated sum of all action obligations for a PIID (award-to-date total). Cheapest award-level total without a 108M-row self-join. (Grain caveat §4.)
- **`base_exercised_options_val`** (⬜ out) — base + *exercised* options value; distinct from `base_and_all_options_value` / `current_total_value_of_award` (both IN). Real recompete/renewal signal.
- **`number_of_actions`** (⬜ out) — count of actions in a modification; volume/dedup signal (feeds "frequency by NAICS+PSC").
- **`transaction_number`** (⬜ out) — documented tie-breaker for legal unique transactions sharing a key. **PK-integrity-relevant** given the pipeline's fail-closed PK gate.
- **`solicitation_date`** (⬜ out, date32) — procurement-cycle timing; pairs with `solicitation_identifier` (IN).
- Consider: `award_date_signed` / `award_certified_date` (award-lifecycle dates, not on spine).

---

## 3. True gaps the curation missed
1. **`subcontracting_plan`** — named GTM filter; on unmerged PR #873, not yet on `origin/main`/built. **Land it first.** (See correction note above.)
2. **Congressional-district as a *serving* filter** — the spine carries `pop_congressional_code` + `recipient_location_congressional_code` (enrichment), but `map_decoders.py` exposes **no CD filter**, so the flagship "primes performing in district X" audience can't be built at the serving layer. **Decoder-side fix**, not a spine gap.
3. **No award-level obligation total precomputed** — "by-$" audience math needs an aggregate the spine doesn't precompute (see `total_obligated_amount` rescue).

---

## 4. Grain / reconciliation hazards
- **Award-level $ repeated per transaction:** `total_obligated_amount`, `current_total_value_of_award`, `base_and_all_options_value`, `award_amount` carry the *same* award-level value on every transaction row for a PIID → **SUM() at transaction grain double-counts catastrophically.** Any rescued total must be documented award-grain-repeated, never summed at transaction grain.
- **`*_population` reconcile poorly:** BULK carries them; FRESH/MONTHLY (all-VARCHAR) largely do not → BULK-only NULLs correlated with `canonical_source` (a coverage cliff on ~2M FRESH + ~3M MONTHLY keys).
- **Socioeconomic booleans reconcile poorly:** FRESH carries ~20 flag names vs BULK's 39 → source-correlated NULL patterns. `business_types` (already IN) reconciles cleanly across all three.

---

## Evidence (key files)
- `pipelines/usaspending/usaspending_fpds_canonical.py` — live `COLUMN_SPEC`; `BTREE_COLS` includes `recipient_uei`; `primary_place_of_performance_zip_4` documented lossy zip5-only (~line 227)
- `docs/reference/FPDS_CANONICAL_FIELD_DICTIONARY.md` — the curation under review
- `pipelines/sam_gov/sam_entity_master.py:147-150` — `business_types`, `naics_codes[]`, `psc_codes[]` per UEI
- `pipelines/serving/materialize_subawardee_designations.py:7-9, 50-66` — 12 socioeconomic designations decoded, "zero-join" verbatim vocabulary
- `pipelines/sba_dsbs/crosswalk_dsbs_sam.py` — `active_8a/hz/wosb/edwosb/sdvosb/vosb`
- `apps/catalyst_api/src/map_decoders.py:435, 478-484` — awards map set-aside filters; **no congressional-district filter**
- `pipelines/reference/materialize_naics_psc_deliverable.py` + `materialize_naics_psc_labor_profile.py` — `(naics, psc)` → work_type/deliverable/labor, joinable on codes already on spine
