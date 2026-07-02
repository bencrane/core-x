# Subaward Canonical Spine — Adversarial Review of Omissions & Proposed Adds

_Four-lens adversarial adjudication (Opus 4.8) of the draft `COLUMN_SPEC` for [`usaspending_subaward_canonical.py`](../../pipelines/usaspending/usaspending_subaward_canonical.py). Lenses: REDUNDANCY, GRAIN-HAZARD + source-NULL-cliff, SERVING-LOAD-BEARING, COMPLETENESS/PARSIMONY. All crosswalk bindings re-verified against the ACTUAL `subaward_search` (210) / `contract_subaward` (118) live schemas — zero MISSING. Generated 2026-07-02._

> **Shipped-spec reconciliation.** The adjudication below produced a tight column set that then took three engineering corrections before shipping as the final **91-column** spec (the pre-publish binding validator surfaced the first two):
> 1. The two per-source mod-frontiers were unified into ONE core column **`subaward_last_modified_date`** (BULK `broker_updated_at` ⊕ FRESH parsed SAM mtime) — it IS the 2-way argmax driver, so it must be a real projected column, not `prov` (the projection generator special-cases `prov`). The raw per-source frontiers are no longer carried separately.
> 2. **`subaward_sam_report_id`** ships FRESH-only enrich (its draft BULK leg `internal_id` is a different id space — a broker surrogate on the §3 internal-cut list; BULK's surrogate is `broker_subaward_id`, carried separately).
> 3. Added a synthesized single-column PK **`subaward_unique_key`** = `prime_award_unique_key|subaward_number` (BTREE point-lookup ergonomics; the fail-closed gate uses the TUPLE + a collision gate).

---

## Bottom line

**Final = 91 cols (key 7 · core 64 · enrich 18 · prov 2).** The §3 socioeconomic/name/FIPS/DUNS/raw/dead-column cuts were largely executed by omission; the spine correctly carries ONLY the raw `subawardee_business_types` / `prime_awardee_business_types` code strings while the 12 decoded socioeconomic booleans stay served UEI-keyed by `govcon_subawardee_designations`. **Verified prod build: `rows_out=1,315,680`, `pk_unique=true`, exactly the reconciliation-probe centerline.**

| Decision | Δ | Rationale |
|---|---|---|
| **Cut: prime-awardee HQ street/city/zip** | −3 | UEI-redundant with `sam_entity_master.physical_*`; live grep of all 10 §4 consumers + `pipelines/` = 0 reads. Kept `prime_awardee_state_code`/`country_code` as BITMAP filters |
| **Rescue: `broker_subaward_id`** | +1 | HARD serving-load-bearing omission — the BULK collapse `ORDER subaward_last_modified_date DESC, broker_subaward_id DESC` is undefined without the row surrogate. Mirrors FPDS `transaction_id` enrich BIGINT |
| **Rescue: `prime_award_disaster_emergency_fund_codes`** | +1 | FRESH-only 87.8%, decodable surge/contingency GTM signal, no on-spine twin, denser than the COVID/IIJA money cols it supersedes |
| Cut confirmed (recorded): `business_categories`, `sub_naics`, `award_piid_fain`, `prime_award_group`, FRESH funding-string dupes, all geo NAME/FIPS twins, DUNS, internal/ETL, dead <6% block | — | See the field-dictionary dead-column inventory |

---

## 1. The redundancy teardown

### 1a. Socioeconomic / business-type flags — the largest cut (executed by omission)
`business_categories` (BULK 100%) is a decoded socioeconomic category array — redundant with [`materialize_subawardee_designations.py`](../../pipelines/serving/materialize_subawardee_designations.py) (`govcon_subawardee_designations`, 1 row/`subawardee_uei`, 12 flags decoded from SAM: QF→SDVOSB, A6→8(a), XX→HUBZone…). `subawardee_uei` is on-spine. Zero consumers (`grep business_categories pipelines/` = 0). **The spine carries the RAW `subawardee_business_types` / `prime_awardee_business_types` code strings ONLY** — the analog of the FPDS `business_types` IN decision. Decoding on-spine would duplicate the UEI-keyed surface. Parse downstream.

### 1b. `sub_naics` — a denorm, not an independent code
BULK `sub_naics` (98.6%) is NOT a subaward-grain NAICS (no `Subaward Element` in the dictionary → rpt denorm copy of the prime NAICS). Proven by consumers: `materialize_sub_diversification.py` and `build_subawardee_capability_profiles.py` derive the subawardee NAICS as `mode(prime_award_naics_code)`. Redundant twin of `prime_award_naics_code` (on-spine). Cut.

### 1c. Geography name / FIPS / county — cut per §3
All `*_state_name`/`*_country_name`/`*_county_name`/`*_state_fips`/`*_county_fips` twins cut (codes carried). County is a derivable rollup, NOT in the §3 PoP keep-set (city/zip/district/state/country). The single exception — `subawardee_county_code` (BULK-only ~95%) — is rescued as the one entity-resolution county anchor; prime-side and sub-PoP county correctly cut. The asymmetry is defensible; do not expand.

### 1d. FRESH funding-string decompositions — cut
`prime_award_treasury_accounts_funding_this_award` (90.4%) and `prime_award_program_activities_funding_this_award` (87.3%) are redundant decompositions of the SAME award-funding data carried by the kept `federal_accounts` + `object_classes` strings; unlike `object_classes` (joins `materialize_psctool_maps`) they have no on-spine join surface and no serving consumer. Cut, recorded.

### 1e. DUNS / agency abbreviation / raw / internal / dead — cut
UEI supersedes all DUNS. Agency `*_abbreviation`/`_id`/`_raw` are pre-normalization dupes. `sub_federal/funding_agency` id+name are FSRS-submitter dupes of the prime hierarchy carried in full. `*_ts_vector`, `broker_created_at`, `ingested_at`, `internal_id`, `source_*`, surrogate ids are 🔒 internal. The 0% block (`cfda_*`/`grant_*`/`recovery_*`/`treasury_*`/`fain`/`dunsplus4`/`compensation_q*`/`place_of_perform_street|scope`) is dead in contract scope.

---

## 2. Rescues

- **`broker_subaward_id`** (BULK, int64, 100%) — **HARD.** Each source is collapsed `ORDER subaward_last_modified_date DESC, native_pk DESC`; without the BULK row surrogate the per-composite tie-break is undefined. The FPDS reference worker carries its analog `transaction_id` as enrich BIGINT. Added as `enrich` BIGINT, non-PK.
- **`prime_award_disaster_emergency_fund_codes`** (FRESH, 87.8%) — decodable emergency/disaster/wildfire PL codes; a distinct GTM lens (which subs rode disaster/emergency supplementals) with no on-spine twin, materially denser than the COVID/IIJA supplemental-money cols (1.8-13.9%) it supersedes. Added as `enrich` VARCHAR, BITMAP.

No other rescues warranted (`total_outlayed`, `treasury_accounts`, `program_activities`, COVID/IIJA supplementals — all prime-grain-repeated, redundant, or below-bar).

---

## 3. Contested calls (documented, not silently shipped)

- **`subaward_type`**: kept, BITMAP **dropped** — single-cardinality ('sub-contract') in contract-only scope; retained un-indexed for provenance + future grant-canonical UNION alignment.
- **`subaward_sam_report_id`**: kept (`enrich`, FRESH-only) for FRESH collapse-ORDER determinism + provenance. Caveated: report grain (~2.18 FRESH rows/composite, 94,276 dup ids), NOT a PK, NOT unique post-collapse, NEVER join or dedup on it. The fail-closed PK gate is the determinism guarantee.
- **`subawardee_county_code`**: kept as sole county anchor; source-correlated NULL cliff (NULL on every `canonical_source='fresh'` row) documented as coverage, not data loss.

---

## 4. Grain / reconciliation hazards (carry to serving)

1. **Sub-grain vs award-grain:** ONLY `subaward_amount` is subaward-grain and safe to SUM. Every `prime_award_*` column is prime-award-grain **repeated on every subaward row of that prime** → dedup to `prime_award_unique_key` before any award rollup; **NEVER SUM at sub grain.** `prime_award_amount` is the highest-blast-radius footgun.
2. **Sentinels:** `subaward_amount` 1.0e18 → clamp MAX $100B; `subaward_action_date` 1900/2106 → clamp [1776-01-01, today].
3. **Source-correlated NULL cliffs:** every single-source enrich column is NULL on the opposite leg by construction → union-grain populatedness reads below the single-source pct. Document as `canonical_source` correlation, not data loss; do not impute.
4. **Officer comp:** `subawardee_highly_compensated_officer_{1..5}_*` ~10-13% populated (only >$300k reporters). ~88% NULL is inherent; do not treat NULL as $0 — a forward capability surface, not a load-bearing read yet.
5. **Mod frontier:** the unified `subaward_last_modified_date` carries the WINNING leg's frontier only; combined with `canonical_source` it is the meaningful recency signal. Cross-clock (BULK broker ETL time vs FRESH SAM report mtime) — FRESH generally ≥ BULK and tie→FRESH, so FRESH is the freshness overlay by construction.

---

## Evidence (key files)
- [`usaspending_fpds_canonical.py`](../../pipelines/usaspending/usaspending_fpds_canonical.py) — reference COLUMN_SPEC shape; `transaction_id` enrich BIGINT + `…last_modified_date DESC, transaction_id DESC` collapse (the `broker_subaward_id` analog)
- [`usaspending_api_subaward_fresh.py`](../../pipelines/usaspending/usaspending_api_subaward_fresh.py) — FRESH pull `INDEX_COLS`; confirms subaward file carries NO PSC
- [`materialize_subawardee_designations.py`](../../pipelines/serving/materialize_subawardee_designations.py) — 12 socio designations decoded UEI-keyed (the `business_categories` redundancy)
- [`sam_entity_master.py`](../../pipelines/sam_gov/sam_entity_master.py) — `business_types`/`naics_codes[]`/`psc_codes[]`/`physical_*` per UEI (prime-HQ redundancy)
- `build_subawardee_capability_profiles.py`, `materialize_sub_diversification.py` — subawardee NAICS = mode(prime_award_naics_code) (the `sub_naics` denorm proof)
