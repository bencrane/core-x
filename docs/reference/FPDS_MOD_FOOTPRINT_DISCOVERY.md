# FPDS Modification Footprint — Schema Discovery

**Question answered.** For each `(kind = award_or_idv, action_type)` pair, **which of BULK's 378
columns register a state change** when that modification fires — split into columns whose content is
**on the canonical spine** (`usaspending_fpds_canonical_txn`, 131 cols) versus columns that are
**truly blind** (in the 261 dropped from the spine). This is a *structural checklist* of mutation
footprints, not a ledger reconstruction. It exists to decide whether to **expand the spine** or
**build a separate Lance Entity Dimension**.

**Verified live:** 2026-07-03. Substrate = a full-width slice of BULK
(`s3://data-sink/active/usaspending/transaction_search_fpds/`).

**Artifacts (this directory):**
- [`data/fpds_mod_footprint_matrix.json`](data/fpds_mod_footprint_matrix.json) — the raw computed
  matrix: one row per `(kind, action_type, col)` with `n_changed`, `transitions`, `frac`, `projected`.
- [`data/fpds_mod_footprint_verification.json`](data/fpds_mod_footprint_verification.json) — the
  adversarial-verification blob: 6 independently-recomputed cells, 156 content-classified columns,
  and the synthesis (blueprint + recommendation).

---

## 1. Headline verdict — the socioeconomic worry is falsified

**During option exercise (`G`) and re-representation (`R`/`P`), no socioeconomic / joint-venture /
ownership data mutates in the 261 dropped columns in any way the spine is blind to.**

Independent self-join recompute over the full 20-column socio class:

| Cell | max socio mutation | every statutory ownership/JV boolean |
|---|---|---|
| AWARD/G (146,953 transitions) | `organizational_type` 0.062 · `business_categories` 0.045 | ≤ 0.009 |
| IDV/G (26,337) | `business_categories` 0.049 · `organizational_type` 0.047 | ≤ 0.0006 |
| AWARD/R (88) | `business_categories` **0.62** · `organizational_type` 0.18 | ≤ 0.07 |

The individual statutory booleans (`woman_owned_business`, `veteran_owned_business`,
`minority_owned_business`, `small_disadvantaged_busine`, all `joint_venture_*`, all race/ethnicity
`*_owned_*`) are **award-invariant** — pinned to the base award, never approaching the 0.30
material-mutation bar. What *does* churn under re-representation is **`business_categories`** (the
composite rollup, 0.62–0.83) — and that column **is on the spine**. Re-representation rewrites the
coarse rollup (observed) and, secondarily, the blind `organizational_type` / `corporate_entity_not_tax_e`
structural class (0.15–0.25) — it does **not** flip the granular set-aside certification flags.

Any design that re-materializes granular socioeconomic flags per-modification is over-engineering
against 1–2-row noise.

---

## 2. The mutation matrix — substantive blind spots collapse to two families

After removing information-free **twins** — `generated_pragmatic_obligation` (derived double-twin of
on-spine `federal_action_obligation`, identical frac in every cell) and every `*_desc` (description
twin of a code the spine already carries) — all truly-blind ≥ 0.30 mutations across 24 cells fall
into two disjoint families, **neither of which is a socioeconomic flag**:

| # | Blind family (per-scope) | Columns | Fires under |
|---|---|---|---|
| ① | **Ceiling economics** (award-scoped) | `period_of_perf_potential_e`, `potential_total_value_awar` | AWARD **A** .54, **D** .43, **X**, **F**, **E**, **N**, **H/L** (value), **T**, **V** — the scope-change actions. **Not G/C.** |
| ② | **Entity attributes** (recipient-scoped) | `legal_entity_address_line1`, `legal_entity_zip4`/`zip_last4`, `vendor_phone_number`, `vendor_fax_number`, the `recipient_location_*` geo ladder (state/county/congressional × code+name+fips+population+zip5), `parent_recipient_name_raw`, `parent_recipient_unique_id`, `recipient_unique_id`; +`awarding_office_code`/`name` (T only) | AWARD/IDV **J** (novation), **V** (UEI change), **W** (address), **T** (transfer) |

### 2.1 Per-cell footprint (substantive only; twins removed)

`on-spine` = already captured (direct name or documented alias/MONTHLY source). `blind` = mutates
≥ 0.30 and absent from the spine in any form.

| Cell | n | On-spine mutations (covered) | Truly-blind mutations |
|---|--:|---|---|
| AWARD/G | 146,953 | oblig, current_end, exercised/all-options value, PoP-start, total/current value | **— none** |
| AWARD/C | 574,169 | oblig, PoP-start, exercised/all-options value | — |
| AWARD/B | 445,118 | oblig, PoP-start, current_end, options value | ① potential_end |
| AWARD/A | 26,707 | oblig, PoP dates, options/total/current value | ① potential_end, potential_value |
| AWARD/D | 63,195 | oblig, PoP dates, options/total/current value | ① potential_end, potential_value |
| AWARD/H | 2,240 | oblig, PoP, options/total/current value | ① potential_value |
| AWARD/L | 3,853 | oblig, PoP, options/total/current value | ① potential_value |
| AWARD/R | 88 | **business_categories**, oblig, PoP-start, options value, contracting_officers_deter | — (`organizational_type`/`corporate_entity` mutate 0.15–0.25, sub-threshold) |
| AWARD/P | 59 | **business_categories**, oblig, PoP, options value, CO-determ | ① potential_end |
| AWARD/N | 2,942 | oblig, PoP, value, clinger_cohen, county_name, price_eval_adj | ① potential_value + foreign_funding + county geo |
| AWARD/F | 12,739 | oblig, PoP, value, clinger_cohen, county_name | ① potential_value + foreign_funding + county geo |
| AWARD/E | 624 | oblig, PoP, value, clinger_cohen, county_name, price_eval_adj | ① potential_value + foreign_funding + county geo |
| AWARD/X | 486 | oblig, options value, PoP, current value | ① potential_end, potential_value |
| AWARD/J | 1,073 | **recipient_uei/hash/name**, parent_uei/hash, cage, biz_categories, oblig, city/county_name | ② full entity block |
| AWARD/V | 922 | recipient_uei/hash/name, cage, oblig, city/county_name | ② entity block + potential_value |
| AWARD/W | 1,251 | oblig, options value | ② address/zip/geo-zip5 |
| AWARD/T | 97 | oblig, value, funding_office | ② awarding_office_code/name + potential_value |
| IDV/G | 26,337 | PoP-start, **ordering_period_end_date**, fiscal | **— none** |
| IDV/B | 37,171 | PoP-start, ceiling value, fiscal | — |
| IDV/M | 46,647 | PoP-start, ceiling value, fiscal | — |
| IDV/D | 4,917 | PoP-start, ceiling value, fiscal | — |
| IDV/R | 157 | **business_categories**, CO-determ, ordering_period_end_date | — |
| IDV/P | 35 | **business_categories**, CO-determ, ordering_period_end_date | — |
| IDV/J | 526 | recipient_uei/hash/name, parent_uei/hash, cage, biz_categories | ② full entity block |

**FAR-uniformity holds:** footprints are near-identical *within* each action family, and IDVs carry
the same footprint on their vehicle-scoped fields (`ordering_period_end_date` in place of
`current_end`; ceiling value in place of exercised-options value).

---

## 3. Blind-spot taxonomy — the four classes of truly-blind ≥ 0.30 mutation

1. **Derived twins of on-spine fields** — `generated_pragmatic_obligation` (twin of
   `federal_action_obligation`) and the `*_desc` description tags (twin of on-spine codes:
   `contracting_officers_desc`, `type_of_contract_pric_desc`, `contract_award_type_desc`,
   `commercial_item_acqui_desc`, `type_of_idc_description`, `sam_exception_description`,
   `a_76_fair_act_action_desc`, `program_system_or_equ_desc`). **Carry none — reconstruct on read.**
2. **Per-award economics** — `period_of_perf_potential_e`, `potential_total_value_awar`. The spine
   carries only the *current* end-date/value pair; the *potential* (ceiling) pair is a genuine
   information loss on scope-increasing mods. **→ spine expansion.**
3. **Entity-identity churn** — the family-② block under J/V/W/T. The on-spine identity keys
   (`recipient_uei`, `recipient_hash`, `recipient_name`, `parent_uei`, `parent_recipient_hash`,
   `cage_code`, `business_categories`) mutate **in lockstep**, so the spine records *that* the
   identity changed; only the granular address/geography/contact attributes are blind. **→ entity dimension.**
4. **New-award enrichment metadata under N/F/E** — `foreign_funding` + the `*_desc` twins.
   Low value; reconstruct or ignore.

---

## 4. Architectural recommendation — hybrid, not either/or

**(A) Expand the spine by exactly two columns.** Add `period_of_performance_potential_end_date`
(blind twin `period_of_perf_potential_e`) and `potential_total_value_of_award` (blind twin
`potential_total_value_awar`). These are per-award economic **ceilings** with **no on-spine
surrogate**, mutating ≥ 0.30 under the high-volume scope-change actions (D=63k, A=27k, plus
H/L/N/F/E/X). A scope-increase is currently **invisible in ceiling terms** on the spine. Two value
columns, no new index.

**(B) Build a separate Lance Entity Dimension.** Key on `recipient_uei`/`recipient_hash` (BTREE),
SCD-versioned, holding family ②: `legal_entity_address_line1`, `legal_entity_zip4`/`zip_last4`,
`vendor_phone_number`, `vendor_fax_number`, the `recipient_location_*` geo ladder,
`parent_recipient_name_raw`, `parent_recipient_unique_id`, `recipient_unique_id`, plus
`organizational_type` and `corporate_entity_not_tax_e`. These are slowly-changing entity attributes
that churn **only on identity-boundary actions** (J/V/W/T), not per-transaction — re-snapshotting
them onto 108M transaction rows is wrong. The spine already carries the resolution keys that mutate
in lockstep: the spine records *that* the entity changed, the dimension carries *what* changed.

**(C) Carry nothing else.** Granular socioeconomic/JV booleans are award-stable (max 0.06) — leave
them on the base-award snapshot. Twins are information-free — reconstruct on read.

---

## 5. Method

- **Substrate.** Full-width slice of BULK filtered to a single awarding sub-agency
  (`awarding_sub_tier_agency_c = '5700'`, Air Force) → **3,197,097 rows · 1,313,280 awards · 378
  cols**, materialized to a local parquet. Agency-scoping guarantees **complete modification ladders**
  (an award's `generated_unique_award_id` is agency-fixed), avoiding the cross-table completeness
  gap of diffing a partial slice. Ladder depth: avg 2.43 mods, max 3,277; all 21 action_types
  present in both kinds.
- **Diff.** Per award (`generated_unique_award_id`), ordered by `(action_date, modification_number,
  detached_award_proc_unique)`, each of the 366 diffable columns compared to the immediately-preceding
  transaction via `IS DISTINCT FROM lag(...)` — which **strips restatement noise** (a column counts
  only when its value actually changes). The change is attributed to the *current* row's
  `(pulled_from, action_type)`. Coordinate/identity/timestamp columns (12) excluded.
- **Classification.** Each mutating column resolved **content-aware** against the live spine schema
  (131 cols) + `FPDS_CANONICAL_FIELD_DICTIONARY.md` §3/§6: `on_spine_direct` (same name),
  `on_spine_alias` (different canonical name or MONTHLY/FRESH source — e.g. `officer_N_amount` →
  `highly_compensated_officer_N_amount`, `recipient_name_raw` → `recipient_name`), or `blind`.
- **Verification.** 6 high-stakes cells (G, R, P, J × AWARD/IDV) independently recomputed by a
  **self-join on consecutive rank** (not `LAG`) to catch window bugs — agreement within **0.01** on
  every substantive column. 156 columns content-classified. The two `DISCREPANCY` verdicts were false
  alarms on deliberately-excluded coordinate/timestamp columns.
- **Thresholds.** `frac ≥ 0.30` = reliably mutates (the blueprint); `0.15–0.30` = conditional band.
  The fraction distribution is bimodal at the core (68 cells ≥ 0.90) atop a noise floor (4,687 cells
  < 0.10 — corrections/conditional), confirming the FAR-uniformity premise for the structural
  footprint.

---

## 6. Caveats

1. **Single agency.** Air Force (5700), 3.2M rows. FAR uniformity makes the *footprint* portable, but
   entity-churn and potential-value **base rates** may differ for civilian agencies. **Validate
   against one civilian agency before committing the schema.**
2. **Thin cells.** AWARD/R (88), P (59), IDV/P (35), AWARD/T (97) — exact fractions are directional;
   one row moves them ~0.01–0.02.
3. **Non-standard `action_type = 'Y'`** (24 AWARD / 24 IDV) is outside the FPDS 20-code set —
   undiagnosed, excluded from the blueprint. Flag for the `fpds_action_type_ref` dim.
4. **Matrix build note.** A handful of columns marked `projected=True` in the build classification are
   emitted in zero cells (they sit on the coordinate-exclusion list, e.g. `last_modified_date`,
   `transaction_unique_id`) — an internal emit gap, orthogonal to the architecture decision.

---

*Computed live over BULK `transaction_search_fpds` (2026-07-03) via DuckDB-over-Lance. Matrix +
verification blob committed alongside this report. Reproduce by materializing an agency-scoped
full-width BULK slice and running the lag/diff described in §5.*
