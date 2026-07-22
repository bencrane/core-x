# Query-Sidecar — Agent Navigation Map

**Read this before scanning Lance.** A warm, read-only DuckDB endpoint serves the GTM analytical
substrate — ~1.37B rows across 113 sorted tables — in milliseconds-to-seconds per SQL statement.
If your question is answerable from the tables below, USE THIS. Do not open Lance datasets, do
not register Lance into DuckDB, do not scan `usaspending_fpds_canonical_txn` (392 cols, 108M
rows) for a question `gtm_txn_events_slim` answers in 50 ms.

Provenance: built by [pipelines/query_sidecar/build_query_sidecar.py](../../pipelines/query_sidecar/build_query_sidecar.py)
from the frozen manifest ([SIDECAR_PHASE0_MART_MANIFEST.md](../plans/SIDECAR_PHASE0_MART_MANIFEST.md));
program record + runbook: [QUERY_SIDECAR_PROGRAM.md](../plans/QUERY_SIDECAR_PROGRAM.md);
full-stack onboarding (platform-app → phrase → this artifact): [PHRASE_QUERY_STACK_ONBOARDING.md](PHRASE_QUERY_STACK_ONBOARDING.md).

---

## 1. Connect (copy-paste)

```bash
TOKEN=$(doppler secrets get QUERY_SIDECAR_TOKEN -p core-x -c prd --plain)
curl -s -X POST https://query-sidecar-api.onrender.com/api/v1/sql \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"sql": "SELECT count(*) FROM gtm_txn_events_slim WHERE uei = '\''ABC123DEF456'\''", "limit": 1000}'
```

- `POST /api/v1/sql` `{"sql", "limit"?, "require_artifact"?}` — ONE statement,
  `SELECT`/`WITH`/`DESCRIBE`/`SHOW` only. Default limit 1000, max 50000, 120 s timeout,
  rows returned as `{columns, rows, elapsed_ms, artifact, instance}`.
  **Multi-statement analyses that must reconcile to the dollar:** pin
  `require_artifact` to the first response's `artifact`; a 409 means the snapshot
  moved mid-session — re-run the batch against the new stamp instead of mixing states.
- `GET /api/v1/tables` (bearer) — live table catalog: every table with source dataset, tier,
  sort key, pinned Lance version, row count.
- `DESCRIBE <table>` (via the sql endpoint) — full column list for any table. **Introspect
  instead of guessing column names.**
- `GET /healthz` (no auth) — `artifact` key = the snapshot stamp you are reading.

## 2. Decision rule — sidecar vs Lance

| Question | Where |
|---|---|
| GTM analytics: entities, awards, transactions-by-recipient, teaming, lanes, capabilities, expiry, people/POC lookups | **Sidecar** |
| Per-ACTION description text (`transaction_description`), canonical txn columns beyond `txn_rows`' 16, the full 392-col canonical, `gtm_subaward_recipient_code_evidence` | Lance (not in artifact). Award-grain descriptions ARE here: `award_descriptions` |
| Enrichment identity coverage: PDL match, LinkedIn URLs, icypeas profiles (see §3 Identity/enrichment) | **Sidecar** |
| Secured-debt posture: UCC liens, lenders, collateral, win-then-borrow timing (CA/CO; see §3 Debt/UCC) | **Sidecar** |
| Labor wage floor/market + union exposure: SCA/DBA WD rates per county, OEWS state wage envelope, SCA↔SOC bridge, CBA expiry by uei (see §3 Labor) | **Sidecar** |
| Non-GTM domains (EPA, CMS, MSHA, FDIC, SoS, UCC…) | Lance (not in artifact) |
| Ingest verification / anything needing LIVE data | Lance — the sidecar is a snapshot (see §6) |

## 3. Table catalog (grain · rows · sorted by)

**Join key almost everywhere: `uei` (12-char SAM identifier).**

### Entity spine
| Table | Grain · rows | Sorted | Load-bearing columns |
|---|---|---|---|
| `gtm_sam_entities` | 1/uei · 2.0M | uei | legal_business_name, physical_state/city/zip, primary_naics, in_dsbs, sam_is_active, normalized_domain, cage_code, business_types |
| `gtm_entity_behavior_rollup` | 1/uei · 262k (only entities with award behavior) | uei | prime_obl_12/24/36/60mo/lifetime, prime_award_ct_*, active_award_ct, active_obl, pop_expiring_180d_ct, sub_amt_24/60mo/lifetime, sub_ct_lifetime, is_prime_24mo, is_sub_60mo, prime_and_sub, top_naics, top_agency_code, last_action_date |
| `gtm_entity_geo` | 1/uei · 1.5M | uei | latitude, longitude, geo_precision (HQ, not place of performance) |
| `gtm_audience_entities` | 1/uei · 2.0M | uei | THE audience-spec spine (2026-07-15 cycle): primary_pop_state/county + physical_state, sub_amt_12/24/60mo/lifetime, prime_obl_12/24/60mo/lifetime, **total_amt_12/24/60mo/lifetime (sub+prime combined, derived)**, all *_band cols, dsbs_*/fsrs_* designation flags, n_dialable/n_emailable people coverage, naics_2..6 rollups. One-table audience counts — no 3-way join. |

### Capability lanes (verb doctrine: demonstrated vs inferred)
| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `gtm_entity_code_lanes` | 1/(uei, side, code_type, code) · 1.7M | uei, code | DEMONSTRATED: side='prime' (primed in) or 'sub' (subbed under); code_type 'naics'\|'psc'; obl_12/24/60mo/lifetime, action_ct |
| `gtm_prime_code_signature` | 1/(uei, code_type, code), prime side only · 1.18M | uei, code_type, rank_lifetime | The allocation/signature primitive: per-firm ranked prime record — `rank_24mo`, `rank_lifetime`, `share_24mo`, `share_lifetime` precomputed per (uei, code_type); floors/top-N applied at query time (`WHERE rank_lifetime <= 5 AND share_lifetime >= 0.05`). Never re-derive ranks with window functions over code lanes |
| `gtm_entity_inferred_primeable_codes` | 1/(uei, code_type, code) · 263M | code_type, code | INFERRED could-prime (cooccurrence evidence). Filter by code first — that's the sort |
| `gtm_entity_inferred_subbable_codes` | 1/(uei, code_type, code) · 160M | code_type, code | mirror, could-sub |

### Award/transaction facts
| Table | Grain · rows | Sorted | Notes |
|---|---|---|---|
| `gtm_txn_events_slim` | 1/FPDS action · 108M | uei, action_date | Columns: uei, action_date, action_type_code (A–Y mod events), subcontracting_plan, naics_code, psc_code, awarding_agency_code, **obligation** (≠ federal_action_obligation), action_key, award_key |
| `usaspending_fpds_prime_award_state` | 1/contract_award_unique_key · 83M | current_end_date | 52 cols: award_topology, recipient_uei/name, life_to_date_obligated, current_end_date (expiry queries prune HARD on this), naics/psc, agency, PIIDs, **window state** (own `ordering_period_end_date` + resolved-parent `parent_ordering_period_end_date`/`parent_current_end_date`/`parent_potential_end_date`) **and parent attribution** (`parent_awarding_agency_code`/`parent_awarding_sub_agency_code` = whose vehicle, `parent_idv_type_code`/`parent_award_type_code` = what instrument, `parent_type_of_set_aside_code`). Parent cols populated only when `parent_match_flag='resolved'`; 'self'/'dangling' rows stay NULL. Position/active ladders and "agency behind the parent instrument" are ONE pass, never a self-join. **Since 2026-07-16 also carries the pricing latest-state** (`latest_plan`, `latest_pricing_code`, `latest_financing_code`, `latest_business_size`) — billing shapes are single-table. DESCRIBE it |
| `award_ordering_windows` | 1/award with an ordering window · 982k | contract_award_unique_key | `ordering_period_end_date` (latest-action `arg_max`) + `latest_action_date` — IDV/vehicle ordering-window universe ("which vehicles' windows close in N days") |
| `gtm_position_orders` | 1/order with OPEN window (as of build date) · ~17M | contract_award_unique_key | The position-ladder substrate: contract_award_unique_key, recipient_uei, parent_award_key_resolved, `window_end` (own-else-parent ordering end). **Position rungs join ring keys to THIS, never to the 83M award_state** — any 83M-side join saturates the 2-thread serving box (measured 17–22s) |
| `subaward_canonical_slim` | 1/subaward · 1.3M | prime_awardee_uei | 38 cols incl. `subaward_description`, `prime_award_base_transaction_description`, `subawardee_business_types` (designation flags); `subaward_amount` is VARCHAR — use `subaward_amount_num` |
| `subaward_canonical_slim_by_sub` | same rows | subawardee_uei | second copy, sub-side clustering |
| `gtm_open_awards` | 1/open award · 163k | recipient_uei | active-PoP/open-IDV universe, centroid geo pre-joined |
| `txn_rows` | 1/FPDS action · 108M | action_date | The wire-contract row serving with CANONICAL names (recipient_name, award_id_piid, action_type_description, subcontracting_plan_desc, federal_action_obligation, base_and_all_options_value, awarding_agency_name…) + `type_of_contract_pricing_code`/`type_of_contract_pric_desc` (2026-07-15) — use when you need names/descriptions per action; `gtm_txn_events_slim` for uei-first aggregation |
| `usaspending_award_pop_centroids` | 1/award PoP centroid · 30.7M | state_code, zip5 | Place-of-performance lat/lon per award (zip5→ZCTA). Ad-hoc geo: bounding-box prefilter on state/zip5 (the sort), then haversine; joins awards on generated_unique_award_id |

### The combo-portrait layer (industry × work × time × geo × agency × sub-out, zoomable)

| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `txn_events_combo` | 1/FPDS action · 108M | naics_code, psc_code, action_date | **THE portrait fact.** Every dial as a column: `fy` (federal FY precomputed), `action_type_code`, `subcontracting_plan`, `award_topology` (task orders = 'vehicle_order'), `award_type_code`, `pop_state`, `pop_county_fips`, `pop_county_name`, **`pop_country_code`** (ISO3 — splits the no-US-state bucket into overseas vs unstated; names via `country_vocab`), **`type_of_set_aside_code`** (the set-aside dial), `awarding_agency_code`, `awarding_sub_agency_code`, **`funding_agency_code`, `funding_sub_agency_code`** (who pays vs who signs — `funding_agency_code <> awarding_agency_code` is the split; names via `agency_vocab`/`agency_sub_vocab`), **the pricing-terms dials (2026-07-15 cycle): `pricing_code`** (type of contract pricing — J=FFP, U=CPFF, Y=T&M…; the cash-flow-shape signal), **`financing_code`** (progress/performance-based payments), **`pba_code`**, **`co_business_size`** (CO size determination S/O — the effective net-15 tier), **`labor_standards_code`** (SCA/DBA applies) — names for all five via `fpds_code_vocab`, `obligation`, `uei`, `award_key`. Zoom = `substr()`: NAICS3/4/6 via `substr(naics_code,1,n)`, PSC letter via `substr(psc_code,1,1)`, family = `substr(naics_code,1,4)||'x'||substr(psc_code,1,1)` |
| `txn_events_combo_by_geo` | same rows | pop_state, pop_county_fips, action_date | Second copy — **state/county-anchored** questions prune here |
| `txn_events_combo_by_award` | same rows | **award_key_pfx**, award_key, action_date | **Award-key point-read copy** (2026-07-21 award-key cycle): per-award FY/ledger reads. See the award-key-pfx note below — probe `WHERE award_key_pfx = substr('<key>',10,12) AND award_key = '<key>'` |
| `txn_rows_by_award` | same rows as `txn_rows` · 108M | **award_key_pfx**, contract_award_unique_key, action_date | Award-key copy of the wire-row serving — per-award "recent actions". Same pfx probe shape |
| `award_subout_rollup` | 1/prime award with subs · ~197k | prime_award_unique_key | `sub_ct`, `distinct_subs`, `sub_amount_total`, first/last sub date. Join on `award_key` → "is this work getting subbed out". GROUP-BY aggregate (197k rows, not the 6.3M raw subawards) — a bare `WHERE prime_award_unique_key = '<key>'` scan is already cheap; no award-key-pfx copy needed |
| `prime_award_state_by_key` | 1/award · 83M | **award_key_pfx**, contract_award_unique_key | **Award-key point-read copy** of `usaspending_fpds_prime_award_state` (full 56-col width). The award-drawer anchor read: was 4.8s on the `current_end_date`-sorted spine, ms-class here. Probe `WHERE award_key_pfx = substr('<key>',10,12) AND contract_award_unique_key = '<key>'` |
| `award_pop_centroids_by_key` | 1/award centroid | **award_key_pfx**, generated_unique_award_id | Award-key copy of `usaspending_award_pop_centroids` — per-award PoP point-read. Same pfx probe shape |
| `agency_sub_vocab` | 1/sub-agency code | code | code → majority name (agency trends display) |
| `award_descriptions` | 1/award · 30.7M | recipient_uei | Award requirement `description` + `solicitation_identifier`/`solicitation_date` (PDF-handoff join keys) + PIID + both award keys. **History tabs:** a UEI's awards + descriptions (or the glaring lack) = one pruned read. Sub-side: `subaward_canonical_slim.subaward_description` AND the prime's `prime_award_base_transaction_description` on the same row |
| `award_plan_state` | 1/award · ~40M | contract_award_unique_key | Latest-action state per award: `latest_plan`, `latest_pricing_code`, `latest_financing_code`, `latest_business_size`. **Since 2026-07-16 these are ALSO denormalized onto `usaspending_fpds_prime_award_state` — never join this to award_state at query time** (32–49 s measured; the 83M-join saturation class). Billing shapes read award_state single-table; entity-level mix reads `gtm_entity_pricing_mix` |
| `gtm_entity_fy_won` | 1/(uei, federal FY) | uei, fy | **Market-composition cycle (2026-07-17): the FY-window won measure as a pruned leg.** `won_obl` (Σ obligations), `action_ct`, `award_ct`, and the set-aside WIN family — `won_obl_set_aside` (any) + `won_obl_8a/sdvosb/wosb/hubzone`. "Won FY23–25" = `SUM(won_obl) WHERE fy IN (2023,2024,2025)` per uei — the window stays a query-time dial deliberately. Replaces the 790ms 108M group-by |
| `gtm_entity_award_book` | 1/uei | uei | **The ontology's committed/vehicle book as named columns** (doctrine pinned once: active = PoP live AND NOT terminated; committed = topology ≠ vehicle; every $ floored 0/award): `committed_award_ct/value/obligated/runway`, `committed_award_median/avg` (size texture), `committed_value_set_aside`, `vehicle_ct/ceiling/headroom` (NEVER blended with committed), `next_committed_end_date`, `active_agency_ct` |
| `gtm_entity_firmographics` | 1/uei bridged · ~464k | uei | **The employee-size/industry predicate, ms-class** (was a 10.0s query-time bridge⋈PDL join): employee_size_range, industry, locality/region/country, year_founded, linkedin_slug, normalized_domain + is_generic_domain, company_name, duns, pdl_company_id. Best-row-per-uei (prefers employee_size, then non-generic domain). Coverage = the bridge (~464k of 2.0M registrants) — disclose |
| `gtm_entity_pricing_flow` | 1/uei · 163k | uei | **The trailing-window pricing/labor FLOW** (complement to `gtm_entity_pricing_mix`'s active STOCK; 2026-07-21 cycle): 12/24/48-month windows of pricing-class transition (FFP→cost/T&M shift) + SCA/DBA labor exposure. Windows anchored to the data's max(action_date) watermark. "Who is shifting into cash-intensive contract types" = one uei-sorted read |
| `gtm_entity_pricing_mix` | 1/uei · 767k · **71 cols** | uei | **The billing/capital-provider lens** (2026-07-16; combo matrix 2026-07-17 operator directive): active vs total book by pricing class — `active_obl_fixed/cost/tm_lh/other` (class map: fixed A,B,J,K,L,M · cost R,S,T,U,V · tm_lh Y,Z), FFP-unfinanced pair + `active_obl_small_determined` + `active_fixed_share`/`active_ffp_unfinanced_share`. **Financing classes** (`unfin` NULL/Z/NA · `prog` A,B,E+text twins · `perf` C · `comm` D · `othfin` rest incl. undocumented F): `active_obl_fin_{cls}`/`active_fin_{cls}_ct` + `active_financed_share`. **Full pricing×financing matrix**: `active_obl_{fixed,cost,tm_lh,other}_{unfin,prog,perf,comm,othfin}` + `active_{pc}_{fc}_ct` (20 cells ×2). **Instrument riders** (standalone split D vs B): `lifetime/active_definitive_ct`, `lifetime/active_purchase_order_ct`, `active_obl_definitive/purchase_order`. "Primes ≥X% progress-payment-financed" = one uei-sorted read, ms-class |
| `action_type_vocab` | 1/action_type_code · 22 (21 codes + NULL base row) | — | **The action-type language layer** (2026-07-15): `source_description` (empirical majority — source pairs are messy: 102 raw tuples), `plain_english` (subject-first query phrase: "received additional funding", "had an option year exercised"), `family` (new_award\|more_work\|funding_only\|termination\|definitization\|closeout\|admin), `is_more_work` (A,B,D,G,L), `is_funding_released` (C,G — G carries BOTH: option exercise turns on work AND its money; C is the only pure-money event). NULL code row = base/initial award (FPDS stamps action type on mods only) |
| `fpds_code_vocab` | 1/(field, code) · ~100 | field, code | Name resolution for the five pricing-terms code spaces: `field` ∈ pricing \| financing \| performance_based \| business_size_determination \| labor_standards, majority name per code |
| `naics_psc_labor_profile` / `naics_psc_deliverable` | 1/(naics, psc) · 16.3k / 21k | naics_code, psc_code | The combo-grain LANGUAGE layers: `work_summary` + labor-play/OEWS mapping; `what_was_done` + work_type/regime/confidence — plain-language rendering joins these onto any sidecar code set (letters, on-page copy). Complements the code-grain to-verb vocabulary in the phrase compiler |
| `naics_psc_labor_profile_categories` | 1/(naics, psc, rank) · 54k | naics_code, psc_code, rank | Ranked SOC/SCA occupational categories per combo (the "additional ___" candidates), wage medians, growth |
| `naics_psc_vertical_map` | 1/(naics, psc) · 279 | naics_code, psc_code | Curated vertical + **equipment_intensity** + regime per anchor combo |
| `naics_psc_equipment_needs` | 1/(naics, psc) · 9,693 | naics_code, psc_code | **Equipment demand per combo.** LLM verdict `proposed_equipment_needs` (comma-joined phrases; explode via `v_equipment_needs_phrases`) + `reasoning`/`confidence`, and the deterministic heavy-iron slice: `in_scope` (5,729 true), `equipment_buckets` (LIST — `list_contains(...,'material_handling_cranes')`), `primary_bucket` (industrial_power_support\|material_handling_cranes\|heavy_earthmoving_civil\|trucks_heavy_haul\|aerial_access; NULL when out-of-scope), `core_phrase_count`/`other_phrase_count`. Join combo demand ($/geo) on (naics, psc) |
| `combo_award_active_state` | 1/(naics, psc) · ~20k | naics_code, psc_code | **Combo-grain award-lifecycle mart** (snapshot, from the 83M award_state). Active (`days_to_expiry>0 AND is_terminated=FALSE`) split — `active_award_ct`, `active_recipients`, `active_obligated`, `active_current_value`, `active_ceiling_headroom` — alongside totals (`award_ct`, `recipients`, `obligated_total`) and the `terminated_*`/`expired_no_followon_ct` denominators. "Active $ where the work needs [bucket]" = this ⋈ `naics_psc_equipment_needs` (or `v_combo_active_equipment`). Zoom to family via `substr()` re-aggregation |
| `v_combo_fy` / `v_family_fy` / `v_award_subout` | views | — | Baked portrait queries: combo×FY measures (prime $, plan-attached share, task-order share); family grain; award×sub-out join |
| `v_combo_active_equipment` / `v_equipment_needs_phrases` | views | — | Product surface: `combo_award_active_state` ⋈ equipment verdict on (naics, psc) — "active $ of [bucket]-needing work" is one GROUP BY; and the phrase-grain vocabulary explode of `proposed_equipment_needs` (per-combo phrase profile / head coverage) |
| `v_psc_names` / `v_naics_names` / `v_sam_declared_codes` | views | — | Vintage-safe reference names (active-else-latest, 1 row/code); SAM declarations unnested to (uei, is_active, code_type, code) |
| `v_staffing_absorption` | view | — | **The staffed-out residual** (2026-07-15, directional v1): per (uei, naics, psc) prime lane — `implied_labor_dollars_60mo` (prime $ × `loaded_labor_share`), `implied_fte_per_year_60mo` (÷ combo avg SOC wage ÷ 5 — methodology is visible in the view SQL, a query-time dial, deliberately not a baked mart), `farmout_amt_60mo`/`farmout_share_60mo` (reported farm-out), `employees_on_linkedin`/`pdl_employee_size_range` (observable headcount via SAM↔PDL bridge). Big implied labor + small headcount + little reported farm-out = invisible staffing arrangements (the Optum-nurses shape). Seconds-class (~9 s even filtered — the headcount CTE walks the full SAM↔PDL bridge): fine for directional pulls, not for hot paths |

### Rollups & expiry
| Table | Grain · rows | Sorted |
|---|---|---|
| `gtm_txn_recipient_month_rollup` | uei × action_type × plan_class × month · 34M | uei |
| `txn_recipient_month_by_type` | same fact re-sorted · 34M | action_type_code, month — cross-entity "actions of type X in window Y" prunes here (9.6ms vs 2.0s on the uei-sorted copy) |
| `txn_recipient_month_pop` | uei × action_type × pop_state × pop_county_fips × **naics × psc** × month · 37M | action_type_code, pop_state, pop_county_fips, month — **the entity-event-GEO rollup**. Since 2026-07-16 the grain carries naics/psc, so event verb × state × job/need combo composes in one statement (SUM to the coarser grain stays correct). "Entities that had action X in state S in window W" prunes on (type, state) |
| `gtm_construction_lane_months` | construction work-lane × uei × month · ~1M | lane, uei, month — **the growth-lane mart** (2026-07-19 cycle): the 5 surety work-typed pair-sets (544 pairs, vertical-building / building-repair-alteration / building-maintenance / civil-infrastructure / industrial-defense-facilities) pre-joined to month grain. Columns: obligation_sum, n_actions, n_awards, n_new_awards + new_award_obligation_sum (base-award rows — "new work vs mods"), n_agencies. Growth windows are QUERY-TIME dials over month ranges (never baked); anchor to `max(month)` (FPDS publication lag: the freshest ~1 month is empty, months 2–3 half-reported — DoD ~90-day embargo) |
| `gtm_award_recipient_rollup` | uei × naics × psc × agency × topology · 6.3M | uei |
| `gtm_award_expiry_months` | uei × end_month · 221k | uei, end_month |
| `gtm_prime_pop_lanes` | 1/(uei, pop_state, county) · 547k | uei |

### Entity change & signal events (the "structural change in the last N days" layer)
| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `sam_master_profile_deltas` | 1/(uei, field, to_label) · ~5.8M | uei, to_date | SAM vintage-diff CHANGE events. `field` ∈ {cage_code, entity_structure, primary_naics, purpose_of_registration, registration_status, legal_business_name, naics_added, naics_removed, naics_sb_flag_changed, bus_type_added/removed, psc_added/removed}. `old_value`/`new_value`; NAICS carries `sb_flag_old`/`sb_flag_new` (small-business Y/N/E). Bounded to `(from_date, to_date]` with `window_days`. A row exists only when the UEI is in BOTH adjacent vintages (whole-vintage absence = churn). **Net-new CAGE** = `field='cage_code' AND coalesce(old_value,'')=''`; **high-liability NAICS add** = `field='naics_added' AND new_value IN (...)`; **sizing-posture flip** = `field='naics_sb_flag_changed'`. Filter `to_label`/`to_date` for the window (latest MEANINGFUL transition is `2026_MAY`; `20260503` is a near-dup of it → ~empty). |
| `gtm_fpds_entity_signal_events` | 1/(uei, signal_type, signal_value) · ~285k | uei | FPDS day-precision demonstrated signals. `signal_type='cage_txn'` → `signal_value`=cage, `first_action_date`/`last_action_date`/`action_ct`/`obl_sum` (the exact day a UEI's CAGE first transacts — cross-ref a net-new CAGE from the delta mart). Flag types `jv_8a_certified`, `jv_econ_disadv`, `jv_women_owned`, `c8a_participant` = verified structured JV/8(a) events (replaces the polluted SAM name-pattern JV heuristic). |

### Teaming / relationship substrate
| Table | Grain · rows | Sorted |
|---|---|---|
| `gtm_prime_sub_pairs` / `gtm_prime_sub_pairs_by_sub` | 1/(prime_uei, sub_uei) · 269k | prime_uei / sub_uei |
| `gtm_prime_combo_lanes` | 1/(uei, naics, psc) · 5.1M | uei |
| `gtm_sub_combo_lanes` | 1/(uei, naics, psc) · 339k | uei |
| `gtm_prime_farmout_combo_lanes` | 1/(uei, naics, psc) with farm-out · 38k | uei | **carries its denominators + shares** (2026-07-15): `prime_obl_24mo/60mo/lifetime`, `prime_txns_lifetime`, `farmout_share_24mo/60mo/lifetime` (farm-out ÷ prime obligations; NULL when the prime has no obligations in the combo). "Primes that sub out >N% of X-shaped work" = one pruned read |
| `gtm_prime_vehicle_lanes` | 16k | uei |
| `gtm_prime_demand_events` | event/prime uei (24mo) · 11.3M | uei |
| `gtm_primes_by_recipient_code` | 1/(code_type, code) marginal · 1.7M | recipient_code |
| `gtm_prime_subout_by_recipient_code` | prime × context_code × recipient_code cube · 11.8M | prime_awardee_uei | **Farm-out characterized by the RECIPIENT's shape** — `recipient_code_source` ∈ awarded_prime_contracts_in_code (the sub's own prime history) \| delivered_subawards_under_code \| sam_registered_naics \| sam_primary_naics \| subaward_reported_naics. ⚠ lenses OVERLAP — filter ONE source per query, never sum across. Since 2026-07-15: `prime_obl_24/60mo/lifetime_in_context` + `prime_action_ct/last_action_in_context` (denominator family), **`subout_rate_lifetime`** (subaward $ ÷ prime obl in context; can exceed 1 on pass-throughs), **`share_of_context_subout`** (within-lens: of what this prime subs out in context X, the fraction going to shape Y) |
| `gtm_prime_subout_by_code` | same 11.8M rows | recipient_code_source, recipient_code_type, recipient_code | Second copy — **recipient-shape-anchored** reads ("primes that route ≥N% of X work to subs who prime in Y") prune here |
| `gtm_subbed_under_to_primed_in_cooccurrence` | code × code matrix · 589k | subbed_under_code |
| `gtm_sub_profiles` · `govcon_subawardee_profiles` | 1/sub uei · 105k · 25k | uei / sub_uei |
| `gtm_sub_universe_pairs` / `_targets` | pair-grain recipe precompute · 30k | target_uei |

### Identity / enrichment layer (gap-pass-4: "does population X have coverage" in one statement)
| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `bridge_sam_pdl` | 1/uei matched · 802k | uei | SAM↔PDL identity bridge: uei, duns, pdl_company_id, normalized_domain — THE coverage join for "what % of this firm set has a PDL match" |
| `pdl_normalized_companies` | 1/pdl_company_id · 35.4M | pdl_company_id | Canonical PDL company: names, normalized_domain, **linkedin_slug** (the company LinkedIn URL), locality/region/country, industry, employee_size_range, year_founded. Hydrate matches via the bridge; domain-anchored matching joins `normalized_domain` (unsorted — seconds-class scan) |
| `icypeas_company_scrapes` / `icypeas_dsbs_company_profiles` | 1/uei scraped · 6.6k / 5.8k | uei | Scraped company LinkedIn profiles (URL, headcount, industry, description); raw blobs excluded |
| `icypeas_person_profiles` / `icypeas_person_profile_scrapes` | 14.8k / 9.5k | uei / person_linkedin_url_norm | Person-profile scrape ledger (status/found per input) and the scraped profile content (title, summary, company block, education); raw blobs excluded |
| `pdl_slug_lookup` | 1/(linkedin_slug, pdl_company_id) · 35.4M | linkedin_slug | **LinkedIn-URL resolution hop** (2026-07-17): slug (lowercased at build) → pdl_company_id, company_name, normalized_domain, is_generic_domain. Slug probes prune to ms (unsorted probe on the base measured 18.4s). Registrants via `bridge_sam_pdl` (DISTINCT — the bridge carries dup rows) or the domain leg |
| `entity_hierarchy` | 1/uei · 148,766 | uei | SAM parent hierarchy: immediate + **ultimate** parent uei/name, hierarchy_depth, in_cycle. THE family disambiguator for shared-domain/slug resolution and the rollup dimension for family analysis |
| `bridge_dsbs_pdl_linkedin` | 1/uei · 53k | uei | DSBS→PDL/LinkedIn resolution (best_domain + matched pdl id + company_linkedin_url) |
| `dsbs_poc_linkedin` · `exa_person_linkedin_candidates` | 821 · 33k | uei | Person-side LinkedIn resolution candidates (raw JSON excluded) |
| `us_software_companies` | 1/domain · 173,119 | domain | **US software/SaaS commercial universe** (2026-07-20 compliance-friction cycle). "Is this domain a commercial software vendor" as a warm join — 27 firmographic cols: company_name, `industry` (Software Development / IT Services / Technology-Internet dominant), employee_size_range, founded, total_funding_range, annual_revenue_clay/hubspot, specialties, description, linkedin_url, slug, `country` (~94% US). **Federal bridge: `JOIN gtm_sam_entities s ON s.normalized_domain = us_software_companies.domain`** (uei↔domain; ~73% of registrants resolve a domain) — commercial-software ∩ federal-behavior is ONE native join (measured 162 ms; was a Lance+Python cross-system hand-join). ~6% non-US present — add `country='United States'` when the population must be US-only |

### Debt / UCC layer (CA + CO; SAM∩UCC via the SoS crosswalk hub)
| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `sam_ucc_debtor_overlap` | 1/(uei, sos_entity_key) · 87k | uei | "Carries debt?" — n_ucc_financing, n_active_ucc_liens, `has_active_lien`, `has_tax_lien` (involuntary liens kept SEPARATE from "taking $"), officer corroboration, `overlap_confidence` (very_high…low). CA/CO registrants only — coverage is the federal∩state-registered intersection, not all debtors |
| `sam_ucc_filings` | 1/(uei, ucc_state, filing_id) · 376k | uei, first_filing_date | "When / from whom / against what" — first/last filing dates (recency = fresh borrowing), lapse/terminated, `filing_class` financing\|tax_or_judgment, `is_active_financing`, `is_lease` (CA Lessee/Lessor = true equipment leases), **`secured_parties`** (who holds the paper), `collateral_text` (CO). Interleaves with `gtm_txn_events_slim` on uei for award→borrow sequencing |
| `ucc_filings_all` | 1/(ucc_state, filing_id, debtor_key) | ucc_state, debtor_name_norm | The FULL CA/CO corpus — every debtor (org AND individual, `is_org`), not just SAM registrants; `uei`/`sos_entity_key` NULLABLE enrichment + `in_sam` flag; debtor identity/geo (`debtor_name`, `debtor_name_norm`, `debtor_city/state/zip`); same filing attributes as sam_ucc_filings. Use for lender-book and non-SAM debtor questions; use sam_ucc_filings for uei-first joins |

### Lender surface (CA/CO secured parties, classified)
| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `sam_ucc_lenders` | 1/normalized lender · ~17k | lender_key | Secured parties on SAM-firm financing filings, classified `lender_class` = bank_or_cu (FDIC/NCUA authority match + token) \| filing_agent \| government_sba \| **non_bank**; `in_efc` = name-matched to equipment_finance_candidates (incumbent vs whitespace); sam_firms/filings/active_filings, CA/CO firm splits, first/last filing dates. Known residual: vendor/trade creditors (equipment makers filing their own paper) classify non_bank — curate on use |
| `ucc_lenders_all` | 1/normalized lender | lender_key | Full-corpus analogue of sam_ucc_lenders (same class brackets + `in_efc`): counts over ALL CA/CO debtors — **`total_firms`** (every distinct debtor) alongside `sam_firms` (SAM-registered subset; the ratio = federal-exposure share), filings/active_filings, CA/CO firm splits, first/last filing dates |
| `ucc_lender_filings` | 1/(lender_key, ucc_state, filing_id, debtor_key) · ~9M | lender_key, uei | **The lender→book bridge** (lender-book cycle 2026-07-17): `secured_parties` exploded + normalized at build (lockstep with `_LK` in sam_ucc_debtor_overlap.py), so one lender's FULL debtor book is a pruned probe (was a ~4s corpus scan that also row-capped mega-lenders). Carries raw `lender_name` as filed + full debtor identity/geo (`debtor_name/_norm/_city/_state/_zip`, `is_org`, `uei`/`sos_entity_key`/`in_sam` nullable) + all filing attributes (`filing_class`, `terminated`, `is_active_financing`, `is_lease`, first/last/lapse dates, `n_secured_parties`). Blobs (`secured_parties`, `collateral_text`) deliberately absent — join back to `ucc_filings_all` on `(ucc_state, filing_id, debtor_key)` when needed. `lender_class` at read time: join `ucc_lenders_all` on `lender_key` |
| `fdic_institutions` | 1/bank · 27.8k | name | Slim authority: name, cert, active, city/state, webaddr, asset |
| `ncua_credit_unions` | 1/CU · 4.3k | credit_union_name | Slim authority: name, charter, location, members, total_assets |
| `equipment_finance_candidates` | 1/candidate · 429 | company_name | The GTM candidate list (name, domain, LinkedIn; verdict unset — another lane's dataset, read-only here). Reconcile: join `sam_ucc_lenders.in_efc` or name-match |

### Equipment supply (the shop side of the equipment GTM — classify · inventory · award-overlap)
Keyed on domain; join `firmographics_blitz` on `domain_norm` for name/geo. The `supported_pscs`/`qualified_pscs` LISTs join combo demand (`naics_psc_equipment_needs`, `combo_award_active_state`) on the shared PSC taxonomy — supply ⋈ demand.

| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `equipment_provider` | 1/(record) · 4,700 (**not** unique on domain — 4,499 domains; dedup via `v_equipment_supply`) | domain_norm | Classifier verdict: `is_equipment_provider` (2,269 true), `mode`, `confidence`, `reasoning`/`steps_taken`/`evidence_url`/`evidence_snippet` (the evidence trail); raw_payload excluded |
| `equipment_matchmaking` | 1/domain · 3,096 | domain_norm | Scraped inventory → PSC: `verified_inventory_matches` (LIST of inventory phrases), `supported_pscs` (LIST), `matched_psc_count`. "Which shops carry [PSC/bucket]" = `list_contains(supported_pscs, ...)` |
| `equipment_rental_golden_overlap` | 1/firm · 879 | firm_domain | Award-overlap capability score (`firm_domain` == domain_norm): `qualified_pscs` (LIST), `qualified_psc_count`, `qualified_value_exposure`, `capability_capture_ratio` (qualified vs all nearby award count), `qualified_nearby_award_count`/`all_nearby_award_count` |
| `v_equipment_supply` | view | — | Shop profile in one read: `equipment_provider` (deduped to best row/domain — is_equipment_provider TRUE first, then latest `materialized_at`) ⋈ inventory ⋈ golden-overlap on domain |

### Labor occupation-grain layer (gap-pass-6: market-vs-floor wage, per-county, + union exposure)

The connected subgraph: award `(naics, psc)` → the combo labor layer (`naics_psc_labor_profile_categories`) → SCA/SOC bridge → wage (statutory **floor** via WD rates + county coverage + FIPS crosswalk; **market** via `soc_state_wage`) → uei union exposure. Every market-vs-floor wage answer for a staffing GTM pitch lives here.

| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `sam_wd_rates_structured` | 1/(wd_id, occupation_code, classification) · 522k | wd_id, occupation_code | The priced statutory **floor**: parsed SCA/DBA wage determination rates. `wage_rate`, `fringe`, `fringe_is_pct`, `hw_rate`, `hw_rates_all`, `wd_type`, `revision_number`, `classification_title`, `footnote_ref`. Probe `wd_id` (the sort) or `occupation_code` |
| `sam_wd_county_coverage` | 1/(wd_id, county) · 33k | wd_id | Which counties a WD covers: `state_code`, `state_name`, `county_code`, `county_name`. The hop that binds a WD's rates to geography |
| `sam_county_fips_crosswalk` | 1/(state, county) · 3.3k | state_code, sam_county_name | `(state_code, sam_county_name)` → `county_fips` (98.5% resolved). `sam_is_city_flag`, `gazetteer_name`, `resolution_status`. Bridges WD locality to award-spine PoP county FIPS |
| `soc_state_wage` | 1/(soc_code, state) · 35k | soc_code, prim_state | The **market** half: OEWS state wage envelope per SOC. `a_median`/`a_pct25`/`a_pct75` (annual), `h_median`/`h_pct25`/`h_pct75` (hourly), `tot_emp`, `a_mean`, `state_fips`, `soc_vintage` |
| `sca_soc_crosswalk` | 1/occupation_code · 424 | occupation_code | The bridge both halves meet on: SCA `occupation_code` → `soc_code` with `tier`/`method`/`confidence`/`dominance_ratio`/`primary_dollar_weight`. Also carries `occupation_title`/`family_code`/`family_title` (its own name layer) |
| `dol_sca_occupations` | 1/occupation_code · 502 | occupation_code | SCA occupation taxonomy — the name/display layer (analog of `v_psc_names` for PSC): `occupation_title`, `occupation_definition`, `family_code`, `family_title`, `edition` |
| `olms_cba_crosswalk` | 1/(doc_id) uei-matched · 4.8k | uei | Union exposure column for any target list: `uei` → `union_name`, `exp_date` (CBA expiration), `emp_name`, `tier`/`score`/`on_spine`/`is_active`/`geo_corroborated`. Join warm award tables on `uei` for §4(c) successorship exposure |
| `v_wd_county_rates` | view | — | The county-priced floor in one SELECT: `sam_wd_rates_structured` ⋈ `sam_wd_county_coverage` ⋈ `sam_county_fips_crosswalk`. Columns: wd_id, revision_number, wd_type, occupation_code, classification_title, wage_rate, fringe, fringe_is_pct, hw_rate, state_code, state_name, county_name, county_fips, resolution_status. Predicates on wd_id / occupation_code / county_fips prune the underlying sorted tables |
| `naics_labor_share` | 1/6-digit naics · 1.1k | naics_code | **The award-dollar pricing scalar** (labor-pricing cycle 2026-07-14): `loaded_labor_share = payroll_share × burden_multiplier` (SUSB × ECEC burden; BEA `bea_comp_share_of_output` cross-check) + provenance dials `payroll_share_level` (0 = sector-92 economy fallback) / `burden_match_level`. Closes `expected labor $ = award_$ × loaded_labor_share × pct_of_industry/100` — ⚠ `pct_of_industry` is PERCENT; prefer `v_role_priced_combos.category_award_share`, which bakes the /100 |
| `occupation_alias_lookup` | 1/(alias_norm, code_type, code) · 66.9k | alias_norm, code | **The role-name entry hop**: free text → normalized `alias_norm` probe → SOC/SCA. O*NET primary/reported/alternate + SCA titles, parenthetical variants split ("Travel RN" and "Travel Registered Nurse" both resolve); SCA rows carry `bridged_soc_code`/`bridge_tier` inline; `in_combo_layer` = reachable through the ranked combo profiles. Ambiguous aliases (~8k map to >1 code): rank by `source_priority` then `in_combo_layer` |
| `v_role_priced_combos` | view | — | The pre-call composite in one SELECT: alias → (soc leg ∪ sca leg) → `naics_psc_labor_profile_categories` ⋈ `naics_labor_share`. Probe `alias_norm` (prunes the sorted alias table); carries combo rank, soc/sca titles, cast `a_median`, growth, labor-share provenance, and precomputed `category_award_share = loaded_labor_share × pct_of_industry/100` |

### People / identity / reference
| Table | Grain · rows | Sorted |
|---|---|---|
| `gtm_sam_people` | 1/(uei, name_key) · 2.3M | uei |
| `gtm_sam_person_contactability` | 1/sam_person_id · 152k | sam_person_id |
| `gtm_person_channels` | 1/(uei, sam_person_id) · 2.25M | uei | **SAM people ⋈ enrichment contactability, pre-joined at uei grain** (2026-07-21 cycle): `display_name`, `first_name`, `last_name`, `best_title`, `email`, `email_verification_status`, `phone`, `phone_status`, `person_linkedin_url_norm`, `is_govt_poc`, `is_ebiz_poc`, `n_sources`. One uei-sorted point-read for "the people at firm X + how to reach them". Channel coverage is thin (~10% have email/phone) — email/phone/linkedin where enrichment resolved, name/title otherwise (LEFT JOIN, every SAM person preserved) |
| `sam_pocs` | 1/(uei, role, slot) · 8.1M | uei |
| `sam_master_entities` | 1/uei SAM registration master · 1.5M | uei | ⚠ declaration columns: use the LIST columns `naics_codes`/`psc_codes` (VARCHAR[]) — or `v_sam_declared_codes`, which unnests them. The `*_counter`/`*_string` near-duplicates are raw ingest artifacts; counting on them produced a retracted figure (84% vs the true ~21% PSC declaration rate). Measured semantics: NAICS declarations near-universal among registrants, PSC sparse/optional |
| `people_canonical` | 1/canonical_person_id · 132k | canonical_person_id |
| `firmographics_blitz` | 1/domain · 255k | domain_norm |
| `federal_sites_lance` | 1/federal site · 300k | state_code, zip5 |
| `military_installations` | 1/DoD MIRTA site point · 831 (792 USA-active) | state_code | Installation overlay: `site_name`, `feature_name`/`feature_description`, `component` (usa/usn/usaf/…), `operational_status` (`act` = active), `is_joint_base`, `latitude`/`longitude`. Filter `country='USA' AND operational_status='act'` for the serving overlay; join territory cuts via state or lat/lon distance |
| `naics_reference` · `psc_reference` · `gtm_naics_psc_pairs` · `agency_vocab` · `country_vocab` | code refs · 2.1k/6.1k/321k/75/~250 | code | ⚠ vintages: both reference tables carry multiple `source_vintage` rows per code; `psc_reference WHERE is_active` returns NULL names for retired-vintage codes that still carry award dollars. **Display names: join `v_psc_names` / `v_naics_names`** (active-else-latest-vintage, one row per code) |
| `_sidecar_manifest` · `_sidecar_meta` | provenance: per-table pinned Lance version, build stamp | — |

### VA veteran demand-side cluster (county-grain, FIPS-keyed)
Demand denominator for the VA C&P exam lane (naics `621111` × psc `Q403`): rank where clinician-staffing demand outruns local medical-labor supply. Both key on 5-char county `fips` → join `txn_events_combo_by_geo.pop_county_fips` (the award-spine geo grain) or SAM `physical_state`. VA `state` is the full name ("Alabama"); for 2-letter joins derive via `substr(fips,1,2)` → `sam_county_fips_crosswalk`.
| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `va_vetpop_county_total` | 1/(fips, snapshot_year) · 98k | fips, snapshot_year | Veteran population per county, **31 projection years FY2023→FY2053** (VetPop2023). `veterans_total`, `county_state`, `state`. Filter `snapshot_year=2023` for the current denominator; range it for the per-county veteran **trend**. FY2023 national = 18,266,748 |
| `va_disability_comp_county` | 1/(fiscal_year, fips) · 16k | fips, fiscal_year | Disability-compensation **recipients** by county, FY2019/21/23/24/25 — the PACT-Act-driven exam-demand signal. `recipients` + SCD-severity bands `scd_0_20`…`scd_100` (higher rating → re-exam intensity) + `age_17_44`/`age_45_64`/`age_65_plus` + `male`/`female`. 8 "Unknown"/foreign rows carry null `fips` (kept so totals stay whole — filter `fips IS NOT NULL` for county joins). Parked Lance-only: `va_vetpop_county` (781k age×sex×year population detail) |

## 4. Query patterns (proven shapes)

```sql
-- Entity point profile (ms)
SELECT * FROM gtm_entity_behavior_rollup WHERE uei = 'XXX';

-- "Companies that X and Y" = INTERSECT legs on uei, then hydrate
WITH f AS (
  SELECT DISTINCT uei FROM gtm_entity_code_lanes
   WHERE side='prime' AND code_type='naics' AND code='236220'
  INTERSECT
  SELECT uei FROM gtm_entity_behavior_rollup WHERE prime_obl_lifetime >= 1e7
)
SELECT e.legal_business_name, r.prime_obl_lifetime
FROM f JOIN gtm_sam_entities e USING(uei) JOIN gtm_entity_behavior_rollup r USING(uei)
ORDER BY r.prime_obl_lifetime DESC LIMIT 100;

-- Event collapse: "recipients who got a code-G mod in 90d", $-ranked
SELECT uei, count(*) n, sum(obligation) amt
FROM gtm_txn_events_slim
WHERE action_type_code='G' AND action_date >= current_date - 90
GROUP BY uei ORDER BY amt DESC LIMIT 50;

-- Expiring award universe (prunes on the sort key)
SELECT recipient_uei, recipient_name, life_to_date_obligated, current_end_date
FROM usaspending_fpds_prime_award_state
WHERE award_topology IN ('standalone','vehicle_order')
  AND current_end_date BETWEEN current_date AND current_date + 90;

-- Lookalikes: never-primed firms with inferred capability (filter code FIRST — it's the sort)
SELECT i.uei FROM gtm_entity_inferred_primeable_codes i
JOIN gtm_entity_behavior_rollup r USING(uei)
WHERE i.code_type='naics' AND i.code='541330'
  AND r.sub_ct_lifetime > 0 AND r.prime_award_ct_lifetime = 0;

-- Teaming: who subs under this prime
SELECT p.sub_uei, e.legal_business_name FROM gtm_prime_sub_pairs p
JOIN gtm_sam_entities e ON e.uei = p.sub_uei WHERE p.prime_uei = 'XXX';

-- INFLECTION: high-liability NAICS added to SAM profile in the latest window, w/ sizing posture
SELECT d.uei, e.legal_business_name, d.new_value AS naics, d.sb_flag_new, d.window_days
FROM sam_master_profile_deltas d JOIN gtm_sam_entities e USING(uei)
WHERE d.field='naics_added' AND d.new_value IN ('236220','541512','561612')
  AND d.to_label='2026_MAY'                     -- latest MEANINGFUL vintage transition
ORDER BY d.new_value;

-- INFLECTION: net-new CAGE (delta) → the exact day it first transacts (FPDS adjacency)
SELECT d.uei, d.new_value AS cage, s.first_action_date, s.obl_sum
FROM sam_master_profile_deltas d
JOIN gtm_fpds_entity_signal_events s
  ON s.uei = d.uei AND s.signal_type='cage_txn' AND s.signal_value = d.new_value
WHERE d.field='cage_code' AND coalesce(d.old_value,'')='' AND d.to_label='2026_MAY';

-- COMBO PORTRAIT: zoom out (family × FY, national) — one view
SELECT * FROM v_family_fy WHERE family = '5413xJ' ORDER BY fy;

-- Zoom in (exact combo × county × FY, with the event/plan dials)
SELECT fy, pop_county_name, sum(obligation) obl,
       avg((subcontracting_plan IN ('C','D','E','F','G','H'))::INT) plan_share,
       avg((award_topology = 'vehicle_order')::INT) task_order_share
FROM txn_events_combo_by_geo
WHERE pop_state = 'VA' AND naics_code = '541330' AND psc_code LIKE 'J%'
GROUP BY 1, 2 ORDER BY 1, obl DESC;

-- Sub-out trend: is rising prime work in a category being subbed out more?
SELECT c.fy, sum(c.obligation) prime_obl,
       sum(s.sub_amount_total) FILTER (WHERE s.prime_award_unique_key IS NOT NULL) subbed_amt,
       count(DISTINCT c.award_key) awards,
       count(DISTINCT s.prime_award_unique_key) subbed_awards
FROM txn_events_combo c
LEFT JOIN award_subout_rollup s ON s.prime_award_unique_key = c.award_key
WHERE c.naics_code LIKE '5413%' AND c.psc_code LIKE 'J%'
GROUP BY 1 ORDER BY 1;

-- POSITION LADDER (agency-lens): firms with an IDV seat whose ordering window is
-- open at snapshot, carrying <agency> orders in a PSC ring — ONE pass, no self-join.
-- NOTE: Navy '1700' is a SUB-agency code (top-tier DoD = '097'); ring on
-- awarding_sub_agency_code. Join gtm_position_orders (17M narrow, open-window
-- only), NOT the 83M award_state — big-side joins saturate the serving box.
SELECT count(DISTINCT p.recipient_uei)
FROM txn_events_combo c
JOIN gtm_position_orders p ON p.contract_award_unique_key = c.award_key
WHERE c.awarding_sub_agency_code = '1700'
  AND (substr(c.psc_code,1,1) IN ('J','N') OR substr(c.psc_code,1,2) = '42');  -- the ring

-- Overseas vs unstated: name the no-US-state bucket (geo map legend / market share)
SELECT coalesce(v.name, 'UNSTATED') AS country,
       count(DISTINCT c.award_key) awards, sum(c.obligation) obl
FROM txn_events_combo c
LEFT JOIN country_vocab v ON v.code = c.pop_country_code
WHERE c.pop_state IS NULL AND c.awarding_agency_code = '1700'
GROUP BY 1 ORDER BY obl DESC;

-- Signature allocation seed: each receiver's top-5 prime PSC lanes with shares —
-- ranks are PRECOMPUTED; floors/top-N are query-time dials
SELECT uei, code, share_24mo, v.psc_name
FROM gtm_prime_code_signature s
LEFT JOIN v_psc_names v ON v.psc_code = s.code
WHERE s.uei IN (/* receiver set */) AND s.code_type = 'psc'
  AND s.rank_24mo <= 5 AND s.obl_24mo > 0;

-- Declaration coverage of a firm set (never count the *_counter/*_string columns)
SELECT code_type, count(DISTINCT uei) declaring_firms
FROM v_sam_declared_codes WHERE uei IN (/* set */) GROUP BY 1;

-- WIN-THEN-BORROW timing trigger: firms with fresh money now, a history of
-- borrowing after winning, and no new filing since the award (the loan window).
-- ALWAYS pre-prune the 108M event stream to the debt-layer UEIs first — the
-- unbounded interval join saturates the serving box; pruned it runs ~5s.
WITH debt_ueis AS (SELECT DISTINCT uei FROM sam_ucc_filings),
evts AS (SELECT t.uei, t.action_date, t.action_type_code, t.obligation
         FROM gtm_txn_events_slim t JOIN debt_ueis USING(uei)),
fresh AS (
  SELECT uei, sum(obligation) new_money, max(action_date) last_award
  FROM evts WHERE action_type_code IN ('A','C','G')
    AND action_date >= current_date - 90
  GROUP BY 1 HAVING sum(obligation) >= 250000),
borrow_hist AS (
  SELECT f.uei, count(*) paired
  FROM sam_ucc_filings f JOIN evts t
    ON t.uei = f.uei AND f.first_filing_date BETWEEN t.action_date AND t.action_date + 90
  WHERE f.filing_class = 'financing' GROUP BY 1 HAVING count(*) >= 2)
SELECT e.legal_business_name, fr.new_money, b.paired, o.has_active_lien
FROM fresh fr JOIN borrow_hist b USING(uei)
JOIN gtm_sam_entities e USING(uei)
LEFT JOIN (SELECT uei, max(has_active_lien) has_active_lien
           FROM sam_ucc_debtor_overlap GROUP BY 1) o USING(uei)
WHERE NOT EXISTS (SELECT 1 FROM sam_ucc_filings x
                  WHERE x.uei = fr.uei AND x.first_filing_date > fr.last_award)
ORDER BY fr.new_money DESC;

-- LENDER BOOK (lender-book cycle 2026-07-17): a lender's full CA/CO debtor
-- book as a pruned probe — never scan ucc_filings_all for this. lender_key =
-- the normalized key from ucc_lenders_all (probe that table by name first).
SELECT count(DISTINCT debtor_key)                                    AS debtors,
       count(DISTINCT uei)                                           AS sam_ueis,
       count(*) FILTER (WHERE filing_class='financing'
                          AND is_active_financing)                   AS active_financings
FROM ucc_lender_filings WHERE lender_key = 'CITY NATIONAL BANK';
-- ...then hydrate the federal slice: DISTINCT uei -> gtm_sam_entities /
-- gtm_entity_award_book / gtm_entity_fy_won / gtm_award_expiry_months.
-- Collateral text (CO) or the co-lender list for specific filings: equality
-- join back to ucc_filings_all ON (ucc_state, filing_id, debtor_key).

-- Enrichment coverage funnel: PDL / LinkedIn / profile coverage of a firm set, one pass
SELECT count(*) firms,
       count(b.pdl_company_id)                       pdl_matched,
       count(p.linkedin_slug)                        with_linkedin,
       count(ic.uei)                                 icypeas_scraped
FROM (SELECT DISTINCT uei FROM gtm_sam_entities WHERE /* population */) f
LEFT JOIN bridge_sam_pdl b USING(uei)
LEFT JOIN pdl_normalized_companies p ON p.pdl_company_id = b.pdl_company_id
LEFT JOIN icypeas_company_scrapes ic USING(uei);

-- LABOR MARKET-VS-FLOOR SPREAD: for an SCA occupation, the statutory floor per
-- county alongside the OEWS state market envelope. Bridge (occ→soc) meets both halves.
SELECT x.occupation_code, o.occupation_title, x.soc_code,
       f.county_name, f.county_fips, f.wage_rate AS floor_hourly, f.fringe,
       w.h_pct25 AS mkt_p25, w.h_median AS mkt_median, w.h_pct75 AS mkt_p75
FROM sca_soc_crosswalk x
JOIN dol_sca_occupations o ON o.occupation_code = x.occupation_code
JOIN v_wd_county_rates f ON f.occupation_code = x.occupation_code
LEFT JOIN soc_state_wage w ON w.soc_code = x.soc_code AND w.prim_state = f.state_code
WHERE x.occupation_code = '23130' AND f.state_code = 'VA';

-- ROLE TEXT → PRICED COMBOS (the pre-call entry hop, labor-pricing cycle).
-- Normalize the free text like the table does (lowercase, punctuation → space),
-- probe alias_norm, take the top-ranked combos with the precomputed
-- expected-labor share of the award dollar. NEVER multiply pct_of_industry
-- yourself — it is PERCENT; category_award_share already divides by 100.
SELECT alias, code_type, code, occupation_title, naics_code, psc_code, rank,
       a_median, loaded_labor_share, category_award_share
FROM v_role_priced_combos
WHERE alias_norm = 'travel rn' AND rank <= 3
ORDER BY category_award_share DESC NULLS LAST LIMIT 25;

-- Ambiguous role text: resolve the alias FIRST, pick the code, then price
SELECT DISTINCT code_type, code, occupation_title, title_source, in_combo_layer
FROM occupation_alias_lookup WHERE alias_norm = 'superintendent'
ORDER BY in_combo_layer DESC;   -- then rank by source_priority

-- EXPECTED LABOR $ for a target's award: award (naics) → the one-join scalar
SELECT naics_code, payroll_share, burden_multiplier, loaded_labor_share,
       payroll_share_level, burden_match_level
FROM naics_labor_share WHERE naics_code = '541512';

-- COUNTY-PRICED STATUTORY FLOOR for a given WD (the recurring Entry-2 shape)
SELECT occupation_code, classification_title, wage_rate, fringe, hw_rate,
       county_name, county_fips
FROM v_wd_county_rates WHERE wd_id = 'XXX' ORDER BY occupation_code;

-- UNION EXPOSURE column for a target list: incumbent workforce unionized + CBA expiry
SELECT e.legal_business_name, c.union_name, c.exp_date, c.is_active
FROM (SELECT DISTINCT uei FROM gtm_entity_code_lanes
       WHERE side='prime' AND code_type='naics' AND code='561720') f
JOIN gtm_sam_entities e USING(uei)
JOIN olms_cba_crosswalk c USING(uei);   -- inner join = union-exposed subset only

-- EQUIPMENT DEMAND × ACTIVE $: national active obligated $ by heavy-iron bucket
-- (the geo product's national roll; add a geo/agency predicate to localize)
SELECT primary_bucket,
       sum(active_obligated)   AS active_obl,
       sum(active_award_ct)    AS active_awards,
       sum(obligated_total)    AS all_obl
FROM v_combo_active_equipment
WHERE in_scope                               -- the heavy-iron slice
GROUP BY 1 ORDER BY active_obl DESC;

-- ONE specific bucket, active $ + how many primes hold a seat (LIST membership)
SELECT sum(active_obligated) active_obl, sum(active_recipients) primes
FROM v_combo_active_equipment
WHERE list_contains(equipment_buckets, 'material_handling_cranes');

-- EQUIPMENT VOCABULARY rollup: distinct phrases + head coverage across in-scope combos
SELECT lower(phrase) AS phrase, count(*) instances,
       count(DISTINCT naics_code || psc_code) combos
FROM v_equipment_needs_phrases WHERE in_scope
GROUP BY 1 ORDER BY instances DESC LIMIT 200;

-- SUPPLY ⋈ DEMAND on the shared PSC taxonomy: shops whose inventory covers a
-- PSC that carries active crane-needing demand (the matchmaking join)
WITH crane_pscs AS (
  SELECT DISTINCT psc_code FROM v_combo_active_equipment
   WHERE list_contains(equipment_buckets, 'material_handling_cranes') AND active_obligated > 0)
SELECT s.domain_norm, s.is_equipment_provider, s.matched_psc_count, s.capability_capture_ratio
FROM v_equipment_supply s, crane_pscs c
WHERE list_contains(s.supported_pscs, c.psc_code)
GROUP BY ALL ORDER BY s.capability_capture_ratio DESC NULLS LAST;
```

**(g) VA exam-demand geography — veteran density × disability recipients, per county.** The exam lane (`621111`×`Q403`) has no real place-of-performance; rank demand by where veterans live. Both VA tables key on `fips` → the same county grain as `txn_events_combo_by_geo`:
```sql
SELECT v.fips, v.county_state, v.veterans_total,
       d.recipients, d.scd_100 AS severe_recipients
FROM va_vetpop_county_total v
LEFT JOIN va_disability_comp_county d
  ON v.fips = d.fips AND d.fiscal_year = 2025
WHERE v.snapshot_year = 2023
ORDER BY d.recipients DESC NULLS LAST;
-- cross to local medical-labor supply: join gtm_sam_entities on physical_state
-- (2-letter) via substr(v.fips,1,2) → sam_county_fips_crosswalk.state_code, or
-- to award geography directly on txn_events_combo_by_geo.pop_county_fips = v.fips.
```

### Growth-lane windows (2026-07-19 cycle)

Firm growth per construction work-lane: recent-vs-baseline month windows are dials at
query time; ALWAYS anchor to the mart's `max(month)` watermark, never `current_date`
(FPDS publication lag — the freshest month is empty, months 2–3 ~half-reported under
DoD's ~90-day embargo; short windows systematically understate and must be labeled).
Per-lane qualification only — never blend a firm's lanes (operator ruling 2026-07-19).

```sql
-- "vertical-building firms whose last 12 months run >= 4x their prior-24 pace,
--  recent window $1M-$1B" (annualize: recent/12 vs baseline/24 -> factor 0.5)
WITH edge AS (SELECT max(month) AS mx FROM gtm_construction_lane_months),
w AS (
  SELECT uei,
         sum(obligation_sum) FILTER (WHERE month >  (SELECT mx FROM edge) - INTERVAL '12 months') AS recent,
         sum(obligation_sum) FILTER (WHERE month <= (SELECT mx FROM edge) - INTERVAL '12 months'
                                 AND month >  (SELECT mx FROM edge) - INTERVAL '36 months') AS baseline,
         sum(new_award_obligation_sum) FILTER (WHERE month > (SELECT mx FROM edge) - INTERVAL '12 months') AS recent_new_work
  FROM gtm_construction_lane_months
  WHERE lane = 'construction-vertical-building'
    AND month > (SELECT mx FROM edge) - INTERVAL '36 months'
  GROUP BY 1
)
SELECT count(*) FROM w
WHERE recent >= 1e6 AND recent <= 1e9 AND baseline > 0 AND recent >= 4 * baseline * 0.5
-- lane prunes (sort key); per-firm monthly series = the mart rows themselves
-- (sparklines); new entrants = recent > 0 AND baseline IS NULL;
-- "new work or mods" = new_award_obligation_sum / obligation_sum;
-- "one buyer or many" = n_agencies. Whole-universe (no lane) growth: use
-- txn_recipient_month_by_type (ms-class short windows) — this mart is
-- lane-scoped only.
```

### Market-composition legs (2026-07-17 cycle)

Composed market = AND of predicate legs intersected on uei, ONE statement. The three
entity-grain marts make the common legs ms-class:

```sql
-- "Won FY23-25 > $5M AND active committed book AND based in CO AND 11-200 people"
WITH won AS (
  SELECT uei, sum(won_obl) AS won
  FROM gtm_entity_fy_won WHERE fy IN (2023, 2024, 2025)
  GROUP BY 1 HAVING sum(won_obl) > 5e6)
SELECT count(*)
FROM won
JOIN gtm_entity_award_book  bk USING(uei)
JOIN gtm_sam_entities       e  USING(uei)
JOIN gtm_entity_firmographics f USING(uei)
WHERE bk.committed_award_ct > 0
  AND e.physical_state = 'CO'
  AND f.employee_size_range IN ('11-50', '51-200');

-- set-aside WINNERS (not merely certified): actually won 8(a) work in the window
SELECT uei, sum(won_obl_8a) FROM gtm_entity_fy_won
WHERE fy IN (2024, 2025) GROUP BY 1 HAVING sum(won_obl_8a) > 0;

-- award-size texture: firms whose typical committed award is $1-10M
SELECT uei FROM gtm_entity_award_book
WHERE committed_award_median BETWEEN 1e6 AND 1e7 AND committed_award_ct >= 3;
```

### Award-key point-reads — the award_key_pfx pruning leg (2026-07-21 cycle)

Per-award drawer reads (anchor row, FY ledger, recent actions, PoP) hit the four award-key
copies: `prime_award_state_by_key`, `txn_events_combo_by_award`, `txn_rows_by_award`,
`award_pop_centroids_by_key`. **Every probe MUST carry the `award_key_pfx` leg** or it
full-scans:

```sql
-- the award drawer's FY ledger, ms-class (was 11.5s)
SELECT fy, sum(obligation) FROM txn_events_combo_by_award
WHERE award_key_pfx = substr('CONT_AWD_N0001922F2503_9700_N0001919G0029_9700', 10, 12)
  AND award_key = 'CONT_AWD_N0001922F2503_9700_N0001919G0029_9700'
GROUP BY 1 ORDER BY 1;
```

**Why the pfx leg is load-bearing (durable substrate lesson):** every FPDS award key opens the
literal 9-char prefix `CONT_AWD_` or `CONT_IDV_`, and DuckDB's string min/max zone-map statistics
**truncate to an 8-byte prefix**. So a table sorted by the full award key alone has an *identical*
zone-map min/max on every row group (`CONT_AWD`) → the scan cannot prune → it reads all 108M/83M
rows (measured 8.6–11.5s, ∝ row count). The fix: materialize `award_key_pfx = substr(key, 10, 12)`
(the PIID region, selective in its first bytes) as the **leading** sort key; probes filter on it
first, then the full key for exactness. This applies to ANY future table sorted by a key with a
long shared prefix (award keys, `CONT_*` PIIDs, prefixed transaction ids). `award_subout_rollup` is
exempt only because it is a 197k-row aggregate where a full scan is already cheap.

### Audience-spec counts (2026-07-15 cycle)

"How many entities fit: <geo> × <$ window> × <designations>" — ONE table, no joins:

```sql
SELECT COUNT(*), ROUND(SUM(total_amt_24mo)/1e9,1) AS bn
FROM gtm_audience_entities
WHERE primary_pop_state = 'TX' AND total_amt_24mo >= 1000000;   -- 43 ms
```

Laser-in clause — "entities with N actions of type X in window Y" — use the
type/month-sorted copy, NOT the uei-sorted base:

```sql
SELECT COUNT(DISTINCT uei) FROM txn_recipient_month_by_type
WHERE action_type_code = 'C' AND month >= DATE '2026-04-01';    -- 9.6 ms (209x vs base)
```

### Pricing-terms cycle (2026-07-15)

**(h) Event verb × PoP state — the phrase layer's former refusal.** "Entities that had an
option year exercised in Virginia this quarter" prunes on (type, state):

```sql
SELECT uei, SUM(n_actions) AS actions, SUM(obligation_sum) AS obl
FROM txn_recipient_month_pop
WHERE action_type_code = 'G' AND pop_state = 'VA' AND month >= DATE '2026-04-01'
GROUP BY 1 ORDER BY obl DESC;
```

**(i) Action-type semantics — phrase → codes via the vocab, never hardcode.**
"more work" / "funding released" aggregations read the flags:

```sql
SELECT t.uei, SUM(t.obligation_sum) AS more_work_obl
FROM txn_recipient_month_by_type t
JOIN action_type_vocab v ON v.action_type_code = t.action_type_code
WHERE v.is_more_work AND t.month >= DATE '2026-01-01'
GROUP BY 1;
-- NULL action_type_code on facts = the base award itself (vocab's NULL row).
```

**(j) Cash-stress shape — pricing × financing × size, single-table (never join plan_state).**
Pricing latest-state is denormalized onto award_state (2026-07-16); entity-level mix is
pre-aggregated:

```sql
-- award grain: active FFP, unfinanced, small-determined — one pruned read
SELECT recipient_uei, count(*) AS ffp_awards, sum(life_to_date_obligated) AS obl
FROM usaspending_fpds_prime_award_state
WHERE latest_pricing_code = 'J'
  AND coalesce(latest_financing_code, 'Z') IN ('Z', 'NOT APPLICABLE')
  AND latest_business_size = 'S'
  AND current_end_date >= current_date AND is_terminated = FALSE
GROUP BY 1;

-- entity grain: "primes whose active book is predominantly FFP-unfinanced" — ms-class
SELECT uei, active_obl, active_ffp_unfinanced_share
FROM gtm_entity_pricing_mix
WHERE active_ffp_unfinanced_share >= 0.70 AND active_obl >= 1e6;
```

**(k) Farm-out share — propensity to sub out a shape of work, pre-divided:**

```sql
SELECT uei, naics_code, psc_code, farmout_share_60mo, farmout_amt_60mo, prime_obl_60mo
FROM gtm_prime_farmout_combo_lanes
WHERE farmout_share_60mo >= 0.30 AND prime_obl_60mo >= 1e6;
```

**(l) Staffing absorption (directional) — implied labor the entity can't absorb W2:**

```sql
SELECT * FROM v_staffing_absorption
WHERE naics_code = '561612' AND implied_fte_per_year_60mo > 50
  AND coalesce(employees_on_linkedin, 0) < implied_fte_per_year_60mo / 2
  AND coalesce(farmout_share_60mo, 0) < 0.10;
-- methodology (wage divisor, annualization) lives in the view SQL — a query-time dial
```

**(m) Farm-out by recipient shape — "routes X-shaped work to Y-shaped subs".** The
recipient's identity comes from its own record (`awarded_prime_contracts_in_code` = the
sub's own prime-award history). Filter ONE `recipient_code_source` — lenses overlap:

```sql
-- primes routing ≥30% of their 541712 work to subs who themselves prime in 541330
SELECT prime_awardee_uei, subaward_amt_total, subout_rate_lifetime, share_of_context_subout
FROM gtm_prime_subout_by_code               -- recipient-anchored sort copy (25 ms)
WHERE recipient_code_source = 'awarded_prime_contracts_in_code'
  AND recipient_code_type = 'naics' AND recipient_code = '541330'
  AND context_code = '541712' AND subout_rate_lifetime >= 0.30;
-- prime-anchored portrait: same columns on gtm_prime_subout_by_recipient_code (uei sort)
```

-- Installations near a work territory (military_installations, 831 rows):
-- active US installations within ~40km of a point (deg approx; fine at overlay grain)
SELECT site_name, component, state_code
FROM military_installations
WHERE country = 'USA' AND operational_status = 'act'
  AND abs(latitude - 32.72) < 0.36 AND abs(longitude - (-117.16)) < 0.44;

### Commercial-software membership (2026-07-20 compliance-friction cycle)

**(n) "Is this federal entity a commercial software vendor" — native, no Lance hand-join.**
`us_software_companies` is the 173k-domain US software/SaaS universe; bridge to any federal
population through `gtm_sam_entities.normalized_domain`. This replaces the sidecar→Lance→Python
cross-system intersect used across the SBIR/GWAC/compliance analyses (measured 162 ms warm):

```sql
-- commercial-software vendors newly subbing under Tier-1 primes, with POC (SecurityPal shape)
SELECT g.sub_uei, g.sub_name, u.domain, u.employee_size_range, g.n_teaming_primes, g.poc_full_name
FROM govcon_subawardee_profiles g
JOIN gtm_sam_entities s     ON s.uei = g.sub_uei
JOIN us_software_companies u ON u.domain = s.normalized_domain   -- the membership test
WHERE g.n_teaming_primes >= 3 AND g.poc_available
  AND g.teaming_last_action_date >= DATE '2024-01-20';
-- domain resolves for ~73% of registrants (gtm_sam_entities.normalized_domain coverage) —
-- the join is a FLOOR on true membership; disclose it. ~6% of the universe is non-US
-- (add u.country='United States' to force US-only).
```

## 5. Performance model

Tables are physically clustered by their sort key — filter on it and DuckDB reads only matching
row groups (a `uei=` probe on the 108M-row table is ~ms). Filters off the sort key still work
(full-column scan, seconds-class on the giants). First touch of a cold table pays Render disk
page-cache (one-time seconds); repeats are fast. Queries serialize — keep single statements
tight; you have `elapsed_ms` in every response.

## 6. Caveats

1. **Snapshot, not live.** `/healthz` → `artifact` stamp; `_sidecar_meta`/`_sidecar_manifest`
   carry the build time and per-table pinned Lance versions. Rebuild:
   `modal run pipelines/query_sidecar/build_query_sidecar.py::run` (auto-refreshes serving).
   Serving instances converge on the newest artifact within ~60 s of a publish (LATEST
   poll); across that window, cross-statement totals can mix states — pin
   `require_artifact` (§1) when numbers must reconcile.
2. **Read-only by construction** — write/DDL statements are rejected; don't try.
3. **Not everything is here** (§2). If a needed table/column is absent, say so rather than
   silently falling back to a spine scan — absence is signal for the next manifest revision.
4. SQL parse quirk: a bare column alias immediately after a closing `)` fails — write
   `) AS alias`. Sub-PoP county codes are 3-digit-in-state; prime PoP county fips are
   5-digit — stitch with the state FIPS before joining the two.
5. `gtm_txn_events_slim` renames: `obligation` (not federal_action_obligation), `psc_code`
   (not product_or_service_code), `uei` (not recipient_uei).

## 7. Gap reporting (demand capture — MANDATORY when you fall back)

Whenever you answer a data question WITHOUT the sidecar (Lance scan, pylance probe,
catalyst endpoint, DuckDB-over-Lance, or a degraded answer), append an entry AS YOU GO to
`docs/sidecar_gaps/SIDECAR_GAP_REPORT_<YYYY-MM-DD>-<short-slug>.md` (one file per
session/topic; header = date + `/healthz` artifact stamp + topic). Five fields, exact:

1. **Intent** — the question in plain English, as asked.
2. **Why not the sidecar** — `missing table` / `missing column(s)` / `wrong grain` /
   `missing sort (too slow unpruned)` / `freshness required` / `didn't know it was there`
   — plus the specific dataset/columns involved.
3. **What I ran instead** — exact query/scan, dataset hit, ONLY the columns actually needed.
4. **Cost** — wall time; rows scanned vs returned.
5. **Recurrence** — one-off vs recurring shape (honest).

Footer: rank gaps by recurrence × cost. Report demand only — no solutions. The promotion
cycle gates every entry (promote / routing fix / correctly-on-Lance) and archives the file
to `docs/sidecar_gaps/processed/` with a Disposition. This is how the artifact grows —
silent fallbacks are lost demand signal.
