# Phase 0 — Sidecar Mart Manifest (frozen)

Ground-truthed 2026-07-08 from four evidence sources, live systems only:
1. **Usage** — `ops.map_query_runs` (84 phrase-lane queries, all ≤30d: awards 31 · company 18 · active 18 · auto 8 · winners 8 · contracts 1).
2. **Live surface trace** — all 27 catalyst routes in `apps/catalyst_api/main.py`, per-route dataset reads resolved through the store modules.
3. **Supersession trace** — `legacy: true` flags (main.py:549), market_registry v6, freeze-doc/pair-mart handoffs, commit history (#1022–#1078).
4. **Live R2 probe** — all 50 candidates: rows, cols, indices (probe script + results archived in session scratchpad; row counts below are the probed values).

Operator directives baked in: **stale consumers are left as-is — nothing here bends to them.** Superseded marts are excluded from the sidecar even where still live-served.

---

## The decisive facts

- **`/phrase` = `POST /api/v1/market/phrase`** — closed-grammar (zero-LLM) compiler → market-query plan, called by the rare-structure-hq BFF (backs the front-end `/ask`). Its substrate is the **market grains**, not the 5 map marts.
- **The 5 map serving marts are officially legacy** (`legacy: true`, main.py:549; workbench hides their tabs). They keep serving the edge_api map-ask decoders untouched. They are **excluded** from the sidecar.
- **The market grains are the current generation:** `entities` (behavior_rollup + code_lanes + sam_entities + geo + the two inferred-code projections), `prime_awards` (`usaspending_fpds_prime_award_state`), `transactions` (`usaspending_fpds_canonical_txn`).
- **The Cycle B phrase-precompute marts are built but deliberately unwired** ("marts only — NO serving/routing change"): `gtm_txn_events_slim`, `gtm_txn_recipient_month_rollup`, `gtm_award_recipient_rollup`, `gtm_award_expiry_months` (+ `gtm_prime_pop_lanes`, `gtm_naics_psc_pairs`). **The sidecar is their first serving lane.**
- The txn-grain projection problem is pre-solved: **`gtm_txn_events_slim` (107.9M × 13) is the export source** for transactions-grain — never the 392-col canonical.

## Tier A — Sidecar v1 core (export now, sorted)

The market-grain scalar substrate + vocab. Small; exports in minutes.

| Dataset | Rows | Grain | Sort key |
|---|--:|---|---|
| `gtm_entity_behavior_rollup` | 261,789 | 1/uei | `uei` |
| `gtm_sam_entities` | 2,025,707 | 1/uei | `uei` |
| `gtm_entity_code_lanes` | 1,672,844 | 1/(uei, side, code_type, code) | `uei, code` |
| `gtm_entity_geo` | 1,452,430 | 1/uei | `uei` |
| `gtm_naics_psc_pairs` | 320,846 | 1/(naics, psc) | `naics_code, psc_code` |
| `naics_reference` | 2,129 | 1/code | `naics_code` |
| `psc_reference` | 6,108 | 1/(code, period) | `psc_code` |

## Tier B — Cycle B rollups (export now — the sidecar is their intended serving lane)

| Dataset | Rows | Grain | Sort key |
|---|--:|---|---|
| `gtm_txn_events_slim` | 107,948,116 | 1/FPDS action | `uei, action_date` |
| `gtm_txn_recipient_month_rollup` | 34,080,799 | uei × action_type × plan_class × month | `uei` |
| `gtm_award_recipient_rollup` | 6,301,649 | uei × naics × psc × agency | `uei` |
| `gtm_award_expiry_months` | 221,444 | uei × end_month | `uei, end_month` |
| `gtm_prime_pop_lanes` | 547,379 | 1/(uei, pop_state, county) | `uei` |

## Tier C — Giant grains (benchmark-gated: include, project, or leave on Lance BTREE)

Phase 2 decides each on `EXPLAIN ANALYZE` + wall clock. Projections mandatory if included.

| Dataset | Rows | Note | Sort key if included |
|---|--:|---|---|
| `usaspending_fpds_prime_award_state` | 82,868,654 | prime_awards grain (43 cols — exportable whole) | `recipient_uei` |
| `gtm_entity_inferred_primeable_codes` | 263,366,277 | inference projection; phrase pseudo-fields | `uei` |
| `gtm_entity_inferred_subbable_codes` | 159,518,051 | mirror | `uei` |
| `gtm_subaward_recipient_code_evidence` | 92,306,700 | drill-down evidence; cube source | `prime_awardee_uei` |
| `usaspending_fpds_canonical_txn` | 107,962,341 | **do not export** — `gtm_txn_events_slim` is its serving projection | — |
| `usaspending_award_canonical` | 30,697,295 | 393 cols — export only a small **agency-vocab derivation**, not the fact | — |
| `usaspending_award_pop_centroids` | 30,697,295 | pre-joined into `gtm_open_awards`; include only if radius math moves in | `generated_unique_award_id` |

## Tier D — Recipe/relationship substrate (export now; powers sub-universe, subout, teaming, people)

| Dataset | Rows | Sort key |
|---|--:|---|
| `gtm_prime_sub_pairs` | 268,562 | `prime_uei` (+ 2nd copy `sub_uei` — duplication free at this size) |
| `gtm_sub_universe_pairs` | 29,605 | `target_uei` |
| `gtm_sub_universe_targets` | 1 | — (see anomaly note) |
| `gtm_prime_combo_lanes` | 5,116,397 | `uei` |
| `gtm_sub_combo_lanes` | 339,485 | `uei` |
| `gtm_prime_farmout_combo_lanes` | 37,569 | `uei` |
| `gtm_prime_vehicle_lanes` | 16,128 | `uei` |
| `gtm_open_awards` | 163,061 | `recipient_uei` |
| `gtm_prime_demand_events` | 11,339,168 | `uei` |
| `gtm_primes_by_recipient_code` | 1,720,331 | `recipient_code` |
| `gtm_prime_subout_by_recipient_code` | 11,844,606 | `prime_awardee_uei` |
| `gtm_subbed_under_to_primed_in_cooccurrence` | 589,260 | `subbed_under_code` |
| `gtm_sub_profiles` | 105,189 | `uei` |
| `govcon_subawardee_profiles` | 25,450 | `sub_uei` |
| `usaspending_subaward_canonical` | 1,315,680 | **projected** (258→~40 cols) · `prime_awardee_uei` (+ copy `subawardee_uei`) |
| `federal_sites_lance` | 300,414 | `state_code, zip5` |
| `firmographics_blitz` | 255,418 | `domain_norm` |
| `gtm_sam_people` | 2,252,385 | `uei` |
| `gtm_sam_person_contactability` | 152,447 | `sam_person_id` |
| `sam_pocs` | 8,065,679 | `uei` |
| `sam_master_entities` | 1,541,566 | `uei` |
| `people_canonical` | 131,545 | `canonical_person_id` |

## Excluded — superseded / legacy (left as-is per operator directive)

| Dataset | Superseded by |
|---|---|
| `usaspending_winners_map_serving`, `firmographics_company_map_serving`, `usaspending_awards_map_serving`, `usaspending_contracts_map_serving`, `govcon_active_awards_map_serving` | market grains (entities.v6 / transactions / prime_awards.v2); still serve the edge_api map-ask decoders — untouched |
| `contractor_award_summary` | `gtm_entity_behavior_rollup` (+ `gtm_award_recipient_rollup` for lane/agency grain) |
| `capability_profile` | code_lanes + the two inferred-code projections + behavior_rollup posture |
| `entity_profile_gold` | behavior_rollup v2 (post PoP-fix #1024/#1025) + `gtm_sam_entities` identity |
| `entity_award_lines_gold` | flagged legacy but **only** served path for dossier line items today — successor pending; excluded from sidecar, replacement owns it |
| `gtm_sub_universe_blobs` | dead — v3 pair-grain replaced it; binding is stale code |

## Anomalies & observations (recorded, not blocking)

1. `gtm_sub_universe_targets` probes at **1 row** — recipe has been run for a single target so far; export is trivial either way, wire it as-is.
2. Catalyst's `/healthz` + boot reachability **anchor on `contractor_award_summary`** — a stale mart is the liveness anchor. Non-binding here; belongs to whoever replaces it.
3. Stale config comments in catalyst (`gtm_primes_by_recipient_code` "boot-loads" claim; centroids "LEFT-joined" claim) — code disagrees; recorded for that repo's owner.
4. `usaspending_award_search` (line-item read behind `?awards=N`) and `person_source_platforms` are live bindings outside this manifest's scope — profile plumbing, not query substrate.

## Sizing (probed rows, v1 = A+B+D)

~180M rows v1 (dominated by `gtm_txn_events_slim` 108M + `gtm_txn_recipient_month_rollup` 34M + `gtm_prime_demand_events` 11M + `gtm_prime_subout_by_recipient_code` 12M); narrow schemas throughout → estimated 10–25 GB `.duckdb` file. Tier C giants add ~500M rows and are gated on the Phase 2 benchmark.

## Hand-off to Phase 1 (Modal export builder)

Inputs frozen by this manifest: dataset list + sort keys + projections (subaward_canonical ~40 cols; award_canonical → agency vocab only). Builder requirements unchanged from the phased plan: idempotent, ledgered, per-mart row-count parity vs the source Lance version, recipe views baked in (phrase grammar vocab + sub-universe/subout shapes), blue-green artifact swap.
