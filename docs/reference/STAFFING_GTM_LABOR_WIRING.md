# Staffing-GTM Labor Wiring — Canonical State & Angle Catalog

**Status:** canonical as of 2026-07-11 UTC · supersedes all prior agent briefs on this thesis
(any doc claiming "occupation-grain reads are Lance scans until the promotion lands" is stale —
that promotion shipped as [#1117](https://github.com/bencrane/core-x/pull/1117)).

**Companions:**
[`QUERY_SIDECAR_AGENT_GUIDE.md`](QUERY_SIDECAR_AGENT_GUIDE.md) (authoritative 78-table warm catalog) ·
[`LABOR_x_GOVCON_CROSSWALK_GTM.md`](LABOR_x_GOVCON_CROSSWALK_GTM.md) (join graph + recipes) ·
[`LABOR_MARKET_SUBSTRATE.md`](LABOR_MARKET_SUBSTRATE.md) (L0–L4 profile model) ·
[`LABOR_SHARE_OF_REVENUE_STACK.md`](LABOR_SHARE_OF_REVENUE_STACK.md) (share/burden calibration)

## The thesis in one line

A staffing agency's pre-call form (roles staffed, geos, clearances, labor holds, time-to-fill)
maps **backward** through the labor chain to a specific, timed, priced list of federal awards —
and the kinetic layer (wins, mods, funding released, expiry) says **when** to act on each one.

```
pre-call answers → SOC/SCA codes → (naics, psc) combos → awards + primes + subs
                                          ↕
                     price it: statutory floor (county) · market wage (state)
                               · burden multiplier · labor share of award $
                                          ↕
                     time it: action_type C wins · mod_delta Σ$ · consumed_pct
                              · days_to_expiry · CBA expiry (§4(c))
```

## Wiring state — surface by surface (verified 2026-07-11)

### Kinetic signal — WHEN to act

| Dataset | Surface | Role |
|---|---|---|
| `gtm_txn_events_slim` (uei-sorted) | **warm** | "just won / new funding" — action_type C, windowed Σ$ per firm |
| `gtm_position_orders` | **warm** | order grain, pre-pruned UEI sets (never build-side the 83M table) |
| `prime_award_state` / `usaspending_fpds_prime_award_state` | Lance (L2) | `consumed_pct`, `days_to_expiry`, `life_to_date_obligated`, `is_terminated` — starvation/expiry timing |
| `mod_delta` | Lance (L2) | `delta_federal_action_obligation` — supplemental / funding-released events |
| `usaspending_subaward_canonical` | Lance | prime→sub labor cascade (who performs beneath the prime) |
| `gtm_sam_entities` + rollup | **warm** | entity hydration, HQ state (≠ PoP) |

### Labor wiring — WHAT work → WHO → AT WHAT PRICE

All of the occupation-grain layer is **warm** since #1117 (gap-pass-6 promotion; measured
Lance→serving deltas: county-priced floor ~60s → 10.5 ms, union exposure ~90s → 1.7 ms,
ranked SCA↔SOC ~20s → 2.8 ms).

| Dataset | Surface | Role |
|---|---|---|
| `naics_psc_labor_profile_categories` | **warm** | the accelerator: ranked SOC/SCA per combo, wage medians, growth |
| `naics_psc_labor_dim` / `naics_psc_labor_profile` | **warm** | `is_labor_play`, rank-1 SOC/SCA, `work_summary` language |
| `sca_soc_crosswalk` | **warm** | SCA↔SOC bridge (tier/confidence/dominance) + own name layer |
| `dol_sca_occupations` | **warm** | SCA taxonomy — title, definition, family |
| `soc_state_wage` | **warm** | market wage percentiles by state |
| `sam_wd_rates_structured` | **warm** | statutory floor: wage + fringe per WD × occupation × classification |
| `sam_wd_county_coverage` + `sam_county_fips_crosswalk` | **warm** | binds the floor to county FIPS |
| `v_wd_county_rates` (view) | **warm** | the county-priced floor in one SELECT |
| `olms_cba_crosswalk` | **warm** | UEI → union identity + CBA expiry (§4(c) successorship) |
| `bls_oews_2025` | Lance | industry staffing pattern (roles per 1,000) |
| `occupation_alias_lookup` | Lance | **the entry hop** (66.9k aliases, 2026-07-14): free-text role names → SOC/SCA via normalized `alias_norm` probe (O*NET primary/reported/alternate titles + SCA taxonomy, parenthetical variants split, SCA rows carry the bridged `soc_code` inline, `in_combo_layer` = reachable through the ranked combo profiles) |

### Pricing calibration — the labor-share/burden stack (Lance, landed #1118–#1120)

| Dataset | Surface | Role |
|---|---|---|
| `bls_ecec_costs` | Lance | **full ECEC universe** (627,050 rows, 7,998 series, history from 2004; #1120) — comp/wage/benefit components per ownership × industry × occupation group × size/union/region subcell |
| `bls_ecec_burden` | Lance | 321 burden multipliers (`total_comp / wages_salaries`) at the widened grain; economy-private 1.4294 @ 2026 Q01 |
| `census_susb_naics_payroll_receipts` | Lance | `payroll_share = payroll / receipts` per NAICS × size class (economy 0.1763) |
| `bea_industry_value_added` | Lance | comp-share-of-output cross-check (economy 0.2966) |
| `bea_naics_concordance` | Lance | BEA line ↔ 2017 NAICS bridge (#1119) |
| `naics_labor_share` | Lance | **the composed dim** (1,133 rows, 1/6-digit NAICS, 2026-07-14): `loaded_labor_share = payroll_share × burden` + BEA cross-check + provenance flags — closes `expected labor $ = award_$ × loaded_labor_share × pct_of_industry/100` in one join (mix column is PERCENT) |

Rebuild commands and gate values: [`LABOR_SHARE_OF_REVENUE_STACK.md`](LABOR_SHARE_OF_REVENUE_STACK.md).
`labor_share_ingest --stream ecec` is **superseded** — do not re-run (would clobber the full
universe with the old 48-series slice); costs/burden rebuild via
`pipelines/reference/ecec_full_universe.py`.

### Demand & people edges (parked — the honest gaps)

| Dataset | Surface | State |
|---|---|---|
| `govcon_labor_demand` (20.6k) | Lance, **parked** | solicitation-linked headcount / clearance_level / wage_floor. **LLM-extracted — every read must gate on `confidence`.** Directional signal, not a quotable number. |
| `sam_labor_poc_people` (29.5k) | Lance, **parked** | uei-keyed staffing POC people (overlaps `gtm_sam_people`/`sam_pocs`) |

Both sit on the promoted subgraph's edge; promote when the pre-call workflow makes their
question shapes recur (per gap-pass-6 disposition,
`docs/sidecar_gaps/processed/SIDECAR_GAP_REPORT_2026-07-11-labor-occupation-grain.md`).

## The pre-call reverse map

| Pre-call answer | Maps through | Datasets |
|---|---|---|
| Roles they staff | role names → `occupation_alias_lookup` (alias_norm probe) → SOC/SCA → reverse-lookup combos → awards | `occupation_alias_lookup` → `naics_psc_labor_profile_categories` (reverse) → kinetic layer |
| Geos served / national | PoP county FIPS / HQ state | `sam_county_fips_crosswalk`, spine `pop_county_fips`, entity HQ |
| Clearances | extracted solicitation demand (confidence-gated) | `govcon_labor_demand` |
| Labor holds / bench | occupation inventory vs ranked combo demand | combo layer + `soc_state_wage` |
| Time-to-fill | matched against award timing | `days_to_expiry`, `consumed_pct`, pop_end, CBA expiry |
| Contactable target | UEI → staffing POC | `sam_labor_poc_people` (parked) / `gtm_sam_people` (warm) |

## The GTM angle catalog — what we can SHOW a staffing agency

1. **"Who just won work that needs your people"** (roles × kinetic). Firm's SOC/SCA codes →
   reverse-lookup combos where they rank top-N → `gtm_txn_events_slim` action_type C, last 90
   days. *"17 primes won $214M across 22 awards last quarter whose labor profile ranks your
   exact occupations #1–2 — sorted by Σ$."*
2. **"Where the ramp is happening"** (geos × mods). Firm's counties → spine `pop_county_fips` →
   `mod_delta` positive deltas. *"Supplemental funding released against orders in your 6 counties
   this month — new work someone must staff now."*
3. **"The margin math, done for them"** (price × floor × burden). Per occupation × county:
   statutory floor + fringe (`v_wd_county_rates`, 10.5 ms) vs market percentiles
   (`soc_state_wage`) vs fully-loaded cost via ECEC burden by occupation group and size class.
   A pricing conversation, not a cold call.
4. **"The expiry/starvation clock"** (time-to-fill × timing). Firm's fill window vs
   `days_to_expiry` / `consumed_pct`. *"These 9 awards hit 85% consumed within your 45-day fill
   window — the incumbent's recompete staffing crunch is your entry."*
5. **"The §4(c) successorship wedge"** (union). `olms_cba_crosswalk`: matched awards where the
   incumbent is unionized with CBA expiring near pop_end — successorship reshapes pricing and
   creates displacement/capture moments. Warm, 1.7 ms per UEI list.
6. **"The sub-tier cascade."** `usaspending_subaward_canonical`: the sub doing the labor is often
   the better staffing customer than the prime. Cross with angle 1's role match.
7. **"Cleared headcount demand"** (clearances). `govcon_labor_demand` vs the firm's clearance
   inventory — weakest link today (Lance, parked, LLM-extracted, confidence-gated).

Angles 1–6 are wired end-to-end and warm where it matters. Angle 7 is directional pending
promotion.

## Remaining deltas to fully closed-loop

1. ~~**Reverse-map entry hop**~~ — **CLOSED 2026-07-14**: landed as `occupation_alias_lookup`
   (66,878 rows; `pipelines/reference/materialize_occupation_alias_lookup.py`). Normalized
   alias probe → SOC/SCA; verified "travel rn" → 29-1141 → 213 ranked combos, priced via
   `naics_labor_share`. Coverage: 664/674 combo SOCs + 320/320 combo SCAs alias-reachable
   (the 10 residuals are OEWS broad-group aggregate codes O*NET's detailed taxonomy lacks).
   Matching doctrine: exact `alias_norm` probe → token/LIKE → fuzzy/LLM last; 8,017 aliases
   map to >1 code — rank by `title_source` priority then `in_combo_layer`.
2. ~~**Composed `naics_labor_share` dim**~~ — **CLOSED 2026-07-14**: landed as
   `naics_labor_share` via `pipelines/reference/materialize_naics_labor_share.py`
   (details + anchors in `LABOR_SHARE_OF_REVENUE_STACK.md`). The identity closes as
   `expected labor $ by category = award_$ × loaded_labor_share × pct_of_industry/100`
   (the categories mix column is percent, not fraction).
3. **Promotions on recurrence** — `govcon_labor_demand`, `sam_labor_poc_people`, and the ECEC
   burden layer if the pricing lens recurs warm (file gap entries via `/sidecar-gaps`).

## Read discipline

- Analytical questions on this graph go to the **query-sidecar first** (`sidecar-query` skill);
  Lance scans only for the surfaces marked Lance above (L2 award-state, mod_delta, subawards,
  OEWS, ECEC/labor-share stack, parked tables).
- Never build-side the 83M order table — probe `gtm_position_orders` with pre-pruned UEI sets.
- Any clearance/headcount claim sourced from `govcon_labor_demand` carries its `confidence`
  gate forward into the output.

## Provenance (landed PRs)

| PR | Delivered |
|---|---|
| [#1110](https://github.com/bencrane/core-x/pull/1110) | `sam_wd_rates_structured` — parsed WD floor rates |
| [#1111](https://github.com/bencrane/core-x/pull/1111) | WD county coverage + FIPS crosswalk |
| [#1117](https://github.com/bencrane/core-x/pull/1117) | occupation-grain layer promoted warm (71→78 tables, `v_wd_county_rates`) |
| [#1118](https://github.com/bencrane/core-x/pull/1118) | labor-share stack (SUSB + BEA + ECEC slice) |
| [#1119](https://github.com/bencrane/core-x/pull/1119) | BEA↔NAICS concordance |
| [#1120](https://github.com/bencrane/core-x/pull/1120) | ECEC full universe (627k costs / 321 burden) |
