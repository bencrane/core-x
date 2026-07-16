# SIDECAR GAP REPORT — 2026-07-15 — action-types-pricing-staffing

- **Date:** 2026-07-15
- **Sidecar artifact:** `query-sidecar/query_sidecar_20260715T215456Z.duckdb` (built 2026-07-15T21:54:56Z, 91 tables, ready)
- **Session topic:** FPDS action-type (DEC codes) semantics + query-phrase layer; labor/equipment combo-emission layers review; sub-out propensity; staffing-out inference; payment terms / contract pricing type.
- **Session note:** most questions WERE served warm by the sidecar (action-type distincts, labor profile, equipment needs, farmout lanes, labor share — all sub-second to ~5 s). The entries below are the shapes that could NOT be answered from serving tables, or required authored/model-knowledge layers outside the sidecar. Operator has declared this set an official priority (explicit demand).

---

## Entry 1 — Action-type vocabulary with query-phrase semantics

1. **Intent** — "What are the distinct action types on prime award transactions, and give me a plain-English/query-phrase layer for each so a rebuilt phrase-driven query search can map natural language ('received additional funding', 'had an option year exercised', 'was terminated for cause') onto `action_type_code`." Follow-on intents in the same session: "which codes aggregate to *more work*" and "which mean *funding released*" — i.e. semantic groupings over the codes.
2. **Why not the sidecar** — `missing table`. No action-type vocabulary/reference table exists (cf. `agency_vocab`, `country_vocab`, `psc_reference`, `naics_reference` precedents). The raw pairs on `txn_rows` are source-messy: 102 distinct `(action_type_code, action_type_description)` tuples across 22 real codes — truncated variants (`...FAR PART 6 APPLI`), null descriptions per code, cross-contaminated code/description pairs. No canonical one-row-per-code layer, no query-phrase column, no semantic grouping (more_work / funding_released / termination / admin) column anywhere.
3. **What I ran instead** — sidecar `SELECT action_type_code, action_type_description, COUNT(*) FROM txn_rows GROUP BY 1,2` (full-table aggregate), then authored the 23-row canonical table (code → canonical description → query phrase → semantic buckets) in-chat as an unversioned artifact.
4. **Cost** — 5.0 s for the GROUP BY over 107.96M rows (fine); the real cost is that the authored vocabulary lives nowhere queryable — every future session re-derives or re-pastes it.
5. **Recurrence** — recurring, structurally: operator stated this feeds the revamped phrase/query-search layer; every phrase-mapped query over transactions needs this lookup.

## Entry 2 — Contract pricing type (and financing indicators) on transactions / entity state

1. **Intent** — "Does the government indicate payment terms on prime awards?" → resolved to: pricing type (FFP / cost-reimbursement / T&M) is the cash-flow-shape signal, plus FPDS financing indicators (progress payments, performance-based payments) and small-business flags → effective payment-terms tier. Immediate analytical shapes wanted: "entities whose active obligations are predominantly FFP" (cash-stress / factoring lens), "FFP × no financing × long PoP × small entity."
2. **Why not the sidecar** — `missing column(s)`. Probed `information_schema.columns` across all 91 tables: no `type_of_contract_pricing*` column anywhere; only `award_type_code` (instrument/vehicle: definitive contract, DO, BPA call, PO), which is not pricing. Financing-arrangement columns likewise absent. Source columns exist on the Lance FPDS canonical (392-col spine: `type_of_contract_pricing_code/_description`, financing/progress-payment fields — names to be probe-verified against `ds.schema` at build time).
3. **What I ran instead** — `information_schema` probe on serving (12 ms) to confirm absence; substantive answer given from model knowledge of FPDS/FAR, not from data. No Lance scan was run — the analytical shapes remain unanswered against real data.
4. **Cost** — probe trivial; the unanswered queries would each require a multi-minute 392-col Lance spine scan today.
5. **Recurrence** — recurring: pricing-type mix is a first-class cash-stress/GTM-lending feature; every underwriting-angle question over the portfolio wants it. Operator explicitly engaged on the mechanics (two follow-up questions on invoicing rhythm and self-financing).

## Entry 3 — Prime farm-out share (% of combo revenue subbed out)

1. **Intent** — "A company's historical likelihood of subbing out certain shapes of work" — as a *ratio*: farmed-out dollars over prime obligations, per `uei × naics × psc`.
2. **Why not the sidecar** — `missing column(s)` / split grain: numerator lives on `gtm_prime_farmout_combo_lanes` (`farmout_amt_*`), denominator on `gtm_prime_combo_lanes` (prime obligations), same `uei, naics_code, psc_code` key, but no pre-materialized share column and no single table carrying both sides.
3. **What I ran instead** — described the two-table join in-chat; the ratio was not actually computed. Any consumer must hand-write a 5.1M × 37.5K join with null-safe denominators each time.
4. **Cost** — none paid this session (question answered structurally, not numerically) — which is itself the gap: the number wasn't cheap enough to just produce.
5. **Recurrence** — recurring: sub-out propensity is a declared input to the revamped query search ("primes that farm out X-shaped work").

## Entry 4 — Staffing absorption gap (implied labor vs. capacity vs. reported farm-out)

1. **Intent** — "Primes don't report staffing arrangements as formal subawards; for certain combos the staffed-out reality is directionally discernible" — e.g. implied FTEs from award dollars vs. the entity's actual headcount vs. reported farm-out; residual = invisible staffed-out labor. Query shape: "primes whose active awards imply labor they can't absorb W2, by SOC family, by geography."
2. **Why not the sidecar** — `missing table` (composition mart). All four inputs are sidecar-resident (`naics_labor_share.loaded_labor_share`, `naics_psc_labor_profile_categories` wage/role structure, `firmographics_blitz`/`pdl_normalized_companies` headcount via `bridge_sam_pdl`, `gtm_prime_farmout_combo_lanes`), but the composed metric (implied FTE, absorption residual, likelihood tier at `uei × naics × psc` or per-award grain) exists nowhere; the multi-way composition is too heavy/fragile to hand-write per session.
3. **What I ran instead** — schema-level reasoning across the four tables in-chat; no numeric answer produced.
4. **Cost** — none paid numerically; the shape is unanswerable interactively today without a hand-built 4-table composition including a 35.4M-row PDL join.
5. **Recurrence** — recurring and operator-prioritized: staffing-agency prospecting and "who actually needs subs despite reporting none" are declared GTM lenses.

## Entry 5 — Macro subcontracted-share of industry output (BEA I-O purchased-services)

1. **Intent** — "% of work 'staffed out' at industry/macro level from government reports" — distinct from labor *intensity* (`naics_labor_share` covers that); this is subcontracted/purchased-services share of output by industry.
2. **Why not the sidecar** — `missing table`, and missing upstream entirely: no BEA input-output / purchased-services ingest exists in the plane (not a sidecar-routing issue; the source was never landed). Ingest-scale work, not a rebuild rider.
3. **What I ran instead** — answered from model knowledge that the asset does not exist; flagged as ingest gap.
4. **Cost** — n/a (no data to scan).
5. **Recurrence** — plausible-recurring as the macro prior behind Entry 4's entity-level residual; unproven beyond this session.

---

## Ranking (recurrence × cost)

1. **Entry 2 — pricing type + financing columns** (recurring; every fallback is a minutes-scale 392-col Lance scan; column-grain rider on scans the build already does).
2. **Entry 1 — action-type vocabulary + query phrases** (recurring, feeds the phrase-search rebuild; tiny table; currently re-derived per session).
3. **Entry 3 — farm-out share** (recurring; column-grain on an existing key).
4. **Entry 4 — staffing absorption gap** (recurring, operator-prioritized; structural — new composed mart).
5. **Entry 5 — BEA purchased-services share** (ingest-scale, demand unproven beyond this session).

Demand only — no proposed solutions.

---

## Disposition (build cycle 2026-07-15, artifact `query_sidecar_20260716T014507Z.duckdb`)

Build: single run, success — 94 tables, 1,303,136,487 rows, 47.23 GiB, all parity gates OK
(ops ledger row `success` 2026-07-16T02:18Z; heavy step `usaspending_fpds_prime_award_state`
208 s, in precedent). Serving hot-swapped; every measurement below is on the live endpoint.

### Build scope block (adjacency sweep, written before the build fired)

**Ships from demand:**
- E2 → `pricing_code`, `financing_code`, `pba_code`, `co_business_size`, `labor_standards_code` on `txn_events_combo` (+ `txn_events_combo_by_geo` inherits); `type_of_contract_pricing_code`/`type_of_contract_pric_desc` on `txn_rows`; `latest_pricing_code`/`latest_financing_code`/`latest_business_size` arg_max riders on `award_plan_state` (award_state Lance carries NO pricing columns — probe-verified; the plan_state scan is the award-grain home).
- E1 → `action_type_vocab` (22 rows: 21 codes + NULL base-award row): empirical majority description FULL-joined with the authored layer — `plain_english`, `family`, `is_more_work` (A,B,D,G,L), `is_funding_released` (C,G). Query phrases ship as data for the phrase-agent to consume.
- E3 → `gtm_prime_farmout_combo_lanes` gains `prime_obl_24mo/60mo/lifetime`, `prime_txns_lifetime`, `farmout_share_24mo/60mo/lifetime` via row-preserving LEFT JOIN to `gtm_prime_combo_lanes` (exact-parity gate kept).
- Operator directive (phrase layer's disclosed refusal) → `txn_recipient_month_pop`: uei × action_type × pop_state × pop_county_fips × month off the local fact, sorted (action_type_code, pop_state, pop_county_fips, month).

**Adjacency riders (one line each):**
- `fpds_code_vocab` (field, code, name): all five new code spaces get name resolution off the same canonical scan — single-scan lateral UNNEST (a 5-leg UNION over the one-shot Arrow stream would silently read an exhausted reader).
- `pop_county_fips` in the month-pop grain: the zoom-in next question rides the same GROUP BY.
- 24/60/lifetime share triplet + denominators on farmout lanes: sibling columns of the demanded ratio.
- `labor_standards_code` on the fact: SCA/DBA exposure is the labor layer's sibling dial, same scan.
- E4 → `v_staffing_absorption` VIEW (not a mart): implied labor $ / v1 FTE estimate / observable headcount (SAM↔PDL bridge) / reported farm-out at uei×naics×psc. Methodology (wage divisor, annualization) deliberately stays query-time-visible — baking a still-moving formula into a materialized mart is the signature-precedent anti-pattern.

**Parked (structural-gated) with rationale:**
- Entity-grain pricing-mix mart ("% of active obligations FFP" per uei): derivable via `award_plan_state ⋈ award_state` at query time; materialization waits for demonstrated recurring demand at that grain.
- Staffing-absorption as a materialized mart: gated pending methodology freeze (wage weighting, window choice, FTE formula).
- E5 (BEA I-O purchased-services share): ingest-scale, not a rebuild rider; demand unproven beyond one session.

### Per-entry verdicts

| Entry | Verdict | Shipped |
|---|---|---|
| E1 action-type vocabulary | **Promoted** | `action_type_vocab` table (empirical + authored layers) |
| E2 pricing type + financing | **Promoted** (column-grain) | 5 fact dials, txn_rows pair, 3 award-grain arg_max riders, `fpds_code_vocab` |
| E3 farm-out share | **Promoted** (column-grain) | denominators + 3 share columns on the lanes |
| E4 staffing absorption | **Promoted as view** | `v_staffing_absorption`; mart stays gated |
| E5 BEA I-O share | **Correctly-absent** | ingest-scale; documented, no build |
| Screenshot directive (PoP on events) | **Promoted** (structural, operator-directed) | `txn_recipient_month_pop` |

### Measured deltas (before → after)

| Shape | Before | After (measured on serving) |
|---|---|---|
| E1 action-type semantics | 5.0 s GROUP BY over 108M + unversioned in-chat authoring, re-derived per session | `SELECT * FROM action_type_vocab` — **3.5 ms**, 22 rows |
| E2 market pricing mix (NAICS 541512 by pricing_code) | unanswerable warm; minutes-scale 392-col Lance scan | **64 ms** on `txn_events_combo` |
| E2 cash-stress award shape (FFP × small × latest state) | unanswerable warm | **1.28 s** over 83M `award_plan_state` (unpruned aggregate — acceptable) |
| E3 farm-out share ≥30%, ≥$1M combo | hand-written 5.1M × 37.5K join per session | **7.2 ms** one-table read, 3,158 lanes |
| PoP-on-events (G in VA since 2026-01) | phrase-layer refusal (no PoP on month rollup) | **18.4 ms** on `txn_recipient_month_pop` (27.5M rows) |
| Semantic-flag aggregation (`is_more_work` since 2026-01) | hardcoded code lists per consumer | **33.8 ms** vocab-join |
| E4 staffing absorption (561612, FTE>50, headcount gap) | unanswerable (4-table composition incl. 35M PDL join) | **9.2 s** via `v_staffing_absorption` — seconds-class: the headcount CTE walks the full bridge before filtering; fine for directional pulls, promote to a mart only after methodology freeze |

### Build-correctness notes

- All five new dispatch branches fixture-tested through `_build_one` dispatch; every new join EXPLAIN-gated (no NESTED_LOOP/CROSS_PRODUCT/BLOCKWISE).
- New stream-hygiene rule learned at fixture time, now encoded in the SQL comment: a multi-leg UNION over a registered Arrow stream reads an exhausted reader after leg 1 — single-scan lateral UNNEST instead.
