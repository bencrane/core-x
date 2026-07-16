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
