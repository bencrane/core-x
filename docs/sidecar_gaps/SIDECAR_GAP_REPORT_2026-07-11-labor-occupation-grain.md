# SIDECAR GAP REPORT — 2026-07-11 — labor occupation-grain surface

**Artifact at session:** `query-sidecar/query_sidecar_20260711T032135Z.duckdb` (71 tables)
**Topic:** labor-wiring execution session (Gaps 1–3a of `LABOR_x_GOVCON_CROSSWALK_GTM.md`).
Every labor answer below required Lance fallbacks — the combo-grain layer
(`naics_psc_labor_profile*`) is warm but ALL occupation-grain, wage-floor, and
union-identity questions leave the sidecar. Two NEW datasets were landed this
session (PRs #1110, #1111) that are natural promotion candidates alongside the
existing Lance-only labor tables.

---

## Entry 1

1. **Intent** — "Show the ranked SCA↔SOC mapping for a construction combo's labor
   categories (codes 23130/23470/23160/23810), with tier/confidence/evidence."
2. **Why not the sidecar** — missing table: `sca_soc_crosswalk` (424 rows,
   occupation_code↔soc_code + tier/method/confidence/dominance) is Lance-only.
3. **What I ran instead** — pylance probe `s3://data-sink/active/sca_soc_crosswalk`
   → duckdb over full 424-row table; columns: occupation_code, soc_code, soc_title,
   tier, method, confidence, dominance_ratio, primary_dollar_weight.
4. **Cost** — ~20 s cold (uv + Lance handshake); 424 scanned / 5 returned.
5. **Recurrence** — recurring: every market-vs-floor wage question crosses this bridge.

## Entry 2

1. **Intent** — "What is the statutory wage floor + fringe for a given SCA occupation
   on a given wage determination / in a given county?" (exercised repeatedly:
   verification of Gap-1 output; Carpenter@Fairfax example.)
2. **Why not the sidecar** — missing table: `sam_wd_rates_structured` (NEW this
   session, PR #1110 — 521,711 rows, grain wd_id × occupation/classification,
   wage_rate + fringe/hw_rate; BTREE wd_id, occupation_code).
3. **What I ran instead** — pylance probe + duckdb joins over
   `sam_wd_rates_structured` × `sam_wd_county_coverage` × `sam_county_fips_crosswalk`.
4. **Cost** — ~60 s per probe session (3-table Lance pull); 522k+33k+3.3k scanned /
   handfuls returned.
5. **Recurrence** — recurring: this is the priced-labor floor for every staffing
   GTM pitch; per-award and per-county lookups will be constant.

## Entry 3

1. **Intent** — "Bind WD locality to true county FIPS (join WD geography to the
   award spine's PoP county)."
2. **Why not the sidecar** — missing table: `sam_county_fips_crosswalk` (NEW this
   session, PR #1111 — 3,342 rows, (state_code, sam_county_name)→county_fips,
   98.51% coverage bind).
3. **What I ran instead** — Lance probe of the new crosswalk + coverage table.
4. **Cost** — seconds-class once cached; trivial rows.
5. **Recurrence** — recurring: prerequisite hop for EVERY county-grain wage query
   (rides with Entry 2).

## Entry 4

1. **Intent** — "State-level market wage percentiles for a SOC (e.g. Carpenters
   47-2031 in VA: p25/median/p75)" — the market side of the market-vs-floor spread.
2. **Why not the sidecar** — missing table: `soc_state_wage` (35,223 rows,
   soc_code × state) is Lance-only. (Recipe 3.3 of the crosswalk doc does this
   entirely on Lance.)
3. **What I ran instead** — (this session referenced it; prior sessions scan it
   directly) pylance → duckdb, columns soc_code, prim_state, a_median, a_pct25, a_pct75.
4. **Cost** — ~15–30 s per probe; 35k scanned / tens returned.
5. **Recurrence** — recurring: every wage-envelope answer pairs this with Entry 1+2.

## Entry 5

1. **Intent** — "Is this UEI's incumbent workforce unionized; when does the CBA
   expire?" (§4(c) successorship exposure; also the 3a measurement inputs.)
2. **Why not the sidecar** — missing tables: `olms_cba_crosswalk` (4,844, uei-keyed),
   `sam_wd_cba_pointers` (4,298), `olms_cba_index` (4,849) — all Lance-only.
3. **What I ran instead** — cached all three to parquet via pylance, matched in
   local duckdb/python (Gap-3a measurement, now in
   `docs/reference/OLMS_CBA_POINTER_JOIN_MEASUREMENT.md`).
4. **Cost** — ~90 s to cache; 14k rows scanned total.
5. **Recurrence** — `olms_cba_crosswalk` (uei→union/exp_date) is the recurring one —
   it joins warm sidecar award tables on uei for the union-exposure column of any
   target list. Pointers/index were one-off measurement inputs.

## Entry 6

1. **Intent** — "SCA occupation taxonomy lookups (code → title/definition/family)"
   — needed to label every Entry-2 answer.
2. **Why not the sidecar** — missing table: `dol_sca_occupations` (502 rows).
3. **What I ran instead** — rode along in the Entry-1/2 Lance probes.
4. **Cost** — negligible alone; forces the Lance detour when the rest is warm.
5. **Recurrence** — recurring as the display/name layer for SCA codes (analog of
   `v_psc_names` for PSC).

---

## Ranking (recurrence × cost)

1. **Entry 2+3 together** — `sam_wd_rates_structured` + `sam_county_fips_crosswalk`:
   the county-priced statutory floor; new, load-bearing, and every GTM wage answer
   needs them.
2. **Entry 4** — `soc_state_wage`: the market half of the same answer.
3. **Entry 1** — `sca_soc_crosswalk`: the 424-row bridge both halves meet on.
4. **Entry 6** — `dol_sca_occupations`: name layer, trivial size.
5. **Entry 5** — `olms_cba_crosswalk` only (uei-keyed union exposure).

Adjacency note for the build cycle (one rebuild ships the complete thought): these
six tables are one connected subgraph — award (naics,psc) → [warm combo layer] →
soc/sca → wage (market: soc_state_wage; floor: sam_wd_rates_structured via
sam_county_fips_crosswalk) → uei union exposure (olms_cba_crosswalk). Total added
volume ≈ 560k rows / ~40 MB — negligible against the 1.23B-row artifact.
`govcon_labor_demand` (20k) and `sam_labor_poc_people` (29k) sit on the same
subgraph's edge (award-linked demand, uei-keyed POC) — sweep candidates if the
gate passes them.
