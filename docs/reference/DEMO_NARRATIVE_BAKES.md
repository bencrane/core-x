# Demo-Narrative Bakes — every number on the ⌘B walk, end-to-end (2026-07-26)

Authority for how each figure in the gc-hq-new Explore rehearsal (⌘B) demo was computed,
which durable dataset backs it, and how to recompute. Companion program records:
`~/Desktop/hq/MARKET_COLLECTIONS_PROGRAM.md` (collections), the sidecar guide
(`docs/reference/QUERY_SIDECAR_AGENT_GUIDE.md`). Baked TS artifacts live in
gc-hq-new `apps/platform-app/src/map/` (macroEcon.ts, drillDemo.ts, demoRegions.ts,
INDUSTRY_SHAPE in rehearsal.ts, RehearsalDollarSplit values) — each carries a
GENERATED header naming its source mart.

## The company/geo substrate (equipment yards)

| Dataset (Lance, s3://data-sink/active/) | What it is |
|---|---|
| `equipment_company_region_counties` | 2,450 confirmed sub-500/unknown-size equipment providers → derived county sets from RAW service-area strings (flattened service_states deliberately ignored). `basis` col records derivation path (city/zip/county/radius/state/adjacency_ring/hq_fallback). |
| `reference/state_region_county_map` | 91 operator-declared within-state regions → 1,398 counties (authored in-session, gazetteer-validated, in-doubt-is-IN). |
| `reference/demo_region_catalog` | All 3,222 counties tiled into 290 canonical demo regions (authored 91 kept verbatim; rest deterministic centroid tiling; ≤8-county states = statewide). |
| `reference/macro_region_catalog` | 14 natural macro regions → composed states. |
| `equipment_company_demo_region` | Per company: macro_region + demo_region + declared_states. Assignment: ≥80% of derived counties in one region → region; else state; else multi-state; smallest containing macro. |

## The dollars (sidecar-derived, FY window = FY23–25 = 2022-10-01..2025-09-30)

**Substrate (2026-07-26 region-grain cycle).** Every region number below reads a
precomputed place-grain mart — `pop_entity_fy` (firm × place × FY), `pop_combo_fy`
(NAICS × PSC × place × FY), `pop_award_fy` (award × place × FY, awards ≥$100K),
`award_geo_active` (the live book) — instead of scanning the 108M-row transaction fact
once per region. Measured across all 21 regions: **584.5 s → 6.8 s of server time (86×)**;
the two bakes now run end-to-end in 42.5 s and 11.7 s. The marts are keyed by place, so a
new region is a dict entry, never a sidecar rebuild. Query shapes:
`docs/reference/QUERY_SIDECAR_AGENT_GUIDE.md` §4.

**Place-of-performance is TRANSACTION-level** for every region firm/dollar number: each
action counts where it happened. It is *not* award-level (one place per award, its whole
history counted there). Before 2026-07-26 the 14 macros already worked this way — the
award-level join 408'd on them — while the 7 drills did not, so two regions in the same
table were computed by different rules; they are now one rule. Measured at the switch:
macros exact (±0.000% on firms/growth/first-time), drills +0.5–0.9% on firm counts,
+0.0–2.3% on first-time, and 0.2–1.7 percentage points on growth. Active-book numbers are
unaffected — `award_geo_active` inherits award-level PoP, as before.

- **Macro econ card** (`reference/macro_region_econ`, 14 rows): per macro region —
  obligated = sum of in-region transaction obligations (`pop_entity_fy`); firms = distinct
  UEI; outside % = share of dollars won by hq_state outside the region; top-3 outside HQ
  states by dollars (ties broken by state code so the ordering is stable run to run);
  equipment-scope obligations = `pop_combo_fy` ⋈ `naics_psc_equipment_needs` (in_scope).
  `flowdown_factor` is a **flat 0.30** (v1), which is what macroEcon.ts documents and
  displays. ⚠ This is the one card that does NOT use per-industry
  `equipment_flowdown_factors` like every other flow-down number here. The per-industry
  value is available and measured — 0.044 (New England) to 0.129 (Hawaii) — and adopting
  it drops displayed flow 60–85% (Great Lakes ~$18B → ~$3.7B). Open decision; flip
  `FLOW_MODE = "per_industry"` in the bake to take it.
- **Drill cards** (drillDemo.ts, 7 seeded drill regions + all 14 macros): same query family
  filtered by state(s) or region county FIPS. Firms stats: firms ≥$500K FY23–25; median
  award over awards ≥$250K (the $250K floor is a positioning choice — medians are ~3× any
  floor in this power-law book); growth = FY25/FY23 obligations − 1; first-time = UEIs whose
  first region action ≥ 2024-10-01. All four thresholds are named constants at the top of
  the bake, applied at query time — no mart is baked at these values. Active card:
  `award_geo_active`, current_end_date ≥ today, sum(obligated).
- **Outlook/window**: region share = region FY23–25 obligations / national ($2,309.9B);
  OBBA uplift = $785B × share × 0.40 ramp (v1 assumption); window total = active + uplift.
- **Work-order cards**: 3 fixed archetypes — roads (237310×Y1LB + 237990×Y1K*/Y1PZ),
  newbuild (236220×Y1JZ/Y1PZ/Y1AA/Y1AZ/Y1DA), repair (236220×Z2JZ/Z2AA/Z1JZ/Z2AZ) —
  real awards $25–250M in-region ($5M fallback floor for thin regions) via
  `pop_award_fy`; firm names via bridge_sam_pdl → entity_profile_gold; active counts
  via contractor_award_summary. Ordering carries `award_key` as a tiebreaker — ties on
  the dollar total are real (two Central California newbuild awards sit at exactly
  $19.00M) and without it the displayed pick flips between runs.

## The industry shape + cost structure

- **Shape card** (INDUSTRY_SHAPE): active book (`combo_award_active_state`, $2,393.7B)
  by NAICS6 → KLEMS industry (bea_naics_concordance + prefix fallback; 99.8% mapped,
  public-admin residual) → 8 sectors. Full coverage by construction.
- **Cost-structure marts** (`reference/cost_structure_vectors` 771 rows,
  `reference/cost_structure_weighted` 14 rows): KLEMS-2024 $M sheets per industry
  (labor col/noncol, energy, materials, purchased services, capital IT/software/R&D/
  artistic/other — shares of gross output, sum=1.0 per industry); VA comp share and
  FA equipment-share-of-investment and IO-2017 equipment-rental input share carried
  side-by-side. Weighted by the mapped active-book dollars.
- **Dollar-split card**: the weighted vector with rental channels pulled out of
  purchased services; payroll 39.3% ($941B); healthcare line = payroll × 6.9%
  (ECEC 2026Q1, `reference/ecec_comp_components`).
- **Equipment flow-down** (`reference/equipment_flowdown_factors`, 60 industries):
  OUR definition (not BEA's): purchase (KLEMS capital_other × FA equipment-share +
  capital_IT) + bare rental (IO 2017 commodities 532100/532400/532A00) + operated-rental
  estimate (bare × 3.0 classification-gap multiplier, capped at 50% of purchased
  services). Weighted national: 3.9% ≈ $94B ($80B purchase / $3B bare / $10B operated).
  The 3.0 multiplier is the ONE authored dial — upgrade path: rental-industry-side
  triangulation. Drill-card flow-down applies these factors per region's NAICS mix
  (PR gc-hq-new#168 replaced the older equip-scope×0.30 method).

## Deals / demo wiring

8 seeded real-yard deals in `business.deals` (edge_api POST /api/v1/deals) spanning all
demo tiers; ActiveDeal.domain → demoRegions.ts lookup keys every parameterized card.

## Recompute

As-run bake scripts: `scripts/demo_bakes/` (this repo). They are session-extracted,
deterministic, and runnable with doppler creds (`-p core-x -c prd`); each writes the
mart it owns. Regenerated TS goes to gc-hq-new `apps/platform-app/src/map/`.
