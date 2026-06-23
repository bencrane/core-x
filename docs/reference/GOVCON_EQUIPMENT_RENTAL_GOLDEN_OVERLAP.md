# GovCon Equipment-Rental Golden Overlap — Triple-Side Recon

**Purpose:** fuse the three sides of the rental-GTM thesis into one capability-qualified target list,
and quantify how much sharper "the right yard for the right nearby work" is than raw proximity.

> **Updated 2026-06-23 — Building-Codes Expansion (Gap B remediated).** The PSC equipment seed grew
> 15 → **19 codes** (added Z1AA, Y1AA, Y1JZ, Z2JZ — the top uncovered building demand). Golden firms
> **834 → 879**, qualified demand pairs **7,067 → 9,380 (+33%)**, qualified value **$99.6B → $130.3B**.
> Full before/after in §8. All tables below reflect the post-expansion (19-code) state.

**The three sides**
1. **Where the work is** — active federal construction awards by PSC, geo-located (ZCTA centroid of `pop_zip`).
2. **The yard's geography** — candidate firm HQ location. *Sides 1+2 are pre-fused* in
   `govcon_firm_construction_proximity` → "firm is within 50mi of N active awards of PSC P."
3. **What the yard can supply** — `equipment_matchmaking` → the PSCs a yard's *scraped inventory* can serve.

**The golden join**
```
proximity(firm_domain, psc_code, nearby_award_count, nearby_total_award_value)
  ⋈  matchmaking(domain_norm, supported_pscs)
  ON firm_domain = domain_norm  AND  psc_code ∈ supported_pscs
```
Raw proximity says a yard is *near* Z2AA demand. The join asks whether it actually *stocks Z2AA iron*.
That converts proximity into **capability-qualified demand** — the only demand a yard can convert.

**System of record:** `s3://data-sink/active/equipment_rental_golden_overlap/` (Lance v2.1, **879 firms**)
**Inputs:** `active/govcon_firm_construction_proximity/` (#626) · `active/equipment_matchmaking/` (19-code) · `active/reference/psc_equipment_mapping/` (19-code seed)
**Date:** 2026-06-23 · read-only DuckDB/Lance join (`scripts/golden_overlap_probe.py`)

---

## 1. The triple-overlap funnel

| Stage | Firms |
|---|---:|
| Geo-placed candidates (proximity matrix) | 5,759 |
| Scraped for capability (in `equipment_matchmaking`) | 3,096 |
| **Both — geo-placed AND scraped** | **2,437** |
| └─ scraped firm is capable of ≥1 mapped PSC | 1,139 |
| &nbsp;&nbsp;└─ **GOLDEN: near ≥1 active project in a PSC it can supply** | **879** |

**879 golden firms** — geographically on top of federal construction demand they are equipped to win.
This is the SoR. Everything downstream (outreach, scoring) consumes it.

---

## 2. Capability qualification — the sharpening

The whole point: proximity alone overcounts. Most "near construction" demand is in PSCs a given yard
can't serve.

| Demand measure (firm↔award proximity pairs) | Pairs |
|---|---:|
| Entire proximity matrix (all 333 PSCs) | 285,382 |
| Among the 2,437 intersection firms, in the 19 mapped PSCs | 69,520 |
| **…capability-qualified (yard can actually serve)** | **9,380** |

**Capability qualification discards ~87% of the mapped-PSC proximity demand** (69,520 → 9,380).
That is signal raw proximity cannot produce: the difference between "near *a* jobsite" and "near a
jobsite that needs what I rent." Qualified value exposure across the golden set: **$130.3B**¹.

**Qualified demand density (golden firms):**

| Qualified nearby projects | Firms |
|---|---:|
| 1 | 171 |
| 2–5 | 323 |
| 6–25 | 309 |
| 26–100 | 63 |
| **100+** | **13** |

The top of the distribution thickened sharply with the expansion — **13 firms now sit on 100+ qualified
projects** (was 3). Capability-capture ratio (qualified ÷ mapped nearby demand): **mean 17.4%**; median
across all intersection firms is 0% (half can serve none — see §5). Capture is concentrated, not diffuse.

¹ Value exposure double-counts an award shared by multiple nearby firms — read as value-weighted demand exposure, not distinct contract value.

---

## 3. PSC beachhead — where capability × proximity actually concentrate

Among the 2,437 intersection firms, qualified demand by PSC. `capable` = firms near that PSC's demand
that can also serve it; `capture` = qualified ÷ all-nearby demand. **★ = code added in the expansion.**

| PSC | Work | Nearby demand | Firms near | **Capable** | **Qualified** | Capture |
|---|---|---:|---:|---:|---:|---:|
| **Z2AA** | Office repair/alteration | 15,214 | 1,880 | 427 | **2,836** | 19% |
| **F108** | Environmental remediation | 7,280 | 1,322 | 246 | 1,186 | 16% |
| **Y1JZ ★** | Misc-building construction | 8,153 | 1,319 | 151 | **1,123** | 14% |
| **Z2DA** | Hospital repair/alteration | 6,067 | 1,066 | 217 | 1,086 | 18% |
| **Y1DA** | Hospital construction | 4,374 | 1,118 | 287 | 871 | 20% |
| **Z1AA ★** | Office maintenance | 11,877 | 890 | 54 | **580** | 5% |
| **Z2JZ ★** | Misc-building repair/alteration | 5,608 | 946 | 78 | 449 | 8% |
| Y1PZ | Other non-building | 1,678 | 1,013 | 275 | 436 | 26% |
| Z1DA | Hospital maintenance | 4,503 | 1,335 | 95 | 300 | 7% |
| Y1LB | Highway construction | 1,638 | 752 | 72 | 177 | 11% |
| **Y1AA ★** | Office construction | 1,428 | 833 | 91 | 165 | 12% |
| Z1LB | Highway maintenance | 801 | 582 | 55 | 82 | 10% |
| P400 | Demolition | 112 | 111 | 29 | 29 | 26% |
| Y1PC | Unimproved land | 207 | 188 | 22 | 24 | 12% |
| Y1NE | Water supply | 280 | 227 | 17 | 20 | 7% |
| Z2KA | Dam/dredging repair | 98 | 94 | 12 | 12 | 12% |
| Z1KF | Dredging maintenance | 193 | 193 | 4 | 4 | 2% |
| F014 | Tree thinning | 9 | 9 | 0 | 0 | 0% |

**Y1JZ (misc-building construction) entered as the #3 code** on first appearance — 1,123 qualified pairs
across 151 yards. The four new building codes contribute **2,317 qualified pairs (+33%)**. The campaign is
now an even tighter **buildings cluster: Z2AA + F108 + Y1JZ + Z2DA + Y1DA + Z1AA** (~92% of qualified demand).
Z1AA carries the most *latent* demand (11,877 nearby) at the lowest capture (5%) — many office-maintenance
jobsites, comparatively few yards stocking the chiller/genset/aerial maintenance set; a targeted
supply-expansion lane.

---

## 4. The golden target list (top 20 by qualified nearby projects)

**★ = surfaced into the top tier by the expansion** (was invisible on 15 codes).

| Firm | Qualified | Value exposure | Top qualified PSCs |
|---|---:|---:|---|
| washair.com | 248 | $3.13B | Z2AA 84, Y1JZ 75, Z2JZ 29, Z1AA 17 |
| alliedcontractor.com | 246 | $3.23B | Z2AA 82, Y1JZ 74, Z2JZ 29, Z1AA 17 |
| brandywinerentals.com | 225 | $2.35B | Z2AA 83, Y1JZ 73, Z2JZ 29, Z1AA 17 |
| dandbrentals.com | 222 | $2.33B | Z2AA 74, Y1JZ 73, Z2JZ 26, Z2DA 20 |
| brandywine-eqp.com | 206 | $2.50B | Z2AA 83, Y1JZ 73, Z2JZ 29, Y1AA 5 |
| **bbfyale.com ★** | 122 | $0.16B | Z1AA 84, Z2AA 24, Z2JZ 11 |
| **blakleyequipment.com ★** | 119 | $0.21B | Z1AA 85, Z2AA 28, Z1DA 6 |
| **empiretoolrental.com ★** | 119 | $0.21B | Z1AA 85, Z2AA 28, Z1DA 6 |
| **brandtcrane.com ★** | 119 | $0.16B | Z1AA 84, Z2AA 24, Z2JZ 11 |
| **elliottfrantz.com ★** | 108 | $2.76B | Y1JZ 74, Y1DA 9, Y1LB 7, Y1AA 6 |
| **strittmattermetro.com ★** | 107 | $2.76B | Y1JZ 75, Y1DA 8, Y1LB 7, Y1AA 5 |
| skyreachequipment.com | 106 | $1.13B | Z2AA 82, Z2DA 15, Y1DA 9 |

These cluster hard in the **NJ/DE/PA federal corridor**. The expansion surfaced two new archetypes that
15 codes missed entirely: **office-maintenance specialists** (bbfyale, blakley, empiretoolrental,
brandtcrane — Z1AA:84-85) and **misc-building crane/general fleets** (elliottfrantz, strittmattermetro,
rentalsunlimited, cranerentalnow — Y1JZ:73-75). Full 879-firm list: query the SoR or
`reports/golden_overlap_firm_level.jsonl`.

---

## 5. Gap A — the candidate universe is leaking non-yards (prune lever)

**1,475 intersection firms sit near mapped-PSC demand but can serve *none* of it.** The top of that list
is the tell — telecom, A/V, mobile-kitchen, and laser firms tagged `equipment_rental_candidates` upstream,
geo-placed onto rich demand (now near all 12 building codes), but the matchmaking **bouncer correctly found
they have no construction iron**:

| Firm | Mapped near | Serves |
|---|---:|---|
| mobilekitchensolutions.com | 258 | (none) |
| ameritelcorporation.com | 257 | (none) |
| cds-yes.com | 257 | (none) |
| klassicsound.com | 257 | (none) |
| amalaserline.com | 256 | (none) |

Gap A is a *cross-validation*: it isolates ~1,475 false candidates that proximity-only scoring would rank
as prime (they're near the most demand). **Prune them from the candidate universe** — they inflate the TAM
and waste outreach. A residual slice is real-yet-mismatched yards (e.g. `revdrill.com` serves Y1KD while the
nearby demand is buildings) — a capability↔demand PSC mismatch, not a fake firm.

---

## 6. Gap B — REMEDIATED (building-codes expansion)

The original recon found the largest pools of nearby demand fell in PSCs **absent from the equipment seed**.
This pass added the top four buildings codes (Z1AA, Y1AA, Y1JZ, Z2JZ) → §8. **The meaningful building demand
is now qualifiable.** What remains uncovered is the long tail + non-equipment codes (a service/product, not
a supply gap):

| Remaining uncovered PSC | Nearby pairs | Note |
|---|---:|---|
| 6350 | 2,604 | alarm/signal systems (product code — not rental iron) |
| S216 | 1,746 | facilities operations support (service) |
| Z2AZ | 1,582 | repair/alt of other admin/service buildings (marginal) |
| Y1JA | 1,484 | misc building construction (marginal) |
| Y1AZ / N059 / F999 / Y1DZ | ≤1,425 ea | long tail / non-construction |

Diminishing returns: the remaining building codes (Z2AZ, Y1JA) are marginal and 6350/S216/N059/F999 are not
equipment-mappable. **No further seed expansion warranted** — the next lever is Gap A pruning + address
recovery (§7), not more PSCs.

---

## 7. Verdict & next actions

1. **879 capability-qualified golden firms** are live in `active/equipment_rental_golden_overlap/`. Start with
   the top of §4 in the NJ/DE/PA corridor.
2. **Lead with the buildings cluster** — Z2AA, F108, Y1JZ, Z2DA, Y1DA, Z1AA hold ~92% of qualified demand.
   Do not spend supply-matching effort on horizontal/dredging.
3. **Targeted supply expansion on Z1AA** — 11,877 nearby office-maintenance pairs but only 54 capable yards
   (5% capture). Recruiting yards that stock chiller/genset/aerial maintenance kits unlocks the largest latent pool.
4. **Prune the candidate universe** using Gap A — ~1,475 tagged "equipment" firms are non-yards the bouncer
   rejected. Removing them de-noises every downstream metric.
5. **Address-resolution is still the supply ceiling** — 659 of 3,096 scraped yards never geo-placed
   (3,096 scraped − 2,437 intersection). Recovering their `company_addresses` rows promotes them into the funnel.

---

## 8. Building-codes expansion — before/after

Added to `psc_equipment_mapping` (#624 seed), mirroring the equipment vocabulary of their existing twins so
the matchmaking agents bridge cleanly:

| New PSC | Work | required_equipment (seed) |
|---|---|---|
| Z1AA | Maintenance of Office Buildings | Scissor/Boom Lifts, Telehandlers, Towable Generators, Temporary Chiller Units, Light Towers |
| Y1AA | Construction of Office Buildings | Excavators, Rough-Terrain/Crawler Cranes, High-Reach Telehandlers, Boom Lifts, Wheel Loaders, Skid Steers |
| Y1JZ | Construction of Miscellaneous Buildings | Excavators, Rough-Terrain Cranes, Telehandlers, Wheel Loaders, Skid Steers, Boom/Scissor Lifts |
| Z2JZ | Repair/Alteration of Misc Buildings | Scissor/Boom Lifts, Telehandlers, Skid Steers, Portable Generators, Light Towers |

| Metric | 15-code (before) | 19-code (after) | Δ |
|---|---:|---:|---:|
| PSC seed codes | 15 | 19 | +4 |
| Matchmaking matched domains | 1,452 | 1,467 | +15 |
| Intersection firms (geo+capable) | 1,132 | 1,139 | +7 |
| Mapped-PSC nearby pairs (intersection) | 42,454 | 69,520 | **+64%** |
| **Capability-qualified pairs** | **7,067** | **9,380** | **+33%** |
| Qualified value exposure | $99.6B | $130.3B | **+31%** |
| **Golden firms** | **834** | **879** | **+45** |
| Golden firms with 100+ qualified projects | 3 | 13 | **4.3×** |

The expansion's value is not just volume — it **surfaced new top-tier firms** (office-maintenance and
misc-building specialists) that scored zero on 15 codes, and pushed the corridor leaders (washair, allied,
brandywine) from ~120 to ~250 qualified projects each by exposing the Y1JZ/Z2JZ/Z1AA demand they were
already sitting on.

---

## Appendix — reproduction

```bash
# 0. (one-time) expand the seed, re-materialize the 19-code reference
doppler run -p core-x -c prd -- python3 pipelines/reference/materialize_psc_equipment.py
# 1. re-run matchmaking (Claude Code Workflow over scripts/mm_workflow.js) -> reports/mm_out/
# 2. grounding gate -> equipment_matchmaking Lance
doppler run -p core-x -c prd -- python3 pipelines/gtm/materialize_equipment_matchmaking.py
# 3. join proximity × matchmaking -> golden JSONL + printed analysis
doppler run -p core-x -c prd -- python3 scripts/golden_overlap_probe.py
# 4. materialize the golden target list
doppler run -p core-x -c prd -- python3 pipelines/serving/materialize_equipment_rental_golden_overlap.py
```

**SoR schema:** `firm_domain` (PK) · `qualified_pscs[]` · `qualified_psc_count` · `qualified_nearby_award_count`
· `qualified_value_exposure` · `mapped_nearby_award_count` · `all_nearby_award_count` · `capability_capture_ratio`
· `qualified_psc_demand` (JSON) · `materialized_at`.
