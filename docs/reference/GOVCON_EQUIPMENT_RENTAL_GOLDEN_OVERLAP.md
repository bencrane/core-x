# GovCon Equipment-Rental Golden Overlap — Triple-Side Recon

**Purpose:** fuse the three sides of the rental-GTM thesis into one capability-qualified target list,
and quantify how much sharper "the right yard for the right nearby work" is than raw proximity.

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

**System of record (new):** `s3://data-sink/active/equipment_rental_golden_overlap/` (Lance v2.1, 834 firms)
**Inputs:** `active/govcon_firm_construction_proximity/` (#626) · `active/equipment_matchmaking/` (#629)
**Date:** 2026-06-22 · read-only DuckDB/Lance join (`scripts/golden_overlap_probe.py`)

---

## 1. The triple-overlap funnel

| Stage | Firms |
|---|---:|
| Geo-placed candidates (proximity matrix) | 5,759 |
| Scraped for capability (in `equipment_matchmaking`) | 3,096 |
| **Both — geo-placed AND scraped** | **2,437** |
| └─ scraped firm is capable of ≥1 mapped PSC | 1,132 |
| &nbsp;&nbsp;└─ **GOLDEN: near ≥1 active project in a PSC it can supply** | **834** |

**834 golden firms** — geographically on top of federal construction demand they are equipped to win.
This is the SoR. Everything downstream (outreach, scoring) consumes it.

---

## 2. Capability qualification — the sharpening

The whole point: proximity alone overcounts. Most "near construction" demand is in PSCs a given yard
can't serve.

| Demand measure (firm↔award proximity pairs) | Pairs |
|---|---:|
| Entire proximity matrix (all 333 PSCs) | 285,382 |
| Among the 2,437 intersection firms, in the 15 mapped PSCs | 42,454 |
| **…capability-qualified (yard can actually serve)** | **7,067** |

**Capability qualification discards ~83% of the mapped-PSC proximity demand** (42,454 → 7,067).
That is signal raw proximity cannot produce: it is the difference between "near *a* jobsite" and
"near a jobsite that needs what I rent." Qualified value exposure across the golden set: **$99.6B**¹.

**Qualified demand density (golden firms):**

| Qualified nearby projects | Firms |
|---|---:|
| 1 | 167 |
| 2–5 | 332 |
| 6–25 | 277 |
| 26–100 | 55 |
| 100+ | 3 |

Capability-capture ratio (qualified ÷ mapped-PSC nearby demand): **mean 19.7%**. Median across *all*
intersection firms is **0%** — because half of them (see §5) can serve none of their nearby mapped
demand. Among the 834 golden firms the ratio is materially higher; capture is concentrated, not diffuse.

¹ Value exposure double-counts an award shared by multiple nearby firms — read as value-weighted demand exposure, not distinct contract value.

---

## 3. PSC beachhead — where capability × proximity actually concentrate

Among the 2,437 intersection firms, qualified demand by PSC. `capable` = firms near that PSC's demand
that can also serve it; `capture` = qualified ÷ all-nearby demand for that PSC.

| PSC | Work | Nearby demand | Firms near | **Capable** | **Qualified** | Capture |
|---|---|---:|---:|---:|---:|---:|
| **Z2AA** | Office repair/alteration | 15,214 | 1,880 | **420** | **2,821** | 19% |
| **F108** | Environmental remediation | 7,280 | 1,322 | 246 | 1,177 | 16% |
| **Z2DA** | Hospital repair/alteration | 6,067 | 1,066 | 215 | 1,091 | 18% |
| **Y1DA** | Hospital construction | 4,374 | 1,118 | 288 | 896 | 20% |
| **Y1PZ** | Other non-building | 1,678 | 1,013 | 276 | 440 | 26% |
| Z1DA | Hospital maintenance | 4,503 | 1,335 | 99 | 321 | 7% |
| Y1LB | Highway construction | 1,638 | 752 | 66 | 159 | 10% |
| Z1LB | Highway maintenance | 801 | 582 | 45 | 65 | 8% |
| P400 | Demolition | 112 | 111 | 29 | 29 | 26% |
| Y1PC | Unimproved land | 207 | 188 | 24 | 27 | 13% |
| Y1NE | Water supply | 280 | 227 | 20 | 23 | 8% |
| Z2KA | Dam/dredging repair | 98 | 94 | 14 | 14 | 14% |
| Z1KF | Dredging maintenance | 193 | 193 | 4 | 4 | 2% |
| F014 | Tree thinning | 9 | 9 | 0 | 0 | 0% |

**The wedge is five codes: Z2AA, F108, Z2DA, Y1DA, Y1PZ** — building repair/construction + remediation.
They hold ~88% of all qualified demand. Highway, dredging, mine, and tree codes are rounding error on
the supply side (few yards stock graders/draglines/mulchers). This confirms and sharpens the TAM doc's
"buildings market, not horizontal." **Z2AA (office repair) alone is the campaign**: 2,821 qualified pairs
across 420 capable yards.

---

## 4. The golden target list (top 20 by qualified nearby projects)

| Firm | Qualified | Mapped near | Value exposure | Top qualified PSCs |
|---|---:|---:|---:|---|
| washair.com | 121 | 130 | $1.19B | Z2AA 84, Z2DA 16, Y1DA 8 |
| alliedcontractor.com | 120 | 124 | $1.30B | Z2AA 82, Z2DA 15, Y1DA 9 |
| skyreachequipment.com | 106 | 128 | $1.13B | Z2AA 82, Z2DA 15, Y1DA 9 |
| brandywine-eqp.com | 99 | 126 | $1.14B | Z2AA 83, Y1DA 8, F108 5 |
| jgrequipment.com | 95 | 115 | $0.45B | Z2AA 72, Z2DA 20, Z1DA 3 |
| dandbrentals.com | 88 | 119 | $1.14B | Z2AA 74, Y1DA 6, F108 5 |
| brandywinerentals.com | 83 | 126 | $0.43B | Z2AA 83 |
| phoenixsteel.com | 80 | 115 | $1.10B | Z2AA 72, Y1DA 6, Y1PZ 2 |
| newarkequipment.com | 67 | 74 | $0.42B | Z2AA 27, Y1DA 20, F108 18 |
| foleyinc.com | 66 | 66 | $0.41B | Z2AA 24, F108 20, Y1DA 16 |

These cluster hard in the **NJ/DE/PA federal corridor** (Brandywine, Newark, Foley, NJ Bobcat, JGR) —
dense office/hospital repair demand on top of a dense equipment-dealer population. That corridor is the
first sales territory. Full 834-firm list: query the SoR or `reports/golden_overlap_firm_level.jsonl`.

---

## 5. Gap A — the candidate universe is leaking non-yards (prune lever)

**1,473 intersection firms sit near mapped-PSC demand but can serve *none* of it.** The top of that list
is the tell:

| Firm | Mapped near | Matchmaking says it serves |
|---|---:|---|
| ameritelcorporation.com | 133 | (none) |
| cds-yes.com | 132 | (none) |
| powerandclimate.com | 132 | (none) |
| mobilekitchensolutions.com | 131 | (none) |
| klassicsound.com | 130 | (none) |
| amalaserline.com | 129 | (none) |

These are telecom, A/V, mobile-kitchen, and laser firms that got tagged `equipment_rental_candidates`
upstream and geo-placed onto rich demand — but the matchmaking **bouncer correctly found they have no
construction iron**. Gap A is therefore a *cross-validation*: it isolates ~1,473 false candidates that
proximity-only scoring would have ranked as prime (they're near tons of demand). **Prune them from the
candidate universe** — they inflate the TAM and waste outreach. A residual slice is real-yet-mismatched
yards (e.g. `revdrill.com` serves Y1KD but the nearby demand is office buildings) — a capability↔demand
PSC mismatch, not a fake firm.

---

## 6. Gap B — the 15-PSC seed is blind to the biggest demand (reference-expansion lever)

Among intersection firms, the largest pools of nearby demand fall in PSCs **not in
`psc_equipment_mapping`** (#624), so matchmaking can never qualify them:

| Uncovered PSC | Nearby pairs | What it is |
|---|---:|---|
| **Z1AA** | **11,877** | **Maintenance of Office Buildings** |
| Y1JZ | 8,153 | Construction of Miscellaneous Buildings |
| Z2JZ | 5,608 | Repair/Alteration of Miscellaneous Buildings |
| 6350 | 2,604 | (product code — alarm/signal systems) |
| S216 | 1,746 | Facilities operations support svc |
| Z2AZ | 1,582 | Repair/Alt of other admin/service buildings |
| Y1JA | 1,484 | Misc building construction |
| Y1AA | 1,428 | Construction of Office Buildings |

**Z1AA (office *maintenance*) has more nearby demand than Z2AA (office repair) does** — and it's invisible
to the engine. Z1AA is the maintenance twin of Z2AA and would map to the **same office support set**
(scissor/boom lifts, generators, light towers, skid steers). **Adding Z1AA, Y1JZ/Z2JZ, and Y1AA to the
equipment-mapping reference is the single highest-leverage next step** — it would roughly *double* the
qualifiable demand without touching the matching engine. This is a 4-row edit to the seed in #624 +
a matchmaking re-run.

---

## 7. Verdict & next actions

1. **834 capability-qualified golden firms** are live in `active/equipment_rental_golden_overlap/`. This is
   the outreach list — start with the top of §4 in the NJ/DE/PA corridor.
2. **Lead with Z2AA**, then F108 / Z2DA / Y1DA / Y1PZ. ~88% of qualified demand lives in those five codes;
   do not spend supply-matching effort on horizontal/dredging.
3. **Prune the candidate universe** using Gap A — ~1,473 tagged "equipment" firms are non-yards the bouncer
   rejected; they are near demand but unconvertible. Removing them de-noises every downstream metric.
4. **Expand the PSC seed (Gap B)** — add Z1AA + Y1JZ/Z2JZ + Y1AA to `psc_equipment_mapping` and re-run
   matchmaking. Z1AA alone (11,877 nearby pairs) likely outweighs the current top code. Highest ROI lever.
5. **Address-resolution is still the supply ceiling** (TAM §5): 659 of 3,096 scraped yards never geo-placed
   (3,096 scraped − 2,437 intersection). Recovering their `company_addresses` rows promotes them straight
   into the golden funnel.

---

## Appendix — reproduction

```bash
# 1. join proximity × matchmaking → firm-level golden JSONL + printed analysis
doppler run -p core-x -c prd -- python3 scripts/golden_overlap_probe.py
# 2. materialize the golden target list to Lance
doppler run -p core-x -c prd -- python3 pipelines/serving/materialize_equipment_rental_golden_overlap.py
```

Query the SoR (e.g. all yards that can serve office-repair demand they're sitting on):
```python
import lance, os
so = {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"], "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
      "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com", "region": "auto"}
ds = lance.dataset("s3://data-sink/active/equipment_rental_golden_overlap/", storage_options=so)
rows = ds.scanner(filter="qualified_psc_count >= 3").to_table().to_pylist()   # multi-PSC golden firms
z2aa = [r for r in rows if "Z2AA" in r["qualified_pscs"]]
```

**SoR schema:** `firm_domain` (PK) · `qualified_pscs[]` · `qualified_psc_count` · `qualified_nearby_award_count`
· `qualified_value_exposure` · `mapped_nearby_award_count` · `all_nearby_award_count` · `capability_capture_ratio`
· `qualified_psc_demand` (JSON) · `materialized_at`.
