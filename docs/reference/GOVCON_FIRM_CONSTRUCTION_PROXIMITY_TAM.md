# GovCon Firm × Construction Proximity — Geographic TAM Reconnaissance

**Purpose:** quantify the geographic density of supply-side target firms against active federal construction demand, to validate the addressable market before wiring the LLM catalog-matching engine.

**System of record:** `s3://data-sink/active/govcon_firm_construction_proximity/` (Lance v2.1)
**Joined against:** `s3://data-sink/active/companies/` (GTM candidate universe)
**Matrix state at analysis:** 109,121 rows · 5,759 firms · 333 PSCs · 285,382 firm↔award proximity pairs
**Date:** 2026-06-22 (post-Overture-recovery rebuild — see §4)

---

## Definitions & Methodology

- **Match:** a candidate firm's HQ lies within **50 miles (Haversine, 3958.8 mi earth radius)** of an active federal award where `construction_wage_rate_requirements = 'YES'` AND `pop_current_end ≥ today`. Award location is the ZCTA centroid of `pop_zip`; firm location is the ZCTA centroid of `winner_postal_code` from `company_addresses`.
- **Candidate universe:** distinct firms in `active/companies` tagged `equipment_rental_candidates` or `epd_lec_status_candidates` (bare + `enrichment:` prefix). = **8,056** firms (full table is 9,108; other `source_platform` tags excluded by the filter).
- **Grain of the matrix:** one row per `(firm_uei, firm_domain, psc_code)`, carrying `nearby_award_count`, `nearby_total_award_value`, and `nearby_award_keys`.
- **Distinct-projects invariant:** each award carries exactly one `psc_code`, so `SUM(nearby_award_count)` over a firm's PSC rows equals its count of distinct nearby awards (validated: `285,382 = 285,382 = 285,382` across sum-of-counts, unnested keys, and distinct firm×award pairs).
- All probes are **read-only** DuckDB over the live Lance datasets.

---

## 1. Overall Hit Rate

The hit rate is **geocoding-bound, not demand-bound.** Once a firm can be placed on a map, it is essentially guaranteed to sit near active federal construction.

| Funnel stage | Firms | % of candidates |
|---|---:|---:|
| Distinct candidate firms (both tags) | 8,056 | 100.0% |
| └─ resolved to a postal code | 5,876 | 72.9% |
| &nbsp;&nbsp;&nbsp;└─ resolved to lat/lon centroid (**mappable universe**) | 5,833 | 72.4% |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└─ **matched ≥1 project ≤50 mi** | **5,759** | **71.5%** |

**5,759 distinct firms matched — 71.5% of all candidates, 98.7% of the mappable universe.** The ceiling on the raw rate is address resolution, not nearby demand.

### Split by candidate tag

The two segments are cleanly separated (0 firms carry both tags).

| Bucket | Candidates | Matched | Hit rate |
|---|---:|---:|---:|
| Equipment Rental | 4,420 | 3,314 | 75.0% |
| EPD/LEC | 3,636 | 2,445 | 67.2% |
| **Total** | **8,056** | **5,759** | **71.5%** |

---

## 2. Demand Density — The "Golden" Segment

Density is extremely high: **90.0% of matched firms are "golden" (>5 nearby active projects).** The median matched firm sits near **27** active federal construction projects; the densest near **436**.

| Density band | Firms | % of matched |
|---|---:|---:|
| Exactly 1 nearby project | 90 | 1.6% |
| 2–5 nearby projects | 489 | 8.5% |
| **>5 nearby projects (golden)** | **5,180** | **90.0%** |
| **Total matched** | **5,759** | 100% |

*avg 49.6 · median 27 · max 436 projects per firm*

**Golden segment (>5) by tag:** Equipment Rental **2,969** · EPD/LEC **2,211** · (both 0) → **5,180**.

---

## 3. PSC Distribution — What Drives the Overlap

The overlap is driven by **building repair / alteration / maintenance** (Z-codes) and **vertical building construction** (Y-codes), plus **environmental remediation** (F108). Highway (Y1LB) is not in the top tier — this is a *buildings* market, not a horizontal/roads market.

**Top 5 by total `nearby_award_count` (overlap volume = firm↔award proximity pairs):**

| Rank | PSC | Description | `nearby_award_count` | Firms in radius | Σ value exposure¹ |
|---|---|---|---:|---:|---:|
| 1 | **Z2AA** | Repair/Alteration of Office Buildings | **36,160** | 4,439 | $134.5B |
| 2 | **Z1AA** | Maintenance of Office Buildings | **31,461** | 2,093 | $20.5B |
| 3 | **F108** | Environmental Remediation | **18,021** | 3,242 | $196.1B |
| 4 | **Y1JZ** | Construction of Miscellaneous Buildings | **18,018** | 3,154 | $326.4B |
| 5 | **Z2DA** | Repair/Alteration of Hospitals & Infirmaries | **14,824** | 2,567 | $45.2B |

¹ Value exposure double-counts an award shared across nearby firms; read it as value-weighted demand exposure, not distinct contract value.

**Cross-check — Top 5 by firm reach (breadth):** Z2AA (4,439) → Z1DA *Maintenance of Hospitals* (3,295) → F108 (3,242) → Y1JZ (3,154) → Y1DA *Construction of Hospitals* (2,765). Office-building repair (Z2AA) tops both volume and breadth; hospital construction/maintenance reaches many firms at lower per-firm density.

---

## 4. Overture Address-Recovery Uplift

The matrix above reflects a recovery pass shipped in **[PR #630](https://github.com/bencrane/core-x/pull/630)**. The root cause it fixed:

> #628's Overture domain-join recovery was scoped to the `SAM∪Prospeo∪Blitz` universe. Candidate firms in `active/companies` outside all three — **83% of the unmapped pool** — were never looked up in Overture and never got a `company_addresses` row, so they were structurally unplaceable regardless of nearby demand.

**Fix:** added a 4th record source to `materialize_company_addresses.py` — GTM-companies orphans (`active/companies` domains in none of SAM/Prospeo/Blitz). They enter `universe_domains` (so the Overture window covers them) and become `dom:`-keyed rows; `winner_postal_code` is recovered through the existing Overture tier (street + ZCTA postal + lat/lon).

| Metric | Before (#628) | After (#630) |
|---|---:|---:|
| `company_addresses` rows | 1,581,103 | 1,584,946 |
| Overture winners | 13,934 | 16,265 (+2,331) |
| Proximity matched firms | 3,621 | **5,759 (+2,138, +59%)** |
| Proximity output rows | 70,849 | 109,121 |
| EPD/LEC matched | 525 (14.4%) | **2,445 (67.2%)** — 4.7× |
| Equipment matched | 3,096 (70.0%) | 3,314 (75.0%) |
| Candidate hit rate | 44.9% | **71.5%** |

The recovery converted EPD/LEC from the weak segment to roughly on par with equipment rental — it was an *address-resolution* gap, not a demand gap.

---

## 5. TAM Verdict & Implications

1. **Validated demand-side TAM: 5,759 firms**, of which **5,180 are golden** (>5 nearby active projects, median 27). The geographic market is real and dense — proceed to LLM catalog-matching.
2. **Both segments are now viable** at scale: Equipment Rental 3,314 matched, EPD/LEC 2,445. The matched set is the input the LLM matching engine consumes.
3. **Seed the matching engine on building work, not horizontal/highway.** Overlap concentrates in office/hospital repair-alteration-maintenance (Z2AA, Z1AA, Z2DA, Z1DA) and miscellaneous + hospital building construction (Y1JZ, Y1DA) plus environmental remediation (F108). These map cleanly onto the `psc_equipment_mapping` reference (#624) — that is where matchmaking will generate signal first.
4. **New ceiling — 2,223 candidates remain unmapped** (EPD/LEC 1,157 + Equipment 1,066): no Overture domain hit at all. Closing the last gap requires a non-Overture resolution source (Prospeo expansion, web enrichment, or manual). This is the next supply-expansion lever, independent of and prior to the matching engine.

---

## Appendix — Reproduction

Read-only probe (no writes, no index changes):

```bash
doppler run --project core-x --config prd -- python3 pipelines/serving/probe_proximity_tam.py
```

Core queries (DuckDB over Lance datasets `co = companies`, `prox = govcon_firm_construction_proximity`, `aw = govcon_active_awards`):

```sql
-- Hit rate: matched firms vs candidate universe
SELECT count(DISTINCT firm_domain) FROM prox;                      -- 5,759 matched
-- denominator: distinct candidate domains tagged equipment_rental / epd_lec_status  -- 8,056

-- Demand density per firm (distinct nearby projects = SUM(nearby_award_count))
SELECT count(*) FILTER (WHERE n = 1)              AS exactly_1,
       count(*) FILTER (WHERE n BETWEEN 2 AND 5)  AS from_2_to_5,
       count(*) FILTER (WHERE n > 5)              AS more_than_5
FROM (SELECT firm_domain, sum(nearby_award_count) n FROM prox GROUP BY 1);

-- PSC distribution: top 5 by overlap volume
SELECT p.psc_code, n.psc_description,
       sum(p.nearby_award_count)     AS total_nearby_award_count,
       count(DISTINCT p.firm_domain) AS firms_in_radius
FROM prox p
LEFT JOIN (SELECT psc_code, any_value(psc_description) psc_description
           FROM aw WHERE psc_code IS NOT NULL GROUP BY 1) n USING (psc_code)
GROUP BY 1,2 ORDER BY total_nearby_award_count DESC LIMIT 5;
```

**Pipelines:** `pipelines/serving/materialize_company_addresses.py` → `pipelines/serving/materialize_firm_construction_proximity.py`.
