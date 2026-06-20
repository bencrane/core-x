# Equipment-Rental ↔ Construction-Prime Geo Match — reference

**Date:** 2026-06-20 (UTC) · Turns the "rental firms can serve construction primes that won awards" inference into a geo-matched named-account target list, plus the zip-centroid primitive it required.

| dataset | SoR | grain | shape | builder |
|---|---|---|---|---|
| `zcta_zip_centroids` | `s3://data-sink/active/zcta_zip_centroids/` | 1 / zcta5 | 33,780 × 5 · 1 BTREE | `pipelines/serving/ingest_zcta_zip_centroids.py` |
| `govcon_equipment_rental_construction_match` | `s3://data-sink/active/govcon_equipment_rental_construction_match/` | 1 / (award × firm) | 1,258,662 × 36 · 4 BTREE + 20 BITMAP | `pipelines/serving/materialize_equipment_rental_construction_match.py` |

Both Lance v2.1, idempotent snapshot-overwrite.

---

## 1. Why — and what's reliable

The thesis: **active construction primes** can be served by **equipment-rental firms physically near the worksite**. Both sides come from reliable sources — FPDS prime data (`govcon_active_awards`) and the SAM registry (`sam_master_entities`). **No FSRS subaward-propensity signal is used** — subaward reporting is sparse/non-compliant, so it is neither a denominator nor a disqualifier here. The rental firm is qualified by being a *real, active, geographically-proximate* firm — **not** by any federal-award history (no prime-win gate).

**Demand is ungated.** Every active construction job needs equipment rental regardless of prime size or whether it filed a subcontracting plan. The match therefore does **not** filter on `business_size` or `has_subcontracting_plan` — both are **carried columns** for optional filter-on-top (e.g. recover the socioeconomic-compliance subset with `WHERE has_subcontracting_plan AND business_size <> 'SMALL BUSINESS'`). Gating on them would collapse demand from ~5,987 active construction awards to ~376 (a compliance artifact, not rental demand).

## 2. Why radius, not metro

`geocode_xwalk` (the in-house rooftop cache) resolves 98.7% of rental-firm zips but only 79.1% of construction *worksite* zips, and **no zip→CBSA/metro crosswalk exists anywhere** in the sink — metro matching was infeasible without a from-scratch ingest. Radius is also the physically-correct model: rental is a delivery business (a circle around a yard), metro hard-fails rural/base worksites (~25–30% of zips) and cross-boundary neighbors. So: **drive-radius via haversine on zip centroids.**

## 3. `zcta_zip_centroids` — the missing primitive

Census 2023 Gazetteer ZCTA internal-point centroids, **33,780 zip-area centroids** covering essentially every populated US zip — closing the worksite-zip gap that `geocode_xwalk` (contractor mailing addresses) left open. Columns: `zcta5`, `lat`, `lon`, `land_sqmi`, `source_version`. BTREE on `zcta5`. The match build unifies it (primary) with a `geocode_xwalk` zip5-rollup (fallback) into one zip→centroid lookup; **99.3%** of match resolutions come from ZCTA.

## 4. `govcon_equipment_rental_construction_match` — the target list

One row per (construction award × rental firm) within road-radius.

**Sides:**
- **Demand** — `govcon_active_awards`: `naics_code LIKE '23%'` · `active_potential` (ALL active construction, any size, plan or not). 5,421 of 5,987 awards geocode (the ~566 unresolved are overseas/military APO + zip-less worksites — out of scope for a US rental match). Join key `LEFT(pop_zip,5)` (**never int-cast** — preserves leading-zero zips).
- **Supply** — `sam_master_entities`: equip-rental NAICS bundle `('532412','532490','532310','532120')` · `is_active` · `country='USA'`. 8,340 firms geocode.

**Distance:** centroid haversine × **1.3** road-circuity factor. Tiers: `local ≤50mi`, `regional ≤150mi`; pairs >150 road-mi dropped. Raw `straight_miles` and `road_miles` are columns so thresholds are query-time.

**Coverage (measured):** 1,258,662 pairs · 5,415 awards · 8,321 firms · **497,701 local pairs** (5,226 awards / 8,118 firms). Demand size mix (filterable): SMALL BUSINESS 4,629 awards · OTHER THAN SMALL 785 awards; with-subcontracting-plan 346 awards.

**Schema (35):**
- *Demand:* `contract_award_unique_key`, `prime_uei`, `prime_name`, `award_naics_code`, `award_naics_desc`, `award_value`, `business_size`, `has_subcontracting_plan`, `awarding_agency_name`, `pop_city`, `pop_state_code`, `pop_zip5`, `demand_centroid_source`.
- *Supply:* `sub_uei`, `sub_name`, `sub_primary_naics`, `sub_city`, `sub_state`, `sub_zip5`, `sub_website` (normalized domain from `sam_master_domains` — blocklist-filtered; ~59.5% of firms; unindexed display/outreach field), `supply_addr_is_hq_pin`, `supply_centroid_source`.
- *Distance:* `straight_miles`, `road_miles`, `tier`.
- *Rental-firm designations (11, bool — SAM Reps & Certs lineage, see §4.1):* `sub_sdvosb`, `sub_veteran_owned`, `sub_wosb`, `sub_edwosb`, `sub_woman_owned`, `sub_hubzone`, `sub_8a`, `sub_self_cert_sdb`, `sub_minority_owned`, `sub_jv_wosb`, `sub_any_designation`.

**Indexes (24):** BTREE `sub_uei`, `prime_uei`, `road_miles`, `award_value`; BITMAP `contract_award_unique_key` (5,415 distinct, long key → BITMAP not BTREE), `tier`, `supply_addr_is_hq_pin`, `sub_state`, `pop_state_code`, `supply_centroid_source`, `demand_centroid_source`, `business_size`, `has_subcontracting_plan`, and all 11 `sub_*` designation flags.

### 4.1 Rental-firm designation flags (the diverse-vendor cross-filter)

Each pair carries the **rental firm's** socioeconomic designations, decoded from the firm's SAM Reps & Certs (`business_types` + `sba_business_types_string`) via the validated crosswalk in `sam_business_type_code_dict`. This is the **SAM current-registry lineage** — the firm's live self-cert — NOT the FPDS award-stamped flags on `govcon_active_awards` (those are prime-keyed, and rental firms are mostly not primes). `8(a)`/`HUBZone`/`EDWOSB` are **floors** (SBA-cert string ~13% populated, ~68% recall); the `business_types` self-certs (SDVOSB/veteran/WOSB/woman/minority/SDB) have no ceiling.

Of the 8,321 matched firms: **5,400 carry ≥1 designation** — SDVOSB 1,134 · WOSB 1,642 · woman-owned 1,835 · veteran 1,465 · minority 2,492 · HUBZone ≥212 · 8(a) ≥209. A designated rental firm local to a construction award is a **double-value target**: equipment capability **and** small-business/socioeconomic subcontracting credit for the prime.

## 5. Caveats that ship with the data

1. **`supply_addr_is_hq_pin` (systematic):** SAM carries ONE registered HQ. National chains (United Rentals, Sunbelt, Herc, …) register a corporate HQ, not their branch yards — flagged rows (6,501 pairs) are **advisory**; the match is reliable for single-location/regional firms. True fix needs a branch/yard dataset.
2. **Worksite-grade, not parcel-grade:** construction PoP zip can be a base/installation centroid → radius is metro-accurate, not drive-time-precise. The ×1.3 factor approximates roads; it is not a routing engine. Don't market sub-25mi tiers as exact.
3. **Geo-resolvable population:** 5,421/5,987 demand (US worksites), 8,340/8,431 US-active supply. Overseas/military and zip-less rows are excluded; report match rates against the resolvable base.
4. **`centroid_source`** flags `zcta` vs `geocode_xwalk` so coverage is visible, not silent.

## 6. Query patterns

```sql
-- For one construction prime award: rental firms within local reach, single-location first
SELECT sub_name, sub_city, sub_state, road_miles, supply_addr_is_hq_pin
FROM govcon_equipment_rental_construction_match
WHERE contract_award_unique_key = :award AND tier='local'
ORDER BY supply_addr_is_hq_pin, road_miles;

-- Diverse-vendor cross-filter: SDVOSB rental firms within 50mi of a construction award (zero-join)
-- (1,119 distinct firms across 4,214 awards in the live table)
SELECT contract_award_unique_key, prime_name, sub_name, sub_city, sub_state, road_miles
FROM govcon_equipment_rental_construction_match
WHERE sub_sdvosb AND tier='local'
ORDER BY award_value DESC;

-- For one rental firm: every construction award it can reach, biggest demand first
SELECT prime_name, award_value, pop_city, pop_state_code, road_miles, tier
FROM govcon_equipment_rental_construction_match
WHERE sub_uei = :uei ORDER BY award_value DESC;

-- Demand heat: construction awards by state with reachable local rental supply
SELECT pop_state_code, count(DISTINCT contract_award_unique_key) awards,
       count(DISTINCT sub_uei) local_firms
FROM govcon_equipment_rental_construction_match WHERE tier='local' GROUP BY 1 ORDER BY 2 DESC;
```

## 7. Rebuild

```bash
doppler run --project core-x --config prd -- python pipelines/serving/ingest_zcta_zip_centroids.py
doppler run --project core-x --config prd -- python pipelines/serving/materialize_equipment_rental_construction_match.py
```
ZCTA refresh is annual (Census gazetteer vintage); the match refreshes off `govcon_active_awards` + `sam_master_entities` rebuilds.
