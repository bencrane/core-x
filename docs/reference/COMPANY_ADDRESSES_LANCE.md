# `company_addresses` Lance — quick reference

**SoR:** `s3://data-sink/active/company_addresses/`
**Build:** [pipelines/serving/materialize_company_addresses.py](../../pipelines/serving/materialize_company_addresses.py)
**Grain:** 1 row per firm. PK = `entity_key` = `uei` when SAM-resolved, else `'dom:' + domain_norm`.
**Live rows:** 1,574,607

## What it is

A consolidated firm-address lookup. Joins SAM + Prospeo + Blitz into one Lance so any
downstream answering "where is this firm physically located" hits one dataset instead of
coalescing across three sources.

## Universe (UNION, deduped by domain_norm in precedence order)

| Source | Rows | Notes |
|---|---|---|
| `sam_master_entities` | 1,541,566 | Every UEI in SAM |
| `prospeo_company_export` orphans | 30 | Prospeo domains NOT in SAM |
| `firmographics_blitz` orphans | 30,192 + 2,819 stubs | Blitz domains NOT in SAM AND NOT in Prospeo |

## Address-winner precedence

1. **SAM physical** (street + line2 + city + state + zip + country + congressional district) — 97.9% of rows win here
2. **SAM mailing** — fallback when physical empty
3. **Prospeo** — city/state/country + free-text `prospeo_raw_address`
4. **Blitz HQ** — city/state only (no street, no postal) — floor of last resort

**Whole-address pick.** No field-level mixing across sources. If SAM physical has city, every
winner field comes from SAM physical (never a Prospeo state grafted onto a SAM city).
`address_source` stamps which source won (`sam_physical` / `sam_mailing` / `prospeo` / `blitz` / `none`).

## Bridge keys

- `uei` (BTREE) — bridges to `sam_master_entities`, the geo-match MV, every UEI-keyed table
- `domain_norm` (BTREE) — bridges to `firmographics_blitz`, `equipment_catalog`,
  `industries_served`, `equipment_provider`, `equipment_finance_candidates`, `prospeo_company_export`

47.16% of rows have `domain_norm` populated. Either or both may be NULL per row (non-SAM
orphans have `uei=NULL`; SAM firms without a registered website have `domain_norm=NULL`).

## Column layout (42 cols total)

| Group | Columns |
|---|---|
| Identity / bridge | `entity_key` (PK), `uei`, `domain_norm`, `legal_business_name`, `primary_naics` |
| Winner (the picked address) | `address_source`, `winner_line_1/2`, `winner_city`, `winner_state`, `winner_postal_code`, `winner_zip_plus_4`, `winner_country_code`, `winner_congressional_district` |
| Presence flags | `had_sam_physical`, `had_sam_mailing`, `had_prospeo`, `had_blitz` |
| SAM physical (verbatim) | `sam_physical_line_1/2`, `sam_physical_city/state/postal_code/zip_plus_4/country_code/congressional_district` |
| SAM mailing (verbatim) | `sam_mailing_line_1/2`, `sam_mailing_city/state/postal_code/zip_plus_4/country` |
| Prospeo (verbatim) | `prospeo_city/state/country/country_code/raw_address` |
| Blitz HQ (verbatim) | `blitz_hq_city/state/region` |
| Lineage | `materialized_at` |

## Indexes

- **BTREE:** `entity_key`, `uei`, `domain_norm`, `primary_naics`, `legal_business_name`
- **BITMAP:** `address_source`, `winner_state`, `winner_country_code`, `had_sam_physical`, `had_sam_mailing`, `had_prospeo`, `had_blitz`

## Reference queries

Pull a firm's full address with source provenance:

```sql
SELECT uei, legal_business_name, winner_city, winner_state, winner_postal_code,
       winner_line_1, address_source,
       blitz_hq_city, blitz_hq_state   -- override if Blitz is more current
FROM company_addresses
WHERE uei = 'V6JSJB6E6J24';            -- United Rentals
```

Join across the offerings stack (cross-source firm dossier):

```sql
SELECT ca.legal_business_name, ca.winner_city, ca.winner_state, ca.address_source,
       isv.industries_served, ec.equipment_item_names, ep.is_equipment_provider
FROM company_addresses ca
LEFT JOIN industries_served isv      USING (domain_norm)
LEFT JOIN equipment_catalog ec       USING (domain_norm)
LEFT JOIN equipment_provider ep      USING (domain_norm)
WHERE ca.primary_naics IN ('532412','532490','532420','532120','532310','532411');
```

Geo density — how many active construction awards are within 50 miles of this rental firm:

```sql
SELECT count(*) FILTER (WHERE tier='local')    AS local_awards_50mi,
       count(*) FILTER (WHERE tier='regional') AS regional_awards_150mi
FROM govcon_equipment_rental_construction_match
WHERE sub_uei = 'V6JSJB6E6J24';
```

## Rebuild

```bash
doppler run -p core-x -c prd -- python pipelines/serving/materialize_company_addresses.py
doppler run -p core-x -c prd -- python pipelines/serving/materialize_company_addresses.py --verify
```

Idempotent snapshot-overwrite. Pulls latest SAM + Prospeo + Blitz state on every build.

---

# Geo / location lookup Lance datasets

Three companion Lance datasets that turn a `company_addresses` row into a real point on a
map (lat/lon). Use whichever has the granularity you need.

| Lance | Rows | Grain | Granularity | Use when |
|---|---|---|---|---|
| `zcta_zip_centroids` | 33,780 | 1 row per US ZCTA-5 | Zip-tabulation centroid (county-grade) | Plotting "this firm sits in ZCTA X" on a map. What the rental-construction MV uses for radius matching. |
| `geocode_xwalk` | 369,218 | 1 row per geocoded street address | Rooftop where available, else street-segment | You have an actual `street + city + state + zip5` and need precise lat/lon. |
| `overture_places` | 16,273,123 | 1 row per POI | Real-world point-of-interest | Live-map / POI search — names, categories, lat/lon, phone, domain. Open POI dataset (Meta / Microsoft / Linux Foundation / AWS). |

## `s3://data-sink/active/zcta_zip_centroids/`

Census ZCTA (Zip Code Tabulation Area) centroids. The canonical "zip5 → lat/lon" lookup.

| Column | Type | Meaning |
|---|---|---|
| `zcta5` | string | 5-digit ZCTA code (the lookup key) |
| `lat` | double | Centroid latitude |
| `lon` | double | Centroid longitude |
| `land_sqmi` | double | ZCTA land area in square miles |
| `source_version` | string | Census vintage stamp |

**Coverage gap:** military/point zips (e.g. `35898` Redstone Arsenal) are NOT in ZCTA. The
serving builds fall back to `geocode_xwalk` and then to a ZIP3-prefix centroid for those.

## `s3://data-sink/active/geocode_xwalk/`

Address-level geocoder cache. 369K rooftop/street-segment geocodes with provenance.

| Column | Type | Meaning |
|---|---|---|
| `addr_hash` | string | sha256 of the normalized address — the row PK |
| `street`, `city`, `state`, `zip5` | string | The input address components |
| `latitude`, `longitude` | double | Resolved coordinates |
| `match_type` | string | Rooftop / interpolated / centroid / etc. — geocoder's confidence tier |
| `matched_address` | string | The address the geocoder normalized to (sanity check) |
| `geocode_source` | string | Which provider / pipeline produced this row |
| `geocoded_at` | string | When it was geocoded |

## `s3://data-sink/active/overture_places/`

Overture Maps Foundation POI dataset. 16M points — every business / venue / public POI
the consortium has open-sourced. This is your **live-map dataset**.

| Column | Type | Meaning |
|---|---|---|
| `id` | string | Overture global place ID |
| `latitude`, `longitude` | double | The point on the map |
| `hilbert` | uint32 | Hilbert-curve geo-key (spatial-locality sort accelerator) |
| `region`, `locality`, `postcode` | string | Hierarchical place reference (state / city / zip) |
| `name` | string | Place name |
| `category`, `taxonomy` | string | Overture category + the full taxonomy path |
| `confidence` | float | Overture's confidence score (0..1) |
| `domain` | string | Place website domain (bridge → firmographics_blitz!) |
| `phone` | string | Listed phone |
| `street` | string | Street address |

The `domain` column makes it joinable to `firmographics_blitz` / `company_addresses` /
the offerings stack — same key as everything else in the GTM plane.

## Reference queries

Pin every rental firm on a map by ZCTA centroid:

```sql
SELECT ca.uei, ca.legal_business_name, ca.winner_city, ca.winner_state,
       z.lat, z.lon
FROM company_addresses ca
JOIN zcta_zip_centroids z ON ca.winner_postal_code = z.zcta5
WHERE ca.primary_naics IN ('532412','532490','532420','532120','532310','532411');
```

Promote a firm to street-rooftop precision when geocode_xwalk has it:

```sql
SELECT ca.uei, ca.legal_business_name,
       coalesce(gx.latitude, z.lat)   AS lat,
       coalesce(gx.longitude, z.lon)  AS lon,
       CASE WHEN gx.latitude IS NOT NULL THEN gx.match_type ELSE 'zcta_centroid' END AS precision
FROM company_addresses ca
LEFT JOIN geocode_xwalk gx
  ON gx.zip5 = ca.winner_postal_code
 AND gx.street = ca.winner_line_1
 AND gx.state = ca.winner_state
LEFT JOIN zcta_zip_centroids z ON z.zcta5 = ca.winner_postal_code
WHERE ca.uei = 'V6JSJB6E6J24';
```

Find every Overture POI within a 5-mile box of a coordinate, filtered by category:

```sql
-- Pre-filter with a lat/lon box (fast), then sharpen with haversine.
WITH bbox AS (
  SELECT 33.0 AS lat0, -97.0 AS lon0,
         5.0/69.0 AS dlat, 5.0/(69.0*cos(radians(33.0))) AS dlon
)
SELECT op.id, op.name, op.category, op.latitude, op.longitude,
       3958.8*2*asin(sqrt(
         pow(sin(radians(op.latitude  - bbox.lat0)/2),2)
       + cos(radians(bbox.lat0))*cos(radians(op.latitude))
         * pow(sin(radians(op.longitude - bbox.lon0)/2),2))) AS miles
FROM overture_places op, bbox
WHERE op.latitude  BETWEEN bbox.lat0 - bbox.dlat AND bbox.lat0 + bbox.dlat
  AND op.longitude BETWEEN bbox.lon0 - bbox.dlon AND bbox.lon0 + bbox.dlon
  AND op.category LIKE 'construction%'
ORDER BY miles
LIMIT 50;
```

Find every Overture POI whose domain matches a firm in `firmographics_blitz`:

```sql
SELECT op.name, op.category, op.latitude, op.longitude, op.phone,
       fb.industry, fb.size, fb.hq_city
FROM overture_places op
JOIN firmographics_blitz fb ON op.domain = fb.domain_norm
WHERE op.category LIKE '%rental%';
```

## Precision tiers — which one to use

| Tier | Source | When | Caveat |
|---|---|---|---|
| Rooftop | `geocode_xwalk` (match_type = rooftop) | You need a parcel-accurate pin | 369K rows — coverage gaps |
| Street-segment | `geocode_xwalk` (interpolated) | Rooftop unavailable, address is well-formed | Can be off by a few houses |
| ZCTA centroid | `zcta_zip_centroids` | Coarse map placement, density binning | County-grade — don't claim parcel accuracy |
| ZIP3 centroid | derived in build (avg of all ZCTAs sharing 3-digit prefix) | Military/point zips not in ZCTA | Last-resort; SCF/metro-grade only |
| POI lat/lon | `overture_places.latitude/longitude` | Live-map UX, category-filtered search | A firm's HQ pin may differ from its Overture POI pin (national chains have one HQ, many POIs) |
