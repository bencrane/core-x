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
