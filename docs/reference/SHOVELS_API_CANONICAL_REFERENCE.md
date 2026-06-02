# Shovels.ai API — Canonical Reference

> **Authored from independent live verification on 2026-05-29.**
> Base URL: `https://api.shovels.ai/v2` · API version: **2.0.0** (OpenAPI 3.1.0).
> This document **supersedes a prior reference that was deleted for material staleness** (wrong tag vocabulary, wrong 422 error-body shape, missing endpoints, outdated counts). Every claim here was re-derived from scratch against the live API, the live OpenAPI spec, and the live docs on the authored date.

## Verifiability legend

Every non-obvious claim is tagged with its basis:

- **`[probe]`** — observed directly in a live API response on 2026-05-29 (ground truth for runtime behavior, response shape, credit cost).
- **`[openapi]`** — from `https://api.shovels.ai/v2/openapi.json` fetched 2026-05-29 (authoritative for request surface, required-ness, enums).
- **`[docs:<url>]`** — from Shovels documentation (authoritative only for things not observable via API — pricing, CLI, EDL, corporate).

**Confidence tiers** are called out explicitly: `VERIFIED-LIVE` (I hit it), `DOCUMENTED-ONLY` (doc claim, not independently runtime-verified), `UNKNOWN` (could not verify).

**Reconciliation rule applied throughout:** where a live response carried fields absent from the spec, the live response wins for response shape (noted inline). Where the spec listed params absent from the docs, the spec wins for the request surface.

---

## 1. Platform overview & corporate status

Shovels, Inc. is a third-party data platform that aggregates, cleans, geocodes, enriches, and structures **U.S. building-permit, contractor, property, address, and resident data** sourced from thousands of city/county permitting jurisdictions. It positions itself as "the intelligence layer for the built world." Access is offered through three products: **Shovels Online** (web app), the **Shovels API** (this document, REST/v2), and **EDL** (Enterprise Data License — bulk database delivery). `[docs:https://www.shovels.ai/ | docs:https://docs.shovels.ai/llms.txt]`

**Corporate status (VERIFIED via web, 2026-05-29):**
- **Independent and operating; not acquired.** Shovels is the *acquirer* in recent M&A, not a target.
- **Raised a $5M seed round in mid-2025** (Commercial Observer, June 2025); reports tripling revenue and growing from 6 → 11 employees since. `[docs:https://commercialobserver.com/2025/06/shovels-proptech-permits/]`
- **Acquired ReZone on 2026-01-07** — an AI company that tracks city/local-government meeting *decisions* (zoning changes, project discussions, approval timelines). This is the origin of "Decisions"/meeting-intelligence data. `[docs:https://www.shovels.ai/blog/shovels-acquires-rezone/]`
- **Partnership with Precisely** (2025/2026) to connect permit data with property/location datasets. `[docs:https://www.einpresswire.com/article/913853011/...]`
- **API continuity impact: none observed.** The v2 REST surface is stable; the ReZone acquisition added a *new* data domain (meeting decisions) that — as of this writing — is **not exposed through the v2 REST API** (see §15).

**Data scale (most recent published release, V2.1.8, dated 2026-05-02):** `[docs:https://docs.shovels.ai/release-notes/release-notes.md]`
- **141,032,194 permits** (+5.4% over prior release)
- **2,638,372 contractors** (up from 2,550,254)
- **105.2M permits with a linked address (74.6%, +0.8pp)**
- 32 newly covered jurisdictions in the latest release (Portland OR ~1.6M permits, Orlando FL ~1.0M, Twin Cities suburbs).
- Permit distribution by type: electrical ~1.4M, plumbing ~1.2M, HVAC ~942K (leading derived types).

**Live data-release timestamp (VERIFIED-LIVE):** `GET /v2/meta/release` → `{"released_at":"2026-05-15"}` `[probe]`. Note this is the *data snapshot* date and is **13 days newer** than the V2.1.8 release-note publication date (2026-05-02). The two timestamps measure different things (data freshness vs. release-note authoring) and should not be conflated.

---

## 2. Authentication & transport

- **Transport:** HTTPS/SSL only, UTF-8, HTTP/2 (server: `uvicorn`). All read calls are **GET**. `[probe | openapi]`
- **Auth:** single header **`X-API-Key: <key>`**. No OAuth, no bearer token, no query-param key. `[openapi | probe]`
- **Missing/invalid key → HTTP 403** with body `{"detail":"Not authenticated"}` (VERIFIED-LIVE — note: **403, not 401**, despite the docs' generic table listing 401 for auth). `[probe]`
- **Dates:** all date params and date fields are `YYYY-MM-DD` strings. Month-only (`YYYY-MM`) is **rejected** with a 422 (`input is too short`). `[probe]`
- **No request body** on any endpoint — everything is path + query params. `[openapi]`

Example (key injected via secrets manager; never echoed):

```sh
curl -sS -H "X-API-Key: $SHOVELS_API_KEY" "https://api.shovels.ai/v2/meta/release"
# -> {"released_at":"2026-05-15"}
```

---

## 3. Complete endpoint inventory

**30 endpoints**, all GET. `[openapi]` "Paginated" = returns the cursor envelope (§11) and honors `cursor`. "Credit cost" is **empirically measured** from the `X-Credits-Request` response header on 2026-05-29 `[probe]`; "1/record" means the header equaled the number of `items` returned.

| Method | Path | Required params | Returns | Paginated | Credit cost |
|---|---|---|---|---|---|
| GET | `/permits/search` | `permit_from`, `permit_to`, `geo_id` | Permit objects | Yes | **1 / record** |
| GET | `/permits` | `id` (array) | Permit objects (by id) | Yes (envelope) | **1 / record** |
| GET | `/contractors/search` | `permit_from`, `permit_to`, `geo_id` | Contractor objects | Yes | **1 / record** |
| GET | `/contractors` | `id` (array) | Contractor objects (by id) | Yes (envelope) | **1 / record** |
| GET | `/contractors/{id}/permits` | `id` (path) | Permit objects | Yes | **1 / record** |
| GET | `/contractors/{id}/employees` | `id` (path) | Employee objects (PII) | Yes | **1 / record** |
| GET | `/contractors/{id}/metrics` | `id`, `metric_from`, `metric_to`, `property_type`, `tag` | Monthly contractor metrics | Yes | **1 / record** (404 if none) |
| GET | `/addresses/search` | `q` | Address objects + `geo_id` | **No** (see §11) | **FREE** |
| GET | `/addresses/{geo_id}/residents` | `geo_id` (path) | Resident objects (PII) | Yes | **1 / record** |
| GET | `/addresses/{geo_id}/metrics/current` | `geo_id`, `tag` | Current address metrics | Yes | 1 / record (404 if none) |
| GET | `/addresses/{geo_id}/metrics/monthly` | `geo_id`, `metric_from`, `metric_to`, `tag` | Monthly address metrics | Yes | 1 / record (404 if none) |
| GET | `/cities/search` | `q` | Geo entities + `geo_id` | No¹ | **FREE** |
| GET | `/cities` | `geo_id` | City detail (nested hierarchy) | No (single obj in envelope) | **FREE** |
| GET | `/cities/{geo_id}/metrics/current` | `geo_id`, `property_type`, `tag` | Current city metrics | Yes | 1 / record (404 if none) |
| GET | `/cities/{geo_id}/metrics/monthly` | `geo_id`, `metric_from`, `metric_to`, `property_type`, `tag` | Monthly city metrics | Yes | 1 / record (404 if none) |
| GET | `/counties/search` | `q` | Geo entities + `geo_id` | No¹ | **FREE** |
| GET | `/counties` | `geo_id` | County detail | No | **FREE** |
| GET | `/counties/{geo_id}/metrics/current` | `geo_id`, `property_type`, `tag` | Current county metrics | Yes | 1 / record (404 if none) |
| GET | `/counties/{geo_id}/metrics/monthly` | `geo_id`, `metric_from`, `metric_to`, `property_type`, `tag` | Monthly county metrics | Yes | 1 / record (404 if none) |
| GET | `/jurisdictions/search` | `q` | Geo entities + `geo_id` | No¹ | **FREE** |
| GET | `/jurisdictions` | `geo_id` | Jurisdiction detail | No | **FREE** |
| GET | `/jurisdictions/{geo_id}/metrics/current` | `geo_id`, `property_type`, `tag` | Current jurisdiction metrics | Yes | 1 / record (404 if none) |
| GET | `/jurisdictions/{geo_id}/metrics/monthly` | `geo_id`, `metric_from`, `metric_to`, `property_type`, `tag` | Monthly jurisdiction metrics | Yes | 1 / record (404 if none) |
| GET | `/states/search` | `q` | State objects | Yes | **FREE** (assumed²) |
| GET | `/zipcodes/search` | `q` | `{geo_id, state}` | Yes | **FREE** |
| GET | `/list/tags` | — | Tag vocabulary | Yes (envelope; 1 page) | **FREE** |
| GET | `/list/zip` | — | `{zip_code}` list | Yes | **FREE** |
| GET | `/meta/coverage` | `geo_type`, `geo_id`, `date_from`, `date_to` | Per-field fill-rate report | No (own envelope) | **FREE** |
| GET | `/meta/release` | — | `{released_at}` | No | **FREE** |
| GET | `/usage` | — | Credit usage summary | No | **FREE** |

¹ `/cities|counties|jurisdictions/search` accept `q` plus (per `[openapi]`) `cursor`/`size`; behavior of `size`/`cursor` not independently confirmed for these three (treat like the address-search caveat in §11 until verified). State/zip search **do** accept `size`/`cursor` per spec.
² `/states/search` returned no credit header on a 200 with results; classified FREE by analogy to the other free metadata/search endpoints (not explicitly isolated — see §16).

**Empirical credit-cost summary (VERIFIED-LIVE `[probe]`):**
- **Credit-consuming (1 credit per returned record):** `permits/search`, `permits`, `contractors/search`, `contractors`, `contractors/{id}/permits`, `contractors/{id}/employees`, `contractors/{id}/metrics`, `addresses/{geo_id}/residents`, and all `*/metrics/current` and `*/metrics/monthly`. (Measured: `size=5` → `X-Credits-Request: 5`; `size=2` → `2`; `size=1` → `1`.)
- **FREE (no `X-Credits-*` headers emitted):** `addresses/search`, `cities/search`, `counties/search`, `jurisdictions/search`, `zipcodes/search`, `cities` / `counties` / `jurisdictions` detail, `list/tags`, `list/zip`, `meta/coverage`, `meta/release`, `usage`.
- **Billing model:** "Each record returned counts as one credit." `[docs:llms.txt]` — confirmed empirically: the header tracks `len(items)`, not request count. **Search/resolution endpoints are free; data-bearing record endpoints bill per row.** This means pagination size directly equals spend on billable endpoints.

---

## 4. Response envelopes

### 4.1 Paginated envelope (the dominant shape) `[probe | openapi]`

Every list endpoint returns this object (even by-id lookups, which wrap a single result in `items`):

```json
{
  "items": [ ... ],
  "size": 50,
  "next_cursor": "Qk8gQklSRFNPTkc" | null,
  "total_count": null | {"value": 10000, "relation": "gte"}
}
```

- `items` — array of records.
- `size` — number of items in *this* page (echoes returned count, not the requested `size`).
- `next_cursor` — opaque base64 token; `null` when exhausted.
- `total_count` — **`null` unless `include_count=true` was passed AND you are on the first page (no cursor).** See §5.

### 4.2 Non-paginated envelopes

- `GET /meta/release` → `{"released_at":"2026-05-15"}` `[probe]`
- `GET /usage` → see §10.1.
- `GET /meta/coverage` → `{"items":[CoverageItem, ...]}` — its own `CoverageResponse` envelope, **no** `size`/`next_cursor`/`total_count`. `[probe | openapi]`
- `GET /cities|counties|jurisdictions?geo_id=...` (detail) → still wrapped as `{items:[...], size, next_cursor, total_count}` with a single detail object inside `items`. `[probe]`

---

## 5. The `include_count` parameter (RESOLVED)

- **Accepted by exactly three endpoints** `[openapi]`: `permits/search`, `contractors/search`, `contractors/{id}/permits`. (Not on metrics, residents, employees, geo searches, or `/permits`/`/contractors` by-id.)
- **Response field name: `total_count`** (NOT `count`, NOT `total`). `[probe]`
- **Structure: an OBJECT, not an integer** — Elasticsearch-style: `[probe | openapi schema `TotalCount`]`

```json
"total_count": {"value": 10000, "relation": "gte"}   // capped/approximate
"total_count": {"value": 6,     "relation": "eq"}    // exact
```

  - `relation: "eq"` → `value` is the exact match count.
  - `relation: "gte"` → the true count is *at least* `value`; the engine caps reported counts at **10000** (observed: broad CA queries all report `{value:10000, relation:"gte"}`). **Do not treat a `gte` value as a real total** — it is a ceiling.
- **Only populated on the first page** (no `cursor`). On subsequent pages `total_count` is `null`. `[openapi description | probe]`
- Without `include_count`, `total_count` is `null`. `[probe]`

---

## 6. Data model & full field dictionary

Types and nullability below are from the live `[probe]` responses, cross-checked against `[openapi]` schemas. "nullable" = observed `null` or declared `anyOf[..., null]`. Most enrichment fields are **frequently null** (see §13).

### 6.1 Permit object (`PermitsRead`) — 35 fields `[probe]`

Returned identically by `permits/search`, `permits?id=`, and `contractors/{id}/permits`.

| Field | Type | Nullable | Notes |
|---|---|---|---|
| `id` | string | no | 16-hex-char permit id, e.g. `001716eff25f78ef`. Stability: §12. |
| `number` | string | yes | Jurisdiction's permit number, e.g. `SOLR25-0166`. |
| `description` | string | yes | Free-text scope of work. |
| `jurisdiction` | string | yes | Issuing jurisdiction name (uppercased). |
| `job_value` | int | yes | Declared job value (USD). |
| `type` | string | yes | Raw permit type, e.g. `Photovoltaic`. |
| `subtype` | string | yes | Raw subtype. |
| `fees` | int/number | yes | Permit fees. |
| `status` | string | yes | One of `final`, `in_review`, `inactive`, `active`. |
| `file_date` | date | yes | |
| `issue_date` | date | yes | |
| `final_date` | date | yes | |
| `start_date` | date | yes | Drives `permit_from`/`permit_to` filtering (see §7). |
| `end_date` | date | yes | |
| `total_duration` | int | yes | Days. |
| `construction_duration` | int | yes | Days. |
| `approval_duration` | int | yes | Days. |
| `inspection_pass_rate` | number | yes | 0–100. Coverage dropped to ~3% in V2.1.7 (§13). |
| `contractor_id` | string | yes | FK to contractor; ~46% fill in CA (§13). |
| `tags` | array[string] | yes | Subset of the 22-tag vocabulary (§8.3), e.g. `["battery","electrical","solar"]`. |
| `address` | object | yes | Nested (see 6.2). |
| `geo_ids` | object | yes | `{address_id, city_id, county_id, jurisdiction_id}` — all nullable strings. `[openapi GeoIdsRead]` |
| `property_census_tract` | string | yes | |
| `property_congressional_district` | string | yes | |
| `property_type` | string | yes | Derived; enum in §7. |
| `property_type_detail` | string | yes | |
| `property_legal_owner` | string | yes | **PII-ish** (owner name). |
| `property_owner_type` | string | yes | |
| `property_lot_size` | int | yes | sq ft. |
| `property_building_area` | int | yes | sq ft. |
| `property_story_count` | int | yes | |
| `property_unit_count` | int | yes | |
| `property_year_built` | int | yes | |
| `property_assess_market_value` | int | yes | Assessed market value. |

> **Spec note:** the V2.1.7 release notes mention a `description_derived` field added to 81M permits `[docs]`. It was **not present** in any live permit object I probed `[probe]`, and is **not** in the `PermitsRead` OpenAPI schema `[openapi]`. Treat `description_derived` as **EDL/Online-only or not-yet-on-v2-REST** (UNKNOWN whether it surfaces via a param I didn't trigger).

### 6.2 Nested permit `address` object `[probe]`

```json
"address": {
  "street_no": "73852", "street": "MOJAVE DESERT DR", "city": "PALM DESERT",
  "county": "RIVERSIDE", "zip_code": "92211", "zip_code_ext": null,
  "state": "CA", "jurisdiction": "PALM DESERT", "latlng": [33.781786, -116.377099]
}
```

Note: the permit-nested address uses **`latlng: [lat, lng]`** (array). The *standalone* address-search object instead uses scalar **`lat`/`long`** (§6.6). This is a real shape divergence between the two address representations. `[probe]`

### 6.3 Contractor object (`ContractorsRead`) — 34 fields `[probe]`

Returned identically by `contractors/search` and `contractors?id=` (verified field-for-field identical, 34 keys each).

| Field | Type | Nullable | Notes |
|---|---|---|---|
| `id` | string | no | 10-char id, e.g. `000e9PLZDb`. **Regenerated in V2.1.7** (§12). |
| `license` | string | yes | Contractor license number. |
| `name` | string | yes | Person/principal name. |
| `business_name` | string | yes | |
| `business_type` | string | yes | Enum: `JointVenture`, `Corporation`, `Partnership`, `Limited Liability`, `Sole Owner`. `[openapi]` |
| `classification` | string/array | yes | Raw classification. |
| `classification_derived` | array[string] | yes | Standardized; enum in §7. Supersedes deprecated `contractor_classifications`. |
| `license_issue_date` | date | yes | |
| `license_exp_date` | date | yes | |
| `license_inact_date` | date | yes | |
| `license_act_date` | date | yes | |
| `primary_phone` | string | yes | **PII.** |
| `primary_email` | string | yes | **PII.** |
| `phone` | string | yes | **PII.** |
| `email` | string | yes | **PII.** |
| `website` | string | yes | |
| `dba` | string | yes | "Doing business as". |
| `sic` | string | yes | SIC code. |
| `naics` | string | yes | NAICS code. |
| `linkedin_url` | string | yes | |
| `revenue` | string/number | yes | Firmographic. |
| `employee_count` | string | yes | **Range string**, e.g. `"11 to 25"`, `"251 to 500"` — not an integer. `[probe]` |
| `primary_industry` | string | yes | |
| `review_count` | int | yes | |
| `rating` | number | yes | |
| `status_tally` | object | yes | Per-status permit counts, e.g. `{"final":3}`. Controlled by `include_tallies` (§7.3). |
| `tag_tally` | object | yes | Per-tag permit counts, e.g. `{"electrical":3}`. Controlled by `include_tallies`. |
| `permit_count` | int | yes | Lifetime permit count. |
| `avg_job_value` | int | yes | |
| `total_job_value` | int | yes | |
| `avg_construction_duration` | int | yes | Days. |
| `avg_inspection_pass_rate` | number | yes | |
| `first_seen_date` | date | yes | **All reset to 2025-05-19 … 2026-03-28 range in V2.1.7** (§12) — do not treat as true historical first-seen. |
| `address` | object | yes | Same nested shape as 6.2 with extra `address_id`; fields often null at search scope (only `state` reliably populated). `[probe]` |

### 6.4 Employee object (`Employees`) — 23 fields, **PII** `[probe | openapi]`

From `contractors/{id}/employees`. Required: `id`, `contractor_id`. All other fields nullable. Example with PII redacted:

```json
{
  "id": "<32-hex-id>", "contractor_id": "02nKdTTE2k",
  "name": "<REDACTED>", "phone": "<REDACTED>", "email": "<REDACTED>",
  "business_email": "<REDACTED>", "linkedin_url": "<REDACTED>",
  "street_no": null, "street": null, "city": "SAN DIEGO", "state": "CA",
  "zip_code": "92101", "zip_code_ext": null,
  "gender": "M", "age_range": null, "is_married": null, "has_children": null,
  "income_range": null, "net_worth": "More than $1,000,000",
  "homeowner": null, "job_title": "Founder", "seniority_level": "Cxo",
  "department": "Executive"
}
```

Field set `[openapi]`: `id, contractor_id, name, street_no, street, city, zip_code, zip_code_ext, state, phone, email, business_email, linkedin_url, homeowner, gender, age_range, is_married, has_children, income_range, net_worth, job_title, seniority_level, department`. Many small/owner-operator contractors return an **empty** employee list (`items:[]`, `total_count:null`). `[probe]`

### 6.5 Resident object (`ResidentsRead`) — 13 fields, **PII** `[probe | openapi]`

From `addresses/{geo_id}/residents`. **No required fields** (all `anyOf[..., null]`). PII redacted:

```json
{
  "name": "<REDACTED>", "personal_emails": "<REDACTED>", "phone": "<REDACTED>",
  "linkedin_url": null, "net_worth": null, "income_range": null,
  "is_homeowner": null,
  "street_no": "1", "street": "BEACH ST", "city": "SAN FRANCISCO",
  "state": "CA", "zip_code": "94133", "zip_code_ext": null
}
```

Field set `[openapi]`: `name, personal_emails, phone, linkedin_url, net_worth, income_range, is_homeowner, street_no, street, city, state, zip_code, zip_code_ext`. Note `personal_emails` is typed as a **single string** (`anyOf[string,null]`), not an array, despite the plural name. `[openapi | probe]` This endpoint **only accepts an address-type `geo_id`**; passing a city `geo_id` returns 422 "No address metrics found." style errors — resolve via `addresses/search` first. `[probe]`

### 6.6 Geo entities

- **`addresses/search` item** (`AddressesRead`) `[probe]`: `street_no, street, city, county (nullable), zip_code, zip_code_ext, state, jurisdiction (nullable), lat, long, geo_id, name`. Uses scalar `lat`/`long` (cf. 6.2's `latlng` array).
- **`cities/search` / `counties/search` / `jurisdictions/search` item** (`GeoEntitiesRead`) `[probe | openapi]`: `{geo_id, name, state}` (3 fields; `name`/`state` required).
- **`zipcodes/search` item** (`ZipCodesView`) `[probe]`: `{geo_id, state}` where `geo_id` is the literal 5-digit ZIP string (e.g. `"94133"`).
- **`states/search` item** (`States`): state objects (returned an empty page for fuzzy `q=Cali`; state search appears prefix/exact, not fuzzy — §16).
- **City/County/Jurisdiction detail** (`CitiesDetailsRead`, etc.) `[probe | openapi]`: `{geo_id, name, state, counties, jurisdictions, zipcodes}`.
  - `counties` / `jurisdictions` → **object map** `{NAME: geo_id}` or `null`, e.g. `{"SAN FRANCISCO": "xJ43Nm7uajA"}`.
  - `zipcodes` → **array of ZIP strings** or `null`, e.g. `["94122","94121",...]`.

  Example:
  ```json
  {
    "geo_id": "RE4X6dFUndM", "name": "San Francisco, San Francisco, CA", "state": "CA",
    "counties": {"SAN FRANCISCO": "xJ43Nm7uajA"},
    "jurisdictions": null,
    "zipcodes": ["94122","94121","94111", "..."]
  }
  ```

### 6.7 Metric rows

All metric endpoints return rows with shared core fields; `current` vs `monthly` differ by two fields. `[probe (city current) | openapi (rest)]`

**Current metrics** (`*/metrics/current`) add `permit_active_count`, `permit_in_review_count`; geo (city/county/jurisdiction) variants also carry `property_type`. Verified live (city, residential, solar):

```json
{
  "geo_id": "RE4X6dFUndM", "tag": "solar", "property_type": "residential",
  "permit_count": 6, "contractor_count": 2,
  "avg_construction_duration": 133, "avg_approval_duration": 2,
  "total_job_value": 0, "avg_inspection_pass_rate": null,
  "permit_active_count": 0, "permit_in_review_count": 0
}
```

**Monthly metrics** (`*/metrics/monthly`) replace the two `*_count` status fields with a **`date`** field (the month bucket) and otherwise match. `[openapi]`

**Contractor metrics** (`ContractorsMetricsMonthlyRead`) `[openapi]`: `{property_type, date, tag, permit_count, avg_job_value, total_job_value, avg_construction_duration, avg_inspection_pass_rate}` (no `geo_id`/`contractor_count`).

**Address metrics** drop `property_type` (addresses have no property-type axis).

### 6.8 Coverage row (`CoverageItem`) `[probe | openapi]`

```json
{"field": "contractor_id", "tier": "partial", "fill_pct": 0.464, "permits_total": 1294026}
```

- `field` — enum `[openapi]`: `fees, job_value, description, contractor_id, owner_name, property_type, property_year_built, property_building_area, issue_date, file_date`.
- `tier` — enum `[openapi]`: `missing`, `partial`, `reliable`. (Live: saw `partial` for CA-state window and `missing`/`fill_pct:0.0` for a sparse ZIP.)
- `fill_pct` — float 0–1.
- `permits_total` — denominator (total permits in the requested geo/window).

---

## 7. Filters & query semantics

### 7.1 Permit / contractor search filters (full list) `[openapi]`

`permits/search` and `contractors/search` share most filters. **Required on both:** `permit_from`, `permit_to`, `geo_id`.

> **`permit_from` / `permit_to` semantics** `[openapi description]`: "Return permits that started on or after / before the specified date." Filtering is on permit **start date**, inclusive on both ends.

Common optional filters (all nullable):

- **`permit_q`** *(string)* — see §7.2.
- **`permit_tags`** *(array)* — see §8 (AND-combine, `-` excludes).
- **`permit_status`** *(array)* — enum `final | in_review | inactive | active`. Invalid value → 422 (§9). `[probe]`
- **`permit_min_approval_duration`**, **`permit_min_construction_duration`**, **`permit_min_inspection_pr`**, **`permit_min_job_value`**, **`permit_min_fees`** *(int)* — minimum thresholds.
- **`property_type`** *(string)* — enum `[openapi]`: `residential, commercial, industrial, agricultural, vacant land, exempt, miscellaneous, office, recreational`.
- **`property_min_market_value`**, **`property_min_building_area`**, **`property_min_lot_size`**, **`property_min_story_count`**, **`property_min_unit_count`** *(int)*.
- **`contractor_classification_derived`** *(array)* — enum `[openapi]`: `concrete_and_paving, demolition_and_excavation, electrical, fencing_and_glazing, framing_and_carpentry, general_building_contractor, general_engineering_contractor, hvac, landscaping_and_outdoor_work, other, plumbing, roofing, specialty_trades`. **AND-combine; `-` prefix excludes** ("Returns results containing ALL specified classifications").
- **`contractor_name`** *(string)* — partial match; **must be ≥ a minimum length** (per description).
- **`contractor_website`** *(string)* — without `http(s)://`.
- **`contractor_min_total_job_value`**, **`contractor_min_total_permits_count`**, **`contractor_min_inspection_pr`** *(int)* — lifetime thresholds (`inspection_pr` is 0–100).
- **`contractor_license`** *(string)*.
- **`size`** *(int)*, **`cursor`** *(string)* — pagination.

**`permits/search`-only:** `permit_has_contractor` *(bool)* — "Return only records that have a contractor ID." `[openapi]`

**`contractors/search`-only:** `include_tallies` *(bool)* — see §7.3.

**`include_count`** *(bool)* — on both searches (§5).

### 7.2 `permit_q` — SUBSTRING, NOT stemmed (VERIFIED-LIVE) `[probe]`

`permit_q` is a **case-insensitive substring** match over the permit description ("Matches anywhere in the text, including partial words"). It is **NOT full-text/stemmed.** Proof (same geo/window, CA 2024):

| Query | `total_count` | Interpretation |
|---|---|---|
| `permit_q=kitchen` | `{value:10000, relation:"gte"}` | broad — matches "kitchen", "kitchens", etc. |
| `permit_q=kitc` | `{value:10000, relation:"gte"}` | **same breadth** — `kitc` is a substring of `kitchen` ⇒ substring matching confirmed |
| `permit_q=kitchens` | `{value:276, relation:"eq"}` | **far narrower** — only descriptions literally containing "kitchens"; a stemmer would have treated `kitchen`≈`kitchens` |

The collapse from 10000+ (`kitchen`) to 276 (`kitchens`) decisively rules out stemming; `kitc` matching as broadly as `kitchen` decisively confirms substring (not whole-word) matching.

### 7.3 `include_tallies` (contractors/search only) `[probe]`

- `include_tallies=true` → each contractor's `status_tally` / `tag_tally` populated (e.g. `{"final":3}`, `{"electrical":3}`).
- `include_tallies=false` → both fields returned as **empty objects `{}`** (computation skipped — a performance lever). `[probe]`

### 7.4 AND / exclude semantics (VERIFIED-LIVE) `[probe]`

Repeated `permit_tags` params **AND-combine**; a `-`-prefixed value **excludes**. Proof (ZIP 94110, 2020–2024):

| Query | `total_count` |
|---|---|
| `permit_tags=solar` | `{value:3, relation:"eq"}` |
| `permit_tags=battery` | `{value:1, relation:"eq"}` |
| `permit_tags=solar&permit_tags=battery` | `{value:1, relation:"eq"}` (solar **AND** battery) |
| `permit_tags=solar&permit_tags=-battery` | `{value:2, relation:"eq"}` (solar **NOT** battery) |

`3 (solar) = 1 (solar∧battery) + 2 (solar∧¬battery)` — arithmetic confirms AND-intersection + exclusion. `contractor_classification_derived` uses the identical "ALL specified" + `-`-exclude convention per `[openapi]`.

### 7.5 Numeric filters

All `*_min_*` filters are **inclusive minimums** (no max counterparts exist on the v2 surface). `inspection_pr` fields expect integers 0–100. `[openapi]`

---

## 8. Complete permit tag vocabulary (RESOLVED)

`GET /v2/list/tags` returns **exactly 22 tags** on a single page (`size:22`, `next_cursor:null`). FREE. Each item is `{"id": "...", "description": "..."}`. **Complete enumeration (VERIFIED-LIVE 2026-05-29)** `[probe]`:

| # | Tag `id` | Description |
|---|---|---|
| 1 | `addition` | Permit involves an addition. |
| 2 | `adu` | New accessory dwelling unit (ADU). |
| 3 | `bathroom` | Bathroom remodel. |
| 4 | `battery` | Battery installation or upgrade. |
| 5 | `demolition` | Demolition. |
| 6 | `electric_meter` | Electric meter installation or modification. |
| 7 | `electrical` | Electrical work. |
| 8 | `ev_charger` | EV charger installation. |
| 9 | `fire_sprinkler` | Fire sprinkler installation or maintenance. |
| 10 | `gas` | Gas line installation or repair. |
| 11 | `generator` | Generator installation. |
| 12 | `grading` | Grading work. |
| 13 | `heat_pump` | Any type of heat pump installation or repair. |
| 14 | `hvac` | Heating, ventilation, and air conditioning (HVAC) work. |
| 15 | `kitchen` | Kitchen remodel. |
| 16 | `new_construction` | New construction project. |
| 17 | `plumbing` | Plumbing work. |
| 18 | `pool_and_hot_tub` | Pool or hot tub installation or modification. |
| 19 | `remodel` | Remodeling project. |
| 20 | `roofing` | Roofing work. |
| 21 | `solar` | Solar panel installation or maintenance. |
| 22 | `water_heater` | Water heater installation or repair. |

**Gotcha:** `permit_tags` is **NOT validated against this enum.** Passing a bogus tag (`permit_tags=notarealtag`) returns **HTTP 200 with `items:[]`** — silently empty, not a 422. `[probe]` Always validate tag spellings client-side against this list.

---

## 9. Errors (REAL probe bodies)

Two distinct 422 shapes exist. The framework-level validator (FastAPI/Pydantic v2) emits a `detail` **array**; custom domain validators emit a `detail` **object**. **This corrects the prior reference's wrong error shape.**

### 9.1 422 — framework validation, `detail` is an ARRAY `[probe]`

**Missing required param** (`states/search` with no `q`; `permits/search` with no `permit_from`):

```json
{"detail":[{"type":"missing","loc":["query","q"],"msg":"Field required","input":null}]}
```

Note: `type` is literally **`"missing"`** and there is an **`input`** field. (The OpenAPI doc-text *examples* still show the legacy `value_error.missing` shape — that prose is **stale**; the live wire format is the Pydantic-v2 shape above. `[probe]` overrides `[openapi]` doc-text here.)

**Bad date format** (`permit_from=01-01-2025`):

```json
{"detail":[{"type":"date_from_datetime_parsing","loc":["query","permit_from"],
  "msg":"Input should be a valid date or datetime, invalid character in year",
  "input":"01-01-2025","ctx":{"error":"invalid character in year"}}]}
```

**Month-only date** (`metric_from=2024-01`):

```json
{"detail":[{"type":"date_from_datetime_parsing","loc":["query","metric_from"],
  "msg":"Input should be a valid date or datetime, input is too short",
  "input":"2024-01","ctx":{"error":"input is too short"}}]}
```

**Bad enum** (`meta/coverage?geo_type=zip`):

```json
{"detail":[{"type":"literal_error","loc":["query","geo_type"],
  "msg":"Input should be 'state', 'county', 'city', 'zipcode' or 'jurisdiction'",
  "input":"zip","ctx":{"expected":"'state', 'county', 'city', 'zipcode' or 'jurisdiction'"}}]}
```

Per-element fields: `type`, `loc` (`[in, field]`), `msg`, `input`, optional `ctx`. `loc[0]` ∈ `{query, path, body, header}`.

### 9.2 422 — domain validator, `detail` is an OBJECT `[probe]`

**Bad status enum** (`permit_status=bogus`):

```json
{"detail":{"loc":["query","status"],"msg":"Status param must be one of: final, in_review, inactive, active","type":"value_error.in"}}
```

**Bad/unresolvable geo_id**:

```json
{"detail":{"loc":["query","geo_id"],
  "msg":"Invalid geolocation ID ''. Accepted: 2-letter US state code (e.g. CA), 5-digit ZIP (e.g. 90210), ZIP+4 (e.g. 90210-1234), or a Shovels geolocation ID. Resolve addresses, cities, counties, or jurisdictions via the matching /search endpoint.",
  "type":"value_error"}}
```

**Client parsing rule:** `detail` may be an **array** (framework) **or an object** (domain) **or a plain string** (auth/402). Parse defensively — do not assume `detail` is always a list.

### 9.3 Other status codes (VERIFIED-LIVE / DOCUMENTED)

| Code | When | Body | Verified |
|---|---|---|---|
| **200** | Success **and** "not found" on search/by-id (see §13) | normal envelope, possibly `items:[]` | `[probe]` |
| **403** | Missing/invalid API key | `{"detail":"Not authenticated"}` | `[probe]` (docs say 401; **wire is 403**) |
| **404** | **Metrics** endpoints with no matching rows; unknown route | `{"detail":"No city metrics found."}`, `{"detail":"No address metrics found."}`, or generic `{"detail":"Not Found"}` (contractor metrics) | `[probe]` |
| **402** | Credit/trial limit exceeded | structured object (paid) or plain string with upgrade URL (trial) — see §10.2 | `[docs:llms.txt]` (not triggered — plan effectively unlimited) |
| **400 / 429 / 500** | Bad request / rate limit / server error | per docs table | `[docs:llms.txt]` (not triggered) |

---

## 10. Credits, rate limits, pricing, plans

### 10.1 `GET /usage` (VERIFIED-LIVE) `[probe | openapi]`

```json
{
  "credits_used": 8,
  "credit_limit": null,
  "is_over_limit": false,
  "available_at": null,
  "daily_usage": [{"date":"2026-05-29","credits":8,"expires":"2026-06-28"}]
}
```

- `credit_limit: null` ⇒ **unlimited plan** (no `X-Credits-Limit`/`X-Credits-Remaining` headers were ever emitted on this key — consistent with unlimited).
- `daily_usage[].expires` confirms the **rolling 30-day window** (used on 2026-05-29 expires 2026-06-28). `[probe | docs]`
- `available_at` — populated only when over-limit (when the limit would reset). `[openapi]`

**Credit accounting (empirically confirmed):** billed endpoints emit `X-Credits-Request` = number of records returned; **1 credit per record**; free endpoints emit no credit headers. See §3.

### 10.2 Limit-exceeded behavior (DOCUMENTED-ONLY) `[docs:llms.txt]`

Not triggered (unlimited key). Per spec:
- **Paid over-limit → HTTP 402**, `detail` is an **object**: `{"error":"Monthly credit limit exceeded.","limit":1000000,"upgrade_url":"https://pay.shovels.ai/p/login/..."}`.
- **Trial over-limit → HTTP 402**, `detail` is a **plain string** with the upgrade URL inline.

### 10.3 Rate limits

- **HTTP 429 "Too Many Requests"** is a documented response code `[docs:llms.txt]`, but **no numeric rate limit (req/s) is published** and none was hit during probing. **UNKNOWN** exact threshold. No `Retry-After`/`X-RateLimit-*` headers observed on 200s.

### 10.4 Pricing & plans (DOCUMENTED-ONLY) `[docs]`

- **Free trial: 250 API requests/"pings"** with full historical access. `[docs:llms.txt | docs:coldiq.com/tools/shovels]`
- **Paid plans start at $599/month** (Online and API tiers). `[docs:coldiq.com | docs:permit-stack.com]`
- Shovels Online also has a **free-forever limited tier** (web app, separate from API trial). `[docs:coldiq]`
- Beyond the $599 entry, exact tier breakpoints/credit allotments are surfaced only inside `app.shovels.ai` after login — **not publicly enumerated** (UNKNOWN at the dollar/credit granularity). EDL is custom/contact-sales. `[docs:https://www.shovels.ai/pricing]`
- Self-serve upgrade portal: `pay.shovels.ai`. Sales: `sales@shovels.ai`, `1-800-511-7457`. `[docs:openapi info]`

---

## 11. Pagination

- **Cursor-based** for paginated endpoints `[docs:llms.txt]`. First page: omit `cursor` (optionally set `size`). Next page: pass `cursor=<next_cursor>` from the prior response. `next_cursor:null` ⇒ end. `[probe — residents returned a live `next_cursor`]`
- `size` controls page size on billable list endpoints; **`size` directly equals credit spend per page** (1/record).
- **`addresses/search` is NOT truly paginated (VERIFIED-LIVE):** it **ignores `size`** (returned 20 items for `size=2` and `size=3`), **always returns `next_cursor:null`**, and **ignores a passed `cursor`** (bad cursor → still 20 items, HTTP 200). Treat it as a fixed top-N (~20) resolver. `[probe]` The OpenAPI spec lists no `size`/`cursor` on `addresses/search` either, consistent with this. `[openapi]`
- `list/tags` returns its full 22-item vocabulary on a single page (`next_cursor:null`). `[probe]`

---

## 12. Identifier stability (RESOLVED — critical)

- **Contractor `id`: NOT stable across releases.** Regenerated in **V2.1.7 (2026-04-02)** during the "fully Shovels-owned pipeline" cutover; only **81.8% mapped** in the changelog. Earlier churn: V2.1.5 updated ~5M IDs (Oregon/Douglas County/Omaha); V2.0.7 regenerated ~170K contractors. **Do not use contractor `id` as a durable cross-release join key without a remap step.** `[docs:release-notes]`
- **Permit `id`: mostly stable, but not guaranteed.** V2.1.7 *retained* 118.3M original permit IDs ("kept original IDs where possible"); historically ~6% of permits received new IDs in a given regeneration (V2.0.7). More stable than contractor IDs, but treat as "stable with periodic partial churn." `[docs:release-notes]`
- **`first_seen_date` was globally reset** in V2.1.7 to the window 2025-05-19 … 2026-03-28 — it reflects pipeline-onboarding date, **not** true first observation. `[docs:release-notes]`
- **Format observations** `[probe]`: permit `id` = 16 lowercase hex chars (`001716eff25f78ef`); contractor `id` = 10-char base62-ish (`000e9PLZDb`); employee `id` = 32 hex chars; geo `geo_id` = short opaque base64-ish token (`RE4X6dFUndM`) except ZIP geo_ids which are the literal 5-digit ZIP.

---

## 13. Stability & gotchas

1. **404-as-empty on SEARCH/by-id, but real 404 on METRICS.** A non-existent permit id (`/permits?id=ffffffffffffffff`) returns **200 with `items:[]`**, not 404. But every `*/metrics/{current,monthly}` endpoint returns a **real 404** (`{"detail":"No ... metrics found."}` or `{"detail":"Not Found"}`) when no rows match the geo/tag/property/date combo. Handle 404 as "no data" for metrics, and empty-`items` as "no data" everywhere else. `[probe]`
2. **`total_count` is capped at 10000** with `relation:"gte"`. Never treat a `gte` value as a real total. `[probe]`
3. **Tags and (some) filters are not enum-validated** — bogus `permit_tags` silently yields empty results (200), whereas `permit_status` and `geo_type` *are* validated (422). Mixed validation discipline. `[probe]`
4. **High null prevalence on enrichment fields.** `meta/coverage` for **CA, 2024** (1,294,026 permits) reported fill rates `[probe]`: `property_type` 70.4%, `property_building_area` 53.1%, `property_year_built` 52.9%, `contractor_id` 46.4%, `owner_name` 41.4%, `job_value` 27.2%, **`fees` 14.1%** — all tiered `partial`. Expect most `job_value`/`fees`/property/contractor-link fields to be null on the majority of permits.
5. **Coverage cliff after V2.1.7 pipeline change:** geocoding dropped 72%→67% and **inspection data 15%→3%** `[docs:release-notes]`. `inspection_pass_rate` is now sparse.
6. **Two address shapes** (`latlng` array on permit-nested vs `lat`/`long` scalars on address-search) — §6.2/6.6.
7. **`employee_count` is a range string** (`"11 to 25"`), not a number — do not parse as int. `[probe]`
8. **Residents endpoint requires an address-type `geo_id`** specifically; city/county geo_ids 422. `[probe]`
9. **Auth failure is 403, not 401** (docs table is wrong on this point). `[probe]`
10. **Month-only dates rejected** — always send full `YYYY-MM-DD` even to monthly-metrics endpoints. `[probe]`
11. **Two `detail` JSON shapes (array vs object) plus a string variant for 402** — parse defensively (§9.2). `[probe]`
12. **`description_derived`** is referenced in release notes but absent from live v2 REST responses and the OpenAPI schema — likely EDL/Online-only (§6.1). `[docs vs probe discrepancy]`

---

## 14. Coverage

- **Permits:** 141,032,194. **Contractors:** 2,638,372. **Permits with address:** 105.2M (74.6%). (V2.1.8, 2026-05-02.) `[docs:release-notes]`
- **Jurisdictions:** Shovels covers "thousands of U.S. jurisdictions"; the latest release added 32 (Portland OR, Orlando FL, Twin Cities suburbs). An **exact total jurisdiction count and a nationwide population-% coverage figure are NOT published** in the sources reviewed — **UNKNOWN** at precise granularity. `[docs:release-notes | docs:llms.txt]`
- **State-level gaps:** coverage is jurisdiction-by-jurisdiction (cities/counties onboard incrementally), so coverage is uneven by state and within states; no published per-state completeness matrix. **UNKNOWN** as a precise table. The recommended way to assess coverage for a given area+window is the **`/meta/coverage`** endpoint (per-field fill rates) plus `permits_total` as the denominator. `[probe]`
- **Field fill-rates:** see gotcha #4 (live CA-2024 numbers). Fill rates vary heavily by jurisdiction and field; use `/meta/coverage` per target geo rather than assuming national averages.

---

## 15. ReZone / "Decisions" / zoning REST availability (RESOLVED: NOT in REST API)

**Decisions / meeting-intelligence / zoning data is NOT exposed via the v2 REST API.** Probed for plausible routes — all **HTTP 404 (route does not exist)** `[probe]`:
- `/v2/decisions/search` → 404
- `/v2/meetings/search` → 404
- `/v2/zoning/search` → 404
- `/v2/rezone/search` → 404

The OpenAPI spec lists **only the 30 permit/contractor/address/geo/list/meta/usage endpoints** — no decisions/zoning/meetings namespace whatsoever. `[openapi]` ReZone (acquired 2026-01-07) meeting-decision data is therefore, as of 2026-05-29, **delivered via Shovels Online / EDL / possibly CLI — not the public v2 REST API.** (Whether a separate API surface for Decisions exists outside `/v2` is **UNKNOWN**; none is documented in `llms.txt`.)

---

## 16. Shovels CLI (DOCUMENTED-ONLY) `[docs:knowledge-base/cli]`

- **Exists.** Launched in **V2.1.6 (2026-03-02)** for command-line permit/contractor queries. `[docs:release-notes]`
- **Install (one-liner):** `curl -LsSf https://shovels.ai/install.sh | sh` — detects platform, fetches the binary, validates SHA256, installs to `~/.shovels/bin`. Manual binaries on GitHub Releases; `SHOVELS_VERSION` / `SHOVELS_INSTALL_DIR` env overrides. `[docs]`
- **Auth model:** uses your Shovels **API key** (same credential as the REST API), configured via a "CLI authentication" step; the docs excerpt did not spell out the exact mechanism (env var vs config file) — **partially UNKNOWN.** `[docs]`
- **Commands:** docs confirm `shovels version` (prints version/commit/build date); the full command set was not enumerated in the reviewed excerpt — **UNKNOWN** beyond `version`. The CLI is positioned as a wrapper over the same permit/contractor query surface. `[docs]`

---

## 17. API vs EDL (field/visibility split)

- **API (this doc):** real-time, per-record REST access with credit-metered billing; the 30 endpoints above; record-level objects (permits, contractors, employees, residents, metrics, geo). `[probe | openapi]`
- **EDL (Enterprise Data License):** bulk delivery of the **full structured database** (not metered per-record), for customers who want the entire corpus / fields not surfaced via REST. `[docs:llms.txt | docs:release-notes]`
- **Observed/likely split:** `description_derived` (81M permits per release notes) is **not** on the REST surface — an example of an EDL/Online field not (yet) in the API. ReZone "Decisions" data is EDL/Online-only (§15). Beyond these, an exhaustive field-by-field API-vs-EDL diff is **not published** — **UNKNOWN** in full. `[docs vs probe]`

---

## 18. Geographic hierarchy & the `geo_id` system

- **`geo_id` is the universal geographic key.** It accepts (for filtering on `permits/search`/`contractors/search`) `[openapi | probe]`:
  - a **2-letter US state code** (`CA`),
  - a **5-digit ZIP** (`90210`) or **ZIP+4** (`90210-1234`),
  - a **Shovels geolocation ID** for an address / city / county / jurisdiction (opaque token, e.g. `RE4X6dFUndM`).
- **Resolve place names → geo_id** via the matching `*/search` endpoint (all FREE): `addresses/search`, `cities/search`, `counties/search`, `jurisdictions/search`, `zipcodes/search`, `states/search`. `[probe]`
- **Hierarchy is navigable** via detail endpoints: `cities?geo_id=` returns the city's `counties` (`{name:geo_id}`), `jurisdictions`, and `zipcodes[]`; same pattern for counties/jurisdictions. `[probe]`
- **ZIP geo_ids are literal ZIP strings**; all other geo_ids are opaque tokens. `[probe]`
- Each permit carries a `geo_ids` map (`address_id`/`city_id`/`county_id`/`jurisdiction_id`) tying it into the hierarchy. `[probe]`
- **Practical recipe** (since permit/contractor search require `permit_from`+`permit_to`+`geo_id`): resolve a `geo_id` from a `*/search` call first, then query with a date window. State code or ZIP can be passed directly as `geo_id` without a resolve step. `[probe]`

---

## 19. Contractor / resident / employee intelligence

- **Contractor enrichment** (firmographics): license + dates, classification (raw + derived enum), business_type, SIC/NAICS, website/LinkedIn, revenue, `employee_count` (range string), rating/review_count, plus computed performance (`permit_count`, `avg/total_job_value`, durations, inspection pass rate) and per-contractor `status_tally`/`tag_tally`. Many fields null for small operators. `[probe]`
- **Employee intelligence** (PII): per-employee identity + contact (name/phone/email/business_email/LinkedIn), demographics (gender/age_range/marital/children/income_range/net_worth/homeowner) and role (job_title/seniority_level/department). Sourced for larger contractors; small contractors return empty. **Billed 1/record, PII-bearing.** `[probe]`
- **Resident intelligence** (PII): per-address occupant identity + contact + demographics (`personal_emails` single-string, phone, LinkedIn, net_worth, income_range, is_homeowner). Address-geo_id-scoped, paginated, **billed 1/record, PII-bearing.** `[probe]`

> **PII handling:** employees and residents carry real names, emails, phone numbers, LinkedIn URLs, and financial demographics. Treat these endpoints as PII sources (downstream storage/retention/consent obligations apply). All examples in this document have such values redacted.

---

## 20. Residual unknowns (honest)

- **Exact numeric rate limit** (req/s) and any `Retry-After` semantics — **UNKNOWN** (429 documented, not triggered).
- **402 over-limit wire bodies** — **DOCUMENTED-ONLY** (key is unlimited; not reproduced live).
- **Exact paid-tier breakpoints / credit allotments** beyond the $599 entry — **UNKNOWN** (login-gated).
- **`size`/`cursor` honoring on `cities/counties/jurisdictions/search`** — spec lists them, but not independently confirmed (the analogous `addresses/search` ignores them). Treat with caution.
- **`/states/search` credit status** — classified FREE by analogy, not isolated (returned an empty page for fuzzy `q`; state search appears prefix/exact, the fuzzy behavior is **UNKNOWN**).
- **Full CLI command set + exact auth config** — only `version` confirmed.
- **`description_derived`** delivery channel (EDL/Online vs hidden REST param) — **UNKNOWN**.
- **Existence of any non-`/v2` API surface for ReZone "Decisions"** — none documented; **UNKNOWN**.
- **Exact national jurisdiction count and population-% coverage** — not published; **UNKNOWN**.
- **`property_type` filter strictness** — a `property_min_market_value=1` filter returned a permit with null property fields, suggesting numeric filters may be permissive toward nulls or the filter targets a different field than the response column; **not fully characterized**.

---

## 21. Source citations

- **Live API** (`https://api.shovels.ai/v2`, probed 2026-05-29): `/usage`, `/meta/release`, `/meta/coverage`, `/list/tags`, `/list/zip`, `/permits/search`, `/permits`, `/contractors/search`, `/contractors`, `/contractors/{id}/permits`, `/contractors/{id}/employees`, `/contractors/{id}/metrics`, `/addresses/search`, `/addresses/{geo_id}/residents`, `/addresses/{geo_id}/metrics/current`, `/cities/search`, `/cities`, `/cities/{geo_id}/metrics/current`, `/cities/{geo_id}/metrics/monthly`, `/counties/search`, `/jurisdictions/search`, `/zipcodes/search`, `/states/search`, plus negative probes (`/decisions`, `/meetings`, `/zoning`, `/rezone` → 404) and error probes (422/403/404). `[probe]`
- **Live OpenAPI spec:** `https://api.shovels.ai/v2/openapi.json` (title "The Shovels API v2", version 2.0.0, OpenAPI 3.1.0), fetched 2026-05-29. `[openapi]`
- **Live docs:** `https://docs.shovels.ai/llms.txt`; `https://docs.shovels.ai/release-notes/release-notes.md` (V2.1.8 / 2026-05-02 top entry); `https://docs.shovels.ai/docs/knowledge-base/cli`.
- **Corporate/pricing (web):** `https://www.shovels.ai/blog/shovels-acquires-rezone/` (ReZone acquisition 2026-01-07); `https://commercialobserver.com/2025/06/shovels-proptech-permits/` ($5M seed); `https://www.einpresswire.com/article/913853011/...` (Precisely partnership); `https://www.shovels.ai/pricing`; `https://coldiq.com/tools/shovels`; `https://permit-stack.com/compare/shovels-ai/`.

---
*End of canonical reference. Generated from independent live verification; no prior summary was trusted. Discrepancies between the live wire format and the published OpenAPI doc-text (notably the 422 shape and the 401-vs-403 auth code) are resolved in favor of the live `[probe]` observations per the stated reconciliation rule.*
