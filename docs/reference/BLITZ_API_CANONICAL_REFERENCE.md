# BlitzAPI — Canonical Reference (v2)

> **Status:** Authoritative. This file supersedes the March-2026 snapshot trees
> (`blitzapi/`, `blitz-api/{api-reference,guide}`, `blitzapi-other/`), which are
> stale (missing endpoints, wrong `utilities`→`utils` path, understated limits).
> Prefer this file for payloads; use the live docs MCP for anything newer.
>
> - **Source of truth:** live BlitzAPI docs + OpenAPI v2 spec, retrieved via the
>   official `blitz-api` docs MCP and `https://docs.blitz-api.ai/llms.txt`.
> - **Verified:** 2026-06-02.
> - **Spec:** "Blitz API Reference", OpenAPI v2. Upstream version labels are
>   inconsistent (the served `v2.openapi.json` reports `2.0.0`; the
>   `find-companies` page embeds `2.1.0`) — content, not the label, is canonical.
> - **Base URL:** `https://api.blitz-api.ai`
> - **Auth:** `x-api-key` header (see [Authentication](#authentication)).
> - **Re-scrape:** `https://docs.blitz-api.ai/llms.txt` (append `.md` to any docs
>   page URL for its Markdown form).

---

## Table of contents

1. [Authentication](#authentication)
2. [Conventions](#conventions) — filter logic, exact-match syntax, pagination, limits
3. [Rate limits](#rate-limits)
4. [Credits & plans](#credits--plans)
5. [Error model](#error-model)
6. [Endpoint index](#endpoint-index)
7. [Account](#account) — `key-info`
8. [Search](#search) — waterfall ICP, employee finder, find people, company search
9. [Enrichment](#enrichment) — email, phone, reverse lookups, company, domain↔linkedin
10. [Utilities](#utilities) — current date, employment distribution
11. [Shared objects](#shared-objects) — Person, Experience, Education, Certification, Company
12. [Enum appendix](#enum-appendix) — job level, job function, company type, employee range, continent, sales region, country code, **industry (534)**

---

## Authentication

All endpoints require an API key passed in the `x-api-key` request header.

```
x-api-key: <your-api-key>
Content-Type: application/json
```

- Security scheme: API key in header, name `x-api-key`.
- Base URL: `https://api.blitz-api.ai` (production; only server in the spec).
- Validate a key and read its live limits/credits with
  [`GET /v2/account/key-info`](#get-v2accountkey-info).

---

## Conventions

### Filter combination (search endpoints)
- **Across filters:** combined with **AND**. Adding `industry` *and* `hq` narrows
  results to companies matching both.
- **Within a filter:** multiple values combine with **OR**. Two industries in
  `industry.include` returns companies in *either*.
- `include` / `exclude` sub-arrays exist on most categorical/keyword filters;
  `exclude` removes matches from the `include` set.

### Exact-match vs. full-text syntax
Title/keyword filters (`people.job_title`, waterfall `include_title`) default to
**full-text keyword** matching:

- `"CEO"` (no brackets) → FTS; also matches `Co-CEO`, `CEO Office`, etc.
- `"[CEO]"` (wrapped in square brackets) → **exact**, case- **and**
  accent-insensitive (`CEO` / `ceo` / `Céo`).
- Mixed arrays are allowed: `["[CEO]", "Founder"]`.

### Pagination
Three different schemes — do not assume one:

| Endpoint | Scheme | Mechanic |
|---|---|---|
| `POST /v2/search/people` | **cursor** | Pass `cursor` from each response into the next request; stop when `cursor` is `null`. Hard cap **50,000** results. |
| `POST /v2/search/companies` | **cursor** | Same cursor mechanic. |
| `POST /v2/search/employee-finder` | **page** | Request `page`; response returns `page` + `total_pages`. |
| `POST /v2/search/waterfall-icp-keyword` | none | Single response, bounded by `max_results`. |
| enrichment / utilities | none | Single object per call. |

### Result-size caps (`max_results`)
- Waterfall ICP Search: **1–100** (example `10`).
- Find People / Company Search / Employee Finder: **1–50**, default `10`.

### Enum normalization
Categorical fields (`industry`, `type`, `continent`, `sales_region`,
`job_level`, `job_function`, `employee_range`) accept **exact** normalized values
only — see the [Enum appendix](#enum-appendix). `country_code` is ISO 3166-1
alpha-2 (free-form string, length 2–3).

---

## Rate limits

- **5 requests/second** standard on all plans.
- Your account's exact limit is returned by
  [`GET /v2/account/key-info`](#get-v2accountkey-info) as
  `max_requests_per_seconds` — read it to configure a client-side limiter.
- Exceeding it returns **429** `{"success": false, "message": "Rate limit exceeded, please try again later"}`.

---

## Credits & plans

BlitzAPI is **flat-rate unlimited** on paid plans (no per-request fees). Credits
are metered **only on the free trial**.

- **Free trial:** 1,000 credits; consumed per request (≈1 credit per result /
  per enrichment / per call). `current-date` is free (0 credits).
- **Paid plans:** unlimited calls to the endpoints the plan unlocks.

| Plan | Price | Unlocks (beyond lower tiers) |
|---|---|---|
| **Unlimited Leads** | $399/mo | Waterfall ICP, Company Search, Company Enrichment, Employee Finder, Find People, Domain→LinkedIn, Reverse Lookups (email & phone). 380M+ contacts, 60M+ companies. |
| **Unlimited Email** ⭐ | $499/mo | + Email Enrichment (`/v2/enrichment/email`). 62M+ verified emails, ~97% accuracy. |
| **Unlimited Phone** | $599/mo | + Phone Enrichment (`/v2/enrichment/phone`). 40M+ mobile numbers, **US only**. |

Endpoint gating by plan:

| Endpoint | Leads | Email | Phone |
|---|:--:|:--:|:--:|
| Waterfall ICP, Company Search, Company Enrichment, Employee Finder, Find People, Domain→LinkedIn, Reverse Lookups | ✅ | ✅ | ✅ |
| Email Enrichment (`/v2/enrichment/email`) | ❌ | ✅ | ✅ |
| Phone Enrichment (`/v2/enrichment/phone`) | ❌ | ❌ | ✅ |

---

## Error model

JSON over HTTP. Status codes returned **vary by endpoint** (per-endpoint sets are
listed in each section). Bodies:

| Status | Meaning | Body shape |
|---|---|---|
| `200` | OK | endpoint-specific |
| `401` | Unauthorized — missing/invalid key | `{"success": false, "message": "Invalid API key, please provide a valid API key in the 'x-api-key' header"}` |
| `402` | Payment Required — trial credits exhausted | `{"message": "Insufficient credits balance"}` |
| `404` | Not Found | `{"success": false, "message": "Not Found"}` |
| `422` | Unprocessable Entity — invalid/missing input | `{"success": false, "error": {"code": "INVALID_INPUT", "message": "Missing required fields"}}` |
| `429` | Too Many Requests — rate limit | `{"success": false, "message": "Rate limit exceeded, please try again later"}` |
| `500` | Internal Server Error | `{"success": false, "message": "..."}` |

---

## Endpoint index

| # | Method | Path | Purpose | Pagination |
|---|---|---|---|---|
| 1 | GET | `/v2/account/key-info` | Key validity, credits, rate limit, plans | — |
| 2 | POST | `/v2/search/waterfall-icp-keyword` | Best decision-maker via cascade tiers | — |
| 3 | POST | `/v2/search/employee-finder` | All employees at one company | page |
| 4 | POST | `/v2/search/people` | Decision-makers across many companies | cursor |
| 5 | POST | `/v2/search/companies` | Companies matching ICP criteria | cursor |
| 6 | POST | `/v2/enrichment/email` | LinkedIn URL → verified work email | — |
| 7 | POST | `/v2/enrichment/phone` | LinkedIn URL → direct mobile phone | — |
| 8 | POST | `/v2/enrichment/email-to-person` | Work email → full person profile | — |
| 9 | POST | `/v2/enrichment/phone-to-person` | Phone → full person profile | — |
| 10 | POST | `/v2/enrichment/company` | Company LinkedIn URL → company profile | — |
| 11 | POST | `/v2/enrichment/domain-to-linkedin` | Domain → company LinkedIn URL | — |
| 12 | POST | `/v2/enrichment/linkedin-to-domain` | Company LinkedIn URL → email domain | — |
| 13 | POST | `/v2/utils/current-date` | Server date/time for a timezone | — |
| 14 | POST | `/v2/utils/company-employment-distribution` | Employee count by country | — |

> ⚠️ **Path note:** utilities live under `/v2/utils/` (not `/v2/utilities/`, as the
> old snapshot stated).

---

## Account

### `GET /v2/account/key-info`
Check API key validity, remaining credits, rate limit, allowed endpoints, and
active subscription plans. No request body. **Errors:** 401, 404, 500.

**Response 200**
```json
{
  "valid": true,
  "id": "XXX",
  "remaining_credits": 99.5,
  "next_reset_at": "2026-02-12T17:48:25.199Z",
  "max_requests_per_seconds": 5,
  "allowed_apis": ["/search/waterfall-icp-keyword", "/enrichment/email_domain", "/enrichment/email"],
  "active_plans": [
    { "name": "Startup Trial", "status": "active", "started_at": "2026-01-12T17:48:25.200Z" }
  ]
}
```

---

## Search

### `POST /v2/search/waterfall-icp-keyword`
Find the best decision-maker at a target company using a prioritized **cascade**:
tiers are attempted in order; the next tier is used only if the previous yields
nothing. **Cost:** 1 credit/result (trial). **Errors:** 402, 422, 500.

**Request body**

| Field | Type | Req | Notes |
|---|---|:--:|---|
| `company_linkedin_url` | string (uri) | ✅ | Target company LinkedIn URL |
| `cascade` | array | ✅ | Ordered tiers, **min 1 item** |
| `cascade[].include_title` | string[] | ✅ | Titles to match (FTS; `[...]` for exact) |
| `cascade[].exclude_title` | string[] | | Disqualifying title keywords |
| `cascade[].location` | string[] | ✅ | Country codes or `"WORLD"` |
| `cascade[].include_headline_search` | boolean | ✅ | Also match LinkedIn headline text |
| `max_results` | number | | **1–100** (example 10) |

**Example request**
```json
{
  "company_linkedin_url": "https://www.linkedin.com/company/openai",
  "cascade": [
    { "include_title": ["CMO", "Chief Marketing Officer"], "exclude_title": ["assistant", "intern"], "location": ["WORLD"], "include_headline_search": false },
    { "include_title": ["VP Marketing", "Head of Marketing"], "exclude_title": ["assistant"], "location": ["US", "GB"], "include_headline_search": true }
  ],
  "max_results": 5
}
```

**Response 200** — note `person` is **nested**, with `icp` (1-based matched tier)
and `ranking`.
```json
{
  "company_linkedin_url": "https://www.linkedin.com/company/openai",
  "max_results": 5,
  "results_length": 1,
  "results": [
    { "icp": 1, "ranking": 1, "person": { "...": "see Person object" } }
  ]
}
```

### `POST /v2/search/employee-finder`
Search **all** employees at a single company by level, function, location, and
seniority. **Page-based** pagination. **Cost:** 1 credit/result (trial).
**Errors:** 402, 422, 500.

**Request body**

| Field | Type | Req | Notes |
|---|---|:--:|---|
| `company_linkedin_url` | string | ✅ | |
| `country_code` | string[] | | ISO 3166-1 alpha-2 |
| `continent` | string[] | | [enum](#continent-7) |
| `sales_region` | string[] | | [enum](#sales-region-4) |
| `job_level` | string[] | | [enum](#job-level-6) |
| `job_function` | string[] | | [enum](#job-function-22) |
| `min_connections_count` | number | | 0–500 |
| `max_results` | number | | 1–50 |
| `page` | number | | 1-based page |

**Response 200** — flat `results[]` of [Person](#person), plus `page` /
`total_pages`.
```json
{
  "company_linkedin_url": "https://www.linkedin.com/company/openai",
  "max_results": 3,
  "results_length": 3,
  "page": 1,
  "total_pages": 1285,
  "results": [ { "...": "Person" } ]
}
```

### `POST /v2/search/people`
Search decision-makers **across many companies** in one call. Combine
company-level filters with person-level filters. **Cursor** pagination (cap
50,000). **Cost:** 1 credit/person (trial). **Errors:** 401, 404, 429.

**Request body (top level)**

| Field | Type | Notes |
|---|---|---|
| `company` | object | Company-level filters (see [Company filter](#company-filter-object)) — plus `linkedin_url: string[]` to scope to specific companies |
| `people` | object | Person-level filters (below) |
| `cursor` | string \| null | `minLength 3`; from previous response |
| `max_results` | number | 1–50, default 10 |

**`people` (person-level filters)**

| Field | Type | Notes |
|---|---|---|
| `job_title.include` | string[] | FTS keywords; `[...]` for exact match |
| `job_title.exclude` | string[] | |
| `job_title.include_linkedin_headline` | boolean | Also search headline (default false) |
| `job_function` | string[] | [enum](#job-function-22) |
| `job_level` | string[] | [enum](#job-level-6) |
| `min_connections` | number | 0–500 |
| `location.city` | string[] | |
| `location.country_code` | string[] | ISO alpha-2 |
| `location.continent` | string[] | [enum](#continent-7) |
| `location.sales_region` | string[] | [enum](#sales-region-4) |
| `education.include` | string[] | Phrases matched per education entry; tokens must co-occur within one entry (e.g. `"Stanford 2025"`); token order irrelevant |
| `education.exclude` | string[] | Excludes any person with a matching entry |

**Example request**
```json
{
  "company": {
    "industry": { "include": ["IT Services and IT Consulting"] },
    "hq": { "country_code": ["US"] },
    "employee_range": ["51-200", "201-500"]
  },
  "people": {
    "job_title": { "include": ["[CEO]", "Founder"], "include_linkedin_headline": false },
    "job_level": ["C-Team"],
    "location": { "continent": ["North America"] }
  },
  "max_results": 25
}
```

**Response 200** — flat `results[]` of [Person](#person) (experiences include
`company_name` + `company_domain`); `total_results` is the full match count;
`cursor` for the next page.
```json
{
  "total_results": 14337505,
  "results": [ { "...": "Person" } ],
  "results_length": 25,
  "max_results": 25,
  "cursor": "eyJzIjpbNTAwLDQwNTE3ODE4LDMzMTk2MV0sIm8iOjN9..."
}
```

### `POST /v2/search/companies`
Find companies matching ICP criteria. **Cursor** pagination. **Cost:** 1
credit/result (trial). **Errors:** 401, 404, 429.

**Request body (top level):** `company` ([Company filter](#company-filter-object)),
`cursor` (string|null, `minLength 3`), `max_results` (1–50, default 10).

**Example request**
```json
{
  "company": {
    "keywords": { "include": ["SaaS", "cloud platform"] },
    "industry": { "include": ["Software Development", "Technology; Information and Internet"] },
    "hq": { "continent": ["Europe"] },
    "employee_range": ["51-200", "201-500"]
  },
  "max_results": 25
}
```

**Response 200** — `results[]` of [Company](#company-object) (search variant).
```json
{
  "total_results": 100,
  "results": [
    {
      "linkedin_url": "https://www.linkedin.com/company/google",
      "linkedin_id": 1441,
      "name": "Google",
      "about": "…",
      "specialties": ["machine learning", "search", "cloud", "ads"],
      "industry": "Software Development",
      "type": "Public Company",
      "size": "10001+",
      "employees_on_linkedin": 328177,
      "followers": 40093219,
      "founded_year": null,
      "hq": { "city": "Mountain View", "state": "California", "country_code": "US", "country_name": "United States", "region": "NORAM", "continent": "North America" },
      "domain": "google.com",
      "website": "https://www.google.com"
    }
  ],
  "results_length": 10,
  "max_results": 10,
  "cursor": "eyJpIjoiY2E5OTcxZjUt..."
}
```

#### Company filter object
Used by `company` in both Company Search and Find People.

| Field | Type | Notes |
|---|---|---|
| `name.include` / `.exclude` | string[] | Keywords in company name |
| `keywords.include` / `.exclude` | string[] | Across description, specialties, NAICS/SIC descriptions, Crunchbase/G2 categories (all tokens of a phrase must hit one field) |
| `industry.include` / `.exclude` | string[] | [534-value enum](#industry-534) |
| `type.include` / `.exclude` | string[] | [Company type enum](#company-type-10) |
| `employee_range` | string[] | [enum](#employee-range-8) |
| `employee_count.min` / `.max` | number | 0–1,000,000 (0 = unset) |
| `min_linkedin_followers` | number | 0–10,000,000 (default 1) |
| `revenue.min` / `.max` | number | USD; 0 = unset |
| `naics_code.include` / `.exclude` | string[] | e.g. `"541511"` |
| `sic_code.include` / `.exclude` | string[] | e.g. `"7372"` |
| `web_traffic.min` / `.max` | number | Monthly visits; 0 = unset |
| `ad_spend.min` / `.max` | number | Monthly Google ad spend USD; 0 = unset |
| `founded_year.min` / `.max` | number | 0 = unset |
| `hq.city.include` / `.exclude` | string[] | Keyword search on HQ city |
| `hq.country_code` | string[] | ISO alpha-2 |
| `hq.continent` | string[] | [enum](#continent-7) |
| `hq.sales_region` | string[] | [enum](#sales-region-4) |
| `linkedin_url` *(Find People only)* | string[] | Scope people search to these companies |

---

## Enrichment

All return a top-level `found` boolean. **Plan gating** in [Credits & plans](#credits--plans).

### `POST /v2/enrichment/email` — Find Work Email
LinkedIn profile URL → verified work email(s). **Plan:** Email ($499)+.
**Errors:** 401, 402, 500.

Request: `{ "person_linkedin_url": "https://www.linkedin.com/in/antoine-blitz-5581b7373" }`
```json
{
  "found": true,
  "email": "antoine@blitz-agency.com",
  "all_emails": [
    { "email": "antoine@blitz-agency.com", "job_order_in_profile": 1, "company_linkedin_url": "https://www.linkedin.com/company/blitz-api", "email_domain": "blitz-agency.com" }
  ]
}
```

### `POST /v2/enrichment/phone` — Find Mobile & Direct Phone
LinkedIn profile URL → direct mobile (US only). **Plan:** Phone ($599).
**Errors:** 401, 402, 500.

Request: `{ "person_linkedin_url": "…" }` → `{ "found": true, "phone": "+1234567890" }`

### `POST /v2/enrichment/email-to-person` — Reverse Email Lookup
Work email → full profile. **Errors:** 401, 402, 500.

Request: `{ "email": "antoine@blitz-agency.com" }` → `{ "found": true, "person": { … } }` ([Person](#person))

### `POST /v2/enrichment/phone-to-person` — Reverse Phone Lookup
Phone → full profile. **Errors:** 401, 402, 500.

Request: `{ "phone": "+1234567890" }` → `{ "found": true, "person": { … } }` ([Person](#person))

### `POST /v2/enrichment/company` — Company Enrichment
Company LinkedIn URL → full company profile. **Errors:** 401, 404, 429.

Request: `{ "company_linkedin_url": "https://www.linkedin.com/company/blitz-api" }`
```json
{
  "found": true,
  "company": {
    "linkedin_url": "https://www.linkedin.com/company/blitz-api",
    "linkedin_id": 108037802,
    "name": "Blitzapi",
    "about": "…",
    "specialties": null,
    "industry": "Technology; Information and Internet",
    "type": "Privately Held",
    "size": "1-10",
    "employees_on_linkedin": 3,
    "followers": 6,
    "founded_year": null,
    "hq": { "city": "Paris", "state": null, "postcode": null, "country_code": "FR", "country_name": "France", "region": null, "continent": null, "street": null },
    "domain": "blitz-api.ai",
    "website": "https://blitz-api.ai"
  }
}
```

### `POST /v2/enrichment/domain-to-linkedin` — Domain to LinkedIn URL
**Errors:** 402, 422, 500.

Request: `{ "domain": "https://www.blitz-agency.com" }` → `{ "found": true, "company_linkedin_url": "https://www.linkedin.com/company/blitz-api" }`

### `POST /v2/enrichment/linkedin-to-domain` — LinkedIn URL to Domain
**Errors:** 401, 402, 500.

Request: `{ "company_linkedin_url": "https://www.linkedin.com/company/blitz-api" }` → `{ "found": true, "email_domain": "blitz-agency.com" }`

---

## Utilities

### `POST /v2/utils/current-date` — Get Current Date and Time
Server date/time for an IANA timezone. **Cost:** 0 credits. **Errors:** 422, 500.

Request: `{ "region": "America/New_York" }`
```json
{ "datetime": "2026-01-08 12:00:00 -05:00", "timestamp": 1736385600, "timezone": "America/New_York", "timezone_name": "(GMT-05:00) New York" }
```

### `POST /v2/utils/company-employment-distribution` — Company Employment Distribution
Employee count grouped by ISO 3166-1 alpha-2 country (undetermined →
`"unknown"`). If you only have a domain, run `domain-to-linkedin` first.
**Cost:** 1 credit/call (trial). **Errors:** 401, 429.

Request: `{ "company_linkedin_url": "https://www.linkedin.com/company/openai" }`
```json
{
  "company_linkedin_url": "https://www.linkedin.com/company/openai",
  "total_employees": 1234,
  "distribution": [ { "country": "US", "count": 900 }, { "country": "GB", "count": 200 }, { "country": "unknown", "count": 54 } ]
}
```

---

## Shared objects

### Person
Returned by reverse lookups and (nested or flat) by the search endpoints.

| Field | Type | Notes |
|---|---|---|
| `first_name` / `last_name` / `full_name` | string | |
| `nickname` / `civility_title` | string \| null | |
| `headline` / `about_me` | string \| null | |
| `location` | object | `{ city, state_code, country_code, continent }` (each nullable) |
| `linkedin_url` | string | |
| `connections_count` | number | LinkedIn often caps display at 500 |
| `profile_picture_url` | string \| null | |
| `experiences` | [Experience](#experience)[] | |
| `education` | [Education](#education)[] | |
| `skills` | string[] | |
| `certifications` | [Certification](#certification)[] | |

> In Waterfall results the person sits at `results[].person` alongside `icp` /
> `ranking`. In Find People & Employee Finder, person fields are **flat** in
> `results[]`.

### Experience
| Field | Type | Notes |
|---|---|---|
| `company_name` | string | Present in Find People / Employee Finder results |
| `job_title` | string \| null | |
| `company_linkedin_url` | string | |
| `company_linkedin_id` | string | |
| `company_domain` | string | |
| `job_description` | string \| null | |
| `job_start_date` / `job_end_date` | string (date) \| null | |
| `job_is_current` | boolean | |
| `job_location` | object | `{ city, state_code, country_code }` (nullable) |

### Education
`{ "degree": string, "start_date": date|null, "end_date": date|null }`
(some responses also surface `organization` / `linkedin_url`).

### Certification
`{ "authority": string, "name": string, "url": string|null }`

### Company object
Returned by Company Enrichment (full) and Company Search (subset). Fields:
`linkedin_url`, `linkedin_id` (number), `name`, `about`, `specialties`
(string[] \| null), `industry`, `type`, `size`, `employees_on_linkedin`,
`followers`, `founded_year` (number \| null), `domain` (\| null), `website`, and
`hq`:

- **Enrichment `hq`:** `{ city, state, postcode, country_code, country_name, region, continent, street }`
- **Search `hq`:** `{ city, state, country_code, country_name, region, continent }`

---

## Enum appendix

### Job level (6)
`C-Team`, `Director`, `Manager`, `Other`, `Staff`, `VP`

### Job function (22)
`Advertising & Marketing`, `Art, Culture and Creative Professionals`,
`Construction`, `Customer/Client Service`, `Education`, `Engineering`,
`Finance & Accounting`, `General Business & Management`,
`Healthcare & Human Services`, `Human Resources`, `Information Technology`,
`Legal`, `Manufacturing & Production`, `Operations`, `Other`,
`Public Administration & Safety`, `Purchasing`, `Research & Development`,
`Sales & Business Development`, `Science`, `Supply Chain & Logistics`,
`Writing/Editing`

### Company type (10)
`Educational`, `Educational Institution`, `Government Agency`, `Nonprofit`,
`Partnership`, `Privately Held`, `Public Company`, `Self-Employed`,
`Self-Owned`, `Sole Proprietorship`

### Employee range (8)
`1-10`, `11-50`, `51-200`, `201-500`, `501-1000`, `1001-5000`, `5001-10000`,
`10001+`

### Continent (7)
`Africa`, `Antarctica`, `Asia`, `Europe`, `North America`, `Oceania`,
`South America`

### Sales region (4)
`NORAM`, `LATAM`, `EMEA`, `APAC`

### Country code
ISO 3166-1 alpha-2 (string, length 2–3), e.g. `US`, `FR`, `DE`, `GB`.

### Industry (534)
Exact normalized values only. Note semicolons appear where LinkedIn uses commas
(e.g. `Glass; Ceramics and Concrete`).

<details>
<summary>Full 534-value industry enum</summary>

```
Abrasives and Nonmetallic Minerals Manufacturing
Accessible Architecture and Design
Accessible Hardware Manufacturing
Accommodation and Food Services
Accounting
Administration of Justice
Administrative and Support Services
Advertising Services
Agricultural Chemical Manufacturing
Agriculture; Construction; Mining Machinery Manufacturing
Airlines and Aviation
Airlines/Aviation
Air; Water; and Waste Program Management
Alternative Dispute Resolution
Alternative Fuel Vehicle Manufacturing
Alternative Medicine
Ambulance Services
Amusement Parks and Arcades
Animal Feed Manufacturing
Animation
Animation and Post-production
Apparel and Fashion
Apparel Manufacturing
Appliances; Electrical; and Electronics Manufacturing
Architectural and Structural Metal Manufacturing
Architecture and Planning
Armed Forces
Artificial Rubber and Synthetic Fiber Manufacturing
Artists and Writers
Arts and Crafts
Audio and Video Equipment Manufacturing
Automation Machinery Manufacturing
Automotive
Aviation and Aerospace
Aviation and Aerospace Component Manufacturing
Baked Goods Manufacturing
Banking
Bars; Taverns; and Nightclubs
Bed-and-Breakfasts; Hostels; Homestays
Beverage Manufacturing
Biomass Electric Power Generation
Biotechnology
Biotechnology Research
Blockchain Services
Blogs
Boilers; Tanks; and Shipping Container Manufacturing
Book and Periodical Publishing
Book Publishing
Breweries
Broadcast Media
Broadcast Media Production and Distribution
Building Construction
Building Equipment Contractors
Building Finishing Contractors
Building Materials
Building Structure and Exterior Contractors
Business Consulting and Services
Business Content
Business Intelligence Platforms
Business Supplies and Equipment
Cable and Satellite Programming
Capital Markets
Caterers
Chemical Manufacturing
Chemical Raw Materials Manufacturing
Chemicals
Child Day Care Services
Chiropractors
Circuses and Magic Shows
Civic and Social Organization
Civic and Social Organizations
Civil Engineering
Claims Adjusting; Actuarial Services
Clay and Refractory Products Manufacturing
Climate Data and Analytics
Climate Technology Product Manufacturing
Coal Mining
Collection Agencies
Commercial and Industrial Equipment Rental
Commercial and Industrial Machinery Maintenance
Commercial and Service Industry Machinery Manufacturing
Commercial Real Estate
Communications Equipment Manufacturing
Community Development and Urban Planning
Community Services
Computer and Network Security
Computer Games
Computer Hardware
Computer Hardware Manufacturing
Computer Networking
Computer Networking Products
Computers and Electronics Manufacturing
Computer Software
Conservation Programs
Construction
Construction Hardware Manufacturing
Consumer Electronics
Consumer Goods
Consumer Goods Rental
Consumer Services
Correctional Institutions
Cosmetics
Cosmetology and Barber Schools
Courts of Law
Credit Intermediation
Cutlery and Handtool Manufacturing
Dairy
Dairy Product Manufacturing
Dance Companies
Data Infrastructure and Analytics
Data Security Software Products
Death Care Services
Defense and Space
Defense and Space Manufacturing
Dentists
Design
Design Services
Desktop Computing Software Products
Digital Accessibility Services
Distilleries
Economic Programs
Education
Education Administration Programs
Education Management
E-learning
E-Learning Providers
Electrical and Electronic Manufacturing
Electrical Equipment Manufacturing
Electric Lighting Equipment Manufacturing
Electric Power Generation
Electric Power Transmission; Control; and Distribution
Electronic and Precision Equipment Maintenance
Embedded Software Products
Emergency and Relief Services
Energy Technology
Engineering Services
Engines and Power Transmission Equipment Manufacturing
Entertainment
Entertainment Providers
Environmental Quality Programs
Environmental Services
Equipment Rental Services
Events Services
Executive Office
Executive Offices
Executive Search Services
Fabricated Metal Products
Facilities Services
Family Planning Centers
Farming
Farming; Ranching; Forestry
Fashion Accessories Manufacturing
Financial Services
Fine Art
Fine Arts Schools
Fire Protection
Fisheries
Fishery
Flight Training
Food and Beverage Manufacturing
Food and Beverage Retail
Food and Beverages
Food and Beverage Services
Food Production
Footwear and Leather Goods Repair
Footwear Manufacturing
Forestry and Logging
Fossil Fuel Electric Power Generation
Freight and Package Transportation
Fruit and Vegetable Preserves Manufacturing
Fuel Cell Manufacturing
Fundraising
Funds and Trusts
Funeral Services
Furniture
Furniture and Home Furnishings Manufacturing
Gambling and Casinos
Gambling Facilities and Casinos
Geothermal Electric Power Generation
Glass; Ceramics and Concrete
Glass; Ceramics and Concrete Manufacturing
Glass Product Manufacturing
Golf Courses and Country Clubs
Government Administration
Government Relations
Government Relations Services
Graphic Design
Ground Passenger Transportation
Health and Human Services
Health; Wellness and Fitness
Higher Education
Highway; Street; and Bridge Construction
Historical Sites
Holding Companies
Home Health Care Services
Horticulture
Hospital and Health Care
Hospitality
Hospitals
Hospitals and Health Care
Hotels and Motels
Household and Institutional Furniture Manufacturing
Household Appliance Manufacturing
Household Services
Housing and Community Development
Housing Programs
Human Resources
Human Resources Services
HVAC and Refrigeration Equipment Manufacturing
Hydroelectric Power Generation
Import and Export
Individual and Family Services
Industrial Automation
Industrial Machinery Manufacturing
Industry Associations
Information Services
Information Technology and Services
Insurance
Insurance Agencies and Brokerages
Insurance and Employee Benefit Funds
Insurance Carriers
Interior Design
International Affairs
International Trade and Development
Internet
Internet Marketplace Platforms
Internet News
Internet Publishing
Interurban and Rural Bus Services
Investment Advice
Investment Banking
Investment Management
IT Services and IT Consulting
IT System Custom Software Development
IT System Data Services
IT System Design Services
IT System Installation and Disposal
IT System Operations and Maintenance
IT System Testing and Evaluation
IT System Training and Support
Janitorial Services
Judiciary
Landscaping Services
Language Schools
Laundry and Drycleaning Services
Law Enforcement
Law Practice
Leasing Non-residential Real Estate
Leasing Residential Real Estate
Leather Product Manufacturing
Legal Services
Legislative Offices
Leisure; Travel and Tourism
Libraries
Lime and Gypsum Products Manufacturing
Loan Brokers
Logistics and Supply Chain
Luxury Goods and Jewelry
Machinery
Machinery Manufacturing
Magnetic and Optical Media Manufacturing
Management Consulting
Manufacturing
Maritime
Maritime Transportation
Marketing and Advertising
Marketing Services
Market Research
Mattress and Blinds Manufacturing
Measuring and Control Instrument Manufacturing
Meat Products Manufacturing
Mechanical Or Industrial Engineering
Media and Telecommunications
Media Production
Medical and Diagnostic Laboratories
Medical Device
Medical Equipment Manufacturing
Medical Practice
Medical Practices
Mental Health Care
Metal Ore Mining
Metal Treatments
Metal Valve; Ball; and Roller Manufacturing
Metalworking Machinery Manufacturing
Military
Military and International Affairs
Mining
Mining and Metals
Mobile Computing Software Products
Mobile Food Services
Mobile Games
Mobile Gaming Apps
Motion Pictures and Film
Motor Vehicle Manufacturing
Motor Vehicle Parts Manufacturing
Movies and Sound Recording
Movies; Videos; and Sound
Museums
Museums and Institutions
Museums; Historical Sites; and Zoos
Music
Musicians
Nanotechnology
Nanotechnology Research
Natural Gas Distribution
Natural Gas Extraction
Newspaper Publishing
Newspapers
Nonmetallic Mineral Mining
Non-profit Organization Management
Non-profit Organizations
Nonresidential Building Construction
Nuclear Electric Power Generation
Nursing Homes and Residential Care Facilities
Office Administration
Office Furniture and Fixtures Manufacturing
Oil and Coal Product Manufacturing
Oil and Energy
Oil and Gas
Oil Extraction
Oil; Gas; and Mining
Online and Mail Order Retail
Online Audio and Video Media
Online Media
Operations Consulting
Optometrists
Other
Outpatient Care Centers
Outsourcing and Offshoring Consulting
Outsourcing/Offshoring
Package/Freight Delivery
Packaging and Containers
Packaging and Containers Manufacturing
Paint; Coating; and Adhesive Manufacturing
Paper and Forest Product Manufacturing
Paper and Forest Products
Parts Distribution
Pension Funds
Performing Arts
Performing Arts and Spectator Sports
Periodical Publishing
Personal and Laundry Services
Personal Care Product Manufacturing
Personal Care Services
Pet Services
Pharmaceutical Manufacturing
Pharmaceuticals
Philanthropic Fundraising Services
Philanthropy
Photography
Physical; Occupational and Speech Therapists
Physicians
Pipeline Transportation
Plastics
Plastics and Rubber Product Manufacturing
Plastics Manufacturing
Political Organization
Political Organizations
Postal Services
Primary and Secondary Education
Primary Metal Manufacturing
Primary/Secondary Education
Printing
Printing Services
Professional Organizations
Professional Services
Professional Training and Coaching
Program Development
Public Assistance Programs
Public Health
Public Policy
Public Policy Offices
Public Relations and Communications
Public Relations and Communications Services
Public Safety
Public Works
Publishing
Racetracks
Radio and Television Broadcasting
Railroad Equipment Manufacturing
Railroad Manufacture
Rail Transportation
Ranching
Ranching and Fisheries
Real Estate
Real Estate Agents and Brokers
Real Estate and Equipment Rental Services
Recreational Facilities
Recreational Facilities and Services
Regenerative Design
Religious Institutions
Renewable Energy Equipment Manufacturing
Renewable Energy Power Generation
Renewable Energy Semiconductor Manufacturing
Renewables and Environment
Repair and Maintenance
Research
Research Services
Residential Building Construction
Restaurants
Retail
Retail Apparel and Fashion
Retail Appliances; Electrical; and Electronic Equipment
Retail Art Dealers
Retail Art Supplies
Retail Books and Printed News
Retail Building Materials and Garden Equipment
Retail Florists
Retail Furniture and Home Furnishings
Retail Gasoline
Retail Groceries
Retail Health and Personal Care Products
Retail Luxury Goods and Jewelry
Retail Motor Vehicles
Retail Musical Instruments
Retail Office Equipment
Retail Office Supplies and Gifts
Retail Pharmacies
Retail Recyclable Materials and Used Merchandise
Reupholstery and Furniture Repair
Robotics Engineering
Robot Manufacturing
Rubber Products Manufacturing
Satellite Telecommunications
Savings Institutions
School and Employee Bus Services
Seafood Product Manufacturing
Secretarial Schools
Securities and Commodity Exchanges
Security and Investigations
Security Guards and Patrol Services
Security Systems Services
Semiconductor Manufacturing
Semiconductors
Services for Renewable Energy
Services for the Elderly and Disabled
Sheet Music Publishing
Shipbuilding
Shuttles and Special Needs Transportation Services
Sightseeing Transportation
Skiing Facilities
Smart Meter Manufacturing
Soap and Cleaning Product Manufacturing
Social Networking Platforms
Software Development
Solar Electric Power Generation
Sound Recording
Space Research and Technology
Specialty Trade Contractors
Spectator Sports
Sporting Goods
Sporting Goods Manufacturing
Sports
Sports and Recreation Instruction
Sports Teams and Clubs
Spring and Wire Product Manufacturing
Staffing and Recruiting
Steam and Air-Conditioning Supply
Strategic Management Services
Subdivision of Land
Sugar and Confectionery Product Manufacturing
Supermarkets
Surveying and Mapping Services
Taxi and Limousine Services
Technical and Vocational Training
Technology; Information and Internet
Technology; Information and Media
Telecommunications
Telecommunications Carriers
Telephone Call Centers
Temporary Help Services
Textile Manufacturing
Textiles
Theater Companies
Think Tanks
Tobacco
Tobacco Manufacturing
Translation and Localization
Transportation Equipment Manufacturing
Transportation; Logistics; Supply Chain and Storage
Transportation Programs
Transportation/Trucking/Railroad
Travel Arrangements
Truck Transportation
Trusts and Estates
Turned Products and Fastener Manufacturing
Urban Transit Services
Utilities
Utilities Administration
Utility System Construction
Vehicle Repair and Maintenance
Venture Capital and Private Equity
Venture Capital and Private Equity Principals
Veterinary
Veterinary Services
Vocational Rehabilitation Services
Warehousing
Warehousing and Storage
Waste Collection
Waste Treatment and Disposal
Water Supply and Irrigation Systems
Water; Waste; Steam; and Air Conditioning Services
Wellness and Fitness Services
Wholesale
Wholesale Alcoholic Beverages
Wholesale Apparel and Sewing Supplies
Wholesale Appliances; Electrical; and Electronics
Wholesale Building Materials
Wholesale Chemical and Allied Products
Wholesale Computer Equipment
Wholesale Drugs and Sundries
Wholesale Food and Beverage
Wholesale Footwear
Wholesale Furniture and Home Furnishings
Wholesale Hardware; Plumbing; Heating Equipment
Wholesale Import and Export
Wholesale Luxury Goods and Jewelry
Wholesale Machinery
Wholesale Metals and Minerals
Wholesale Motor Vehicles and Parts
Wholesale Paper Products
Wholesale Petroleum and Petroleum Products
Wholesale Photography Equipment and Supplies
Wholesale Raw Farm Products
Wholesale Recyclable Materials
Wind Electric Power Generation
Wine and Spirits
Wineries
Wireless
Wireless Services
Women's Handbag Manufacturing
Wood Product Manufacturing
Writing and Editing
Zoos and Botanical Gardens
```

</details>
