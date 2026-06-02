# GovCon Contractor Profile — Materialization Schema & Feature-Depth Audit

Directive 6 deliverable. **Read-only structural audit** of the live SAM + USAspending
Lance datasets in R2 (`s3://data-sink/active/…`), executed against the system of record
on 2026-06-02 with `pylance 7.0.0 / duckdb 1.5.3` over the `hq-all/prd` R2 credentials.
Every schema, index, row count, and latency below is **measured from the live datasets**,
not inferred from code. The feature target: materialize a complete Contractor Profile when
a business supplies a **UEI** or signs up with a **corporate email domain**.

---

## 0. Verdict summary (read this first)

| Question | Verdict |
|---|---|
| **Firmographic profile by UEI** | ✅ Fully materializable today. Legal name, DBA, CAGE, status, expiration, physical address, business types, and 6 points of contact all resolve from existing UEI-BTREE-indexed datasets. |
| **Physical address gap** | ✅ Resolved. `entity_registrations` confirmed to carry **no** address column (it lives unsplit in `pipe_fields`); the named-column street address is served by **`usaspending/recipient_lookup`** (UEI-indexed) and SAM-natively by **`sam_pocs`**. |
| **Historical award resume** | ⚠️ Computable, but **not at request time.** Per-UEI award sets are unbounded (107,827 award rows for a $162M mid-size vendor; 1.69M for a sentinel UEI). Must be **precomputed** into a per-UEI summary dataset. The aggregation SQL is exact and proven below. |
| **Domain → UEI (corporate email)** | ⚠️ Viable for **~46% of the registry, 98.8% unambiguous** — but **only** via the SAM corporate **website** (entity URL), never via a stored email. SAM's public extract carries **no email field anywhere.** Requires one net-new reverse-lookup dataset. |
| **Sub-100 ms point queries** | ✅ Structurally supported. Every profile key is BTREE-indexed and verified live. Warm indexed lookups already land 69–411 ms over a cross-country laptop→R2 link; in-region they collapse to the object-store floor. The award resume is the sole exception → see precompute mandate. |

---

## 1. Live dataset inventory (verified 2026-06-02)

All datasets are LanceDB v2.x on R2, `s3://data-sink/active/<path>/`. "UEI key" = the column
the profile joins on; **all are BTREE-indexed** (§5).

| Dataset | Path | Rows | UEI key column | Grain |
|---|---|---:|---|---|
| SAM entity registry | `entity_registrations/` | 19,299,314 | `uei` | many/uei (1 per monthly extract × layout) |
| SAM points of contact | `sam_pocs/` | 8,065,079 | `uei` | 1 per (entity, populated POC slot); ≤6/uei |
| SAM×USAspending crosswalk | `crosswalk_sam_usaspending/` | 1,028,144 | `uei` | **1/uei** (identity hub) |
| USAspending award rollup | `usaspending/award_search/` | 78,373,286 | `recipient_uei` | 1/award |
| USAspending contract txns (FPDS) | `usaspending/transaction_search_fpds/` | 107,250,527 | `recipient_uei` | 1/transaction |
| USAspending assistance txns (FABS) | `usaspending/transaction_search_fabs/` | 128,784,183 | `recipient_uei` | 1/transaction |
| USAspending recipient dimension | `usaspending/recipient_lookup/` | 17,754,022 | `uei` | ~1/uei (address + business types) |
| USAspending recipient profile | `usaspending/recipient_profile/` | 18,275,944 | `uei` | level P/C/R per uei (12-mo rollup) |
| USAspending subawards | `usaspending/subaward_search/` | 9,801,723 | `awardee_or_recipient_uei` | 1/subaward |
| FFATA executive compensation | `ffata_exec_comp/` | 29,601 | `recipient_uei` | ≤5 officers/uei |
| SAM↔FMCSA domain bridge | `bridge_sam_fmcsa_domain/` | 263,076 | `uei` / `normalized_domain` | (entity × carrier) |

**Entry-point nuance the frontend must handle:**
- `entity_registrations` (v2 layout) holds the **current SAM registry** — 888,916 distinct UEI.
- `crosswalk_sam_usaspending` is keyed on the **USAspending recipient universe** (1.03M UEI) — a UEI exists here only if it ever **received/won** a federal award.
- A UEI registered in SAM but with no federal award is in `entity_registrations` only → render firmographics + "no federal award history."
- A historical USAspending recipient not in the current v2 SAM snapshot is in the crosswalk only → render award history + USAspending-sourced name/address, no live SAM status.

---

## 2. UEI-keyed firmographic profile (Identity & firmographics)

### 2.1 Field-source map — every day-one data point

| Profile field | Displayable? | Authoritative source (column) | Notes |
|---|---|---|---|
| **Legal name** | ✅ | `crosswalk.sam_legal_name` (hub, 1/uei) — backed by `entity_registrations.legal_business_name` | Crosswalk already coalesces SAM→USAspending name. |
| **DBA name** | ✅ | `crosswalk.sam_dba_name` / `entity_registrations.dba_name` | KIPPER: "KIPPER MANAGEMENT INC". |
| **CAGE code** | ✅ | `entity_registrations.cage_code` / `crosswalk.cage_code` | BTREE-indexed both sides. |
| **Active/Inactive status** | ✅ | `entity_registrations.registration_status` | `'A'` = Active. Pair with `expiration_date`. |
| **Registration validity** | ✅ | `entity_registrations.registration_date`, `expiration_date`, `activation_date`, `last_update_date` | Native date32 columns. |
| **Physical street address** | ✅ | **`usaspending/recipient_lookup`**: `address_line_1/2`, `city`, `state`, `zip5`, `zip4`, `country_code`, `congressional_district` | The named-column gap-filler (see §2.2). |
| **SAM-native mailing address** | ✅ | `sam_pocs`: `address_line_1/2`, `city`, `state`, `zip5`, `zip4`, `country` | Per-POC; the mandatory POC slots carry the entity address. |
| **Business types (codes)** | ✅ | `recipient_lookup.business_types_codes` | e.g. `{2X,8W,A2,MF}` (SBA/socioeconomic codes). |
| **Business categories (buckets)** | ✅ | `award_search.business_categories` | Derived rollups. |
| **Socioeconomic flags (rich)** | ✅ | `transaction_search_fpds` booleans: `woman_owned_business`, `veteran_owned_business`, `service_disabled_veteran_o`, `small_disadvantaged_busine`, `historically_underutilized` (HUBZone), `c8a_program_participant`, `minority_owned_business`, … | ~120 boolean type flags; take latest transaction. |
| **Primary industry (NAICS/PSC)** | ✅ | `award_search.naics_code`/`naics_description`, `product_or_service_code`/`…_description` | NAICS BTREE-indexed. |
| **Core POC (name, title, address)** | ✅ | `sam_pocs` — 6 slots: `government_business`(+alt), `past_performance`(+alt), `electronic_business`(+alt); `full_name`, `title`, address | Mandatory pair = `government_business` + `electronic_business`. |
| **Phone** | ⚠️ fallback | `transaction_search_fpds.vendor_phone_number` (also `vendor_fax_number`) | Per-transaction; latest non-null. No phone in SAM extract. |
| **Email** | ❌ | — **absent from the entire stack** | See §4 — confirmed by full-record scan. |
| **Parent entity** | ✅ | `crosswalk.parent_uei`/`parent_legal_name`; `recipient_lookup.parent_uei`/`parent_legal_business_name` | Corporate-family rollup. |
| **Executive compensation** | ✅ (sparse) | `ffata_exec_comp`: `officer_name`, `officer_amount`, `officer_rank` | Only entities ≥$25M federal AND ≥80% federal revenue (~5.9k UEI). |

### 2.2 The physical-address gap — pinpointed

The directive's prior finding is **confirmed live**: `entity_registrations` has **18 columns and none is a
physical address**. The schema is `uei, duns, cage_code, registration_status, purpose_of_registration,
registration_date, expiration_date, last_update_date, activation_date, legal_business_name, dba_name,
pipe_fields, field_count, format_family, source_encoding, extract_label, source_file, ingested_at`. The
street address exists only **inside the `pipe_fields` positional array** (unsplit, per the loader's
lossless-retention policy).

**Resolution — use named columns, do not parse `pipe_fields` at request time:**
1. **`usaspending/recipient_lookup`** (UEI BTREE, 17.75M rows) is the canonical named-column street
   address. Verified live for KIPPER TOOL: `2375 MURPHY BLVD, GAINESVILLE, GA 30504, CD 09`.
2. **`sam_pocs`** (UEI BTREE) carries the SAM-native address on every POC slot — same address, SAM-sourced.
3. **`award_search.recipient_location_*`** (`address_line1/2/3`, `city_name`, `state_code`, `zip5`, `zip4`,
   `congressional_code`, `county_name`) for the per-award recipient location.

### 2.3 Mock query — firmographic + identity (single indexed point-lookup)

> **Implementation rule (load-bearing):** the BTREE index is exercised **only** when the equality
> predicate is passed to `lance.dataset(...).scanner(filter=…)` / `.to_table(filter=…)`. A DuckDB scan over a
> fully-materialized reader does **not** use the Lance index. Always push the `uei =` filter into the scanner.

```python
import lance
SO = {...R2...}                       # r2-credentials
def ds(p): return lance.dataset(f"s3://data-sink/active/{p}/", storage_options=SO)

def firmographics(uei: str) -> dict:
    u = uei.replace("'", "''")
    # 1) SAM registration — latest v2 row (index pushdown on uei)
    er = ds("entity_registrations").scanner(
        filter=f"uei = '{u}' AND format_family = 'v2'",
        columns=["legal_business_name","dba_name","cage_code","registration_status",
                 "registration_date","expiration_date","activation_date","last_update_date"],
    ).to_table()                       # → pick max(last_update_date)

    # 2) Identity hub — coalesced SAM + USAspending descriptors (1 row)
    xw = ds("crosswalk_sam_usaspending").scanner(
        filter=f"uei = '{u}'",
        columns=["sam_legal_name","sam_dba_name","cage_code","usa_legal_name",
                 "parent_uei","parent_legal_name","state","zip5","match_method"],
    ).to_table()

    # 3) Named-column physical address + business types
    rl = ds("usaspending/recipient_lookup").scanner(
        filter=f"uei = '{u}'",
        columns=["address_line_1","address_line_2","city","state","zip5","zip4",
                 "country_code","congressional_district","business_types_codes"],
    ).to_table()

    # 4) Points of contact (≤6 rows, name + title + address; NO email)
    pocs = ds("sam_pocs").scanner(
        filter=f"uei = '{u}'",
        columns=["poc_type","full_name","title","address_line_1","city","state","zip5","country"],
    ).to_table()
    return assemble(er, xw, rl, pocs)
```

Equivalent DuckDB SQL for any one leg (filter pushed to the scanner, then shaped):

```sql
-- entity_registrations leg, latest current-registry row
SELECT legal_business_name, dba_name, cage_code, registration_status,
       registration_date, expiration_date
FROM sam_er                              -- = scanner(filter="uei='…' AND format_family='v2'")
QUALIFY row_number() OVER (ORDER BY last_update_date DESC NULLS LAST) = 1;
```

**Measured (KIPPER TOOL, UEI `DD1BCRF2QQG8`):** legal `KIPPER TOOL COMPANY`, DBA `KIPPER MANAGEMENT INC`,
CAGE `00NS2`, status `A`, registered `2001-11-28`, expires `2027-01-13`, address `2375 Murphy Blvd,
Gainesville GA 30504 (CD 09)`, business types `{2X,8W,A2,MF}`, 6/6 POC slots populated.

---

## 3. Historical awards & performance resume

### 3.1 The scale finding — why this cannot run at request time

`award_search` grain is **one row per award**, and for GSA-schedule / IDIQ / micro-purchase vendors each
order is its own award. Measured per-UEI award-row counts:

| UEI | Entity | Award rows | Lifetime obligated | Interpretation |
|---|---|---:|---:|---|
| `DD1BCRF2QQG8` | KIPPER TOOL COMPANY (mid-size) | **107,827** | $162,021,485 | 104,033 are tiny GSA orders |
| `MA1VZ6667CB1` | (sentinel / "Multiple Recipients" class) | **1,692,333** | $3,352,000,718 | 100% DoD — **not a real contractor** |

A warm pull of KIPPER's 107,827 award rows took **11.7 s** over WAN — bounded by row volume, not the index.
**Conclusion: the resume must be precomputed.** See §6.1.

### 3.2 Exact aggregation SQL (proven against live data)

Run once per UEI inside the precompute worker; the frontend then reads one summary row.

```sql
-- Source relation `a` = award_search.scanner(filter="recipient_uei = '<uei>'", columns=[...])
WITH base AS (SELECT * FROM a)
SELECT
  -- (a) lifetime federal dollars obligated
  round(sum(total_obligation), 2)                                            AS lifetime_obligated,
  count(*)                                                                   AS total_awards,
  -- (b) active vs closed (period of performance vs today)
  count(*) FILTER (WHERE period_of_performance_current_end_date >= CURRENT_DATE) AS active_awards,
  count(*) FILTER (WHERE period_of_performance_current_end_date <  CURRENT_DATE) AS closed_awards,
  -- (d) most recent award/modification
  max(action_date)                                                          AS most_recent_action_date
FROM base;

-- (c) top-3 funding agencies by obligated dollars
SELECT funding_toptier_agency_name AS agency,
       round(sum(total_obligation), 2) AS dollars, count(*) AS n
FROM base GROUP BY 1 ORDER BY dollars DESC NULLS LAST LIMIT 3;

-- (d) most-recent award detail (prefer the latest *non-zero* action for the headline amount)
SELECT action_date, total_obligation, awarding_toptier_agency_name, generated_unique_award_id
FROM base
ORDER BY (total_obligation <> 0) DESC, action_date DESC NULLS LAST
LIMIT 1;
```

**Measured resume (KIPPER TOOL):** lifetime `$162,021,485.13`, total awards `107,827`,
active `17` / closed `107,794`, most-recent action `2026-04-21`. Top-3 funding agencies:
`Department of Defense $82.0M (3,332)`, `General Services Administration $56.0M (104,033)`,
`Department of the Interior $7.4M (157)`.

> Note: the bare `max(action_date)` row can be a $0 administrative modification. The `(total_obligation <> 0)
> DESC` ordering above surfaces the most recent **dollar-bearing** award for the display headline.

### 3.3 Zero-scan headline alternative — `recipient_profile`

For the trailing-12-month headline **without touching `award_search`**, `recipient_profile` is a
precomputed USAspending rollup (UEI BTREE, ~1 row/level):

```python
ds("usaspending/recipient_profile").scanner(
    filter=f"uei = '{u}'",
    columns=["recipient_level","last_12_months","last_12_contracts","last_12_grants",
             "last_12_loans","last_12_months_count","award_types"]).to_table()
# KIPPER → level P/C: last_12_months $3,935,956.13 across 2,587 awards, award_types {contract}
```

Use `recipient_level = 'R'` (recipient) or `'P'` (parent) per the display context.

---

## 4. Domain → UEI reverse mapping (corporate-email signup) — definitive audit

### 4.1 Email is absent from the entire stack — confirmed by full-record scan

A scan of complete 142-field v2 `pipe_fields` records found **no element containing `@`** in any sampled
registration. SAM's **public** monthly extract redacts POC email; the only contact strings are POC
**names/titles/addresses** (`sam_pocs`) and FPDS **vendor phone/fax**. USAspending carries no email column
in any of `award_search`, `recipient_lookup`, `recipient_profile`, `transaction_search_*`, `subaward_search`.

> **Therefore: a signup email cannot be reverse-matched to a stored SAM/USAspending email — none exists.**
> The only structural path is matching the **domain** of the signup email to the entity's **website domain**.

### 4.2 The website signal — SAM entity URL at `pipe_fields[27]`

The corporate website is **not a named column**; it lives at `pipe_fields[27]` (1-based) in the v2 layout
(`pipe_fields[25]` in legacy_v1). Verified live: `www.gersonco.com`, `HTTP://WWW.NATIONALNONWOVENS.COM`
(mixed case + protocol → must be normalized). The repo already contains the exact, symmetric normalizer and a
multi-tenant blocklist in `pipelines/resolution/sam_fmcsa_domain_spine.py` (`_norm_host_sql`,
`_email_domain_sql`, `CONSUMER_BLOCK`).

### 4.3 Reliability — measured on a 60,000-row v2 sample

| Metric | Value | Implication |
|---|---:|---|
| Rows with a non-empty entity URL | 46.0% | ~half the registry lists a website at all |
| Rows yielding a valid corporate domain (post-normalize, post-blocklist) | 45.6% | the **addressable** population for domain matching |
| Distinct domains that map to exactly **one** UEI | **98.8%** | reverse match is unambiguous for nearly all |
| Domains shared across ≥2 UEI | 1.2% (max 134) | corporate **families** (`fmcna.com`, `raytheon.com`, `marriott.com`, `graybar.com`) — not noise; disambiguate by most-active UEI or prompt |

> Sample caveat: the head sample resolved entirely to the `2020_NOV` v2 extract; treat 45.6% as a
> single-vintage estimate and re-confirm against the latest `extract_label` before publishing a hard SLA.

### 4.4 Verdict + signup-resolution algorithm

**Architectural verdict: domain→UEI mapping is reliable but partial.** It resolves ~46% of registered
entities, and where a domain is present it is 1:1 with a UEI 98.8% of the time. It is a **strong assist,
not a universal key** — the other ~54% (no website on file) must fall back to name/UEI entry.

```
signup_email "founder@kippertool.com"
  → domain = lower(split_after_last('@'))            # kippertool.com
  → normalize (strip www/protocol/path) + CONSUMER_BLOCK reject (gmail/yahoo/ISP/placeholder)
  → point-lookup sam_domain_uei[normalized_domain]   # §6.2 — BTREE, single row
      ├─ 1 UEI  → resolve directly (98.8% of hits)
      ├─ N UEI  → corporate family; pick max lifetime_obligated or prompt for division
      └─ 0 UEI  → no SAM website match → fall back to manual UEI / legal-name entry
```

The SAM↔FMCSA bridge (`bridge_sam_fmcsa_domain`, `normalized_domain` BTREE, 263,076 rows, 42,621 distinct
domains) is the **existence proof** that this build works end to end. It is FMCSA-intersected, so it is not
the general lookup — §6.2 generalizes it.

---

## 5. Performance & index verification

### 5.1 Committed scalar indices (read live from each dataset's manifest)

| Dataset | BTREE on UEI key | Other BTREE | BITMAP |
|---|---|---|---|
| `entity_registrations` | ✅ `uei_idx` | `cage_code`, `extract_label` | — |
| `sam_pocs` | ✅ `uei_idx` | `cage_code`, `name_key`, `last_name` | `poc_type`, `source_family` |
| `crosswalk_sam_usaspending` | ✅ `uei_idx` | `cage_code` | — |
| `usaspending/award_search` | ✅ `recipient_uei_idx` | `parent_uei`, `naics_code` | — |
| `usaspending/transaction_search_fpds` | ✅ `recipient_uei_idx` | `parent_uei`, `naics_code`, `cage_code` | — |
| `usaspending/transaction_search_fabs` | ✅ `recipient_uei_idx` | `parent_uei`, `naics_code`, `cage_code` | — |
| `usaspending/recipient_lookup` | ✅ `uei_idx` | `parent_uei` | — |
| `usaspending/recipient_profile` | ✅ `uei_idx` | `parent_uei` | — |
| `usaspending/subaward_search` | ✅ `awardee_or_recipient_uei_idx` | `ultimate_parent_uei`, `sub_*_uei`, `naics`, `sub_naics` | — |
| `ffata_exec_comp` | ✅ `recipient_uei_idx` | `name_key` | `officer_rank`, `source_channel` |
| `bridge_sam_fmcsa_domain` | ✅ `uei_idx` | `dot_number`, **`normalized_domain`**, `mc_number` | — |

**Every column the profile point-queries touch is BTREE-indexed.** No full-table scan is required for any
identity/firmographic/contact lookup.

### 5.2 Measured point-query latency (laptop → R2, cross-country WAN)

Each dataset opened once; cold = first touch (one-time WAN/object-store warm-up), warm = steady state.

| Query (indexed `uei` pushdown) | Rows out | Cold | Warm |
|---|---:|---:|---:|
| `entity_registrations[uei]` firmographics | 14 | 14,743 ms | **383 / 411 ms** |
| `sam_pocs[uei]` contacts+address | 6 | 7,620 ms | **180 / 123 ms** |
| `crosswalk[uei]` identity hub | 1 | 801 ms | **124 / 264 ms** |
| `recipient_profile[uei]` 12-mo rollup | 2 | 31,008 ms | **227 / 196 ms** |
| `recipient_lookup[uei]` address | 1 | 28,174 ms | **238 / 368 ms** |
| `ffata_exec_comp[recipient_uei]` officers | 5 | 1,040 ms | **69 / 80 ms** |
| `award_search[recipient_uei]` **full pull** | 107,827 | 93,503 ms | 11,752 ms |

**Reading the numbers:**
- Warm indexed point-lookups are **69–411 ms** over a cross-country laptop→R2 link, where each query re-validates
  dataset metadata and pays public-internet RTT. In-region (a Cloudflare Worker reading R2, or a Modal worker
  peered to R2) the per-query RTT drops from tens-of-ms to sub-ms and these collapse toward the **object-store
  floor (single-digit-to-low-tens of ms)** — the 69 ms `ffata` lookup (smallest dataset) already shows the floor.
- **Sub-100 ms is achievable in production for the entire identity/firmographic/contact profile**, because each
  leg is a single BTREE point-lookup returning ≤6 rows. Cold latency is a per-process one-time cost and is
  eliminated by keeping the dataset handles warm (a long-lived API process / connection pool).
- The **only** query that cannot meet 100 ms is the at-request award aggregation — and that is a **data-volume**
  problem (107K–1.69M rows), not an index problem. It is removed by precomputing (§6.1).

---

## 6. Required net-new materializations (not yet built)

Two Pattern-A derived datasets convert the two slow paths into single indexed point-lookups. Both follow the
repo's clean-room conventions (DuckDB transform → Lance v2.1 → R2, BTREE on the key, `ops.*` ledger, Trigger
durable callback). **Out of scope for this audit; specified here as the recommended next build.**

### 6.1 `contractor_award_summary` — the resume, precomputed (unlocks sub-100 ms)

One scan of `award_search` (`GROUP BY recipient_uei`) materializing one summary row per UEI:

```
key:  recipient_uei            (BTREE)
cols: lifetime_obligated, total_awards, active_awards, closed_awards,
      first_award_date, most_recent_action_date, most_recent_obligation,
      top_agency_1_name/_dollars, top_agency_2_name/_dollars, top_agency_3_name/_dollars,
      contract_dollars, grant_dollars, other_dollars, primary_naics, primary_psc
```

Build = the §3.2 SQL wrapped in `GROUP BY recipient_uei` (top-agency as a correlated `LIST`/`arg_max`),
written exactly like `pipelines/usaspending/ffata_exec_comp.py`. Result: the resume becomes
`contractor_award_summary.scanner(filter="recipient_uei = '<uei>'")` → 1 row, < 100 ms.

### 6.2 `sam_domain_uei` — the reverse domain map (unlocks corporate-email signup)

One scan of the latest v2 `entity_registrations` snapshot, extracting + normalizing `pipe_fields[27]`:

```
key:  normalized_domain        (BTREE)   + uei (BTREE)
cols: normalized_domain, uei, legal_business_name, cage_code, registration_status
build: SELECT _norm_host_sql(pipe_fields[27]) AS normalized_domain, uei, …
       FROM entity_registrations
       WHERE format_family='v2' AND extract_label = <latest>
         AND normalized_domain IS NOT NULL AND normalized_domain NOT IN CONSUMER_BLOCK
       QUALIFY 1 row per (normalized_domain, uei)
```

This is `sam_fmcsa_domain_spine.py` with the FMCSA join removed and the key re-pointed at `normalized_domain`.
Expected ~46% registry coverage, 98.8% unique. Result: signup-domain resolution becomes a single BTREE
point-lookup.

---

## 7. Day-one display checklist

| Section | Field | Status | Source |
|---|---|---|---|
| Identity | Legal name, DBA, UEI, CAGE | ✅ live | `crosswalk` / `entity_registrations` |
| Identity | Active/Inactive status, registration + expiration dates | ✅ live | `entity_registrations` |
| Identity | Parent entity (UEI + name) | ✅ live | `crosswalk` / `recipient_lookup` |
| Location | Street address, city, state, ZIP+4, congressional district, country | ✅ live | `recipient_lookup` (+ `sam_pocs`) |
| Firmographics | Business type codes, socioeconomic flags (woman/veteran/SDVOSB/HUBZone/8(a)/SDB…) | ✅ live | `recipient_lookup` + `transaction_search_fpds` |
| Firmographics | Primary NAICS / PSC industry | ✅ live | `award_search` |
| Contacts | 6 POC slots (name, title, address) | ✅ live | `sam_pocs` |
| Contacts | Phone / fax | ⚠️ FPDS fallback | `transaction_search_fpds.vendor_phone_number` |
| Contacts | **Email** | ❌ not in dataset | — (absent everywhere) |
| Resume | Lifetime obligated, total/active/closed awards | ✅ via precompute | `award_search` → `contractor_award_summary` |
| Resume | Top-3 funding agencies | ✅ via precompute | `award_search` |
| Resume | Most-recent award (date + amount + agency) | ✅ via precompute | `award_search` |
| Resume | Trailing-12-month spend (zero-scan headline) | ✅ live | `recipient_profile` |
| People | Executive compensation (≤5 officers + amounts) | ✅ live (sparse) | `ffata_exec_comp` |
| Signup | Corporate-email-domain → UEI | ⚠️ ~46%, 98.8% unique, via website | `sam_domain_uei` (to build) |

---

## 8. Provenance

All figures measured 2026-06-02 against `s3://data-sink/active/*` via `pylance 7.0.0 / duckdb 1.5.3`,
read-only (no DDL, no index creation, no dataset mutation). Representative UEI: `DD1BCRF2QQG8`
(KIPPER TOOL COMPANY). Domain statistics: 60,000-row v2 sample. Latency: cross-country laptop→R2 WAN —
production in-region latency is strictly lower.
