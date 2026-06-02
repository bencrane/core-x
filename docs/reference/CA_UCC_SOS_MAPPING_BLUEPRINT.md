# California Commercial Debt × Corporate Mapping Blueprint

Read-only forensic audit of the structural overlap between the **California
Secretary-of-State corporate registry** (`sos_normalized_master`, CA slice ≡
`ca_sos_entities`) and the **California UCC filing tables** (`ca_ucc/*`). Goal:
isolate reliable matching vectors to resolve a UCC debtor → a verified CA corporate
record, map the security-interest/lender landscape, and confirm index performance.

- **Scope:** `s3://data-sink/active/` (Lance v2.1 system-of-record, Cloudflare R2). No
  Iceberg/Polaris (Gen-3 clean room, per [ARCHITECTURE.md](ARCHITECTURE.md)).
- **As-of:** SoS snapshot `2026-05-31`; UCC `as_of 2026-05-31`; active-lien reference `2026-06-01`.
- **Evidence harness (reproducible, non-mutating):**
  [`pipelines/resolution/recon_ca_ucc_sos.py`](pipelines/resolution/recon_ca_ucc_sos.py)
  — run `modal run …::run` (profile + Test 1/2 + lenders) and `…::run2` (cardinality + index bench).
- **Attestation:** every figure below is from `lance.dataset(...).scanner()/to_table()` +
  DuckDB `SELECT`. Zero writes — no Lance datasets, no indexes, no `ops.*` rows, no R2 puts.

---

## 0. Headline findings

1. **No native join key exists.** The CA UCC bulk export carries **no CA SoS
   entity/filing number** on any debtor row (Test 1, confirmed across all 5 UCC
   datasets). UCC Article 9 financing statements are *name-indexed* (UCC §9-503), not
   keyed to the organic record. **Name + ZIP is the only bridge.**
2. **Canonical name-norm match is strong and high-precision.** Running the canonical
   `_name_norm` on UCC organization debtors against the `normalized_legal_name` BTREE
   spine matches **51.1%** of distinct org names exactly / **59.5%** of debtor
   appearances; suffix-stripping lifts this to **57.9% / 67.6%**. **93.4%** of exact
   matches resolve to a **single** SoS entity — low false-positive risk.
3. **The dominant "lenders" are governments, not banks.** 28.5% of filings are
   involuntary statutory liens (`filing_type` ∈ State/Federal Tax Lien, Judgment Lien).
   For consensual commercial debt, **filter to `filing_type = 'UCC'`** (71.5%).
4. **Lender names fragment even after normalization** — `WELLS FARGO BANK NA` and
   `WELLS FARGO BANK NATIONAL ASSOCIATION` land in two separate normalized buckets;
   filing-agents (`… AS REPRESENTATIVE`) mask the true secured party. A lender-alias
   canonicalization layer is required.
5. **Indices are built and exploited.** Every probed lookup hits a `ScalarIndexQuery …
   (BTree)` in the Lance physical plan; the master name lookup is **142 ms median vs
   4,496 ms** unindexed (~31×). Sub-second resolution confirmed.

---

## 1. Dataset topology, grain & committed indices

| Dataset | URI (`s3://data-sink/active/…`) | Rows | Grain | Committed indices |
|---|---|--:|---|---|
| `sos_normalized_master` | `sos_normalized_master/` | 17,926,543 | 1 / entity (CA·NY·FL·CO) | **BTREE** `normalized_legal_name`, `zip_code`; **BITMAP** `source_state` |
| `ca_sos_entities` | `ca_sos_entities/` | 9,389,688 | 1 / CA entity | **BTREE** `entity_num`, `entity_name_clean`, `last_si_file_number`; **BITMAP** `entity_type`, `entity_status`, `jurisdiction`, `filing_type`, `standing_sos`, `standing_ftb`, `principal_state`, `mailing_state` |
| `ca_ucc/filings` | `ca_ucc/filings/` | 7,751,890 | 1 / filing **event** | **BTREE** `ucc1_num`, `ucc3_num`; **BITMAP** `action_type`, `filing_type`, `alt_designation_type` |
| `ca_ucc/debtors` | `ca_ucc/debtors/` | 5,855,416 | N / filing | **BTREE** `ucc1_num`, `ucc3_num`, `org_name`, `last_name`, `postal_code`, `city`; **BITMAP** `debtor_type`, `state`, `country` |
| `ca_ucc/secured_parties` | `ca_ucc/secured_parties/` | 4,743,627 | N / filing | same key set as debtors (`org_name` etc.); **BITMAP** `secured_party_type`, `state`, `country` |
| `ca_ucc/filing_amendments` | `ca_ucc/filing_amendments/` | 3,305,823 | UCC3 → UCC1 bridge | **BTREE** `ucc1_num`, `ucc3_num`; **BITMAP** `action_type` |
| `ca_ucc/debtor_index` | `ca_ucc/debtor_index/` | 5,855,416 | 1 / debtor appearance (Tier-2 derived) | **BTREE** `ucc1_num`, `ucc3_num`, `debtor_org_name`, `debtor_last_name`, `debtor_postal_code`; **BITMAP** `debtor_type`, `debtor_state`, `action_type` |

The CA slice of the master (`source_state='CA'`) is **9,389,688 rows ≡ `ca_sos_entities`** row-for-row.
For CA, `master.original_entity_id == ca_sos_entities.entity_num` — that equality is the
spine→raw join used wherever `entity_type` (master-absent) is needed.

---

## 2. Schema deep-dive & null densities

### 2.1 Corporate layer — CA SoS

**Resolution key — `entity_num`** (VARCHAR, never cast; leading zeros are significant):

| Length | Rows | Example | Vintage |
|--:|--:|---|---|
| 7 | 4,952,263 | `1011108` | legacy corporate |
| 12 | 4,275,004 | `198504600073` | modern `YYYY…seq` |
| 8 | 162,411 | `70006740` | mixed |

**Blocking key — `normalized_legal_name`** (the master's pre-normalized name, BTREE-indexed):
99.99997% populated (3 nulls of 9.39M), **8,879,368 distinct** → duplication ratio **1.057**
(≈5.7% of names shared by ≥2 entities — the homonym floor that drives match ambiguity).
`zip_code` (ZIP5 from `principal_postal_code`): **13.18% null** — a real ceiling on the ZIP co-block.

**Corporate form — `entity_type`** (lives **only** on raw `ca_sos_entities`, BITMAP-indexed; the
master drops it). Rich descriptive vocabulary, not a bare LLC/INC/LP code:

| entity_type | Rows |
|---|--:|
| Stock Corporation - CA - General | 3,800,663 |
| Limited Liability Company - CA | 3,366,572 |
| Stock Corporation - Out of State - Stock | 455,646 |
| Limited Liability Company - Out of State | 440,291 |
| Nonprofit Corporation - CA - Public Benefit | 367,598 |
| Limited Partnership - CA | 216,107 |
| Legacy Corporation | 154,763 |
| Name Reservation | 118,307 |
| Stock Corporation - CA - Professional | 108,583 |
| (28 more: nonprofit subtypes, cooperatives, corporation sole, …) | … |

**Status — `entity_status`** (raw title-case; master upper-cases). "Active" is **~40%**; the
forfeited/dissolved/suspended population the directive targets is the ~60% complement:

| entity_status | Rows | Class |
|---|--:|---|
| Active | 3,718,645 | active |
| Terminated | 2,641,741 | dissolved |
| Suspended - FTB | 1,818,376 | suspended (tax) |
| Suspended - FTB/SOS | 540,040 | suspended |
| Forfeited - FTB | 161,001 | forfeited |
| Inactive | 117,734 | inactive |
| Merged Out | 95,461 | merged |
| Terminated - FTB Admin | 80,869 | dissolved |
| Forfeited - FTB/SOS | 72,151 | forfeited |
| Converted Out | 47,935 | converted |
| Active - Pending Termination | 40,776 | active(→term) |
| (13 more SOS/VCFCF/court-order variants) | … | |

`standing_sos`, `standing_ftb`, `standing_agent`, `suspension_date` give finer-grained
good-standing signals; `principal_*` and `mailing_*` give two distinct address blocks
(plus `principal_*_in_ca` for foreign entities' CA address).

### 2.2 UCC layer — CA UCC

**Debtors (5,855,416 rows).** `debtor_type` cleanly partitions org vs individual, perfectly
aligned with name-field population:

| debtor_type | Rows | has org_name | has last_name |
|---|--:|--:|--:|
| Organization | 3,681,852 (62.9%) | 3,681,852 | 0 |
| Individual | 2,173,564 (37.1%) | 0 | 2,173,560 |

Null density: `org_name` 37.12% (= the individuals), `last_name` 62.88% (= the orgs),
**`addr1` 0.00%**, `city` 0.03%, `state` 0.06%, `postal_code` 0.27%. **Address coverage is
effectively total** — debtor location is reliable blocking material.

Geography: **CA 5,634,621 (96.2%)**, then TX 25,351 · NY 18,234 · NV 15,884 · AZ 14,021 · FL 13,619 …

`postal_code` format is a **mix** (the processing pitfall): ZIP5 `92373` 4,698,047 (80.2%),
ZIP9 no-hyphen `801115002` 831,461 (14.2%), ZIP+4 hyphen `92335-4347` 306,664 (5.2%), plus a
small junk tail (`HMHX`, foreign, truncated). The canonical `_zip5` (digits-only → left-5)
normalizes all valid forms to a clean ZIP5.

**Secured parties (4,743,627 rows).** `secured_party_type`: Organization 4,684,136 (98.7%),
Individual 59,491 (1.3%). Null density: `org_name` 1.25%, `last_name` 98.75%, `city` 0.07%,
`state` 0.11%, `postal_code` 0.26% — lender name + location is dense.

**Filings (7,751,890 rows / 4,437,136 distinct `ucc1_num`).** `ucc1_num`/`ucc3_num` are STRICT
VARCHAR with significant leading zeros. Dates (`filing_date`, `processed_date`, `lapse_date`)
are timestamps; `lapse_date` is **100% populated** (sentinel `9999-12-31` = non-lapsing).

---

## 3. Cross-layer entity resolution

### 3.1 Test 1 — State Filing-ID match → **NEGATIVE (structural)**

The recon scanned every column of all five UCC datasets for any SoS-entity/filing-number-like
field. **Result: zero.** Debtor columns are exactly: `ucc1_num, ucc3_num, debtor_type, org_name,
last_name, first_name, middle_name, suffix, addr1, addr2, addr3, city, state, postal_code,
country` (+ provenance). `ucc1_num`/`ucc3_num` are the UCC's **own** document keys, not SoS keys.

**Why this is structural, not a feed defect:** a UCC-1 financing statement is sufficient under
**UCC §9-503(a)** if it provides the debtor's name *as it appears on the public organic record*.
The statute indexes by **name**, never by the filing office's entity number. The CA SOS bulk
"Data Request" export therefore has no column to carry it. **No deterministic ID join is
possible; all resolution is name+geo.** (Contrast: the federal spines bridge on hard keys —
LEI in [`crosswalk_hmda_gleif.py`](pipelines/resolution/crosswalk_hmda_gleif.py), UEI/CAGE in
[`crosswalk_sam_usaspending.py`](pipelines/resolution/crosswalk_sam_usaspending.py). The
UCC↔SoS bridge has no such luxury and must mirror the *name-block* discipline instead.)

### 3.2 Test 2 — Composite string-normalization match → **empirical baseline**

**Canonical normalization** (byte-identical to
[`sos_normalized/normalize.py::_name_norm`](pipelines/sos_normalized/normalize.py); the same
macro that built the spine's indexed column, so equality against the BTREE is valid):

```sql
-- _name_norm: UPPER → strip every non-[A-Z0-9 space] → collapse whitespace → trim → NULL if empty
nullif(trim(regexp_replace(regexp_replace(upper(CAST(x AS VARCHAR)),
       '[^A-Z0-9 ]+', '', 'g'), '\s+', ' ', 'g')), '')
```

It strips punctuation/`&`/accents and uppercases, but **preserves the entity-ending token**:
`"PACIFIC TRUCKING, LLC"` → `PACIFIC TRUCKING LLC`, while `"PACIFIC TRUCKING"` → `PACIFIC
TRUCKING`. The two do **not** match under exact equality — the central drift the directive named.

**Match tiers** (UCC organization debtors → CA spine; `org_name IS NOT NULL`):

| Tier | Logic | Distinct-name grain | Appearance grain |
|---|---|--:|--:|
| **A. Exact** | `_name_norm(org_name) = normalized_legal_name` | 855,099 / 1,672,689 = **51.12%** | 2,190,099 / 3,681,845 = **59.48%** |
| **B. Suffix-stripped** | strip trailing legal-form tokens both sides, then equality | 912,497 / 1,576,624 = **57.88%** | 2,488,672 / 3,681,845 = **67.59%** |
| **C. Core + ZIP5 co-block** | Tier-B core **and** equal ZIP5 | 604,356 / 1,852,654 = **32.62%** | — |

Suffix-strip (the controlled trailing-token peel — `INCORPORATED|CORPORATION|COMPANY|LIMITED|
LLLP|PLLC|LLP|LLC|CORP|INC|LTD|LP|CO|PC|PA`, longest-first for RE2 leftmost matching) adds
**+6.8 pp** of distinct names and **+8.1 pp** of appearances over exact. Tier C is the
*high-precision* lens: requiring a ZIP5 co-hit suppresses homonym collisions but is capped by
the spine's 13.2% null ZIP and the geographic spread of principal vs operational addresses.

**Match ambiguity (Tier A — distinct SoS entities per matched name):**

| SoS entities per matched name | Matched names | Share |
|---|--:|--:|
| **1** | 798,801 | **93.4%** |
| 2–3 | 53,693 | 6.3% |
| 4–10 | 2,588 | 0.3% |
| 10+ | 17 | ~0% |

**93.4% of exact matches are unambiguous** (resolve to one entity) → exact-tier resolution is
safe to auto-accept. The 6.6% multi-entity tail is where ZIP5/address disambiguation earns its
keep. Drift recovered by suffix-strip (Tier-B-not-A) is exactly the predicted pattern — the UCC
debtor omits the ending the SoS record carries: `STARK LEAK DETECTION LLC`→core `STARK LEAK
DETECTION`; `STEP BY STEP`, `JWF CONSTRUCTION`, `ALM TRUCKING`, `JOES TRUCKING` matched a SoS
`…INC/…LLC`. (Watch the false-positive risk: `SEAN WAYNE PIERCE`-style sole-prop org names can
collide with a person registered as an entity — Tier B should be ZIP-gated, never auto-accepted.)

### 3.3 The Structural Mapping Vector (production join logic)

A `bridge_ca_ucc_sos` Pattern-B worker (under `pipelines/resolution/`) mirroring the
established name-block convention — derive the key with the **same macro on both sides**, then
equijoin the indexed column; suppress ambiguity the way
[`sam_fmcsa_domain_spine.py`](pipelines/resolution/sam_fmcsa_domain_spine.py) NULLs multi-tenant
keys:

```sql
-- Spine (read once, BITMAP-pushed to CA): the indexed blocking key + a core + ZIP5.
WITH spine AS (
  SELECT original_entity_id AS sos_entity_num, normalized_legal_name AS nln,
         strip_suffix(normalized_legal_name)               AS core,   -- trailing legal-form peel
         nullif(left(regexp_replace(zip_code,'[^0-9]','','g'),5),'') AS zip5
  FROM sos_normalized_master WHERE source_state='CA' AND normalized_legal_name IS NOT NULL
),
-- Ambiguity suppression: names mapping to ≥4 distinct entities are blocked from the EXACT tier
-- (homonym noise); they fall through to ZIP-gated resolution only.
amb AS (SELECT nln FROM spine GROUP BY 1 HAVING count(DISTINCT sos_entity_num) >= 4),
-- UCC org debtors normalized with the IDENTICAL macro.
ucc AS (
  SELECT ucc1_num, _name_norm(org_name) AS nln, strip_suffix(_name_norm(org_name)) AS core,
         nullif(left(regexp_replace(postal_code,'[^0-9]','','g'),5),'') AS zip5
  FROM ca_ucc_debtors WHERE debtor_type='Organization' AND org_name IS NOT NULL
)
SELECT u.ucc1_num, s.sos_entity_num,
       CASE WHEN u.nln = s.nln  AND a.nln IS NULL                 THEN 'exact'
            WHEN u.nln = s.nln                                    THEN 'exact_ambiguous'
            WHEN u.core = s.core AND u.zip5 IS NOT NULL
                                 AND u.zip5 = s.zip5              THEN 'core_zip'
       END AS match_method
FROM ucc u
JOIN spine s ON (u.nln = s.nln) OR (u.core = s.core AND u.zip5 = s.zip5)
LEFT JOIN amb a ON a.nln = s.nln
```

- **Tier order = confidence order:** `exact` (auto-accept, 93.4% unique) → `core_zip` (geo-gated
  recovery) → `exact_ambiguous` (needs address/agent tiebreak). Emit `match_method` so consumers
  can threshold.
- **Key schema:** carry `sos_entity_num` (→ join `ca_sos_entities` for `entity_type`,
  `entity_status`, addresses), `ucc1_num` (→ join filings/secured_parties), and the
  `normalized_legal_name` block key. BTREE all three on the output (the daily-rebuild +
  in-place `add_columns`/`create_scalar_index` integrity-gated pattern from
  `crosswalk_sam_usaspending.py::patch_normalized_name`).
- **`strip_suffix`** is a deterministic UDF/SQL macro, applied identically both sides — never a
  one-sided transform (that would re-introduce drift).

---

## 4. Lender & collateral landscape

### 4.1 Secured-party cleanliness & the classification that matters

**The biggest pitfall is conceptual, not textual:** the highest-frequency "secured parties" are
**government statutory-lien filers**, not commercial lenders. The `filings.filing_type` split:

| filing_type | Rows | Share | Nature |
|---|--:|--:|---|
| **UCC** | 5,542,772 | **71.5%** | consensual commercial security interest |
| Notice of State Tax Lien | 1,961,550 | 25.3% | involuntary (EDD/CDTFA/BOE/FTB) |
| Notice of Federal Tax Lien | 170,877 | 2.2% | involuntary (IRS) |
| Judgment Lien | 75,370 | 1.0% | involuntary (court) |
| Pension/Attachment/Estate | 1,321 | <0.1% | involuntary |

> **For "business pledged its assets as collateral for commercial debt," filter
> `filing_type = 'UCC'`.** Tax/judgment liens are involuntary and must be classified separately.

Top secured parties by normalized name (with raw-variant spread per normalized form):

| Normalized secured party | Appearances | Raw variants | Read |
|---|--:|--:|---|
| EMPLOYMENT DEVELOPMENT DEPARTMENT | 887,498 | 1 | CA tax lien (clean) |
| US SMALL BUSINESS ADMINISTRATION | 391,112 | **26** | SBA (EIDL/PPP) — heavy variant spread |
| CALIFORNIA DEPARTMENT OF TAX AND FEE ADMINISTRATION | 211,884 | 6 | CA tax lien |
| SNAPON CREDIT LLC | 132,550 | 2 | equipment (tool) finance |
| JPMORGAN CHASE BANK NA | 117,141 | **13** | bank |
| GOODLEAP LLC | 117,001 | 4 | solar finance |
| CORPORATION SERVICE COMPANY AS REPRESENTATIVE | 90,911 | 7 | **filing agent (masks true lender)** |
| C T CORPORATION SYSTEM AS REPRESENTATIVE | 70,929 | 5 | **filing agent** |
| SOLAR MOSAIC INC / SOLAR MOSAIC LLC | 52,543 / 44,335 | 2 / 2 | **same brand, Inc vs LLC split** |
| KUBOTA CREDIT CORPORATION USA | 38,375 | 3 | equipment finance |
| **WELLS FARGO BANK NA** / **WELLS FARGO BANK NATIONAL ASSOCIATION** | 23,987 / 13,050 | **19 / 9** | **same bank, two normalized buckets** |
| US BANK NA / US BANK NATIONAL ASSOCIATION | 25,430 / 14,391 | 24 / 11 | **same bank, two buckets** |
| DE LAGE LANDEN FINANCIAL SERVICES INC | 15,451 | 4 | equipment-lease factorer |

**Processing pitfalls (lender side):**

1. **`_name_norm` under-consolidates lenders.** `"WELLS FARGO BANK, N.A."` → `WELLS FARGO BANK
   NA` (punctuation stripped) but `"WELLS FARGO BANK NATIONAL ASSOCIATION"` stays distinct — the
   two abbreviation styles of "N.A." never reconcile under the macro. True Wells Fargo volume =
   37,037 across **28 raw variants in 2 normalized buckets**; US Bank similarly split. **A
   lender-alias canonicalization table** (regex-fold `NATIONAL ASSOCIATION`↔`NA`, `Inc`↔`LLC`
   brand unification, `… A DIVISION OF …` truncation) is required before any lender ranking is trustworthy.
2. **Filing-agent masking.** `… AS REPRESENTATIVE` / `CORPORATION SERVICE COMPANY` / `C T
   CORPORATION SYSTEM` / `FIRST CORPORATE SOLUTIONS` / `CHTD COMPANY` name a **representative**,
   not the lender (≈200k+ filings). The true secured party is not recoverable from
   `secured_party_name` alone — flag these and treat lender identity as unknown.
3. **DBA aliasing** (`PARAMOUNT EQUITY MORTGAGE LLC DBA LOANPAL` vs `LOANPAL LLC`) splits one
   originator across names.

### 4.2 Collateral / relationship types

`alt_designation_type` encodes the special UCC collateral relationship: `Lessee/Lessor` 222,202
(true equipment leases), `Seller/Buyer` 39,747, `Bailee/Bailor` 6,003, `Consignee/Consignor`
4,238, `Licensee/Licensor` 445; the rest `No Value`/`Not Applicable`. (The bulk feed carries no
free-text collateral description column — collateral *type* is inferred from
`alt_designation_type` + `filing_type`, not from a narrative field.)

---

## 5. Cardinality, churn & index performance

### 5.1 Filing grain — origination vs amendment

`action_type` cleanly separates the originating financing statement from its UCC-3 lifecycle
events. **`action_type = 'Lien Financing Stmt'` (4,437,111) ≈ distinct `ucc1_num` (4,437,136)** —
that is the origination grain:

| action_type | Rows |
|---|--:|
| **Lien Financing Stmt** (origination) | 4,437,111 |
| Continuation | 1,314,826 |
| Termination | 1,215,687 |
| Amendment | 235,380 |
| Erroneous Termination | 220,702 |
| Assignment | 120,306 |
| Add/Change/Delete Collateral·Debtor·Secured Party (tail) | ~200,000 |

**Active-lien logic.** `lapse_date` is 100% populated (sentinel `9999-12-31` = non-lapsing tax
liens). At the filing-event grain, **64.08% have a future lapse** (4,967,781 active vs 2,784,109
lapsed, ref `2026-06-01`; min lapse `2020-03-11`). A precise *active commercial lien* = a UCC-1
origination where `filing_type='UCC'`, the latest lifecycle event is not a `Termination`, and
`lapse_date > now` (UCC-1 lapses 5 yrs after filing unless `Continuation`-extended).

### 5.2 Cardinality grain — liens per entity & parties per lien

- **Secured parties per filing** (from `debtor_index.secured_party_count`): **1 SP in 95.5%**
  (5,594,744), 0 in 150,610, 2–3 in 104,679, 4–10 in 4,013, 10+ in 1,370. Single-lender is the norm.
- **UCC-1 financing statements per resolved CA entity** (Tier-A exact join; 925,503 entities,
  2,026,078 UCC-1s, **avg 2.189**):

  | Distinct UCC-1 per entity | Entities | Share |
  |---|--:|--:|
  | 1 | 497,687 | 53.8% |
  | 2–3 | 271,639 | 29.4% |
  | 4–10 | 133,521 | 14.4% |
  | 11–50 | 22,031 | 2.4% |
  | 50+ | 626 | 0.1% |

  ≈46% of resolved entities carry **2+ distinct financing statements** (cross-collateralization,
  serial equipment leases, refinancing); the 50+ tail (626 entities) is fleet/equipment-lessee
  heavy. A UCC→SoS bridge is therefore **many-to-one** (UCC side fans out); resolve at the
  `(sos_entity_num)` grain and aggregate UCC-1s as the dependent collection.

### 5.3 Index performance — `ScalarIndexQuery` confirmed, sub-second

Warm, symmetric single-column lookups (median of 5; `explain_plan` parsed):

| Dataset · indexed col | Indexed median | Unindexed full-scan | Speed-up | Plan node |
|---|--:|--:|--:|---|
| `sos_normalized_master` · `normalized_legal_name` | **142.3 ms** (108.8 min) | 4,496.2 ms (`source_entity_name`) | **~31×** | `ScalarIndexQuery …@normalized_legal_name_idx(BTree)` |
| `ca_ucc_debtors` · `org_name` | **321.8 ms** (7 hits) | 442.2 ms (`addr1`) | ~1.4× | `ScalarIndexQuery …@org_name_idx(BTree)` |
| `ca_ucc_secured_parties` · `org_name` | **372.8 ms** (10,585 hits) | 363.5 ms (`addr1`, 14,998 hits) | ~1× | `ScalarIndexQuery …@org_name_idx(BTree)` |

Every blocking-key lookup is **sub-second** and provably index-served (the physical plan emits a
`ScalarIndexQuery … (BTree)` node, not a full scan). The master name-index advantage is decisive
(~31× over an 18-fragment full scan); on the smaller, fewer-fragment UCC datasets the absolute
floor is already so low (≈300–440 ms cold-warm) that the relative gap narrows while staying
sub-second. **A name-block join across these spines meets the sub-second-per-lookup bar**; the
bridge build itself is a single bulk hash-join (one scan per side), not per-row lookups.

---

## 6. Bridge build recipe (operationalizing the vector)

Mirror the proven resolution conventions:

1. **Worker:** `pipelines/resolution/bridge_ca_ucc_sos.py`, Modal app
   `resolution-ca-ucc-sos-pipelines`, endpoint-less, dispatcher-spawned (ARCHITECTURE.md
   §"Maintenance workers" — cross-source resolution lives under `pipelines/resolution/`).
2. **Read:** `sos_normalized_master` (CA slice, BITMAP-pushed) + `ca_ucc_debtors`
   (`debtor_type='Organization'`). One scan each; bounded DuckDB (16–48 GB, disk spill).
3. **Transform:** §3.3 tiered join → output grain **1 row per (ucc1_num, sos_entity_num) resolved
   pair** with `match_method`, `normalized_legal_name`, and the joinable keys; aggregate or leave
   UCC-1 fan to the consumer.
4. **Write:** `lance.write_dataset(s3://data-sink/active/bridge_ca_ucc_sos/, v2.1, overwrite)` →
   BTREE `sos_entity_num`, `ucc1_num`, `normalized_legal_name`; `LANCE_BYPASS_SPILLING=true` for
   the index sort.
5. **State:** `ops.bridge_runs` row (match counts by method) + Trigger v4 waitpoint callback.
6. **Enrichment joins** (downstream, all BTREE point-lookups):
   `bridge → ca_sos_entities ON sos_entity_num` (entity_type, status, standing, addresses) and
   `bridge → ca_ucc_filings/secured_parties ON ucc1_num` (filter `filing_type='UCC'`, active lapse,
   lender after alias-canonicalization).

## 7. Pitfalls checklist

- **No SoS-ID join** — name+ZIP only (§3.1). Never expect a deterministic key.
- **`ucc1_num`/`ucc3_num`/`entity_num`/`postal_code` are VARCHAR with leading zeros / ZIP+4** —
  any numeric cast destroys the key. Already enforced in the source workers; preserve downstream.
- **Entity-ending drift** — always apply `strip_suffix` symmetrically; gate Tier-B/C on ZIP.
- **`entity_type` is master-absent** — join `ca_sos_entities` for corporate form.
- **Tax/judgment liens ≠ commercial debt** — filter `filing_type='UCC'` (28.5% otherwise leak in).
- **Lender names need an alias layer** — `NA`↔`NATIONAL ASSOCIATION`, brand `Inc`↔`LLC`, DBA folds,
  and `… AS REPRESENTATIVE` agent flags, before any lender ranking.
- **ZIP co-block ceilinged at 13.2% spine-null** — treat ZIP as a disambiguator, not a hard gate.
