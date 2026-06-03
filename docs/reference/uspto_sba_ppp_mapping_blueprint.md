# USPTO Trademarks × SBA/PPP — IP & Commercial Credit Mapping Blueprint

Read-only data-forensics audit over `s3://data-sink/active/`. Pairs the USPTO Trademark
master (IP layer) to the federal credit spines `ppp`, `sba_7a`, `sba_504` (credit layer)
to isolate active operating companies that hold registered IP. This document is the
structural specification for a future `pipelines/resolution/crosswalk_uspto_sba.py`
Pattern-B bridge.

**Method.** `lance 7.0.0` + `duckdb 1.5.3`, R2 credentials injected via
`doppler run -p core-x -c prd`. Every figure below is the output of an actual query against
the committed Lance datasets — column-projected scans, no sampling on the USPTO side
(66 k rows read in full), full streams of SBA (2.17 M) and PPP (11.47 M). Reads were local
over WAN; **latency figures are NOT representative of in-region Modal execution** and are
excluded here.

---

## 0. Provenance & two findings that bound everything below

| Dataset | URI (`s3://data-sink/active/…`) | Rows | Lance ver | Name index |
|---|---|--:|--:|---|
| `uspto_tm_applications` | `uspto_tm_applications/` | **66,331** | 18 | BTREE `mark_identification` (not owner) |
| `uspto_tm_assignments` | `uspto_tm_assignments/` | 1,557,545 | 12 | — |
| `uspto_tm_ttab` | `uspto_tm_ttab/` | 156,261 | 15 | — |
| `ppp` | `ppp/` | **11,468,210** | 34 | BTREE `loan_number`, `naics_code` (not borrower) |
| `sba_7a` | `sba_7a/` | **1,947,098** | 24 | **BTREE `borr_name`** (raw), `borr_state`, `naics_code` |
| `sba_504` | `sba_504/` | **227,404** | 27 | **BTREE `borr_name`** (raw), `borr_state`, `naics_code` |

**FINDING 1 — the USPTO spine is a partial, pending-heavy backfill, not the master.**
66,331 rows is ~1–2 % of the true USPTO trademark corpus (millions of live registrations,
tens of millions of historical records). The status distribution confirms the skew: a
single status code accounts for **72 %** of current-owner rows, and only **5.7 %** of marks
are registered-and-clean (registration date present, no cancellation/abandonment), vs
**86.8 %** carrying neither a registration nor a death date (i.e. pending applications).
**Every match rate in §2 is therefore a structural floor.** Re-running this audit after a
full Applications backfile is the single highest-leverage action; the join logic does not
change, the denominator does.

**FINDING 2 — no credit spine carries a normalized-name index.** All three credit
datasets index the **raw** borrower string (`borr_name` / `loan_number`), never a
normalized blocking key. Name normalization mutates the string (`"Smith & Co., LLC"` →
`"SMITH CO LLC"`), so the existing raw-name BTREE **cannot serve a normalized-owner
equality join.** The cross-layer match is consequently a **batch normalize + hash-join over
a full column scan**, not an indexed point-lookup. Sub-second exploratory querying is only
achievable *after* materializing a `normalized_legal_name` (+ `*_zip5`) column on each side
and building its own BTREE — i.e. exactly the `crosswalk_*` Pattern-B output, which is what
this blueprint specifies.

---

## 1. The Structural Mapping Vector

### 1.1 Load-bearing columns

**IP layer — `uspto_tm_applications`** (owner identity is *nested and multi-valued*):

| Concern | Column / path | Type |
|---|---|---|
| Entity key | `serial_number` (BTREE), `registration_number` (BTREE) | string |
| Brand asset | `mark_identification` (BTREE) | string |
| **Owner(s)** | `owners` → `LIST<STRUCT<entry_number, party_type, legal_entity_type_code, party_name, address_1, address_2, city, state, country, postcode, nationality<…>>>` | list&lt;struct&gt; |
| Counsel (decoy addr) | `correspondent` `STRUCT<address_1..5>` | struct |
| IP type | `classifications` → `LIST<STRUCT<international_code, us_code[], status_code, …>>` | list&lt;struct&gt; |
| Status / lifecycle | `status_code` (BITMAP), `filing_date`, `registration_date`, `abandonment_date`, `cancellation_date`, `renewal_date` | string / date32 |

**Credit layer** — flat, one row per loan:

| Concern | `ppp` | `sba_7a` / `sba_504` |
|---|---|---|
| Borrower name | `borrower_name` | `borr_name` |
| Operating address | `borrower_address/city/state/zip` | `borr_street/city/state/zip` |
| Industry | `naics_code` | `naics_code` (+ `naics_description`) |
| Execution date | `date_approved` | `approval_date` |
| Status | `loan_status` | `loan_status` |

### 1.2 Nested owner extraction (the "current owner" of a mark)

`owners` is an ownership-history list; `entry_number` orders successive owners (original
applicant → assignees). The *current* owner is the highest `entry_number`. Distribution
observed: 82.5 % of marks single-owner (54,683 / 66,321), the remainder multi-entry.

```sql
-- USPTO → one current-owner row per mark
WITH ex AS (
  SELECT serial_number, status_code, registration_date, cancellation_date, abandonment_date,
         o.party_name, o.legal_entity_type_code AS le, o.state AS o_state, o.postcode AS o_postcode,
         TRY_CAST(o.entry_number AS INTEGER) AS entry_no
  FROM uspto_tm_applications, UNNEST(owners) AS t(o)
)
SELECT *,
       <_name_norm(party_name)>  AS owner_norm,   -- canonical blocking key
       <_zip5(o_postcode)>       AS owner_zip5
FROM ex
QUALIFY row_number() OVER (PARTITION BY serial_number ORDER BY entry_no DESC NULLS LAST) = 1;
```

Current-owner null density: `party_name` 0.0 %, `owner_norm` 0.0 %, **`owner_state` 28.9 %**,
`owner_zip5` 2.4 %. Owner *state* is frequently absent — the geographic vector is weaker
than the name vector (see §2.4). `legal_entity_type_code` of the current owner:
16 = LLC (20,308), 01 = individual (17,563), 03 = corporation (14,130), 99 = other (10,556).
The large individual + "other" share is the seed of the false-positive problem in §2.

### 1.3 Canonical normalization (verbatim from `sos_normalized/normalize.py`)

```python
def _name_norm(c):  # UPPER → strip [^A-Z0-9 space] → collapse ws → trim → NULL if empty
    return ("nullif(trim(regexp_replace(regexp_replace(upper(CAST(%s AS VARCHAR)),"
            " '[^A-Z0-9 ]+', '', 'g'), '\\s+', ' ', 'g')), '')") % c
def _zip5(c):       # digits-only, left 5 (leading zeros survive)
    return "nullif(left(regexp_replace(CAST(%s AS VARCHAR), '[^0-9]', '', 'g'), 5), '')" % c
```

Using `_name_norm` on both `owner_norm` and `borr_name`/`borrower_name` makes the USPTO
owner byte-for-byte rule-compatible with the existing `sos_normalized_master` /
`crosswalk_*` blocking keys — the bridge slots into the established resolution graph with
no second normalization pass.

**Holding-company / suffix variant (`owner_core`).** To probe structural drift between an
IP-holding subsidiary ("X IP HOLDINGS LLC") and the operating borrower ("X MANUFACTURING
INC"), a second key strips up to three trailing legal/holding tokens
(`INC|LLC|CORP|HOLDINGS|IP|GROUP|TRUST|BRANDS|INTERNATIONAL|…`). This is a **recall lever
that trades precision** (it collapses "SMITH LLC" and "SMITH INC" to "SMITH") and must only
be used as a candidate generator behind a geographic/NAICS confirmer.

### 1.4 The join — no shared hard key

Unlike `crosswalk_hmda_gleif` (joins on `lei`) there is **no shared identifier** between
USPTO and SBA/PPP (no LEI, DUNS, EIN, or UEI on either side). The only vectors are the
normalized name (primary) and geography (corroborating). The join is therefore a fuzzy
resolution, and §2's false-positive analysis is the real deliverable.

```sql
-- Tier-1 candidate: exact normalized name
SELECT count(DISTINCT u.serial_number)
FROM uspto_owner u
JOIN (SELECT DISTINCT <_name_norm(borrower_name)> bnorm FROM ppp) p ON u.owner_norm = p.bnorm;
```

---

## 2. Empirical Match Diagnostics

### 2.1 Baseline match rates (denominator = 66,321 marks with a normalized owner)

| Credit spine | distinct norm names | **exact** match | **suffix-stripped** match |
|---|--:|--:|--:|
| `ppp` (11.47 M) | 8,123,415 | **8,886 — 13.40 %** | 12,349 — 18.62 % |
| `sba_7a` + `sba_504` (2.17 M) | 1,682,437 | **1,678 — 2.53 %** | 4,006 — 6.04 % |

PPP matches 5.3× more marks than SBA at the exact tier — its 8.1 M distinct borrower names
(4.8× SBA's) cover the sole-proprietor / micro-business long tail where most small-brand
trademark owners live. Suffix-stripping lifts SBA by **+139 %** and PPP by **+39 %** — the
larger SBA lift indicates SBA borrowers are recorded under fuller legal names (the operating
entity), widening the gap to the trademark holder's shorter trade name.

### 2.2 Entity-type decomposition of PPP matches — the false-positive driver

| USPTO `legal_entity_type_code` | matched marks | interpretation |
|---|--:|---|
| 03 corporation | 3,180 | high-trust (org name) |
| **01 individual** | **2,728** | **homonym risk — see §2.3** |
| 16 LLC | 2,566 | high-trust (org name) |

**30.7 %** of exact PPP matches are individual-owner marks. PPP extended to sole
proprietors, so an individual trademark owner "JAMES SMITH" matching a PPP borrower
"JAMES SMITH" is overwhelmingly a coincidental homonym, not the same economic entity.

### 2.3 False-positive collision profile (PPP)

6,618 matched normalized names map to **29,047** PPP loan rows (mean 4.4, **median 1.0**,
**max 441**). The distribution is bimodal:

- **3,332 names (50 %) hit exactly one PPP loan** — high-confidence, low-FP.
- **679 names appear in ≥ 5 states** — generic/personal, high-FP.

The highest-collision matched names are unambiguous noise — a single normalized string
spread across ~40 states is many distinct humans:

| PPP rows | states | normalized name |
|--:|--:|---|
| 441 | 38 | MICHAEL WILLIAMS |
| 391 | 39 | JAMES SMITH |
| 357 | 40 | JAMES WILLIAMS |
| 336 | 40 | MICHAEL JONES |
| 239 | 36 | ROBERT WILLIAMS |
| 217 | 33 | MICHAEL JACKSON |

> **Nuance — multiple loans ≠ false positive.** On SBA the top collisions are
> *single-state, company-suffixed* names (e.g. `FIREBLAST GLOBAL INC` — 12 loans, 1 state):
> one entity with several loans over time, not 12 false entities. Collision count
> over-states FP risk for org names and under-states it for personal names. The reliable
> FP discriminator is **state/zip dispersion**, not raw row count.

### 2.4 Geographic corroboration (where both sides carry the field)

| Credit spine | name-matched pairs | **state agreement** | **zip5 agreement** |
|---|--:|--:|--:|
| `sba_7a`+`sba_504` | 2,761 | **64.2 %** | 31.4 % |
| `ppp` | 36,962 | **23.8 %** | 12.7 % |

Two structural truths:

1. **PPP state agreement (23.8 %) collapses vs SBA (64.2 %)** — the personal-name homonym
   explosion (§2.3) inflates the PPP pair count with geographically-scattered noise.
   Geography is *mandatory* to clean PPP matches; it is *strongly corroborating* for SBA.
2. **Zip agreement is low on both (12–31 %)** even where states agree. This confirms the
   directive's Test-2 hypothesis empirically: the USPTO owner address is frequently a
   different point than the SBA operating/collateral address — registered-agent office,
   corporate HQ, owner's home, or (via `correspondent`) outside counsel. **Zip is a weak
   confirmer; state is the usable geographic blocking key.**

### 2.5 Recommended match tiers (precision-ordered)

| Tier | Rule | Use |
|---|---|---|
| **T1 — high** | `owner_norm = bnorm` **AND** `owner_zip5 = borr_zip5` | auto-accept |
| **T2 — medium** | `owner_norm = bnorm` **AND** `owner_state = borr_state` | accept for org owners (le ∈ {03,16,…}); review for individuals |
| **T3 — low** | `owner_norm = bnorm`, no geo agreement, **org owner only** | candidate; require NAICS↔class concordance (§3.3) |
| **reject** | individual-owner (le=01) name-only, **or** name in ≥ 5 states | drop — homonym |
| **recall ext.** | `owner_core = bcore` | candidate generator *only*, never auto-accept |

Name+state corroboration yields **5,511** distinct marks on PPP and **1,046** on SBA — the
defensible "active operating company holds registered IP" population at the current 66 k
USPTO floor.

---

## 3. IP-to-Credit Landscape Summary

### 3.1 IP health among matched entities

Name+state-corroborated marks split (date-based proxy): PPP **live ~608 / dead ~687** of
5,511; SBA **live ~100 / dead ~133** of 1,046. The remainder (~76 %) are pending — a direct
consequence of the pending-heavy 66 k spine (Finding 1), not of the matching. Live/dead
should be recomputed off the official USPTO `status_code` lookup once the full backfile
lands; the code semantics were **not** assumed here.

### 3.2 IP-type distribution (overall, exploded over `classifications.international_code`)

Top international classes in the spine: **041** education/entertainment (8,816), **009**
software/electronics (8,696), **042** SaaS/technology services (7,819), **035**
advertising/business (7,705), **025** apparel (6,975) — the canonical high-volume classes.
These segment cleanly and are programmatically usable for IP-type bucketing.

### 3.3 Tightest IP↔industry integration (class × NAICS, PPP name+state corroborated)

The cross-tab is semantically coherent — the trademark class maps to the borrower's NAICS
exactly as a real operating company would file, validating that the join surfaces genuine
entities rather than noise:

| IP class | NAICS | marks | segment |
|---|---|--:|---|
| 043 | 722511 | 86 | restaurant brands ↔ full-service restaurants |
| 009 | 541511 | 42 | software marks ↔ custom programming |
| 045 | 541110 | 39 | legal-services marks ↔ law offices |
| 042 | 541511 | 37 | tech-services marks ↔ programming |
| 028 | 423920 | 31 | toy/sporting marks ↔ toy wholesale |
| 032 | 312120 | 29 | beverage marks ↔ **breweries** |
| 044 | 621111 | 29 | medical marks ↔ physician offices |
| 033 | 312130 | 28 | wine/spirit marks ↔ **wineries** |
| 025 | 424330 | 22 | apparel marks ↔ apparel wholesale |

**Densest blocks for downstream targeting:** food & beverage (classes 043/032/033 ↔ NAICS
7225xx/3121xx) and software/IT (classes 009/042 ↔ NAICS 5415xx/5112xx). These are where IP
filings and commercial borrowing co-occur most tightly and where the bridge will be most
productive.

### 3.4 Cardinality & grain

- **Owners per mark:** 82.5 % single-owner; `current-owner = max(entry_number)` collapses the
  history to one resolution row (66,321 rows).
- **Loans per matched borrower:** median 1, mean 4.4, max 441 (PPP). The umbrella pattern
  the directive anticipated (one corporate mark + several product marks against one borrower)
  is real but **secondary** to the personal-name multiplicity that dominates the tail.
- **Bridge grain:** one row per `(serial_number, matched_loan_key, tier)` — a mark can match
  several loans of one borrower; dedup to `(serial_number, borrower_identity)` for an
  entity-level view.

---

## 4. Build implications for `crosswalk_uspto_sba` (Pattern-B bridge)

1. **Backfill the full USPTO Applications corpus first.** The 66 k spine is the binding
   constraint; the logic here is final, the coverage is not.
2. **Materialize `normalized_legal_name` + `*_zip5` on the credit spines** (or in the bridge
   build) and build their BTREEs — the existing raw-name BTREE does not serve this join
   (Finding 2). The bridge output then carries BTREE `owner_norm`/`serial_number` for the
   sub-second downstream lookups the directive requires.
3. **Geography is not optional for PPP.** Persist `match_tier` (§2.5) on every bridge row;
   never auto-accept individual-owner name-only matches.
4. **Carry `primary_class` + `naics_code`** on the bridge so the NAICS↔class concordance in
   §3.3 is available both as a confirmer and as a segmentation axis.
5. **Run in-region on Modal** to validate the sub-second-lookup claim; the latency seen in
   this local-WAN audit is not representative.
