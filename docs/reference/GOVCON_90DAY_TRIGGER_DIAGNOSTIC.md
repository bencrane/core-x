# GovCon 90-Day Trigger Volume & Schema Diagnostic

Directive 31 deliverable. **Read-only** historical analysis of three programmatic
direct-mail triggers against the live USAspending Lance stack in R2
(`s3://data-sink/active/usaspending/*`), executed 2026-06-03 with
`pylance 7.0.0 / duckdb 1.5.x` over the `core-x/prd` R2 credentials. Every count,
weekly cadence, value percentile, and keyword rate below is **measured from the
system of record** — no DDL, no writes, no dataset mutation.

---

## 0. Verdict summary (read this first)

| Question | Verdict |
|---|---|
| **Can we size the 90-day footprint per trigger?** | ✅ Yes, fully. Prime volumes + distinct contractors + weekly cadence + value distribution computed for all three triggers (§3). |
| **What's the mailable weekly cadence?** | T1 Miller ≈ **240 letters / ~210 distinct contractors per week**; T2 SCIF ≈ 190/wk (but over-broad — see below); T3 Steel ≈ **15/wk**. (§3.2) |
| **Sub-contractor volume?** | ⚠️ **Effectively unusable today.** `subaward_search` lags the prime feed badly — T1 = 8 sub UEIs, T2 = 3, T3 = 0 over 90 days. Subs are not a mailable channel from this dataset right now (§3.3). |
| **Do the description fields name the pain point?** | ❌ **Almost never.** Descriptions are 100% populated but terse (avg ~110 chars). `SCIF` appears in 0.10–0.15% of rows, `bond` 0.19%, `structural steel` <0.06%. Pre-vector keyword targeting on USAspending text is a dead end (§4). |
| **Is the SCIF trigger clean as defined?** | ❌ **No — it over-captures ~9×.** 2,208 of 2,623 SCIF hits are NAICS **236220 (all commercial building)** — the highest-value matches are *border-barrier* jobs, not SCIFs. Only 126 hit the security-specific PSCs. True SCIF intent is recoverable **only** from full-text/vectors (§4.3). |
| **Where do we get full text for embeddings?** | ✅ Path is concrete: SAM.gov Contract Opportunities (already in our sink) → `api.sam.gov` notice description + attachment PWS/SOW PDFs. Endpoints + build recipe in §5. |

---

## 1. Provenance & data currency (the window caveat)

- **Source:** USAspending monthly full-DB dump `usaspending-db_20260506.zip`,
  ingested by `pipelines/usaspending/usaspending_bulk.py` →
  `s3://data-sink/active/usaspending/<table>/` (Lance v2.x). `usaspending_snapshot_date = 2026-05-06`.
- **Data frontier (not "today"):** `award_search` `max(action_date) = 2026-04-23`;
  `subaward_search` `max(action_date) = 2026-04-16`.
- **Reporting-lag taper:** monthly `action_date` row counts run Feb 432K →
  **Mar 88K → Apr 44K**. The most recent ~6 weeks are still backfilling — the
  classic USAspending lag. The next monthly dump extends the frontier.
- **`sub_action_date` is corrupt** (min `0001-01-01`, max `6010-11-01`) — sub
  timing is keyed off the clean prime `action_date` instead.

**Consequence — two windows are reported, not one:**

| Window | Definition | Use |
|---|---|---|
| **Literal cal-90d** | `action_date ∈ [2026-03-05, 2026-06-03]` (today − 90d) | The directive-literal number. A **floor** — May/Jun unreported, Mar/Apr partial. |
| **Trailing-90d-of-data** | `action_date ∈ [frontier − 90d, frontier]` (≈ Jan 23 – Apr 23) | The **cadence-planning** number — a complete-ish 90-day quarter. |

Lead with trailing-90d for "letters per week"; cite cal-90d as the lag-floor.

---

## 2. Datasets, grain, indices, and exact trigger definitions

| Dataset | Rows | Grain | BTREE indices (live) |
|---|---:|---|---|
| `usaspending/award_search` | 78,373,286 | 1/prime award | `recipient_uei`, `parent_uei`, `naics_code` |
| `usaspending/transaction_search_fpds` | 107,250,527 | 1/contract txn | `recipient_uei`, `parent_uei`, `naics_code` |
| `usaspending/subaward_search` | 9,801,723 | 1/subaward | `awardee_or_recipient_uei`, `ultimate_parent_uei`, `sub_awardee_or_recipient_uei`, `sub_ultimate_parent_uei`, `naics`, `sub_naics` |

**Value field (primes):** `val = GREATEST(COALESCE(award_amount,0),
COALESCE(base_and_all_options_value,0), COALESCE(total_obligation,0))` — the
largest available "total contract value" signal, so an award is never missed for
a single null. Sub value = `subaward_amount`.

| Trigger | Source | Predicate (exact) |
|---|---|---|
| **T1 Miller Act Bond** | `award_search` | `naics_code ∈ ['23','24')` **AND** `is_fpds = true` **AND** `val ≥ 150,000` |
| **T2 SCIF / Secure Facility** | `award_search` | `naics_code IN ('238210','236220')` **OR** `product_or_service_code IN ('N063','C1AZ')` |
| **T3 Domestic Steel Mandate** | `award_search` | `naics_code IN ('237310','237110')` **AND** `val > 1,000,000` |

All three filtered to the `action_date` window. Sub counterparts run the same
NAICS/PSC logic on `subaward_search` (NAICS = `naics` OR `sub_naics`), keyed on
distinct `sub_awardee_or_recipient_uei`. `is_fpds = true` isolates contracts for
the Miller Act (statutory bond threshold = $150K); T2/T3 carry no `is_fpds`
filter because they say "any award." NAICS sector 23 is matched as the lexical
range `['23','24')` (BTREE-friendly; covers all 236xxx/237xxx/238xxx codes).

---

## 3. Deliverable 1 — the 90-day footprint

### 3.1 Headline volumes

`letters` = matched awards; `contractors` = distinct non-null UEI.

| Trigger / side | cal-90d letters | cal-90d contractors | data-90d letters | data-90d contractors | distinct names | median val | Σ val |
|---|---:|---:|---:|---:|---:|---:|---:|
| **T1 Miller — prime** | 2,044 | **1,102** | 3,140 | **1,526** | 1,503 | $2,861,371 | $174.9B |
| T1 Miller — sub | 4 | 4 | 9 | 8 | 8 | $355,000 | $4.2M |
| **T2 SCIF — prime** | 1,687 | **870** | 2,623 | **1,177** | 1,163 | $2,288,888 | $139.5B |
| T2 SCIF — sub | 1 | 1 | 3 | 3 | 3 | $209,800 | $1.0M |
| **T3 Steel — prime** | 112 | **93** | 195 | **151** | 151 | $5,883,242 | $5.2B |
| T3 Steel — sub | 0 | 0 | 0 | 0 | 0 | — | — |

### 3.2 Weekly cadence — prime side (the mailing rhythm)

`letters` = awards that week; `uei` = distinct contractors that week. Weeks tagged
`*` fall inside the literal cal-90d window.

**T1 Miller Act (construction prime contracts ≥ $150K)**

| week_start | letters | distinct_uei |  | week_start | letters | distinct_uei |
|---|---:|---:|---|---|---:|---:|
| 2026-01-19 | 63 | 61 |  | 2026-03-09 \* | 251 | 221 |
| 2026-01-26 | 184 | 166 |  | 2026-03-16 \* | 260 | 223 |
| 2026-02-02 | 173 | 149 |  | 2026-03-23 \* | 293 | 245 |
| 2026-02-09 | 193 | 174 |  | 2026-03-30 \* | 320 | 261 |
| 2026-02-16 | 143 | 129 |  | 2026-04-06 \* | 376 | 280 |
| 2026-02-23 | 216 | 197 |  | 2026-04-13 \* | 247 | 221 |
| 2026-03-02 | 186 | 171 |  | 2026-04-20 \* | 235 | 193 |

→ **~224 letters/wk avg (range 143–376); ~195 distinct contractors/wk.**

**T2 SCIF (NAICS 238210/236220 or PSC N063/C1AZ)** — *over-broad, see §4.3*

| week_start | letters | distinct_uei |  | week_start | letters | distinct_uei |
|---|---:|---:|---|---|---:|---:|
| 2026-01-19 | 40 | 38 |  | 2026-03-09 \* | 198 | 176 |
| 2026-01-26 | 170 | 133 |  | 2026-03-16 \* | 201 | 166 |
| 2026-02-02 | 147 | 117 |  | 2026-03-23 \* | 245 | 200 |
| 2026-02-09 | 158 | 140 |  | 2026-03-30 \* | 275 | 215 |
| 2026-02-16 | 120 | 105 |  | 2026-04-06 \* | 326 | 237 |
| 2026-02-23 | 217 | 191 |  | 2026-04-13 \* | 205 | 185 |
| 2026-03-02 | 134 | 124 |  | 2026-04-20 \* | 187 | 150 |

**T3 Domestic Steel (NAICS 237310/237110, > $1M)**

| week_start | letters | distinct_uei |  | week_start | letters | distinct_uei |
|---|---:|---:|---|---|---:|---:|
| 2026-01-19 | 6 | 6 |  | 2026-03-09 \* | 16 | 15 |
| 2026-01-26 | 12 | 12 |  | 2026-03-16 \* | 17 | 16 |
| 2026-02-02 | 12 | 12 |  | 2026-03-23 \* | 18 | 18 |
| 2026-02-09 | 16 | 16 |  | 2026-03-30 \* | 18 | 17 |
| 2026-02-16 | 13 | 13 |  | 2026-04-06 \* | 11 | 11 |
| 2026-02-23 | 17 | 17 |  | 2026-04-13 \* | 7 | 7 |
| 2026-03-02 | 14 | 14 |  | 2026-04-20 \* | 18 | 16 |

→ **~15 letters/wk, steady.** This is the cleanest, most targetable trigger.

### 3.3 Sub-contractor side — not a channel today

The `subaward_search` feed lags the prime feed so far that 90-day sub volume is
single digits (T1: 8 UEIs over 3 reported weeks; T2: 3; T3: 0). FFATA sub-award
reporting is both delayed and incomplete. **Do not plan a sub-contractor mail
stream off this dataset** until the feed is current; revisit after the next dump.

### 3.4 Value distribution (data-90d, `val`)

| Trigger | p25 | p50 | p75 | p95 | max |
|---|---:|---:|---:|---:|---:|
| T1 Miller | $644K | $2.86M | $20.2M | $250.0M | $18.9B |
| T2 SCIF | $326K | $2.29M | $25.0M | $250.0M | $1.86B |
| T3 Steel | $2.59M | $5.88M | $16.6M | $132.6M | $600.0M |

(The $18.9B T1 max is a Bechtel National IDIQ ceiling — IDV ceilings inflate the
tail; median is the honest "typical contract" figure.)

---

## 4. Deliverable 2 — text-specification field audit

### 4.1 Where the scope text lives

| Dataset | Scope-text column(s) | Companion |
|---|---|---|
| `award_search` | **`description`** | `naics_description`, `product_or_service_description` |
| `transaction_search_fpds` | **`transaction_description`**, `research_description`, `place_of_performance_scope` | `solicitation_identifier` |
| `subaward_search` | **`subaward_description`**, `award_description` | `keyword_ts_vector`, `award_ts_vector` (Postgres FTS vectors — not embeddings) |

### 4.2 Completeness + high-intent keyword hit-rate (non-regex `ILIKE` substring)

Measured **within each trigger's matched rows**.

| Population | field | nonempty | avg_len | `SCIF` | `classified facility` | `secure room` | `bond` | `structural steel` |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| T1 Miller prime (3,140) | `description` | 100.0% | 109 | 0.10% | 0.00% | 0.00% | 0.19% | 0.06% |
| T2 SCIF prime (2,623) | `description` | 100.0% | 111 | 0.15% | 0.00% | 0.00% | 0.19% | 0.04% |
| T3 Steel prime (195) | `description` | 100.0% | 129 | 0.00% | 0.00% | 0.00% | 1.03% | 0.00% |

**Finding:** the field is present and clean but **short and templated** (avg
~110 chars, e.g. `TAS::75 0140::TAS CONSTRUCTION OF VACCINE MANUFACTURING
FACILITY`). The pain-point terms are essentially absent — `classified facility`
and `secure room` never appear; `SCIF` ≤ 0.15%. **Keyword/boolean targeting on
USAspending description text cannot find these opportunities.** This is the
quantitative case for the vector pipeline.

### 4.3 The SCIF over-capture (the critical caveat)

Decomposition of the 2,623 T2 prime hits by match reason:

| Match reason | rows |
|---|---:|
| NAICS leg (238210 / 236220) | 2,512 |
| └ NAICS 236220 (all commercial building) | **2,208** |
| └ NAICS 238210 (electrical) | 304 |
| PSC leg (N063 / C1AZ) | 126 |
| └ PSC N063 (physical security equip install) | 70 |
| └ PSC C1AZ (A/E security facilities) | 56 |
| (both) | 15 |

NAICS **236220 is "Commercial & Institutional Building Construction"** — every
office/warehouse/barrier job, not SCIFs. The four highest-value "SCIF" matches
are all Fisher Sand & Gravel **border-barrier** awards. The security-specific
signal (PSC N063/C1AZ = **126 rows / ~5%**) is the only structurally clean part;
everything else needs the text layer to confirm "secure facility" intent.
**Recommendation:** treat T2's NAICS leg as a *candidate pool* to be filtered by
§5's vectors, not as a mailing list.

### 4.4 Sample matched scopes (evidence of depth & noise)

```
T1  [234930/Y181] $18.9B  BECHTEL NATIONAL, INC.   desc: <null>        (IDIQ ceiling, no scope)
    [236210/AR15] $1.58B  BECHTEL NATIONAL, INC.   desc: "DESIGN AND CONSTRUCTION OF THE NASA SLS MOBILE LAUNCHER 2"
T2  [236220/Y1PZ] $1.86B  FISHER SAND & GRAVEL     desc: "YUM-2 VERTICAL BORDER AND WATERBORNE BARRIER CONSTRUCTION"   ← NOT a SCIF
    [236220/Y1FF] $928M   CLARK CONSTRUCTION       desc: "DESIGN BUILD FCI LEAVENWORTH"
T3  [237310/Y1LB] $450M   KIEWIT INFRASTRUCTURE    desc: "CONSTRUCTION OF PHYSICAL SECURITY/SAFETY IMPROVEMENTS ... LONG SPAN BRIDGES"
    [237110/Y1NE] $358M   CDM CONSTRUCTORS         desc: "POJOAQUE BASIN REGIONAL WATER SYSTEM IGF::OT::IGF"
```

---

## 5. Deliverable 3 — gap analysis for the vector pipeline

The USAspending description is too thin to embed meaningfully. The full
performance work statements / scopes live **upstream in the solicitation**, not
in the award record. Sources, in priority order:

### 5.1 Source 1 — SAM.gov Contract Opportunities (already in our sink)

`s3://data-sink/sam-gov-opps/active/` (`pipelines/sam_gov/sam_opps_bulk.py`,
daily). Carries the join keys and the pointer to full text:

| Column | Use |
|---|---|
| `notice_id`, `solicitation_number` | Join key to the live notice + to FPDS `solicitation_identifier`. |
| `naics_code`, `classification_code` (= PSC) | **Direct bridge to all three triggers.** |
| `description` | ⚠️ SAM's `ContractOpportunitiesFullCSV` `Description` is typically a **URL to the notice-description endpoint**, not inline text — it must be fetched (§5.2). |
| `link` | The SAM.gov notice UI — entry point to attachments. |

### 5.2 Source 2 — `api.sam.gov` Opportunities API (full text + attachments)

The PWS/SOW PDFs — where "SCIF", "ICD 705", "secure room", "structural steel"
actually appear — are reached here (requires a `SAM_API_KEY`):

- **Notice description (HTML body):**
  `GET https://api.sam.gov/opportunities/v2/search?...` then the per-notice
  `description` link (a.k.a. `GET .../opportunities/v1/noticedesc?noticeid={id}`).
- **Attachment manifest:** the notice payload's `resourceLinks[]`.
- **Attachment download (the actual PWS/SOW):**
  `GET https://api.sam.gov/opportunities/v3/.../resources/download/{resourceId}?api_key=…`
  → PDF/DOCX → land to `s3://data-sink/landing/sam_attachments/<notice_id>/`.

### 5.3 Source 3 — USAspending richer fields (post-award, already in R2)

For T1/T3 (post-award targeting), `transaction_search_fpds.transaction_description`
+ `research_description` + `place_of_performance_scope` are richer than
`award_search.description` and already committed — no new fetch. Join award →
transaction on `recipient_uei` + `piid`.

### 5.4 Source 4 — FPDS Atom / USAspending REST (fallback)

- FPDS Atom: `https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&q=…`
  (`descriptionOfContractRequirement`).
- Award detail: `GET https://api.usaspending.gov/api/v2/awards/{generated_unique_award_id}/`.

### 5.5 Recommended build (core-x conventions)

A Pattern-A derived dataset, DuckDB transform → Lance v2.1 → R2, per
`docs/reference/02_lancedb_storage.md`:

1. **Candidate bridge** — join trigger NAICS/PSC against `sam-gov-opps` on
   `naics_code` / `classification_code` → `(notice_id, link, naics, psc)`.
2. **Hydrate** — fetch description + `resourceLinks` from `api.sam.gov`; download
   attachments to `s3://data-sink/landing/sam_attachments/…`.
3. **Extract & chunk** — PDF/DOCX → text → chunks.
4. **Embed & commit** — write `s3://data-sink/active/govcon_scope_vectors/`:
   `notice_id` / `award_id` (BTREE), `naics` (BTREE), `psc` (BTREE), `chunk_id`,
   `text`, `embedding` (vector).
5. **Index** — `LanceDataset.create_index(column="embedding",
   index_type="IVF_HNSW_SQ")` (the **vector** path — `create_index`, *not*
   `create_scalar_index`; see `02_lancedb_storage.md` §6.2). BTREE on the scalar
   keys enables hybrid *filter-then-ANN* (e.g. NAICS 236220 **AND** vector-near
   "SCIF / sensitive compartmented information facility").

Stopgap: `subaward_search` already ships Postgres `*_ts_vector` columns — usable
for lexical FTS today, but they are **not semantic embeddings**.

---

## 6. Appendix — method & the performance note

### 6.1 Why the first scan ran in minutes (and the fix)

The BTREE on `naics_code` is real and fast. Measured head-to-head on
`award_search` (78.4M rows):

| Path | query | rows | time |
|---|---|---:|---:|
| **Lance scanner-filter** | `count_rows(filter="naics_code='238210'")` | 17,191 | **0.6s** |
| Lance scanner-filter | `count_rows(filter="naics_code ∈ ['23','24')")` | 193,632 | **1.1s** |
| DuckDB registered + SQL | `SELECT count(*) … WHERE naics_code='238210'` | 17,191 | 2.8s |
| DuckDB registered + SQL | `SELECT count(*) … WHERE naics_code ∈ ['23','24')` | 193,632 | 3.0s |
| DuckDB registered + SQL | **T1 full materialization** | 3,140 | **~206s** |
| **Lance scanner pushdown** | **T1 full materialization** | 3,140 | **~34s** |

**Root cause:** the trigger script registered the `LanceDataset` into DuckDB and
filtered with SQL `WHERE`. Per `docs/reference/GOVCON_PROFILE_MATERIALIZATION_SCHEMA.md`
§2.3, the scalar index is exercised **only** when the predicate is pushed into
`lance.dataset(...).scanner(filter=…)`; a DuckDB scan over the registered relation
does **not** engage it — so it read near-full columns over a cross-country
laptop→R2 WAN. Routing the predicate through the scanner is **~6× faster**
(206s → 34s). Three compounding factors, in order: (a) the registered-relation
index bypass [fixable — use the scanner]; (b) the `GREATEST(3 doubles)` value
expression reads three full `double` columns for the unfiltered population
[fixable — filter first]; (c) the PSC leg (`product_or_service_code`) is genuinely
**un-indexed** and must scan regardless [inherent]. In-region (a Modal worker
peered to R2) all of this collapses ~10×. **Production form:** run as a
scanner-pushdown query, or inside a Modal worker — not as ad-hoc registered-relation
SQL from a laptop.

### 6.2 Reproducibility

Read-only throughout. Queries: `pylance 7.0.0 / duckdb 1.5.x` against
`s3://data-sink/active/usaspending/{award_search,subaward_search}`, R2 creds from
`core-x/prd` Doppler (`R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` / `R2_ENDPOINT`).
Windows: cal-90d `[2026-03-05, 2026-06-03]`; data-90d `[2026-01-23, 2026-04-23]`
(prime), `[2026-01-16, 2026-04-16]` (sub). Snapshot `usaspending-db_20260506`.

---

## 7. Recommended next actions

1. **Ship T1 + T3 now.** Both are structurally clean: NAICS + value floor on the
   indexed prime feed. ~224 (Miller) + ~15 (Steel) letters/week, deduped to
   distinct UEI, joined to `entity_registrations` / `recipient_lookup` for the
   mailing address (per the Directive-6 firmographic profile).
2. **Hold T2 SCIF behind the vector layer.** Mail only the PSC-N063/C1AZ core
   (~5%) until §5 lands; the NAICS-236220 bulk is border-barrier/general-building
   noise.
3. **Build `govcon_scope_vectors` (§5.5).** It is the only way to convert "secure
   facility intent" from a 0.15%-keyword problem into a retrievable segment.
4. **Don't plan a sub-contractor stream** off `subaward_search` until the FFATA
   feed is current (§3.3).
5. **Re-run on each monthly USAspending dump** to roll the 90-day window forward
   (the literal-90d counts here are a lag floor).
