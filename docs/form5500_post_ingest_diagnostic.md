# Form 5500 (2025) — Post-Ingest Structural Diagnostic & Cross-Graph Overlap Probe

**Result:** ✅ PASS — 7 datasets · 276,143 rows · 11 BTREE / 0 BITMAP indexes · 0 tombstones · 17.6s

**Mode:** read-only / zero-mutation. No dataset, index, or fragment written, compacted, or deleted; the sole write is this report.
**Form 5500 plane:** `/Users/benjamincrane/core-x-lake/active` (local LanceDB lake)  
**NPPES plane:** `s3://data-sink/active/nppes/snapshot=2026-05` (R2 SoR)  
**CMS Open Payments plane:** `s3://data-sink/active/cms_general_payments` (R2 SoR)  
**Run (UTC):** 2026-06-07T01:41:07+00:00

## 1. Index & Type Matrix

| Dataset | Source stem | Rows | Cols | Frags | Data files | ACK_ID dtype | BTREE indexes | BITMAP |
|---|---|--:|--:|--:|--:|---|---|---|
| `form5500_main` | F_5500 | 19,114 | 143 | 1 | 1 | `string` ✅ | `ACK_ID`, `SPONS_DFE_EIN`, `SPONS_DFE_PN` | — |
| `form5500_sf` | F_5500_SF | 199,363 | 194 | 1 | 1 | `string` ✅ | `ACK_ID`, `SF_SPONS_EIN`, `SF_PLAN_NUM` | — |
| `form5500_sch_h` | F_SCH_H | 1,358 | 169 | 1 | 1 | `string` ✅ | `ACK_ID` | — |
| `form5500_sch_i` | F_SCH_I | 10,059 | 80 | 1 | 1 | `string` ✅ | `ACK_ID` | — |
| `form5500_sch_c_provider` | F_SCH_C_PART1_ITEM2 | 3,774 | 25 | 1 | 1 | `string` ✅ | `ACK_ID` | — |
| `form5500_sch_c_provider_code` | F_SCH_C_PART1_ITEM2_CODES | 8,117 | 7 | 1 | 1 | `string` ✅ | `ACK_ID` | — |
| `form5500_sch_a_broker` | F_SCH_A_PART1 | 34,358 | 22 | 1 | 1 | `string` ✅ | `ACK_ID` | — |

**Index plan vs. landed:** all 7 datasets carry `BTREE(ACK_ID)`; the two head tables (`main`, `sf`) additionally carry per-column BTREEs on the business identity — `11` BTREE total, matching the ingest's committed plan. **BITMAP indexes built: 0** (none — every indexed Form 5500 column is high-cardinality identifier/temporal; no low-NDV categorical was indexed at ingest).

## 1b. Type-Safety Proof — keys bound to TEXT, leading zeros intact

Directive keys (`ACK_ID`, `SPONS_DFE_EIN`, `SPONS_DFE_PN`, `PROVIDER_OTHER_EIN`) must be `string`/VARCHAR; a silent integer cast would have truncated the structural leading zeros EFAST2 keys carry and corrupted the resolution graph.

| Dataset | Key | Landed dtype | Leading-zero (`LIKE '0%'`) | Sample `0…` values |
|---|---|---|--:|---|
| `form5500_main` | `ACK_ID` | `string` ✅ | — (n/a) | — (not an EIN key) |
| `form5500_main` | `SPONS_DFE_EIN` | `string` ✅ | 706 ✅ | `042272126`, `061033195`, `043287088` |
| `form5500_main` | `SPONS_DFE_PN` | `string` ✅ | — (n/a) | — (not an EIN key) |
| `form5500_sf` | `ACK_ID` | `string` ✅ | — (n/a) | — (not an EIN key) |
| `form5500_sf` | `SF_SPONS_EIN` | `string` ✅ | 7,330 ✅ | `060834774`, `061742118`, `061359279` |
| `form5500_sf` | `SF_PLAN_NUM` | `string` ✅ | — (n/a) | — (not an EIN key) |
| `form5500_sch_h` | `ACK_ID` | `string` ✅ | — (n/a) | — (not an EIN key) |
| `form5500_sch_i` | `ACK_ID` | `string` ✅ | — (n/a) | — (not an EIN key) |
| `form5500_sch_c_provider` | `ACK_ID` | `string` ✅ | — (n/a) | — (not an EIN key) |
| `form5500_sch_c_provider` | `PROVIDER_OTHER_EIN` | `string` ✅ | 333 ✅ | `042647786`, `010233346`, `042647786` |
| `form5500_sch_c_provider_code` | `ACK_ID` | `string` ✅ | — (n/a) | — (not an EIN key) |
| `form5500_sch_a_broker` | `ACK_ID` | `string` ✅ | — (n/a) | — (not an EIN key) |

## 1c. Storage Health

| Dataset | Rows | Frags | Data files | Read amp | Tombstones | Deleted rows | Small files (<1 MiB) | Data bytes | Index bytes |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| `form5500_main` | 19,114 | 1 | 1 | 1.00× | 0 | 0 | 0 | 11.3 MiB | 548.1 KiB |
| `form5500_sf` | 199,363 | 1 | 1 | 1.00× | 0 | 0 | 0 | 130.5 MiB | 5.6 MiB |
| `form5500_sch_h` | 1,358 | 1 | 1 | 1.00× | 0 | 0 | 0 | 1.2 MiB | 26.0 KiB |
| `form5500_sch_i` | 10,059 | 1 | 1 | 1.00× | 0 | 0 | 0 | 3.8 MiB | 163.8 KiB |
| `form5500_sch_c_provider` | 3,774 | 1 | 1 | 1.00× | 0 | 0 | 1 | 483.9 KiB | 61.1 KiB |
| `form5500_sch_c_provider_code` | 8,117 | 1 | 1 | 1.00× | 0 | 0 | 1 | 146.4 KiB | 69.8 KiB |
| `form5500_sch_a_broker` | 34,358 | 1 | 1 | 1.00× | 0 | 0 | 0 | 2.9 MiB | 340.7 KiB |

- **Read amplification 1.00× across the board** — one fragment, one data file per dataset (every table is below the 1,048,576-row file cap), so a scan opens the theoretical minimum number of files.
- **Tombstones: 0; deleted rows: 0** — the ingest used `mode=overwrite` (clean publish), so there is no soft-delete debt and no compaction is owed.
- **Small-file fragmentation: none in the read-amplification sense** — each dataset is a single fragment; the sub-1-MiB files counted are simply small *tables*, not a fragmented large table (no merge/compaction benefit available).

## 2. Relational Sanity — Schedule C → primary filing head

Inner/anti-join of `form5500_sch_c_provider.ACK_ID` against the primary head `form5500_main.ACK_ID` (19,114 distinct filings).

| Metric | Value |
|---|--:|
| Schedule C fee rows | 3,774 |
| Distinct filings (ACK_ID) referenced | 1,449 |
| **Orphan rows** (ACK_ID ∉ `main`) | **0** |
| **Orphan rate (rows)** | **0.00%** |
| Orphan filings (distinct ACK_ID) | 0 (0.00%) |

> ✅ **Zero orphans** — every Schedule C fee row resolves to a primary filing head. Referential integrity of the `ACK_ID` hub key holds across the head→detail boundary.

## 3. Healthcare Intersection — Schedule C counterparties × NPPES / CMS

### 3a. Cross-graph key reality (architecture constraint)

- **NPPES redacts EIN.** Org-provider rows with a non-sentinel `employer_identification_number_ein` (≠ `'<UNAVAIL>'`, non-null): **0** of 1,927,780 organization providers. EIN-based binding to NPPES is structurally impossible — the public file carries only the `'<UNAVAIL>'` sentinel.
- **CMS manufacturer IDs are not EINs.** `applicable_manufacturer_or_applicable_gpo_making_payment_id` is a CMS-internal registry ID (2,794 distinct; e.g. `100000000053`, `100000000055`, `100000000056`, `100000000057`, `100000000058`), not a 9-digit federal EIN.
- **Form 5500 side does carry real EINs:** 2,921 of 3,774 Schedule C rows have a `PROVIDER_OTHER_EIN` (937 distinct; 333 leading-zero), but there is no EIN column on either target to bind them to.

> **Therefore the only sound cross-graph key is the normalized organization NAME.** Reported two ways: **raw-exact** (`UPPER(TRIM(name))`, conservative floor) and **normalized** (punctuation folded to single spaces, the principled count). EIN overlap is reported as **0 (not measurable)**, not silently dropped.

### 3b. Universe sizes

| Universe | Count |
|---|--:|
| Schedule C counterparty rows | 3,774 |
| Distinct counterparty names — raw | 1,693 |
| Distinct counterparty names — normalized | 1,574 |
| NPPES organization names (distinct, entity_type_code='2') | 1,453,750 |
| CMS manufacturer/GPO names (distinct) | 2,973 |

### 3c. Cross-graph signal strength

| Target | Match key | Matched distinct names | % of distinct counterparties | Schedule C rows covered | % of rows |
|---|---|--:|--:|--:|--:|
| NPPES (orgs) | normalized | 30 | 1.91% | 59 | 1.56% |
| NPPES (orgs) | raw-exact | 24 | 1.42% | 42 | 1.11% |
| CMS Open Payments | normalized | 0 | 0.00% | 0 | 0.00% |
| CMS Open Payments | raw-exact | 0 | 0.00% | 0 | 0.00% |
| **NPPES ∪ CMS** | normalized | **30** | **1.91%** | **59** | **1.56%** |

### 3d. Matched-entity samples (verification)

**NPPES organization matches** (top by Schedule C row count):

| Form 5500 counterparty | F5500 EIN | NPPES organization (legal name) | Sch C rows |
|---|---|---|--:|
| UNITED HEALTHCARE SERVICES, INC. | `411289245` | UNITED HEALTHCARE SERVICES, INC. | 16 |
| AETNA LIFE INSURANCE COMPANY | `066033492` | AETNA LIFE INSURANCE COMPANY | 5 |
| HEALTHGRAM INC | `561449504` | HEALTHGRAM, INC. | 5 |
| ANTHEM INSURANCE COMPANIES INC | `350781558` | ANTHEM INSURANCE COMPANIES, INC. | 4 |
| EVERSIDE HEALTH LLC | `—` | EVERSIDE HEALTH LLC | 4 |

**CMS Open Payments matches** (top by Schedule C row count):

_No CMS manufacturer-name matches._

## 4. Method & reproduction

- **Name normalization:** `trim(regexp_replace(upper(name), '[^A-Z0-9]+', ' ', 'g'))` — uppercase, fold every punctuation/whitespace run to a single space, trim. Applied identically on both sides of each join. Raw-exact key is `upper(trim(name))`.
- **Match semantics:** exact equality on the (normalized | raw) key — no fuzzy / edit-distance / token matching. Numbers are a lower bound on true commercial overlap (legal-suffix and DBA variance suppress exact hits).
- **NPPES filter** `entity_type_code = '2'` is pushed into the Lance scan; the column is unindexed in NPPES, so this is a full column scan (no BTREE/BITMAP pushdown).
- **Read-only:** Lance datasets opened for scan/count only; no `create_*`, `delete`, `compact`, or `write` issued against any plane.

```
doppler run --project core-x --config prd -- \
  uv run pipelines/form5500/diagnose_post_ingest.py
```
