# CMS Open Payments ⋈ NPPES Hierarchy — Structural Diagnostic & Relational Mapping

**Targets (Gen-3 SoR, R2 bucket `data-sink`):**

| Entity | R2 URI | Grain | Role |
|---|---|---|---|
| `cms_general_payments` | `active/cms_general_payments/` | 1 / payment record | CMS ledger (fact) |
| `cms_research_payments` | `active/cms_research_payments/` | 1 / payment record | CMS ledger (fact) |
| `cms_ownership` | `active/cms_ownership/` | 1 / interest record | CMS ledger (fact) |
| `ppesRaw` | `active/nppes/snapshot=2026-05/` | 1 / NPI | Immutable provider archive |
| `nppes_provider` | `active/nppes_provider/snapshot=2026-05/` | 1 / NPI | Derived provider core (resolution target) |
| `nppes_provider_taxonomy` | `active/nppes_provider_taxonomy/snapshot=2026-05/` | 1 / (NPI, taxonomy slot) | Specialty long table |
| `nppes_provider_identifier` | `active/nppes_provider_identifier/snapshot=2026-05/` | 1 / (NPI, identifier slot) | External-ID linkage |
| `nppes_taxonomy_ref` | `active/nppes_taxonomy_ref/` | 1 / NUCC code | Specialty dimension (NUCC v25.1) |

**Mode:** Read-only, first-principles. Zero DDL / zero Lance writes / zero index ops / zero data modification. Assessed on physical + mathematical + relational structure alone, independent of every downstream consumer.
**Date:** 2026-06-06 · **Vintage:** CMS program years 2018–2024 (publication `2026-01-23`); NPPES snapshot `2026-05`.
**Method:** `pylance 7.x` manifest/fragment/index introspection (`count_rows`, `get_fragments`→`physical_rows`/`deletion_file`, `list_indices`, `stats.index_stats`, `stats.dataset_stats`) + R2 `ListObjectsV2` byte census (boto3) + `duckdb 1.5.x` exact joins/aggregates over the live R2 datasets registered as `LanceDataset` relations. **All NPI-binding measurements are EXACT** (set-membership, not cardinality estimation): the 82,290,893-row general NPI column was streamed once into an in-memory probe and anti-joined against the exact 9,551,447-NPI provider key set. NPI structural validity verified with the CMS Luhn (prefix `80840`) check digit; the validator was self-checked at **20,000/20,000 = 100%** against known-valid provider NPIs. Read path: `lance.dataset(uri, storage_options={aws_access_key_id, aws_secret_access_key, endpoint, region:'auto'})`; secrets via `doppler run --project core-x --config prd`. No dataset mutation occurred.

---

## 0. Corpus Telemetry Grid (first-hand, live 2026-06-06)

| Dataset | Ver | Rows | Frags | Tomb | Small files | Indices (all 0 unindexed) | Data | Index | Index:Data |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| `cms_general_payments` | 17 | 82,290,893 | 83 | 0 | 0 | 10 (4 BTREE · 6 BITMAP) | 17.727 GiB | 2,308.10 MiB | 0.127× |
| `cms_research_payments` | 17 | 5,936,454 | 8 | 0 | 0 | 10 (4 BTREE · 6 BITMAP) | 3.337 GiB | 174.94 MiB | 0.051× |
| `cms_ownership` | 15 | 27,480 | 7 | 0 | 0 | 8 (3 BTREE · 5 BITMAP) | 3.47 MiB | 0.85 MiB | 0.243× |
| `ppesRaw` | 4 | 9,551,447 | 10 | 0 | 0 | 3 (2 BTREE · 1 BITMAP) | 11.455 GiB | 250.52 MiB | 0.021× |
| `nppes_provider` | 13 | 9,551,447 | 10 | 0 | 0 | 11 (6 BTREE · 5 BITMAP) | 1.397 GiB | 454.91 MiB | 0.318× |
| `nppes_provider_taxonomy` | 6 | 11,952,809 | 12 | 0 | 0 | 4 (1 BTREE · 3 BITMAP) | 206.63 MiB | 165.21 MiB | 0.800× |
| `nppes_provider_identifier` | 6 | 2,759,800 | 3 | 0 | 0 | 4 (2 BTREE · 2 BITMAP) | 58.25 MiB | 68.61 MiB | 1.178× |
| `nppes_taxonomy_ref` | 5 | 883 | 1 | 0 | 0 | 3 (1 BTREE · 2 BITMAP) | 0.34 MiB | 0.02 MiB | 0.063× |

**Aggregate:** 8 datasets · 122,570,310 rows · **0 tombstones · 0 small files · 0 unindexed rows on any of the 53 committed scalar indices.** CMS plane 88,254,827 payment/interest records; NPPES plane 34,264,386 rows across raw + 3 derived + dimension.

---

## A. The Entity Resolution Hub (NPI Binding)

The National Provider Identifier is the singular hub key uniting the CMS ledgers to the NPPES graph. Resolution universe = `nppes_provider.npi`, **exactly 9,551,447 distinct, verified unique (PK integrity holds).**

### A.0 Derivation integrity — `ppesRaw` ↔ `nppes_provider` (prerequisite; never previously measured)

Before binding CMS to the derived core, the derived core must faithfully carry the archive's NPI universe. Exact set-difference both directions:

| Check | Count |
|---|--:|
| `ppesRaw` distinct NPI | 9,551,447 |
| `nppes_provider` distinct NPI | 9,551,447 |
| `raw \ provider` (NPIs lost in derivation) | **0** |
| `provider \ raw` (NPIs invented in derivation) | **0** |

**Perfect bijection.** The derived provider key set is the raw archive key set, exactly. No NPI is dropped or fabricated by the materializer. Every orphan finding below is therefore a property of the CMS↔NPPES boundary, not a derivation artifact.

### A.1 NPI fill rates across the CMS ledgers (exact null density)

| Ledger | Declared key | Key fill % | Key null | Secondary key | Secondary fill % | **Effective fill %** | Effective null |
|---|---|--:|--:|---|--:|--:|--:|
| `cms_general_payments` | `covered_recipient_npi` | **99.5966%** | 331,982 | — | — | **99.5966%** | 331,982 |
| `cms_research_payments` | `covered_recipient_npi` | **3.6053%** | 5,722,429 | `principal_investigator_1_npi` | 95.7325% | **99.2971%** | 41,728 |
| `cms_ownership` | `physician_npi` | **99.9272%** | 20 | — | — | **99.9272%** | 20 |

**Finding.** General and ownership bind directly on a single, well-populated NPI key. **Research's declared `covered_recipient_npi` is 96.39% null** — it is the wrong key in isolation; the resolution key for research is `principal_investigator_1_npi` (95.73% populated). The effective key `coalesce(covered_recipient_npi, principal_investigator_1_npi)` lifts research fill to **99.2971%**. Any unification join MUST coalesce these two columns for research or it discards 96% of research rows.

### A.2 Orphan rates — CMS payment records vs `nppes_provider` (exact anti-join)

Row-weighted (the directive's "percentage of payment records that fail to resolve") and distinct-weighted, effective key vs the 9,551,447-NPI provider set:

| Ledger | Keyed rows | **Orphan rows** | **Orphan % of keyed** | Resolved % of keyed | Distinct keyed NPIs | Distinct orphan NPIs | Distinct orphan % |
|---|--:|--:|--:|--:|--:|--:|--:|
| `cms_general_payments` | 81,958,911 | **360** | **0.0004%** | 99.9996% | 1,598,158 | **13** | 0.0008% |
| `cms_research_payments` | 5,894,726 | **0** | **0.0000%** | 100.0000% | 70,461 | **0** | 0.0000% |
| `cms_ownership` | 27,460 | **0** | **0.0000%** | 100.0000% | 6,271 | **0** | 0.0000% |
| **CMS plane (all 3)** | **87,881,097** | **360** | **0.00041%** | **99.99959%** | — | **13** | — |

**Finding.** The NPI hub is effectively total. **99.99959% of all keyed CMS payment records resolve to an active NPPES provider.** Research and ownership are perfect (0 orphans). General carries 360 unresolvable rows across 13 distinct NPIs out of 1.6M distinct billing NPIs. This is not a fragility — it is a clean spine.

### A.2.1 Orphan classification (general's 13 distinct unresolved NPIs)

| Classification | Count |
|---|--:|
| Total distinct orphans | 13 |
| Present in `ppesRaw` archive but not `nppes_provider` | **0** |
| Present in `nppes_provider_identifier` | **0** |
| **Truly unknown** (absent from raw, provider, and identifier) | **13** |
| Structurally valid NPI format (`^[12]\d{9}$`) | 13 |
| **Luhn-valid** (passes CMS `80840` check digit) | **9** |
| **Luhn-invalid** (malformed in CMS source) | **4** |

**Finding.** All 13 orphans are genuinely outside the NPPES universe (not a derivation gap — A.0 proved raw==provider). Of these, **9 are structurally valid NPIs** absent from the 2026-05 snapshot — almost certainly deactivated-and-purged or issued after the snapshot cut — and **4 are Luhn-invalid**, i.e. corrupt/typo NPIs in the CMS dissemination itself. There is no systemic binding defect: the orphan set is 13 NPIs, 4 of them simply dirty source data.

### A.3 Identifier linkage — capacity to resolve via secondary IDs

`nppes_provider_identifier` holds external identifiers (Medicaid / Other) keyed to NPI:

| Metric | Value |
|---|--:|
| Rows | 2,759,800 |
| Distinct NPIs carrying ≥1 secondary identifier | 1,561,083 |
| **Provider coverage** (NPIs with a secondary ID ÷ 9,551,447) | **16.34%** |
| Distinct identifier values | 2,236,195 |
| Type `05` (Medicaid) | 1,400,421 |
| Type `01` (Other) | 1,359,379 |
| `identifier.npi` ⊄ `provider.npi` (NPIs outside provider) | **0** |
| General orphans recoverable via identifier table | **0** |

**Finding — the linkage cannot recover CMS orphans, by construction.** `nppes_provider_identifier.npi` is a strict subset of `nppes_provider.npi` (0 outside it), so any NPI absent from provider is absent from identifier too — confirmed: 0 of the 13 orphans appear here. Moreover, the modern Open Payments recipient schema keys on NPI directly and exposes **no** secondary-identifier column (no state-license / Medicaid-ID field on the payment row), so the CMS join never traverses this table. Its real role is the **reverse direction**: external-ID → NPI resolution (e.g. mapping a Medicaid provider number to an NPI) for non-CMS ingest paths. It is a resolution asset, but orthogonal to the CMS↔NPPES binding. Do not model it as an orphan-recovery hop for Open Payments.

---

## B. Physical Indexing State (Scalar Topology)

### B.1 BTREE (high-cardinality) — the hub key and surrogate PKs

| Dataset | NPI BTREE | Unindexed | Other BTREE (high-card) |
|---|---|--:|---|
| `cms_general_payments` | `covered_recipient_npi` | 0 | `record_id` (PK), `applicable_manufacturer_..._id`, `date_of_payment` |
| `cms_research_payments` | `covered_recipient_npi` + `principal_investigator_1_npi` | 0 | `record_id` (PK), `applicable_manufacturer_..._id` |
| `cms_ownership` | `physician_npi` | 0 | `record_id` (PK), `applicable_manufacturer_..._id` |
| `nppes_provider` | `npi` | 0 | `last_name`, `practice_address_line1`, `practice_zip5`, `enumeration_date`, `last_update_date` |
| `nppes_provider_taxonomy` | `npi` (147.43 MiB) | 0 | — |
| `nppes_provider_identifier` | `npi` | 0 | `identifier_value` |
| `ppesRaw` | `npi` | 0 | `provider_first_line_business_practice_location_address` |
| `nppes_taxonomy_ref` | `taxonomy_code` (PK) | 0 | — |

**The hub key is fully BTREE-covered on every table on both sides of the join, every index covering 100% of rows (0 unindexed).** Research correctly carries BTREEs on *both* NPI columns — required given the 96%-null primary key. Secondary high-cardinality keys (`record_id` surrogate PKs on all three CMS ledgers; `identifier_value` for reverse lookup) are covered.

### B.2 BITMAP (low-cardinality) — categorical filter axes the directive enumerates

| Required axis | Dataset | Column | NDV (exact) | State |
|---|---|---|--:|:--|
| State / region | `nppes_provider` | `practice_state` | 61 | ✅ BITMAP |
| Taxonomy code | `nppes_provider_taxonomy` | `taxonomy_code` | 873 | ✅ BITMAP (1.10 MiB; the prune index) |
| Payment type | `cms_general_payments` | `nature_of_payment_or_transfer_of_value` | 15 | ✅ BITMAP |
| Payment form | `cms_general_payments` | `form_of_payment_or_transfer_of_value` | 6 | ✅ BITMAP |
| Recipient type | `cms_general_payments` | `covered_recipient_type` | 3 | ✅ BITMAP |
| Program year | `cms_general_payments` | `payment_year` | 7 | ✅ BITMAP |
| Recipient state | `cms_general_payments` | `recipient_state` | 69 | ✅ BITMAP |
| Dispute status | `cms_general_payments` | `dispute_status_for_publication` | 2 | ✅ BITMAP |

Plus on the NPPES graph: `entity_type_code` (2), `is_active` (2), `primary_taxonomy_code` (871), `enumeration_year` (22) on provider; `is_primary` (2), `license_state` (59) on taxonomy; `identifier_type_code` (2), `identifier_state` (59) on identifier; `grouping` (29), `section` (2) on the dimension. **Every low-cardinality axis the directive names is BITMAP-indexed.** No type mismatch exists anywhere (no BTREE on a tiny categorical, no BITMAP on a high-card key).

### B.3 Required indexing blueprint for the unification

**The index plan required to execute the CMS ⋈ NPPES join at high velocity is already complete.** Both sides of the hub key are BTREE-indexed; every categorical filter axis is BITMAP-indexed; every index covers 100% of rows. **No new scalar index is required to unify these tables.** The two residual items are *layout/clustering* facts, not missing indices, and are handled in §D:

1. **CMS NPI is indexed but not clustered.** `covered_recipient_npi` resolves via BTREE to row addresses scattered across all 83 general fragments (an NPI recurs across every program-year append) → row-level pushdown is surgical, but **fragment-level pruning on NPI does not occur** on the CMS side. (Measured in the companion CMS diagnostic via `analyze_plan`.)
2. **`nppes_provider_taxonomy.npi` BTREE (147.43 MiB) delivers no fragment pruning** — the table is `(taxonomy_code, npi)`-clustered, so `npi` is scattered across all 12 fragments (12/12, 19.97 MB, 320 IOPs for a 1,000-NPI batch). Route batch `npi`→taxonomy through the npi-clustered provider table, not directly. (Measured in the companion NPPES-analytical diagnostic.)

---

## C. Fragmentation & File Layout

### C.1 Footprint & fragment density

| Dataset | Rows | Frags | Rows/frag (min · avg · max) | Data on disk | Read-amplification |
|---|--:|--:|--|--:|:--|
| `cms_general_payments` | 82,290,893 | 83 | 17,952 · 991,457 · 1,048,576 | 17.727 GiB | none (76/83 at cap) |
| `cms_research_payments` | 5,936,454 | 8 | 31,223 · 742,057 · 1,048,576 | 3.337 GiB | none |
| `cms_ownership` | 27,480 | 7 | 3,046 · 3,926 · 4,591 | 3.47 MiB | none (trivial scale) |
| `ppesRaw` | 9,551,447 | 10 | 114,263 · 955,145 · 1,048,576 | 11.455 GiB | none |
| `nppes_provider` | 9,551,447 | 10 | 114,263 · 955,145 · 1,048,576 | 1.397 GiB | none |
| `nppes_provider_taxonomy` | 11,952,809 | 12 | 418,473 · 996,067 · 1,048,576 | 206.63 MiB | none |
| `nppes_provider_identifier` | 2,759,800 | 3 | 662,648 · 919,933 · 1,048,576 | 58.25 MiB | none |
| `nppes_taxonomy_ref` | 883 | 1 | 883 · 883 · 883 | 0.34 MiB | none |

All datasets follow the optimal append topology: **N−1 fragments at the 1,048,576-row cap + one tail.** CMS uses append-per-program-year (general's 83 = 76 capped + 7 year-remainders); NPPES uses per-snapshot overwrite. Fragments are large and few relative to row count → no read amplification anywhere.

### C.2 Compaction requirement

| Dataset | Deleted/tombstoned rows | Small files | **Compaction mandated?** |
|---|--:|--:|:--:|
| `cms_general_payments` | 0 (0.000%) | 0 | **NO** |
| `cms_research_payments` | 0 (0.000%) | 0 | **NO** |
| `cms_ownership` | 0 (0.000%) | 0 | **NO** |
| `ppesRaw` | 0 (0.000%) | 0 | **NO** |
| `nppes_provider` | 0 (0.000%) | 0 | **NO** |
| `nppes_provider_taxonomy` | 0 (0.000%) | 0 | **NO** |
| `nppes_provider_identifier` | 0 (0.000%) | 0 | **NO** |
| `nppes_taxonomy_ref` | 0 (0.000%) | 0 | **NO** |

**Binary directive: NO.** Zero tombstones and zero small files across all 8 datasets. There is no compaction debt anywhere in the join graph. No structural compaction pass is required prior to executing heavy DuckDB joins. (The CMS append-per-year and NPPES per-snapshot-overwrite write models structurally prevent fragmentation from accruing.)

---

## D. Relational Execution Strategy (DuckDB)

### D.1 Join topology map (measured grains, fan-out, and referential integrity)

```
                          cms_general_payments  (82,290,893 · N:1 on covered_recipient_npi)
                          cms_research_payments ( 5,936,454 · N:1 on coalesce(cr_npi, pi_1_npi))
                          cms_ownership         (    27,480 · N:1 on physician_npi)
                                   │  (hub key = NPI · 99.99959% of keyed rows resolve)
                                   ▼
        ┌──────────────────  nppes_provider  ◄── 1:1 ── ppesRaw (archive; perfect bijection)
        │   (9,551,447 · 1 row/NPI · PK unique · ORDER BY npi → prunes 3/10 on batch NPI)
        │            │                               │
        │   1:N (avg 1.298)                  1:N (avg 1.768)
        │            ▼                               ▼
        │   nppes_provider_taxonomy          nppes_provider_identifier
        │   (11,952,809 · max 15 slots)      (2,759,800 · max 50 slots · 16.34% NPI coverage)
        │            │  (taxonomy_code)
        │            ▼
        └─►  nppes_taxonomy_ref  (883 · NUCC v25.1 dimension · N:1 on taxonomy_code)
```

| Edge | Cardinality | Integrity check | Result |
|---|---|---|---|
| `cms_* → nppes_provider` | N:1 on NPI | orphan rows ÷ keyed | 360 / 87,881,097 = **0.00041%** |
| `ppesRaw ↔ nppes_provider` | 1:1 on NPI | symmetric set diff | **0 / 0** (perfect) |
| `nppes_provider → nppes_provider_taxonomy` | 1:N | taxonomy NPI not in provider | **0** (FK clean) |
| — providers with ≥1 taxonomy | 9,208,126 of 9,551,447 | providers with no taxonomy | **343,321** (= the deactivated-stub cohort exactly) |
| — `is_primary=true` rows | 9,208,126 | = distinct taxonomy NPIs | exactly one primary per active provider |
| `nppes_provider → nppes_provider_identifier` | 1:N | identifier NPI not in provider | **0** (FK clean) |
| `nppes_provider_taxonomy → nppes_taxonomy_ref` | N:1 | codes in data absent from ref | **1 code · 4 rows** (872/873 resolve) |

**Fan-out is modest:** taxonomy averages **1.298 slots/NPI**, identifier **1.768 slots/NPI**. The only referential gap in the entire graph is **1 taxonomy code (4 rows) absent from the 883-row NUCC dimension** — a deactivated/retired NUCC code present in NPPES but dropped from the v25.1 reference, or a single dirty code. Trivial, but it is the lone RI defect; flag on any taxonomy⋈ref inner join (use a LEFT join to retain the 4 rows).

### D.2 Star-schema viability — flatten or join live?

**Verdict: join live over the raw Lance tables; a flattened materialized view is NOT required.** Rationale, from measured structure:

- **The entire NPPES serving layer is ≈3.60 GiB uncompressed** — RAM-resident. Provider/identifier are `ORDER BY npi`-clustered (batch-NPI prunes 3/10 provider fragments, 11.42 KB, 3 IOPs); taxonomy is `(taxonomy_code, npi)`-clustered (single-specialty filter prunes 2/12, 4.87 MB). The star is `nppes_provider` (dimension-ish core, 1/NPI) ⋈ `nppes_provider_taxonomy` (the long specialty fact) ⋈ `nppes_taxonomy_ref` (NUCC dimension), with `nppes_provider_identifier` as a side dimension. DuckDB handles these real-time over the indexed/clustered Lance tables; the long-table model already solved the "specialty smeared across 15 columns" defect of the raw layer.
- **Do not pre-flatten taxonomy back into provider.** It would re-introduce the 15-column repeating-group pathology the derived layer was built to eliminate. Keep specialty as the long table; filter `taxonomy_code` (prunes to ≤2 fragments) → join to provider.

**The one place a derived projection earns its keep is the CMS↔NPPES join, not the NPPES internal star.** If CMS-payment-by-provider-attribute analytics (spend × specialty, spend × geography, spend × entity_type) becomes a hot, repeated path, materialize a narrow **`cms_payments_enriched`** projection: `cms_general` ⋈ `nppes_provider` (NPI) carrying `[record_id, covered_recipient_npi, payment_year, total_amount, nature_of_payment, entity_type_code, practice_state, primary_taxonomy_code]`, `ORDER BY covered_recipient_npi` (or `payment_year, practice_state`). This is optional and consumer-driven — it is not a structural requirement and is out of scope for this read-only diagnostic.

### D.3 Sorting & partitioning — the CMS-side asymmetry

The two sides of the hub are clustered on **different axes**, and this is the load-bearing execution fact:

| Side | Physical order | NPI fragment pruning | Temporal pruning |
|---|---|:--:|:--:|
| CMS ledgers | append-per-program-year; `payment_year` BITMAP | **No** (NPI scattered across all year-fragments) | **Yes** (`payment_year` BITMAP; `date_of_payment` BTREE) |
| `nppes_provider` / `_identifier` | `ORDER BY npi` | **Yes** (batch NPI prunes 3/10) | n/a |
| `nppes_provider_taxonomy` | `ORDER BY taxonomy_code, npi` | No (taxonomy_code prunes 2/12) | n/a |

**Execution rule for the unification (opinionated):**
1. **Drive batch CMS⋈NPPES resolution from the CMS side's selective predicate, not from NPI.** The CMS ledgers are chronological/categorical-partitioned: filter `payment_year` / `nature_of_payment` / `recipient_state` (all BITMAP, index-pruned) first to shrink the CMS row set, *then* hash-join to provider on NPI. An NPI-first scan of CMS does not prune fragments.
2. **For NPI→provider-attribute enrichment, build the NPI probe set, then push it into `nppes_provider` (npi-clustered → prunes).** Provider is the cheap side to probe by NPI.
3. **For specialty×spend, filter `taxonomy_code` on the taxonomy table (prunes 2/12), join up to provider on NPI, then to CMS.** Never probe taxonomy by NPI in batch (12/12 scan, 320 IOPs).
4. **Keep CMS payment-year-partitioned.** Re-clustering CMS by NPI would buy fragment pruning for NPI joins but forfeit the temporal/categorical locality that the ledger is actually queried on (year + manufacturer + point-NPI). The 99.99959% clean hub means the join is cheap regardless; do not re-sort the 82M-row ledger for a join that already resolves at row-level via BTREE.

---

## E. Sequential List of Physical Structural Optimizations

Ordered by blast radius. **None is mandatory** — the join graph is query-ready today. Every item is index/layout polish on healthy datasets; all are read-isolated from the data plane.

1. **(Zero-cost, do nothing — affirmed)** No compaction, no re-ingest, no index build is required to unify CMS and NPPES. Hub key fully BTREE-covered both sides, every categorical BITMAP-covered, 0 unindexed rows, 0 tombstones, 99.99959% resolution. Ship the join as-is.
2. **(Consumer hygiene, no DDL) Coalesce the research key in every join.** `coalesce(covered_recipient_npi, principal_investigator_1_npi)` is mandatory for `cms_research_payments` — the declared key is 96.39% null. Encode this in the query layer / any derived view; it is not a data defect to fix in the SoR.
3. **(LEFT-join guard, no DDL) Handle the 1 unreferenced taxonomy code (4 rows).** `nppes_provider_taxonomy → nppes_taxonomy_ref` has one code absent from NUCC v25.1. Use a LEFT join (not INNER) on the specialty dimension to avoid silently dropping 4 rows; optionally refresh `nppes_taxonomy_ref` to the NUCC version that includes the retired code.
4. **(Index right-sizing, reindex path) Resolve the 147.43 MiB `npi` BTREE on `nppes_provider_taxonomy`.** It delivers row-selection but zero fragment pruning (90% of the table's index budget for a non-pruning path). Keep only if single-NPI reverse lookup ("this provider's specialties") is a hot path; otherwise drop it and route batch `npi`→taxonomy through the npi-clustered provider join. (Detail in the NPPES-analytical diagnostic §5.1.)
5. **(Optional, reindex path) Trim duplicate/low-yield NPPES indices** — `enumeration_year` BITMAP (3.26 MiB, duplicates the `enumeration_date` BTREE axis); the `npi` BTREE on `nppes_provider_identifier` (30.51 MiB, largely redundant with npi zone-maps at 3 sorted fragments). Index-only, reversible.
6. **(Optional, consumer-driven) Materialize `cms_payments_enriched`** (§D.2) only if CMS-spend-by-provider-attribute becomes a hot repeated analytical path. Not a structural requirement.

**Explicitly NOT warranted:** compaction (0 tombstones/0 small files everywhere); NPI-clustering the CMS ledgers (forfeits temporal locality for a join that already resolves at row-level; 99.99959% clean); any schema recast or re-ingest (typing is tight, derivation is a perfect bijection); any new scalar index for the hub join (the plan is complete).

---

## Appendix — Provenance

- **Telemetry:** `pylance 7.x` (`count_rows`, `get_fragments`→`physical_rows`/`deletion_file`, `list_indices`→name/type/fields, `stats.index_stats`→`num_indexed_rows`/`num_unindexed_rows`, `stats.dataset_stats`→`num_deleted_rows`/`num_small_files`); R2 `list_objects_v2` byte census bucketed by class (`data`/`_indices`/`_versions`/`_transactions`); `duckdb 1.5.x` exact joins over `con.register(name, lance.dataset(...))`.
- **NPI binding (exact, not estimated):** the 82,290,893-row general NPI column streamed once into an in-memory DuckDB temp, anti-joined against the exact 9,551,447-NPI provider key set (and the raw-archive + identifier key sets for classification). Distinct-orphan and orphan-row counts are exact set-membership results. NPI structural validity via the CMS Luhn (`80840`-prefixed) check digit; validator self-checked 20,000/20,000 = 100% on known-valid provider NPIs.
- **Cited cross-references (same date, same method, companion diagnostics):** per-predicate `analyze_plan` pushdown/prune counts and per-index byte sizes for the NPPES analytical layer (`docs/nppes_analytical_structural_diagnostic.md`); CMS per-family null/NDV and NPI non-pruning (`docs/cms_open_payments_structural_diagnostic.md`); raw NPPES physical layout (`docs/nppes_structural_diagnostic.md`).
- **Read path:** `lance.dataset(uri, storage_options={aws_access_key_id, aws_secret_access_key, endpoint, region:'auto'})` against R2; secrets injected via `doppler run --project core-x --config prd`; no secret values persisted.
- **No dataset mutation occurred.** No DDL, no index ops, no writes to any `cms_*`, `nppes*`, or ops prefix. Read-only throughout; the only artifact produced is this document.
