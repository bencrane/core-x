# CMS Medicare Archive — Full-Archive Ingestion & Composition Plan

**Companion to:** [`docs/analysis/medicare_archive_diagnostic.md`](../medicare_archive_diagnostic.md) (the layout/grain/drift recon).
**Scope:** land the entire CMS Medicare archive from the raw landing tier
(`s3://data-sink/landing/cms/medicare-datasets/`, 35 zips / 17.03 GB compressed / **89.78 GB logical**) into the
LanceDB system of record (`s3://data-sink/active/`) — **physically optimized for the longitudinal, 13-year
provider/practice queries a PE acquirer asks** — and unite it **only on unequivocal, published keys** (the **NPI**;
the CMS enrollment **`ENRLMT_ID`**) with the existing **NPPES** and **CMS Open Payments** datasets.
**Explicitly out of scope:** fuzzy entity resolution. Tying a practice to "who it really is" — its legal/corporate
identity, parent, or ownership, via SoS / SBA / FEC / or any other source — is a **separate downstream decision**
layered *on top of* the landed data if and when prioritized. It is **not** a prerequisite for landing and must not be
coupled to it. **No fuzzy name/geo crosswalk is built in this plan.** The 13-year depth exists so those
practice-acquisition questions can be asked on top of cleanly-landed, key-united data — that is the entire mandate.
**Provenance:** every mechanic below is lifted from an existing in-repo pattern (cited inline). The drift/topology
facts were re-verified against the committed recon evidence
([`docs/reference/medicare_archive_recon_evidence.md`](../reference/medicare_archive_recon_evidence.md) — the full
per-member schema/grain/drift ground truth, distilled by `scripts/archive/recon_medicare_evidence.py`) after an Opus-4.8
adversarial review of the diagnostic; §1 records the corrections that review forced.

---

## §1 — Corrections to the diagnostic (verified against the committed recon evidence)

| # | Original claim | Corrected fact (JSON-verified) | Ingest consequence |
|---|---|---|---|
| 1 | DME C1/C2 "STABLE 97 / 93" | **C1 = 72 (2014–16) → 97 (2017+)**, **C2 = 68 → 93** — additive `Bene_CC_*` block, same as A1 | superset-reconcile C1/C2 by name; 2014–16 emit typed NULL for the 25 cols |
| 2 | A1 56-col is a "strict prefix" of 81-col | `h81[:56] != h56` — first divergence at **index 55**; 25 cols *inserted*, `Bene_Avg_Risk_Scre` displaced to tail | reconciliation MUST be **name-keyed**, never positional |
| 3 | R1 Provider Enrollment → one `cms_provider_enrollment` | **Five-table relational archive** keyed on `ENRLMT_ID` (Enrollment / Reassignment / Practice-Location / Secondary-Specialty / Additional-NPIs) | model as 5 datasets; the reassignment graph is the org-affiliation signal |
| 4 | QPP "not drift-diffed; expect churn" | **92 → 165 → 204 → 212** (2017–21 / 22 / 23 / 24) — severe additive | superset-reconcile by name across an exploding schema |
| 5 | Money → `DECIMAL(14,2)` | provider-year *totals* can exceed 10¹² → overflow NULLs the largest providers | **`DECIMAL(18,2)`** for A1/C/Part-D totals; `(14,2)` only for A2 averages |
| 6 | `program_year` under BTREE | NDV ≤13 | index as **`BITMAP`** |
| 7 | A1 grain = "1 row = NPI per year" (asserted) | head-50-bound, self-fulfilling on sorted data — **unproven PK** | prove `(npi, program_year)` with `GROUP BY … HAVING count(*)>1 = 0` before indexing |
| 8 | NPI→EIN→Form 5500 bridge via NPPES org records | **NPPES redacts EIN to constant `<UNAVAIL>`** (`docs/analysis/nppes_structural_diagnostic.md`); Form 5500 carries no NPI | no deterministic key exists → **Form 5500 / corporate-identity resolution is OUT OF SCOPE**; no fuzzy name/geo crosswalk is built (§8.2) |

Validated-correct in the diagnostic (kept): grain separation / never-union; NPI VARCHAR; append-per-year;
geography exclusion from NPI mirrors; A2 financials are averages; suppression three-state model; dedup of the `-2`
2013 zip; UTF-8-with-fallback; no bundle truncation (drift read is complete).

---

## §2 — Final mirror topology (one Lance dataset per grain, `active/…`)

| Lance dataset | Source | Grain | Years | Index (BTREE / BITMAP) |
|---|---|---|---|---|
| **`cms_physician_provider`** ⭐ | A1 | NPI × year | 2013–2024 | `npi` / `program_year`, `rndrng_prvdr_type`, `state`, `ent_cd` |
| `cms_physician_provider_service` | A2 | NPI × HCPCS × POS × year | 2017–2024 | `npi`,`hcpcs_cd` / `program_year`,`place_of_srvc`,`state` |
| `cms_physician_geography_service` | A3 | geo × HCPCS × POS × year | 2013–2024 | `hcpcs_cd` / `geo_lvl`,`program_year` |
| `cms_partd_provider` | B1 | NPI × year | 2013–2020 | `npi` / `program_year`,`prscrbr_type`,`state` |
| `cms_partd_provider_drug` | B2 | NPI × (brnd,gnrc) × year | 2013–2024 | `npi` / `program_year`,`prscrbr_type`,`state` |
| `cms_partd_geography_drug` | B3 | geo × drug × year | 2014–2024 | (drug) / `geo_lvl`,`program_year` |
| `cms_dme_referring_provider` | C1 | NPI × year | 2014–2023 | `npi` / `program_year`,`spclty`,`state` |
| `cms_dme_supplier` | C2 | NPI × year | 2014–2023 | `npi` / `program_year`,`spclty`,`state` |
| `cms_dme_supplier_service` | C3 | NPI × HCPCS × year | 2014–2023 | `npi`,`hcpcs_cd` / `program_year`,`state` |
| `cms_dme_geography_service` | C4 | geo × HCPCS × year | 2017–2023 | `hcpcs_cd` / `geo_lvl`,`program_year` |
| `cms_qpp_experience` | R2 | NPI × year | 2017–2024 | `npi` / `program_year`,`state`,`specialty`,`participation_option` |
| `cms_provider_enrollment` (head) | R1·Enrollment | 1 / `enrlmt_id` (NPI) | 2026-Q1 | `npi`,`enrlmt_id`,`pecos_asct_cntl_id` / `provider_type_desc`,`state` |
| `cms_provider_enrollment_reassignment` | R1·Reassignment | reassign↔receive pair | snapshot | `reasgn_bnft_enrlmt_id`,`rcv_bnft_enrlmt_id` / — |
| `cms_provider_enrollment_practice` | R1·Practice_Location | `enrlmt_id` × location | snapshot | `enrlmt_id` / `state` |
| `cms_provider_enrollment_specialty` | R1·Secondary_Specialty | `enrlmt_id` × specialty | snapshot | `enrlmt_id` / `provider_type_desc` |
| `cms_provider_enrollment_npi` | R1·Additional_NPIs | `enrlmt_id` × NPI | snapshot | `npi`,`enrlmt_id` / — |
| `ref_rbcs_taxonomy` | R3 | 1 / `hcpcs_cd` | RY2025 | `hcpcs_cd` / `rbcs_cat`,`rbcs_subcat` |

**Deferred (documented, not ingested):** R4 Program Statistics (xlsx-only, no CSV member — out of the DuckDB path;
roll-ups derivable from A1); R5 ACO REACH (ACO grain, no NPI — needs an ACO entity spine that does not yet exist).
Re-evaluate only on explicit demand.

**Accretion model (not a grain union):** the A1 bundle (2013–2024) is the **backfill**; a future annual A1 zip is the
**steady-state delta** appended as a new `program_year` partition into the *same* `cms_physician_provider`. Same for
every series. The "never union" rule bites only across *grains* (NPI vs geo vs drug-detail), never across years of one grain.

---

## §3 — The reusable ingest spine (per dataset, per year)

Borrows **EPA's** ZIP random-access (`pipelines/ingest_epa/materialize_epa.py:432` `_member_to_gz`) for member
extraction and **Open Payments'** (`pipelines/cms_open_payments/ingest.py`) DuckDB transform + publish/verify gate.
> **Why EPA, not Form 5500's reader:** `pipelines/form5500/ingest_form5500.py` reads the *whole zip into RAM*
> (`io.BytesIO`). A 505 MB-compressed / 3.25 GB-uncompressed A1 member would OOM/thrash. EPA streams the stored
> deflate bytes between a 10-byte gzip header and an 8-byte trailer (`CRC32`+`ISIZE` low-32) — `/tmp` holds only the
> ~0.5 GB *compressed* member, never the inflated CSV; the low-32 ISIZE is valid even for the 4.06 GB members.

Per-member sequence:

1. **Extract** — `_member_to_gz(archive_key, member, /tmp/stage/m.csv.gz)`: central dir via `_S3RangeReader` tail
   GETs → parse the *local* header for the deflate start → stream stored bytes → valid `.csv.gz`. One member at a time.
2. **Read** — `read_csv('/tmp/stage/m.csv.gz', all_varchar=true, header=true, delim=',', quote='"', escape='"',
   sample_size=-1, null_padding=true, ignore_errors=true, store_rejects=true, parallel=false)`. `parallel=false` is
   **mandatory** (Open Payments proved DuckDB's parallel CSV scanner rejects `null_padding` with the quoted newlines
   CMS names contain). DuckDB reads `.csv.gz` natively.
3. **Project** — dynamic snake_case projection built from the live `DESCRIBE` (Open Payments `_projection()`), with
   the canonical alias + typed-cast allow-list (all via `TRY_CAST(nullif(trim(x),'') AS …)`):
   - `Rndrng_NPI`/`Prscrbr_NPI`/`Rfrg_NPI`/`Suplr_NPI`/`NPI`/`npi` → **`npi VARCHAR`** (trimmed; never numeric).
   - Money totals (A1/C/Part-D) → **`DECIMAL(18,2)`**; A2 averages → `DECIMAL(14,2)`.
   - Counts (`Tot_Benes/Tot_Srvcs/Tot_Clms/Tot_HCPCS_Cds/*_Cnt`) → **`BIGINT`**.
   - Rates/scores (`Bene_CC_*_Pct`, `Bene_Avg_Risk_Scre`, `Bene_Avg_Age`) → **`DOUBLE`**.
   - Everything else (names, codes, state, ZIP5, **all `*_Sprsn_Ind/_Flag`**) → trimmed **VARCHAR**.
4. **Stamp** — inject `CAST(<year> AS SMALLINT) AS program_year` first. Year source: folder segment for bundles,
   `_D{YY}_`/`_DY{YY}_` filename token for annual zips. Without it, fragments are indistinguishable across years.
5. **Reconcile (name-keyed superset)** — project against the dataset's superset column set (A1 81, C1 97, C2 93,
   QPP 212); absent columns in a short-schema year emit `CAST(NULL AS <type>) AS <col>` in the *named* position.
   **Never positional** (§1 #2).
6. **Append** — `con.sql(sql).to_arrow_reader(1<<20)` → `lance.write_dataset(reader, local_ds,
   mode='create' if first_year else 'append', data_storage_version='2.1', max_rows_per_file=1048576)`. Never materialize.
7. **`rm`** the staged `.csv.gz`. Bounded disk: one compressed member at a time.
8. **Publish** — after all years land locally, `_publish_full_swap` (Open Payments hardened path: stage to
   `__staging` → size-census == local → atomic swap, manifest LAST → live-verify). **Not** NPPES's older
   `_replace_r2_prefix` (wipe-then-upload — vulnerable to the partial-publish failure the swap path was written to fix).
9. **Verify gate** — `_verify_published(uri, expected_rows, expected_indices, 'npi', so)`: reopen fresh from R2,
   assert row + index counts + a `npi` point-probe **before** the ledger records success. (The gate whose absence let
   an Open Payments run record "success over a corpse.")

**DuckDB config:** `threads=8`; `memory_limit='24GB'`; `temp_directory='/tmp/cms_medicare/spill'` (local NVMe);
`preserve_insertion_order=false`. **Modal:** `ephemeral_disk=524288` (512 GiB) on the `/tmp` overlay (accepts Lance's
commit-rename; a Modal Volume does **not** — FUSE EPERM); `memory=49152` (48 GiB) for the in-RAM BTREE sort.

### 3.1 Per-year extraction & dedup
- **Bundles** (A1/A3/B1/B3/C1–C4/QPP/R1): one zip, per-year subfolders (`…/<YEAR>/…`); R1 is one snapshot folder, 5 files.
- **Annual zips** (A2 ×8, B2 ×13): one CSV, year in filename token.
- **Dedup:** drop `2013-…by Provider and Drug-2.zip` (byte-identical 2013 B2 dup); the `-2` on 2021–23 B2 is the
  *only* copy — ingest it; ignore the A2 nested template `MUP_PHY_Ryy_…zip` and the standalone CPT-license PDF.
- **Encoding:** full strict-UTF-8 validate → latin-1 transcode fallback (NPPES `_is_utf8`/`_transcode_latin1_to_utf8`)
  before the read — not a head sniff (NPPES proved the utf-8 reader hard-fails on a raw latin-1 body).

---

## §4 — Idempotency ledger (`ops.cms_medicare_runs`)

Mirrors `ops.cms_open_payments_runs`. Key: `(dataset, program_year, source_object_etag)`.

```sql
CREATE TABLE IF NOT EXISTS ops.cms_medicare_runs (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  feed text NOT NULL DEFAULT 'cms_medicare',
  phase text NOT NULL,                 -- ingest | index | publish | verify
  dataset text, program_year smallint,
  source_archive text, source_member text, source_object_etag text,
  candidate_key_dups bigint,           -- §1 #7 grain proof, recorded
  decimal_overflow_nulls bigint,       -- §1 #5 money-width assertion, recorded
  rows_processed bigint, rejected_rows bigint,
  status text NOT NULL, error text,
  started_at timestamptz, completed_at timestamptz,
  recorded_at timestamptz NOT NULL DEFAULT now()
);
```
One `(dataset, program_year)` = one append unit = one blast-radius cell. A re-run of a landed `(dataset, year, etag)`
is a no-op; a **new etag for the same year** (CMS re-publishes a prior year under a new release-year `R{NN}`) triggers
the per-year `ds.delete("program_year = N")` + re-append (Open Payments `ingest_family_year` pattern). The two quality
probes are recorded per unit → the grain/width validations become permanent audit facts.

---

## §5 — Index plan (build once, after all years land; isolated from appends)

- `npi`, `hcpcs_cd`, `enrlmt_id`, `pecos_asct_cntl_id` → **BTREE** (high-cardinality resolution keys).
- `program_year` (≤13), `*_state*`, `*_type/spclty*`, `place_of_srvc`, `geo_lvl`, `ent_cd`, `participation_option`,
  `rbcs_cat/subcat` → **BITMAP** (low-cardinality filters).
- **Magnitude gate first.** Row counts are unknown by design. Before any index build, `SELECT count(*)` over each
  staged member, recorded in the ledger. Expected: A1 ≈ 10–12M (12 yr × ~1M providers — same envelope as
  `nppes_provider_taxonomy` 11.95M, trains in 32–48 GiB in-RAM). **B2 (~48 GB CSV, likely >100M rows) and A2** cross
  R2's multipart escalation threshold → build their `npi` BTREE via the federal-spine **`run_staged` append-only
  path** (`pipelines/resolution/federal_spine_index_campaign.py`): stage locally, build, upload only the new
  `_indices/<uuid>/` + manifest + txn via boto3 uniform parts; never rewrite data files.
- **Candidate-key proof gate.** Do not build `npi` BTREE until `GROUP BY <hub> HAVING count(*)>1 = 0` passes per
  (dataset, year). A non-zero result means the true key includes another axis (likely `ent_cd`); widen + re-document
  the grain first. Index builds run in a **process isolated from appends** (Open Payments boundary) — a failed build
  never corrupts landed data.

### 5.1 Physical layout — cluster for the longitudinal query (the optimization that makes the 13-year questions fast)
The dominant query is **per-provider (or per-practice) across all 13 years** — "NPI X's payment / volume / service-mix
trajectory 2013→2024" — and **cohort-by-year** — "all dermatology NPIs in TX, payment trend by year." Append-per-year
lands each year as its own fragment, so a given `npi` is **scattered across 13 fragments** and a per-provider pull
touches all of them (the diagnostic's "BTREE-indexed but not clustered" caveat, §8.1). For an *optimized* landing:
- **After the backfill lands, run one compaction pass that sorts each NPI dataset by `(npi, program_year)`** (Lance
  `compact_files` / rewrite with a sort order). This **clusters** `npi`: a single-provider 13-year scan then prunes to
  a handful of contiguous fragments instead of all 13, and the `npi` BTREE delivers fragment-level pruning (the NPPES
  analytical "clustering dividend"). **This is the single highest-leverage landing optimization for the PE use case.**
- **Steady state:** an annual append adds one npi-scattered fragment for the new year; re-compact (sort `npi,
  program_year`) on a cheap cadence to restore clustering. Compaction is isolated from appends and from index builds.
- **A2 / C3** (NPI×HCPCS) cluster by `(npi, hcpcs_cd, program_year)` — provider-and-procedure trend is their hot path.
- **Fragment/row-group sizing:** `max_rows_per_file=1<<20` keeps fragments scan-efficient; the cohort BITMAPs
  (`program_year`, `state`, specialty) prune by-year cohort queries without a full scan.
This is what lets the landed datasets answer the PE questions (§8.4) with surgical pruning — no fuzzy resolution needed.

---

## §6 — Orchestration

- **Modal app `cms-medicare-pipelines`** (mirrors `cms-open-payments-pipelines`): `apply_state_schema`,
  `discover_members` (read-only central-dir enumeration → ingest units), `ingest_dataset` (one dataset, all years →
  stage → append → index → publish → verify), `ingest_dataset_year` (surgical single-year / new-vintage replace),
  `reindex_dataset`, `verify_dataset`. R2 + Postgres via Modal secrets `r2-credentials` + `hqx-postgres`.
- **Isolation/resumability:** one container per dataset; per-member failures recorded + skipped (one bad year never
  sinks the dataset); `retries=3` absorbs spot preemption (publish idempotent + read-back-verified → re-run cannot
  corrupt the live dataset).
- **Trigger.dev schedule** (control plane, mirrors `src/trigger/cms_open_payments.ts`): **annual** primary cadence
  (CMS publishes a new program year ~spring under a new `R{NN}`) + a **quarterly light `discover_members`** that diffs
  zip ETags against the ledger and fires `ingest_dataset_year` only for changed `(dataset, year)` pairs (catches CMS's
  prior-year re-publishes). Durable callback writes terminal run rows + POSTs a flat summary.

---

## §7 — Sequencing

1. **A1 → `cms_physician_provider`** — directive target: native provider-year *totals*, full decade+ (2013–2024), the
   primary entity-360 utilization spine.
2. **A2 → `cms_physician_provider_service`** — NPI×HCPCS companion; unlocks RBCS enrichment. (Magnitude-probe → likely staged index.)
3. **B1 + B2 → Part D** — prescriber totals + prescriber×drug detail. B2 is the largest dataset → staged append-only index.
4. **C1/C2/C3 → DME** — referring/supplier/service; superset-reconcile the corrected drift.
5. **Geography (A3/B3/C4)** — market/denominator context; NPI-orthogonal; cheap (<1 GB each).
6. **References (R1 5-table enrollment, R3 BETOS)** — resolution enrichers; R1 last (snapshot, value realized once the NPI mirrors exist).

Order rationale: dependency (A1 before its A2 companion), blast radius (giants B2/A2 magnitude-probed + isolated
before index builds), directive priority (decade+ totals first).

---

## §8 — Composition with the existing graph (the payoff)

### 8.1 The NPI join spine (already proven live)
`docs/analysis/cms_nppes_relational_diagnostic.md` measured this spine with live R2 data:
- **NPPES is the NPI master:** `nppes_provider` (1/NPI, **9,551,447 verified-unique**, BTREE `npi`),
  `nppes_provider_taxonomy` (specialty axis, BITMAP `taxonomy_code`), `nppes_provider_identifier` (external-ID→NPI).
- **Open Payments keys:** general/research `covered_recipient_npi`, ownership `physician_npi`. **Research is 96.39%
  null on `covered_recipient_npi`** → the effective key is `coalesce(covered_recipient_npi,
  principal_investigator_1_npi)` (→99.30% fill). Any join touching research MUST coalesce. (Medicare mirrors have one
  clean `npi` — no such split.)
- **Unified key = canonical `npi VARCHAR`.** All Medicare aliases collapse to it at ingest. Every NPI dataset carries
  the `npi` BTREE → the join plan is already complete; no new spine index is required.
- **Expected resolution:** the live anti-join measured **99.99959%** of keyed CMS payment records resolving to an
  active NPPES provider. **Validate at first A1 land:** anti-join `cms_physician_provider.npi` vs `nppes_provider.npi`,
  expect <0.01% orphans, classify each via the CMS Luhn (`80840` prefix) to split deactivated-and-purged (valid) from
  dirty-source (invalid).
- **Clustering reality:** Medicare `npi` is BTREE-indexed but **not clustered** (an NPI recurs across every year →
  no fragment pruning on `npi`; row-level pushdown only). `nppes_provider_taxonomy` is `(taxonomy_code, npi)`-clustered
  → a batch npi→taxonomy lookup must route through the npi-clustered `nppes_provider`, never the taxonomy table directly.

### 8.2 Form 5500 & corporate identity — explicitly OUT OF SCOPE
No **deterministic** key links Medicare to Form 5500: NPPES redacts EIN to a constant `'<UNAVAIL>'` sentinel
(`docs/analysis/nppes_structural_diagnostic.md`) and Form 5500 carries no NPI anywhere — it is `ACK_ID` / `SPONS_DFE_EIN`-keyed,
and every detail-table `*_EIN` is a *counterparty*, not the sponsor (`docs/analysis/form5500_relational_diagnostic.md`). The
only way to bridge them is **fuzzy name+geo matching, which is non-deterministic and a business judgment — explicitly
excluded from this plan.** **No `crosswalk_*_form5500` dataset is built.**

Resolving a practice to its legal/corporate identity, parent, or ownership — whether later via SoS, SBA, FEC, or any
other source — is a **separate downstream layer**, built *on top of* the landed, key-united data and joined by its own
key once it exists. Landing does not wait on it and is never coupled to it. This plan's job is to land the data
correctly and optimally so those questions *can* be asked, not to answer the identity question.

### 8.3 Reference crosswalks unlocked
- **R1 Provider Enrollment — the DETERMINISTIC practice/group rollup (key-based, CMS-published; this is how
  "practice" is reached with zero fuzzy matching).** `NPI ↔ PECOS_ASCT_CNTL_ID ↔ ENRLMT_ID`, plus the
  **`PPEF_Reassignment` graph** (`REASGN_BNFT_ENRLMT_ID → RCV_BNFT_ENRLMT_ID`): an individual provider's enrollment
  reassigns its billing to the group/practice enrollment that receives it. Traversed `provider NPI → ENRLMT_ID →
  (reassignment) → group ENRLMT_ID`, this rolls per-NPI Medicare utilization/payment up to the **billing practice
  entirely on CMS keys** — exactly the grain a PE acquirer evaluates, with no name/geo guessing. `MULTIPLE_NPI_FLAG` +
  `PPEF_Additional_NPIs` flag multi-NPI providers (a dedup guard for per-provider aggregates).
- **R3 BETOS/RBCS** — `HCPCS_Cd → RBCS_Cat/Subcat/Family` enriches A2/C3 with clinical service lines (imaging, major
  procedures, E&M…), respecting `HCPCS_Cd_Add_Dt`/`End_Dt` against `program_year`.

### 8.4 The analytical surface — `provider_360` (deterministic composition, key-united only)
Built **on top of** the landed data, uniting **only on unequivocal keys** (NPI; `ENRLMT_ID`). Per
`docs/analysis/entity-360-master-plan.md` and `docs/analysis/nppes_analytical_implementation_plan.md` (per-snapshot derived serving
layer `…/snapshot=YYYY-MM/`), the NPI-grain analogue:

- **`provider_360`** — 1 row / NPI, base `nppes_provider` (the 9.55M verified-unique key set), LEFT JOINs on canonical
  `npi`/`ENRLMT_ID` (BTREE-aligned), **all deterministic**:
  - identity + specialty ← `nppes_provider` + `nppes_provider_taxonomy` (route via npi-clustered provider, §8.1).
  - longitudinal Medicare utilization/payment (2013–2024) ← `cms_physician_provider` (per-year totals + `Bene_CC_*`
    acuity mix for 2017+, as longitudinal arrays or latest-snapshot + trend deltas); + Part D + DME dimensions.
  - industry transfers ← Open Payments (with the mandatory research coalesce) + ownership.
  - quality ← `cms_qpp_experience` (MIPS `final_score`).
  - **practice/group rollup ← R1 reassignment graph** (`ENRLMT_ID`, §8.3) — the deterministic path to practice-level.
  No fuzzy-resolved attributes; no corporate-identity columns. "Who owns this" is the separate downstream layer
  (§8.2), joined later by its own key once built — never folded in here.
- **Indexes:** BTREE `npi` (clustered — `ORDER BY npi` so npi-filter prunes fragments, §5.1); BITMAP
  `primary_taxonomy_code`, `entity_type_code`, `state`, latest-active-year. Per-snapshot, pure function of inputs,
  idempotent overwrite; chained off the raw ingests but decoupled (a materialize failure pages, never rolls back the SoR).
- **PE practice-acquisition questions this answers (single indexed scan, all key-deterministic):**
  - "Dermatology NPIs/practices in TX whose Medicare payment grew >X% 2019→2024, ranked by 2024 `Tot_Mdcr_Pymt_Amt`."
  - "A target practice's 13-year trajectory — payment, beneficiary panel (`Tot_Benes`), service mix (HCPCS/RBCS),
    acuity (`Bene_CC_*`) — rolled to the group via the reassignment graph."
  - "Every provider reassigning billing to a target group `ENRLMT_ID`, with their individual utilization + Open
    Payments exposure + MIPS scores."
  - "DME suppliers by RBCS service line and state, ranked by Medicare payment trend."
  None require entity resolution — all answered on landed data united by `npi` / `ENRLMT_ID`.

---

## §9 — Validate-before-build gates (hard stops)

1. **Grain PK proof** — `GROUP BY <hub> HAVING count(*)>1 = 0` per (dataset, year) **before** the `npi` BTREE.
2. **Money width** — `count(*) WHERE try_cast(col AS DECIMAL(18,2)) IS NULL AND nullif(trim(col),'') IS NOT NULL = 0` per money column.
3. **NPI resolution** — anti-join first A1 land vs `nppes_provider`, expect <0.01% orphans, Luhn-classify the residue.
4. **Research coalesce** — wherever Open Payments enters `provider_360`, `coalesce(covered_recipient_npi, principal_investigator_1_npi)`.
5. **No fuzzy joins** — `provider_360` and every composition unite on published keys only (`npi`, `ENRLMT_ID`); no name/geo-resolved attribute enters the SoR or the serving layer. Corporate-identity resolution is a separate, later, key-joined layer.
6. **Magnitude → index path** — `count(*)` per dataset before index build; B2/A2 → staged append-only path.
7. **Suppression preserved** — assert every `*_Sprsn_Ind/_Flag` survives into the final schema; never coalesce suppressed→0.
