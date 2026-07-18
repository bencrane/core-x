# Federal LanceDB Ingest — Ground-Truth Diagnostic

**Date:** 2026-06-06 · **Repo:** `core-x` · **System of record probed:** live R2 `s3://data-sink/`
**Scope:** SAM.gov + USAspending federal ingest, audited against the target two-track + `merge_insert` Reconciled-Mirror architecture.

> Every figure below was read from disk (Lance manifest / `count_rows` / index list / `MAX()` column scan) on 2026-06-06, not inferred from code or docs. Codebase claims were cross-checked against disk; where they diverge it is called out. Nothing here is assumed to exist or to work without verification.

---

## 0. Method & provenance

| Layer | How it was established |
|---|---|
| Dataset existence | Recursive R2 leaf discovery — a prefix is a dataset iff it has a `_versions/` manifest. Container prefixes (`usaspending/`, `fmcsa/`, `ca_ucc/`, `nppes/`) were expanded to true leaves. |
| Schema / PK / date / vector cols | `lance.dataset(uri).schema` (typed Arrow schema). Vector = `FixedSizeList<float>`; temporal = timestamp/date/time. |
| Row counts | `LanceDataset.count_rows()` (manifest metadata — exact, not sampled). |
| Indexes | `LanceDataset.list_indices()` — name, type (BTree/Bitmap/LabelList/ANN), columns. |
| System staleness | latest entry of `LanceDataset.versions()` (manifest commit timestamp, UTC). |
| Data staleness | `SELECT max(<date_col>)` streamed through DuckDB over a single projected column (giants capped at 150 s, cap recorded not dropped). |
| Write mode / cadence | read from the worker scripts (`pipelines/**`) and Trigger tasks (`src/trigger/**`), not assumed. |

Reproducible probe: [`scripts/archive/lance_ground_truth_probe.py`](../../scripts/archive/lance_ground_truth_probe.py) (complements the name-only [`scripts/data-factory-catalog.py`](../../scripts/data-factory-catalog.py)). Run:

```bash
doppler run -p core-x -c prd -- uv run --no-project \
  --with boto3 --with pylance --with duckdb \
  python3 scripts/archive/lance_ground_truth_probe.py > /tmp/lance_probe.json
```

**Surface size:** 166 leaf Lance datasets · 2,217,834,380 rows total. Federal subset: **64 leaf datasets · 879,877,313 rows.**

---

## 1. Headline — the gap in one screen

The target architecture (raw bulk table ⟂ raw daily-API table → `merge_insert` → read-optimized Reconciled Mirror; **all** BTREE + vector indexes built **only** on the Mirror) is **not implemented for any federal feed.** Ground truth:

| Target property | Current reality | Status |
|---|---|---|
| `merge_insert`-compiled Reconciled Mirror | **Zero** `merge_insert` calls in `pipelines/sam_gov/**` or `pipelines/usaspending/**`. Every "mirror-like" table is a full `overwrite` rebuild. | ❌ absent |
| Bulk and daily-API isolated into their own raw tables | SAM: **no daily raw table exists** (daily delta is docs-only). USAspending: daily-API landing exists but is partitioned into per-`pull_date` sub-datasets, not one appendable raw table, and has **no scheduled control plane**. | ◐ partial |
| Indexes consolidated on the Mirror only | BTREE/BITMAP indexes live scattered across **raw** (`entity_registrations`, `usaspending/*`) **and** derived (`sam_master_*`, `sam_normalized_entities`, `sam_pocs`) tables. On the USAspending facts they are **resolution-key-only** (uei/naics/cage) — no temporal or award-identity index. | ❌ wrong layer |
| Vector embeddings on the Mirror | **Zero vector columns and zero ANN indexes anywhere** in the 166-dataset surface (one non-federal scalar `LabelList` aside). | ❌ absent |
| Freshness | Served federal-spend data is stale to **~2026-04-24** while a fresher daily pull (action_date **2026-06-02**) sits unreconciled in the landing tier; daily-rebuilt derivatives show **today's** system timestamp over **6-week-old** data. | ❌ stale |

---

## 2. Step 1 — Directory & asset map

### 2.1 Container-vs-leaf correction

The flat catalog reports `usaspending`, `fmcsa`, `ca_ucc`, `nppes` as single "datasets." On disk they are **container prefixes**. Recursive discovery resolves the true count:

| Container prefix | True leaf datasets |
|---|---|
| `active/usaspending/` | **56** (7 indexed fact/dimension tables + ~49 reference/dimension tables) |
| `active/fmcsa/` | 7 (`auth_hist`, `boc3`, `carrier`, `census`, `census_mail_ready`, `insurance`, `oos`, `revocation`) |
| `active/ca_ucc/` | 5 (`debtors`, `debtor_index`, `filings`, `filing_amendments`, `secured_parties`) |
| `active/nppes/` + siblings | `nppes/snapshot=2026-05` (raw) + 3 analytical (`nppes_provider*`) |
| `sam-gov-opps/` | 1 (`sam-gov-opps/active`) |
| `usaspending_api_landings/` | 1 (`award_search/pull_date=2026-06-04`) — **outside `active/`** |

`landing/` (raw gz transport: `entity_registrations_raw_public-v2/`, `usaspending/`, …) holds **no Lance datasets** — transport-only, correctly excluded from the SoR.

### 2.2 Federal dataset → writer map (verified by reading each script)

| Dataset | Writer script | Write mode | Control-plane cadence | Source(s) read |
|---|---|---|---|---|
| `entity_registrations` | `sam_gov/entity_registrations_bulk.py` | `create`+`append` per file | `entity_registrations_backfill` — **bounded backfill, no cron** | R2 `landing/entity_registrations_raw_*` |
| `usaspending/<table>` (×56) | `usaspending/usaspending_bulk.py` | `overwrite` | `usaspending_bulk` — **bounded 2026-05-06 dump, no cron** | files.usaspending.gov pg_dump |
| `sam-gov-opps/active` | `sam_gov/sam_opps_bulk.py` | `overwrite` | `sam_opps_bulk` — **daily 12:00 UTC** | SAM Opportunities API |
| `usaspending_api_landings/award_search/*` | `usaspending/usaspending_api_landing.py` | `overwrite` per `pull_date` | **no Trigger task** (ran ad-hoc; ledger has 1 run) | USAspending award-search API |
| `sam_master_entities` / `_contacts` / `_domains` | `sam_gov/sam_master.py` | `overwrite` | `sam_spine_refresh` — **cron removed (PR #233)** | `entity_registrations` |
| `sam_normalized_entities` | `sam_gov/sam_normalized_entities.py` | `overwrite` | via spine refresh (cron-disabled) | `sam_master_entities` |
| `sam_pocs` | `sam_gov/sam_pocs.py` | `overwrite` | `sam_pocs` — **daily 16:30 UTC** | `entity_registrations` |
| `sam_opps_attachment_manifest` | `sam_gov/sam_attachment_manifest.py` | `overwrite` | event (post-opps) | `sam-gov-opps/active` |
| `contractor_award_summary` | `usaspending/contractor_award_summary.py` | `overwrite` | **daily 18:00 UTC** | `usaspending/award_search` + `subaward_search` |
| `ffata_exec_comp` | `usaspending/ffata_exec_comp.py` | `overwrite` | **daily 17:00 UTC** | USAspending FFATA |
| `usaspending_api_catalog` | `usaspending/usaspending_api_catalog.py` | `overwrite` | manual | USAspending API spec |
| `crosswalk_sam_usaspending` | `resolution/crosswalk_sam_usaspending.py` | `overwrite` | **daily 16:00 UTC** | `entity_registrations` + `usaspending/recipient_lookup` + `transaction_search_fpds` |
| `bridge_sam_pdl`, `bridge_sam_fmcsa_domain`, `crosswalk_sos_sam` | `resolution/*` | `overwrite` | daily/event | SAM + PDL/FMCSA/SoS |

**Index provenance (verified):** BTREE indexes on the USAspending fact tables were **not** built by `usaspending_bulk.py`'s Phase-3 `INDEX_PLAN`. They were built by the cross-domain maintenance worker [`pipelines/resolution/federal_spine_index_campaign.py`](../../pipelines/resolution/federal_spine_index_campaign.py), whose `EXACT_TARGETS` map (resolution keys only) matches disk byte-for-byte. `usaspending_bulk.py`'s 13-column-per-table plan (with `action_date`, `award_id`, `fiscal_year`, …) **was never applied**.

---

## 3. Step 2 — State & schema (federal deep table)

PK column = the dataset's resolution grain (verified from the worker's `QUALIFY`/dedup/`merge` key, not guessed). **Vector cols: none on any federal dataset.**

| Dataset | Rows | Grain / PK key | Indexes (on disk) | Date columns present |
|---|--:|---|---|---|
| `entity_registrations` | 19,299,314 | append-only (uei + extract_label) | `uei`·BT, `cage_code`·BT, `extract_label`·BT | `last_update_date`, `registration_date`, `expiration_date`, `activation_date` |
| `sam_master_entities` | 1,541,566 | 1 row / `uei` | `uei`·BT, `primary_naics`·BT, `cage_code`·BT | `last_update_date`, `registration_date`, … |
| `sam_master_contacts` | 4,373,319 | ≤6 rows / `uei` | `uei`·BT | — |
| `sam_master_domains` | 709,546 | (`normalized_domain`,`uei`) | `normalized_domain`·BT, `uei`·BT | — |
| `sam_normalized_entities` | 1,541,566 | 1 row / `uei` | `uei`·BT, `normalized_legal_name`·BT, `legal_name_base`·BT, `cage_code`·BT, `primary_naics`·BT, `is_active`·BM | — (carries no date col) |
| `sam_pocs` | 8,065,679 | POC / `uei` | `uei`·BT, `cage_code`·BT, `name_key`·BT, `last_name`·BT, `poc_type`·BM, `source_family`·BM | — |
| `sam_opps_attachment_manifest` | 331,401 | `resource_id` | `notice_id`·BT, `resource_id`·BT, `naics_code`·BT, `trigger_relevant`·BM, `mime_type`·BM, `access_level`·BM | `posted_date` |
| `sam-gov-opps/active` | 79,211 | `notice_id` (daily snapshot) | **none (0)** | `posted_date`, `snapshot_date` |
| `usaspending/award_search` | 78,373,286 | `award_id` / `generated_unique_award_id` | `recipient_uei`·BT, `parent_uei`·BT, `naics_code`·BT | `action_date`, `last_modified_date`, `period_of_performance_*` |
| `usaspending/transaction_search_fpds` | 107,250,527 | `transaction_id` | `recipient_uei`·BT, `parent_uei`·BT, `naics_code`·BT, `cage_code`·BT | `action_date`, `last_modified_date` |
| `usaspending/transaction_search_fabs` | 128,784,183 | `transaction_id` | `recipient_uei`·BT, `parent_uei`·BT, `naics_code`·BT, `cage_code`·BT | `action_date`, `last_modified_date` |
| `usaspending/subaward_search` | 9,801,723 | `broker_subaward_id` | 6× uei/naics variants·BT | `sub_action_date`, `last_modified_date` |
| `usaspending/financial_accounts_by_awards` | **454,215,610** | `financial_accounts_by_awards_id` | **none (0)** ⚠ | `last_modified_date` (all-NULL on disk) |
| `usaspending/recipient_lookup` | 17,754,022 | `recipient_hash` / `uei` | `uei`·BT, `parent_uei`·BT | `ingested_at` |
| `usaspending/recipient_profile` | 18,275,944 | `recipient_hash` / `uei` | `uei`·BT, `parent_uei`·BT | `ingested_at` |
| `usaspending/*` (≈49 dimension/reference) | 1 – 3.3M each | various | **none (0)** (small → full-scan, intentional) | `ingested_at` (load stamp) |
| `usaspending_api_catalog` | 176 | `endpoint_path` | `endpoint_path`·BT | `fetched_at` |
| `usaspending_api_landings/award_search/pull_date=2026-06-04` | 583,776 | `generated_unique_award_id` | **none (0)** | `action_date`, `last_modified_date` |
| `contractor_award_summary` | 578,958 | 1 row / `recipient_uei` | `recipient_uei`·BT | `prime_most_recent_action_date`, `prime_first_award_date` |
| `ffata_exec_comp` | 29,601 | `recipient_uei` | `recipient_uei`·BT, `name_key`·BT, `officer_rank`·BM, `source_channel`·BM | `disclosure_action_date` |
| `crosswalk_sam_usaspending` | 1,028,144 | 1 row / `uei` | `uei`·BT, `cage_code`·BT, `normalized_legal_name`·BT | — |

`BT` = BTREE · `BM` = BITMAP. (`crosswalk_sam_usaspending` carries a third BTREE — `normalized_legal_name` — beyond the two its docstring claims; disk is authoritative.)

**Index totals:** of the 64 federal leaves, **47 have zero indexes.** Most are small USAspending reference/dimension tables (full-scan cheap — intentional), plus the daily-overwrite `sam-gov-opps/active` snapshot and the transient API landing. **Two are material, unintentional gaps: `financial_accounts_by_awards` (454,215,610 rows — the single largest dataset in the entire factory) and `financial_accounts_by_program_activity_object_class` (10,235,016 rows) — both completely unindexed.**

---

## 4. Step 3 — Staleness diagnostic (dual-layer)

System staleness = when the dataset was last written. Data staleness = newest record inside it. The divergence is the story.

| Dataset | System staleness (write) | Data staleness (max date) | Lag | Read |
|---|---|---|--:|---|
| `sam-gov-opps/active` | 2026-06-06 08:00 | 2026-06-05 22:54 (`posted_date`) | ~1 d | ✅ fresh, as designed (daily snapshot) |
| `entity_registrations` | 2026-06-01 22:52 | 2026-05-03 (`last_update_date`) | ~30 d | ◐ bounded by last monthly extract (`20260503`); next monthly 2026-06-07 |
| `sam_master_entities` | 2026-06-06 15:41 | 2026-05-03 (`last_update_date`) | ~34 d | ◐ correctly mirrors the raw monthly; no daily delta to advance it |
| `usaspending_api_landings/.../2026-06-04` | 2026-06-04 16:56 | 2026-06-02 23:56 (`action_date`) | ~2 d | ✅ fresh — but stranded (not reconciled into served tables) |
| `usaspending/award_search` | 2026-06-01 19:26 | **2026-04-24** (`last_modified_date`) | **~43 d** | ❌ served spend data ~6 wk stale |
| `usaspending/transaction_search_fpds` | 2026-06-01 19:34 | **2026-04-23** | ~44 d | ❌ stale (bounded dump) |
| `usaspending/transaction_search_fabs` | 2026-06-01 20:47 | **2026-04-24** | ~43 d | ❌ stale |
| `usaspending/subaward_search` | 2026-06-01 19:20 | **2026-04-24** (`sub_action_date`) | ~43 d | ❌ stale |
| `contractor_award_summary` | **2026-06-06** 14:01 | **2026-04-23** | ~44 d | ⚠ **cosmetic freshness** — rebuilt daily, but re-derives the stale bulk |
| `ffata_exec_comp` | **2026-06-06** 13:02 | **2026-04-23** (`disclosure_action_date`) | ~44 d | ⚠ cosmetic freshness — same root cause |
| `crosswalk_sam_usaspending` | **2026-06-06** 12:04 | n/a (no date col) | — | ⚠ rebuilt daily over a stale USAspending side |

**Two findings the dual-layer metric exposes that a single timestamp would hide:**

1. **Cosmetic freshness.** `contractor_award_summary`, `ffata_exec_comp`, and `crosswalk_sam_usaspending` all carry a *today* system timestamp because their daily cron fires — but their **data** is frozen at the bulk's 2026-04-23/24 frontier. The daily rebuild burns compute to reproduce stale output. Any "is it fresh?" check on system staleness alone is misleading for the entire USAspending-derived layer.
2. **Stranded fresh data.** The award-search API landing already holds records to 2026-06-02, ~6 weeks ahead of the served `award_search`. The freshness exists in the factory; nothing reconciles it forward. This is precisely the hole the `merge_insert` Mirror is meant to fill.

---

## 5. Step 4 — Architecture classification

The directive's four buckets do not cleanly hold the federal surface, because most "mirror-like" tables are **full-rebuild derivatives**, which are *not* `merge_insert` Reconciled Mirrors. Honest taxonomy (Reconciled Mirror reserved for merge/insert-compiled tables):

| Class | Definition | Federal datasets | Count |
|---|---|---|--:|
| **Raw Bulk** | historical/point-in-time bulk load | `entity_registrations`; all 56 `usaspending/*` | 57 |
| **Raw API** | daily/delta API pulls | `sam-gov-opps/active` (wired daily); `usaspending_api_landings/award_search/*` (**control-plane orphan**) | 2 |
| **Reconciled Mirror** (`merge_insert`) | bulk⋈delta compiled, read-optimized | **— none —** | **0** |
| **Derived / Materialized** (full `overwrite` rebuild) | recomputed from raw each run; the *intended* Mirror candidates | `sam_master_entities/contacts/domains`, `sam_normalized_entities`, `sam_pocs`, `sam_opps_attachment_manifest`, `contractor_award_summary`, `ffata_exec_comp`, `usaspending_api_catalog` | 9 |
| **Cross-source bridge** | identity crosswalk | `crosswalk_sam_usaspending` (+ SAM-adjacent `bridge_sam_pdl`, `bridge_sam_fmcsa_domain`, `crosswalk_sos_sam`) | 1 (+3) |
| **Orphaned / Unknown** | on disk, no live writer | — (federal: none) | 0 |

**Orphan analysis (whole surface):**
- **Dataset orphans (on disk, no writer):** only 2, both non-federal backup snapshots — `overture_places__bak_2026-05-20.0_20260606T192125Z`, `overture_places__bak_v3_2026-05-20.0_20260606T220113Z`. Candidates for GC.
- **Script orphan (writer, no dataset):** `pipelines/sam_gov/sam_entity_master.py` targets `active/sam_entity_master/` which **does not exist on disk** — superseded by `sam_master.py` (`sam_master_entities`). Dead worker; retire it.
- **Control-plane orphan:** `usaspending_api_landing.py` has a worker + `ops.usaspending_award_search_api_landing_runs` ledger + on-disk data, but **no `src/trigger/*` task** schedules it. The USAspending "two-track" is half-wired.

---

## 6. Strict gap list — current ground truth → target Reconciled-Mirror architecture

Ordered by blast radius. Each is a verified delta, not a recommendation.

**G1 — No Reconciled Mirror exists for any federal feed.** Zero `merge_insert` in `pipelines/sam_gov/**` and `pipelines/usaspending/**`. The target's central mechanism is absent; the closest analogues (`sam_master_entities`, `contractor_award_summary`) are full-`overwrite` rebuilds.

**G2 — Raw tiers are not cleanly isolated per the two-track model.**
- *SAM:* no daily raw table at all. `SAM_ENTITY_DAILY_DELTA_BUILD_PLAN.md` status = "Proposed (not started)"; the master advances only on the monthly bulk.
- *USAspending:* a daily-API raw tier exists (`usaspending_api_landings/award_search/`) but (a) is partitioned into per-`pull_date` sub-datasets rather than one appendable raw table, and (b) has no scheduled Trigger task — so it does not run daily today.

**G3 — Indexes are on the wrong layer and incomplete for reconciliation.** BTREE/BITMAP live across raw + derived tables, never on a single Mirror. On the USAspending facts they are **resolution-key-only** (`recipient_uei`/`parent_uei`/`naics`/`cage`, from `federal_spine_index_campaign.py`). There is **no index on `action_date`/`last_modified_date` and none on `award_id`/`generated_unique_award_id`/`unique_award_key`** — exactly the date-range + award-identity keys a `merge_insert` reconcile and incremental refresh require. `usaspending_bulk.py`'s richer `INDEX_PLAN` was never applied.

**G4 — Zero vector embeddings / ANN indexes anywhere.** The target builds vectors on the Mirror; the factory has no `FixedSizeList<float>` column and no ANN index across all 166 datasets. This is greenfield.

**G5 — Derived "masters" rebuild by full overwrite, not incrementally.** `sam_master.py` is a 128 GB cold-start full rebuild (its daily auto-refresh cron is disabled, PR #233) and `contractor_award_summary`/`ffata_exec_comp` rebuild daily from a static bulk — compute spent to reproduce unchanged, stale output (see G6). `merge_insert` is the intended replacement for all of these.

**G6 — Served federal-spend data is ~6 weeks stale despite green system timestamps.** `award_search`/`transaction_search_*` data frontier = 2026-04-23/24; a 2026-06-02 pull sits unreconciled in the landing tier. Daily-rebuilt derivatives report today's write time over April data — the staleness is invisible to any system-timestamp-only monitor.

**G7 — The two `financial_accounts_*` tables are completely unindexed** — `financial_accounts_by_awards` (454M rows, the largest dataset in the factory) and `financial_accounts_by_program_activity_object_class` (10.2M). Neither is in the spine campaign's target map; `usaspending_bulk.py`'s plan was unrun. Any lookup is a full scan.

**G8 — Dead-code / orphans to retire:** `pipelines/sam_gov/sam_entity_master.py` (script orphan, target dataset absent); 2 `overture_places__bak_*` snapshots (GC candidates). The `usaspending_api_landing` control-plane wiring is missing (G2).

### What the target requires that does not exist yet (build-order)

1. SAM daily-delta raw table (append-only) — the only entirely-missing **raw** tier.
2. USAspending daily-API → single appendable raw table + a scheduled Trigger task (promote the orphaned landing worker).
3. Per-feed `merge_insert` Mirror keyed on award/entity identity, fed by bulk + daily raw.
4. Re-home all BTREE indexes onto the Mirror; add `action_date`/`last_modified_date` + award-identity BTREEs (currently absent).
5. Add the vector column + ANN index on the Mirror (greenfield).
6. Retire the full-overwrite derivatives (`sam_master`, `contractor_award_summary`, …) once the Mirror subsumes them.

---

## Appendix A — Full inventory (166 leaf datasets)

`#idx` = index count · staleness in UTC · `vec` = has vector column (none do). Full per-column schema for every dataset is in the probe JSON artifact.

<!-- BEGIN_FULL_INVENTORY -->
| dataset | rows | #idx | system staleness (UTC) | date col | data staleness | vec |
|---|--:|--:|---|---|---|--:|
| `bridge_sam_fmcsa_domain` | 263,076 | 4 | 2026-06-01T19:21:01 | — | no-date-col |  |
| `bridge_sam_pdl` | 801,831 | 4 | 2026-06-01T19:21:08 | — | no-date-col |  |
| `ca_sos_agents` | 8,560,095 | 4 | 2026-05-31T18:43:22 | snapshot_date | 2026-05-31 |  |
| `ca_sos_entities` | 9,389,688 | 11 | 2026-05-31T18:44:00 | snapshot_date | 2026-05-31 |  |
| `ca_sos_principals` | 18,670,722 | 5 | 2026-05-31T18:46:03 | snapshot_date | 2026-05-31 |  |
| `ca_ucc/debtor_index` | 5,855,416 | 8 | 2026-05-31T18:36:03 | ingested_at | 2026-05-31 18:34:05 |  |
| `ca_ucc/debtors` | 5,855,416 | 9 | 2026-05-31T18:32:04 | ingested_at | 2026-05-31 18:30:04 |  |
| `ca_ucc/filing_amendments` | 3,305,823 | 3 | 2026-05-31T18:33:59 | ingested_at | 2026-05-31 18:33:28 |  |
| `ca_ucc/filings` | 7,751,890 | 5 | 2026-05-31T18:30:03 | ingested_at | 2026-05-31 18:28:36 |  |
| `ca_ucc/secured_parties` | 4,743,627 | 9 | 2026-05-31T18:33:27 | ingested_at | 2026-05-31 18:32:04 |  |
| `cms_general_payments` | 82,290,893 | 10 | 2026-06-06T17:07:48 | ingested_at | 2026-06-06 17:02:50 |  |
| `cms_ownership` | 27,480 | 8 | 2026-06-06T17:33:36 | ingested_at | 2026-06-06 17:33:34 |  |
| `cms_research_payments` | 5,936,454 | 10 | 2026-06-01T20:09:52 | ingested_at | 2026-06-01 20:09:27 |  |
| `co_sos` | 3,056,896 | 7 | 2026-05-31T18:26:20 | snapshot_date | 2026-05-31 |  |
| `co_ucc_transactions` | 2,555,824 | 9 | 2026-05-31T18:30:33 | snapshot_date | 2026-05-31 |  |
| `companies` | 758 | 1 | 2026-06-04T16:29:00 | — | no-date-col |  |
| `company_target_industries` | 2,050 | 3 | 2026-06-02T19:43:04 | — | no-date-col |  |
| `contractor_award_summary` | 578,958 | 1 | 2026-06-06T14:01:40 | prime_first_award_date | 2026-04-23 |  |
| `crosswalk_hmda_gleif` | 6,470 | 2 | 2026-06-06T04:00:44 | built_at | 2026-06-06 08:00:37 |  |
| `crosswalk_sam_usaspending` | 1,028,144 | 3 | 2026-06-06T12:04:05 | — | no-date-col |  |
| `crosswalk_sos_sam` | 941,838 | 6 | 2026-06-06T11:05:07 | — | no-date-col |  |
| `cslb_licenses` | 244,760 | 9 | 2026-06-01T01:16:59 | last_update_date | 2026-05-30 |  |
| `cslb_personnel` | 406,192 | 4 | 2026-06-01T01:16:47 | last_update_date | 2026-05-29 |  |
| `cslb_workers_comp` | 247,732 | 4 | 2026-06-01T01:16:41 | last_update_date | 2026-05-29 |  |
| `discovered_websets` | 5 | 3 | 2026-06-02T20:29:07 | snapshot_date | 2026-06-03 00:00:00 |  |
| `edgar_cik_map` | 10,365 | 4 | 2026-06-01T06:00:54 | snapshot_date | 2026-06-01 |  |
| `edgar_form_4` | 191,998 | 5 | 2026-06-01T02:13:37 | ingested_at | 2026-06-01 02:13:27 |  |
| `edgar_form_d` | 57,496 | 5 | 2026-06-01T02:14:27 | ingested_at | 2026-06-01 02:14:12 |  |
| `entity_registrations` | 19,299,314 | 3 | 2026-06-01T22:52:37 | last_update_date | 2026-05-03 |  |
| `epa_aim_triggering_events` | 5,375 | 2 | 2026-06-02T21:52:04 | MONITORING_PERIOD_TRIGGERED_STRT | 2026-01-01 |  |
| `epa_air_facilities` | 278,944 | 7 | 2026-06-05T19:41:01 | — | no-date-col |  |
| `epa_case_enforcements` | 135,053 | 2 | 2026-06-02T22:01:32 | ACTIVITY_STATUS_DATE | 2026-05-29 |  |
| `epa_case_milestones` | 508,088 | 2 | 2026-06-02T21:52:04 | ACTUAL_DATE | 2026-05-29 |  |
| `epa_entity_compliance` | 142,933 | 6 | 2026-06-06T00:45:41 | first_period | 2026-06-30 |  |
| `epa_facilities` | 3,240,591 | 2 | 2026-06-02T21:52:46 | — | no-date-col |  |
| `epa_npdes_dmrs` | 422,447,436 | 6 | 2026-06-06T01:12:08 | MONITORING_PERIOD_END_DATE | 2026-09-30 |  |
| `epa_npdes_eff_violations` | 46,361,587 | 2 | 2026-06-02T22:51:11 | MONITORING_PERIOD_END_DATE | 2026-12-31 |  |
| `epa_npdes_qncr_history` | 7,866,031 | 2 | 2026-06-02T21:52:27 | — | no-date-col |  |
| `epa_permit_compliance` | 156,014 | 9 | 2026-06-06T00:44:20 | first_period | 2026-06-30 |  |
| `epa_permit_parameter_compliance` | 1,884,617 | 8 | 2026-06-06T00:54:06 | first_period | 2026-09-30 |  |
| `epa_permits` | 1,686,705 | 8 | 2026-06-06T00:42:21 | ORIGINAL_ISSUE_DATE | 2060-01-01 |  |
| `epa_pipeline_caa` | 66,655 | 3 | 2026-06-02T21:52:05 | SORT_DATE | 2026-05-28 |  |
| `epa_pipeline_rcra` | 456,773 | 3 | 2026-06-02T21:52:16 | EVAL_DATE | 2026-05-29 |  |
| `epa_program_links` | 4,360,148 | 3 | 2026-06-02T21:53:15 | — | no-date-col |  |
| `epa_rcra_handlers` | 1,578,504 | 7 | 2026-06-05T19:41:08 | — | no-date-col |  |
| `epa_to_sos_bridge` | 406,191 | 2 | 2026-06-06T00:33:31 | sos_candidate_count | — |  |
| `epiq_cases` | 946 | 6 | 2026-06-01T15:15:25 | ingested_at | 2026-06-01 15:15:17 |  |
| `epiq_claims` | 605,236 | 7 | 2026-06-01T19:48:43 | ingested_at | 2026-06-01 19:48:07 |  |
| `epiq_dockets` | 763,318 | 7 | 2026-06-01T15:34:36 | ingested_at | 2026-06-01 15:33:24 |  |
| `fec_individual_contributions` | 282,923,196 | 16 | 2026-06-06T00:41:29 | ingested_at | 2026-06-01 19:21:36 |  |
| `ffata_exec_comp` | 29,601 | 4 | 2026-06-06T13:02:39 | disclosure_action_date | 2026-04-23 |  |
| `firmographics_blitz` | 142,638 | 6 | 2026-06-06T12:06:12 | source_updated_at | 2026-06-06 11:43:22 |  |
| `fl_federal_tax_liens` | 22,519 | 3 | 2026-06-06T00:32:24 | ingested_at | 2026-06-06 00:32:17 |  |
| `fl_sos_corporations` | 1,260,599 | 5 | 2026-06-05T23:37:57 | snapshot_date | 2026-05-31 |  |
| `fl_sos_events` | 14,455,118 | 2 | 2026-05-31T19:59:24 | snapshot_date | 2026-05-31 |  |
| `fmcsa/auth_hist` | 15,830 | 2 | 2026-06-06T13:33:55 | snapshot_date | 2026-06-06 |  |
| `fmcsa/boc3` | 5,369 | 2 | 2026-06-06T13:33:56 | snapshot_date | 2026-06-06 |  |
| `fmcsa/carrier` | 5,369 | 2 | 2026-06-06T13:33:55 | snapshot_date | 2026-06-06 |  |
| `fmcsa/census` | 4,448,677 | 1 | 2026-06-06T13:44:37 | snapshot_date | 2026-06-06 |  |
| `fmcsa/census_mail_ready` | 4,437,561 | 6 | 2026-06-01T19:35:25 | snapshot_date | 2026-06-01 |  |
| `fmcsa/insurance` | 5,803 | 1 | 2026-06-06T13:33:58 | snapshot_date | 2026-06-06 |  |
| `fmcsa/oos` | 390,330 | 1 | 2026-06-06T13:34:39 | snapshot_date | 2026-06-06 |  |
| `fmcsa/revocation` | 1,803 | 2 | 2026-06-06T13:33:57 | snapshot_date | 2026-06-06 |  |
| `gleif_l1_entities` | 3,332,281 | 1 | 2026-06-06T02:09:09 | ingested_at | 2026-06-06 02:00:37 |  |
| `gleif_l2_relationships` | 475,082 | 2 | 2026-06-06T02:01:18 | ingested_at | 2026-06-06 02:00:37 |  |
| `hmda_lar` | 168,296,950 | 4 | 2026-06-01T20:36:26 | ingested_at | 2026-06-01 19:16:08 |  |
| `hmda_panels` | 52,009 | 2 | 2026-06-01T19:32:26 | ingested_at | 2026-06-01 19:21:05 |  |
| `msha_accidents` | 273,065 | 15 | 2026-06-05T22:40:52 | ingested_at | 2026-06-05 20:31:17 |  |
| `msha_contractors` | 1,630,676 | 5 | 2026-06-05T22:40:40 | ingested_at | 2026-06-05 20:31:03 |  |
| `msha_corporate_history` | 168,809 | 9 | 2026-06-05T22:40:29 | ingested_at | 2026-06-02 21:32:49 |  |
| `msha_enforcement_ledger` | 3,076,347 | 16 | 2026-06-05T22:41:59 | ingested_at | 2026-06-02 21:33:18 |  |
| `msha_mines` | 91,803 | 13 | 2026-06-05T22:40:18 | ingested_at | 2026-06-02 21:32:34 |  |
| `nmls_mcr_applications_received` | 2,746 | 3 | 2026-06-01T18:51:49 | snapshot_date | 2026-06-01 |  |
| `nmls_mcr_forward_by_business_line` | 8,130 | 4 | 2026-06-01T18:51:46 | snapshot_date | 2026-06-01 |  |
| `nmls_mcr_forward_by_purpose` | 8,181 | 4 | 2026-06-01T18:51:44 | snapshot_date | 2026-06-01 |  |
| `nmls_mcr_forward_by_type` | 10,836 | 4 | 2026-06-01T18:51:44 | snapshot_date | 2026-06-01 |  |
| `nmls_mcr_license_activity` | 11,240 | 4 | 2026-06-01T18:51:43 | snapshot_date | 2026-06-01 |  |
| `nmls_mcr_reverse_by_business_line` | 7,390 | 4 | 2026-06-01T18:51:46 | snapshot_date | 2026-06-01 |  |
| `nmls_state_entity_counts` | 59 | 2 | 2026-06-01T18:51:45 | snapshot_date | 2026-06-01 |  |
| `nppes/snapshot=2026-05` | 9,551,447 | 3 | 2026-06-01T02:22:06 | last_update_date | — |  |
| `nppes_provider/snapshot=2026-05` | 9,551,447 | 11 | 2026-06-06T16:07:36 | last_update_date | 2026-05-11 |  |
| `nppes_provider_identifier/snapshot=2026-05` | 2,759,800 | 4 | 2026-06-06T16:07:53 | — | no-date-col |  |
| `nppes_provider_taxonomy/snapshot=2026-05` | 11,952,809 | 4 | 2026-06-06T16:07:49 | — | no-date-col |  |
| `nppes_taxonomy_ref` | 883 | 3 | 2026-06-06T17:25:33 | — | no-date-col |  |
| `ny_sos` | 4,219,360 | 9 | 2026-05-31T18:50:32 | snapshot_date | 2026-05-31 |  |
| `osha_daily_triggers` | 1,758 | 5 | 2026-06-06T09:01:22 | snapshot_date | 2026-06-06 |  |
| `overture_places` | 16,273,123 | 9 | 2026-06-06T18:09:06 | — | no-date-col |  |
| `overture_places__bak_2026-05-20.0_20260606T192125Z` | 16,273,123 | 7 | 2026-06-05T02:28:36 | snapshot_date | 2026-06-05 |  |
| `overture_places__bak_v3_2026-05-20.0_20260606T220113Z` | 16,273,123 | 7 | 2026-06-06T15:22:08 | — | no-date-col |  |
| `pdl_companies` | 35,446,771 | 10 | 2026-05-31T19:55:51 | snapshot_date | 2026-05-31 |  |
| `pdl_normalized_companies` | 35,446,771 | 6 | 2026-06-06T12:01:29 | built_at | 2026-06-06 11:58:42 |  |
| `people` | 7,740 | 2 | 2026-06-04T14:31:41 | — | no-date-col |  |
| `ppp` | 11,468,210 | 11 | 2026-06-02T10:29:05 | snapshot_date | 2024-09-30 |  |
| `sam-gov-opps/active` | 79,211 | 0 | 2026-06-06T08:00:52 | posted_date | 2026-06-05 22:54:15 |  |
| `sam_master_contacts` | 4,373,319 | 1 | 2026-06-06T15:41:32 | — | no-date-col |  |
| `sam_master_domains` | 709,546 | 2 | 2026-06-06T15:41:42 | — | no-date-col |  |
| `sam_master_entities` | 1,541,566 | 3 | 2026-06-06T15:41:07 | last_update_date | 2026-05-03 |  |
| `sam_normalized_entities` | 1,541,566 | 6 | 2026-06-06T15:55:53 | — | no-date-col |  |
| `sam_opps_attachment_manifest` | 331,401 | 6 | 2026-06-06T17:57:47 | posted_date | 2026-06-05 22:54:15 |  |
| `sam_pocs` | 8,065,679 | 6 | 2026-06-06T12:59:38 | — | no-date-col |  |
| `sba_504` | 227,404 | 15 | 2026-06-02T10:23:33 | ingested_at | 2026-05-31 17:32:40 |  |
| `sba_7a` | 1,947,098 | 15 | 2026-06-02T10:28:25 | ingested_at | 2026-05-31 17:33:35 |  |
| `sec_adv_part1` | 36,846 | 3 | 2026-06-01T09:01:05 | snapshot_date | 2026-06-01 |  |
| `sec_adv_w` | 21,076 | 2 | 2026-06-01T09:00:44 | snapshot_date | 2026-06-01 |  |
| `shovels_tags` | 22 | 1 | 2026-06-01T23:07:46 | snapshot_date | 2026-06-02 00:00:00 |  |
| `sos_normalized_master` | 17,926,543 | 4 | 2026-06-05T23:40:59 | snapshot_date | 2026-05-31 |  |
| `ucc_co_collateral` | 1,682,948 | 5 | 2026-06-02T07:41:31 | snapshot_date | 2026-05-08 |  |
| `ucc_co_debtors` | 1,985,901 | 8 | 2026-06-02T07:39:57 | snapshot_date | 2026-05-08 |  |
| `ucc_co_secured_parties` | 2,055,777 | 8 | 2026-06-02T07:40:53 | snapshot_date | 2026-05-08 |  |
| `usaspending/agency` | 1,530 | 0 | 2026-06-01T12:20:37 | ingested_at | 2026-06-01 12:20:36 |  |
| `usaspending/appropriation_account_balances` | 627,988 | 0 | 2026-06-01T02:22:21 | last_modified_date | — |  |
| `usaspending/award_category` | 14 | 0 | 2026-06-01T12:20:40 | ingested_at | 2026-06-01 12:20:40 |  |
| `usaspending/award_search` | 78,373,286 | 3 | 2026-06-01T19:26:05 | last_modified_date | 2026-04-24 23:48:18 |  |
| `usaspending/budget_authority` | 7,661 | 0 | 2026-06-01T02:22:08 | ingested_at | 2026-06-01 02:22:07 |  |
| `usaspending/bureau_title_lookup` | 5,496 | 0 | 2026-06-01T02:22:10 | ingested_at | 2026-06-01 02:22:09 |  |
| `usaspending/cgac` | 192 | 0 | 2026-06-01T12:20:38 | ingested_at | 2026-06-01 12:20:38 |  |
| `usaspending/covid_faba_spending` | 3,443 | 0 | 2026-06-01T02:22:08 | ingested_at | 2026-06-01 02:22:06 |  |
| `usaspending/dabs_submission_window_schedule` | 112 | 0 | 2026-06-01T12:20:39 | ingested_at | 2026-06-01 12:20:39 |  |
| `usaspending/disaster_emergency_fund_code` | 48 | 0 | 2026-06-01T12:20:39 | ingested_at | 2026-06-01 12:20:39 |  |
| `usaspending/duns` | 2,915,289 | 0 | 2026-06-01T02:22:18 | ingested_at | 2026-06-01 02:22:01 |  |
| `usaspending/federal_account` | 3,436 | 0 | 2026-06-01T02:22:06 | ingested_at | 2026-06-01 02:22:05 |  |
| `usaspending/financial_accounts_by_awards` | 454,215,610 | 0 | 2026-06-01T03:47:10 | last_modified_date | — |  |
| `usaspending/financial_accounts_by_program_activity_object_class` | 10,235,016 | 0 | 2026-06-01T02:22:55 | last_modified_date | — |  |
| `usaspending/frec` | 166 | 0 | 2026-06-01T12:20:38 | ingested_at | 2026-06-01 12:20:38 |  |
| `usaspending/frec_map` | 13,464 | 0 | 2026-06-01T02:22:06 | ingested_at | 2026-06-01 02:22:05 |  |
| `usaspending/gtas_sf133_balances` | 968,328 | 0 | 2026-06-01T02:22:06 | ingested_at | 2026-06-01 02:22:01 |  |
| `usaspending/historic_parent_duns` | 3,198,417 | 0 | 2026-06-01T02:22:12 | ingested_at | 2026-06-01 02:22:01 |  |
| `usaspending/historical_appropriation_account_balances` | 249,643 | 0 | 2026-06-01T02:22:15 | ingested_at | 2026-06-01 02:22:05 |  |
| `usaspending/naics` | 1,741 | 0 | 2026-06-01T12:19:34 | ingested_at | 2026-06-01 12:19:34 |  |
| `usaspending/object_class` | 105 | 0 | 2026-06-01T12:20:38 | ingested_at | 2026-06-01 12:20:38 |  |
| `usaspending/office` | 86,510 | 0 | 2026-06-01T02:22:06 | updated_at | 2026-04-25 00:50:36 |  |
| `usaspending/overall_totals` | 141 | 0 | 2026-06-01T12:20:39 | ingested_at | 2026-06-01 12:20:39 |  |
| `usaspending/parent_award` | 987,705 | 0 | 2026-06-01T02:22:05 | ingested_at | 2026-06-01 02:22:02 |  |
| `usaspending/program_activity_park` | 9,115 | 0 | 2026-06-01T02:22:05 | ingested_at | 2026-06-01 02:22:04 |  |
| `usaspending/psc` | 3,836 | 0 | 2026-06-01T02:22:07 | ingested_at | 2026-06-01 02:22:06 |  |
| `usaspending/recipient_lookup` | 17,754,022 | 2 | 2026-06-01T19:19:44 | ingested_at | 2026-06-01 02:22:07 |  |
| `usaspending/recipient_profile` | 18,275,944 | 2 | 2026-06-01T19:19:04 | ingested_at | 2026-06-01 02:22:04 |  |
| `usaspending/ref_city_county_state_code` | 202,520 | 0 | 2026-06-01T02:22:07 | ingested_at | 2026-06-01 02:22:03 |  |
| `usaspending/ref_country_code` | 260 | 0 | 2026-06-01T12:20:37 | ingested_at | 2026-06-01 12:20:37 |  |
| `usaspending/ref_population_cong_district` | 441 | 0 | 2026-06-01T12:20:39 | ingested_at | 2026-06-01 12:20:38 |  |
| `usaspending/ref_population_county` | 3,290 | 0 | 2026-06-01T02:22:04 | ingested_at | 2026-06-01 02:22:03 |  |
| `usaspending/ref_program_activity` | 79,173 | 0 | 2026-06-01T02:22:04 | ingested_at | 2026-06-01 02:22:03 |  |
| `usaspending/references_cfda` | 4,149 | 0 | 2026-06-01T02:22:05 | ingested_at | 2026-06-01 02:22:03 |  |
| `usaspending/references_definition` | 151 | 0 | 2026-06-01T12:20:37 | ingested_at | 2026-06-01 12:20:37 |  |
| `usaspending/reporting_agency_missing_tas` | 304,969 | 0 | 2026-06-01T02:22:04 | ingested_at | 2026-06-01 02:22:03 |  |
| `usaspending/reporting_agency_overview` | 10,989 | 0 | 2026-06-01T02:22:05 | ingested_at | 2026-06-01 02:22:04 |  |
| `usaspending/reporting_agency_tas` | 626,552 | 0 | 2026-06-01T02:22:06 | ingested_at | 2026-06-01 02:22:03 |  |
| `usaspending/rosetta` | 1 | 0 | 2026-06-01T11:30:50 | ingested_at | 2026-06-01 11:30:49 |  |
| `usaspending/state_data` | 448 | 0 | 2026-06-01T12:20:36 | ingested_at | 2026-06-01 12:20:36 |  |
| `usaspending/subaward_search` | 9,801,723 | 6 | 2026-06-01T19:20:50 | last_modified_date | 2026-04-24 22:31:54 |  |
| `usaspending/submission_attributes` | 7,532 | 0 | 2026-06-01T02:22:05 | ingested_at | 2026-06-01 02:22:04 |  |
| `usaspending/subtier_agency` | 1,490 | 0 | 2026-06-01T12:20:36 | ingested_at | 2026-06-01 12:20:36 |  |
| `usaspending/summary_state_view` | 15,971 | 0 | 2026-06-01T13:02:52 | ingested_at | 2026-06-01 13:02:33 |  |
| `usaspending/toptier_agency` | 198 | 0 | 2026-06-01T12:20:37 | ingested_at | 2026-06-01 12:20:37 |  |
| `usaspending/transaction_search_fabs` | 128,784,183 | 4 | 2026-06-01T20:47:34 | last_modified_date | 2026-04-24 23:48:18 |  |
| `usaspending/transaction_search_fpds` | 107,250,527 | 4 | 2026-06-01T19:34:55 | last_modified_date | 2026-04-23 23:56:31 |  |
| `usaspending/treasury_appropriation_account` | 25,544 | 0 | 2026-06-01T02:22:07 | ingested_at | 2026-06-01 02:22:06 |  |
| `usaspending/uei_crosswalk` | 3,323,130 | 0 | 2026-06-01T02:22:08 | ingested_at | 2026-06-01 02:22:02 |  |
| `usaspending/uei_crosswalk_2021` | 3,279,911 | 0 | 2026-06-01T02:22:09 | ingested_at | 2026-06-01 02:22:01 |  |
| `usaspending/zips_grouped` | 53,646 | 0 | 2026-06-01T02:22:05 | updated_at | 2026-04-18 17:27:42 |  |
| `usaspending_api_catalog` | 176 | 1 | 2026-06-04T19:31:02 | fetched_at | 2026-06-04 23:29:28 |  |
| `usaspending_api_landings/award_search/pull_date=2026-06-04` | 583,776 | 0 | 2026-06-04T16:56:46 | last_modified_date | 2026-06-02 23:56:52 |  |
| `uspto_tm_applications` | 66,331 | 5 | 2026-06-06T08:01:23 | registration_date | 2026-06-16 |  |
| `uspto_tm_assignments` | 1,557,545 | 5 | 2026-05-31T19:34:52 | last_update_date | 2026-04-09 |  |
| `uspto_tm_assignments_historical` | 1,380,594 | 4 | 2026-06-01T15:53:26 | last_update_date | 2024-02-02 |  |
| `uspto_tm_ttab` | 156,261 | 4 | 2026-06-06T08:00:44 | ingested_at | 2026-06-06 08:00:33 |  |
<!-- END_FULL_INVENTORY -->
