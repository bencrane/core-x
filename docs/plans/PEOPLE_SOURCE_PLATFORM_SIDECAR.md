# Design — Extract `source_platform` into a `person_source_platforms` sidecar; canonicalize `active/people`

**Status:** Design / investigation only. Zero writes performed to Lance or Postgres.
**Date:** 2026-07-02
**Datasets in scope:** `s3://data-sink/active/people/` (v67, 116,837 rows) → new `s3://data-sink/active/person_source_platforms/` (sidecar) + rebuilt canonical `active/people`.

---

## 0. TL;DR / RECOMMENDATION

- **`person_id` is NOT a human-canonical key.** It is *source-specific*. The same human is loaded under **different** `person_id`s by different writers because every writer derives `person_id` differently (source-native UUID, upstream Clay id, `sha256(name-key)`, `md5`, `uuid5`). Evidence: **11,759 normalized LinkedIn URLs map to 2–8 distinct `person_id`s** (30,308 rows); the reverse is clean (`person_id` → exactly 1 URL, 0 exceptions). So `person_id` is unique 1:1 on rows today only because it is *manufactured per-source*, not because it identifies a human.
- **De-dupe is REAL, not trivial.** Collapsing `active/people` to one row per human is genuine entity resolution over LinkedIn URL: **114,013 with-URL rows collapse to 95,461 canonical people (18,552 duplicate human-rows merge).** A sidecar over `person_id` alone would be pure provenance and *would not* fix the duplication — the operator decision correctly makes the canonical key the **normalized LinkedIn URL**, not `person_id`.
- **Target architecture (recommended):**
  1. New **`canonical_person_id = sha256(canonical_linkedin_url)`** for the 97.6% of rows with a URL; a stable fallback id for the 2.4% without.
  2. **`active/people`** rebuilt to **1 row per canonical person** (95,461 URL-canonical + 2,824 null-URL = **98,285 rows**), `source_platform` column **dropped**.
  3. New append-only, idempotent **`active/person_source_platforms`** sidecar at grain **(canonical_person_id × source_platform)** that captures **every** current `(person_id → source_platform)` mapping — so the up-to-8-person_ids-per-URL cases become up-to-8 sidecar rows under one canonical person. Backfill = 116,837 sidecar rows; then land the `dsbs-pocs-match` cohort as **12,353 net-new sidecar rows, zero writes to `people`.**
  4. Keep the legacy `person_id → canonical_person_id` map (either as a column on the sidecar or a tiny `person_id_map` dataset) so existing external `person_id` references still resolve.
- **Blast radius:** one physical consumer breaks on schema (`catalyst_api` person-by-linkedin projects `source_platform`), two MCP consumers project it in their documented contract (`gtm_mcp` audience + batch_lookups), and ~8 writer pipelines must stop writing `source_platform` into `people` and instead append to the sidecar. No `source_platform` **filter** exists on `people` anywhere — every use is projection/lineage, which softens the break to a contract change, not a query rewrite.

---

## 1. person_id derivation across sources — THE crux (RESOLVED: divergent)

### 1.1 Live measurements (`active/people` v67, 116,837 rows)

| Metric | Value |
|---|---|
| rows | 116,837 |
| distinct `person_id` | 116,837 (strict 1:1 — 0 null) |
| distinct `person_id` → >1 `person_linkedin_url` | **0** (reverse is clean) |
| rows with a non-null `person_linkedin_url` | 114,013 |
| **normalized LinkedIn URLs → >1 `person_id`** | **11,759 URLs, 30,308 rows** (coordinator norm) / 10,827 URLs, 27,473 rows (basic norm) |
| distinct canonical people (URL-dedup) | **95,461** |
| duplicate human-rows that merge | **18,552** |

`person_id` length is a fingerprint of the derivation scheme, and it is **not uniform**:

| length | count | scheme |
|---|---|---|
| 36 | 67,061 | UUID (source-native or `uuid5`) |
| 64 | 35,348 | `sha256(...)` hex |
| 32 | 14,407 | `md5(...)` hex |
| 47 / 18 / 11 | 21 | dirty upstream ids |

### 1.2 How each writer derives `person_id` (grepped, with file:line)

| source_platform | writer | `person_id` derivation | id shape |
|---|---|---|---|
| `dexarchive_staffing_agencies` | `pipelines/gtm/backfill_staffing_agencies_people.py:109` (`CAST(id AS VARCHAR)`) | **source-native dex UUID** (`target_people.id`) | 36-char UUID |
| `clay_find_people` | `pipelines/gtm/materialize_clay_find_people.py:73` (projects `person_id` straight from `gtm.clay_find_people`) + `backfill_dsbs_clay_mobile_people.py:126` | **upstream Clay `person_id`** (copied verbatim; docstring *claims* `sha256(linkedin_url_norm)` at `materialize_clay_find_people.py:34` but the value is passed through from Postgres) | 36-char UUID (+3,609 rows 64-char from a mixed clay cohort) |
| `dsbs_poc` | `pipelines/gtm/backfill_dsbs_poc_people.py` ← `active/dsbs_poc_people.person_id` produced by `pipelines/sba_dsbs/materialize_dsbs_poc_people.py:130-152` | person_id taken from the **matched clay/people row** inside the name-within-uei join (`materialize_dsbs_poc_people.py:135-152`); the 64-char ids are the `sha256`-style ids carried from the people/work-email side. Spot-checked: NOT raw `sha256(linkedin)` under any obvious normalization — the norm is internal. | 64-char sha256 (11,949) + 38 UUID + 19 dirty |
| `work_emails` | `pipelines/work_emails/land_supplied_work_emails.py` (contact_id = supplied person_id; `:12`) | **supplied verbatim** (upstream `contact_id`); mixed `md5`/`sha256` from the vendor | 64-char (19,056) + 32-char md5 (14,407) |
| `blitz_find_people` | `pipelines/gtm/materialize_blitz_find_people.py` | upstream Blitz `person_id` | 36-char UUID |
| `title_enrichment` | `pipelines/gtm/backfill_people_from_title_enrichment.py:111` | **`uuid5(app-namespace, person_linkedin_url_norm)`** — deterministic, LinkedIn-derived, but a *different scheme* than every other source | 36-char UUID |
| `sfnet` / `csv_2026_05_23` / `elfa` / `manual-seed` / `phone_resolution_orphan` / `work_emails_sfnet_dm` | various one-off backfills | source-native ids / uuid | 36 & 64 char |

**Conclusion (the crux):** because each writer manufactures `person_id` under a *different* function, two sources that find the same LinkedIn person emit **different** `person_id`s. The only cross-source-stable human key present in the data is the **LinkedIn URL** (once normalized). Hence:
- De-dupe is a **real entity-resolution task**, not a no-op.
- The correct canonical key is **normalized `person_linkedin_url`**, per the operator decision.
- Fan-out distribution (person_ids per canonical URL): 1→83,701 · 2→7,547 · 3→2,501 · 4→1,098 · 5→413 · 6→157 · 7→36 · 8→8.

---

## 2. Writers of `active/people`

Every pipeline that mutates `active/people`, its write mode, and how it sets `source_platform`:

| # | Writer (file) | Mode | source_platform set to | Notes |
|---|---|---|---|---|
| 1 | `pipelines/gtm/backfill_staffing_agencies_people.py:166` | `append` | `'dexarchive_staffing_agencies'` literal (`:133`) | idempotent anti-join on `person_id`; reindexes 4 BTREE |
| 2 | `pipelines/gtm/backfill_dsbs_poc_people.py:158` | `append` | `'dsbs_poc'` literal (`:124`) | idempotent anti-join on `person_id` |
| 3 | `pipelines/gtm/backfill_dsbs_clay_mobile_people.py:158` | `append` | `'clay_find_people'` literal (`:128`) | idempotent anti-join on `person_id` |
| 4 | `pipelines/gtm/backfill_people_from_title_enrichment.py:128` | `merge_insert(person_id).when_not_matched_insert_all` | `'title_enrichment'` literal (`:119`) | insert-only; deterministic `uuid5` id |
| 5 | `pipelines/gtm/enrich_staffing_people_title_from_clay.py:132` | `merge_insert(person_id).when_matched_update_all` | *unchanged* (carries existing) | **title-only update**, row count invariant; touches no source_platform |
| 6 | `pipelines/gtm/backfill_people_person_linkedin_from_contacts.py:126` | **`overwrite`** | *unchanged* (carries existing) | fills NULL `person_linkedin_url` in place; **full overwrite** — rebuilds all indices |
| 7 | `pipelines/gtm/companies_people_bulk.py` (retired ingest path) | `overwrite` | from `source` col (`:288`) | historical bulk builder; the canonical INDEXES/DATASET_URI source of truth (`:160,:180`) |
| — | `pipelines/gtm/materialize_clay_find_people.py`, `materialize_blitz_find_people.py`, `materialize_dsbs_poc_people.py` | write their OWN `active/<x>` datasets, NOT `people` | — | upstream of writers 1–4 |

**Canonical index plan** (`companies_people_bulk.py:194-197`): `people` carries `BTREE [person_id, company_id, normalized_domain, person_linkedin_url]` + `BITMAP [verification_status]`. Live dataset confirmed 4 BTREE indices (`person_id_idx, company_id_idx, normalized_domain_idx, person_linkedin_url_idx`). (The live v67 schema is the 9-col form; the `verification_status`/work-email drift columns referenced in code are the wide form that some writers assemble against `ds.schema` at runtime — the sidecar plan must read the live schema, not a hardcoded list.)

---

## 3. Readers / consumers of `active/people` and `source_platform`

### 3.1 Consumers that PROJECT `source_platform` (would break on removal)

| Consumer | File:line | Uses `source_platform` how | Break class |
|---|---|---|---|
| `catalyst_api` person-by-linkedin | `apps/catalyst_api/src/lance_store.py:654-657` (`_PEOPLE_COLS`), `models.py:675,692` (`PersonMatch.source_platform`) | projects `source_platform` in `columns=` and serializes it in the API response | **Physical break** — scanner names a column that no longer exists → error. Must drop from `_PEOPLE_COLS`, populate from sidecar, or return null. |
| `gtm_mcp` audience | `apps/gtm_mcp/src/tools/audience.py:77` (`_PEOPLE_COLUMNS`), `:135,:207` (docstring + SQL) | projects `source_platform` as documented people-column contract | **Contract break** — same fix. |
| `gtm_mcp` batch_lookups | `apps/gtm_mcp/src/tools/batch_lookups.py:192` (docstring) + shared `_search_by_domains` | documents `source_platform` in people projection | **Contract break.** |
| tests | `apps/catalyst_api/tests/test_person_by_linkedin.py:66`, `apps/gtm_mcp/tests/test_batch_lookups.py`, `test_lookup_cache.py:220`, `scripts/benchmarks/gtm-mcp-lance-retrieval-opt.py` | fixtures assert `source_platform` key present | update fixtures. |

### 3.2 Consumers that FILTER on `source_platform` — of `people`: **NONE**

Every `WHERE source_platform ...` / `filter="source_platform = ..."` hit is on **`companies`** or other datasets (`pipelines/serving/materialize_firm_construction_proximity.py`, `probe_proximity_tam.py`, `build_sfnet_main_contacts.py:208-216`, `backfill_*_companies.py`, `exa_websets/ingest.py`), **not on `people`**. The `people` writers filter `source_platform` only within their own verify/coverage steps (e.g. `backfill_dsbs_poc_people.py:171`, `enrich_staffing_people_title_from_clay.py:76`) — those move to the sidecar. So **no production query path filters people by source** — the split is a projection/contract change, not a query rewrite. This materially lowers blast radius.

### 3.3 Other people consumers (do NOT touch source_platform — safe)
- `catalyst_api` person-by-linkedin lookup uses BTREE `person_linkedin_url` (`lance_store.py:697-710`) — unaffected by the source split; benefits from the URL becoming canonical.
- `gtm_mcp` `search_people_by_domain` / `search_people_by_domains` use BTREE `normalized_domain` (`audience.py:127-156`, `batch_lookups.py`) — must drop `source_platform` from projection only.
- `pipelines/sba_dsbs/materialize_dsbs_poc_people.py:112` and `build_sfnet_main_contacts.py:200` read `people` columns that don't include `source_platform` — unaffected.

---

## 4. The sidecar Lance — `active/person_source_platforms`

**Name:** `s3://data-sink/active/person_source_platforms/`
**Grain:** one row per **(canonical_person_id × source_platform × legacy_person_id)** — i.e. it preserves EVERY current `(person_id → source_platform)` mapping while grouping them under the canonical human. A canonical person with 8 source person_ids yields up to 8 rows (fewer if several share a source_platform).

**Schema:**

| column | type | nullable | notes |
|---|---|---|---|
| `canonical_person_id` | string | no | `sha256(canonical_linkedin_url)` (URL people) or `sha256('name+domain:' || key)` fallback (null-URL). **BTREE.** |
| `source_platform` | string | no | the source tag (`dsbs_poc`, `clay_find_people`, `dsbs-pocs-match`, …). **BITMAP.** |
| `legacy_person_id` | string | yes | the ORIGINAL per-source `person_id` this mapping came from (preserves external references; the value external systems currently hold). **BTREE.** |
| `person_linkedin_url_norm` | string | yes | canonical URL the id was derived from (null for null-URL fallback people). **BTREE** (bridge to people). |
| `first_seen_at` | timestamp(us, UTC) | no | when this (person,source) pair first landed. Set to `now()` on backfill/first insert; **never updated** (append-only provenance). |
| `source_ref` | string | yes | free-form note / cohort ref (e.g. CSV filename `dsbs_poc_people_clay.csv`, run id). |

**Idempotency / write mode:** append-only. Idempotent via
`merge_insert(["canonical_person_id","source_platform","legacy_person_id"]).when_not_matched_insert_all()`.
Re-running any writer never duplicates a pair and never disturbs `first_seen_at`. A brand-new source tag for an already-known human (the `dsbs-pocs-match` case) is a pure net-new insert.

**Indexes:** `BTREE [canonical_person_id, legacy_person_id, person_linkedin_url_norm]` + `BITMAP [source_platform]` (source_platform is a ~12-value low-cardinality enum → BITMAP is the right pushdown for "all people from source X").

**Why this grain (vs person_id-only):** a sidecar keyed only on `person_id` would faithfully record provenance but would leave `active/people` still carrying 18,552 duplicate human-rows. Keying the sidecar on `canonical_person_id` while retaining `legacy_person_id` is what lets `people` collapse to one row per human AND keeps every historical id resolvable.

---

## 5. `active/people` after refactor

- **1 row per canonical person.** Rebuilt count: **98,285** (95,461 URL-canonical + 2,824 null-URL kept as singletons). Down from 116,837 (−18,552 merged duplicates).
- **`source_platform` column: DROPPED.** It moves wholesale to the sidecar.
- **New PK: `canonical_person_id`** (BTREE). Retain `person_linkedin_url` (verbatim canonical form, BTREE — the catalyst_api lookup depends on it) and `normalized_domain`, `company_id`, name/title columns.
- **`primary_source` — RECOMMENDED: add a derived `primary_source` string** on `people` (denormalized convenience) computed by a deterministic priority order so single-value consumers that want "the" source keep working without a sidecar join. Suggested priority (highest-trust identity first): `dsbs_poc > clay_find_people > blitz_find_people > work_emails > dexarchive_staffing_agencies > sfnet > title_enrichment > csv_2026_05_23 > work_emails_sfnet_dm > phone_resolution_orphan > elfa > manual-seed`. `primary_source` is a lossy convenience; the sidecar remains the full truth. (If a strict-minimal `people` is preferred, omit it — but then the two MCP projections must join the sidecar.)
- **Field merge policy when collapsing duplicates** (needed because 18,552 rows merge): for each canonical person choose non-null field values by the same source-priority order (COALESCE in priority sequence) so `title`/`company_id`/`normalized_domain`/name come from the highest-trust contributing row. This is deterministic and re-derivable.

### 5.1 Null-URL fallback (the 2.4% without a LinkedIn URL) — quantified
- 2,824 null-URL rows. Of these **2,770 have both name + normalized_domain**; 54 have no domain; 0 have no name.
- `(name, domain)` collisions among null-URL rows: only **348 rows** would merge (348 groups with >1 person_id). i.e. the null-URL set is almost entirely singletons.
- **Recommendation:** keep null-URL rows as **singletons** in `people` with `canonical_person_id = sha256('nd:' || lower(full_name) || '|' || lower(normalized_domain))` where both present, else `sha256('pid:' || legacy_person_id)` (degenerate but stable). Do NOT attempt name+domain fuzzy merge for the 348 — the payoff (348 rows) is not worth the false-merge risk on names. Sidecar rows for null-URL people carry `person_linkedin_url_norm = NULL` and are keyed on the same fallback `canonical_person_id`.

---

## 6. Migration plan (append-only / idempotent; investigation-only here)

All steps below are the **proposed** sequence. Nothing has been executed.

**Phase A — build the sidecar (no change to `people` yet; fully reversible)**
1. Read `active/people` v67 (116,837 rows).
2. Compute `canonical_linkedin_url` + `canonical_person_id` per row (URL norm spec in §7.1; fallback per §5.1).
3. Emit one sidecar row per existing `(person_id, source_platform)` → `person_source_platforms`:
   `canonical_person_id`, `source_platform`, `legacy_person_id = person_id`, `person_linkedin_url_norm`, `first_seen_at = now()`, `source_ref = 'backfill:people_v67'`.
   Write `mode="overwrite"` for the initial build (single deterministic snapshot), then switch to `merge_insert` for all subsequent writers. Result: **116,837 sidecar rows**, distinct `canonical_person_id` ≈ 98,285.
4. Build BTREE + BITMAP indices per §4.
5. **Verify:** every `people.person_id` appears as exactly one `legacy_person_id`; `count(distinct canonical_person_id)` == 98,285; each `source_platform` bucket count matches the §"Established facts" distribution.

**Phase B — land the `dsbs-pocs-match` cohort into the sidecar ONLY (zero writes to `people`)**
6. Read `exports/dsbs_poc_people_clay.csv` (12,353 rows; all 12,353 `person_id`s confirmed already in `people`, net-new = 0 — verified live).
7. For each CSV row, resolve `canonical_person_id` by joining its `person_id`/`person_linkedin_url` through the sidecar's `legacy_person_id` / `person_linkedin_url_norm`. (634 of the CSV's LinkedIn URLs already map to a *different* person_id in people — the ER collapse in Phase A puts them under the same canonical person, which is exactly the desired behavior.)
8. `merge_insert(["canonical_person_id","source_platform","legacy_person_id"])` with `source_platform = 'dsbs-pocs-match'`, `source_ref = 'dsbs_poc_people_clay.csv'`, `first_seen_at = now()`. → **≤12,353 net-new sidecar rows, 0 writes to `people`.** Idempotent on re-run.
9. **Verify:** `filter="source_platform = 'dsbs-pocs-match'"` returns the cohort; `people.count_rows()` unchanged.

**Phase C — canonicalize `active/people` (the higher-risk step; gate behind Phase A/B success)**
10. Build the collapsed `people` (98,285 rows, `source_platform` dropped, `canonical_person_id` PK, `primary_source` derived, field-merge per §5) into a **new URI** (`active/people_canonical/`) via `mode="overwrite"`. Do NOT overwrite `active/people` in place first.
11. Build indices, verify counts + that every canonical person resolves from every consumer probe.
12. **Cutover:** repoint `PEOPLE_URI` (env `PEOPLE_LANCE_URI` / `GTM_PEOPLE_URI`) or atomically swap the dataset. Keep v67 of `active/people` retained (Lance versioning) for rollback.

**Phase D — writer + consumer changes** (see §7 below). Ship consumer changes (drop `source_platform` from projections; optionally join sidecar for it) *before or with* cutover so nothing scans a dropped column.

---

## 7. Blast radius, risks, rollback

### 7.1 Canonical LinkedIn normalization (must be nailed — it sets the merge boundary)
Spec (union of the operator's requirement + the two norms already in the codebase at `backfill_people_from_title_enrichment.py:65` and `lance_store.py:665-694`):
`lower → strip scheme (https?://) → strip any cc-subdomain + www. so host reduces to linkedin.com → drop everything up to and including linkedin.com/ → strip query (?...) and fragment (#...) incl. ?trk= → strip trailing slash → NULL if empty`. Decide `/pub/` vs `/in/` handling explicitly (current data is overwhelmingly `/in/`). Getting this exactly right matters: raw distinct URLs = 97,367 vs canonical distinct = 95,461 — ~1,900 merges come purely from normalization (trailing-slash/host variants). Under-normalizing leaves dupes; over-normalizing false-merges humans.

### 7.2 Required changes (checklist)
- **Consumers (physical/contract):** `catalyst_api/src/lance_store.py:654-657` (drop `source_platform` from `_PEOPLE_COLS`), `catalyst_api/src/models.py:675,692` (`PersonMatch`), `gtm_mcp/src/tools/audience.py:77` + docstrings, `gtm_mcp/src/tools/batch_lookups.py:192` + `_search_by_domains`. Either drop `source_platform` from the response, or backfill it from a sidecar join (BITMAP-cheap). Update fixtures in the 4 test files.
- **Writers:** the 6 people-writers (§2 #1–6) must stop writing `source_platform` into `people` and instead append `(canonical_person_id, source_platform, legacy_person_id)` to the sidecar. Their idempotency key changes from `person_id` to `canonical_person_id`. `enrich_staffing_people_title_from_clay.py` (title-only update) and `backfill_people_person_linkedin_from_contacts.py` (overwrite) must be re-pointed at the canonical schema.
- **Index cost:** the sidecar is ~117k rows — BTREE+BITMAP build is seconds. Rebuilding `people` (98k rows) + 4 BTREE indices on overwrite is the existing cost profile (writers already reindex on every append). Negligible.

### 7.3 Is the contract of the split safe?
- **Safe:** no production query filters `people.source_platform` (§3.2), so removing it breaks only projections — a contained, greppable set. `person_linkedin_url` and `normalized_domain` BTREE lookups (the hot paths) survive and improve (URL is now canonical → the person-by-linkedin lookup stops fan-out surprises).
- **Watch:** external systems hold the *legacy* `person_id`. Preserving `legacy_person_id` in the sidecar (BTREE) keeps those resolvable. The `catalyst_api` response already mirrors `contact_id := person_id` (`models.py:679-691`); after canonicalization decide whether the API returns `canonical_person_id` or continues surfacing a legacy id via the sidecar (recommend: return `canonical_person_id` as `person_id`, and add a `source_platforms: []` array populated from the sidecar — richer than the old single value).

### 7.4 Rollback
- Phases A/B touch only the new sidecar — drop the dataset to fully revert; `people` is byte-unchanged.
- Phase C writes to a NEW URI and swaps via env/pointer — rollback = repoint back to `active/people` v67 (retained by Lance versioning). No destructive in-place overwrite until confidence is high.

---

## Appendix — evidence provenance
All counts from live R2 `active/people` v67 via `doppler run -p core-x -c prd -- uv run --with duckdb --with pylance` (read-only `lance.dataset(...).to_table()` + DuckDB). Trigger CSV: `/Users/benjamincrane/core-x/exports/dsbs_poc_people_clay.csv` (12,354 lines incl. header; 12,353 data rows; person_id lengths: 64→12,002, 36→332, 47→19). Overlap with `people`: 12,353 / 12,353 already present (net-new = 0), confirmed live. Canonical-dedup and null-URL fallback counts from `probe4.py`; per-source person_id schemes from `probe2.py`.
