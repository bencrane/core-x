# Design — Extract `source_platform` into a `company_source_platforms` sidecar

**Status:** Code shipped (writers/consumers repointed). The NEW datasets were built + verified out-of-band by the coordinator; this workstream does NOT rebuild or overwrite them.
**Date:** 2026-07-02
**Datasets in scope:** `s3://data-sink/active/companies/` (v131, 117,037 rows — FROZEN for rollback) → `s3://data-sink/active/companies_canonical/` (identity) + `s3://data-sink/active/company_source_platforms/` (sidecar).

---

## 0. TL;DR

- `source_platform` is removed from the wide company row and lands wholesale in the `company_source_platforms` sidecar. `companies_canonical` carries EVERY companies column EXCEPT `source_platform`.
- **NO DEDUP.** `company_id` stays the 1:1 primary key. All **117,037 rows are preserved**. There is **NO `canonical_company_id`, NO row collapse, NO merge** on `linkedin`/`domain`. This split is purely a column extraction.
- Cutover is a **REPOINT via the code default**: `GTM_COMPANIES_URI` is UNSET in Doppler, so changing the default from `…/active/companies/` to `…/active/companies_canonical/` is what takes effect. The gtm_mcp registry unconditionally overrides `companies` → `companies_canonical`.
- The live `active/companies` v131 is never written by this workstream — it is the rollback anchor.

---

## 1. Why companies is a column-extraction, NOT an entity-resolution collapse (contrast with people)

The parallel `person_source_platforms` workstream (see `PEOPLE_SOURCE_PLATFORM_SIDECAR.md`) is a genuine entity-resolution collapse: `person_id` is manufactured per-source (source-native UUID / Clay id / sha256 / md5 / uuid5), so the same human lands under multiple ids and the sidecar must be keyed on a derived `canonical_person_id` while `people` collapses 116,837 → 98,285 rows.

**Companies are structurally different.** `company_id` is ALREADY the stable, cross-source-unique 1:1 key that every companies writer emits:

| source_platform | writer | `company_id` derivation |
|---|---|---|
| `dsbs` | `backfill_dsbs_companies.py` | `uei` (stable DSBS/SAM PK) |
| `sfnet-directory` | `backfill_sfnet_companies.py` | `sfnet_company_id` (stable SFNet directory UUID) |
| `dexarchive_staffing_agencies` | `backfill_staffing_agencies_companies.py` | `target_company_id` (stable dex UUID) |
| (firmographic enrich, no new rows) | `backfill_companies_firmographics_from_blitz.py` | preserves `company_id` verbatim (overwrite, 1:1) |

So the company sidecar is keyed **directly on `(company_id, source_platform)`** — no id manufacturing, no derivation, no collapse. The only job is to get `source_platform` off the wide row so a company is one identity row and its origins are a joinable set.

---

## 2. Datasets (built + verified out-of-band — do NOT rebuild)

### `active/company_source_platforms/` (sidecar)
| column | type | null | index | notes |
|---|---|---|---|---|
| `company_id` | string | no | BTREE | the companies PK (1:1 join key) |
| `source_platform` | string | no | BITMAP | origin tag; 23 values today (low-cardinality → BITMAP) |
| `first_seen_at` | timestamp(us, UTC) | no | — | set `now()` on first insert; NEVER updated on re-run |
| `source_ref` | string | yes | — | free-form cohort / run note |

- **117,037 rows** (1 per company_id today — every company has exactly one source at backfill; the grain permits many).
- **Idempotency key = `(company_id, source_platform)`** via `merge_insert(...).when_not_matched_insert_all()`.

### `active/companies_canonical/`
- **117,037 rows**, `company_id` unique PK, EVERY companies column EXCEPT `source_platform`.
- Indices: BTREE `[company_id, normalized_domain, uei]` + BITMAP `[company_type, employee_size_band, hq_region, industry]`.

---

## 3. Shared helper — `pipelines/gtm/_company_source.py`

- `COMPANIES_URI` default → `s3://data-sink/active/companies_canonical/` (env `GTM_COMPANIES_URI` overrides).
- `COMPANY_SOURCE_PLATFORMS_URI` default → `s3://data-sink/active/company_source_platforms/` (env `COMPANY_SOURCE_PLATFORMS_URI` overrides).
- `record_company_sources(company_ids, source_platform, source_ref, storage_options)` — idempotent `merge_insert(["company_id","source_platform"]).when_not_matched_insert_all()` into the sidecar, `first_seen_at = now()`, null/empty/duplicate ids dropped. One row per `(company_id, source_platform)`.

Every company writer imports this so provenance routes to the sidecar identically across sources.

---

## 4. Blast radius — writers of `active/companies`

The complete set of pipelines that WRITE `active/companies` (verified via `write_dataset` grep):

| writer | mode | source_platform | change |
|---|---|---|---|
| `backfill_dsbs_companies.py` | append | `'dsbs'` | drop from projection; `record_company_sources(...)`; verify scan → sidecar |
| `backfill_sfnet_companies.py` | append | `'sfnet-directory'` | drop from `populated`; `record_company_sources(...)`; verify scan → sidecar |
| `backfill_staffing_agencies_companies.py` | append | `'dexarchive_staffing_agencies'` | drop from projection; `record_company_sources(...)`; verify scan → sidecar |
| `backfill_companies_firmographics_from_blitz.py` | **overwrite** | carried verbatim (was a BASE_COL) | **drop `source_platform` from `BASE_COLS`**; read+write `companies_canonical`; do NOT re-add. Introduces no NEW companies → records nothing to the sidecar. **HIGHEST-RISK writer** — it is the only in-place overwrite of the full row set. |
| `companies_people_bulk.py` | (RETIRED — refuses every call) | — | not a live writer; owns the `DATASET_URI["companies"]` constant the firmographics writer no longer reads (firmographics now points at `_company_source.COMPANIES_URI`). Left as-is. |

`materialize_clay_find_companies.py` / `materialize_clay_enrich_companies.py` write their OWN `clay_*_companies` datasets, NOT `active/companies` — untouched. `exa_websets/ingest.py` writes `discovered_websets`/`webset_membership` (which keep their own `source_platform` column) and only READS companies domains.

---

## 5. Blast radius — consumers

| consumer | use of companies.source_platform | change |
|---|---|---|
| `apps/gtm_mcp/src/tools/audience.py` | PROJECT (`_COMPANY_COLUMNS`, 2 docstrings, name-match SQL) | drop from projection + docstrings |
| `apps/gtm_mcp/src/tools/batch_lookups.py` | PROJECT (`_COMPANY_DOC = audience._COMPANY_COLUMNS`) | propagates; docstring updated |
| `apps/gtm_mcp/src/database.py` | registry wiring | `reg["companies"] = companies_canonical` (unconditional override, mirrors `people`) |
| `pipelines/serving/materialize_firm_construction_proximity.py` | **FILTER** `WHERE source_platform IN (…)` | rewritten to a **JOIN** `companies_canonical ⨝ company_source_platforms ON company_id WHERE csp.source_platform IN (…)`. Repointed. **Second-highest-risk change** — a filter→join rewrite. |
| `pipelines/gtm/backfill_staffing_agencies_people.py` | **FILTER** cohort by `source_platform='dexarchive_staffing_agencies'` | cohort ids resolved from the sidecar, joined back to `companies_canonical` for `normalized_domain`. Repointed. |
| `pipelines/gtm/build_sfnet_main_contacts.py` | PROJECT → `is_sfnet` tie-break flag | sidecar LEFT JOIN on `company_id` derives `is_sfnet`. Repointed. (The people-side `source_platform` read in this file is the PEOPLE workstream's concern and is left untouched.) |
| `pipelines/gtm/materialize_capital_provider_signals.py` | PROJECT → curated lender origins (`elfa`/`sfnet`/`exa`/`exa-all`) | curated-tagged ids resolved from the sidecar, joined back for `normalized_domain`. Repointed. |
| `pipelines/enrichment_blitz/enrich_blitz.py` | default only | repointed |
| `pipelines/gtm/blitz_hydration_waterfall.py` | default only | repointed |
| `pipelines/enrichment_blitz/run_capital_provider_icp.py` | hardcoded companies URI (reads `firmo_linkedin_url`) | repointed → `companies_canonical` |
| `pipelines/exa_websets/ingest.py` | default only (reads `normalized_domain`) | repointed |
| `pipelines/catalog/schema_catalog.py` | catalog comparison list | `companies` → `companies_canonical`; added `company_source_platforms` |
| Fixtures: `test_lookup_cache.py`, `test_batch_lookups.py` | company fixtures asserting `source_platform` | dropped from fixtures + `COMPANY_DOC` |

**Known untouched diagnostic:** `pipelines/serving/probe_proximity_tam.py` is an explicitly read-only ad-hoc probe (`NO writes, NO index changes`) that HARDCODES `s3://data-sink/active/companies/` (the FROZEN v131, which still carries `source_platform`) and filters on it heavily. It keeps working against the frozen dataset. Deliberately out of scope for this repoint; rewrite if it is ever pointed at the canonical dataset.

---

## 6. The deferred linkedin>domain dedup (NOT in this workstream)

A LinkedIn- then domain-level dedup of `companies_canonical` is a **separate, future workstream the operator decides later**. It is deliberately excluded here because collapsing company rows is genuinely hazardous:

- **Tribal / holding-company structure.** Multiple legally distinct entities (subsidiaries, tribal 8(a) holding companies, SAM registrations under one parent) legitimately share a LinkedIn page or a corporate domain. Collapsing on `company_linkedin_url` / `normalized_domain` would fuse distinct awardable entities and destroy per-UEI federal-spend resolution.
- **Placeholder / cruft domains.** Junk anchors (parked domains, registrar placeholders, `linkedin.com/company/` stubs) fan many unrelated companies onto one key. A naive dedup would merge them.
- Any future dedup must therefore introduce an EXPLICIT `canonical_company_id` derived under a vetted entity-resolution rule (with a holding-company / placeholder blocklist), keep `company_id` as the retained legacy key, and land the collapse in a NEW dataset behind the same non-destructive repoint. Until then, `company_id` is 1:1 and every row stands.

The sidecar is dedup-neutral: it records `(company_id, source_platform)` regardless of any future company collapse, so a later dedup can re-group sidecar rows under a `canonical_company_id` without a re-ingest.

---

## 7. Cutover checklist

- [x] Sidecar + `companies_canonical` built and verified out-of-band (117,037 rows each; NO `source_platform` on canonical; NO dedup).
- [x] `_company_source.py` helper: URIs + idempotent `record_company_sources`.
- [x] Company writers stop writing `source_platform`; route to sidecar; self-verify scans moved to sidecar.
- [x] Firmographics overwrite writer reads/writes `companies_canonical`, `source_platform` dropped from `BASE_COLS`, never re-added.
- [x] Proximity FILTER rewritten to a sidecar JOIN; staffing-people + capital-provider + sfnet-main-contacts consumers repointed with sidecar joins.
- [x] All `GTM_COMPANIES_URI` default consumers repointed → `companies_canonical`.
- [x] gtm_mcp registry override + projection/docstring drops; fixtures updated.
- [x] `apps/gtm_mcp` test suite green (60 passed); all edited pipelines byte-compile; helper functional self-test (idempotency, grain) green.
- [ ] **Coordinator-owned (NOT this workstream):** overwrite/retire `active/companies` v131, Doppler env changes, deploy, merge.

---

## Appendix — rollback

- The split touches only NEW datasets. Rollback = repoint `GTM_COMPANIES_URI` / the gtm_mcp registry back to `active/companies` v131 (byte-unchanged, retained by Lance versioning) and revert the code defaults. No destructive in-place overwrite of the frozen dataset occurs anywhere in this workstream.
