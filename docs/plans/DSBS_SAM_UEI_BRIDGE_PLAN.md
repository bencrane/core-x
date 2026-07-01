# DSBS ↔ SAM.gov UEI Crosswalk — Canonical Substrate Plan

**Status:** proposed (investigation complete, no build performed)
**Scope:** net-new firm-grain (1 row / `uei`) crosswalk Lance surfacing SAM resolution keys + firmographics onto the DSBS certified-firm universe. Zero SoR mutation.
**Verified against live Lance:** 2026-06-30. Every number below was measured, not assumed.

---

## 0. Direct answers to the operator's three questions

### Q1 — "Do we have any type of Lance that joins DSBS to SAM.gov so I don't have to cobble this together again to get the `entity_url` value etc.?"

**No.** There is **no** DSBS↔SAM join Lance today, and **no committed builder** for one. Confirmed by enumerating all 410 `active/` datasets (boto3) and grepping `pipelines/` — nothing joins `sba_dsbs_certified_firms` to `sam_master_*` as a persisted, indexed dataset. The `bridge_dsbs_pdl_linkedin` built this session has no committed builder either and is a narrow LinkedIn-only slice, not a general crosswalk.

What **does** exist is everything needed to build it cheaply, because DSBS and SAM share `uei` natively and both are already 1-row-per-`uei` with a `BTREE` on `uei`. The `entity_url` you cobbled together lives in `sam_master_entities.entity_url` (raw) and, canonicalized, in `sam_master_domains.normalized_domain`. **This plan makes that a single indexed point-lookup instead of an ad-hoc multi-join.**

### Q2 — "There is also the `sam_pocs` table etc.?" — what is it, is it usable?

**Yes, and it is the human/contact layer — highly usable, but it must stay its own dataset.**

- `s3://data-sink/active/sam_pocs/` — **8,065,679 rows**, **1,540,965 distinct `uei`**. Grain = **1 row per (entity, populated POC slot)**, up to 6 slots/entity. `BTREE(uei, cage_code, name_key, last_name)`, `BITMAP(poc_type, source_family)`.
- Carries **person name** (`first_name`/`middle_name`/`last_name`/`full_name`, SAM-native pre-split, never re-parsed), **`title`**, **`poc_type`** (the 6 SAM slots), and **mailing address** (`address_line_1/2`, `city`, `state`, `zip5`, `zip4`, `country`).
- **It does NOT carry email or phone.** Neither does `sam_master_contacts` (a thinner 4.37M-row variant of the same POC content). Person-level email/phone for a DSBS firm comes from **DSBS itself** (`email` 58,961 filled / `phone` 58,163 filled of 67,234) or from downstream PDL/enrichment feeds — not from SAM POCs.
- **DSBS coverage: 66,734 of 67,234 DSBS UEIs (99.3%) have ≥1 `sam_pocs` row.** The two mandatory slots (`government_business`, `electronic_business`) are present for all 66,734. Slot distribution per DSBS firm: 2 slots → 39,291; 3–4 → 15,255; 5–6 → 12,188.

**Design consequence:** `sam_pocs` is multi-per-`uei`. It is **NOT** folded into the firm row (that would fan out the firm grain). It stays its own 1-row-per-POC dataset, already `BTREE`-indexed on `uei`, joined to the crosswalk on `uei` on demand.

### Q3 — "I am fairly certain both DSBS and SAM.gov have `uei` — right?"

**Correct, and `uei` is a clean shared primary key on both sides.**

| Dataset | rows | distinct `uei` | null `uei` | grain |
|---|---:|---:|---:|---|
| `sba_dsbs_certified_firms` | 67,234 | 67,234 | 0 | **exactly 1 / uei** |
| `sam_master_entities` | 1,541,566 | 1,541,566 | 0 | **exactly 1 / uei** |

Both are UEI-native (DSBS carries no DUNS; SAM master is deduped to latest-per-uei). Both already have `BTREE(uei)`. **`uei` is the join key — no DUNS bridge, no name-match, no fuzzy resolution required for the spine.** The DSBS→SAM `uei` join lands **66,734 of 67,234 (99.3%)**.

---

## 1. TL;DR recommendation

Build **`s3://data-sink/active/crosswalk_dsbs_sam/`** — a **1-row-per-`uei`** fully-indexed join spine over the 67,234 DSBS certified firms, surfacing the SAM resolution keys and firmographics — plus a **resolved `best_domain`** (the single most-accurate domain per firm, picked by a fixed preference order **`entity_url` > `website` > `email`-suffix > `additional_website`**, each junk-blocklisted (no name gate), then resolved to its PDL **`company_linkedin_url`**; `best_domain_source` + `best_domain_name_consistent` recorded so you can exclude/drill any source downstream; see §4.3.1), `cage_code`, `primary_naics`, `is_active`, `exclusion_status_flag`, `registration_expiration_date`, `pdl_company_id` — plus DSBS cert flags and a `has_sam_poc` presence bit. POCs stay in `sam_pocs` (join on `uei`). Modal worker mirrors `pipelines/gtm/materialize_clay_find_people.py`; one Trigger.dev control-plane line; zero new endpoints/secrets. **The exact ad-hoc joins done this session collapse to `WHERE uei = ?` against one indexed dataset.**

---

## 2. Current-state map — what is joinable on `uei` TODAY (zero new build)

All of the following are already 1/uei (or reducible to it) and `BTREE(uei)`-indexed, so any one is a live point-lookup — the friction is that you must **know all of them and hand-join every time.**

| SAM-side asset | URI (`s3://data-sink/active/…`) | rows | grain | `BTREE(uei)` | DSBS-uei coverage (of 67,234) | What it gives on a `uei` join |
|---|---|---:|---|:--:|---:|---|
| `sam_master_entities` | `sam_master_entities/` | 1,541,566 | **1/uei** | ✓ | **66,734 (99.3%)** | `entity_url` (raw), `is_active`, `exclusion_status_flag`, `registration_expiration_date`, `primary_naics`, `cage_code`, `legal_business_name`, `bus_type_string`, `sba_business_types_string`, physical address, +67 cols |
| `sam_master_domains` | `sam_master_domains/` | 709,546 | **~1/uei** | ✓ | **41,816 (62.2%)** | canonical `normalized_domain` (core.web_norm of `entity_url`, non-generic) |
| `sam_pocs` | `sam_pocs/` | 8,065,679 | **≤6/uei** | ✓ | **66,734 (99.3%) have ≥1** | POC person name, `title`, `poc_type`, mailing address (NO email/phone) |
| `sam_master_contacts` | `sam_master_contacts/` | 4,373,319 | multi/uei | ✓ | (subset of pocs) | thinner POC variant — superseded by `sam_pocs`; not needed |
| `sam_normalized_entities` | `sam_normalized_entities/` | 1,541,566 | **1/uei** | ✓ | ~99% | `normalized_legal_name`, `legal_name_base`, `is_active` — name-match spine, not needed for a `uei` join |
| `bridge_sam_pdl` | `bridge_sam_pdl/` | 801,831 | **fans out (max 96/uei)** | ✓ | **24,563 (36.5%)** | `pdl_company_id`, `normalized_domain`, `duns` — resolution key is **domain**; fans on DUNS location |

**`entity_url` on the DSBS universe today:** of the 66,734 matched firms, **41,851 (62.7%)** have a non-null `entity_url` in `sam_master_entities`; **41,816** have a canonical `normalized_domain` in `sam_master_domains`. Unioning DSBS's own `website`/`additional_website` with SAM's `entity_url`/`normalized_domain` yields **45,288 (67.4%)** of DSBS firms with ≥1 web signal, of which **1,128** get a web signal **only** from SAM (DSBS had none). So SAM `entity_url` is both the primary source for firms that hid their DSBS website and a corroborating second source for the rest.

**`bridge_sam_pdl` caveat (measured):** it is **NOT 1/uei** — 801,831 rows, 463,741 distinct `uei`, **max 96 rows/uei**. It fans out because each row is a SAM-registration × DUNS-location, and `duns` varies while `pdl_company_id` stays constant. Critically, **457,676 of 463,741 UEIs (98.7%) map to exactly ONE distinct `pdl_company_id`** — so `SELECT DISTINCT uei, pdl_company_id` collapses cleanly to firm grain (last-wins on the 1.3% multi-PDL UEIs). DSBS coverage is only **36.5% (24,563)** — PDL is a partial enrichment, not a spine.

---

## 3. The gap — what forced the ad-hoc cobbling

To get "`entity_url` etc." for a DSBS cohort today you must, every time, hand-assemble:

1. `sba_dsbs_certified_firms` (the 67,234 spine),
2. LEFT JOIN `sam_master_entities ON uei` for `entity_url` + activity/exclusion/expiration/NAICS,
3. LEFT JOIN `sam_master_domains ON uei` for the canonical `normalized_domain`,
4. optionally collapse-and-JOIN `bridge_sam_pdl` (which **fans out** — a naive join silently multiplies your firm rows up to 96×) for `pdl_company_id`,
5. optionally JOIN `sam_pocs ON uei` for a contact.

Every consumer re-derives this. The `bridge_sam_pdl` fan-out is a live footgun: join it without a `DISTINCT`/`QUALIFY` collapse and the DSBS cohort inflates. The `bridge_dsbs_pdl_linkedin` built this session is exactly this cobble frozen for one narrow output (LinkedIn URL) — with **no committed builder**, so it is unreproducible and unmaintained.

A canonical crosswalk fixes it: **the multi-join, the fan-out collapse, and the normalization all happen once, at build time, behind a single `BTREE(uei)` lookup.** Consumers (the LinkedIn bridge, GTM targeting, enrichment cohorts) become `WHERE uei = ?` or a single `JOIN … USING(uei)` against a guaranteed-1/uei dataset. The LinkedIn bridge becomes a thin downstream of this spine rather than a parallel re-derivation.

---

## 4. RECOMMENDED DESIGN

### 4.1 Shape

- **Firm-grain spine** `crosswalk_dsbs_sam` — **exactly 1 row per DSBS `uei`** (67,234 rows). Left-anchored on DSBS so the dataset **is** the DSBS certified universe; SAM columns are nullable enrichment. Surfaces resolution keys + SAM essentials + DSBS cert flags + POC presence bit.
- **POCs stay separate.** `sam_pocs` already exists at 1-row-per-POC with `BTREE(uei)`. **Do not explode it into the firm row.** The crosswalk carries only a bounded presence/count summary (`has_sam_poc`, `sam_poc_count`, `sam_primary_poc_name` from the mandatory `government_business` slot) so the common "is there a contact?" question is answered without a join; full contact retrieval is `sam_pocs WHERE uei = ?`.
- **PDL is collapsed, not joined raw.** `pdl_company_id` is pulled via `SELECT DISTINCT uei, pdl_company_id … QUALIFY row_number() OVER (PARTITION BY uei ORDER BY registration_status) = 1` so the fan-out never reaches the firm grain.

### 4.2 Output URI

```
s3://data-sink/active/crosswalk_dsbs_sam/
```
(naming mirrors the existing `crosswalk_sam_usaspending/`, `crosswalk_sos_sam/` convention.)

### 4.3 Column contract + dtypes + source + index plan

| # | column | dtype | source dataset → column (join key) | index |
|---:|---|---|---|---|
| 1 | `uei` | string (not null) | `sba_dsbs_certified_firms.uei` (spine PK) | **BTREE** |
| 2 | `dsbs_legal_business_name` | string | `sba_dsbs_certified_firms.legal_business_name` | — |
| 3 | `in_sam` | bool | derived: `uei` present in `sam_master_entities` | BITMAP |
| 4 | `sam_legal_business_name` | string | `sam_master_entities.legal_business_name` (uei) | — |
| 5 | `cage_code` | string | `sba_dsbs_certified_firms.cage_code`, coalesce `sam_master_entities.cage_code` (uei) | **BTREE** |
| 6 | `entity_url` | string | `sam_master_entities.entity_url` (uei) — **raw, verbatim** | — |
| 7 | `dsbs_website` | string | `sba_dsbs_certified_firms.website` — raw | — |
| 8 | `dsbs_additional_website` | string | `sba_dsbs_certified_firms.additional_website` — raw | — |
| 8a | `domain_entity_url` | string | `sam_master_domains.normalized_domain` (uei) = `core.web_norm(entity_url)`, non-generic | — |
| 8b | `domain_website` | string | `core.web_norm(website)`, non-generic (per-source, audit) | — |
| 8c | `domain_additional` | string | `core.web_norm(additional_website)`, non-generic (per-source, audit) | — |
| 8d | `domain_email` | string | `core.web_norm(split_part(email,'@',2))`, non-generic (per-source, audit) | — |
| 8e | **`best_domain`** | string | **RESOLVED** — single best-of-all-four domain per firm (see §4.3.1); the canonical firm↔domain key | **BTREE** |
| 8f | `best_domain_source` | string | which source won, in preference order: `entity_url`\|`website`\|`email`\|`additional_website` (queryable — exclude/drill-in) | BITMAP |
| 8g | `best_domain_source_count` | int32 | # of the four sources emitting `best_domain` (1–4) — confidence only, not a ranker | — |
| 8h | `best_domain_in_pdl` | bool | `best_domain` exists in `pdl_normalized_companies` | BITMAP |
| 8i | `matched_domain` | string | **LINKEDIN-MAX**: highest-ranked source-domain that RESOLVES in PDL (may differ from `best_domain` when the identity domain isn't in PDL); null if none resolve | **BTREE** |
| 8j | `matched_domain_source` | string | which source `matched_domain` came from: `entity_url`\|`website`\|`email`\|`additional_website` | BITMAP |
| 8k | `company_linkedin_url` | string | `matched_domain` → `pdl_normalized_companies.linkedin_slug` → `https://www.linkedin.com/company/<slug>`; null if unmatched | **BTREE** |
| 8l | `matched_pdl_company_id` | string | `matched_domain` → `pdl_normalized_companies.pdl_company_id` (domain-resolved; distinct from col 16's SAM-identity `pdl_company_id`) | **BTREE** |
| 8m | `matched_name_consistent` | bool | resolved PDL company name-matches the firm — **recorded flag, not a gate** (filter lever) | BITMAP |
| 10 | `primary_naics` | string | `sam_master_entities.primary_naics`, coalesce `sba_dsbs_certified_firms.naics_primary` (uei) | **BTREE** |
| 11 | `is_active` | bool | `sam_master_entities.is_active` (uei) | BITMAP |
| 12 | `exclusion_status_flag` | string | `sam_master_entities.exclusion_status_flag` (uei) | BITMAP |
| 13 | `registration_expiration_date` | date32 | `sam_master_entities.registration_expiration_date` (uei) | — |
| 14 | `entity_structure` | string | `sam_master_entities.entity_structure` (uei) | — |
| 15 | `sba_business_types_string` | string | `sam_master_entities.sba_business_types_string` (uei) | — |
| 16 | `pdl_company_id` | string | `bridge_sam_pdl.pdl_company_id` (uei, **DISTINCT-collapsed**) | **BTREE** |
| 17 | `pdl_normalized_domain` | string | `bridge_sam_pdl.normalized_domain` (uei, collapsed) | — |
| 18 | `dsbs_email` | string | `sba_dsbs_certified_firms.email` | — |
| 19 | `dsbs_phone` | string | `sba_dsbs_certified_firms.phone` | — |
| 20 | `dsbs_contact_person` | string | `sba_dsbs_certified_firms.contact_person` | — |
| 21 | `cert_programs` | string | `sba_dsbs_certified_firms.cert_programs` | BITMAP |
| 22 | `active_8a` | bool | `sba_dsbs_certified_firms.active_8a_boolean` | BITMAP |
| 23 | `active_hz` | bool | `sba_dsbs_certified_firms.active_hz_boolean` | BITMAP |
| 24 | `active_wosb` | bool | `sba_dsbs_certified_firms.active_wosb_boolean` | BITMAP |
| 25 | `active_edwosb` | bool | `sba_dsbs_certified_firms.active_edwosb_boolean` | BITMAP |
| 26 | `active_sdvosb` | bool | `sba_dsbs_certified_firms.active_sdvosb_boolean` | BITMAP |
| 27 | `active_vosb` | bool | `sba_dsbs_certified_firms.active_vosb_boolean` | BITMAP |
| 28 | `has_sam_poc` | bool | derived: `uei` present in `sam_pocs` | BITMAP |
| 29 | `sam_poc_count` | int32 | `count(*)` over `sam_pocs` for `uei` (bounded ≤6) | — |
| 30 | `sam_primary_poc_name` | string | `sam_pocs.full_name` where `poc_type='government_business'` (uei) | — |
| 31 | `materialized_at` | timestamp[us,UTC] (not null) | build lineage (`now()`) | — |

**Index summary:** `BTREE(uei, cage_code, normalized_domain, primary_naics, pdl_company_id)` — the five keys any downstream resolves on. `BITMAP` on the low-cardinality facets (`in_sam`, `is_active`, `exclusion_status_flag`, `cert_programs`, `has_sam_poc`, the six DSBS cert booleans) for cohort filtering. `uei` `BTREE` is mandatory and is the PK for merge_insert.

> **Grain discipline honored:** POCs are NOT exploded. Columns 28–30 are a bounded rollup (a presence bit, a ≤6 count, and the single mandatory-slot primary name) — justified because "does this firm have a SAM contact, and who is the primary?" is the highest-frequency question and answering it inline avoids the 8M-row `sam_pocs` join for the common case. Full multi-POC retrieval remains `sam_pocs WHERE uei = ?`. `bridge_sam_pdl` is collapsed to 1/uei before it touches the spine.

### 4.3.1 `best_domain` resolution — strict preference order, junk-stripped, name-gated

`best_domain` is picked by a **fixed source-preference order** (operator-set), not a corroboration score. Per `uei`, walk the sources in order and take the **first** that yields a *qualifying* domain:

1. **`entity_url`** (normalized) — the SAM registration URL; the accuracy assessment and junk audit both rank it cleanest (0 generic-host contamination; on the 149 firms where website and entity_url disagree it resolved to a correctly-named PDL company **68% vs website's 32%**).
2. **`website`** (normalized) — the firm's self-published site.
3. **`email`-suffix** (normalized, junk-stripped) — real but leaks ISP-webmail; gated (below).
4. **`additional_website`** (normalized, junk-stripped) — weakest; ~73% the firm's own, but ~27% points to another company; gated (below).

Each candidate is `core.web_norm.normalized_domain(_bare_host(x))` and **qualifies** only if it is **non-generic under the *extended* blocklist** (canonical `core.web_norm._GENERIC_DOMAINS` + the curated additions in §4.3.2). This is the **"without the junk"** step — it strips ISP-webmail (`comcast.net`, `cox.net`…), directories (`psychologytoday.com`, `fedlinks.com`), doc/file hosts (`drive.google.com`, `acrobat.adobe.com`), shorteners (`tinyurl.com`), buying co-ops (`doitbest.com`), franchisor brands (`uslawns.com`, `redboxplus.com`), etc. **No name gate — nothing is dropped for failing a name check.**

**Then resolve to PDL — LinkedIn-max, a SEPARATE fact from `best_domain`:** independently of the identity pick, walk the same four source-domains in order and take the first that RESOLVES in PDL as **`matched_domain`** (+ `matched_domain_source`); attach **`company_linkedin_url`** (`https://www.linkedin.com/company/` + `linkedin_slug`) and `matched_pdl_company_id`. `matched_domain` equals `best_domain` when the identity domain is in PDL, and falls to a lower-ranked source-domain when it isn't — so LinkedIn coverage is maximized (28,467 firms) **without demoting `best_domain` from the firm's #1 identity domain**. This folds `bridge_dsbs_pdl_linkedin` **into** the spine.

Recorded provenance (all queryable — **your filter levers, not hard gates**): **`best_domain_source`** (`entity_url`|`website`|`email`|`additional_website`), `best_domain_source_count` (confidence), `best_domain_in_pdl`, **`matched_domain_source`**, `matched_name_consistent` (whether the resolved PDL company name matches the firm — **recorded, not gating**). The four per-source `domain_*` columns are retained for audit / re-resolution.

**On the different-company tail:** `matched_domain` via `additional_website` (and, rarely, `email`) can resolve to a *different real company's* LinkedIn (`2 RANGERS CONSULTING` → `patriotspridewindows.com` → "Patriots' Pride Windows"). Per your call these are **not gated out** — they stay visible and filterable via `matched_domain_source = 'additional_website'` and/or `matched_name_consistent = false`. The blocklist removes the non-company junk; the source + name-consistency **flags** let you dial the different-company precision up or down downstream without dropping rows at build time.

### 4.3.2 Extended `_GENERIC_DOMAINS` (curated from the live 514 review + email audit)

Root-cause fix lives in canonical `core/web_norm.py` `_GENERIC_DOMAINS` (single definition — extending it cleans every consumer). Additions are exact-host matches; subdomain-only hosts (e.g. `a2z.espwebsite.com`) are left to the name gate, not blocklisted at the apex.

- **ISP / telco webmail:** `comcast.net`, `att.net`, `verizon.net`, `sbcglobal.net`, `bellsouth.net`, `cox.net`, `charter.net`, `frontier.com`, `earthlink.net`, `roadrunner.com`, `windstream.net`, `centurylink.net`, `optonline.net`, `juno.com`, `netzero.net`, `mac.com`, `me.com`, `rocketmail.com`, `gvtc.com`
- **Directory / listing / registry / agent-locator:** `psychologytoday.com`, `veteranownedbusiness.com`, `mybaseguide.com`, `orcid.org`, `agents.allstate.com`, `newyorklife.com`, `kw.com`
- **Doc / file / cloud host:** `drive.google.com`, `docs.google.com`, `acrobat.adobe.com`, `storage.googleapis.com`, `canva.com`, `dropbox.com`
- **Search / share / shortener:** `google.com`, `share.google`, `tinyurl.com`, `tiny.cc`
- **Aggregator / buying co-op / brand-network:** `fedlinks.com`, `doitbest.com`, `avoyatravel.com`, `johncmaxwellgroup.com`
- **Marketplace / retailer:** `napaonline.com`, `uhaul.com`, `snap.com`, `stores.ebay.com`
- **Franchisor brands** (franchisee ≠ corporate LinkedIn; `uslawns.com` alone wrongly pins 13 firms): `advantaclean.com`, `uslawns.com`, `redboxplus.com`, `myvoda.com`, `greasemonkeyauto.com`
- **Hold / re-review** (single-firm, ambiguous — leave to name gate for now): `supplypointe.com`, `govconhacks.com`, `growfedbiz.com`, `searchpath.com`, `homeasap.com`, `iamrealestate.com`, `fullypromoted.com`

Source artifacts: `~/Desktop/additional_website_junk_blocklist.csv` (38 rows) + `additional_website_classification.csv` (all 514, `final_class`). The **113 `DIFFERENT_COMPANY`** cases are deliberately **not** blocklisted — those domains are legit companies for *someone*; the name gate, not the blocklist, is the correct tool.

### 4.4 Pipeline shape (mirror `materialize_clay_find_people.py`)

- **New Modal app** `crosswalk-dsbs-sam` (domain-grouped, **endpoint-less**, dispatcher-resolvable via `core/modal_dispatcher.py`). New feed = new worker + one control-plane line. **Zero new endpoints, zero new secrets** — reuses `r2-credentials` + `hqx-postgres`.
- **Data plane (clean-room):** DuckDB does 100% of read/cast. Register the **seven** source Lance datasets as Arrow readers (`lance.dataset(...).scanner(columns=[...]).to_reader()`) — DSBS, `sam_master_entities`, `sam_master_domains`, `sam_pocs`, `bridge_sam_pdl`, and `pdl_normalized_companies` (for `best_domain_in_pdl` + the resolution ranking) — run the LEFT-JOIN + **`best_domain` resolution (§4.3.1)** + PDL-collapse + POC-rollup SQL, stream Arrow batches (`READ_BATCH_ROWS=50000`, out-of-core) straight to `lance.write_dataset(mode="overwrite", data_storage_version="2.1", max_rows_per_file=1048576, max_bytes_per_file=90*1024**3)`. No catalog.
- **Indexes** created post-write via `ds.create_scalar_index(col, index_type=...)` for the BTREE/BITMAP plan above (index miss logs, never fails a good load — same as the reference).
- **Ops ledger:** `ops.crosswalk_dsbs_sam_runs` (mirror `ops.clay_find_people_runs`: feed, source_db, datasets jsonb, mode, rows_total/source/added, watermark, status, error, timestamps; three indexes). One run row per execution.
- **Callback:** on terminal state POST the flat Trigger.dev durable callback (same `_post_callback` retry shape).
- **Entrypoints:** `ingest` (overwrite hydrate), `append_only` (watermark merge_insert on `uei`), `init_ops`, `reindex_only`, `verify_only` — the reference's five-entrypoint contract.

### 4.5 Idempotency, blast radius, refresh

- **Idempotency:** overwrite hydrate is deterministic (all sources are stable Lance snapshots; the PDL collapse and POC rollup are `QUALIFY`/`GROUP BY` — bit-stable over fixed inputs). Incremental path is **`merge_insert` on `uei`** (PK) — re-running lands only changed firms.
- **Blast radius: net-new create only.** No existing SoR is touched. `sba_dsbs_certified_firms`, `sam_master_*`, `bridge_sam_pdl`, `sam_pocs` are read-only inputs. No columns inlined onto any SoR. Deleting `crosswalk_dsbs_sam/` reverts the world.
- **Refresh cadence (control plane, Trigger.dev — not embedded cron):** rebuild on the **downstream** of the upstream feeds — i.e. after any of `sba_dsbs_certified_firms`, `sam_master_entities`, `sam_master_domains`, `sam_pocs`, `bridge_sam_pdl` refreshes. Simplest correct policy: a single **weekly** Trigger.dev task line firing the overwrite hydrate (the whole build is one cheap join over ≤1.5M-row masters; a full overwrite is trivially within envelope and sidesteps watermark bookkeeping across five heterogeneous upstreams). Watermark/append is available for hot-path incremental if cadence tightens.

### 4.6 What it replaces

The session's ad-hoc chain — `dsbs ⨝ sam_master_entities` (entity_url) `⨝ sam_master_domains` (normalized_domain) `⨝ collapse(bridge_sam_pdl)` (pdl_company_id) `⨝ sam_pocs` (contact) — becomes:

```sql
SELECT best_domain, best_domain_source, entity_url, pdl_company_id, is_active, sam_primary_poc_name
FROM   lance('s3://data-sink/active/crosswalk_dsbs_sam/')
WHERE  uei = ?          -- single BTREE point-lookup, 1 row guaranteed
```

The `bridge_dsbs_pdl_linkedin` dataset becomes a **thin downstream consumer**: it joins `crosswalk_dsbs_sam.best_domain` → `pdl_normalized_companies.normalized_domain`, dropping its own re-derivation of the four-source domain union (the `best_domain` resolution now lives once, in the spine).

---

## 5. Build order (sequence · dependency · blast radius)

1. **`init_ops`** — create `ops.crosswalk_dsbs_sam_runs` in HQX Postgres. *(No data blast radius.)*
2. **Author the worker** `pipelines/sba_dsbs/crosswalk_dsbs_sam.py` (or `pipelines/resolution/…`) mirroring `materialize_clay_find_people.py` — six Lance readers, LEFT-JOIN SQL, PDL `DISTINCT`-collapse, POC rollup, Arrow-streamed overwrite, BTREE/BITMAP index plan, ops ledger, callback, five entrypoints. *(Pure create; nothing shipped yet.)*
3. **Dry-run / `verify_only`** on a scratch URI (`CROSSWALK_DSBS_SAM_URI` override, feed → `…_scratch`) — assert 67,234 rows, distinct `uei` = row count (1/uei invariant), `in_sam` ≈ 66,734, `entity_url` non-null ≈ 41,851, **`best_domain` non-null ≈ 53,422 (79.5%)** with `best_domain_source_count`/`best_domain_source` populated, `has_sam_poc` ≈ 66,734, no firm-grain fan-out from the PDL/POC joins. *(Scratch only — prod baseline unpolluted.)*
4. **`ingest` overwrite hydrate → prod URI**, build indexes, record the ledger row. *(Net-new dataset appears; zero existing-asset blast radius.)*
5. **`modal deploy`** the app (dispatcher-resolvable) + **one Trigger.dev control-plane line** (weekly overwrite). *(No new endpoints/secrets.)*
6. **Repoint `bridge_dsbs_pdl_linkedin`** (and any future DSBS×SAM consumer) at `crosswalk_dsbs_sam` — retire the ad-hoc web-signal union. *(Downstream simplification; the LinkedIn bridge stops re-deriving the join.)*
