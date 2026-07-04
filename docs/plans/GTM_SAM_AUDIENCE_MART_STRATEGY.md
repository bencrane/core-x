# GTM SAM Audience Mart — Strategy Memo

**Date:** 2026-07-04
**Status:** PROPOSED — for alignment before build
**Scope:** New Lance dataset family for building contactable audiences of people at SAM.gov entities (subawardees, DSBS firms, primes), with award-based prioritization and contact-resolution state.
**Non-goal:** Touching, replacing, or migrating `active/people`, `active/people_canonical`, or any existing canonical dataset. Existing datasets are read-only inputs.

---

## 1. Objective

Two queries must become trivial:

1. **Entity audience:** "All UEIs that won a subaward in the last 18 months with sub-lifetime $1–10M, active 8(a), 11–50 employees" — one filtered scan.
2. **People audience:** "The govt-business POCs and top-paid officers at those UEIs, split into: mobile-ready / has-email-needs-mobile / has-LinkedIn-needs-email / name-only-needs-LinkedIn" — one join, with the enrichment work-queue falling out of the same query.

Today neither is possible because three facts live in three disconnected planes: **who the people are** (POC/officer names keyed by UEI, no LinkedIn), **what the entity has won** (award facts keyed by UEI, no people), and **what contact data exists** (emails/phones keyed by `person_id`/`linkedin_url`, no UEI). The mart is the join fabric across those planes.

---

## 2. Grounded Current State

All inputs already exist in `s3://data-sink/active/`. Nothing below requires new ingestion.

### 2.1 Entity plane (keyed by UEI)

| Dataset | Rows | What it contributes |
|---|--:|---|
| `sam_master_entities` | ~1.54M | 1/UEI, all v2 fields: naics, psc, business_types, sba cert codes, `is_active`, entity_url |
| `sam_master_domains` | ~709k | (normalized_domain, uei) reverse index, mailbox/placeholder/platform blocklists applied |
| `sba_dsbs_certified_firms` | ~67.2k | 1/UEI, active/prev cert flags (8a, HUBZone, WOSB, EDWOSB, VOSB, SDVOSB), firm email/phone/website, bonding |
| `crosswalk_dsbs_sam` | ~67k | DSBS↔SAM resolution + `best_domain`, `pdl_company_id`, `company_linkedin_url` already attached |
| `bridge_sam_pdl` | — | SAM↔PDL company bridge (existing) |
| `pdl_normalized_companies` | 35.4M | Blocking keys: `normalized_domain`, `linkedin_slug`, `company_name_norm` → `employee_size_range`, industry, `linkedin_url` |
| `firmographics_blitz` | 133k | Domain-keyed web enrichment (26.9% coverage of SAM domains) |

### 2.2 Award plane (keyed by UEI)

| Dataset | Rows | What it contributes |
|---|--:|---|
| `usaspending_fpds_prime_award_state` (L2) | 82.9M | 1/award; `recipient_uei`; `life_to_date_obligated` (**only award-grain SUM-safe amount**); `current_end_date` |
| `usaspending_fpds_canonical_txn` (L1) | 108M | Txn grain; `federal_action_obligation` SUM-safe by `action_date` → windowed obligations (12m/24m) |
| `usaspending_subaward_canonical` | 1.32M | Sub grain; `subawardee_uei` 100% fill; `subaward_amount` SUM-safe; `subaward_action_date`; `prime_awardee_uei` |
| `ffata_exec_comp` | ~29.5k | (recipient_uei, officer_rank) → prime top-5 officer names, `name_key` BTREE'd |
| `contractor_award_summary` | 579k | Existing recipient rollup — predates L2; treated as legacy, superseded by mart rollups |

Grain hazards honored throughout: only `life_to_date_obligated`, `delta_federal_action_obligation`, `federal_action_obligation` (txn), and `subaward_amount` are SUM-safe; `prime_award_*` context repeated at sub grain is never aggregated without dedup to `prime_award_unique_key`.

### 2.3 People-mention plane (keyed by UEI, **no LinkedIn**)

| Source | Rows | Person roles |
|---|--:|---|
| `sam_pocs` | 8.07M (4.37M v2/UEI-keyed) | govt_business, electronic_business, past_performance (+alts); verbatim names + title, `name_key` |
| `dsbs_pocs` | variable | contact_person + current_principals (with titles), NFKD order-independent `name_key` |
| `ffata_exec_comp` | ~29.5k | Prime top-paid officers |
| `usaspending_subaward_canonical` officers 1–5 | ~10–13% of 1.32M subs | Sub-awardee top-paid officers (FFATA, verbatim) |
| `sam_labor_poc_people` | 29.5k | uei → staffing POC **with LinkedIn URL already** |

### 2.4 Contact-supply plane (keyed by person_id / linkedin_url — **no UEI**)

| Dataset | Rows | Keys |
|---|--:|---|
| `clay_find_people` | 818k | `record_id`, `person_id`, `linkedin_url_norm`, title, company name/domain |
| `work_emails` | 139.8k | `person_id`, `email_norm`, `company_domain`, MV verification status |
| `phone_resolutions` | 89.0k | `person_id`, `phone`, `person_linkedin_url`, `company_domain` |
| `active/people` | 98.8k | `person_id` → `person_linkedin_url` mapping (read-only lookup; known dup issue is irrelevant for this use) |

### 2.5 The gap

- No dataset has grain **person@entity**. POCs, principals, and officers exist only inside their source datasets.
- No person-level LinkedIn/email/phone attachment for any POC/officer population (except the 29.5k `sam_labor_poc_people`).
- No entity scorecard combining registration + DSBS certs + prime/sub award rollups + firmographics in one filterable row.
- No recorded state for "what enrichment step is next for this person" — which is why sequencing (LinkedIn→email→mobile vs email→mobile) is currently guesswork.

---

## 3. Design Principles

1. **The mart is derived and disposable.** It is a serving layer, never a system of record. Every column traces to a canonical upstream. Recovery path = full rebuild. This is what makes "build new, leave existing as-is" safe.
2. **UEI is the entity spine; (uei, name_key) is the person spine.** No cross-entity human fusion in v1 — name-only fusion across companies is a false-merge machine. `person_linkedin_url_norm`, once resolved, is the only cross-dataset person key.
3. **Zero-alteration names survive.** Verbatim name strings are carried in the evidence layer; `name_key` (NFKD, order-independent, per the `dsbs_pocs` builder) is a derived accelerator, never the record.
4. **Domain is the company-matching backbone.** `sam_master_domains.normalized_domain` ⋈ `pdl_normalized_companies.normalized_domain` ⋈ `clay_find_people` company domain ⋈ `work_emails.company_domain` ⋈ `phone_resolutions.company_domain` — one normalization convention end-to-end.
5. **Snapshot rebuild, versioned overwrite.** Rollups and match states churn; the mart rebuilds as a unit (Lance overwrite = new version, prior versions retained). Append-only semantics live in the canonical layer, not here.
6. **Every dataset gets an ops ledger** (`ops.gtm_*_runs` in HQX, existing pattern) and pre/post-write gates (row floors, key-uniqueness, fill-rate, per-run Δ bounds).

---

## 4. The Mart — Five Datasets

Namespace: `s3://data-sink/active/gtm_*`.

### 4.1 `gtm_entity_universe` — entity spine + scorecard

**Grain:** 1 row per UEI. **Universe = union**, not intersection: `sam_master_entities` UEIs ∪ distinct `subawardee_uei` ∪ DSBS UEIs ∪ distinct prime `recipient_uei` (award-side UEIs may have lapsed SAM registrations — subawardee targeting must not silently drop them). Est. ~2.0–2.5M rows.

| Block | Columns |
|---|---|
| Identity | `uei` (PK), `cage_code`, `legal_business_name`, `normalized_legal_name`, `dba_name` |
| Presence flags | `in_sam`, `sam_is_active`, `in_dsbs`, `is_prime_recipient`, `is_subawardee` |
| Registration | `registration_status`, `expiration_date`, `purpose_of_registration` |
| Classification | `primary_naics`, `psc_codes`, `business_types`, `physical_state`, `physical_zip5` |
| Domain | `normalized_domain`, `domain_source` (sam_entity_url \| dsbs_best_domain \| firmographics) |
| PDL / firmographics | `pdl_company_id`, `company_linkedin_url`, `employee_size_range`, `pdl_industry`, `year_founded` |
| DSBS certs | `active_8a`, `active_hubzone`, `active_wosb`, `active_edwosb`, `active_vosb`, `active_sdvosb` (+ `prev_*`), `cert_programs` |
| Prime rollups | `prime_award_count`, `prime_lifetime_obligated`, `prime_obligated_12m`, `prime_obligated_24m`, `last_prime_action_date`, `active_prime_count` (via `current_end_date`), `top_awarding_agency_code` |
| Sub rollups | `sub_award_count`, `sub_lifetime_amount`, `sub_amount_12m`, `sub_amount_24m`, `last_subaward_date`, `distinct_prime_partner_count` |
| Derived | `award_role` (prime_only \| sub_only \| both \| none), `lifetime_band`, `band_24m` (categorical: 0, <250k, 250k–1M, 1–10M, 10–100M, >100M) |
| Contactability (denorm from 4.3/4.4) | `n_people`, `n_people_linkedin`, `n_people_work_email`, `n_people_mobile` |
| Meta | `build_id`, `built_at`, per-block `as_of` labels |

**Rollup sources:** prime lifetime from `prime_award_state.life_to_date_obligated` grouped by `recipient_uei`; windowed prime from L1 spine `federal_action_obligation` by `action_date`; sub rollups from `subaward_canonical.subaward_amount` by `subawardee_uei` / `subaward_action_date`.

**Indexes:** BTREE `uei`, `normalized_domain`, `company_linkedin_url`, `primary_naics`; BITMAP `award_role`, `lifetime_band`, `band_24m`, `employee_size_range`, all cert flags, presence flags, `physical_state`.

### 4.2 `gtm_people_evidence` — every person mention, verbatim

**Grain:** 1 row per (uei, source_dataset, source_role, source_slot). Append-only union of the five mention sources. Est. ~5M rows.

Columns: `evidence_uid` (sha256 of grain), `uei`, `source_dataset`, `source_role` (govt_business \| electronic_business \| past_performance \| dsbs_contact \| dsbs_principal \| exec_comp_prime \| exec_comp_sub \| labor_poc), `slot_no`, `full_name_verbatim`, `first_name`, `last_name`, `title`, `name_key`, `firm_email` (DSBS only), `firm_phone` (DSBS only), `linkedin_url_norm` (labor_poc only), `officer_amount` (FFATA only), `source_as_of`, `ingested_build_id`.

Sub-officer extraction: unpivot `subawardee_highly_compensated_officer_{1..5}_name/_amount` keyed to `subawardee_uei`, deduped latest per (uei, name_key) by `subaward_action_date`.

**Indexes:** BTREE `uei`, `name_key`; BITMAP `source_dataset`, `source_role`.

### 4.3 `gtm_people` — resolved person@entity

**Grain:** 1 row per (uei, name_key). PK `sam_person_id = sha256(uei || '|' || name_key)` — deterministic, stable across rebuilds. Est. ~2.5–3M rows.

Columns: `sam_person_id` (PK), `uei`, `name_key`, `display_name` (best verbatim: DSBS principal > SAM POC > officer), `first_name`, `last_name`, `best_title` (priority: dsbs_principal > sam_poc > none), role flags (`is_govt_poc`, `is_ebiz_poc`, `is_past_perf_poc`, `is_dsbs_contact`, `is_dsbs_principal`, `is_exec_officer_prime`, `is_exec_officer_sub`, `is_labor_poc`), `n_sources`, `max_officer_amount`, `firm_email`, `first_seen`, `last_seen` + **denormalized hot entity filters** (`award_role`, `band_24m`, `lifetime_band`, `in_dsbs`, `cert_programs`, `employee_size_range`, `sam_is_active`) — safe to denormalize because the mart rebuilds as one unit from one `build_id`.

**Indexes:** BTREE `sam_person_id`, `uei`, `name_key`; BITMAP all role flags + denormalized filters.

### 4.4 `gtm_person_contacts` — identity resolution + contact state

**Grain:** 1 row per `sam_person_id`. The contact-resolution state machine. Rebuilt on its own cadence (this is the volatile layer; separating it contains blast radius from matcher changes).

| Block | Columns |
|---|---|
| Key | `sam_person_id` (PK), `uei`, `name_key` |
| LinkedIn | `person_linkedin_url_norm`, `linkedin_match_method` (labor_poc_direct \| clay_name_domain \| clay_name_company_li \| manual), `linkedin_match_score` |
| Email | `work_email`, `email_norm`, `email_status` (MV verdict), `email_source` (seeded_work_emails \| vendor), `email_matched_via` |
| Mobile | `mobile_phone`, `phone_status`, `phone_source`, `phone_matched_via` |
| State | `resolution_stage` (MOBILE_READY \| EMAIL_ONLY \| LINKEDIN_ONLY \| NAME_ONLY), `next_action` (NONE \| RESOLVE_MOBILE \| RESOLVE_EMAIL \| RESOLVE_LINKEDIN), `last_resolved_at`, `build_id` |

`resolution_stage`/`next_action` are **derived, not asserted** — recomputed each build from which fields are populated. This directly answers the sequencing question: the work queue for "buy LinkedIn matches" is `WHERE next_action = 'RESOLVE_LINKEDIN'`, filtered by whatever entity criteria matter that week.

**Indexes:** BTREE `sam_person_id`, `uei`, `person_linkedin_url_norm`, `email_norm`; BITMAP `resolution_stage`, `next_action`, `linkedin_match_method`, `email_status`, `phone_status`.

### 4.5 `gtm_audience_log` — reproducible audience builds

**Grain:** 1 row per (audience_id, sam_person_id), append-only. Columns: `audience_id`, `audience_name`, `definition_json` (the filter predicate, verbatim), `sam_person_id`, `uei`, `resolution_stage` at build time, `mart_build_id`, `built_at`. Purpose: audiences are reproducible artifacts, exports are auditable, and re-contact suppression ("already in audience X") is a join, not tribal memory.

**Indexes:** BTREE `audience_id`, `sam_person_id`; BITMAP `resolution_stage`.

---

## 5. Matching & Seeding Plan

### 5.1 LinkedIn resolution (fills 4.4 `person_linkedin_url_norm`)

Tiered, highest-precision first; each tier only touches rows unresolved by prior tiers; every hit records method + score:

- **Tier A — direct:** `sam_labor_poc_people` (uei, name → LinkedIn). ~29.5k, effectively free.
- **Tier B — clay name×domain:** block on `gtm_entity_universe.normalized_domain` = clay company domain, match `name_key` (exact key, then high-threshold similarity). Clay's 818k rows are BTREE'd on domain; DuckDB hash join, trivially in-core.
- **Tier C — clay name×company-LinkedIn:** block on `company_linkedin_url` (entity's PDL LinkedIn) = clay company LinkedIn slug, match `name_key`. Catches entities whose domain missed but whose PDL match landed.
- **Tier D (explicitly deferred):** external Find-People enrichment for unresolved high-value rows — this becomes a *purchasable work queue*, not a build blocker.

### 5.2 Email/phone seeding (before buying anything new)

- `phone_resolutions` → normalize `person_linkedin_url` → join on resolved `person_linkedin_url_norm`. Direct.
- `work_emails` → `person_id` → `linkedin_url_norm` via `clay_find_people.person_id`, fallback `active/people` (`person_id` → `person_linkedin_url`, used strictly as a read-only lookup). Residual fallback: `email_norm` domain = entity domain AND local-part ≈ name_key tokens (recorded as `email_matched_via='domain_localpart'`, lower confidence).
- Expected effect: the existing 140k emails / 89k mobiles get *situated* — for the first time it is knowable which of the mobiles already owned belong to people at subawardees in a target band.

### 5.3 What is deliberately not matched

No name-only matching across entities, no fuzzy company-name matching where a domain or LinkedIn block is absent (PDL `company_name_norm` blocking exists as a Tier-C' option later if coverage demands it — decision deferred until Tier A–C coverage is measured).

---

## 6. The Canonical Audience Query (post-build)

```sql
-- "People to contact at recent mid-band subawardees that are 8(a), 11-50 employees"
SELECT p.sam_person_id, p.display_name, p.best_title, e.legal_business_name,
       c.resolution_stage, c.mobile_phone, c.work_email, c.next_action
FROM gtm_people p
JOIN gtm_entity_universe e USING (uei)
JOIN gtm_person_contacts c USING (sam_person_id)
WHERE e.is_subawardee
  AND e.last_subaward_date >= current_date - INTERVAL 18 MONTH
  AND e.band_24m IN ('1M-10M')
  AND e.active_8a
  AND e.employee_size_range = '11-50'
  AND (p.is_govt_poc OR p.is_exec_officer_sub OR p.is_dsbs_principal);
```

Segmenting the result by `resolution_stage` yields, in one pass: the contact-now list and the three enrichment purchase queues, priced and prioritized by the entity's award band.

---

## 7. Build Sequence

Ordered by dependency and standalone value; each phase ships, gates, and merges independently.

1. **`gtm_entity_universe`** — pure recombination of existing canonicals. Delivers the *entity* audience capability on its own (the "build the group of sam entities" ask). Highest value : effort ratio; zero matching risk.
2. **`gtm_people_evidence` + `gtm_people`** — mechanical union/unpivot/aggregate. Delivers "who are the people" with role and entity context, before any contact resolution.
3. **`gtm_person_contacts`** — Tier A–C LinkedIn resolution + email/phone seeding. Delivers resolution stages and the purchase queues. Coverage report (per-tier hit rates, per-band contactability) is a required gate artifact of this phase.
4. **`gtm_audience_log`** + saved audience recipes; backfill `n_people_*` contactability counts onto `gtm_entity_universe`.

Blast-radius note: phases 1–2 are deterministic transforms of canonical data; phase 3 is where matcher judgment lives. Keeping 4.4 a separate dataset means a bad matcher run is rolled back by rebuilding one table, never touching the spine.

---

## 8. Ops Model

- **Orchestration:** Modal detached, one job per dataset, DuckDB → `lance.write_dataset` (overwrite mode, new Lance version) → index build → gates → ledger row. Same skeleton as `sam_pocs.py` / `usaspending_subaward_canonical.py`.
- **Ledgers:** `ops.gtm_entity_universe_runs`, `ops.gtm_people_runs`, `ops.gtm_person_contacts_runs`, `ops.gtm_audience_log_runs` — standard columns + phase-specific counters (tier hit counts, stage distribution).
- **Gates (representative):** universe row floor ≥1.8M and distinct-UEI==rows; people distinct (uei,name_key)==rows; contacts PK uniqueness; `resolution_stage` distribution Δ ±20% vs prior run; linkedin match-score floor per tier; per-run Δ bounds on every rollup total.
- **Cadence:** universe + people monthly (tracks SAM extract cycle); contacts on demand after any enrichment batch lands; audience_log event-driven.
- **Resource envelope:** largest single build input is the L1 spine scan for windowed obligations (108M rows, projected to 3 columns) — standard `memory_limit` + NVMe `temp_directory` spill config; everything else is ≤10M-row joins, in-core.

---

## 9. Explicit Non-Goals (v1)

- No modification of `active/people` / `people_canonical` or their Phase-2 plans.
- No global human identity (one person across multiple UEIs stays two rows until LinkedIn proves otherwise — and even then fusion is a v2 decision).
- No grant-subaward coverage (subaward canonical is contract-only today; mart inherits that scope and widens automatically when the canonical does).
- No replacement of `contractor_award_summary` consumers; it is simply not used as an input.

## 10. Open Questions for Alignment

1. **Band edges** — proposed: 0 / <250k / 250k–1M / 1–10M / 10–100M / >100M, applied to both `lifetime_band` and `band_24m`. Confirm against actual targeting tiers.
2. **Universe breadth** — proposed union includes all prime `recipient_uei`s (~adds hundreds of thousands of non-SAM-active entities). Alternative: SAM ∪ subawardee ∪ DSBS only, with primes flagged via rollups. Proposed default: full union (flags make narrowing free; widening later is a rebuild).
3. **Tier-B name-match threshold** — exact `name_key` join first (zero-risk), similarity tier behind a score column so precision is tunable at query time rather than baked into the build. Confirm appetite for the similarity tier in v1 or exact-only first.
4. **`gtm_audience_log` placement** — Lance (proposed, keeps everything queryable in one plane) vs HQX Postgres (transactional suppression checks). Proposed: Lance for members, definitions duplicated into the ledger row.
