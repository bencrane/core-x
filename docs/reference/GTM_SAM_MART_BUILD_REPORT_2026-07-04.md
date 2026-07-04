# GTM SAM Audience Mart — Cycle 1 Build Report

**Date:** 2026-07-04
**Strategy:** docs/plans/GTM_SAM_AUDIENCE_MART_STRATEGY.md (v2, aligned)
**Outcome:** SHIPPED — all three datasets live in R2, all gates passed, acceptance verified in pure DuckDB over Lance.

---

## 1. Datasets Shipped

| Dataset | URI | Rows | Grain | Indexes |
|---|---|--:|---|---|
| `gtm_sam_entities` | `s3://data-sink/active/gtm_sam_entities/` | 2,025,707 | 1/uei | BTREE uei, normalized_domain, primary_naics, cage_code · BITMAP in_sam, sam_is_active, in_dsbs, is_subawardee, is_prime_recipient, physical_state, domain_source |
| `gtm_sam_people_evidence` | `s3://data-sink/active/gtm_sam_people_evidence/` | 4,523,673 | 1/(uei, source, role, slot, name_key) | BTREE uei, name_key · BITMAP source_dataset, source_role |
| `gtm_sam_people` | `s3://data-sink/active/gtm_sam_people/` | 2,252,457 | 1/(uei, name_key) | BTREE sam_person_id, uei, name_key · BITMAP 7 role flags |
| `gtm_sam_person_identity` | `s3://data-sink/active/gtm_sam_person_identity/` | 0 (by design) | 1/sam_person_id | built on first append |

**Builders (committed):** `pipelines/gtm/gtm_sam_entities.py`, `pipelines/gtm/gtm_sam_people.py`, `pipelines/gtm/gtm_sam_person_identity.py`. Plain-python cores (in-session runnable) + Modal wrappers. Ledgers: `ops.gtm_sam_entities_runs`, `ops.gtm_sam_people_runs`, `ops.gtm_sam_person_identity_runs` — each run records **input lineage (URI, Lance version, live row count at read)**.

## 2. Universe Composition (build 20260704T202234-ee9d8531)

| Component | Count |
|---|--:|
| Total UEIs | 2,025,707 |
| in_sam | 1,541,566 (== `sam_master_entities` v8 rows, gate-verified) |
| is_prime_recipient | 766,803 |
| is_subawardee | 105,189 |
| in_dsbs | 67,234 (== DSBS input rows, gate-verified) |
| FFATA-only residue | 51 |
| legal_business_name fill | 99.96% |
| normalized_domain fill | 720,895 (SAM entity_url 1st, DSBS best_domain fallback, source recorded) |

People build (20260704T202927-97fe0e0b): evidence = sam_pocs v2 4,373,317 + dsbs_pocs 102,742 + ffata 29,616 + subaward officers 17,998 (deduped latest per (uei, name_key)); people = 2,252,457 across 1,542,235 UEIs; **orphan people UEIs vs spine = 0** (referential-integrity gate).

Past-performance POCs confirmed present via `sam_pocs`: `past_performance` 347,954 + `past_performance_alt` 212,714 v2 rows, carried with role flag `is_past_perf_poc`.

## 3. Examination Gates (pre-build, read-only, live-probed 2026-07-04)

- **`bridge_sam_pdl` v5 — PASS with discipline.** 801,831 rows / 463,741 UEIs / max 96 rows-per-uei (DUNS-location fan-out) / 98.7% single-`pdl_company_id` → consumers MUST collapse to 1/uei (QUALIFY). No committed builder (docs-only provenance) — flagged, unchanged since the 2026-06-30 audit. Not consumed by the spine build.
- **`crosswalk_dsbs_sam` v24 — PASS clean.** Exactly 1/uei (67,234); `best_domain` 53,066; `company_linkedin_url` 28,467. Consumed for the DSBS domain fallback only.
- **Hierarchy quantified:** 21,455 domains span >1 UEI (max 2,649); 19,051 PDL companies span >1 UEI (max 2,650). Family sharing is structural — spine treats domain as attribute, no fusion.
- **Deviation from memo v2:** `sam_master_entities` carries **no parent fields** (SAM public v2 extract has none) — the verbatim-parent block was dropped; hierarchy joins on demand via `resolution/entity_hierarchy`.
- **MV chain:** `work_email_mv_validations` v10, 77,570 rows, BTREE on email/person_id/mv_resultcode — raw-payload filtering is a point-lookup hop. `work_emails` v62, 142,374 rows, BTREE email_norm/company_domain/person_id (and now carries `person_linkedin_url` + inline MV fields).

## 4. Acceptance (pure DuckDB over Lance — the bar)

Query: *active 8(a) DSBS subawardees, subaward within 24 months, $1–10M sub-24m band* (spine bitmap filters + live `subaward_amount` aggregation + `sba_dsbs_certified_firms` join):

- **178 entities** → **418 people** (250 govt POCs, 227 DSBS principals, 70 sub officers, 133 past-perf POCs), names + titles resolved.
- Owned contact supply at those 178 (domain-keyed): **63 entities with ≥1 owned work email** (99 emails), **94 with ≥1 owned mobile** (125 phones).
- Spine BTREE point-lookup: 1 row in 727ms cold from R2 (first-seek; consistent with prod cold-seek behavior on fresh indices).

Person-level email/mobile linkage activates when the identity match phase populates `gtm_sam_person_identity` (next cycle, in-session, supervised).

## 5. Operational Notes

- **pylance ≥ 8 required.** pylance 7.0.0 panics in lance-encoding (`chunk_bytes <= max_chunk_size`, primitive.rs:4063) writing this table shape; 8.0.0 writes clean. Modal images pinned `pylance>=8`.
- **`LANCE_BYPASS_SPILLING=true` required** for index builds (bitmap index external-sorter pool exhaustion otherwise); already standard in repo Modal images — required locally too.
- Writer feeds a bounded `RecordBatchReader` (128k-row batches), not a monolithic table.
- Sub-officer dedup tiebreak made total-order (`action_date DESC, amt DESC, slot_no, nm`) after observing ±36-row drift between identical-input runs; next rebuild is exactly reproducible.

## 6. Cycle 2 — Identity Match (executed same day)

**Tool:** `pipelines/gtm/match_sam_person_identity.py` — supervised in-session
matcher; dry-run report first, `--apply` for the validated append. Tier A only:
exact `name_key` × domain, two co-equal sources adjudicated before append
(never sequential first-wins).

**Inputs (live at match):** `gtm_sam_entities` v12 · `gtm_sam_people` v11 ·
`clay_find_people` v128 (1,273,516) · `blitz_find_people` v20 (549,175 —
whale-pattern coverage; carries `company_linkedin_url`, which Clay lacks).

**Adjudication (1,149,820 domain-bearing people):**

| Verdict | Count | Disposition |
|---|--:|---|
| corroborated (both sources, same slug) | 785 | accepted, score 1.0 |
| clay_only (unique) | 69,770 | accepted, score 0.9 |
| blitz_only (unique) | 752 | accepted, score 0.9 |
| conflict (both unique, different) | 88 | EXCLUDED |
| ambiguous (>1 slug within a source) | 3,501 | EXCLUDED |

**Applied:** 71,307 rows → `gtm_sam_person_identity` (v6), indices built on
first append, per-row method/score/source-record lineage; ledger row carries
both sources' URI + version + rows at match time.

**Person-level acceptance (pure DuckDB over Lance):** of 71,307 matched
people — 13,321 have owned `phone_resolutions` rows, 7,973 owned
`work_emails`, 16,391 any contact. Proof slice: **2,712 DSBS-subawardee
POC/principal/officer rows with owned mobiles**, names + titles + phones
resolving through the full chain.

Notes: (a) `phone_resolutions` includes failed attempts (null phone) —
audience queries filter `phone IS NOT NULL`; (b) MV payload vocabulary is raw
by design (`mv_resultcode` is numeric) — contactability verdicts remain
query-time operator criteria, never baked.

## 7. Cycle 2b — Tier B (name_key × company-LinkedIn, residue-scoped)

**Tool:** `pipelines/gtm/match_sam_person_identity_tier_b.py` (dry-run default,
`--apply` gated). Residue: 1,078,513 domain-bearing unmatched people across
698,757 entities.

**Entity-side company-LinkedIn resolution worked well:** 428,359 residue
entities (61%) resolved a company slug via dsbs-crosswalk (uei-direct) >
PDL sidecar (35.4M, `is_generic_domain` excluded) > `clay_find_companies`
domain votes, cross-carrier conflicts excluded. This intermediate is
recomputed per run — full-spine materialization of entity→company-LinkedIn
remains a separate, unapproved decision.

**Person yield thin, as expected** — Tier A had already harvested the
name×domain overlap; company-LI only adds source rows whose domain diverges
from the entity's: **90 accepted (blitz_only, score 0.85)**, 111 ambiguous
abstained. Applied → `gtm_sam_person_identity` **71,397 rows (v7)**.

**Structural finding — Clay people↔companies FK is dead in the Lance mirrors:**
`clay_find_people.company_record_id` is a Clay-native row ref (`r_…`) with no
counterpart in `clay_find_companies` (`record_id` = sha hash → 0-row join;
`clay_company_id` = int). Clay-side Tier B is blocked on provenance until a
usable key ships upstream. Recorded, not forced.

## 8. Cycle 3 — Owned-Identity Harvest (executed same day)

**Tool:** `pipelines/gtm/harvest_owned_identity.py` (dry-run default,
`--apply` gated). Sources — uei-direct, already paid for, zero new spend:

- `dsbs_poc_linkedin` v32 (821 rows) — serper-resolved, name-AND-company
  validated DSBS POC LinkedIn (spend-ledgered; provenance:
  docs/reference/DSBS_POC_LINKEDIN_RESOLUTION.md). All 821 joined the spine;
  all new. Method `dsbs_serper_validated`, score 0.95.
- `sam_labor_poc_people` v11 (29,464 rows) — staffing-segment derivative;
  name-agreement filter kept 29,464/29,464 (built name-consistent —
  provenance exam passed). Method `labor_poc_direct`, score 0.90.

**Applied: +1,037 → `gtm_sam_person_identity` 72,434 rows (v8).**
2,049 ambiguous abstained. Foreign `name_key` columns were not trusted —
mart convention recomputed from verbatim names on both sources.

**Precision certificate (unplanned but decisive):** 23,584 existing Tier-A
rows were independently CONFIRMED by `sam_labor_poc_people`'s own resolution;
only 56 conflicts (0.24% disagreement, read-only, flagged — recomputable via
the tool's dry-run). Two independent processes converging at 99.76% validates
the exact name×domain method.

**Acquisition channel registered (operator-gated, not run):** the serper
pipeline (`resolve_dsbs_poc_linkedin.py`, credit-metered via
`core/serper_gateway.py`, ~29.6% resolve rate) is the repeatable paid queue
for the DSBS identity residue.

## 9. Cycle 3b — Homepage Meta Crawl (executed same day)

**Motivation (12-entity live test):** meta tags are not firmographic-payload
equivalents — when both exist, Blitz `about` is strictly richer 5/6 times
(meta drops the 8(a)/WOSB/SDVOSB + HQ hooks) — but they fill the
no-description gap at zero vendor cost and liveness-classify domains.

**Dataset:** `s3://data-sink/active/web_homepage_meta/` — append-only,
1 row/(normalized_domain, run_id), raw strings never normalized; BTREE
`normalized_domain`, BITMAP `liveness_class`. Builder:
`pipelines/gtm/web_homepage_meta.py` (worklist = DSBS award-active window,
domain-bearing, undescribed; 30-day recrawl skip; ledger
`ops.web_homepage_meta_runs` with input lineage).

**Run crawl-20260704T214305:** 6,095 domains → **ok 2,957 (48.5%)** ·
no_meta 2,186 · unreachable 792 (13.0%) · parked/default 160. The 952
dead/parked domains are personalization-hygiene flags in their own right.

**Description funnel after crawl (loose audience, spine v24):**
14,425 → 13,402 with domain → 8,463 vendor-described (Blitz `about` carries
98% of it; Clay unique adds <2%) → **10,677 described by any source (74.0%)**.
Priority order at query time: Blitz `about` > Clay > `web_homepage_meta`
(liveness_class='ok') > none.

## 10. Cycle 3c — DSBS Firm-Email → Person Rulings (executed same day)

**Dataset:** `s3://data-sink/active/gtm_sam_person_firm_emails/` (v5) —
**37,441 rulings**, 1 row per (uei, email) attributed to exactly one
`sam_person_id`. Builder `pipelines/gtm/gtm_sam_person_firm_emails.py`
(snapshot overwrite, deterministic; ledger
`ops.gtm_sam_person_firm_emails_runs`). Email SoR remains
`sba_dsbs_certified_firms.email` — this table is the match result only
(identity-table doctrine). BTREE sam_person_id/uei/email_norm; BITMAP
match_tier. `email_norm` joins directly to `work_emails`/MV Lances.

**Matcher:** alpha-only local-part canon; tiers t1_full_name 0.95 (8,793) ·
t2_initial 0.90 (10,565) · t3_single_name 0.85 (10,959) · t4_containment
0.70-0.75 (7,124); nickname dictionary; candidates = all gtm_sam_people at
the uei; unique-best-only written. Excluded and ledger-counted: 3,913
generic mailboxes, 2,026 surname-ties, 272 true ambiguities, 15,309
unmatched. Versus the old existence-only 69% check: person-grade attribution
for 63.5% of all DSBS emails, 19,358 at ≥0.90.

**Residue prioritization:** of 17,607 un-ruled non-generic emails, only
**3,344 sit at firms with award activity in the 2-year window** (2,841 under
strict won-a-prime) — the only slice worth further matching effort.

**Normalization hazard (recorded):** `regexp_replace(x,'[^a-z]')` BEFORE
`lower()` silently empties ALL-CAPS names — first measurement pass read 26%
instead of 67% until corrected. lower→strip_accents→strip is the only valid
order (the mart's `name_key_sql` already does this).

## 11. Next

1. **Cycle 4 — full-spine entity→company-LinkedIn materialization**
   (`gtm_sam_company_identity`, 1/uei): supersedes fan-out-prone
   `bridge_sam_pdl` for employee-range filtering (spine → company identity →
   `pdl_normalized_companies.employee_size_range`) and keys the
   LeadMagic/Blitz firmo + description thread (`clay_find_companies` carries
   `description`; PDL does not). Tier-B tool already computes this
   residue-scoped at 61% hit rate.
2. Award rollup satellite — PARKED pending the parallel award-workstream
   agents (operator instruction 2026-07-04).
