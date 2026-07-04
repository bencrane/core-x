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

## 6. Next Cycle

1. **In-session Clay identity match** → populate `gtm_sam_person_identity` via `append_matches()` (validated append, PK-deduped, match lineage mandatory). Target: `clay_find_people` (1,273,516 rows @ v128 at probe time — live-probe at match time).
2. Post-match: person-level acceptance query (people → identity → phone_resolutions / work_emails → MV payloads).
3. Award rollup satellite — remains post-spine, operator-gated (decision log #2).
