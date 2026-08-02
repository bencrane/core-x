# Healthcare GTM Sidecar Tier — Build-Scope Document

**Status:** scope only. No build fired, no builder/manifest edits. This document decides what
gets promoted into the query-sidecar; execution rides the normal `/sidecar-gaps` →
`/sidecar-build` cycle.
**Date:** 2026-08-01. Every column named below was read from a live Lance schema via pylance
(R2, `s3://data-sink/active/`) on this date — nothing is guessed.

---

## 1. Demand statement

Operator directive (this IS the demand evidence): a PE roll-up / practice-intelligence thesis
over healthcare providers requires lightning-fast on-screen GTM querying. The sidecar today is
133 tables of federal-contracting GTM with **zero** healthcare marts; every healthcare question
currently scans Lance cold.

Per the mid-investigation directive, the tier is scoped **opportunistically and greedily**
across client archetypes — PE acquirers, direct-mail partners, equipment/supply distributors,
practice lenders / equipment finance, insurance brokers (malpractice, P&C/BOP at formation),
payer network development, recruiting/staffing. The unifying prerogative: precise targeting —
confidence that a list row is the RIGHT-fit practice/provider for a client's mandate. Greed
applies to columns-per-mart and archetype coverage; structural mart count stays disciplined.

Query families designed against:

- **PE screen:** independent practices, specialty X, state/metro Y, 2–9 providers, owner
  vintage 15+ yr, not absorbed by a platform, ranked by Medicare $.
- **Direct mail:** practice addresses + authorized-official name for specialty × ZIP3 cut,
  deduped, mail-ready.
- **Consolidation monitoring:** rosters shrinking / re-pointing to platform billing anchors;
  platform footprint map for specialty × region.
- **Zoom moves:** name it (practice + official names) · split it (specialty/geo/size/vintage)
  · trend it (delta months — **parked**, see §5) · join it (NPI→payments, NPI→group).

## 2. Probed inventory

| Dataset (`active/…`) | Rows | Grain | Key columns (verified) | Freshness | Partition shape |
|---|---:|---|---|---|---|
| `nppes_provider/snapshot=2026-05/` | 9,551,447 | 1/npi | npi, entity_type_code, is_active, provider_name/organization_name, primary_taxonomy_code, practice_address_line1/2, practice_city/state/zip5, practice_phone, mailing_city/state/zip5 (no mailing street line), enumeration_date/year, deactivation/reactivation_date, authorized_official_first/last_name + title, is_sole_proprietor, is_organization_subpart, parent_organization_lbn | 2026-05 | snapshot-partitioned; **only `snapshot=2026-05` on disk** |
| `nppes_provider_taxonomy/snapshot=2026-05/` | 11,952,809 | 1/(npi, taxonomy_rank) | npi, taxonomy_rank, taxonomy_code, is_primary, license_number, license_state, taxonomy_group | 2026-05 | snapshot-partitioned (2026-05 only) |
| `provider_360/snapshot=2026-06/` | 9,551,447 | 1/npi | 152 cols: full NPPES identity/geo/vintage block; med_a1_* Medicare Part B (latest/lifetime $, growth pcts, panel risk/dual/dx shares, by-year JSON strings); med_b1_*/rx_* Part D; svc_*; dme_*; op_* Open Payments; mips_*; enrollment_enrlmt_ids (list), pecos_asct_cntl_id, practice_group_count, largest/smallest_practice_group_enrlmt_id + size + org_name, is_independent_candidate | derived 2026-06 | snapshot-partitioned; **only `snapshot=2026-06` on disk** |
| `practice_group_360/snapshot=2026-06/` | 253,740 | 1/group_enrlmt_id | group_enrlmt_id, org_name, group_state, member_count, member_npis (list), total_medicare_paid_usd, total_rx_cost_usd, avg_panel_risk_score, avg_dual_share, top_specialty, distinct_specialties, avg_mips_score, independent_member_count, total_op_payments_usd, n_states, avg_pymt_yoy_pct, avg_pymt_cagr_3yr_pct, active_member_count | derived 2026-06 | snapshot-partitioned (2026-06 only) |
| `cms_provider_enrollment` | 2,981,788 | 1/(npi, enrlmt_id) | npi, enrlmt_id, pecos_asct_cntl_id, multiple_npi_flag, provider_type_cd/desc, state_cd, first/last/org_name | PECOS snapshot col | flat |
| `cms_provider_enrollment_npi` | 111,196 | enrlmt_id↔npi supplement | enrlmt_id, npi | PECOS | flat |
| `cms_provider_enrollment_practice` | 1,080,813 | 1/enrlmt_id location | enrlmt_id, city_name, state_cd, zip_cd | PECOS | flat |
| `cms_provider_enrollment_reassignment` | 3,857,023 | 1/reassignment edge | reasgn_bnft_enrlmt_id (individual) → rcv_bnft_enrlmt_id (group) | PECOS | flat |
| `cms_provider_payment_rollup` | 1,603,039 | 1/npi | general/research totals + counts, ownership records/value, distinct_manufacturers, total_payments_usd, first/last_payment_year | built rollup | flat |
| `cms_physician_provider` | 13,528,933 | 1/(npi, program_year) | rndrng_npi, program_year (2013–2024), tot_mdcr_pymt_amt, tot_benes, bene_avg_risk_scre, dual counts, dx-% block, RUCA | 2013–2024 | flat |
| `cms_general_payments` | 87,655,770 | 1/payment record | covered_recipient_npi, amount, date, manufacturer, nature/form, product cols ×5 | multi-year | flat |
| `form5500_main` | 19,114 | 1/filing (ACK_ID) | SPONS_DFE_EIN, SPONSOR_DFE_NAME, addresses, participant counts, BUSINESS_CODE | filing years | flat |
| `nppes_taxonomy_ref` | 883 | 1/taxonomy_code | taxonomy_code, grouping, classification, specialization, display_name, section | ref | flat |

Probe failures (recorded): `provider_360/` and `practice_group_360/` do **not** exist at
`snapshot=2026-05` — the 360 layer exists only at `snapshot=2026-06`, while the NPPES bedrock
partitions on disk are `2026-05` only (provider_360 carries an `nppes_snapshot` column for
provenance). The 06/07 NPPES gate failure means the bedrock is pinned at 2026-05; the tier
serves the newest partition that exists per dataset and stamps it in the manifest.

## 3. Proposed marts (4 structural, greedy columns)

All joins below are **pure equality keys** (npi, enrlmt_id, taxonomy_code) per build doctrine —
no CASE-derived keys needed anywhere in this tier. List-typed columns (`member_npis`,
`enrollment_enrlmt_ids`, `all_taxonomy_codes`) are dropped from projections; membership is
served relationally by `hc_practice_roster`.

### 3.1 `hc_practice_screen` — ACCEPT (the PE-screen anchor)
- **Grain:** 1/group_enrlmt_id · **253,740 rows** (exact parity vs `practice_group_360`).
- **Sort:** `group_state, top_specialty, group_enrlmt_id`.
- **Source:** `practice_group_360/snapshot=2026-06` LEFT JOIN `cms_provider_enrollment_practice`
  ON `group_enrlmt_id = enrlmt_id` (equality; practice is 1/enrlmt_id — 1:1, row-preserving).
- **Columns:** all 17 non-list practice_group_360 columns (org_name, group_state, member_count,
  active_member_count, independent_member_count, total_medicare_paid_usd, total_rx_cost_usd,
  avg_panel_risk_score, avg_dual_share, top_specialty, distinct_specialties, avg_mips_score,
  total_op_payments_usd, n_states, avg_pymt_yoy_pct, avg_pymt_cagr_3yr_pct) + PECOS location
  riders `city_name`, `state_cd`, `zip_cd` (practice ZIP → ZIP3 cuts without a per-NPI hop).
- **Serves:** "independent practices, specialty X, state Y, 2–9 providers, ranked by
  Medicare $" as a single-table predicate: `member_count BETWEEN 2 AND 9 AND
  independent_member_count/member_count high AND top_specialty=… ORDER BY
  total_medicare_paid_usd DESC`.
- **Parity:** row-preserving, exact.

### 3.2 `hc_provider_screen` — ACCEPT (provider-grain + mail-cut in one)
- **Grain:** 1/npi · **9,551,447 rows** (exact parity vs `provider_360`).
- **Sort:** `practice_state, primary_taxonomy_code, npi` (screens prune on state+specialty;
  npi point-reads scan ≤9.5M native rows — ms-class, no second sort copy earned yet).
- **Source:** `provider_360/snapshot=2026-06` (projection ~60 of 152 cols) LEFT JOIN
  `nppes_provider/snapshot=2026-05` ON `npi` (equality, 1:1) for the address/phone fields
  provider_360 dropped, LEFT JOIN `nppes_taxonomy_ref` ON
  `primary_taxonomy_code = taxonomy_code` (equality, 1:1) for specialty display names.
- **Columns (all verified in source schemas):**
  - Identity/precision: npi, entity_type_code, entity_type, is_active, provider_name,
    organization_name, last_name, first_name, credential, sex_code, is_sole_proprietor,
    is_organization_subpart, parent_organization_lbn (platform-absorption string signal).
  - Specialty: primary_taxonomy_code, taxonomy_slot_count, primary_license_state + ref riders
    `display_name`, `classification`, `grouping` (renamed taxonomy_display_name etc.).
  - Mail/geo (the mail-cut lives here): practice_address_line1, practice_address_line2,
    practice_city, practice_state, practice_zip5, practice_phone, mailing_city, mailing_state,
    mailing_zip5 (NPPES carries **no mailing street line** — practice address is the mail
    address; disclose), authorized_official_first/last_name, authorized_official_title.
    Dedup key for mail: `(organization_name, practice_address_line1, practice_zip5)`.
  - Vintage: enumeration_date, enumeration_year, last_update_date, deactivation_date,
    reactivation_date ("owner vintage 15+ yr" = `enumeration_year <= 2011` for
    entity_type_code='1').
  - Medicare headline: has_a1, med_a1_latest_year, med_a1_latest_mdcr_pymt,
    med_a1_latest_benes, med_a1_lifetime_mdcr_pymt, med_a1_active_latest,
    med_a1_pymt_yoy_pct, med_a1_pymt_cagr_3yr_pct, med_a1_pymt_growth_2019_latest_pct,
    med_a1_panel_avg_risk_score, med_a1_dual_share, med_a1_provider_type.
  - Rx/DME/OpenPayments/MIPS riders: has_rx, rx_total_drug_cost_usd, rx_top1_generic,
    is_dme_supplier, dme_supplied_total_paid_usd, has_op, op_total_payments_usd,
    op_has_ownership_interest, op_distinct_manufacturers, has_mips, mips_final_score,
    mips_clinician_specialty.
  - Group linkage (consolidation): has_ffs_enrollment, pecos_asct_cntl_id,
    practice_group_count, largest_practice_group_enrlmt_id, largest_practice_group_size,
    largest_practice_org_name, smallest_practice_group_enrlmt_id,
    smallest_practice_group_size, smallest_practice_org_name, is_independent_candidate.
- **Parity:** row-preserving, exact (both joins 1:1 on unique keys).

### 3.3 `hc_provider_taxonomy` — ACCEPT (specialty membership, long)
- **Grain:** 1/(npi, taxonomy_rank) · **11,952,809 rows** (exact parity vs source).
- **Sort:** `taxonomy_code, license_state, npi` (specialty-first pulls: "every NPI holding
  taxonomy T in state S", full multi-specialty, not just primary).
- **Source:** `nppes_provider_taxonomy/snapshot=2026-05` LEFT JOIN `nppes_taxonomy_ref` ON
  `taxonomy_code` (1:1) LEFT JOIN `nppes_provider/snapshot=2026-05` ON `npi` (1:1) for the
  two prune riders `practice_state`, `entity_type_code`, `is_active`.
- **Columns:** npi, taxonomy_rank, taxonomy_code, is_primary, license_number, license_state,
  taxonomy_group + ref `display_name`, `classification`, `grouping`, `section` + the three
  provider riders.
- **Parity:** row-preserving, exact.

### 3.4 `hc_practice_roster` — ACCEPT (reassignment edges, resolved)
- **Grain:** 1/reassignment edge · **3,857,023 rows**.
- **Sort:** `group_enrlmt_id, member_npi` (roster reads are group-anchored; member-side entry
  goes through `hc_provider_screen.largest_practice_group_enrlmt_id`).
- **Source:** `cms_provider_enrollment_reassignment` with both sides resolved via
  `cms_provider_enrollment` on `enrlmt_id` (equality). **Fan-out control:**
  `cms_provider_enrollment` is 1/(npi, enrlmt_id) and carries `multiple_npi_flag` — the
  resolution legs MUST be pre-aggregated to 1/enrlmt_id (arg-max or any-value per enrlmt_id,
  same pattern provider_360 §4.7 uses) before joining, or edge rows multiply.
- **Columns:** member_enrlmt_id (=reasgn_bnft_enrlmt_id), member_npi, member first/last name,
  member provider_type_desc, member state_cd, group_enrlmt_id (=rcv_bnft_enrlmt_id),
  group_npi, group org_name, group state_cd + group location riders (city_name, zip_cd from
  `cms_provider_enrollment_practice`, 1:1 on enrlmt_id).
- **Serves:** consolidation monitoring — "which groups gained/lost members vs the practice
  screen's member_count", platform footprint (one org_name/pecos_asct_cntl_id across states),
  "who bills through platform anchor X".
- **Parity:** **derived/aggregate-legged** — edge count must still equal 3,857,023 (the base
  is row-preserving; only the resolution legs aggregate). Gate on the edge count exactly;
  treat the mart as aggregate-class in the manifest so the parity gate compares against the
  reassignment source count.

### Candidates evaluated — accept/reject line each
| Candidate | Verdict |
|---|---|
| Practice-grain screen mart | **Accept** — `hc_practice_screen` (§3.1). |
| Provider-grain slim mart | **Accept** — `hc_provider_screen` (§3.2), widened to a provider_360 projection rather than raw nppes_provider: same row count, vastly more archetype coverage per doctrine (columns are free on a projection). |
| Taxonomy long mart | **Accept** — `hc_provider_taxonomy` (§3.3); primary-only filtering loses multi-specialty targeting precision. |
| Practice-roster mart | **Accept** — `hc_practice_roster` (§3.4); the only relational membership path once list columns are dropped. |
| Payments rollup rider | **Accept as columns, reject as mart** — `cms_provider_payment_rollup` is already denormalized onto provider_360 (`op_*`); rides §3.2 free. |
| Mail-cut mart | **Reject as separate mart** — address + authorized-official + dedup keys ride `hc_provider_screen`; a distinct grain earns nothing. |
| form5500 employer mart | **Reject** — EIN/plan grain with no NPI/enrlmt key into the provider graph; separate concern, and `form5500_main` is only 19k rows (trivially Lance-served if ever needed). |
| `cms_general_payments` row-level (87.7M) | **Reject** — the rollup + provider_360 riders serve the GTM question; row-level manufacturer-payment forensics is Lance work. |

## 4. Adjacency sweep (per join, per doctrine — evaluated across ALL client archetypes)

- **§3.2 npi → nppes_provider:** the projection already touches the row, so ride everything
  any two archetypes use: address_line2 (mail), practice_phone (recruiting/staffing +
  lender outreach), name_prefix/suffix + credential (mail salutation), sex_code (recruiting),
  certification_date (vintage corroboration — **ride it**, it is in the schema). Analyst's
  next moves: "give me the phone" (covered), "is this address current?" → last_update_date
  (covered), "ZIP3 cut" → substr(practice_zip5,1,3) at query time (covered).
- **§3.2 primary_taxonomy_code → taxonomy_ref:** ride `section` and `specialization` too
  (payer network dev filters on specialization granularity). 883-row ref — free.
- **§3.1 group_enrlmt_id → enrollment_practice:** zip_cd is the only geo PECOS carries at
  practice grain; next move "practice street address" is NOT in PECOS extracts — served by
  joining the roster's group_npi into `hc_provider_screen` (equality, in-sidecar). Covered.
- **§3.4 enrlmt_id → enrollment:** ride `pecos_asct_cntl_id` on BOTH sides — the PACID is the
  platform-absorption anchor (one owner control id across many groups = platform footprint;
  targeting-confidence priority per directive). Ride `multiple_npi_flag` (dedup confidence).
- **Cross-mart next moves simulated:** PE screen row → "name the members" = roster read →
  "each member's Medicare book + vintage" = provider_screen IN-list — all equality,
  all in-tier. Direct-mail cut → dedup on the §3.2 key → done single-table. Consolidation →
  roster GROUP BY group_enrlmt_id vs practice_screen.member_count — in-tier. **True roster
  DELTAS (shrank vs last month) need a second PECOS snapshot — structural-gated, parked (§5).**
- **Parked structural adjacencies:** per-year Medicare trend table
  (`cms_physician_provider`, 13.5M × year grain) — provider_360's growth pcts + by-year JSON
  strings cover the screen question; a year-grain mart waits for demand evidence.

### Client-archetype coverage matrix
| Mart | PE acquirer | Direct mail | Equip/supply dist. | Lender/equip finance | Insurance broker | Payer network dev | Recruiting/staffing |
|---|---|---|---|---|---|---|---|
| `hc_practice_screen` | screen + rank | list seed (group grain) | account targeting by size/specialty | practice-size/revenue proxy | book-of-business sizing | group adequacy gaps | client-side demand map |
| `hc_provider_screen` | vintage/independence/$; absorption flags | THE mail file (address+official+dedup) | DME/device users (is_dme_supplier, dme_$) | equipment spend proxy (dme_*, svc riders via 360) | formation signals (enumeration_date, new-org) | specialty/geo/panel fit | provider identity+contact+credential |
| `hc_provider_taxonomy` | specialty precision | specialty × state cuts | modality targeting | specialty risk tiers | license state (malpractice) | full multi-specialty adequacy | credential/license sourcing |
| `hc_practice_roster` | roster size/absorption | official-per-group resolution | multi-site account mapping | group stability | group formation/turnover | network membership truth | placement targets (who works where) |

## 5. Parked / rejected

- **NPPES delta marts (trend it):** PARKED. The NPPES derived layer is pinned at `2026-05`;
  the `2026-06`/`2026-07` bedrock snapshots gate-failed and do not exist on disk. Delta marts
  (enumeration/deactivation month-over-month, roster shrinkage trend) are
  **blocked on the NPPES 06/07 gate fix as the explicit prerequisite** — do not fake trend
  from a single snapshot.
- **PECOS roster deltas:** parked with the same shape — needs a retained second PECOS
  snapshot; today only one lives under `active/`.
- **TiC rate marts:** rejected here — blocked on production fan-out; separate workstream.
- **Snapshot-skew caveat (not parked, disclosed):** the tier joins provider_360 (built on
  NPPES 2026-05, published under snapshot=2026-06) with nppes_provider 2026-05 — internally
  consistent, but the manifest must pin the exact partition URIs, not "latest".
- Speculative rejects: cms_partd/physician service-line marts, DME supplier marts, EPA-style
  facility spines — no query family named them.

## 6. Build-cost honesty

| Mart | Rows | Est. size | Parity class |
|---|---:|---|---|
| `hc_practice_screen` | 253,740 | ~40 MB | row-preserving, exact |
| `hc_provider_screen` | 9,551,447 | ~2.5–3.5 GB (~60 mixed cols) | row-preserving, exact |
| `hc_provider_taxonomy` | 11,952,809 | ~0.8–1.2 GB | row-preserving, exact |
| `hc_practice_roster` | 3,857,023 | ~0.4 GB | aggregate-legged; edge-count-exact gate |

Total ≈ **4–5 GB** on a multi-GB artifact; ~25.6M rows against the existing 1.83B (+1.4%).
Build-time impact on the ~32-min build: the marts are small-to-mid CTAS with 1:1 hash joins —
estimate **+3 to +6 minutes** (dominated by the 9.5M×60-col provider_screen sort). All four
are single-sort (no second sort copies), so recurring cost stays minimal. Every join must be
EXPLAIN-gated in `test_fixture_explain.py` through the dispatch path per doctrine; the
snapshot-partitioned sources need the partition path in the manifest `ds` field (a manifest
capability check — the current manifest addresses flat dataset names; flag for the builder
pass, not changed here).

## 7. Open questions for the operator (genuine forks only)

1. **Snapshot-pinned vs latest-partition serving:** pin the tier to the audited
   `provider_360/snapshot=2026-06` + NPPES `2026-05` pair explicitly, or auto-serve the
   newest partition present at build time (risks silently mixing vintages when the NPPES
   gate is fixed and 07/08 land)?
2. **Roster mart direction:** one sort copy (group-anchored, as scoped) — or is
   member-anchored entry ("which platform does Dr. X bill through, from a name") hot enough
   to earn a second sort copy now (+0.4 GB recurring)?
3. **Platform-absorption definition:** is `pecos_asct_cntl_id` concentration the committed
   platform signal (as scoped), or should `parent_organization_lbn` string-grouping be
   promoted to a first-class flag despite being name-based (weaker targeting confidence)?
