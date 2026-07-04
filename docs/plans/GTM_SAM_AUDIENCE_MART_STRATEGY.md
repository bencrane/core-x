# GTM SAM Audience Mart — Strategy (v2, ALIGNED)

**Date:** 2026-07-04 (v2 — supersedes v1 in full)
**Status:** ALIGNED — architecture settled through operator review; build not started
**Scope:** New Lance dataset family for querying people at SAM.gov (UEI-bearing) entities and joining them to owned contact data.
**Non-goals (hard):** No modification of existing Lance datasets. No precomputed award rollups in v1. No campaign/lead/contact-lifecycle semantics. No vendor-waterfall state encoding. No non-UEI entities.

---

## 1. Objective and Acceptance Test

The v1 definition of done is one query, runnable from gtm-mcp / a managed agent:

```sql
-- "People at SAM entities for whom mobile numbers (or work emails) already exist"
SELECT p.uei, e.legal_business_name, p.display_name, p.best_title, ph.phone
FROM gtm_sam_people p
JOIN gtm_sam_entities e USING (uei)
JOIN gtm_sam_person_identity i USING (sam_person_id)
JOIN phone_resolutions ph
  ON ph.person_linkedin_url_norm = i.person_linkedin_url_norm;
```

— filterable by any entity criterion (DSBS certs, subaward recency, prime activity) via query-time uei joins to the canonical Lances. Today this query is impossible; after the spine build plus the identity-match phase, it is a four-table join.

## 2. Scope Boundary

**Strictly UEI-keyed.** Every row in the mart carries a real UEI. "SAM.gov world" means UEI-bearing — lapsed registrants and subawardee UEIs that never completed registration are in; anything without a UEI is out. Rationale: UEI is the invariant that keeps every join deterministic (SAM, DSBS, POCs, primes, subawards all speak UEI natively); admitting non-UEI entities forces synthetic keys and entity resolution — the exact swamp this mart routes around. Non-SAM GTM stays in the existing company/people plane; a non-SAM spine, if ever warranted, is a sibling mart, never a widened key space here.

**People flow outward from SAM sources only.** `gtm_sam_people` rows originate exclusively from SAM-world mentions (POCs, principals, officers). `clay_find_people` is a match *target*: matching annotates existing rows via `gtm_sam_person_identity` and never inserts rows. Clay people at SAM entities who are not POCs/officers stay out permanently; coworker expansion, if ever wanted, is a query-time join, not an import.

## 3. Design Principles

1. **Thin spine; speak, don't bake.** The spine holds identity, presence flags, and classification. Attributes stay in their home Lances and are reached by query-time joins (uei, normalized_domain, linkedin). No award rollups, no firmographics, no contact data, no denormalized filters — anywhere in the mart.
2. **Materialize matches, join attributes.** The only thing that earns materialization is the *result of a computation that cannot run at query time* (fuzzy name matching). Everything retrievable by a keyed join stays where it lives.
3. **The mart is derived and disposable.** Every column traces to a canonical upstream; recovery path = full rebuild; snapshot rebuild via Lance versioned overwrite.
4. **Zero-alteration names.** Verbatim strings live in the evidence layer; `name_key` (NFKD, order-independent, per the `dsbs_pocs` convention) is a derived accelerator, never the record.
5. **No documented count is trusted; probe live.** Established by fact: `clay_find_people` was documented at ~818k rows and probed live at 1,273,516 (version 128, 2026-07-04) — it receives ongoing appends. Every build reads inputs live and records each input's URI, Lance version, and row count at read time in its ops ledger. Match runs additionally record the target dataset version they matched against.
6. **Domain is an attribute, never an identity.** Shared domains across UEIs are structurally expected (`sam_master_domains` is grained (domain, uei) for this reason); the same `company_linkedin_url` repeating across parent/subsidiary UEIs is correct behavior. The spine carries SAM's own parent fields verbatim and performs no family fusion. Hierarchy resolution is downstream work against an intact spine.
7. **Existing bridges must stand examination before use.** No blind reuse of `bridge_sam_pdl` / `crosswalk_dsbs_sam` (§6).

## 4. The Mart — Four Datasets

Namespace: `s3://data-sink/active/gtm_sam_*`. Names deliberately SAM-prefixed to avoid collision with other company/people Lances.

### 4.1 `gtm_sam_entities` — entity spine

**Grain:** 1 row per UEI. **Universe = union** of UEI-bearing sources: `sam_master_entities` UEIs ∪ distinct `subawardee_uei` (subaward canonical) ∪ DSBS UEIs ∪ distinct prime `recipient_uei` (prime_award_state). Presence flags record which sources contributed; the union is deliberate — subawardee targeting must not silently drop lapsed registrations.

| Block | Columns |
|---|---|
| Identity | `uei` (PK), `cage_code`, `legal_business_name`, `normalized_legal_name`, `dba_name` |
| Presence | `in_sam`, `sam_is_active`, `in_dsbs`, `is_subawardee`, `is_prime_recipient` |
| Registration | `registration_status`, `registration_date`, `expiration_date`, `purpose_of_registration` |
| Classification | `primary_naics`, `naics_codes[]`, `psc_codes[]`, `business_types`, `physical_city`, `physical_state`, `physical_zip5` |
| Domain | `normalized_domain`, `domain_source` (sam_entity_url \| dsbs_best_domain) |
| Hierarchy (verbatim) | SAM immediate/ultimate parent name + UEI fields, carried as-is, no fusion |
| Meta | `build_id`, `built_at`, per-source `as_of` labels |

**Not in this table:** award rollups (post-spine decision, §7), firmographics/employee range (query-time via examined bridge → `pdl_companies`), contact counts, DSBS cert detail (query-time join to `sba_dsbs_certified_firms` on uei — only the `in_dsbs` presence flag lives here).

**Indexes:** BTREE `uei`, `normalized_domain`, `primary_naics`, `cage_code`; BITMAP presence flags, `registration_status`, `physical_state`.

### 4.2 `gtm_sam_people_evidence` — verbatim mention layer

**Grain:** 1 row per (uei, source_dataset, source_role, slot_no). Lossless union of person mentions with provenance. Purpose: zero-alteration custody and auditability — when a Clay match is judged in-session later, evidence rows are what gets inspected ("which source said this title, as of when"). Also makes `gtm_sam_people` a pure re-derivable aggregation.

**Sources (four):** `sam_pocs` (v2/UEI-keyed rows), `dsbs_pocs`, `ffata_exec_comp`, `usaspending_subaward_canonical` officers 1–5 unpivoted (deduped latest per (uei, name_key) by `subaward_action_date`).
**Excluded:** `sam_labor_poc_people` — derivative with unverified provenance; anything it represented is reproducible post-spine as a single query.

Columns: `evidence_uid` (sha256 of grain), `uei`, `source_dataset`, `source_role` (govt_business | electronic_business | past_performance | dsbs_contact | dsbs_principal | exec_comp_prime | exec_comp_sub), `slot_no`, `full_name_verbatim`, `first_name`, `last_name`, `title`, `name_key`, `officer_amount` (FFATA only), `source_as_of`, `build_id`.

**Indexes:** BTREE `uei`, `name_key`; BITMAP `source_dataset`, `source_role`.

### 4.3 `gtm_sam_people` — person@entity spine

**Grain:** 1 row per (uei, name_key). **PK:** `sam_person_id = sha256(uei || '|' || name_key)` — deterministic, stable across rebuilds, minted the moment a POC appears; LinkedIn plays no role in identity. No cross-entity human fusion: the same human at two UEIs is two rows, by design.

Columns: `sam_person_id` (PK), `uei`, `name_key`, `display_name` (best verbatim: dsbs_principal > sam_poc > officer), `first_name`, `last_name`, `best_title` (same priority), role flags (`is_govt_poc`, `is_ebiz_poc`, `is_past_perf_poc`, `is_dsbs_contact`, `is_dsbs_principal`, `is_exec_officer_prime`, `is_exec_officer_sub`), `n_sources`, `max_officer_amount`, `first_seen`, `last_seen`, `build_id`.

**No denormalized entity attributes** (consistent with speak-don't-bake — entity filters are uei joins).

**Indexes:** BTREE `sam_person_id`, `uei`, `name_key`; BITMAP role flags.

### 4.4 `gtm_sam_person_identity` — materialized match results

**Grain:** 1 row per `sam_person_id` with a resolved identity. **Ships EMPTY at v1** — schema + loader only. Populated afterward by a separate, supervised, in-session match exercise against `clay_find_people` (and any other match source that stands examination). Not before the spine, not simultaneously.

Columns: `sam_person_id` (PK), `uei`, `name_key`, `person_linkedin_url_norm`, `match_source` (e.g. clay `record_id`), `match_method`, `match_score`, `matched_against_uri`, `matched_against_version` (Lance version of the target at match time), `matched_at`.

**Contact data is never copied here.** With identity resolved, contact Lances are reached where they live: `phone_resolutions` on its own `person_linkedin_url`; `work_emails` via `person_id → clay_find_people / active_people → linkedin`; MV verdicts in their own Lance.

**Indexes:** BTREE `sam_person_id`, `uei`, `person_linkedin_url_norm`; BITMAP `match_method`.

## 5. What the Mart Speaks To (query-time joins, never baked)

| Need | Join path |
|---|---|
| DSBS certs / firm email / bonding | `gtm_sam_entities.uei` → `sba_dsbs_certified_firms` |
| Subaward activity, amounts, recency | `uei` → `usaspending_subaward_canonical.subawardee_uei` (`subaward_amount` is the sub-grain SUM-safe column) |
| Prime activity | `uei` → `usaspending_fpds_prime_award_state.recipient_uei` (`life_to_date_obligated` is the award-grain SUM-safe column) |
| Employee range / industry / company LinkedIn | `uei` → examined `bridge_sam_pdl` → `pdl_companies` (DSBS path: `crosswalk_dsbs_sam`) |
| Mobile numbers | `gtm_sam_person_identity.person_linkedin_url_norm` → `phone_resolutions` |
| Work emails | identity → `person_id` map (`clay_find_people` / `active_people`, read-only) → `work_emails` |

Grain hazards inherited from the award workstream apply verbatim: only `subaward_amount`, `life_to_date_obligated`, `federal_action_obligation` (txn), `delta_federal_action_obligation` (mod) are SUM-safe; award-repeated context at sub grain is never aggregated without dedup to `prime_award_unique_key`.

## 6. Input Examination Gates (precede the build)

No blind reuse of existing bridges. Before the spine wires a join path through them, `bridge_sam_pdl` and `crosswalk_dsbs_sam` must pass: (a) key-grain verification (1/uei or documented otherwise); (b) match-method provenance recorded; (c) coverage counts vs. expectation; (d) spot-check sample review; (e) hierarchy quantification — UEIs per domain, UEIs per `pdl_company_id`, whether SAM parent/child UEI pairs resolve to the same PDL company. Failure ⇒ the spine ships without that path wired and the bridge is rebuilt as its own task. The spine build never blocks on bridge remediation.

## 7. Explicitly Post-Spine (not v1, not scaffolded)

1. **In-session Clay match** → populates `gtm_sam_person_identity`. Supervised, separate exercise; records target dataset version.
2. **Award rollup satellite** (1/uei, award-side owned). Open to it only after the bedrock works end-to-end; interim band queries run as live DuckDB aggregations. Nothing precomputed now — precomputed-too-early pulls from the wrong place as easily as the right one.
3. **Derivatives** — staffing-agency intersections, coworker views, `gtm clay find companies` / firmographics evaluation (only differentiator to check: description coverage). All become single queries against a correct spine.

## 8. Build Sequence & Ops

1. Bridge/crosswalk examination gates (§6) — read-only.
2. `gtm_sam_entities`.
3. `gtm_sam_people_evidence` + `gtm_sam_people`.
4. `gtm_sam_person_identity` schema + loader (empty).
5. **Acceptance test (§1)** — from gtm-mcp: the acceptance query returns correct rows (initially via the `person_id`-map email path; full result set after the match phase populates identity).

**Ops:** Modal detached per dataset; DuckDB → `lance.write_dataset` (overwrite = new version) → indexes → gates → ledger. Ledgers `ops.gtm_sam_entities_runs`, `ops.gtm_sam_people_runs`, `ops.gtm_sam_person_identity_runs` with standard columns + **input lineage (URI, Lance version, live row count at read time) per input**. Gates: distinct-key == rows on every table; row floors; name fill/alpha fractions; per-run Δ bounds.

## 9. Decision Log (alignment outcomes)

| # | Decision |
|---|---|
| 1 | Attributes (employee range etc.) never baked; reached via examined bridges. Bridges must stand examination first. Domain/company_linkedin_url shared across family UEIs = expected; no fusion; SAM parent fields carried verbatim. |
| 2 | No precomputed rollups in v1 — struck, not scaffolded. Live aggregation interim; satellite is a post-spine decision. `contractor_award_summary` not consulted. |
| 3 | Evidence layer kept (auditability for match judgment; re-derivable people table). First candidate to cut if leanness ever wins. |
| 4 | `sam_labor_poc_people` excluded from evidence — unverified derivative; reproducible post-spine. |
| 5 | `gtm_person_contacts` (v1 memo) dead: no copied contact data, no waterfall/stage state, no campaign semantics. Survivor = thin identity bridge, populated post-spine in-session. |
| 6 | `gtm_audience_log` dropped from scope. |
| 7 | Strictly UEI-keyed; non-SAM entities excluded permanently from this mart. |
| 8 | People originate from SAM sources only; Clay is a match target, never a row source. |
| 9 | Names: `gtm_sam_entities`, `gtm_sam_people`, `gtm_sam_people_evidence`, `gtm_sam_person_identity` — SAM-prefixed to avoid collision with other company/people Lances. |
| 10 | Live-probe rule: builds record input Lance version + row count at read time; documented counts untrusted (established by the clay 818k→1,273,516/v128 discrepancy). |
