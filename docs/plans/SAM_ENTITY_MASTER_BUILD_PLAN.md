# SAM Entity Master — Build Plan

Plan of record for **building the `sam_master_*` entity family** — a derived, deduped,
fully-materialized master + satellites — from the raw `entity_registrations` snapshot stack.
Authored against the live system of record (`s3://data-sink/active/`) and the official SAM
Feb-2025 layout; every number is measured, not inferred.

**Scope is the datasets.** This plan produces three Lance datasets in R2 and stops. Who reads
them, repointing any existing consumer, and any rename / relocation / "sealing" of raw are
**explicitly out of scope** (§9) — separate, later work.

---

## 1. Objective

From the raw 19.3M-row stacked-snapshot `entity_registrations`, materialize a clean entity family:

- `sam_master_entities` — one **golden row per entity**, carrying every field SAM publishes in
  the public extract, named and typed (NAICS / URL / address / POC data lifted out of the
  positional `pipe_fields` array);
- grain-separated **satellites** — `sam_master_contacts`, `sam_master_domains` — sharing the
  entity key.

Success = the three datasets exist in R2, BTREE-indexed and validated against the live source,
with **zero loss** of any field SAM publishes and **zero interpretive renaming**.

---

## 2. Inputs (verified live this session)

| Input | State |
|---|---|
| Raw `s3://data-sink/active/entity_registrations/` | 19,299,314 rows · 26 monthly snapshots · Lance v2.0 · BTREE(uei, cage_code, extract_label) |
| v2 universe (all-time, = build scope) | **1,541,566 distinct UEI** |
| Latest snapshot (`2026_MAY`) | 884,203 rows · 876,399 distinct UEI · 789,945 active-uei rows |
| Legacy (pre-2020) | 7,720,531 rows · CAGE-keyed · no UEI · 476,484 entities never seen in v2 |
| Official layout | `landing/sam-gov/data-dictionary/entity-information/SAM_MASTER_EXTRACT_MAPPING_Feb2025.xlsx` — parsed → **142-field public layout, 5/5 validation checks pass vs live** |
| Existing thin master | `s3://data-sink/active/sam_entity_master/` — v3 · 782,543 rows · 17 cols (the broad `sam_master_entities` is its complete replacement) |

The master xlsx describes the **362-field master/sensitive** record; our public extract is the
**142 Public-flagged fields, re-sequenced** (220 sensitive fields — banking/EFT/TIN, financials,
IGT, agency hierarchy, parent-entity linkage, POC phone/email — are redacted from our source and
are **not recoverable here**).

---

## 3. Locked decisions

1. **Scope = v2-only.** Every entity has a UEI → key is `uei`; the CAGE↔UEI identity-resolution
   problem does not arise. Legacy (476,484 pre-2020-only entities) is left in raw, untouched — a
   speculative future build (§9), not part of this one. *(Confirm — §8.)*
2. **"Most up-to-date" grain.** One row per `uei` = its latest row across all v2 snapshots
   (`ORDER BY last_update_date DESC, registration_date DESC`). No per-snapshot status timeline
   (cheap tenure aggregates are a separate confirm — §8).
3. **Entity key = `uei`.**
4. **Build topology.** One multi-output worker, **single raw scan** → entities + contacts +
   domains, all at the same snapshot vintage, published atomically (§6).
5. **Field authority.** The committed, validated 142-field map. **Zero reverse-engineering.**
6. **Faithful naming.** Exact SAM dictionary field names (§5a). **No interpretive renames.**

---

## 4. The family (names locked)

Built from raw `s3://data-sink/active/entity_registrations/` (read-only; unchanged by this plan):

| Dataset | Grain | Key | Est. rows | Indexes |
|---|---|---|---|---|
| `sam_master_entities` | **1 / uei** (golden record, all 142 public fields) | `uei` | ~1.54M | BTREE(uei, primary_naics, cage_code) |
| `sam_master_contacts` | 1 / (uei, poc_type), ≤6 / uei | `uei` | dry-run | BTREE(uei) |
| `sam_master_domains` | 1 / (normalized_domain, uei) | `normalized_domain` | dry-run | BTREE(normalized_domain, uei) |

`is_active` is a **materialized boolean column** on `sam_master_entities` (`sam_extract_code='A'`
AND in the latest snapshot) — not a separate dataset, not a query each reader re-derives.

**Naming convention** (locked): served family = `<source>_master_<grain>`; the `raw_<source>_…`
prefix is reserved for any future raw relocation; `crosswalk_*` / `bridge_*` unchanged.

---

## 5. Field → column projection (exact)

Derived from the validated map. Three buckets.

### 5a. Naming rule — faithful mirror
Column = the **exact SAM dictionary field name**, one deterministic slug (lowercase ·
non-alphanumeric → `_` · collapse/trim). **No semantic renames, no abbreviations.** The Phase-0
field map carries `position → dictionary_name → column_name`, so every column is 1:1 traceable
to the dictionary. Two documented choices (not interpretation): `UNIQUE ENTITY ID` → **`uei`**
(the cross-stack join key; `cage_code` is already exact), and `*_DATE` fields cast STRING→`date`
(lossless, same value). Government warts ride verbatim.

### 5b. Master scalar columns (1/entity, exact names)
```
uei(1)  cage_code(4)  sam_extract_code(6)  purpose_of_registration(7)
initial_registration_date(8)  registration_expiration_date(9)  last_update_date(10)
activation_date(11)  entity_start_date(25)  fiscal_year_end_close_date(26)
legal_business_name(12)  dba_name(13)  entity_division_name(14)  entity_division_number(15)
physical_address_line_1(16)  physical_address_line_2(17)  physical_address_city(18)
physical_address_province_or_state(19)  physical_address_zip_postal_code(20)
physical_address_zip_code_4(21)  physical_address_country_code(22)
physical_address_congressional_district(23)
entity_url(27)  entity_structure(28)  state_of_incorporation(29)  country_of_incorporation(30)
primary_naics(33)
business_type_counter(31)  bus_type_string(32)  naics_code_counter(34)  naics_code_string(35)
psc_code_counter(36)  psc_code_string(37)
mailing_address_line_1(40)  mailing_address_line_2(41)  mailing_address_city(42)
mailing_address_zip_postal_code(43)  mailing_address_zip_code_4(44)
mailing_address_country(45)  mailing_address_state_or_province(46)
naics_exception_counter(113)  naics_exception_string(114)  debt_subject_to_offset_flag(115)
exclusion_status_flag(116)  sba_business_types_counter(117)  sba_business_types_string(118)
no_public_display_flag(119)  disaster_response_counter(120)  disaster_response_string(121)
entity_evs_source(122)
entity_eft_indicator(3)  dodaac(5)  d_b_open_data_flag(24)  credit_card_usage(38)
correspondence_flag(39)
+ materialized: is_active
+ provenance (not from the record): sam_extract_label · source_file
```
Each `*_string` list ships **both** verbatim (raw, `~`-delimited — fidelity) **and** as a parsed
LIST sibling — `naics_codes`, `psc_codes`, `business_types`, `sba_business_types`,
`disaster_response` (usability; the verbatim column is never replaced). **Dropped** (structural
noise only): deprecated blank(2), parent-EVS sources(123–126), flex(127–141), end-marker(142),
raw `pipe_fields`.

### 5c. Contacts satellite (6 POC blocks → rows)
Positions 47–112 = six 11-field blocks. Emit one row **per non-empty block**:

```
poc_type ∈ { govt_business(47–57), alt_govt_business(58–68),
             past_performance(69–79), alt_past_performance(80–90),
             electronic_business(91–101), alt_electronic_business(102–112) }
columns: uei · poc_type (defined enum) · first_name · middle_initial · last_name · title ·
         st_add_1 · st_add_2 · city · zip_postal_code · zip_code_4 · country_code · state_or_province
```
Field names = the exact dictionary names **minus** the moved block qualifier (`GOVT BUS POC
FIRST NAME` → `first_name`); `poc_type` carries the block losslessly. No phone/fax/email —
redacted from the public extract.

### 5d. Domains satellite
`normalized_domain ← norm(entity_url, pos 27)`, reusing the canonical normalizer +
`CONSUMER_BLOCK` from `pipelines/resolution/sam_fmcsa_domain_spine.py` (`_norm_host_sql`).
One row per `(normalized_domain, uei)`.

---

## 6. Execution phases

**Phase 0 — Freeze the field map.**
Emit the validated 142-field map to `pipelines/sam_gov/reference/sam_v2_public_field_map.py`
(committed dict) + `.json`. The build imports it to generate the projection — the xlsx stays in
landing as provenance, never read at runtime. *Deliverable: committed artifact + a unit test
asserting the 5 live invariants (count=142, uei@1, url@27, !end@142, ≥1.4M distinct uei).*

**Phase 1 — Entity master worker.**
New `pipelines/sam_gov/sam_master.py` (Modal app `sam-gov-master-pipelines`): scan raw
`filter format_family='v2'`, columns = the projected scalars + `pipe_fields` → DuckDB project
(§5a + §5b from the frozen map) → `QUALIFY row_number() OVER (PARTITION BY uei ORDER BY
last_update_date DESC NULLS LAST, registration_date DESC NULLS LAST)=1` → `sam_master_entities`
Arrow table. BTREE(uei, primary_naics, cage_code). Row floor ≥ 1,400,000. **`--dry-run` first**
(counts + KIPPER full-profile spot-check, no write). The existing thin `sam_entity_master.py` is
left in place until this is verified, then removed.

**Phase 2 — Contacts + domains (same scan).**
The Phase-1 worker is multi-output: the one raw scan also produces `sam_master_contacts` (§5c)
and `sam_master_domains` (§5d) Arrow tables.

**Atomic publish (Phases 1–2).** Materialize all three Arrow tables in-memory **first**, then
write; wrap the three R2 overwrites in a restore-all-on-failure guard (pattern:
`crosswalk_sam_usaspending.py:576`) so a partial failure never leaves a split-vintage dataset.
Per-dataset BTREE + `ops.sam_master_runs` row + floors.

**Phase 3 — Control plane.**
`src/trigger/sam_master.ts` — durable waitpoint → `MODAL_DISPATCHER_URL` → worker. No cron;
rebuild triggered manually or on new-extract landing. (Build + validate via `modal run` in
Phases 1–2 first; wire Trigger only after the compute is proven.)

---

## 7. Validation gates (no phase ships without its gate)

- **Field map:** 5/5 live invariants (frozen as a Phase-0 test).
- **Master:** uei **uniqueness** (exactly 1 row/uei); `count(distinct uei) ≈ 1.54M`; row floor
  ≥ 1.4M; **±25% row-delta guard** vs the prior build; per-column **null-rate floors**;
  **in-worker position re-assert** (url@27 / !end@142 on a live sample before projecting);
  KIPPER (`DD1BCRF2QQG8`) full-profile match; 3 BTREEs present in the manifest.
- **Contacts / Domains:** non-empty floors from dry-run; **domain-coverage floor** (~46%, GOVCON
  single-vintage caveat); a known uei resolves ≤6 POCs and its domain round-trips.
- **Publish:** atomic (all-or-nothing across the 3 datasets); `ops.*` row written;
  sub-100 ms point-lookup smoke test.

---

## 8. Open items to confirm before Phase 1

1. **Scope** — v2-only (recommended), or include legacy now (pays the full identity-resolution
   + dual-layout complexity for the 476k stale cohort).
2. **Temporal scalars** — add cheap same-scan tenure/freshness aggregates (`first_seen_label`,
   `last_seen_label`, `snapshot_count`, `ever_inactive`), or hold to a pure latest-state mirror.
   These are **not** the per-snapshot status timeline that's excluded; `initial_registration_date`
   is already a native field. Recommended: add.

*Resolved: names locked (§4); faithful naming (§5a); build amendments folded in from the review (§10).*

---

## 9. Explicitly out of scope

- **Consumers and raw.** Who reads the family, repointing any existing consumer
  (`crosswalk_sam_usaspending`, `sam_pocs`, `sam_fmcsa_domain_spine`) off raw, and any rename /
  relocation / "sealing" / access-enforcement on raw — **all a separate, later effort.** This
  plan builds datasets and stops. Raw `entity_registrations` is read-only and untouched.
- **Legacy spine** (476k pre-2020 entities) — speculative future build (`sam_master_legacy`,
  cage-keyed, no official 120-field dict); left in raw untouched.
- `CLAUDE.md` / `PROTOCOL.md` `dex-raw-landing-zone` + Polaris reconciliation — *after.*
- Exclusions + FASCSA extracts — a separate feed.
- Email / banking / parent-entity linkage from SAM — **not in the public source.**

---

## 10. Review amendments (adversarial review · 2026-06-04)

Source: [`SAM_ENTITY_MASTER_BUILD_PLAN_REVIEW.md`](SAM_ENTITY_MASTER_BUILD_PLAN_REVIEW.md).
Adopted, build-scope only:

- **Atomic publish** across the 3 outputs (§6) — no partial-vintage system of record.
- **Arrays verbatim AND parsed** (§5b) — fidelity + usability, additive; preserves the working
  projection at `sam_entity_master.py:131-137`.
- **Materialized `is_active`** (§4) instead of a re-derived filter.
- **Hardened gates** (§7) — uei uniqueness, null-rate floors, in-worker position re-assert,
  ±25% delta guard, domain-coverage floor.
- **Legacy = speculative** (§9), not a committed follow-up.

Pending confirm: **temporal scalars** (§8).

The review's seal-raw / consumer-repoint findings (its B1/B2) are **moot for this plan** — that
scope was removed (§9). They belong to the separate consumer/raw effort, if and when it happens.
