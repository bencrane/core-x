# Subawardee Socioeconomic Designations — reference

**Date:** 2026-06-20 (UTC) · Two new datasets that decode SAM "Reps & Certs" into per-subawardee socioeconomic designation flags for zero-join GTM filtering.

| dataset | SoR | grain | shape | builder |
|---|---|---|---|---|
| `sam_business_type_code_dict` | `s3://data-sink/active/sam_business_type_code_dict/` | 1 row / (namespace, code) | 12 rows · 11 cols · 3 BTREE | `pipelines/serving/seed_sam_business_type_code_dict.py` |
| `govcon_subawardee_designations` | `s3://data-sink/active/govcon_subawardee_designations/` | 1 row / `subawardee_uei` | 25,450 rows · 22 cols · 4 BTREE + 13 BITMAP | `pipelines/serving/materialize_subawardee_designations.py` |

Both Lance v2.1, idempotent snapshot-overwrite. Read-only-safe to rebuild.

---

## 1. Why these exist

A federal subawardee's socioeconomic designation is **not** usable from the subaward row: the FSRS `subawardee_business_types` labels are transaction-reported and spotty — they carry **zero 8(a)** and almost no HUBZone, because those are SBA-*administered* certifications, not self-certs. The authoritative source is the **SAM bulk ingest Reps & Certs** (`entity_registrations` → `sam_master_entities`), where each registered entity's self- and SBA-certifications live as **coded** lists. These two tables decode those codes and attach them to every subawardee.

## 2. The decode problem & how it was solved

SAM stores reps & certs as 2-char **codes** in two separate namespaces, and ships **no value-level dictionary** in the public extract:

- `business_types` (← SAM master field 32 `BUS TYPE STRING`) — self-cert, well populated.
- `sba_business_types_string` (← field 118 `SBA BUSINESS TYPES STRING`) — SBA-administered cert; tokens are `<2-char-code><YYYYMMDD?>` (cert effective date welded on). **Only ~13% of entities populate it.**

The same token means different things across namespaces, so a flat decode silently conflates. The crosswalk was **derived empirically** — join `sam_master_entities.uei → govcon_active_awards.recipient_uei` and measure precision `P(flag|code)` / recall `P(code|flag)` of each code against the FPDS prime self-cert booleans (the operator's exact-named flags) — then **cross-validated against the official GSA SAM Functional Data Dictionary + Public V2 Extract Layout**. 9 of 11 codes confirmed verbatim; `A9`/`A0`/`JT` (post-2020 SBA WOSB/EDWOSB/8a-JV certs, absent from the legacy layout PDF) kept on empirical grounds.

`sam_business_type_code_dict` is the persisted legend — the join target that prevents namespace conflation.

### The crosswalk (content of `sam_business_type_code_dict`)

| namespace | code | designation | confidence | emp. p / r |
|---|---|---|---|---|
| business_types | `QF` | service_disabled_veteran_owned_business | gsa_confirmed | .97 / .93 |
| business_types | `A5` | veteran_owned_business | gsa_confirmed | .98 / .92 |
| business_types | `A2` | woman_owned_business | gsa_confirmed | .95 / .91 |
| business_types | `8W` | women_owned_small_business | gsa_confirmed | .92 / .91 |
| business_types | `27` | self_certified_small_disadvantaged_business | gsa_confirmed | .95 / .90 |
| business_types | `23` | minority_owned_business | gsa_confirmed | .94 / .92 |
| business_types | `8C` | joint_venture_women_owned_small_business | gsa_confirmed | .87 / .87 |
| sba_business_types_string | `A6` | c8a_program_participant **(8a)** | gsa_confirmed | .93 / .68 |
| sba_business_types_string | `XX` | historically_underutilized_business_zone_hubzone_firm **(HUBZone)** | gsa_confirmed | .90 / .68 |
| sba_business_types_string | `JT` | joint_venture_8a | empirical_validated | .87 / .58 |
| sba_business_types_string | `A9` | women_owned_small_business (SBA-cert) | empirical_validated | .95 / .37 |
| sba_business_types_string | `A0` | economically_disadvantaged_women_owned_small_business (SBA-cert) | empirical_validated | .69 / .32 |

## 3. `govcon_subawardee_designations` schema

Keyed by `subawardee_uei` (every distinct subawardee in `contract_subaward`; UEIs are never null). The 12 boolean designation columns reuse `govcon_active_awards`' recipient self-cert flag **names verbatim** — primes and subs filter with the identical vocabulary.

**Identity / match (8):** `subawardee_uei`, `subawardee_name` (latest from subaward), `sam_legal_business_name`, `cage_code`, `primary_naics`, `sam_is_active`, `matched_in_sam`, `n_subaward_rows`.

**Designation flags (12, bool):**
`service_disabled_veteran_owned_business` (QF), `veteran_owned_business` (A5∪QF), `women_owned_small_business` (8W∪A9∪A0), `economically_disadvantaged_women_owned_small_business` (A0), `woman_owned_business` (A2∪8W∪A9∪A0), `historically_underutilized_business_zone_hubzone_firm` (XX), `c8a_program_participant` (A6), `small_disadvantaged_business` (**NULL** — see §4), `self_certified_small_disadvantaged_business` (27), `minority_owned_business` (23), `joint_venture_women_owned_small_business` (8C), `emerging_small_business` (**NULL** — see §4).

**Rollups (2):** `any_socioeconomic_designation` (bool — OR over the 10 sourced flags), `designation_count` (int — count of TRUE among the 10 sourced flags; overlapping categories).

### Indexes (17)
- **BTREE (4):** `subawardee_uei`, `cage_code`, `primary_naics`, `designation_count`.
- **BITMAP (13):** the 10 sourced designation flags + `any_socioeconomic_designation`, `matched_in_sam`, `sam_is_active`.

## 4. Coverage & caveats (bake into any collateral)

- **Universe:** 25,450 subawardees; 22,741 (89.4%) resolve in `sam_master_entities`; **7,569 (29.7%) carry ≥1 designation.**
- **8(a) & HUBZone are FLOORS, not counts.** The SBA-cert string populates only ~13% of entities → recall ceiling ~68% (`A6`/`XX` precision ~90%+). Report as "≥ N". The seven `business_types` self-certs (women / veteran / SDVOSB / minority / SDB) have no such ceiling.
- **`economically_disadvantaged_women_owned_small_business`** = `A0` only, ~69% precision — lowest-confidence flag; treat as indicative.
- **`small_disadvantaged_business` = NULL.** The SBA-determined SDB code (`A4`) is absent from SAM (program folded into 8(a)); only `self_certified_small_disadvantaged_business` (27) exists. NULL = undetermined, **not** false.
- **`emerging_small_business` = NULL.** An FPDS size-status construct, not a SAM registration cert — unsourceable.
- **Namespace isolation is mandatory:** decode `QF/A5/A2/8W/27/23/8C` only against `business_types`, `A6/XX/A9/A0/JT` only against `sba_business_types_string`.

## 5. Zero-join GTM query patterns

```sql
-- HUBZone-certified subawardees, currently active in SAM
SELECT subawardee_uei, subawardee_name, primary_naics
FROM govcon_subawardee_designations
WHERE historically_underutilized_business_zone_hubzone_firm AND sam_is_active;

-- every diverse subawardee + how many programs they hold, most-diverse first
SELECT subawardee_uei, subawardee_name, designation_count
FROM govcon_subawardee_designations
WHERE any_socioeconomic_designation
ORDER BY designation_count DESC;

-- decode any raw SAM code to its label (namespace-scoped)
SELECT code, label, designation_key, confidence
FROM sam_business_type_code_dict
WHERE namespace = 'sba_business_types_string';
```

## 6. Per-flag baseline (full universe, measured at build)

| flag | TRUE subawardees |
|---|---:|
| self_certified_small_disadvantaged_business | 4,212 |
| woman_owned_business | 2,805 |
| minority_owned_business | 2,750 |
| women_owned_small_business | 2,517 |
| veteran_owned_business | 1,887 |
| service_disabled_veteran_owned_business | 1,164 |
| c8a_program_participant | ≥ 577 |
| historically_underutilized_business_zone_hubzone_firm | ≥ 534 |
| economically_disadvantaged_women_owned_small_business | 222 |
| joint_venture_women_owned_small_business | 50 |
| **any_socioeconomic_designation** | **7,569** |

## 7. Rebuild

```bash
doppler run --project core-x --config prd -- python pipelines/serving/seed_sam_business_type_code_dict.py
doppler run --project core-x --config prd -- python pipelines/serving/materialize_subawardee_designations.py
# verify either with --verify
```
The dict is hand-seeded from the GSA artifacts; the designations table is a pure projection of `sam_master_entities` ⋈ `contract_subaward`. Refresh after a `sam_master_entities` rebuild to pick up new registrations.
