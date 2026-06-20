# GovCon Active-Award Targeting Stack — state & capabilities

**Report date:** 2026-06-20 (UTC) · **Substrate as_of:** 2026-06-20 · **Repo HEAD at authoring:** `40666c4`
**All figures below are live-measured against the R2 system of record** (`s3://data-sink/active/`) by the probe in §6. Re-run it to reproduce every number.

This document covers three serving datasets built this session and the queries they answer. It deliberately states only measured facts.

---

## 1. What was built this session

| dataset | PR | rows | role |
|---|---|---:|---|
| `govcon_active_awards` | [#548](https://github.com/bencrane/core-x/pull/548) | 189,274 | **work substrate** — prime awards still in performance |
| `govcon_prime_subaward_propensity` | [#549](https://github.com/bencrane/core-x/pull/549) | 2,106 | additive prime-keyed realized-subcontracting prior |
| `govcon_award_scope_requirements` | [#552](https://github.com/bencrane/core-x/pull/552) | 35,028 | additive award-keyed scope + all-11 requirement types |

Workers: `pipelines/serving/materialize_active_awards.py`, `materialize_prime_subaward_propensity.py`, `materialize_award_scope_requirements.py`. Each is snapshot-overwrite (Lance v2.1), self-bootstraps an `ops.*_serving_runs` ledger, runs locally under `doppler run` and is Modal-deployable. No cadence registered.

**Architecture invariant:** the work substrate is the spine. The other two are **additive annotations** joined `LEFT JOIN ... ON` the relevant key. Absence of an annotation row = *unknown*, never an exclusion. Neither annotation narrows the work set.

---

## 2. Dataset catalog (verified)

### 2.1 `govcon_active_awards` — the work substrate (Lance v13, 189,274 rows, 43 cols)
One row per `contract_award_unique_key`, collapsed from `contract_prime_txn` (latest txn per award, deterministic tiebreak). **Membership = `GREATEST(pop_current_end, pop_potential_end) >= as_of` OR both PoP ends NULL.** An award is dropped only when definitively done. No subjective filters baked in.

| liveness flag | awards | distinct primes |
|---|---:|---:|
| `active_current` (committed end ≥ today) | 142,295 | — |
| `active_potential` (incl. option years) | 148,791 | 26,943 |
| `has_option_tail` (unexercised govt option) | 27,119 | — |
| `pop_unknown` (no PoP date; kept + flagged) | 40,483 | 23,381 |
| **all rows** | **189,274** | **40,243** |

Key columns (all raw — consumers filter): `pop_start/pop_current_end/pop_potential_end/ordering_period_end/latest_action_date`, `recipient_uei`, `business_size` (`contracting_officers_determination_of_business_size`), `type_of_set_aside`, `naics_code`, `psc_code`, `federal_action_obligation`, `current_total_value_of_award`, `base_and_all_options_value`, `potential_total_value_of_award`, `awarding_agency_name`, `pop_state_code`, `as_of_date`, `built_at`.
Indexes — BTREE: `contract_award_unique_key, recipient_uei, naics_code, pop_current_end, pop_potential_end`; BITMAP: `business_size, type_of_set_aside, award_or_idv_flag, active_current, active_potential, has_option_tail, pop_unknown`.

### 2.2 `govcon_prime_subaward_propensity` — additive prime prior (Lance v5, 2,106 rows, 16 cols)
One row per `(prime_awardee_uei, naics_code)` = a prime's **full NAICS footprint** of realized subcontracting, aggregated from `contract_subaward` (status-agnostic, full history). 1,317 distinct primes · 193 NAICS · **$263.9B** total subawarded · 1,317 `is_primary_naics` rows (one per prime).
Columns: `prime_awardee_uei`, `prime_awardee_business_types`, `naics_code`, `subaward_dollars`, `n_subawards`, `n_distinct_subs`, `distinct_sub_business_types`, `first_subaward_date`, `last_subaward_date`, `is_primary_naics`, `prime_total_subaward_dollars/subawards/n_naics`.
Join: `govcon_active_awards.recipient_uei = prime_awardee_uei` (LEFT JOIN, additive).

### 2.3 `govcon_award_scope_requirements` — scope + all-11 requirements (Lance v23, 35,028 rows, 39 cols)
One row per `contract_award_unique_key` (the profile's exploded award grain). Driven from `govcon_award_solicitation_profiles.source_resource_ids` (the manifest `award_keys[]` fan-out) joined to `govcon_award_requirements`. Reproduces the canonical profile's `n_requirements`/`n_validated` 100% (35,028/35,028; 1,000,868 fan-out edges).

**Scope:** `has_extracted_scope` (33,724), `scope_summary` (29,836 non-null), `solicitation_scope_tags` (27,366 non-empty; 77-term controlled vocab).

**All 11 requirement types** — each a `has_<type>` boolean + a `<type>_values` deduped/alpha/capped list:

| requirement type | awards with flag | previously surfaced in profile? |
|---|---:|---|
| standard_compliance | 29,586 | no |
| vehicle_constraint (set-aside eligibility) | 27,301 | partial |
| labor_category | 25,185 | yes |
| **deliverable** | 24,718 | no |
| **past_performance** | 19,188 | no |
| **equipment_capability** | 17,082 | no |
| certification | 6,527 | yes |
| clearance | 5,615 | yes |
| **staffing_constraint** | 5,041 | no |
| **insurance_bonding** | 4,433 | no |
| **license** | 2,258 | no |

Derived: `requires_cmmc` (774), `req_clearance_level_max` (SECRET 3,792 · TOP_SECRET 225 · PUBLIC_TRUST 166 · TS_SCI 68 · CONFIDENTIAL 1 · none 30,776), `labor_headcount_total`, `wage_floor_max`. `req_lists_truncated` (699 awards hit a per-type value-list cap). Caps: standard_compliance 50, deliverable 30, all others 25.
Indexes — BTREE: `contract_award_unique_key, labor_headcount_total, wage_floor_max, n_requirement_types, n_requirements`; BITMAP: the 11 `has_<type>` + `requires_cmmc, req_clearance_level_max, has_extracted_scope, is_primary_target, req_lists_truncated, coverage_truncated`.

**Verification:** built and gated by two orchestrated multi-agent workflows. The adversarial gate (8 independent checks) passed with 0 defects: full-population count-fidelity (sums = 1,000,868), byte-exact cross-derivation vs the profile (md5 identical), full-universe new-type recompute (102,306 award-type pairs, 0 mismatch), coupling invariants, value-list integrity (truncation flag 100% correct), additive/lossless join, scope-carry fidelity, grain/schema (35,028 = distinct keys, exact key reconciliation with the profile).

### 2.4 Upstream sources referenced (verified this session)
| dataset | measured | note |
|---|---|---|
| `usaspending_api_fresh/contract_prime_txn` | 1,247,391 distinct awards (1,518,807 txns); action_date 1993-11-15 → 2026-06-07 | full prime feed; source of `govcon_active_awards` |
| `usaspending_api_fresh/contract_subaward` | 199,901 rows; 1,317 primes; 25,450 subs; **6,347 distinct prime awards**; 2001-05-13 → 2026-06-05 | FSRS realized subawards; source of propensity |
| `sam_master_entities` | 1,541,566 entities (782,543 active); 27,687 with an SBA business-type code | firm registry; certs are coded (no in-repo code→label decode) |
| `govcon_award_requirements` | 193,845 rows, all `validated=true`, 11 types | resource-grain; source of the requirements rollup |
| `govcon_award_solicitation_profiles` | 35,028 awards | canonical scope + 4-type profile; source spine |
| `govcon_scope_vectors` | 1,481,167 chunks; 4,988 award-linked; 4,526 BGE-embedded | raw solicitation scope text + embeddings |
| `sam_opps_attachment_manifest_winners` | 155,183 rows; 41,963 distinct awards | solicitation PDF `download_url` / `ui_link` |
| `sam_attachment_files` | 127,607 (126,932 downloaded) | download ledger (`stored_uri` → CAS blob) |
| `sam-gov-opps/active` | 81,602 notices (17,007 with `response_deadline` ≥ today) | pre-award open solicitations |
| `sam-gov-opps/archived` | 2,839,948 notices | inactive/past solicitations |

---

## 3. The composable join

```sql
SELECT aw.*,
       sr.scope_summary, sr.solicitation_scope_tags,
       sr.has_clearance, sr.req_clearance_level_max, sr.requires_cmmc,
       sr.has_deliverable, sr.deliverable_values, sr.has_license, sr.license_values,
       pp.subaward_dollars, pp.n_distinct_subs, pp.is_primary_naics
FROM govcon_active_awards aw
LEFT JOIN govcon_award_scope_requirements sr
       ON aw.contract_award_unique_key = sr.contract_award_unique_key
LEFT JOIN govcon_prime_subaward_propensity pp
       ON aw.recipient_uei = pp.prime_awardee_uei
-- consumer-chosen filters below; the substrate is never gated by the annotations
WHERE aw.pop_current_end >= current_date
```
Both joins are additive and lossless: a LEFT JOIN from `govcon_active_awards` keeps all 189,274 rows (both right tables are unique on their join key). Annotation coverage: `sr` annotates **28,774** active awards (**20,221** active-future); `pp` matches **1,216** of the 40,243 active-award primes and yields **15,007** active awards with a same-NAICS prior.

---

## 4. Queries answerable now

All examples assume the join in §3 or a direct read of one dataset.

**Q1 — Live awards in a firm's NAICS, ranked by remaining runway.**
```sql
SELECT contract_award_unique_key, recipient_name, naics_code, awarding_agency_name,
       pop_current_end, datediff('month', current_date, pop_current_end) AS months_left,
       current_total_value_of_award
FROM govcon_active_awards
WHERE pop_current_end >= current_date AND naics_code = :naics
ORDER BY months_left DESC;
```

**Q2 — Live awards that require a clearance (and at what level), CMMC, a named certification, a license, or bonding.**
```sql
SELECT aw.contract_award_unique_key, aw.recipient_name, aw.naics_code,
       sr.req_clearance_level_max, sr.certification_values, sr.license_values, sr.insurance_bonding_values
FROM govcon_active_awards aw
JOIN govcon_award_scope_requirements sr USING (contract_award_unique_key)
WHERE aw.pop_current_end >= current_date
  AND (sr.has_clearance OR sr.requires_cmmc OR sr.has_license OR sr.has_insurance_bonding);
```

**Q3 — Live awards with explicit deliverables / labor categories / equipment / staffing requirements** (the 7 newly-surfaced types). Swap the flag/list columns: `has_deliverable`/`deliverable_values`, `has_labor_category`/`labor_category_values`, `has_equipment_capability`/`equipment_capability_values`, `has_staffing_constraint`/`staffing_constraint_values`, `has_standard_compliance`/`standard_compliance_values`, `has_past_performance`/`past_performance_values`.

**Q4 — Live awards by work domain (scope tags).**
```sql
SELECT aw.contract_award_unique_key, aw.recipient_name, sr.scope_summary
FROM govcon_active_awards aw
JOIN govcon_award_scope_requirements sr USING (contract_award_unique_key)
WHERE aw.pop_current_end >= current_date
  AND list_contains(sr.solicitation_scope_tags, :scope_tag);  -- e.g. 'electrical_systems'
```

**Q5 — Large-prime, unrestricted live awards** (structural subcontracting-candidate pool; this is a *candidate* segment, not a confirmed subcontracting obligation).
```sql
SELECT * FROM govcon_active_awards
WHERE pop_current_end >= current_date
  AND business_size LIKE 'OTHER%'
  AND (type_of_set_aside IS NULL OR type_of_set_aside IN ('', 'NO SET ASIDE USED.'));
```

**Q6 — Primes with demonstrated realized subcontracting in a NAICS.**
```sql
SELECT prime_awardee_uei, prime_awardee_name, subaward_dollars, n_distinct_subs, last_subaward_date
FROM govcon_prime_subaward_propensity
WHERE naics_code = :naics
ORDER BY subaward_dollars DESC;
```

**Q7 — A firm's registry profile** (status, SBA cert codes, full NAICS portfolio).
```sql
SELECT legal_business_name, is_active, registration_expiration_date,
       sba_business_types_string, primary_naics, naics_codes
FROM sam_master_entities WHERE uei = :uei;
```

**Q8 — Set-aside posture of live awards** (award-level FPDS designation; `type_of_set_aside` on the substrate).

**Q9 — Pre-award open solicitations in a NAICS with a live deadline.**
```sql
SELECT notice_id, title, naics_code, set_aside, response_deadline, link
FROM "sam-gov-opps/active"
WHERE response_deadline >= current_date AND naics_code = :naics;
```

**Q10 — Composite (given a sub UEI):** join the sub's `naics_codes`/`primary_naics` (Q7) to live awards (Q1), optionally restrict by requirements the sub can meet (Q2/Q3), optionally rank by prime propensity (Q6) — via the §3 join. The work set is never reduced by the propensity or requirements layers; they only annotate/rank.

---

## 5. Coverage & limits (measured)

| capability | covered | of base | not covered (measured) |
|---|---:|---|---|
| awards still in performance | 142,295–148,791 | of 1.25M total awards | done/expired awards are excluded by definition |
| scope + structured requirements on live work | **20,221** active-future | 13.6% of 148,791 | 128,570 active-future awards have no extracted scope/requirements |
| BGE-embedded scope on live work | 2,322 active-future | 1.6% | semantic-vector matching only covers this slice |
| solicitation URL captured on live work | 15,639 active-future | 10.5% | — |
| prime realized-subcontracting prior | 1,216 active-award primes | 3% of 40,243 | most primes never report subawards (FSRS) |
| firm registry coverage of set-aside small-prime winners | ~99.8% join | — | — |

**Explicitly NOT answerable with current structured data:**
- *Realized subcontracting on a given active award* — `contract_subaward` spans only 6,347 distinct prime awards across all of 2001–2026; same-award joins to the live set are sparse. Propensity is therefore prime-level, not award-level.
- *Whether a specific prime will subcontract* — propensity is a historical prior (1,216 active primes), not a prediction; absence is unknown.
- *Stated sub-type subcontracting requirements (e.g., "must subcontract to HUBZone")* — present only as raw text in `govcon_scope_vectors.text` (4,988 award-linked solicitations); not extracted into any structured field.
- *Performance requirements for awards without extracted requirements* — the structured requirement types exist only for awards in `govcon_award_scope_requirements` (35,028; 20,221 active-future).
- *Decoded firm certification labels* — `sam_master_entities` SBA/business types are stored as raw codes (e.g. `A6`, `8W`); no code→label decode exists in-repo.

**Freshness:** `govcon_award_scope_requirements` is snapshot-frozen to the profile's requirements version (`reqs:v13787`); the live `govcon_award_requirements` feed is ~58 versions ahead (≈6% more rows, ≈145 net-new awards). Rebuilding `govcon_award_solicitation_profiles` then re-running the worker closes that gap with no logic change. `govcon_active_awards` liveness decays daily and is bounded by `contract_prime_txn` freshness (max action_date 2026-06-07); rebuild to refresh. No refresh cadence is currently registered for any of the three.

---

## 6. How to verify

Run the consolidated probe (read-only; reproduces every number in §2.1–2.3 and §3):

```python
# doppler run --project core-x --config prd -- python this.py
import os, lance, duckdb, json
def so():
    ep=os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id":os.environ["R2_ACCESS_KEY_ID"],"aws_secret_access_key":os.environ["R2_SECRET_ACCESS_KEY"],"endpoint":ep,"region":"auto"}
S=so(); D=lambda n: lance.dataset(f"s3://data-sink/active/{n}/", storage_options=S)
con=duckdb.connect(":memory:"); con.execute("SET memory_limit='8GB'")
con.register("aw",D("govcon_active_awards")); con.register("pp",D("govcon_prime_subaward_propensity")); con.register("sr",D("govcon_award_scope_requirements"))
print(con.execute("SELECT count(*) FROM aw").fetchone(),
      con.execute("SELECT count(*) FROM pp").fetchone(),
      con.execute("SELECT count(*) FROM sr").fetchone())
print(con.execute("SELECT count(*) FROM aw a JOIN sr s USING(contract_award_unique_key) WHERE a.active_potential").fetchone())  # 20221
```

Per-dataset read-back: `python pipelines/serving/materialize_<name>.py --cmd verify`.

---

## 7. Provenance

- Built from: `contract_prime_txn` (active_awards), `contract_subaward` (propensity), `govcon_award_solicitation_profiles` + `govcon_award_requirements` (scope_requirements).
- Requirements→award bridge: the manifest `award_keys[]` fan-out (`sam_opps_attachment_manifest_winners`), via `govcon_award_solicitation_profiles.source_resource_ids`. The inline `contract_award_unique_key` on `govcon_award_requirements` reproduces only 25% of awards and is not used.
- Ledgers: `ops.active_awards_serving_runs`, `ops.prime_subaward_propensity_serving_runs`, `ops.award_scope_requirements_serving_runs`.
