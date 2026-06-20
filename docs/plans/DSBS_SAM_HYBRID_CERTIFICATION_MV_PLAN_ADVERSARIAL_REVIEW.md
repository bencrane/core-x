# Adversarial Review — DSBS × SAM.gov Hybrid Certification MV Plan

**Reviewer:** Principal Data Engineer (red-team). **Date:** 2026-06-20.
**Subject:** `docs/plans/DSBS_SAM_HYBRID_CERTIFICATION_MV_PLAN.md`
**Method:** full read of the plan + the five ground-truth probe findings (`/tmp/wf_findings/{A..E}`), plus 6 independent read-only live probes against `s3://data-sink/active/{sba_dsbs_certified_firms, sam_master_entities, sam_business_type_code_dict, govcon_subawardee_profiles, govcon_sub_targeting}` and the two DSBS cohort builders on disk.

---

## Verdict: **SHIP WITH AMENDMENTS**

The plan's core architecture is sound and almost entirely confirmed by live data: the two-namespace anti-conflation model is correct, the universe arithmetic (90,210 / 64,760 / 22,976) reproduces exactly, the Pending-exclusion logic is the right call and produces the right counts, the latent gap-builder bug is real and proven in code, and the SAM self-cert filter resolves to exactly {23,27,A2,A5}. **But the single most load-bearing artifact in the plan — the §1.3 `certs`-parsing SQL — does not run as written** (`from_json(d.certs, 'JSON[]')` is rejected by DuckDB). That is a one-token fix, but it is in the centerpiece of the build, so the plan cannot ship verbatim. Two smaller correctness amendments (the `sam_registration_active` wiring, the `_record_run` index-DDL drift) round out the amendment set.

---

## Live verification results (what I actually ran)

All probes materialized columns to Arrow first, then registered in DuckDB (per the runbook one-shot-reader gotcha). DuckDB version in env rejects `from_json(x,'JSON[]')` and treats `rows` as a reserved word.

### 1. The §1.3 certs-SQL test — **the headline finding**

**(a) Syntax — FAILS as written.** The plan's literal `UNNEST(from_json(d.certs, 'JSON[]'))` raises:
```
InvalidInputException: Malformed JSON at byte 0 of input: unexpected character. Input: "JSON[]"
```
`from_json` expects a *structure string* (`'["JSON"]'`), not a type name. Two spellings work and produce **identical** results:
- `UNNEST(CAST(d.certs AS JSON[])) AS t(c)`  ← recommended (cleanest, matches house cast idiom)
- `from_json(d.certs, '["JSON"]')` / `json_each(d.certs)`

All three explode to **117,012 rows over 67,234 distinct UEIs**, 100% non-null `name`. `certs` robustness: **0 NULL, 0 empty, 0 uncastable** across all 67,234 rows — `CAST(... AS JSON[])` is safe with no `TRY_` guard needed.

**(b) Per-program true-counts — MATCH finding B (Pending correctly excluded).** Running the full §1.3 pivot with the corrected spelling:

| program | plan SQL (status='Active' ∧ exit≥today) | finding B "future-exit" | raw `active_*_boolean` |
|---|---:|---:|---:|
| VOSB | **40,634** | 40,634 ✅ | 40,636 |
| SDVOSB | **34,851** | 34,851 ✅ | 34,853 |
| WOSB | **20,868** | 20,871 (see note) | 22,930 ⚠️ |
| EDWOSB | **5,969** | 5,969 ✅ | 7,428 ⚠️ |
| HUBZone | **5,181** | 5,181 ✅ | 5,182 |
| 8(a) | **3,605** | 3,605 ✅ | 3,613 |
| 8(a) JV | **375** | 375 ✅ | 375 |

The Pending leak is excluded exactly as intended: WOSB lands at 20,868 (not the raw boolean's 22,930), EDWOSB at 5,969 (not 7,428). **The plan's D2 gate works.** The 3-row WOSB delta vs finding B's 20,871 is fully explained: `active_future` (`exit ≥ today`) = 20,868, with 2 rows exiting *exactly* today and 4 already past; finding B's "future" used strict `> today`. The plan's `>= CURRENT_DATE` (includes today's expirations as still-valid) is the defensible choice. **No defect — internal consistency holds.**

**(c) Grain stays 1-row-per-UEI — CONFIRMED.** The pivot returns **65,224 rows = 65,224 distinct UEIs**. No fan-out leak from the unnest+pivot. (65,224 = firms with ≥1 currently-active cert; the other 2,010 DSBS firms have only Expired/Suspended/Pending certs → `cert_any=false`, in_dsbs=true.)

### 2. `suspended:true` 8(a) handling — clean by construction

All 33 `suspended=true` 8(a) entries carry `status='Suspended'`, **never** `status='Active'`. Query for `status='Active' AND suspended=true AND (exit NULL OR ≥today)` returns **0 rows**. The plan's status-gate already excludes suspended certs; the fact that it doesn't read the `suspended` field is not a hole.

### 3. SAM self-cert filter (§1.4) — CONFIRMED correct + grain-safe

- Dict has exactly 7 `business_types` rows with `sba_administered=false`; minus the 3 adjudicated-equivalent keys (8W/8C/QF) → **exactly {23 minority, 27 sdb, A2 woman, A5 veteran}**. Matches the plan.
- The §1.4 CTE run verbatim: **745,167 rows = 745,167 distinct UEIs** — the unnest+dict-join does **not** fan out. SAM entities are 1-row-per-UEI (1,541,566 = distinct). `business_types`: 0 NULL, 1,421 empty lists (UNNEST drops them safely; fine under the spine LEFT JOIN).
- Self-cert distribution: minority 353,002 · sdb 456,957 · woman 329,779 · veteran 155,929.

### 4. Universe arithmetic (§2.2 / finding E) — reproduces EXACTLY

`|D|=67,234 · |P|=25,450 · |T|=14,610 · universe=90,210 · D∩P=2,474 · T−P=0 (T⊆P) · greenfield=64,760 · sub_only=22,976`. The targeting agg (`GROUP BY candidate_sub_uei`) collapses to **14,610 rows = 14,610 distinct UEIs, max 821 edges/firm** — D6 fan-out risk is real and the pre-aggregation fixes it. 69 firms have all-sentinel `last_subaward_action_date` → NULL after the `< 2100` filter (correct).

### 5. Ledger / cohort-builder code claims — VERIFIED on disk

- **Latent bug (§4.3) — REAL.** `cohort_sba_dsbs_certified_gap_domains.py:313` passes `stats.get("firms_gap", 0)` into the 7th value, bound by the column list (`:307-308`) to **`firms_with_linkedin`**. A successful gap run writes `firms_with_linkedin = firms_gap`. The non-gap builder (`cohort_sba_dsbs_certified.py:312`) correctly passes `stats.get("firms_with_linkedin", 0)`. Fix = literal `0`. Confirmed.
- **Swallowed-exception root cause (§4) — STRUCTURALLY PROVEN.** Both `_record_run` bodies wrap connect+insert in `try/except Exception` printing only `WARN` to **stdout** (`cohort_sba_dsbs_certified.py:319-320`). Publish runs before `_record_run` in the `finally`, so a PG failure yields published-cohort-no-row. The *swallow* is proven; the *specific trigger* (unresolved `hqx-postgres` secret) is plausibly inferred — the plan correctly hedges "most likely."
- **`firms_total=67234` in the backfill — CORRECT.** Gap builder's `firms_total = SELECT count(*) FROM dsbs` over the **full** `DSBS_URI` (no NAICS sub-filter), so 67,234 is right.
- **OPS_DDL index drift — CONFIRMED.** The gap builder's inline `OPS_DDL` (`:103-122`) **omits** the three `CREATE INDEX IF NOT EXISTS` statements that the non-gap builder's `OPS_DDL` (`:131-133`) and the `.sql` sibling include. Harmless (idempotent, indexes already exist) but a real divergence worth aligning.

### 6. Schema column existence — ALL CONFIRMED

Every DSBS-native column the plan carries (`legal_business_name, email, phone, contact_person, website, state, city, zipcode, county, naics_primary`) and every profiles fallback (`sub_name, hq_state, hq_city, poc_full_name, poc_title, n_subawards, total_subaward_amount, top_subaward_description`) exists with the exact spelling. `naics_primary`/`zipcode` are `string` (BTREE on them is fine).

---

## Findings table

| ID | Sev | §Ref | Issue (grounded) | Recommended change | Alters conclusions? |
|---|---|---|---|---|---|
| **F1** | **Blocker** | §1.3, §6.4 | The centerpiece SQL `UNNEST(from_json(d.certs, 'JSON[]'))` **does not execute** — DuckDB raises `Malformed JSON … Input: "JSON[]"`. `from_json`'s 2nd arg is a structure string, not a type name. | Replace with `UNNEST(CAST(d.certs AS JSON[])) AS t(c)` (verified identical output, 0 uncastable rows). Keep `json_extract_string(c,'$.field')` — those work. | No — the *intent* is correct and the corrected SQL reproduces every target count. It is a spelling fix in load-bearing code, so it blocks a verbatim build. |
| **F2** | **Major** | §2.1, §1.4 | `sam_registration_active` is sourced from the §1.4 self-cert CTE (`sc.sam_registration_active`), but that CTE is an **INNER JOIN to the dict** — it only contains the 745,167 self-cert UEIs. A SAM-registered, actively-registered firm with **no retained self-cert code** (e.g. an 8(a)-only firm, or a firm whose only SAM type is an excluded 8W) gets `sc=NULL` → `coalesce(...,false)` → **`sam_registration_active=false` while truly active.** This silently understates deliverability for exactly the adjudicated-cert firms the MV is built for. | Source `sam_registration_active` (and `sam_present`) from a **separate UEI-grain LEFT JOIN** on `sam_master_entities` (`bool_or(is_active)` per UEI, or just `is_active` since 1/UEI), independent of self-cert. The plan already builds a `sm` subquery for `sam_present` — extend it to carry `is_active`. | Yes — corrects a wrong column value for a meaningful slice; strengthens strategic value #3 (deliverability filter). |
| **F3** | **Minor** | §4.2, gap builder `:103-122` | Gap builder's inline `OPS_DDL` omits the 3 `CREATE INDEX IF NOT EXISTS`; non-gap builder includes them. | Add the 3 index statements to the gap builder's `OPS_DDL` for parity (idempotent). Low blast radius. | No. |
| **F4** | **Minor** | §1.5 | The first `verify` query is **explicitly inert** (`… AND false`) and the "load-bearing" assertion is left as a *prose comment*, not SQL. As written, `verify` asserts almost nothing. | Replace with the real, executable assertion: explode `sam_self_codes` and assert **0 rows** where any code ∈ {8W,8C,QF,A9,A6,XX,A0,JT}. Concrete form below. | No — but it converts a claimed integrity gate into an actual one. |
| **F5** | **Minor** | §2.3, §6 (O3) | `cert_programs` is proposed as **BITMAP**. It is a pipe-delimited combination string; with 7 programs the realized distinct combinations can run to dozens–hundreds, and it is a *composite* string a frontend rarely filters on directly (it filters the 7 `cert_<prog>` bools, which are already BITMAP). BITMAP on a higher-cardinality combo string is lower-value than on the atomic bools. | Drop the BITMAP on `cert_programs` (leave it unindexed, audit-only). The 7 per-program bools + `cert_count` cover every real filter axis. Keeps it but stops paying for an index the frontend won't use. | No. |
| **F6** | **Minor** | §2.3 | Six per-program `cert_<prog>_exit_date` columns are each BTREE (6 indexes) **plus** `next_cert_expiration_date` BTREE. The recert-outreach motion (§5.1) and "expires before X" both run off `next_cert_expiration_date`. Per-program exit BTREEs are only justified if the frontend filters "WOSB-specifically expiring before X" — not stated as a requirement. | Keep `next_cert_expiration_date` BTREE (the workhorse). Demote the 6 per-program exit dates to **unindexed** unless a per-program expiry filter is an actual frontend requirement (O-list it). Saves 6 index builds on every overwrite. | No — pure cost trim; reversible. |
| **F7** | **Minor** | §2.3, §3 (sub_only tier) | The `sub_only` tier (22,976 firms) carries **no `cert_*`** and is in a dataset named `…_certifications_mv`. It is *not* noise — it is the inverse "self-certified-but-uncertified" wedge (§5.4) — but its presence means ~25% of rows in a "certifications" MV have zero adjudicated cert. That is a defensible design choice, **but it must be explicit and filterable.** | Keep the tier (the wedge is real GTM value) but make the cut a first-class, indexed predicate. `universe_tier` BITMAP already does this; ensure `cert_any` BITMAP cleanly separates "has adjudicated cert" (67,234-ish minus expired-only) from "self-cert-only" in one filter. No structural change; just confirm the frontend default excludes `sub_only` unless asked. | No. |
| **F8** | **Nit** | §2.1 | `cert_lifecycle='expiring_90d'` is derived but the boundary between `expiring_90d` and `active` is computed from `next_cert_expiration_date`, which is `min(exit) FILTER (exit ≥ today)`. A firm with one cert expiring in 30d and another in 5y is correctly `expiring_90d` (min wins). Verify the CASE also yields `none` when `cert_any=false` (no future exit) and doesn't collapse a Pending-only firm into `active`. | Define explicitly: `none` when `next_cert_expiration_date IS NULL`; `expiring_90d` when `≤ today+90`; else `active`. Confirmed sensible; just pin the NULL branch. | No. |
| **F9** | **Nit** | §4.1 | Backfill `firms_with_domain=52698` and `firms_pdl_matched=28422` are **operator-supplied** and cannot be re-derived read-only (they require re-running the write-side PDL probe). The Parquet row counts (26,502 / 23,973) *are* verified; the funnel intermediates are taken on trust. | Acceptable for a backfill, but note it: if a future re-run of the builder produces different intermediates, the backfilled row is a point-in-time estimate, not a reproduced measurement. Flag in the SQL comment. | No. |
| **F10** | **Nit** | D4 naming / O2 | `_mv` suffix breaks the house `govcon_sub_*` (no-suffix) convention; the plan already flags this. No technical issue. | Operator's call (O2). The plan handles it correctly. | No. |

### F4 — concrete replacement assertion (executable)

```sql
-- must return 0 rows: no retained self-cert code may be an adjudicated/SBA-administered code
SELECT count(*) AS conflation_violations
FROM (
  SELECT uei, unnest(string_split(sam_self_codes, '|')) AS code
  FROM govcon_sub_certifications_mv
  WHERE sam_self_codes IS NOT NULL
)
WHERE code IN ('8W','8C','QF','A9','A6','XX','A0','JT');
```
This is the assertion §1.5 *describes in prose* but never writes as runnable SQL. It directly proves the namespaces never overlap and breaks the build on any future dict edit that reintroduces conflation.

---

## What the plan got right (survived scrutiny)

1. **The two-namespace anti-conflation model (D3, §1.1) is correct and the structural override is the right design.** "DSBS presence = adjudicated truth; SAM contributes only no-DSBS-equivalent self-reps" — verified: the dict cleanly partitions, the retained set is exactly {23,27,A2,A5}, and the disjointness is by construction (no precedence CASE needed). The A2/A5-vs-8W asymmetry is a *legitimate* distinction (general woman/veteran ownership ≠ the adjudicated WOSB/VOSB program), not a misleading one.
2. **The Pending-exclusion decision (D2) is the single most important correctness call and it is right.** Equating `active_*_boolean=true` with certified would over-count WOSB by 2,062 and EDWOSB by 1,459 (pending applications). Gating on `certs[].status='Active'` produces the correct currently-active counts — verified to the row.
3. **`certs` as system-of-record over the scalars (D1) is correct** — `certDateExit_*` is genuinely absent for EDWOSB/HUBZone/WOSB; sourcing expiration from `certs[].exitDate` is mandatory, and the cast is clean (0 unparseable, 0 null/empty certs).
4. **The grain discipline is rigorous and verified end-to-end.** Every join is UEI=UEI at 1:1 after pre-aggregation; the §1.3 pivot, the §1.4 self-cert CTE, and the §2.1 t_agg all hold 1-row-per-UEI under live data. The D6 821× fan-out is real and correctly defused.
5. **The universe arithmetic is exact, not estimated.** 90,210 / 64,760 / 22,976 / 2,474 / T⊆P all reproduce. The plan's correction of the directive's ~58k → 64,760 greenfield is right and material (~6,760 firms of net-new TAM the directive would have missed).
6. **The ledger remediation is correctly diagnosed.** The "builders DO call `_record_run`, the write failed silently" reframing is accurate (not "missing insert"). The latent `firms_gap`→`firms_with_linkedin` bug is real in code. The gap-cohort semantics (`firms_with_linkedin=0`, `firms_pdl_matched=28422`, `distinct_urls=23973`) correctly mirror the live cascade precedent. The #573-chunks-are-a-reslice decision is sound.
7. **The build idiom is faithful to the house template** (snapshot-overwrite, frozen schema + `assert_schema`, `PRAGMA threads=1` for zero-delta, BTREE/BITMAP split, `finally:`-block ledger, Modal-dual entrypoints). Finding C's skeleton is followed.
8. **`sam_business_type_code_dict` "no action — already permanent" is correct.** 12 rows, 3 BTREE, consumers wired, docs exist; correctly not ledgered (hand-curated seed).

---

## Net recommendation — minimal amendment set (priority order)

1. **[Blocker] Fix the §1.3 SQL spelling** → `UNNEST(CAST(d.certs AS JSON[])) AS t(c)`. Verified identical output. Without this the build's centerpiece does not run. (F1)
2. **[Major] Re-wire `sam_registration_active`** to a self-cert-independent LEFT JOIN on `sam_master_entities.is_active`, so adjudicated-cert firms with no retained self-cert code aren't falsely flagged SAM-inactive. (F2)
3. **[Minor] Make the §1.5 integrity assertion executable** (the `unnest(string_split(sam_self_codes,'|'))` form above) instead of an inert templated query + prose. (F4)
4. **[Minor, ship-adjacent] In the ledger patch, add the 3 missing `CREATE INDEX` to the gap builder's `OPS_DDL`** for parity while you're already fixing the latent bug. (F3)
5. **[Optional cost trims] Drop BITMAP on `cert_programs` (F5); demote the 6 per-program `cert_<prog>_exit_date` BTREEs unless a per-program expiry filter is a real requirement (F6).** Reversible; reduces index builds per overwrite.

Items 1–2 are correctness; 3–4 are cheap hardening done in the same pass; 5 is discretionary. Everything else in the plan survives scrutiny and should ship as-is.
