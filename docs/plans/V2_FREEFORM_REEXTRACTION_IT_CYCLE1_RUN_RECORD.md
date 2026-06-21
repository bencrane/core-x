# v2-freeform re-extraction — IT/Professional cycle 1 — RUN RECORD

**Executed:** 2026-06-21 (UTC). **Mode:** destructive `reset-llm` → re-grind → ingest → rematerialize (live R2 writes; opus session-grind). **Outcome:** SUCCESS — the v1 controlled-vocabulary labor signal for the IT/Professional cohort is replaced by free-form raw job titles; serving labor goes from a 36-token field to 1,012 distinct values (976 free-form).

## Premise (verified before any destructive step)
- 24,382 `done` resources; **23,583 (96.7%) under v1 controlled-vocab** prompt hashes (f19feb09 / 425b3bfe / 1798fde0); only 799 already v2-freeform (f3567fc8). The v1 36-token `labor_categories` vocab has **no member** for IT/engineering/analyst/PM roles — structurally blind.
- `reset-llm` is scoped (`--resource-ids-file`), deletes ONLY `llm:%` requirement + doc-scope rows, resets ledger done→pending, and **never touches regex-lane rows** (bonding/regex-labor preserved). Confirmed `phase_llm_reset` (`sam_labor_demand_extract_90day.py`).

## Cohort (this cycle = Tier 1)
**970 IT/Professional v1-done resources** — PSC `R*` professional/admin support (703) + `D*` IT services (268); 612 SB, 378 SB>$500K. Built by `scripts/v2it_cohort.py` (bridge resource → manifest Sol# → active D/R-PSC award), sharded into 16 waves (`hash%16`).

## What ran (the loop)
| Stage | wave 1 (validation) | rest (waves 01–15) |
|---|---|---|
| `reset-llm` | 58 → pending (v1 rows deleted) | 912 → pending |
| `bracket` (CUI Decision-A) | clean (0 marked) | re-partitioned |
| `select` (v2-freeform stage) | 52 task files | 840 task files |
| **grind** (opus, CONC-capped) | 5 shards → 52/52 written, 0 fail | 30 shards (CONC 6) → 840/840 written, 0 fail |
| `ingest` (≥0.98 gate) | 151 rows, **pass-rate 1.0** | 2,427 rows, **pass-rate 0.9996** |

**892 docs ground, 0 failures, both gates passed.** ~8.8M opus subagent tokens (session-opus Max ≈ $0 cash). The grind agents extracted raw titles verbatim and correctly applied the honesty contract (SCA wage-determination tables → `standard_compliance`, not labor; negated requirements suppressed; blank forms → valid empty).

## Uplift (verified)
**Requirements grain** (`extractor='llm:session-opus@v2-freeform-labor'`):
- 3,551 `labor_category` rows / 2,079 distinct titles; **3,543 (99.8%) OUTSIDE the 36-token v1 vocab.**
- Plus full 11-type extraction (deliverable 1,254, standard_compliance 870, clearance 225, certification 202, …).
- Sample IT/professional titles v1 could not express: *software engineer, systems administrator (senior), data analyst, financial analyst, management analyst, configuration management specialist, chief engineer, architect, fraud analyst, Full Spectrum GEOINT Analyst, GIS Integration Engineer, Information System Security Officer*.

**Serving grain** (`govcon_award_scope_requirements`, rematerialized once at end):
| | before | after |
|---|---:|---:|
| distinct labor values | 36 | **1,012** |
| free-form (non-vocab) occurrences | 0 | **13,557** |
| free-form distinct titles | 0 | **976** |
| awards with a free-form title | 0 | **979** |
| **IT/Professional (D/R PSC) awards with free-form labor** | **0** | **290** |

## Safety properties (held)
- **Regression window invisible**: serving rematerialized exactly once, at the very end, after all rows landed — consumers never saw a partial state.
- **Regex/bonding preserved**: `reset-llm` never touched `regex:%` rows; the surety/bonding GTM signal was unaffected throughout.
- **CUI gate PASS** the whole cycle (marking report reconcile_overall=PASS; bracket excluded marked resources before staging; `build_task_payload` hard-asserts marked text never staged).
- **Disjoint-by-construction grind**: static `NR%K` shards → no two grind agents touched the same task file; `test -f result` skip = resumable.

## Next cycles (same loop, larger cohorts)
- **Tier 2 — all-service PSCs**: ~10,500 v1-done resources (8,228 SB, 8,317 >$500K). The natural expansion; captures Y/Z/J/S facilities-and-construction-adjacent services.
- **Tier 3 — full v1-done**: 23,583 (12,210 unbridged tail — gains from v2 but cannot be value/SB-prioritized; grind last).

## Durable artifacts
`scripts/v2it_cohort.py` (IT/Professional cohort builder + wave sharder). Task files + result JSONs were ephemeral under `/tmp`. Throwaway recon/sizing probes left under `scripts/_recon_*` / `scripts/_v2size_*` (uncommitted scratch).
