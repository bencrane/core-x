# Query-Sidecar Phase 2 — Benchmark Gate: Results & Verdicts

Executed 2026-07-09 (UTC). Harness: `pipelines/query_sidecar/benchmark_phase2.py` (Modal, 64 GiB/8 cpu/512 GiB NVMe — the Phase 3 warm-process container class). Workload: **14 representative `phrase.v2` plans, 10 byte-exact compiler test fixtures** (`apps/catalyst_api/tests/test_phrase_compiler.py`), covering every grammar family and weighted to the operator's lookalike/award-universe shapes.

## Arms

| Arm | What it measures |
|---|---|
| **A** | The live lane, replicated call-for-call from `market_store.py`: pylance scanners over Lance-on-R2, Python set algebra, `count_rows` pushdown totals, `IN_CHUNK=500`/`SEMI_JOIN_MAX=10k`/`COLLAPSE_SCAN_CAP=500k`, streamed cap-cut discipline. Warm handles |
| **B** | The prefiltered-slice lane: identical Lance slice reads streamed into DuckDB, set algebra in SQL |
| **C** | The query-sidecar artifact (v2, 23.80 GiB, 40 tables incl. promoted Tier C) on **local NVMe** |

## Results (median of 3, warm; first-run cold values in the harness log)

| # | Phrase (family) | A live | B slices | C sidecar | A→C |
|---|---|--:|--:|--:|--:|
| RQ01 | construction + code-A mod, 90d (collapse) | 10,469 ms | 6,993 ms | **79 ms** | 133× |
| RQ02 | construction + code-Y >$5m, 1y (collapse) | 10,531 ms | 9,903 ms | **57 ms** | 185× |
| RQ03 | primed-in 236220, >$10m lifetime (lanes) | 582 ms | 487 ms | **18 ms** | 32× |
| RQ04 | DSBS + VA + subbed-under 236220 (lanes) | 416 ms | 427 ms | **24 ms** | 17× |
| RQ05 | primed-in 541690 + both-sides (lanes) | 799 ms | 695 ms | **18 ms** | 45× |
| RQ06 | sub-only + inferred primeable 541330 (inferred) | 2,349 ms | 1,723 ms | **29 ms** | 82× |
| RQ07 | active DSBS VA + inferred subbable R499 (inferred) | 7,352 ms | 5,231 ms | **40 ms** | 185× |
| RQ08 | inferred primeable + code-G mod 90d (multi-hop) | 16,660 ms | 8,885 ms | **128 ms** | 130× |
| RQ09 | expiring-180d ∩ code-G-mod-90d construction (two-lane flagship) | 70,337 ms | 65,075 ms | **134 ms** | 525× |
| RQ10 | awards expiring within 90 days (collapse) | 55,879 ms | 51,438 ms | **17 ms** | 3,287× |
| RQ11 | awards >$5m expiring 365d (award rows) | 74,239 ms | 20,013 ms | **4.7 ms** | 15,795× |
| RQ12 | GSA psc D302 acted 90d (award rows) | 14,136 ms | 10,616 ms | **33 ms** | 435× |
| RQ13 | actions >$5m naics 237310 90d (txn rows) | 713 ms | 544 ms | **39 ms** | 18× |
| RQ14 | subk-plan psc R499 180d (txn rows) | 3,121 ms | 24,628 ms | **90 ms** | 35× |

**Result parity: 14/14 exact.** Every arm returned identical totals on every workload (RQ09: 165 = 165; RQ10: 30,465 = 30,465 — the v1 month-grain drift disappeared once the exact `current_end_date` predicate ran against the promoted award-state table).

## Headline

- **Every phrase.v2 family answers in ≤134 ms** from the artifact; median across the matrix ≈ 35 ms.
- The live lane's worst shapes — the two-lane award-universe flagship (70 s) and expiring-award rows (74 s, cold spikes to 244 s) — are exactly the operator-described queries, and they collapse to 134 ms / 4.7 ms.
- Arm B (slice→DuckDB) helps the join shapes ~2–4× but is bounded by R2 streaming; it is not a substitute for the artifact, only a fallback.

## Tier C verdicts (measured, final)

| Table | Verdict | Evidence |
|---|---|---|
| `usaspending_fpds_prime_award_state` (82.9M × 43, sorted `current_end_date`) | **PROMOTED — in the artifact** | RQ11 74s→4.7ms; RQ09/10 exact parity; export cost 131 s |
| `gtm_entity_inferred_primeable_codes` (263M, sorted `code_type, code`) | **PROMOTED** | RQ06/08 82–130×; export 102 s |
| `gtm_entity_inferred_subbable_codes` (160M, sorted `code_type, code`) | **PROMOTED** | RQ07 185×; export 67 s |
| `gtm_subaward_recipient_code_evidence` (92M) | **STAYS OUT** | No phrase.v2 shape touches it (subout drill-down only); re-gate when a workload exists |

Artifact v2: `s3://data-sink/query-sidecar/query_sidecar_20260709T012224Z.duckdb` — **23.80 GiB, 40 tables, ~708M rows, 40/40 parity**, full rebuild <10 min on one container. `LATEST.json` swapped.

## Notes

1. Arm A cold-start variance is real and large (first-run values up to 244 s in v1, 52 s here on RQ01) — production felt latency on cold handles is worse than the warm medians above.
2. The v1 harness's hybrid Tier C legs measured 100–210 s — a harness defect (row-at-a-time `executemany`), superseded by native legs in this run; recorded for honesty, carries no verdict weight.
3. `gtm_award_expiry_months` remains in the artifact (Cycle B wiring) but the benchmark's expiring shapes now use the exact award-state predicate.
4. Sidecar download-to-NVMe: 205 s for 23.8 GiB — a boot-time cost for the Phase 3 warm process, irrelevant per-query.

## Gate outcome

**Phase 3 (warm serving process) and Phase 4 (wiring `/phrase`-lane execution to it) are GO**, with the artifact as-is (Tiers A+B+C+D). The serving process ships as a Render/Railway-shaped read-only HTTP-SQL service (`apps/query_sidecar_api`) — Modal `web_endpoint` is forbidden by doctrine (`docs/reference/03_modal_compute.md`).
