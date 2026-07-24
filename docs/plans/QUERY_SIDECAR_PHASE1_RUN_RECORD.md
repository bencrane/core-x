# Query-Sidecar Phase 1 — Build Run Record

Executed 2026-07-09 (UTC). Builder: `pipelines/query_sidecar/build_query_sidecar.py` (standalone Modal app `query-sidecar`, `modal run` invoked, no dispatcher/cron). Manifest: [SIDECAR_PHASE0_MART_MANIFEST.md](SIDECAR_PHASE0_MART_MANIFEST.md), Tiers A+B+D.

## Published artifact

- **File:** `s3://data-sink/query-sidecar/query_sidecar_20260709T000838Z.duckdb` — **7.75 GiB, 37 tables, 202,035,503 rows**
- **Pointer:** `s3://data-sink/query-sidecar/LATEST.json` (blue-green: versioned file first, pointer swap second; prior versions retained)
- **Ledger:** `ops.query_sidecar_runs` — `status=success`, `latest_updated=true`
- Smoke artifact (Tier A only, 290 MiB) retained at `query-sidecar/smoke/`

## Acceptance probe (cold, laptop → R2 over httpfs, no warm server)

- `ATTACH 's3://…/query_sidecar_….duckdb' (READ_ONLY)`: 3.5 s
- **Point lookup on the 108M-row `gtm_txn_events_slim` (`WHERE uei = ?`): 0.50 s** — sorted clustering → zone-map range reads
- Cross-mart join (`gtm_prime_sub_pairs` × `gtm_entity_behavior_rollup`, group/top-5): 1.15 s
- The Phase 3 warm process (local NVMe, resident engine) strictly improves both.

## Build performance

Whole 202M-row build + publish: ~5 minutes wall on one Modal container (128 GiB RAM, 8 cpu, 512 GiB ephemeral NVMe). Largest single sort: `gtm_txn_events_slim` 107.9M rows in 83 s. DuckDB config: `memory_limit=96GB`, `temp_directory` on NVMe, `preserve_insertion_order=true` (required so CTAS `ORDER BY` survives the parallel insert).

## Run notes

1. First full run **failed by design**: the parity guard compared `agency_vocab` (an aggregation, 75 rows) against its 30.7M-row source — category error in the guard, not the data. Ledger recorded `error`, nothing published, LATEST untouched. Guard fixed (aggregates check non-emptiness), rerun clean. The failure path worked exactly as built.
2. `agency_vocab` yields **75** deduped codes under the market_store rule (NULL/empty-guarded, majority name per code); the `~136 distinct pairs` figure in catalyst's comment counts name variants before dedup.
3. `gtm_sub_universe_targets` exported at its live 1 row (recipe run for a single target so far) — wired, will grow with the mart.
4. Second copies shipped for both-side clustering: `gtm_prime_sub_pairs_by_sub`, `subaward_canonical_slim_by_sub` (sub-side sort; duplication free at these sizes).
5. In-file metadata: `_sidecar_meta` (build stamp, tiers, manifest source) and `_sidecar_manifest` (per-table parity incl. pinned Lance versions) are baked into the artifact — any consumer can audit provenance with SQL.

## Per-table parity (37/37 OK; lance version pinned at read)

```
gtm_entity_behavior_rollup: 261,789 rows in 0.8s (lance v8=261,789) parity=OK
gtm_sam_entities: 2,025,707 rows in 4.0s (lance v24=2,025,707) parity=OK
gtm_entity_code_lanes: 1,672,844 rows in 0.9s (lance v15=1,672,844) parity=OK
gtm_entity_geo: 1,452,430 rows in 1.0s (lance v3=1,452,430) parity=OK
gtm_naics_psc_pairs: 320,846 rows in 0.9s (lance v4=320,846) parity=OK
naics_reference: 2,129 rows in 0.4s (lance v14=2,129) parity=OK
psc_reference: 6,108 rows in 0.3s (lance v8=6,108) parity=OK
gtm_txn_events_slim: 107,948,116 rows in 50.5s (lance v7=107,948,116) parity=OK
gtm_txn_recipient_month_rollup: 34,080,799 rows in 10.3s (lance v7=34,080,799) parity=OK
gtm_award_recipient_rollup: 6,301,649 rows in 2.7s (lance v6=6,301,649) parity=OK
gtm_award_expiry_months: 221,444 rows in 0.6s (lance v3=221,444) parity=OK
gtm_prime_pop_lanes: 547,379 rows in 0.7s (lance v4=547,379) parity=OK
gtm_prime_sub_pairs: 268,562 rows in 0.8s (lance v3=268,562) parity=OK
gtm_prime_sub_pairs_by_sub: 268,562 rows in 0.6s (lance v3=268,562) parity=OK
gtm_sub_universe_pairs: 29,605 rows in 0.4s (lance v22=29,605) parity=OK
gtm_sub_universe_targets: 1 rows in 0.3s (lance v16=1) parity=OK
gtm_prime_combo_lanes: 5,116,397 rows in 2.9s (lance v4=5,116,397) parity=OK
gtm_sub_combo_lanes: 339,485 rows in 1.1s (lance v4=339,485) parity=OK
gtm_prime_farmout_combo_lanes: 37,569 rows in 0.7s (lance v4=37,569) parity=OK
gtm_prime_vehicle_lanes: 16,128 rows in 0.4s (lance v3=16,128) parity=OK
gtm_open_awards: 163,061 rows in 1.2s (lance v4=163,061) parity=OK
gtm_prime_demand_events: 11,339,168 rows in 8.4s (lance v5=11,339,168) parity=OK
gtm_primes_by_recipient_code: 1,720,331 rows in 1.1s (lance v3=1,720,331) parity=OK
gtm_prime_subout_by_recipient_code: 11,844,606 rows in 4.1s (lance v4=11,844,606) parity=OK
gtm_subbed_under_to_primed_in_cooccurrence: 589,260 rows in 0.7s (lance v5=589,260) parity=OK
gtm_sub_profiles: 105,189 rows in 1.0s (lance v2=105,189) parity=OK
govcon_subawardee_profiles: 25,450 rows in 1.0s (lance v70=25,450) parity=OK
subaward_canonical_slim: 1,315,680 rows in 3.9s (lance v31=1,315,680) parity=OK
subaward_canonical_slim_by_sub: 1,315,680 rows in 3.6s (lance v31=1,315,680) parity=OK
federal_sites_lance: 300,414 rows in 1.2s (lance v4=300,414) parity=OK
firmographics_blitz: 255,418 rows in 2.4s (lance v91=255,418) parity=OK
gtm_sam_people: 2,252,385 rows in 2.3s (lance v33=2,252,385) parity=OK
gtm_sam_person_contactability: 152,447 rows in 1.3s (lance v3=152,447) parity=OK
sam_pocs: 8,065,679 rows in 5.9s (lance v302=8,065,679) parity=OK
sam_master_entities: 1,541,566 rows in 5.5s (lance v8=1,541,566) parity=OK
people_canonical: 131,545 rows in 1.1s (lance v10=131,545) parity=OK
agency_vocab: 75 rows in 1.0s (lance v24=30,697,295) parity=OK
```

## Rebuild

> **2026-07-23 SUPERSEDED:** launch via the `/sidecar-build` skill — `modal deploy`, then
> spawn on the deployed app (`modal.Function.from_name("query-sidecar","build").spawn(...)`).
> A client-tethered `modal run …::run` (with or without `--detach`) issues a SYNC input the
> server cancels ~90 s after client loss; it killed 8 builds. `::run` now spawn-fires and
> returns. Historical commands below preserved as the Phase-1 record:

```
modal run pipelines/query_sidecar/build_query_sidecar.py::run          # full A,B,D + LATEST swap
modal run pipelines/query_sidecar/build_query_sidecar.py::smoke        # Tier A → smoke/ prefix, no pointer
modal run pipelines/query_sidecar/build_query_sidecar.py::initdb      # ledger DDL (idempotent)
```

## Next (Phase 2 gate)

Benchmark real compiled `phrase.v2` plans across three arms — (a) status-quo DuckDB-over-Lance, (b) prefiltered-slice lane, (c) this artifact — to decide Tier C inclusion (inferred-code projections 263M/160M, prime_award_state 83M, subaward evidence 92M) and quantify the serving win before Phase 3 (warm process) / Phase 4 (app wiring).
