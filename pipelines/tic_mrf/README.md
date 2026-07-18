# pipelines/tic_mrf — TiC reverse-mapper (Aetna & UHC)

Turns payer Transparency-in-Coverage (TiC) machine-readable files into a flat,
indexed, append-only Lance fact table of **negotiated commercial rates for a
target NPI cohort** — the input to off-market clinical-acquisition rate-positioning.

Full design, measured metrics, and the nationwide projection:
[`docs/analysis/tic_payer_integration_poc.md`](../../docs/analysis/tic_payer_integration_poc.md).

## Files
- `reverse_map.py` — out-of-core engine. Two-source streaming join: stream
  `provider_references` → keep group ids intersecting the cohort → stream
  `in_network[]` → emit one flat row per (npi × billing_code × price). RAM =
  O(cohort), not O(payload). Handles multi-member gzip. Runs standalone for the
  POC (`uv run reverse_map.py --innetwork <url> --npis a,b,c --cap-mb 0`).
- `orchestrate.py` — Modal production fan-out, three blast-radius-isolated phases:
  `build_worklist` (payer-aware manifest → deduped in-network URLs) → `run`
  (`process_file` fanned across workers, idempotency-guarded, append fragments) →
  `rebuild_indexes` (isolated scalar-index build).
- `part1_filter_spine.py` — deterministic cohort extractor from the local SoR
  (`practice_group_360` + `form5500_main`).
- `ops_tic_reverse_map_runs.sql` — idempotency / terminal-state ledger
  (`HQX_DB_URL_POOLED`), keyed `(payer, source_file_url, file_version)`.

## Two corrected premises (proven against live data)
- **No Type-2/org NPI/EIN/TIN in the SoR** — PECOS `group_enrlmt_id` is the group billing anchor; `member_npis` is the Type-1 array.
- **NPIs are not in the ToC** — matching requires descending an in-network file and resolving `provider_references` by id.
- **UHC blind-filename GET fails (SAS-gated, 0/15)** — fetch the master index and use its `downloadUrl`.

## SoR
`s3://data-sink/active/tic_negotiated_rates/` — append-only Lance; BTREE `npi`,
`billing_code`; BITMAP `payer`, `billing_class`, `billing_code_type`.
