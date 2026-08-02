# pipelines/tic_mrf — TiC reverse-mapper (Aetna & UHC)

Turns payer Transparency-in-Coverage (TiC) machine-readable files into a flat,
indexed, append-only Lance fact table of **negotiated commercial rates for a
target NPI cohort** — plus the **employer→file bridge** that supplies the
volume-proxy side of book-of-business sizing.

Full architecture, measured metrics, join rules, and the preflight checklist:
[`docs/analysis/tic_payer_integration_poc.md`](../../docs/analysis/tic_payer_integration_poc.md).

## Files
- `reverse_map.py` — out-of-core engine. Two-source streaming join: stream
  `provider_references` → keep matched groups **with their TIN**
  (`tin_type`/`tin_value`/`tin_business_name`) → stream `in_network[]` → emit one
  flat row per (npi × billing_code × price), TIN-carrying. RAM = O(cohort), not
  O(payload). Multi-member gzip, 429/503 backoff, `yajl2_c` backend asserted at
  worker start. Per-file ingestion is ATOMIC: rows stage to local Lance and
  commit to the SoR as one append only on complete success (`publish_stage_to_sor`).
  Runs standalone for sampling (`uv run reverse_map.py --innetwork <url> --npis a,b,c --cap-mb 0`).
- `orchestrate.py` — Modal production fan-out, blast-radius-isolated phases:
  `build_worklist` (payer-aware manifest → deduped token-stripped URLs + SAS
  carried separately) → `build_employer_bridge` (employer ToCs →
  `tic_employer_file_bridge`) → `run_fanout` (`process_file` fanned, ledger-
  guarded, SAS auto-refresh on 403/409) → `rebuild_indexes` (isolated).
- `part1_filter_spine.py` — deterministic cohort extractor from the local SoR
  (`practice_group_360` + `form5500_main`); `--group-snapshot` pins the partition.
- `ops_tic_reverse_map_runs.sql` — idempotency / terminal-state ledger, keyed
  `(payer, source_file_url, file_version)`: URL **token-stripped** (SAS `sig`
  re-mints per index fetch; the blob path is the identity), `file_version`
  **never NULL** (ETag > Last-Modified > date-slug > bytes surrogate).
- `test_reverse_map.py` — offline unit tests (synthetic fixture, no network):
  `uv run --with "ijson>=3.3" --with pytest python -m pytest pipelines/tic_mrf/test_reverse_map.py -q`.

## Launch (deploy + spawn — never sync-drive the fan-out)
```bash
modal deploy pipelines/tic_mrf/orchestrate.py        # REQUIRED first
modal run pipelines/tic_mrf/orchestrate.py::build_worklist --payer uhc
modal run pipelines/tic_mrf/orchestrate.py::bridge --payer uhc
modal run pipelines/tic_mrf/orchestrate.py::run --payer uhc --cohort-key active/tic_cohort/ny_ortho.json
#   ^ spawn-fires run_fanout on the DEPLOYED app, prints the fc-id, returns
modal run pipelines/tic_mrf/orchestrate.py::rebuild_indexes
```

## Three corrected premises (proven against live data, 2026-06-07)
- **No Type-2/org NPI/EIN/TIN in the SoR** — PECOS `group_enrlmt_id` is the group billing anchor; `member_npis` is the Type-1 array. The org identifier is captured from the MRFs themselves: every provider_group's `tin` rides onto each rate row.
- **NPIs are not in the ToC** — matching requires descending an in-network file and resolving `provider_references` by id.
- **UHC blind-filename GET fails (SAS-gated, 0/15)** — fetch the master index and use its `downloadUrl`; the SAS is a credential, split from the stripped-URL identity.

## Outputs
- `s3://data-sink/active/tic_negotiated_rates/` — append-only Lance fact table;
  BTREE `npi`, `billing_code`, `tin_value`; BITMAP `payer`, `billing_class`,
  `billing_code_type`, `tin_type`. **Join rule:** org-level joins (Form 5500,
  bridge, entity graph) gate on `tin_type='ein'` — `tin_type='npi'` rows are
  sole proprietors whose TIN is their own NPI, never an EIN.
- `s3://data-sink/active/tic_employer_file_bridge/payer=<payer>/` — sponsor EIN
  (digits-normalized) ↔ in-network file URL (token-stripped) ↔ plan metadata;
  BTREE `ein`, `in_network_url`. Candidate-employer sets per file (fan-in ≈9:1
  UHC, up to ~1,075:1 Cigna), joinable to `form5500_main.SPONS_DFE_EIN`.
