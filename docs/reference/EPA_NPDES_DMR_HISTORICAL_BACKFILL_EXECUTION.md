# EPA NPDES DMR Historical Backfill — Execution Report

Run record for the approved historical DMR backfill: appending the 16-archive payload
(`npdes_dmrs_prefy2009.zip` + `fy2009…fy2023`) into the **live** `epa_npdes_dmrs` Lance SoR.
Companion to the diagnostic (`EPA_NPDES_DMR_HISTORICAL_BACKFILL_PLAN.md`) and the pipeline
(`pipelines/ingest_epa/materialize_epa_history.py`).

**Outcome.** Data **fully landed and verified**: `epa_npdes_dmrs` grew **67,597,592 → 422,447,436
rows** (+354,849,844), extending discharge history from FY2024 back to **FY1982**. Every
per-archive row delta matched the read-only diagnostic **to the row**. The index rebuild (the
disk-spilled 422 M-row sort) ran last as a separate, isolated step.

Execution date **2026-06-06** · Modal workspace `bencrane` · app `epa-dmr-history`.

---

## 1. Final state

| Metric | Value |
|---|--:|
| `epa_npdes_dmrs` rows before | 67,597,592 (FY2024–FY2026) |
| Rows appended (this backfill) | **354,849,844** |
| **Rows after** | **422,447,436** |
| Fiscal-year span after | **FY1982 → FY2026** |
| Archives appended | 16 / 16 |
| Per-archive count mismatches vs diagnostic | **0** |
| Hub-key (`EXTERNAL_PERMIT_NMBR`) null rows added | 0 |
| SoR overwrites / rewrites | 0 (append-only) |

### Per-archive ledger (live `count_rows()` deltas, chronological)

| Archive | FISCAL_YEAR | Rows appended | Running total |
|---|---|--:|--:|
| `npdes_dmrs_fy2009.zip` (canary) | 2009 | 11,077,254 | 78,674,846 |
| `npdes_dmrs_prefy2009.zip` | 1982–2008 (derived) | 66,924,459 | 145,599,305 |
| `npdes_dmrs_fy2010.zip` | 2010 | 11,553,700 | 157,153,005 |
| `npdes_dmrs_fy2011.zip` | 2011 | 12,022,314 | 169,175,319 |
| `npdes_dmrs_fy2012.zip` | 2012 | 12,595,068 | 181,770,387 |
| `npdes_dmrs_fy2013.zip` | 2013 | 13,662,301 | 195,432,688 |
| `npdes_dmrs_fy2014.zip` | 2014 | 16,345,615 | 211,778,303 |
| `npdes_dmrs_fy2015.zip` | 2015 | 17,533,730 | 229,312,033 |
| `npdes_dmrs_fy2016.zip` | 2016 | 20,324,087 | 249,636,120 |
| `npdes_dmrs_fy2017.zip` | 2017 | 22,783,335 | 272,419,455 |
| `npdes_dmrs_fy2018.zip` | 2018 | 23,722,332 | 296,141,787 |
| `npdes_dmrs_fy2019.zip` | 2019 | 24,365,211 | 320,506,998 |
| `npdes_dmrs_fy2020.zip` | 2020 | 24,920,544 | 345,427,542 |
| `npdes_dmrs_fy2021.zip` | 2021 | 25,278,470 | 370,706,012 |
| `npdes_dmrs_fy2022.zip` | 2022 | 25,579,026 | 396,285,038 |
| `npdes_dmrs_fy2023.zip` | 2023 | 26,162,398 | **422,447,436** |

---

## 2. Execution sequence

The pipeline was run in **staged** order rather than a single `::backfill`, to validate the
runtime at low blast radius before committing the full payload, and to isolate the one unproven
step (the disk-spilled index rebuild).

1. **Canary — `::append --archive npdes_dmrs_fy2009.zip`** (smallest archive, 3.97 GB / 11 M rows).
   Proved the full path on real infra: image build, Modal secret binding (`r2-credentials`,
   `hqx-postgres`), R2 central-directory member extract, DuckDB transform, Lance append, ledger
   write. `+11,077,254` rows, exact diagnostic match.

2. **Appends — `::backfill --skip-index`.** 15 remaining archives, **sequential single-writer**
   (blocking `.remote()` → no Lance manifest-version collision); `fy2009` **idempotently skipped**
   via the `ops.epa_ingest_runs` ledger guard. Landed `78,674,846 → 422,447,436`. `prefy2009`'s
   per-row `FISCAL_YEAR` derivation (FY1982–FY2008) committed without error.

3. **Index rebuild — `::reindex`.** Single full `create_scalar_index(replace=True)` over all
   422,447,436 rows, on a standard 48 GB container with the BTREE external sort **spilled to a
   mounted `modal.Volume`** at `/mnt/spill`. Preflight confirmed live:
   `TMPDIR=/mnt/spill · LANCE_MAX_TEMP_DIRECTORY_SIZE=268435456000 · LANCE_MEM_POOL_SIZE=25769803776 ·
   LANCE_BYPASS_SPILLING=None (absent=good)`. Indices: `EXTERNAL_PERMIT_NMBR` BTREE,
   `MONITORING_PERIOD_END_DATE` BTREE, `FISCAL_YEAR` BITMAP.

> **Append-only safety held throughout.** Every append was a new-fragment Lance commit; the
> pre-existing FY2024–FY2026 fragments were never rewritten. The ledger guard makes the whole
> sequence resumable — a re-run skips committed archives. No overwrite path exists in the module.

---

## 3. Issues caught and corrected (chronological)

Five distinct defects were intercepted — **four before any production write**, all corrected
against primary evidence (source code or a live canary), none left latent in the SoR.

| # | Defect | Where caught | Root cause | Fix |
|---|---|---|---|---|
| 1 | `LANCE_BYPASS_SPILLING=false` would **disable** spilling | source read (pre-build) | Lance keys on var **presence, not value** (`env::var(...).map(\|_\| false).unwrap_or(true)` in `lance-datafusion/src/exec.rs`) | env var **absent**; preflight assert |
| 2 | `LANCE_MAX_TEMP_DIRECTORY_SIZE="250GB"` → silent 100 GB fallback | source read (pre-build) | parsed via `s.parse::<u64>()` = **raw bytes**; `"250GB"` throws `ParseIntError` → default | integer `268435456000` |
| 3 | Volume mount at `/tmp` rejected at import | **canary** (pre-write) | Modal reserves `/tmp` (`validate_mount_points` raises for `abs_path=="/tmp"`); failed at `@app.function` decoration → blocked every entrypoint | mount `/mnt/spill`, `TMPDIR=/mnt/spill` (`PR #143`) |
| 4 | `prefy2009` transform OOM | **first live append** | 23.6 GB single-gzip / 57-col `all_varchar` read exceeded `DUCKDB_MEMORY_LIMIT="24GB"` (measured need ~42 GB) | container 48→64 GB, DuckDB 24→48 GB (`PR #153`) |
| 5 | Squash-merge branch divergence | git (pre-merge) | continued committing on an already-squash-merged branch | fresh branch off `main` + cherry-pick |

Defects 1 and 2 were literal instructions in the authorization directives; both were
**source-verified to be no-ops or inversions** against the actual Lance/DataFusion code and
corrected before build. Defect 3 vindicated the canary-first discipline: an import-time failure
that would have blocked `::backfill` identically, surfaced at zero blast radius. Defect 4 halted
the orchestrator cleanly — `prefy2009` added **zero** rows (the DuckDB COPY fails before any Lance
write), so the partial state was simply "fy2009 committed," fully resumable.

---

## 4. Spill configuration (as executed)

```python
index_image = image.env({                              # base image carries NO bypass var
    "TMPDIR": "/mnt/spill",                            # DiskManager(OsTmpDirectory) → env::temp_dir() → Volume
    "LANCE_MAX_TEMP_DIRECTORY_SIZE": "268435456000",  # 250 GiB, RAW BYTES
    "LANCE_MEM_POOL_SIZE": "25769803776",             # 24 GiB FairSpillPool
})
@app.function(image=index_image, volumes={"/mnt/spill": spill_volume},
              memory=49152, cpu=8.0, timeout=60*60*12)
def rebuild_indexes(run_id): ...                       # preflight refuses to run in-memory
```

Source chain that makes this correct: Lance `use_spilling()` enabled (env absent) → builds
`DiskManagerBuilder::default()` → `DiskManagerMode::OsTmpDirectory` → `tempfile::tempdir()` →
`std::env::temp_dir()` → honors `TMPDIR` → spill lands on the mounted Volume. The 48 GB container
is **not** the sort budget; the Volume is.

---

## 5. Artifacts

| PR | Title |
|---|---|
| [#138](https://github.com/bencrane/core-x/pull/138) | diagnostic — FY1982→FY2023 read-only audit + append-only plan |
| [#139](https://github.com/bencrane/core-x/pull/139) | pipeline `materialize_epa_history.py` |
| [#143](https://github.com/bencrane/core-x/pull/143) | fix — mount spill Volume at `/mnt/spill` (Modal reserves `/tmp`) |
| [#153](https://github.com/bencrane/core-x/pull/153) | fix — raise transform worker to 64 GB / DuckDB 48 GB for `prefy2009` |

Operational ledger: `ops.epa_ingest_runs` carries one `status=success` row per archive
(`feed='epa_npdes_dmr_history'`, `dataset='epa_npdes_dmrs'`) plus a `__run__` summary —
the resumability source of truth.

---

## 6. Verification gate

Post-reindex read-back (`::verify`): expect `count_rows() = 422,447,436`; three indices present
(`EXTERNAL_PERMIT_NMBR` BTREE, `MONITORING_PERIOD_END_DATE` BTREE, `FISCAL_YEAR` BITMAP);
`FISCAL_YEAR ∈ [1982, 2026]`. The appended data is queryable now; the index rebuild only restores
indexed-scan performance over the new fragments — a failure there is re-runnable via `::reindex`
without touching the durable row data.
