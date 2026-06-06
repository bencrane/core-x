# MSHA Remaining-Archive Ingest Plan (12 un-ingested → 11 new Lance datasets)

> ⚠️ **CORRECTED IN PART — do not execute the flagged sections verbatim.** An adversarial
> review ([`MSHA_REMAINING_INGEST_PLAN_ADVERSARIAL_REVIEW.md`](MSHA_REMAINING_INGEST_PLAN_ADVERSARIAL_REVIEW.md))
> live-probed this plan and found **3 BLOCKERs, independently re-verified** against R2 (2026-06-06):
>
> 1. **§1 MinesProd union is wrong.** `MinesProd{Q,Y}` share only **3 of 13/11** column names —
>    the same fields are renamed twins (`STATE`/`STATE_ABBR`, `CAL_YR`/`CALENDAR_YR`,
>    `COAL_METAL_IND`/`C_M_IND`, `HOURS_WORKED`/`ANNUAL_HRS`, …), so `UNION ALL BY NAME` yields a
>    21-col sparse-NULL table — the exact anti-pattern this plan forbids for samples. It does **not**
>    mirror ContractorProd (which shares 6 natively). **Corrected: ship Q and Y as two separate
>    verbatim datasets** (`msha_mine_production_quarterly`, `_yearly`), no blind union.
> 2. **§0/§1 grain keys are wrong for ≥4 datasets.** `VIOLATION_NO` is absent from ContestedViolations
>    (real key `CITATION_NO`); `EVENT_NO` is absent from Conferences (`CONFERENCE_NO`) and OrdersIssued;
>    `DOCKET_NO` and `EVENT_NO` are 1:many; "sample id" differs per sample file. **Re-derive every key
>    from live `DESCRIBE` before locking `INDEX_PLAN`** — for single-file ingests `lance_rows==spine_rows`
>    does NOT prove grain (add an explicit `count(*)==count(DISTINCT key)` check in Phase 0).
> 3. **§4 OrdersIssued is mis-specified and redundant.** Every value is tab-prefixed (`"\t3605466"` —
>    the `"`-only dequote leaves the tab and breaks the join key) and columns carry spaces/parens/`@`;
>    it duplicates the live `msha_enforcement_ledger` (`CIT_ORD_SAFE='Order'`). **Corrected: drop it
>    from scope.** (`skip=3` per §4 is also a verified no-op.)
>
> Also: patch the cloned `verify_datasets` for non-`[A-Z0-9_]` columns (Inspections has `SUM(...)`
> columns), and add a `worker` column to `ops.msha_ingest_runs` for per-worker provenance.
> **Net corrected shape: 11 archives → 11 datasets (OrdersIssued excluded), 5 → 16 active, 19/20
> archives represented.** Everything else verified **sound** — inventory, arithmetic,
> overwrite-idempotency defense, sub-Giants/resource config, the `single`-path clone recipe, and the
> no-bridge guardrail. **Read the review before executing.**

**Status:** corrected directive — execute per the review's remediations, not the flagged sections verbatim.
**Scope:** materialize the 12 MSHA landing archives that have **zero** active representation
into the Gen-3 Lance SoR, bringing MSHA from **5 → 16** active datasets and **8/20 → 20/20**
archives represented.
**Posture:** read-the-recipe-then-clone. Every helper already exists in the two shipped
workers — you copy proven code, you do **not** invent a new ingestion pattern.

> **Ground-truth correction.** A prior note said "15 remain." That was the *pre-extensions*
> number. The extensions worker (PR #144) ingested `Accidents`, `ContractorProdQuarterly`,
> `ContractorProdYearly`. **Verified live (boto3, 2026-06-06): 20 archives, 8 ingested, 12
> remaining.** Plan against 12.

---

## 0. Verified inventory — the 12 targets (live R2 listing + profiled grain)

| # | Archive | Compressed | Rows¹ | Cols¹ | Native grain / keys¹ | Domain |
|--:|---|--:|--:|--:|---|---|
| 1 | `Inspections.zip` | 69.3 MiB | 1,147,232 | 45 | `EVENT_NO` · `MINE_ID` · `CONTRACTOR_ID` | enforcement-activity |
| 2 | `ContestedViolations.zip` | 28.0 MiB | 448,158 | 39 | `VIOLATION_NO` · `DOCKET_NO` | litigation |
| 3 | `CivilPenaltyDocketsDecisions.zip` | 13.8 MiB | 479,439 | 29 | `DOCKET_NO` | litigation |
| 4 | `Conferences.zip` | 1.0 MiB | 161,623 | 7 | `EVENT_NO` | litigation |
| 5 | `OrdersIssued.zip` ⚠️ | 0.2 MiB | ~3,829 | 13 | `EVENT_NO` / order id | enforcement-activity |
| 6 | `MinesProdQuarterly.zip` | 53.7 MiB | 2,714,840 | 13 | `MINE_ID`·qtr·`SUBUNIT_CD` | production |
| 7 | `MinesProdYearly.zip` | 6.7 MiB | 657,546 | 11 | `MINE_ID`·yr·`SUBUNIT_CD` | production |
| 8 | `CoalDustSamples.zip` ⚠️ | 105.4 MiB | 2,985,614 | 30 | sample id · `MINE_ID` · `EVENT_NO` | IH-sampling |
| 9 | `PersonalHealthSamples.zip` | 5.9 MiB | 310,908 | 20 | sample id · `MINE_ID` | IH-sampling |
| 10 | `NoiseSamples.zip` | 6.0 MiB | 274,645 | 29 | sample id · `MINE_ID` | IH-sampling |
| 11 | `QuartzSamples.zip` | 5.6 MiB | 167,238 | 19 | sample id · `MINE_ID` | IH-sampling |
| 12 | `AreaSamples.zip` | 0.2 MiB | 8,368 | 17 | sample id · `MINE_ID` · `EVENT_NO` | IH-sampling |

¹ Row/col/key figures from `MSHA_DATA_PROFILING_REPORT.md` (live zip central-directory + sample
scan). **Exact column names are NOT hardcoded** — you derive them live via `DESCRIBE` on the
transcoded file (the `_describe()` helper). Total ≈ **9.36 M rows**. Largest single file =
CoalDustSamples 2.99 M rows ≈ 1 GiB uncompressed → **all 12 are far below the ~100 M-row
"Giants" Volume-staging threshold; the direct-R2 streaming path applies to every one.**

⚠️ = non-standard wire format requiring a custom read recipe — see §4. These are the only two
that deviate from the canonical `quote=''` + CP1252 recipe.

---

## 1. Target datasets — 11 new Lance tables under `s3://data-sink/active/`

`MinesProdQuarterly` + `MinesProdYearly` **unite** into one dataset (same entity = mine
production firmographics at two cadences) — this mirrors the shipped
`ContractorProd{Q,Y} → msha_contractors` precedent exactly. The five `*Samples` stay
**separate** (distinct measurement semantics + disjoint column sets; a `UNION ALL BY NAME`
would conflate dust/noise/silica/area into a sparse NULL-filled mega-table — do not do it).

| New dataset | Source archive(s) | Kind | Anchor grain |
|---|---|---|---|
| `msha_inspections` | `Inspections.zip` | single | `EVENT_NO` |
| `msha_contested_violations` | `ContestedViolations.zip` | single | `VIOLATION_NO` |
| `msha_penalty_dockets` | `CivilPenaltyDocketsDecisions.zip` | single | `DOCKET_NO` |
| `msha_conferences` | `Conferences.zip` | single | `EVENT_NO` |
| `msha_orders_issued` ⚠️ | `OrdersIssued.zip` | single | `EVENT_NO` |
| `msha_mine_production` | `MinesProdQuarterly` ⊎ `MinesProdYearly` | **union** | `MINE_ID`·period·`SUBUNIT_CD` |
| `msha_coal_dust_samples` ⚠️ | `CoalDustSamples.zip` | single | sample id |
| `msha_personal_health_samples` | `PersonalHealthSamples.zip` | single | sample id |
| `msha_noise_samples` | `NoiseSamples.zip` | single | sample id |
| `msha_quartz_samples` | `QuartzSamples.zip` | single | sample id |
| `msha_area_samples` | `AreaSamples.zip` | single | sample id |

**Naming/fidelity guardrail (inherit Directive-29):** land in isolation on MSHA's native keys.
Columns stay **verbatim UPPERCASE**; the only non-native columns are `source_file` +
`ingested_at`. **No `core.name_norm`, no `normalized_legal_name`, no cross-universe bridge
column.** Right-side collision-namespacing only if you introduce a join (none planned here).

---

## 2. Worker decomposition — 3 new domain workers (blast-radius containment)

ARCHITECTURE §3 groups Modal apps strictly by domain. The 12 archives span three domains, and
the two ⚠️ custom parsers must not be able to block the clean datasets. Split accordingly:

| Worker file (new) | Modal app | Datasets owned |
|---|---|---|
| `pipelines/ingest_msha/materialize_msha_production.py` | `msha-production-pipelines` | `msha_mine_production` |
| `pipelines/ingest_msha/materialize_msha_enforcement.py` | `msha-enforcement-pipelines` | `msha_inspections`, `msha_contested_violations`, `msha_penalty_dockets`, `msha_conferences`, `msha_orders_issued` ⚠️ |
| `pipelines/ingest_msha/materialize_msha_samples.py` | `msha-samples-pipelines` | `msha_coal_dust_samples` ⚠️, `msha_personal_health_samples`, `msha_noise_samples`, `msha_quartz_samples`, `msha_area_samples` |

**Why three, not one:** (a) domain isolation per ARCHITECTURE §3; (b) the two ⚠️ deviant
parsers land in **different** workers, so a recipe bug in one cannot wedge the other or any
clean dataset; (c) the production union is a proven, low-risk shape that should ship first and
independently. **Intra-worker isolation is already built in:** the canonical `_materialize_one`
loop is per-dataset `try/except` + a `--only <dataset>` flag, so one dataset failing never
aborts its siblings or corrupts a committed table.

---

## 3. The canonical recipe to CLONE (do not rewrite)

Both shipped workers — [`materialize_msha.py`](../../pipelines/ingest_msha/materialize_msha.py)
(single + join kinds) and
[`materialize_msha_extensions.py`](../../pipelines/ingest_msha/materialize_msha_extensions.py)
(`union` kind) — are the templates. Clone `materialize_msha.py` for each new worker; lift the
`union` helpers (`_projection_sql`, `_union_sql`, `UNION ALL BY NAME`) from the extensions
worker for `msha_mine_production`. Copy these helpers **verbatim** (they are load-bearing and
already battle-tested):

- `_r2_storage_options()`, `_s3_client()` — R2 auth (checksum `when_required`).
- `_download_archive()` → `_extract_member()` → `_transcode_cp1252_to_utf8()` → `_acquire()`
  — the **CP1252→UTF-8 transcode-to-scratch** step is mandatory for every archive (degree
  signs / smart quotes / ñ in operator names break DuckDB's utf-8 and latin-1 readers).
- `READ_RECIPE` = `delim='|', quote='', header=true, all_varchar=true, new_line='\r\n',
  strict_mode=false, encoding='utf-8'` — the standard recipe for **10 of 12** archives.
  `quote=''` is mandatory (MSHA wraps values in `"` but does not escape interior quotes);
  dequote per-field with `trim(BOTH '"' FROM col)`.
- `_describe()` (header-derived column names — **never guess**), `_base_expr()`,
  `_cast_expr()`, `_single_sql()` / `_projection_sql()`, `_spine_count()`.
- `_write_lance()` (streaming `reader` + `schema=reader.schema`, `mode="overwrite"`),
  `_create_indexes()`, `_committed_index_names()`.
- `_record_run()` → `ops.msha_ingest_runs` (feed=`'msha'`), `_post_callback()` (Trigger
  waitpoint), `_cleanup()`, and the `run` / `verify` / `reindex_only` / `init_ops` /
  `show_ledger` local entrypoints.

**Per-row transform (unchanged):** `read_csv(all_varchar=true)` → typed projection where every
column is retained losslessly as `nullif(trim(BOTH '"' FROM col), '')`, and the load-bearing
date/numeric columns are additionally cast. **Cast targets are chosen from a live decimal/parse
scan, not guessed** (§5). The streamed Arrow reader (`to_arrow_reader(131072)`) →
`lance.write_dataset(s3://…, mode="overwrite")` → scalar indexes built in place on R2.

**Integrity gate (copy verbatim):** after write, assert
`lance.dataset(uri).count_rows() == spine_rows` (Σ of every input file's
`read_csv` count). A mismatch = no-drop/no-fan-out violation → log loud, do not trust the
dataset. This is the hard correctness check.

---

## 4. The two parser deviations — MANDATORY pre-flight (do not assume the standard recipe)

`MSHA_DATA_PROFILING_REPORT.md` flagged exactly two archives that break the canonical recipe.
**Before locking either worker, run a header/sample dry-run** (download → transcode → print
the first ~8 raw lines + `DESCRIBE`) and confirm the recipe:

1. **`OrdersIssued.zip` → `msha_orders_issued`.** Member is `107(a)OrdersIssued.csv` (a CSV
   *Excel report export*, not the usual `.txt`): a metadata preamble occupies the top rows and
   the **real header is on line 4**, delimiter `|`. The standard `header=true` read will ingest
   preamble as data. Fix: detect the header offset and read with `skip=3` (verify the offset
   live — do not hardcode blindly), keeping `quote=''` + CP1252 transcode. Tiny file (~3.8 K
   rows) — cheap to iterate.

2. **`CoalDustSamples.zip` → `msha_coal_dust_samples`.** Deviates from the quote-wrapped
   convention: values are **bare / unquoted numeric**. `quote=''` still parses it correctly and
   the `trim(BOTH '"' …)` dequote becomes a harmless no-op — but **confirm live** that the delim
   and column count match the 30-col profile before committing. It is also the heaviest file
   (2.99 M × 30) → confirm streaming + spill headroom (§6).

If a third archive surprises you at `DESCRIBE` time, treat it the same way: sample first, lock
recipe second, never let a malformed read silently drop rows (the integrity gate in §3 will
catch a drop, but a *shifted* parse can pass the count — eyeball the sample).

---

## 5. Casts & indexes — derivation rules (fill the dicts per worker)

**Cast map (`CASTS`).** Everything defaults to dequoted VARCHAR (lossless). Only add a cast for
load-bearing columns, and only after a **live decimal/parse scan** on the transcoded file
(re-use the dry-run harness pattern referenced in the shipped workers):
- `DATE` ← every `*_DT` / date column (`CAST(try_strptime(col,'%m/%d/%Y') AS DATE)`).
- `INTEGER` ← counts / years / quarters / points (`CAL_YR`, `CAL_QTR`, `SUBUNIT_CD` if numeric, `NO_*`).
- `DOUBLE` ← money / hours / production / exposure concentrations / geo (`*_AMT`, `HOURS_*`,
  `*_PRODUCTION`, sample concentration columns, `LATITUDE`/`LONGITUDE`).
- **Every id stays VARCHAR** — `MINE_ID` (7-char zero-padded), `CONTRACTOR_ID` (alpha-prefixed
  `1AD`), `EVENT_NO`, `DOCKET_NO`, `DOCUMENT_NO`, order ids. Leading zeros / alpha prefixes are
  significant. `try_cast` absorbs the rare bad cell (no row drop).

**Index plan (`INDEX_PLAN`).** Apply the standing rule — derive present columns from `DESCRIBE`,
then classify:
- **BTREE** (high-cardinality resolution + temporal range): `MINE_ID`, `EVENT_NO`,
  `VIOLATION_NO`, `DOCKET_NO`, order id, the per-file sample/document id, `CONTRACTOR_ID`,
  `CONTROLLER_ID`, `OPERATOR_ID`, and load-bearing dates (inspection/sample/decision `*_DT`).
- **BITMAP** (low-cardinality categoricals): `COAL_METAL_IND`, `SUBUNIT_CD`, sample-type / unit
  / status / result-code columns, `FIPS_STATE_CD` where present.
- Keep `LANCE_BYPASS_SPILLING=true` (env) so the high-cardinality BTREE sort stays in-memory
  (lance#2650). Index creation is **best-effort per index** (a miss logs + records, never fails
  an otherwise-good load) and is **separately re-runnable** via the `reindex_only` entrypoint
  (§6 blast-radius).

Skeleton (per worker — names filled from `DESCRIBE`):
```python
DATASETS = {
  "msha_mine_production": {"uri": f"{_ACTIVE}/msha_mine_production/", "kind": "union",
     "sources": ["MinesProdQuarterly.zip", "MinesProdYearly.zip"], "cast_key": "mineprod"},
  # … one entry per dataset the worker owns …
}
INDEX_PLAN = {
  "msha_mine_production": {"BTREE": ["MINE_ID"], "BITMAP": ["COAL_METAL_IND", "SUBUNIT_CD"]},
  # …
}
```

---

## 6. Architecture defense (flagged per mandate)

**Overwrite is the idempotency guarantee here — append-only would corrupt.** MSHA republishes
each archive as a **complete full-history snapshot** (every release restates the entire record
back to the 1970s); there is no incremental delta and no append watermark. Appending a fresh
snapshot onto the prior dataset would **duplicate the entire history on every run** — a direct
idempotency violation. The canonical, idempotent choice for a full-snapshot republished source
is `mode="overwrite"`: a re-run atomically replaces, never duplicates, and yields **one Arrow
schema across all eras** (no per-fragment schema drift — the exact property verified in the
legal-entity diagnostic). Append-only/new-fragment semantics are reserved for genuinely
incremental feeds (e.g. SAM `entity_registrations`, which stacks monthly snapshots *with* an
`extract_label` provenance key + latest-per-key dedup downstream). **Do not "modernize" these
workers to append-on-top — that is the anti-pattern for this source class.**

**Out-of-core / resource config (per Modal function).** All 12 are sub-Giants → direct-R2
streaming, bounded RSS:
- `memory=32768` (32 GiB), `cpu=8.0`, `ephemeral_disk=524288` (512 GiB Modal floor; ≫ the ≤1 GiB
  transcoded working set even for CoalDustSamples).
- DuckDB: `SET memory_limit` (24 GB), `SET threads TO 8`, `SET temp_directory='<scratch>/duckdb_spill'`,
  `SET preserve_insertion_order=false`. The transcode writes to the ephemeral disk; DuckDB spills
  there; never to Modal's reserved `/tmp` if the EPID-spill convention applies (mirror the base
  worker's `SCRATCH_DIR`).
- Streaming write only: `con.execute(sql).to_arrow_reader(131072)` → `lance.write_dataset(...,
  schema=reader.schema, max_rows_per_file=1048576, max_bytes_per_file=90GiB,
  data_storage_version="2.1")`. Never materialize a 3 M-row file whole.

**Blast-radius containment (explicit mandate).** (1) Three workers → a domain failure is
contained. (2) Per-dataset `try/except` + `--only` → one dataset's failure commits nothing for
it but does not abort siblings already written (each `lance.write_dataset` is its own atomic
overwrite). (3) **Index rebuilds are isolated** from materialization in the `reindex` /
`reindex_only` entrypoint — a heavy external-sort index build can be re-run on an
already-committed dataset without re-materializing, and an index failure never rolls back good
data. (4) The `ops.msha_ingest_runs` ledger row (terminal status + grain result) makes every
run auditable and retry-safe.

---

## 7. Execution sequence (phased, gated — each phase fully verified before the next)

**Phase 0 — Profiling dry-run (no writes).** For each of the 12 archives: `_acquire` →
`DESCRIBE` → decimal/parse scan → lock `CASTS` + `INDEX_PLAN`; for the 2 ⚠️ archives, lock the
read recipe (§4). Output: the filled `DATASETS`/`CASTS`/`INDEX_PLAN` dicts per worker.

**Phase 1 — Worker D `materialize_msha_production.py` (lowest risk, proven union shape).**
```
modal run pipelines/ingest_msha/materialize_msha_production.py::run            # dry path then real
modal run pipelines/ingest_msha/materialize_msha_production.py::verify
```
Gate: `lance_rows == spine_rows` (≈ 3,372,386 = 2,714,840 + 657,546); BTREE/BITMAP committed.

**Phase 2 — Worker C `materialize_msha_enforcement.py` (5 datasets; OrdersIssued LAST).**
```
modal run …materialize_msha_enforcement.py::run --only msha_inspections
modal run …materialize_msha_enforcement.py::run --only msha_contested_violations
modal run …materialize_msha_enforcement.py::run --only msha_penalty_dockets
modal run …materialize_msha_enforcement.py::run --only msha_conferences
modal run …materialize_msha_enforcement.py::run --only msha_orders_issued   # ⚠️ custom recipe
modal run …materialize_msha_enforcement.py::verify
```
Gate per dataset: grain match + indices. Do OrdersIssued last so its custom parser is isolated.

**Phase 3 — Worker E `materialize_msha_samples.py` (5 datasets; CoalDust flagged).**
```
modal run …materialize_msha_samples.py::run --only msha_area_samples          # smallest first (8 K) — smoke test
modal run …materialize_msha_samples.py::run --only msha_quartz_samples
modal run …materialize_msha_samples.py::run --only msha_noise_samples
modal run …materialize_msha_samples.py::run --only msha_personal_health_samples
modal run …materialize_msha_samples.py::run --only msha_coal_dust_samples     # ⚠️ heaviest + bare-numeric
modal run …materialize_msha_samples.py::verify
```

**Phase 4 — Reindex sweep + final read-back.** `reindex_only` on any dataset whose `verify`
showed a missing index; then `verify` all 11 → confirm rows, key non-null fill, committed
indices. Independently re-probe with a `pylance` read (the §8 harness) to confirm 16 active
MSHA datasets and 20/20 archives represented.

**Phase 5 — Control plane (optional, mirrors base worker).** `modal deploy` each worker so the
Universal Dispatcher can resolve it; register Trigger v4 tasks if these feeds need cadence.
Materialization correctness does **not** depend on this — it is the durability/automation layer.

---

## 8. Verification & Definition of Done

**Read-back harness (read-only, Doppler-injected R2 creds, py3.12 via uv):**
```bash
doppler run -- uv run --python 3.12 --with 'pylance>=7' --with 'duckdb>=1.5,<2' \
  --with 'pyarrow>=17' python /tmp/msha_active_verify.py
```
For each new dataset assert: `count_rows()` == profiled rows; non-null fill on every indexed
key; `list_indices()` matches `INDEX_PLAN`.

**Done when:**
- [ ] 11 new datasets live under `s3://data-sink/active/`, each grain-verified (`lance_rows == spine_rows`).
- [ ] Every BTREE/BITMAP in each `INDEX_PLAN` committed (or explicitly logged as skipped + why).
- [ ] `ops.msha_ingest_runs` carries a terminal `success` row per worker run.
- [ ] Live re-probe: **16** active MSHA datasets; **20/20** landing archives represented.
- [ ] No `core.name_norm` / normalized-name / bridge column introduced (Directive-29 intact).

---

## 9. Git lifecycle (per operator workflow — own it end-to-end)

Branch → commit each worker with a clean message → push → open PR against `main` → self-verify
(the §8 read-back) → `gh pr merge --squash --delete-branch` → `git pull` in the operator's main
checkout → `git log -1 --oneline`. Three workers may ship as three PRs (clean blast-radius) or
one; prefer one PR per worker so a parser issue in samples never blocks the production/enforcement
landings. Update `MSHA_LANCE_STATE_DIAGNOSTIC.md` (5 → 16 datasets) and the `factory-state`
catalog in the same PR set.
```
