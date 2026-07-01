# USAspending FPDS Canonical Transaction Table — Build Plan

Plan of record for **building `usaspending_fpds_canonical_txn`** — a single typed-v2 Lance
system-of-record that reconciles the USAspending FPDS feeds (BULK pg-dump search table, the FRESH
daily API feed, the MONTHLY bulk-download CSV — physically `usaspending_archive_full_fpds` + its
`usaspending_archive_delta_fpds` deletion ledger) into **one PK-grained canonical transaction table**
under `s3://data-sink/active/`. The merge is a **two-tier per-key argmax reconciliation** (Tier 1
`BULK⊕MONTHLY`, Tier 2 `⊕FRESH`); MONTHLY competes on every shared key so its corrections + 12
monthly-unique enrichment cols land (§3).

Derived verbatim from the defect-resolved, column-validated design spec
(`/tmp/fpds_canonical_build/design_spec.md`). Every column-presence and type claim traces to
`/tmp/fpds_canonical_build/schemas.json` (BULK 378 / FRESH 297 / archive_full 300 / archive_delta
302); every fleet/ledger rule traces to `ledger_arch_ref.md` and `scaffold_ref.md`. Numbers are
measured against those artifacts, not inferred.

- **Pipeline:** `usaspending_fpds_canonical_txn`
- **Worker:** `pipelines/usaspending/usaspending_fpds_canonical.py` (+ co-located `ops_usaspending_fpds_canonical_runs.sql`)
- **Output URI:** `s3://data-sink/active/usaspending_fpds_canonical_txn/`
- **Sample URI:** `s3://data-sink/active/_sample/usaspending_fpds_canonical_txn_sample/`
- **Shape:** LOCAL-CLI (doppler+uv) mirroring `usaspending_archive.py` (local Lance write → boto3
  uniform-part publish) + `materialize_contracts_map.py` (R2-scan → DuckDB merge → typed Lance).
- **Storage:** `data_storage_version="2.1"`, `max_rows_per_file=1048576`, overwrite read-model rebuild.

---

## 1. Objective + locked decision

**Objective.** Collapse the four FPDS feeds into one canonical, PK-unique, typed transaction table
keyed on `contract_transaction_unique_key`, carrying ~78 columns: the volatile-core FPDS fields the
live serving materializers read, plus the high-signal BULK-only enrichment, plus provenance. The
canonical resolves per-transaction precedence deterministically, applies the deletion ledger as a
tombstone, and ships a stable Arrow schema indexed on every load-bearing resolution key.

**Locked decisions (do not relitigate):**

1. **Typed v2, not all-VARCHAR.** The canonical ships `action_date`/PoP dates as `date32`,
   `federal_action_obligation`/`base_and_all_options_value`/`current_total_value_of_award`/
   `award_amount`/`total_funding_amount` as `double`, `action_date_fiscal_year` and the BULK id
   columns as `int64`, `last_modified_date`/`built_at` as **naive `timestamp[us]` (NO tz)`**.
   Casts mirror BULK's `_duck_type` contract (`DATE`→Arrow date32, `DOUBLE`, `BIGINT`); the FRESH
   /archive all-VARCHAR sides are typed in via `TRY_CAST(s(x) AS T)`.
2. **Consumers stay decoupled — this build does NOT re-point them.** The existing serving
   materializers (`materialize_contracts_map.py` L165-171, `govcon_prime_trajectories.py` L233-234)
   keep reading the all-VARCHAR FRESH SoR via `try_cast(... AS DATE/DOUBLE)`. `try_cast(typed AS
   sametype)` is a verified no-op, so a future re-point will not break on the type shift. **The one
   re-point footgun (documented now, not at re-point time):** VARCHAR-only idioms like
   `nullif(trim(col))` applied to a now-typed column fail — a re-point must drop string-massaging on
   the typed columns.
3. **Canonical vocabulary = FRESH / bulk_download names.** FRESH, archive_full, archive_delta carry
   the canonical names verbatim for every merge-relevant column (probe-confirmed). BULK uses rpt.*
   names and is renamed *into* the canonical vocabulary via the §3 crosswalk. Enrichment columns
   (BULK-only) keep their rpt.* names verbatim.
4. **Overwrite read-model rebuild.** The canonical is derived/rebuildable → overwrite each run
   (serving convention), not append-only (FRESH/archive convention).
5. **Scope = the dataset.** This plan produces one Lance dataset in R2 (data + indices) and the ops
   ledger, and stops. The deferred column tail and the flag reconciliation are §8.

---

## 2. The 5 verified ground-truth corrections vs the original §e transcript

The original §e transcript carried five claims that the column-validated probe **overturned**. Each
correction is load-bearing for the merge and is locked below.

| # | Original §e claim | VERIFIED ground truth | Consequence |
|---|---|---|---|
| 1 | "~80 BULK enrichment columns" | BULK is **378 cols**; the enrichment universe beyond the ~297 FPDS-overlap is **277**, not 80. The canonical carries a **curated 27-column pg-only subset PLUS 12 MONTHLY-unique cols** (39 enrich total: the 27 rpt.* pg-only + the 2 TAS/federal-account + 10 officer-comp name/amount cols pg lacks), NOT all 277; the rpt.*↔canonical crosswalk is encoded inline as paired projections (`govcon_prime_trajectories.py` L224-266 is the authoritative crosswalk). | Enrichment set is a deliberate curation; the officer-comp block that §8 formerly deferred is now LANDED via MONTHLY. The remaining ~240-col tail is recoverable in a v2 widening (§8). |
| 2 | `base_and_all_options_value` is numeric in BULK | In BULK it is typed **`string`** (schemas.json), unlike `federal_action_obligation`/`award_amount`/`total_funding_amount` which are native `double`. | The BULK projection MUST `TRY_CAST(s(base_and_all_options_value) AS DOUBLE)` — it cannot pass through. (`current_total_value_award` is likewise BULK `string`.) |
| 3 | `recipient_zip_4_code` exists in BULK | BULK has **no zip4** — only `recipient_location_zip5`. | Canonical `recipient_zip_4_code` = NULL on BULK-sourced rows; FRESH/archive supply it. BULK's `recipient_location_zip5` is ALSO carried as a (c) enrichment column so the 5-digit is never lost. PoP zip: BULK has only `pop_zip5`, mapped best-effort (documented lossy) into `primary_place_of_performance_zip_4`. |
| 4 | `correction_delete_ind` carries multiple delete codes | archive_delta `correction_delete_ind` takes only **`{'D': 656, NULL: 3,059,414}`** (total 3,060,070 rows / 302 cols). | The tombstone filter is exactly `correction_delete_ind = 'D'` (656 keys), fixed and exclusive; nothing else is a delete. |
| 5 | archive_full spans all fiscal years | MONTHLY (physical `usaspending_archive_full_fpds`) is **FY2026-only** (`action_date` 2025-10-01..2026-06-04), 2,975,677 rows / 300 cols; delta 3,060,070 rows / 302 cols (extra cols: `correction_delete_ind`, `agency_id`, `archive_kind`, `archive_snapshot_stamp`, `archive_source_file`). | MONTHLY is NOT backfill-only: it **competes for the volatile core on every shared key** (§3 Tier 1) — landing FY2026 corrections and its **12 monthly-unique enrichment cols** (TAS/federal-account funding + officer-comp) that pg leaves NULL. The sample window `--since 2025-10-01` puts MONTHLY fully in-window → complete correction+enrichment proof scope. |

---

## 3. Merge design summary (TWO-TIER logical reconciliation — CURRENT design of record)

> **Supersession note.** An earlier revision of this section described a FLAT three-disjoint-universe
> merge — a FRESH-only `b_wins` CASE ladder, `bulk_only`/`arch_survivors` anti-join survivor
> universes, an `archive`-tagged leg used only for archive-only backfill, and a `canonical_source`
> that "denotes which leg OWNS the PK." **That design no longer exists in the worker and its
> line-anchored details must not be reintroduced** — applying them would REGRESS the shipped code
> (they were exactly the defects PLAN_REVIEW P0-1 / P0-4 flagged). The design below is the current
> code of record (`usaspending_fpds_canonical.py` `_merge_tail_sql`).

The merge is a **two-tier per-key argmax reconciliation** producing **ONE physical artifact**
(`s3://data-sink/active/usaspending_fpds_canonical_txn/` — unchanged). The naive "UNION ALL of
109M+2M+3M then one global window dedup" forces a ~114M-row external sort — the exact OOM the fleet
rules forbid. The plan pays the per-key collapse cost only on the small side and **once over BULK**,
then reconciles the three ≤1-per-key collapses in a single window.

**MONTHLY = the monthly bulk-download CSV feed** (source #2 in the operator's mental model). Its
physical R2 upstream is still named `usaspending_archive_full_fpds` / `usaspending_archive_delta_fpds`;
**renaming those datasets is a tracked follow-up.** In-code the semantic name is now **MONTHLY** — the
`archive` src tag, ledger column, and docstrings were rolled to `monthly`; the URIs and the
`archive_r`/`archive_proj` register handles were deliberately kept (commented) so the physical rename
is a clean, separate change.

### Proven keys (the single most load-bearing fact)
- Transaction key (PK): FRESH/monthly `contract_transaction_unique_key` ≡ BULK
  `COALESCE(detached_award_proc_unique, transaction_unique_id)` — **govcon-verified byte-for-byte**
  (`govcon_prime_trajectories.py` L27-28, L219-232).
- Award key: FRESH/monthly `contract_award_unique_key` ≡ BULK `generated_unique_award_id`.
- Two shared macros, defined ONCE, used identically in every projection:
  - `s(x) := nullif(nullif(trim(x), ''), '-NONE-')` — VARCHAR sentinel-null.
  - `kbulk(detached, txnuid) := s(COALESCE(s(detached), s(txnuid)))` — the OUTER `s()` applies the
    SAME `''`+`'-NONE-'` whole-string strip as FRESH, so a literal `-NONE-` whole-string key maps to
    NULL on BOTH sides. Internal `-NONE-` tokens inside a real grammar key are preserved.

### The three per-key collapses (≤1 row per key each)
Each source is collapsed to latest-per-key with a deterministic tiebreaker BEFORE reconciliation:
- **`fresh_latest`** — FRESH deduped: `last_modified_date DESC NULLS LAST, (federal_action_obligation
  IS NULL) ASC, modification_number DESC, contract_award_unique_key DESC`.
- **`bulk_latest`** — ONE per-key collapse over FULL BULK (109M scanned once): `last_modified_date
  DESC NULLS LAST, (recipient_hash IS NULL) ASC, transaction_id DESC`. Enrichment-maximizing. It is
  BOTH a core competitor AND the sole pg-enrichment source — **no separate `bl_probe` 107M copy is
  materialized** (that duplicate was deleted; the collapse already carries the PARTITION key).
- **`monthly_latest`** — collapse over the FULL MONTHLY projection (NOT an anti-joined survivor set),
  so **monthly competes on every shared key** — THE fix. CORE dedup stays core-populatedness/mtime
  (`last_modified_date DESC NULLS LAST, (federal_action_obligation IS NULL) ASC,
  contract_award_unique_key DESC`).

### TIER 1 — `bulk_base = bulk_latest ⊕ monthly_latest` (LOGICAL CTE, NOT materialized)
Reconcile pg (BULK) with MONTHLY by per-key `argmax(last_modified_date)`; **equal-mtime tie →
MONTHLY wins over pg** (`source_rank` MONTHLY=2 < BULK=3). This is the semantic "reconciled base" the
enrichment fill draws from. **Decision: `bulk_base` is a documented `CREATE TEMP VIEW`, NOT a second
physical Lance dataset.** Rationale: `argmax` is associative, so the CORE values `bulk_base` would
emit are subsumed by the flat 3-way `core_winner` window below — materializing a second artifact would
duplicate ~107M rows to no purpose. The single artifact `usaspending_fpds_canonical_txn/` is unchanged.

### TIER 2 — `canonical core = bulk_base ⊕ fresh_latest` (executed as ONE flat 3-way window)
The volatile core is `argmax(last_modified_date)` over `{FRESH, MONTHLY, BULK}` with locked precedence
**FRESH(1) > MONTHLY(2) > BULK(3)** on mtime ties. Because `argmax` is associative, the explicit
two-tier order `(BULK⊕MONTHLY)⊕FRESH` is **byte-identical** to a single flat `row_number()` window over
the vertical union of the three tagged collapses (`core_union` → `core_winner`). The flat window is
the executed path. Tier-1 tie→MONTHLY and tier-2 tie→FRESH are both subsumed by the `source_rank`
total order, and after the three upstream collapses there is at most one row per source per key, so
`source_rank` alone disambiguates every cross-source mtime tie → **PK-uniqueness is structural**
(one `row_number()=1` survivor per key), not anti-join-disjointness-dependent.

**CORE byte-identity INVARIANT.** Emitted CORE is proven identical to the pre-two-tier build:
`monthly.last_modified_date ≥ pg` on **100% of 2,189,379 shared FY2026 keys** (46,197 strictly-newer,
2,143,182 equal, 0 older) on the `--since 2025-10-01` window. The strictly-newer monthly rows are
exactly the landed corrections (`canonical_source='monthly'`); every other shared key keeps its
pre-monthly core value.

### `canonical_source` — the per-key winner, derived ONCE
`canonical_source` is the winning core row's `src` tag ∈ **{fresh, bulk, monthly}**, derived exactly
once as `w.src AS canonical_source` in `resolved`. It is the **true per-key winner**, NOT a partition
literal and NOT "which leg owns the PK." (The three `*_proj` legs carry a typed-NULL `canonical_source`
placeholder so the schema-identity gate compares identically; `resolved` EXCLUDEs the placeholder and
re-derives from `w.src` — keeping the placeholder would collide and DuckDB would silently rename the
derived column.)

### Enrichment fill — pg-preferred `COALESCE(pg, monthly)` from the reconciled base
Enrichment is overwritten in `resolved` INDEPENDENT of the core winner, via two LEFT JOINs to
PK-unique collapses (no fan-out):
- **27 pg-only enrich cols** (`feed_expr` None): plain `b.<col>` from `bulk_latest`. No monthly source
  exists → no COALESCE.
- **12 MONTHLY-unique enrich cols** — `treasury_accounts_funding_this_award`,
  `federal_accounts_funding_this_award`, `highly_compensated_officer_1..5_name`, and
  `highly_compensated_officer_1..5_amount` (all VARCHAR; the officer `*_amount` cols are raw strings —
  NOT cast to DOUBLE). pg LACKS all 12, so the value is pg-preferred `COALESCE(b.<col>, m.<col>)`
  sourced from the reconciled base. Today `b` is a typed-NULL placeholder for these 12 so the COALESCE
  degenerates to `m.<col>`; the form is kept so a future pg schema add is picked up automatically.
- **`recipient_uei` is CORE** (argmax-resolved), NOT pulled into an enrichment COALESCE.
- The MONTHLY leg (`m`) is **`monthly_enrich_latest`** — a SEPARATE **enrichment-populatedness** dedup,
  NOT `monthly_latest`'s core dedup. It ranks enrichment-populated rows ABOVE enrich-NULL rows for the
  same key, then latest-mtime, then award-key surrogate. Required so a latest-but-enrich-NULL monthly
  row cannot surface and forfeit the gain (empirically Δ=0 vs the latest-mtime dedup on the
  `--since 2025-10-01` window, but locked as a cadence-robustness safeguard).

### Tombstone (R6-scoped) − reinstatement (R5) → `canonical_out`
Delete is **one coupled final-state op applied to `resolved`** (post fresh overlay, never to the base
alone). The delta scanner is filtered ONLY by `correction_delete_ind = 'D'` and **NEVER** receives
`--since`/`action_date`.
- **R6 SNAPSHOT-STAMP SCOPING** (locked pre-2nd-cycle requirement): `delete_keys` is scoped to the
  **latest `archive_snapshot_stamp`** present in the delta-'D' set. `archive_delta` is append-only and
  stamped per monthly snapshot; without scoping an OLD-month delete tombstones forever. The stamp col
  is added to `delta_scan_cols`; `delta_has_stamp` gates the scoping fragment (fall back to the whole
  'D' set only if the feed lacks the column).
- **R5 REINSTATEMENT GATE** (locked pre-2nd-cycle requirement): a 'D' tombstone is honored ONLY when
  the WINNING reconciled-winner mtime is **NOT strictly newer** than the delete's `last_modified_date`
  (`delta_lmt`). A strictly-newer non-'D' row REINSTATES the key — it must NOT be deleted. Implemented
  as `LEFT JOIN delete_keys d … WHERE d.k IS NULL OR resolved.last_modified_date > d.delta_lmt`. The
  earlier revision computed `delta_lmt` but never consumed it (dead scaffolding) and deleted all 656
  'D' keys unconditionally — the P0-4 defect. **Ground fact:** 92/656 'D' keys are live (non-D) in
  `monthly_full`; **39 are strictly-newer → those 39 survive** (the 39-key floor).

### Fail-closed PK gate
`count(*) == count(DISTINCT contract_transaction_unique_key)` on `canonical_out` **before publish** —
raise on any dup (structural: one survivor per key). Also re-checked in `verify()` on read-back.

---

## 4. Build / index / verify CLI + exact run commands

### Subcommands (mirror serving's `main()`; `build()` returns a JSON metrics dict)
```
python -m pipelines.usaspending.usaspending_fpds_canonical init_ops
python -m pipelines.usaspending.usaspending_fpds_canonical build  [--since YYYY-MM-DD] [--target-uri URI]
python -m pipelines.usaspending.usaspending_fpds_canonical index  [--target-uri URI]
python -m pipelines.usaspending.usaspending_fpds_canonical verify [--target-uri URI]
```
- **`build`** — merge + data write + boto3 publish. **NO index build** (blast-radius split: a failed
  index never corrupts the data write). FAIL-CLOSED PK-uniqueness gate before publish.
- **`index`** — opens the published dataset, builds the §4 BTREE/BITMAP set. Separate so the
  full-giant index runs on proper compute while the data write is verified independently; on Giants
  it uses the Volume-staged build (local copy → index → boto3 upload of only the new index files).
- **`verify`** — §7 assertions, read-back only (own scanner → DuckDB), returns JSON.
- **`init_ops`** — applies `ops_usaspending_fpds_canonical_runs.sql` via psycopg on
  `HQX_DB_URL_POOLED`; `_record_run` self-bootstraps the same DDL on first write.

`--since DATE` pushes `action_date >= DATE` into the **THREE DATA scanners ONLY**: BULK (date32
`action_date >= DATE`), FRESH + archive_full (lexical `action_date >= 'DATE'`, safe — FRESH/archive
`action_date` is uniformly ISO len-10, 0 nulls). It is **NEVER** applied to the archive_delta
scanner.

### Exact run commands

**On-box SAMPLE (end-to-end correctness pass — the 48GiB/3GB-free laptop):**
```
doppler run -- python -m pipelines.usaspending.usaspending_fpds_canonical init_ops
doppler run -- python -m pipelines.usaspending.usaspending_fpds_canonical \
    build  --since 2025-10-01 \
    --target-uri s3://data-sink/active/_sample/usaspending_fpds_canonical_txn_sample/
doppler run -- python -m pipelines.usaspending.usaspending_fpds_canonical \
    index  --target-uri s3://data-sink/active/_sample/usaspending_fpds_canonical_txn_sample/
doppler run -- python -m pipelines.usaspending.usaspending_fpds_canonical \
    verify --target-uri s3://data-sink/active/_sample/usaspending_fpds_canonical_txn_sample/
```
The sample window `--since 2025-10-01` is chosen deliberately: archive_full (FY2026-only) is fully
in-window → exercises the archive-only leg; FRESH extends to 2026-06-26 → exercises the FRESH-only
tail; BULK overlaps 2025-10-01..2026-04-23 → exercises BULK survivors + the precedence probe +
enrichment onto FRESH winners; the delta scanner is NOT `--since`-filtered so all 656 'D' keys load
exactly as on the full build → the tombstone anti-join is exercised. Sample `rows_out` is
FY2026-grain (low-millions) → fits the 3GB-free box.

**Full giant (proper compute — ≥96GiB box OR Modal httpfs-stream):**
```
FPDS_CANONICAL_DUCKDB_MEM=96GB \
doppler run -- python -m pipelines.usaspending.usaspending_fpds_canonical build      # threads=8, full BULK
doppler run -- python -m pipelines.usaspending.usaspending_fpds_canonical index      # Giants Volume-staged
doppler run -- python -m pipelines.usaspending.usaspending_fpds_canonical verify
```
`--target-uri` defaults to `CANONICAL_URI` (prod). The local-CLI shape runs identically on the ≥96GiB
box with `FPDS_CANONICAL_DUCKDB_MEM=96GB` + `threads=8`; the Modal alternative is the disciplined
httpfs-stream + bounded-batch pattern (`to_arrow_reader(batch_size=500_000)`, no `ephemeral_disk`).

---

## 5. Box-routing rationale (why NOT the laptop for the full giant)

| Run | Box | Why |
|---|---|---|
| SAMPLE | On-box (48GiB total / **3GB free**) | FY2026-grain `rows_out` is low-millions; `bulk_latest` over the `--since`-filtered BULK slice fits RAM+spill; full correctness pass (all 3 sources + tombstone) without touching prod. |
| FULL | Proper compute (**≥96GiB** box or Modal httpfs-stream) | Two reasons. (1) `bulk_latest` is full-width, one row per ~107M BULK keys — it is the dominant memory/spill object, NOT a small in-RAM table; it is a materialized TEMP TABLE backed by `temp_directory` spill. The 3GB-free box has nowhere to spill it. (2) The `contract_transaction_unique_key` BTREE sorts ~107M values; with `LANCE_BYPASS_SPILLING=true` the sort is in-RAM (32–64GiB), which the laptop cannot hold → the half-indexed-live-table failure. |

**Hard rule:** the on-box path writes ONLY to the SAMPLE URI, NEVER prod. Avoid the large
`ephemeral_disk` override on the full build — it forced the prior USAspending giant ingest onto
preemptible spot capacity that was killed and restarted; prefer the httpfs-stream pattern.

---

## 6. d.8 + fleet disciplines (non-negotiable)

- **`LANCE_BYPASS_SPILLING`** — module-top `os.environ.setdefault("LANCE_BYPASS_SPILLING","true")`
  BEFORE any `import lance`. Load-bearing: the ~107M-value PK BTREE sort OOMs the default DataFusion
  external-merge pool ("ExternalSorterMerge") mid-build, leaving a half-indexed live table.
- **Local-write → boto3 publish (Giants-safe).** `lance.write_dataset(reader, local_ds,
  mode="overwrite", data_storage_version="2.1", max_rows_per_file=1048576,
  max_bytes_per_file=90*1024**3)` to a LOCAL dir, then boto3 file-by-file upload (uniform multipart
  parts; s3transfer auto-retries individual parts). Prior-prefix wipe via S3/R2 `DeleteObjects` in
  batches of **≤1000 keys** (the API hard cap; correct at any count). **There is NO direct-R2 write
  code path anywhere in this pipeline** — a direct `write_dataset(..., storage_options=so)` of the
  107M table trips R2's "all non-trailing parts equal length" rule (`400 InvalidPart`). Do not copy
  serving's direct-to-R2 64k write-shape. (`max_rows_per_file=1048576` is valid ONLY on the boto3
  path; all 78 columns are flat scalars — zero list/struct/map — so serving's 64k hedge does not
  apply. Any direct-R2 fallback MUST drop to 250_000.)
- **No auto-retries** in pipeline logic. boto3 s3transfer already retries individual parts; a giant
  re-run is operator-initiated.
- **Overwrite idempotency.** `build` wipes the target prefix then publishes — re-running is safe. An
  optional `--force` guard around `_dataset_exists` is acceptable (overwrite is inherently
  idempotent).
- **Giants index staging.** The full-giant `index` uses the Volume-staged variant: R2 → local copy →
  build BTREE/BITMAP on the local FS → upload only the new files (`_indices/<uuid>/`, the new
  `_versions/<n>.manifest`, `_transactions/*.txn`) via boto3. Never wipe or re-upload data files.
  `create_scalar_index(..., replace=True)` guarded by `try/except TypeError`; columns filtered by
  `if col in present` so an absent column is skipped, never fatal.
- **DuckDB.** `PRAGMA threads=4` (sample) / 8 (full); `SET memory_limit` from
  `FPDS_CANONICAL_DUCKDB_MEM` (default 8GB sample / 96GB full); `SET
  temp_directory='/tmp/fpds_canonical_duckdb'`; `SET preserve_insertion_order=false`. BULK read once
  via `.to_reader()` (single pass); `.to_arrow_table()`/`.to_arrow_reader()` only (never
  `.fetch_arrow_table()`); no `use_lsm_write`; no bare DuckDB VARIANT on the Arrow wire.
- **`built_at` discipline.** ONE Python naive-UTC literal (`datetime.now(timezone.utc).replace
  (tzinfo=None)`) injected as `TIMESTAMP '<iso>'` into all three projections — NOT `now()`. `now()`
  is fixed per-transaction (three different sub-second stamps across three `CREATE TEMP TABLE`
  autocommits) AND returns `timestamp[us, tz=box-local]`, breaking the sample↔prod schema-identity
  guarantee.
- **`last_modified_date` discipline.** FRESH/archive parse via
  `TRY_CAST(replace(s(last_modified_date),'+00','') AS TIMESTAMP)` — **NO `strptime`**. `strptime`
  raises `InvalidInputException` (not NULL) on any non-matching value (fractional second, odd offset,
  date-only, trailing junk), and a single bad row aborts the entire 107M build; `TRY_CAST` wrapping
  does NOT catch the exception that fires inside `strptime`. `replace`+`TRY_CAST` yields NULL, never
  throws. BULK's `last_modified_date` is already naive `timestamp[us]` → pass through.
- **Programmatic schema enforcement.** Before any union, assert the three projections' `(name, type)`
  sequences are identical; `raise` on mismatch. Combined with `UNION ALL BY NAME`, a misordered or
  mistyped column is a hard build failure, not a silent transposition.

---

## 7. Verify gates (expected real numbers)

Computed independently on read-back (own scanner → DuckDB), mirroring govcon `verify()`. ALL
set-membership checks use `ANTI JOIN` (or `WHERE k IS NOT NULL` subqueries), **NEVER bare `NOT IN`**
(NULL-poison zeroes out the exact regression the check exists to catch).

| Assertion | Expected | Tolerance | Derivation |
|---|---|---|---|
| `bulk_self_dup` (one-time probe) | report | — | `count(*) - count(DISTINCT kbulk(...))` over full BULK. Pins the `rows_out` centerline; BULK self-uniqueness was never proven (only FRESH⊆BULK membership). |
| `rows_out` | **≈ 107,200,000** | ±0.5M | full-BULK distinct-key universe + FRESH-only tail (≈523K beyond BULK) − tombstones removed. FRESH/archive shared keys collapse onto BULK's universe. |
| `pk_unique` | **TRUE (0 dupes), exact** | exact | `count(*) == count(DISTINCT contract_transaction_unique_key)`. Disjoint by construction (§3.7). ALSO the FAIL-CLOSED gate inside `build` BEFORE publish — not only a read-back assertion. |
| `max(action_date)` | **2026-06-26** | exact | FRESH's frontier wins the latest dates (BULK maxes 2026-04-23). |
| `delta_d_keys` | **656** | exact | distinct delta-'D' keys (the full tombstone set, `--since`-independent). |
| `deletes_tombstoned` (keys actually removed) | ≤ 656, **POST R5** | — | in-universe 'D' keys NOT R5-reinstated (reconciled-winner mtime ≤ `delta_lmt`). Under R5 the **39 strictly-newer 'D' keys SURVIVE** (the 39-key floor) and are NOT counted here. Ledger column = keys ACTUALLY removed. |
| `deletes_reinstated` (R5, GATED behavior) | **39 present** on `--since 2025-10-01` | — | 'D' keys whose reconciled-winner mtime > `delta_lmt` — REINSTATED, must be PRESENT in `canonical_out`. `delta_lmt` is now CONSUMED (was dead scaffolding). Not delete-wins-always. |
| `fresh_only_tail` | scope-dependent | — | `fresh_latest ANTI JOIN bulk_latest` — the frontier nothing else has (full-build ≈711K; scales with `--since`). |
| `monthly_corrections_applied` (GATED > 0) | **> 0** | — | `canonical_out` keys with `canonical_source='monthly'` that BULK also holds — monthly WON the core (landed corrections). Zero ⇒ the fix regressed. |
| `canonical_source` domain (GATED) | ⊆ {fresh, bulk, monthly} | exact | no `archive` tag may appear; NULL or out-of-domain ⇒ fail. |
| `canonical_source` distribution | bulk ≫ fresh ≫ monthly | — | sanity: bulk dominant, fresh survivors, monthly = FY2026 corrections + monthly-only keys. |

**Ops ledger** (`ops.usaspending_fpds_canonical_runs`): records
`rows_in_bulk/fresh/archive_full`, `rows_out`, `dedup_collapsed`, `fresh_only_tail`,
`deletes_tombstoned` (keys actually removed, POST R5), **`monthly_corrections_applied`** (monthly
core-wins on keys BULK also holds), `max_action_date`, `columns`, `write_mode='overwrite'`,
`indices_built`, `status`, `error_message`, timestamps. Written by `_record_run` (psycopg,
`HQX_DB_URL_POOLED`) in a `finally:` on every terminal state; WARN-only on ledger failure ("audit
must not mask the build"). The `archive→monthly` rename is a guarded `ALTER … RENAME COLUMN IF EXISTS
archive_corrections_applied TO monthly_corrections_applied` + `ADD COLUMN IF NOT EXISTS` forward-fill
(idempotent, order-safe) so pre-existing ledger schemas pick up the column in place. The extra §7
diagnostics ride in the returned `verify` JSON.

---

## 8. Deferred scope

Explicitly out of scope for this build; recoverable in a v2 widening without a schema migration
(append columns, re-index — no rewrite of existing rows):

- **The 297/378 column tail.** BULK: the ~240 remaining rpt.* columns — the full `*_desc` paired
  description columns, `legal_entity_foreign_*`,
  `pop_*_population` demographics, `cfda_title`/`cfda_id`, `vendor_phone/fax`, raw-name variants, the
  bulk audit cols (`source_schema`, `source_table`, `usaspending_snapshot_date`, `ingested_at`,
  `etl_update_date`, `create_date`). FRESH: the ~210 remaining bulk_download columns — full
  code/description pairs, COVID/IIJA supplemental amounts, `usaspending_permalink`,
  `object_classes_funding_this_award`. (**Now LANDED**, no longer deferred: the MONTHLY-unique
  `treasury_accounts_funding_this_award` / `federal_accounts_funding_this_award` +
  `highly_compensated_officer_1..5_name/amount` block, sourced from MONTHLY via pg-preferred COALESCE — §3.)
- **The bool↔Y/N flag reconciliation.** The ~80 socioeconomic flags (`woman_owned_business`,
  `service_disabled_veteran_o`, …) are typed **`bool` in BULK** but **Y/N VARCHAR in FRESH**.
  Carrying them now would impose a type-reconciliation tax; they are deferred as a separate, explicit
  decision (pick a canonical representation, normalize both sides) rather than smuggled in.

---

## 9. Risks + rollback

| Risk | Mitigation |
|---|---|
| Bad full-giant build corrupts prod SoR | **Overwrite is the rollback boundary.** Each `build` wipes-then-publishes; a re-run with the prior inputs reproduces the prior table bit-for-bit (deterministic dedup). The prior version also survives in Lance `_versions/` until compaction. |
| PK duplication slips to publish | **FAIL-CLOSED PK gate inside `build`** before publish: `count(*) == count(DISTINCT contract_transaction_unique_key)`; mismatch → `raise`, abort publish, ledger records the error. Disjointness is provable (every key + every anti-join key passes the SAME `s()`/`kbulk()` normalization). `verify` re-checks on read-back. |
| Untombstoned table from a `--since` run | The delta scanner is structurally `--since`-immune (filter is the fixed `correction_delete_ind = 'D'`, never `action_date`); all 656 'D' keys always load. |
| `strptime` hard-abort on one malformed mtime | `replace`+`TRY_CAST` (no `strptime`) → NULL, never throws (§6). |
| Sample-passes-but-giant-fails | **Sample before giant, always.** The sample exercises all 3 sources + the tombstone anti-join + the precedence probe + enrichment routing on FY2026 grain; only after a green sample does the giant run. Sample and prod emit byte-identical Arrow schemas (naive `timestamp[us]`, no box-local tz drift) → a green sample schema IS the prod schema. |
| Index build OOM leaves half-indexed live table | `index` is split from `build` (blast-radius); `LANCE_BYPASS_SPILLING=true` + in-RAM sort on proper compute; Giants Volume-staged build never touches data files. |

---

### Source-of-truth index (all absolute, under the worktree root)
- Design: `/tmp/fpds_canonical_build/design_spec.md` · Schemas: `/tmp/fpds_canonical_build/schemas.json`
- Idioms: `/tmp/fpds_canonical_build/scaffold_ref.md` · Ledger/fleet: `/tmp/fpds_canonical_build/ledger_arch_ref.md`
- Outer skeleton: `pipelines/usaspending/usaspending_archive.py`
- Merge body + `_record_run` bootstrap: `pipelines/serving/materialize_contracts_map.py`
- BULK⋈FRESH crosswalk + dedup idiom: `pipelines/usaspending/govcon_prime_trajectories.py`
- Worker (to author): `pipelines/usaspending/usaspending_fpds_canonical.py` (+ `ops_usaspending_fpds_canonical_runs.sql`)
