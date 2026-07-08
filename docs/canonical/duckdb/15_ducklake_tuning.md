# DuckLake Tuning & Sorted Tables — going fast on object storage (incl. R2)

> Canonical upstream reference. Folded from the committed talk-transcript corpus (docs/youtube-transcripts/, docs/batches/) and verified against live upstream docs where they exist (July 2026). Talk-reported claims are attributed inline; upstream-verified facts cite the doc URL.
>
> Primary sources:
> - docs/youtube-transcripts/clean/2026-04-28_the-ducklake-lakehouse-from-getting-started-to-going-fast.clean.md — "The DuckLake Lakehouse: From Getting Started to Going Fast", MotherDuck live webinar, 2026-04-28, speaker Alex Monahan (MotherDuck DevRel; co-author, *DuckLake: The Definitive Guide*) with host Gerald
> - docs/youtube-transcripts/clean/2026-07-07_a-deep-dive-into-ducklakes-sorted-tables-feature.clean.md — "A Deep Dive into DuckLake's Sorted Tables Feature", YouTube (sponsored by MotherDuck), 2026-07-07
> - docs/batches/2026-05-17-duckdb-quack-as-ducklake-catalog.md — Mike Ritchie / Definite, "DuckDB Quack as DuckLake catalog", 2026-05-17 (upd. 2026-06-08) — inlining-accretion example
> - https://ducklake.select/docs/stable/duckdb/advanced_features/sorted_tables — SET SORTED BY / RESET SORTED BY / sort_on_insert / three-stage sort semantics
> - https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining — inlining row limit, DATA_INLINING_ROW_LIMIT, ducklake_flush_inlined_data
> - https://ducklake.select/docs/stable/duckdb/usage/configuration — parquet_compression / parquet_version / parquet_row_group_size / per_thread_output / target_file_size / ducklake_retry_wait_ms
> - https://ducklake.select/docs/stable/duckdb/maintenance/checkpoint — CHECKPOINT compaction bundle
> - https://ducklake.select/docs/stable/duckdb/maintenance/cleanup_of_files — ducklake_cleanup_old_files
> - https://duckdb.org/docs/stable/sql/statements/copy — Parquet COPY params (COMPRESSION, PARQUET_VERSION, ROW_GROUP_SIZE(_BYTES), PER_THREAD_OUTPUT)
> - https://duckdb.org/docs/stable/data/parquet/tips — per-thread output + row-group sizing performance guidance

Scope: the practical levers for making a DuckLake lakehouse fast on object storage — Parquet write settings, sorted (clustered) tables and predicate pushdown, data-inlining thresholds, compaction/cleanup, and R2/object-storage-specific tuning — with every knob's exact name and syntax verified against upstream.

Cross-links: [14_ducklake_lakehouse.md](14_ducklake_lakehouse.md) (architecture), [16_profiling_and_pitfalls.md](16_profiling_and_pitfalls.md) (confirming pushdown actually fires), [../lance/05_scalar_indices.md](../lance/05_scalar_indices.md) and [../lance/09_scanning_filtering.md](../lance/09_scanning_filtering.md) (the Lance analogue).

---

## 1. The always-set write knobs

The webinar frames three settings you "will probably always want to set" out of the gate, plus co-locating compute and storage (MotherDuck webinar, 2026-04-28). Each is set through the DuckLake option mechanism, `CALL <ducklake_name>.set_option(...)`, where `<ducklake_name>` is the name you gave the attached DuckLake (e.g. `lake`, `my_ducklake`).

| Knob | Upstream option name | Default | Set it to | Trade-off |
|---|---|---|---|---|
| Parquet format version | `parquet_version` | `1` (i.e. V1) | `2` | Better compression for common patterns; only risk is reader ecosystem compatibility with Parquet V2 |
| Compression codec | `parquet_compression` | `snappy` | `zstd` | Best-in-class ratio; only risk is reader compatibility |
| Row-group size | `parquet_row_group_size` | `122880` rows | larger, in the cloud | Bigger reads suit object-storage latency; keep the default on a laptop |
| Retry wait | `ducklake_retry_wait_ms` | `100` ms | lower, for bulk ingest | Faster conflict retries against a fast catalog |

Verified names/defaults: `parquet_version` (default `1`), `parquet_compression` (default `snappy`), `parquet_row_group_size` (default `122880`), `per_thread_output` (default `false`), `target_file_size` (default `512MB`), `ducklake_retry_wait_ms` (default `100`), `ducklake_max_retry_count` (default `10`), `ducklake_retry_backoff` (default `1.5`) — all from https://ducklake.select/docs/stable/duckdb/usage/configuration.

```sql
-- <lake> is the attached DuckLake's name
CALL lake.set_option('parquet_version', '2');
CALL lake.set_option('parquet_compression', 'zstd');
```

### 1.1 Parquet V2

> **TALK-REPORTED** (MotherDuck webinar, 2026-04-28): "you want to use Parquet version two … has some improved compression methods … pretty close to a free lunch … The only downside here is ecosystem compatibility." **UPSTREAM-VERIFIED**: DuckDB's Parquet writer accepts `PARQUET_VERSION` with values `V1`/`V2`, default `V1` (https://duckdb.org/docs/stable/sql/statements/copy). DuckLake exposes this as the `parquet_version` option, default `1` (configuration page above).

### 1.2 ZSTD compression (default is Snappy)

> **TALK-REPORTED**: "Zstandard is considered really the best-in-class compression for Parquet. And by default we use Snappy … Zstandard is going to be your best bet. Again, only downside is compatibility." **UPSTREAM-VERIFIED**: DuckDB Parquet `COMPRESSION` accepts `uncompressed, snappy, gzip, zstd, brotli, lz4, lz4_raw`, default `snappy` (copy statement). DuckLake `parquet_compression` default `snappy`.

### 1.3 Row-group sizing (~8–16 MB per column chunk)

> **TALK-REPORTED**: "there's a paper by the TU Munich … it's somewhere between 8 MB and 16 MB that you want to read in one chunk. And the way that DuckDB tends to read is a column-row-group chunk at a time … take your … how many columns are in your table, and then split this up so you get about 8 MB in a given column. So in this case, 80 MB, that's if I had like a 10-column dataset." (Technical University of Munich reference and the 8–16 MB target: as stated in the talk; the numeric target is not restated in the DuckDB docs, though the underlying advice — bigger row groups for object storage — is.)

Two levers exist; know which one you are setting:

- **`parquet_row_group_size`** (DuckLake option) / **`ROW_GROUP_SIZE`** (DuckDB COPY) — sized in **rows**. DuckLake default `122880`; DuckDB COPY minimum is the vector size `2048`, default `122880` (https://duckdb.org/docs/stable/data/parquet/tips, https://duckdb.org/docs/stable/sql/statements/copy).
- **`ROW_GROUP_SIZE_BYTES`** (DuckDB COPY) — sized in **bytes**, default `row_group_size * 1024`, accepts human-readable values like `'2MB'` (copy statement). This is the direct expression of the "~8 MB per column chunk" heuristic.

```sql
-- DuckDB COPY, byte-sized row groups (the object-storage tuning target)
COPY tbl TO 'out.parquet' (FORMAT parquet, ROW_GROUP_SIZE_BYTES '8MB');
```

### 1.4 Co-locate compute and storage

> **TALK-REPORTED**: "you want to keep your compute near storage … put it in the same region as your storage bucket. Same cloud, same region." Load-bearing for out-of-core scans: most queries filter/aggregate and you want that to happen next to the bytes, not after a cross-region download.

---

## 2. Ingest tuning: per-thread output + retry wait

For **bulk** loads (which skip inlining and go straight to Parquet):

### 2.1 `per_thread_output`

> **TALK-REPORTED**: "each DuckDB thread is going to write its own Parquet file, which allows us to blast data at object storage a lot faster … There's a trade-off here. You're going to have more Parquet files … you might want to run your compaction a little bit more often." **UPSTREAM-VERIFIED**: DuckLake option `per_thread_output`, default `false` (configuration page). DuckDB COPY `PER_THREAD_OUTPUT` "generates one file per thread, rather than one file in total", default `false` (copy statement); the Parquet tips page notes "writing one file per thread can significantly improve performance."

```sql
CALL lake.set_option('per_thread_output', true);
```

### 2.2 Lower the optimistic-concurrency retry wait

> **TALK-REPORTED**: "DuckDB handles transactionality, it does optimistic concurrency control like all the other lakehouses do … typically it waits 100 milliseconds before trying that retry … for a transactional database, that's a long time. So you can shrink that number way down and insert data more quickly." **UPSTREAM-VERIFIED**: `ducklake_retry_wait_ms` default `100`, alongside `ducklake_max_retry_count` (default `10`) and `ducklake_retry_backoff` (default `1.5`) (configuration page). These are `SET`-scope extension settings.

```sql
SET ducklake_retry_wait_ms = 10;
```

Note on the "100": the transcript's "100" is the retry **wait** — "typically it waits 100 milliseconds before trying that retry" — which upstream confirms exactly as `ducklake_retry_wait_ms` default `100` (no discrepancy). The transcript does **not** state a default max retry *count*; `ducklake_max_retry_count` default `10` and `ducklake_retry_backoff` default `1.5` are upstream-only facts from the configuration page, not spoken in the talk.

### 2.3 Worker/thread count

> **TALK-REPORTED**: CPU-bound → threads = hyperthread count (e.g. 16 cores / 32 threads → 32). Network-bound (common in a lakehouse) → "take that thread count and multiply it by two to five." As stated in the talk; a workload-dependent heuristic, not an upstream-fixed value.

---

## 3. Sorted (clustered) tables — the read-path optimization

The deep-dive talk (2026-07-07) is the canonical narrative here. The mechanism: DuckLake physically clusters written data by the sort key so that **Parquet footer min/max statistics** and **row groups** let the planner skip whole files and whole row groups on a selective predicate.

> **TALK-REPORTED** (sorted-tables deep dive, 2026-07-07): "Parquet files have min/max statistics in their footers, and this is what a lakehouse uses to decide what it is it needs … if you could have fewer files where certain data that should be close to each other is … you can actually skip some of these other files." And within a file: "the sorted data in that Parquet file is going to be sorted in row groups as well … we can skip that row group."

### 3.1 Syntax — verified

```sql
-- Set a sort order (clustering). Multiple keys, ASC/DESC, NULLS FIRST/LAST all supported.
ALTER TABLE events SET SORTED BY (user_id ASC, ts ASC);

-- Reset / clear the sort order
ALTER TABLE events RESET SORTED BY;
```

**UPSTREAM-VERIFIED** (https://ducklake.select/docs/stable/duckdb/advanced_features/sorted_tables): `ALTER TABLE … SET SORTED BY (col …)`; direction `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`; arbitrary expressions and DuckLake macros are permitted as sort keys (e.g. a bucketing expression). Reset is `ALTER TABLE … RESET SORTED BY`.

> Note on syntax: the deep-dive talk demonstrates reset as a **bare** `ALTER TABLE events SET SORTED BY` (no columns). Upstream documents the reset as `ALTER TABLE … RESET SORTED BY`. Use the upstream `RESET SORTED BY` form as canonical.

Set-at-create-time is **not** yet available in DuckLake: "Today it is a separate command that you run after the table is already created … There is syntax support in DuckDB for putting that into the create-table command, à la Spark … So that's on the roadmap." (TALK-REPORTED, 2026-04-28 webinar — roadmap intention, not shipped.) `SET SORTED BY` is idempotent/deduplicated — re-running the identical statement is a no-op, so it is safe to run unconditionally in a dbt post-hook (TALK-REPORTED, same webinar).

### 3.2 Sort applies at three stages — and insert-time sort can be disabled

**UPSTREAM-VERIFIED** (sorted_tables page): "data is physically sorted by the specified columns whenever it is written out as Parquet — during `INSERT`, file compaction and inlined data flushing." The three stages:

1. **INSERT** (gated by the `sort_on_insert` option)
2. **Inlined-data flush** (`ducklake_flush_inlined_data`)
3. **Compaction** (`ducklake_merge_adjacent_files`, via `CHECKPOINT`)

Insert-time sort carries per-write overhead; you can disable it while keeping flush/compaction sort:

```sql
-- Disable insert-time sorting for this table (flush + compaction still sort)
CALL lake.set_option('sort_on_insert', false, table_name => 'events');
```

> **TALK-REPORTED** (deep dive): "there is overhead to sorting data when it's being inserted … if you're sending a single event to your lakehouse … it might make more sense to turn off the sort on the insert, so that it just goes in unsorted, and then you can worry about … flushing it to Parquet when it will get sorted." **UPSTREAM-VERIFIED**: `set_option('sort_on_insert', false, …)` (sorted_tables page).

### 3.3 Retroactivity — the load-bearing nuance

The two sources describe two different facets; both are correct and must be stated precisely:

- **Not retroactive to already-written data at set time.** TALK-REPORTED (deep dive): "sort order is not retroactive. So if you start putting a bunch of data unsorted into your DuckLake table and then you add a sort order, it's only going to affect all the data after that." Rows already inlined or already in Parquet stay in their existing physical order until they are next written out.
- **The *current* sort order wins at the next flush/compaction.** UPSTREAM-VERIFIED (sorted_tables page): "the **current** sort order is applied at the time of compaction or flush — not the sort order that was active when the source data was written." So a flush/compaction pass re-sorts the data it touches under whatever sort order is set *now*.

Net: setting a sort order does not rewrite history on its own, but the next flush or `CHECKPOINT` over that data will re-cluster it under the current order. To force existing data into the new order, flush and/or compact it.

### 3.4 The ~10 row-groups-per-file → ~10x heuristic

> **TALK-REPORTED** (2026-04-28 webinar): "I tend to aim for a rule of thumb of about 10 row groups per file, plus or minus. So that means that if you sort correctly, you get a 10x boost … That works out pretty well with that like 80 MB example … That means each file is close to a gigabyte … somewhere around that gigabyte to half-a-gigabyte range for each file." As stated in the talk; a rule-of-thumb, not an upstream-fixed figure. Note DuckLake's `target_file_size` default is `512MB` (configuration page), consistent with the half-gigabyte end of that range.

### 3.5 Inspecting the sort + confirming pushdown

```sql
-- The sort expression recorded per table (metadata schema)
SELECT * FROM metadata.ducklake_sort_expression;

-- Inspect Parquet footer stats (min/max per column, row-group boundaries)
SELECT * FROM parquet_metadata('data/.../file.parquet');
```

**UPSTREAM-VERIFIED**: `ducklake_sort_expression` is a documented metadata table (spec tables index). `parquet_metadata()` is a DuckDB table function (not DuckLake-specific). Confirming that pushdown *actually fires* (files/row-groups skipped) belongs to [16_profiling_and_pitfalls.md](16_profiling_and_pitfalls.md).

---

## 4. Data inlining thresholds and the accretion footgun

Inlining batches small inserts as rows inside the catalog database instead of writing a tiny Parquet per insert; reads still see them transactionally.

**UPSTREAM-VERIFIED** (https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining):

| Setting | Scope | Default |
|---|---|---|
| `ducklake_default_data_inlining_row_limit` | global DuckDB setting | `10` |
| `DATA_INLINING_ROW_LIMIT` | `ATTACH` parameter | `10` |

Inlining is enabled by default at a row limit of 10 — an insert of ≤10 rows is inlined (kept in the catalog, no Parquet written); >10 rows writes Parquet directly.

```sql
-- Raise the per-connection inlining threshold at attach time (default is 10)
ATTACH 'ducklake:inlining.duckdb' AS my_ducklake (DATA_INLINING_ROW_LIMIT 1000);

-- Flush accumulated inlined rows into Parquet
-- returns one row per flushed table: schema_name, table_name, rows_flushed
CALL ducklake_flush_inlined_data('my_ducklake');
```

**UPSTREAM-VERIFIED**: the flush function is `ducklake_flush_inlined_data` (data_inlining page). (The deep-dive transcript calls it "DuckLake flush inline"/`ducklake_flush_inline` colloquially; the canonical name is `ducklake_flush_inlined_data`.)

> **Footgun — inlined rows accrete in the catalog if never flushed.** Inlined data lives *in* the catalog database, not object storage. Turn the limit up, run a high-frequency write workload, and the "just metadata" catalog is now holding your most recent data. **From the corpus** (Definite / Mike Ritchie, 2026-05-17 — docs/batches/2026-05-17-duckdb-quack-as-ducklake-catalog.md): a single Shopify orders table showed "an 835,000-row gap between the snapshot row count and the Parquet-backed row count. That gap was inlined data: over three thousand `ducklake_inlined_data_*` tables of rows that had not been flushed yet." Schedule flush/compaction, or that gap grows unbounded.

> **Relevance to core-x:** the core-x plane's system of record is LanceDB on R2, not DuckLake. If DuckLake is used as a staging/compute surface upstream of a DuckDB→Arrow→Lance write, inlining trades object-store round-trips for catalog-resident rows — but any inlined rows are invisible on object storage until flushed, so a Lance materialization step must `ducklake_flush_inlined_data` (or `CHECKPOINT`) before reading the DuckLake as a complete source. Unflushed inline data is the DuckLake-side analogue of an un-checkpointed WAL.

---

## 5. Compaction and cleanup

**UPSTREAM-VERIFIED** (https://ducklake.select/docs/stable/duckdb/maintenance/checkpoint): `CHECKPOINT` runs, in order, `ducklake_flush_inlined_data`, `ducklake_expire_snapshots`, `ducklake_merge_adjacent_files`, `ducklake_rewrite_data_files`, `ducklake_cleanup_old_files`, `ducklake_delete_orphaned_files`.

```sql
CHECKPOINT;                          -- run the whole maintenance bundle
```

Individual pieces:

- **`ducklake_merge_adjacent_files`** — compaction. Merges small adjacent Parquets. Only merges files sharing the same schema version; schema-altering DDL (`ADD/DROP/RENAME/ALTER COLUMN`) starts a new compaction group. Params include `min_file_size`/`max_file_size` (defaults to `target_file_size`). (UPSTREAM-VERIFIED, merge_adjacent_files page.)
- **`ducklake_cleanup_old_files`** — deletes files orphaned by compaction/expiry. Files are not deleted immediately (active queries may still scan them); they land in `ducklake_files_scheduled_for_deletion` first, then this function removes them. (UPSTREAM-VERIFIED, cleanup_of_files page.)

> **TALK-REPORTED** on the read/write flow: newly written Parquet "sits there … nobody knows about it because it's not in the catalog … then we do a commit into the catalog after the fact" — so any engine (Spark, Daft, Polars, Pandas) can write Parquet and register it with the catalog; DuckLake reads whatever the catalog points at (2026-04-28 webinar).

---

## 6. R2 / object-storage specifics

> **TALK-REPORTED** (2026-04-28 webinar, on bring-your-own-bucket on R2):
> - **No egress fees.** "with R2, that's not as much of a cost concern, because they don't charge egress fees. They have a different billing model."
> - **Slower but much cheaper.** "R2 is a little slower, but it's a lot cheaper … you might have to really tune your workload more to get it to perform well on R2."
> - **Tune for fewer, bigger files.** "That will be most acute based on how many files you have. The smaller your files, the more often you pay that latency penalty. The bigger your files, the bigger chunks, the less often you'll pay that penalty."
> - **Compute dominates cost.** "Most of the cost of a data lake of any kind comes on the compute. We tend to see it's 80-90% of the cost is compute … benchmark the whole system cost, not just your storage cost."
> - **SSD-caching extension.** DuckDB caches fetched Parquet pieces in memory during a query; "some extensions in the ecosystem that extend this to your SSD as well. One of them by our own Dr. Peter Boncz here at MotherDuck." As stated in the talk; a community/vendor extension, not a core setting.

These are speaker claims (a live webinar), not upstream-doc facts — treat the cost percentages and the R2 latency characterization as informed vendor guidance, not benchmarked constants.

> **Relevance to core-x:** R2 latency rewards **fewer, bigger files + `SORTED BY` clustering** — exactly the pattern that lets a selective predicate skip whole files and row groups. This is the DuckLake analogue of hard `BTREE`-on-resolution-key indexing in Lance: both turn a high-cardinality point/range lookup from a full scan into a metadata-guided skip. For structured queries over lakehouse or Lance data on R2, cluster (DuckLake `SET SORTED BY` / Lance `BTREE` scalar index) on the resolution key you filter by, and size files big enough to amortize R2's per-request latency. See [../lance/05_scalar_indices.md](../lance/05_scalar_indices.md) and [../lance/09_scanning_filtering.md](../lance/09_scanning_filtering.md); confirm the skip actually fires per [16_profiling_and_pitfalls.md](16_profiling_and_pitfalls.md).

---

## 7. Quick reference — verified option names

| Purpose | Name | Kind | Default | Source |
|---|---|---|---|---|
| Parquet version | `parquet_version` | DuckLake `set_option` | `1` | configuration |
| Compression | `parquet_compression` | DuckLake `set_option` | `snappy` | configuration |
| Row-group size (rows) | `parquet_row_group_size` | DuckLake `set_option` | `122880` | configuration |
| Per-thread output | `per_thread_output` | DuckLake `set_option` | `false` | configuration |
| Target file size | `target_file_size` | DuckLake `set_option` | `512MB` | configuration |
| Insert-time sort | `sort_on_insert` | DuckLake `set_option` (per-table) | `true` | sorted_tables |
| Retry wait | `ducklake_retry_wait_ms` | `SET` | `100` | configuration |
| Max retries | `ducklake_max_retry_count` | `SET` | `10` | configuration |
| Retry backoff | `ducklake_retry_backoff` | `SET` | `1.5` | configuration |
| Inlining limit (global) | `ducklake_default_data_inlining_row_limit` | DuckDB setting | `10` | data_inlining |
| Inlining limit (attach) | `DATA_INLINING_ROW_LIMIT` | `ATTACH` param | `10` | data_inlining |
| COPY compression | `COMPRESSION` | DuckDB COPY | `snappy` | copy |
| COPY parquet version | `PARQUET_VERSION` | DuckDB COPY | `V1` | copy |
| COPY row-group bytes | `ROW_GROUP_SIZE_BYTES` | DuckDB COPY | `row_group_size * 1024` | copy |
| COPY row-group rows | `ROW_GROUP_SIZE` | DuckDB COPY | `122880` | copy/tips |
| COPY per-thread | `PER_THREAD_OUTPUT` | DuckDB COPY | `false` | copy |

Functions: `ducklake_flush_inlined_data`, `ducklake_merge_adjacent_files`, `ducklake_cleanup_old_files`, `ducklake_rewrite_data_files`, `ducklake_expire_snapshots`, `ducklake_delete_orphaned_files`, `CHECKPOINT` (bundle). Metadata: `metadata.ducklake_sort_expression`.
