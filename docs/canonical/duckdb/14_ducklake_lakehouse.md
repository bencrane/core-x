# DuckLake — The Open Lakehouse Format (catalog + metadata as a SQL database)

> Canonical upstream reference. Folded from the committed talk-transcript corpus (docs/youtube-transcripts/, docs/batches/) and verified against live upstream docs where they exist (July 2026). Talk-reported claims are attributed inline; upstream-verified facts cite the doc URL.
>
> Primary sources:
> - docs/youtube-transcripts/clean/2026-04-28_the-ducklake-lakehouse-from-getting-started-to-going-fast.clean.md — MotherDuck live webinar "The DuckLake Lakehouse: From Getting Started to Going Fast", 2026-04-28, Alex Monahan (MotherDuck DevRel) + Gerald (host)
> - docs/youtube-transcripts/clean/2026-07-11_build-a-local-data-lakehouse-with-duckdb-and-ducklake.clean.md — "DataGuy" YouTube review-request video, 2026-07-11
> - docs/youtube-transcripts/clean/2026-07-11_duckcon-7-state-of-the-duck.clean.md — DuckCon #7 opening keynote "State of the Duck", 2026-07-11 (Amsterdam), Hannes Mühleisen + Mark Raasveldt
> - docs/batches/2026-05-17-duckdb-quack-as-ducklake-catalog.md — Mike Ritchie (Definite), "Using DuckDB Quack as the DuckLake catalog", 2026-05-17 (updated 2026-06-08)
> - https://ducklake.select/ and https://ducklake.select/docs/stable/ — verifies spec version 1.0, ATTACH forms, catalog backends, the 28 ducklake_* metadata tables, inlining defaults, time-travel/list-files functions
> - https://duckdb.org/docs/current/core_extensions/ducklake — verifies the DuckDB `ducklake` extension, ATTACH syntax, metadata functions (ducklake_snapshots, ducklake_table_info, ducklake_table_changes)

Scope: What DuckLake is, how to attach it, its metadata-as-a-SQL-database design, and the read/write/time-travel/schema-evolution surface — as reported in the talk corpus and verified against the DuckLake 1.0 upstream spec.

---

## 1. What DuckLake IS

DuckLake is an **open lakehouse table format / specification**. Its defining architectural decision: it stores **both the catalog and the table metadata inside a SQL database** (DuckDB, SQLite, Postgres, or MotherDuck), alongside the actual table data written as **Parquet files** in object storage.

> "DuckLake is an open table Lakehouse specification. So it has 'duck' in the name, but it's a fully open spec, can be implemented by anybody… The key architectural decision to enable that is that we keep both our catalog data and our metadata data inside of a SQL database. That could be DuckDB. It could be SQLite. It could be Postgres, or it could be MotherDuck."
> — MotherDuck webinar, 2026-04-28 (docs/youtube-transcripts/clean/2026-04-28_the-ducklake-lakehouse-from-getting-started-to-going-fast.clean.md)

**Upstream-verified** (https://ducklake.select/): DuckLake is "a lakehouse format built on SQL" that delivers "advanced data lake features without traditional lakehouse complexity by using Parquet files and a SQL database." Unlike Iceberg and Delta Lake — which track metadata in a sprawling tree of on-disk files (JSON manifests, Avro manifest lists, etc.) — DuckLake keeps the catalog layer in an ACID-compliant SQL database and stores data as Parquet.

The motivating contrast, stated plainly in the DataGuy walkthrough:

> "your actual data is a handful of nice clean parquet files, but the bookkeeping files of which files went to which table, which change in which version, all of that lives in a sprawling tree of JSON and Avro and manifest files. And DuckDB actually has a really cool solution to that… embedding a file format to track metadata when databases have been the world's best tool for tracking metadata for 50 years."
> — DataGuy, 2026-07-11 (docs/youtube-transcripts/clean/2026-07-11_build-a-local-data-lakehouse-with-duckdb-and-ducklake.clean.md)

### Spec version and release

- **DuckLake spec version 1.0** — **upstream-verified** (https://ducklake.select/): "DuckLake v1.0 was released in April 2026 and is described as 'production-ready' with guaranteed backward-compatibility."
- Requires **DuckDB v1.5.2+** — **upstream-verified** (https://ducklake.select/docs/stable/duckdb/introduction): "DuckLake v1.0 is supported by … DuckDB v1.5.2+".
- The 1.0 milestone, roughly one year after the initial 0.1 idea, was confirmed at DuckCon #7:

  > "it's only been a little more than a year ago that we first basically published like a 0.1… And it's just been a couple of weeks ago actually that we published DuckLake 1.0." — DuckCon #7 keynote, 2026-07-11 (docs/youtube-transcripts/clean/2026-07-11_duckcon-7-state-of-the-duck.clean.md)

### Multi-engine

The spec is not DuckDB-only. **Talk-reported** (MotherDuck webinar, 2026-04-28): "there's a few implementations in addition to the DuckDB implementation already in **Spark, Trino, and in DataFusion**." Treat the non-DuckDB implementations as vendor/roadmap-reported; this doc verifies only the DuckDB extension surface below against upstream.

---

## 2. Getting started — the `ducklake` extension and `ATTACH`

Getting started is three steps: install the extension, attach a catalog, use it.

> "the getting started for DuckLake are these three commands. Install it as a DuckDB extension… You'll attach a catalog, and then you'll start using it." — MotherDuck webinar, 2026-04-28

### Install + attach (DuckDB catalog — default)

**Upstream-verified** (https://ducklake.select/docs/stable/duckdb/introduction, https://duckdb.org/docs/current/core_extensions/ducklake):

```sql
INSTALL ducklake;

-- catalog stored in a local DuckDB file; data path optional
ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;
USE my_ducklake;
```

With an explicit data path (where the Parquet files land):

```sql
ATTACH 'ducklake:metadata.ducklake' AS my_ducklake (DATA_PATH 'data_files/');
```

The `ducklake:` prefix selects the DuckLake catalog type; the path is the catalog database file. The file extension is arbitrary (`.ducklake`, `.duckdb`, `.db`, `.ddb`) — it is just a DuckDB file. **Talk-reported** clarification (webinar Q&A, 2026-04-28): the string after `ducklake:` is a **filename**, not a `database.schema` reference. `DATA_PATH` for a local run is just a folder name, created if absent, and can later point at an S3/GCS/Azure/R2 path.

### Catalog backends (SQLite, Postgres)

**Upstream-verified** (https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database):

```sql
-- SQLite catalog
INSTALL ducklake; INSTALL sqlite;
ATTACH 'ducklake:sqlite:metadata.sqlite' AS my_ducklake (DATA_PATH 'data_files/');
USE my_ducklake;

-- PostgreSQL catalog (Postgres 12+)
INSTALL ducklake; INSTALL postgres;
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost' AS my_ducklake (DATA_PATH 'data_files/');
USE my_ducklake;
```

Connection-string forms: `ducklake:metadata.ducklake` (DuckDB) · `ducklake:sqlite:metadata.sqlite` · `ducklake:postgres:dbname=<db> host=<host>`.

**Talk-reported** deployment guidance (webinar, 2026-04-28): for lightweight/local single-player, a local DuckDB catalog on the laptop SSD is enough; for a multiplayer cloud DuckLake, "I would highly recommend Postgres" as the metadata database because it is multi-writer, with any object store (S3/GCS/Azure) for the Parquet.

### MotherDuck catalog

**Talk-reported** (webinar, 2026-04-28): on MotherDuck the entry point is `CREATE DATABASE … (TYPE DUCKLAKE)` rather than `ATTACH`. Presented as the MotherDuck vendor path; not independently verified against the open spec here.

### Minimal create / insert / select

**Upstream-verified** (https://ducklake.select/docs/stable/duckdb/introduction):

```sql
ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;
USE my_ducklake;

CREATE TABLE nl_train_stations AS FROM 'https://blobs.duckdb.org/nl_stations.csv';

SELECT name_long FROM nl_train_stations AT (VERSION => 1) WHERE code = 'ASB';
```

(`FROM 'file'` and omitting `SELECT *` are DuckDB shorthands, not DuckLake-specific.)

---

## 3. The latency story — one relational hit vs. four round trips

The core performance argument: resolving DuckLake metadata is a **single relational-database query**, whereas Iceberg requires a **four-hop chain** against object storage, each hop dependent on the last.

**Talk-reported** (MotherDuck webinar, 2026-04-28) — the Iceberg read path:

> "with Iceberg, to query your data, you have to do four round trips. You have to talk to the catalog. You have to talk to your metadata file. And then you have to talk to your manifest list and then your manifest file, and only then do you go get your data. And those have to be in sequence because they each depend on each other. And everything in that metadata layer is talking to object storage, which is great for throughput, but it's very slow for latency. Could be 100 milliseconds each of those requests. So even in the very, very fast case, it takes often seconds to read data out of a traditional Lakehouse format."

Against that, DuckLake:

> "Whereas with DuckLake, you're talking to one relational database. That could be measured in single-digit milliseconds to go talk to a database. Very, very low latency. And at that point, then you can just go grab and start reading the Parquet files you need."

| Claim | Value | Source status |
|---|---|---|
| Iceberg metadata resolution | 4 sequential round trips (catalog → metadata file → manifest list → manifest file), then data | **talk-reported**; the architectural four-hop chain is the standard Iceberg design |
| Per-hop object-storage latency | ~100 ms each | **talk-reported**, spoken round number; not independently verified |
| DuckLake metadata resolution | single relational-DB hit, single-digit ms | **talk-reported**; the single-hit architecture is **upstream-verified** (metadata lives in one SQL DB), the millisecond figure is a spoken round number |
| Ingest frequency | "30 times per second" / "multiple times per second" | **talk-reported**, spoken round number; not independently verified |

The architectural claim (one SQL query vs. a multi-file manifest walk) is confirmed upstream. The specific millisecond and per-second numbers are spoken figures from the webinar — treat as directional, not as SLAs.

---

## 4. Metadata IS a database — the ~28 `ducklake_*` tables

Because the metadata lives in a SQL database, you can query it directly with `SELECT`, join the tables, and — the headline demo — **close DuckLake and reopen the catalog as a plain DuckDB database** to read the metadata by hand.

> "at the end, I'm going to show you that the entire metadata layer is also a database that you can run a select statement against." … "connect to the catalog raw … plain DuckDB database with no DuckLake extension … listing every table whose name starts with DuckLake, and you're going to get this great list of 30 tables … then this is that pile of metadata files that Iceberg and Delta keep on disk."
> — DataGuy, 2026-07-11 (docs/youtube-transcripts/clean/2026-07-11_build-a-local-data-lakehouse-with-duckdb-and-ducklake.clean.md)

The talk says "30 tables" as a spoken round number. **Upstream-verified** (https://ducklake.select/docs/stable/specification/tables/overview): **DuckLake v1.0 uses 28 tables** to store metadata and to stage data fragments for inlining. The 28 tables, by category:

| Category | Tables |
|---|---|
| Snapshots | `ducklake_snapshot`, `ducklake_snapshot_changes` |
| Schema | `ducklake_schema`, `ducklake_table`, `ducklake_view`, `ducklake_column` |
| Macros | `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters` |
| Data files & inlining | `ducklake_data_file`, `ducklake_delete_file`, `ducklake_files_scheduled_for_deletion`, `ducklake_inlined_data_tables` |
| Data-file mapping | `ducklake_column_mapping`, `ducklake_name_mapping` |
| Statistics | `ducklake_table_stats`, `ducklake_table_column_stats`, `ducklake_file_column_stats`, `ducklake_file_variant_stats` |
| Partitioning | `ducklake_partition_info`, `ducklake_partition_column`, `ducklake_file_partition_value` |
| Sorting | `ducklake_sort_info`, `ducklake_sort_expression` |
| Auxiliary | `ducklake_metadata`, `ducklake_tag`, `ducklake_column_tag`, `ducklake_schema_versions` |

The `ducklake_data_file` rows carry per-file `min`/`max` statistics per column, which the engine reads (a single SQL query over the catalog) to decide **which Parquet files it must open** before touching object storage. **Talk-reported** (webinar demo, 2026-04-28): "I'm doing that decision-making with one SQL query, figure out, hey, what are all the files I need? We're running a filter where my value is between my min and max value."

### Metadata table functions

Rather than joining the raw tables, the extension exposes table functions. **Upstream-verified**:

| Function | Purpose | Source |
|---|---|---|
| `ducklake_snapshots('catalog')` | Returns all snapshots and their changesets | https://duckdb.org/docs/current/core_extensions/ducklake |
| `<catalog>.snapshots()` | Same, catalog-scoped form: `SELECT * FROM my_ducklake.snapshots();` | https://ducklake.select/docs/stable/duckdb/usage/snapshots |
| `ducklake_table_info('catalog')` | Per-table metadata: name, schema_id, table_id, file counts, sizes | https://duckdb.org/docs/current/core_extensions/ducklake |
| `ducklake_table_changes(...)` | Rows changed between snapshots: `snapshot_id`, `rowid`, `change_type`, + table schema | https://duckdb.org/docs/current/core_extensions/ducklake |
| `ducklake_list_files('catalog','table')` | Data files + delete files for a table, optionally at a snapshot | https://ducklake.select/docs/stable/duckdb/metadata/list_files |
| `current_snapshot()` / `last_committed_snapshot()` | Latest snapshot id / latest committed snapshot for the open connection | https://ducklake.select/docs/stable/duckdb/usage/snapshots |
| `set_commit_message()` | Attach author/message to a snapshot within a transaction | https://ducklake.select/docs/stable/duckdb/usage/snapshots |

`ducklake_list_files` accepts `snapshot_version => N`, `snapshot_time => '<ts>'`, and `schema => 'main'`; it returns `data_file`, `data_file_size_bytes`, `data_file_footer_size`, `data_file_encryption_key`, and the parallel `delete_file*` columns (**upstream-verified**, https://ducklake.select/docs/stable/duckdb/metadata/list_files). The webinar and DataGuy demos also show `<catalog>.table_info` and `<catalog>.snapshots` in the catalog-scoped form.

---

## 5. Time travel, schema evolution, ACID transactions, rollback

### Time travel

**Upstream-verified** (https://ducklake.select/docs/stable/duckdb/usage/time_travel, and the introduction example):

```sql
-- by version / snapshot id
SELECT show_id, show_name FROM streaming_data AT (VERSION => 1);

-- by timestamp (DuckLake resolves to the matching snapshot)
SELECT * FROM tbl AT (TIMESTAMP => now() - INTERVAL '1 week');
```

Every DDL/DML statement records a snapshot in `ducklake_snapshot`; `AT (VERSION => N)` / `AT (TIMESTAMP => ...)` reads the table as of that snapshot. **Talk-reported** demonstration (DataGuy, 2026-07-11): `AT (VERSION => 4)` returned the orders table exactly as it stood before the next day's batch — "same table, same query, same point in history, and there's no restoring a backup or a separate audit copy."

### Schema evolution is metadata-only

Adding, renaming, or dropping columns rewrites **no Parquet files** — it is a catalog edit. Defaults for new columns are filled at read time.

> "schema changes in DuckDB are actually metadata only… no parquet file is ever going to get rewritten… Existing eight customers didn't have a value for this, so the default gets filled in at read time from the catalog. The old parquet files that don't have those columns are never touched… A rename just then becomes a metadata edit… DuckLake actually wrote zero data because there wasn't any new data, and that's the whole promise of schema evolution."
> — DataGuy, 2026-07-11 (docs/youtube-transcripts/clean/2026-07-11_build-a-local-data-lakehouse-with-duckdb-and-ducklake.clean.md)

Column renames are recorded via `ducklake_column` (and the name-mapping tables): the catalog records that the logical column the world calls `full_name` maps to the physical `name` bytes inside the old Parquet files.

### Multi-table ACID transactions and rollback

DuckLake supports **transactions that span multiple tables**, committing to **one atomic snapshot** — a property the file-tree formats do not hand you cleanly.

> "wrap them in a begin transaction and then commit it. Insert order 1017 into orders and then bump customer three lifetime value… Two tables, one atomic unit… we can look at the newest snapshot, and the changes will show both tables move together. So there's one snapshot, two tables. And an Iceberg, Delta, each table's version on its own. So cleaning atomic commit across two of them is pretty awkward… it's a begin and a commit, because the catalog is a real database, and real databases are able to do this really easily."
> — DataGuy, 2026-07-11

**Upstream-verified** (https://ducklake.select/): "DuckLake allows concurrent access with ACID transactional guarantees over multi-table operations." Rollback works as in any SQL database — open a transaction, mutate, then `ROLLBACK` and the changes "never happened." DuckLake uses **optimistic concurrency control** (write the Parquet file to object storage first, then commit the pointer into the catalog; retry on conflict) — **talk-reported** (webinar, 2026-04-28).

---

## 6. Inlining — small writes go into the catalog first

Rows written in **small batches (default ≤ 10 rows)** are written directly into the catalog database instead of producing a tiny Parquet file, then flushed to Parquet later. Reads still see inlined rows transactionally.

**Upstream-verified** (https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining):

- Default: **enabled, row limit 10**.
- Per-connection override on `ATTACH`: `ATTACH 'ducklake:inlining.duckdb' AS my_ducklake (DATA_INLINING_ROW_LIMIT 1000);` (default `10`)
- Global setting: `SET ducklake_default_data_inlining_row_limit = 50;`
- Flush to Parquet: `CALL ducklake_flush_inlined_data('my_ducklake');`

> "if you insert — by default, inlining is enabled for rows inserting 10 rows or less. So if you inserted five rows in, you won't actually see any Parquet files generated. That's because we have a database there already, and we can use that database to store small row counts of data and flush it out to Parquet later… you get the buffering type of capability of a Kafka, but you keep the transactionality of a lakehouse."
> — MotherDuck webinar, 2026-04-28

**Footgun — inlined data lives in the catalog.** Raising the inlining limit under a high-frequency write load pushes real (unflushed) data into the catalog database. The Definite writeup measured, in a cloned customer lake, an **835,000-row gap** between snapshot row count and Parquet-backed row count — over three thousand `ducklake_inlined_data_*` tables of not-yet-flushed rows (docs/batches/2026-05-17-duckdb-quack-as-ducklake-catalog.md). If the catalog is Postgres, this defeats the "catalog stays small" assumption; **VARIANT inlining is not supported on a Postgres catalog** because the type does not survive the string round-trip (same source; see also `11_quack_extension.md`).

---

## 7. Iceberg interop — same Parquet, V2 delete files, metadata-only import

DuckLake writes the **same Parquet data files** and follows the **Iceberg V2 delete-file spec**, which makes cross-format import cheap.

**Talk-reported** (MotherDuck webinar, 2026-04-28):

- Delete files follow the Iceberg spec, **currently V2**; **V3 in testing** for the next version. (Deletes are append-only delete files, preserved for time travel; `merge` maintenance rewrites files to physically remove deleted rows.)
- **Import from Iceberg is metadata-only**: "if you've got 100 terabytes of data in Iceberg, you don't have to copy those 100 terabytes, you just copy the megabytes or gigabytes you have of metadata, and DuckLake can take off and run from there."
- **Export back to Iceberg is still being worked on** — DuckLake allows multiple snapshots within one file (needed for inlining), which Iceberg's file model does not represent, so an export must collapse to e.g. the latest snapshot per file.

These are speaker-stated roadmap/behavior claims; the metadata-only import and the shared-Parquet basis are consistent with the format design but are marked talk-reported where a number or roadmap intention is involved.

---

## 8. Where DuckLake sits vs. the rest of the library

- **DuckLake is the lakehouse/table layer** over DuckDB + Parquet: catalog and metadata in a SQL database, data as Parquet in object storage.
- **Quack** is the DuckDB client-server protocol that makes a DuckDB-file catalog reachable by many concurrent writers over HTTP — the "missing multi-writer layer" that lets a DuckDB (not Postgres) catalog serve a real lakehouse. See `11_quack_extension.md`.
- **Tuning** — Parquet V2 + Zstandard, row-group sizing, partitioning/bucketing, and sorted (clustered) tables for row-group skipping — is covered in `15_ducklake_tuning.md`.
- **core-x today writes Lance**, not DuckLake. The Lance system-of-record path is documented under `../lance/`.

> **Relevance to core-x.** DuckLake is an **evaluated alternative table layer**, not the current system of record — core-x's Gen-3 SoR is LanceDB under `s3://data-sink/active/` (R2). Where DuckLake is load-bearing is the read/compute side: it is the natural structured-query layer over DuckDB→Arrow→Parquet, and its metadata-in-SQL design (single-hit file pruning via `ducklake_data_file` min/max stats) is the same statistics-skipping discipline core-x already applies with hard `BTREE` scalar indexes on Lance resolution keys. If a lakehouse table layer over the R2 Parquet plane is ever needed alongside Lance, DuckLake-on-R2 with a Postgres or Quack-served DuckDB catalog is the shape to evaluate — with the egress-free R2 billing model and larger file/row-group sizes to offset R2's higher per-request latency (**talk-reported**, webinar 2026-04-28).

---

## Footguns / version-gating summary

| Item | Note | Source status |
|---|---|---|
| Minimum DuckDB | v1.5.2+ for DuckLake 1.0 | upstream-verified |
| Parquet V2 / Zstandard | not the default (Snappy is); set explicitly, watch reader compatibility | talk-reported (tuning) — see `15_ducklake_tuning.md` |
| Inlining default | on, ≤10 rows; small `INSERT`s produce **no** Parquet file until flushed | upstream-verified |
| Inlined data location | lives in the catalog DB; raising the limit bloats the catalog; **VARIANT inlining unsupported on Postgres** | upstream-verified (spec) + docs/batches/2026-05-17 |
| Iceberg delete spec | V2 today, V3 in testing | talk-reported |
| Export to Iceberg | not fully available; multi-snapshot-per-file must be collapsed | talk-reported |
| `SET sorted_by` at CREATE | today a post-create command; in-CREATE syntax on roadmap | talk-reported — see `15_ducklake_tuning.md` |
| Latency numbers (single-digit ms, ~100 ms/hop, 30 writes/s) | spoken round numbers; architecture verified, figures not | talk-reported |
| Table count "30" | actual spec count is **28** in v1.0 | upstream-verified |
