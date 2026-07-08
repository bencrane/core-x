# Parquet — read_parquet, COPY TO, metadata, partitioning, pushdown

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/current/data/parquet/overview.html — reading/writing Parquet, `read_parquet`/`parquet_scan`, `COPY … TO`, glob reads, projection/filter pushdown, `schema` parameter
> - https://duckdb.org/docs/current/data/parquet/metadata.html — `parquet_metadata`, `parquet_schema`, `parquet_file_metadata`, `parquet_kv_metadata`, `parquet_full_metadata`, `parquet_bloom_probe`
> - https://duckdb.org/docs/current/data/partitioning/partitioned_writes.html — `PARTITION_BY`, Hive folder layout, `OVERWRITE_OR_IGNORE`/`OVERWRITE`/`APPEND`, `FILENAME_PATTERN`
> - https://duckdb.org/docs/current/data/partitioning/hive_partitioning.html — `hive_partitioning`, `hive_types`, `hive_types_autocast` on read
> - https://duckdb.org/docs/current/data/parquet/tips.html — per-thread output, row-group sizing, `union_by_name`, sorting for tight zonemaps
> - https://duckdb.org/docs/current/sql/statements/copy.html — full `COPY … TO` general + Parquet-specific option tables
> - https://duckdb.org/docs/current/data/parquet/encryption.html — `PRAGMA add_parquet_key`, `ENCRYPTION_CONFIG`, AES key sizes, limitations

Scope: Reading, writing, inspecting, partitioning, and pushing predicates into Apache Parquet from DuckDB — every documented `read_parquet`/`COPY … TO`/metadata-function parameter with its type, default, and accepted values, plus the pushdown and range-request behavior that makes Parquet the transport format of choice over object storage.

---

## Version context (as of 2026-07-08)

- **Current stable line:** DuckDB **v1.5.x** ("Variegata", after the Paradise shelduck), latest patch **v1.5.4** (2026-06-17). Parquet reader/writer is built into the core engine — no extension `INSTALL`/`LOAD` is required for local files. (Note: "Ossivalis" is the **v1.3** codename, not v1.5.)
- **LTS line:** DuckDB **v1.4.x** ("Andium"), latest patch **v1.4.5**; community support through September 2026. Behaviors below hold on both 1.4.x and 1.5.x unless a version gate is called out.
- **Object storage** (`s3://`, `r2://`, `gcs://`, `https://`) requires the `httpfs` extension, which autoloads on first use. See [07_httpfs_s3_r2.md](07_httpfs_s3_r2.md) and [08_secrets_manager.md](08_secrets_manager.md).
- **Version gates to note:** the `filename` virtual column is emitted automatically for globbed reads since **v1.3.0**; `{uuidv4}` / `{uuidv7}` `FILENAME_PATTERN` tokens and the `SHREDDING` write option are recent (1.4+/1.5+) additions.

---

## 1. Reading Parquet

### 1.1 Path syntax (replacement scan)

DuckDB registers a **replacement scan** for string literals ending in `.parquet`, so you can name the file directly in `FROM`:

```sql
-- direct path (extension recognized)
SELECT * FROM 'test.parquet';

-- non-standard extension → call the function explicitly
SELECT * FROM read_parquet('test.parq');

-- parquet_scan is a hard alias for read_parquet
SELECT * FROM parquet_scan('test.parquet');
```

`read_parquet(...)`, `parquet_scan(...)`, and the bare-path replacement scan are three spellings of the same table function.

### 1.2 Multiple files, globs, lists

```sql
-- explicit list
SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet', 'file3.parquet']);

-- glob (single directory)
SELECT * FROM 'test/*.parquet';

-- recursive glob
SELECT * FROM read_parquet('dir/**/*.parquet');

-- glob over object storage (httpfs)
SELECT * FROM read_parquet('s3://bucket/prefix/*.parquet');
```

All matched files are read as one unified relation. By default schemas must be **positionally** compatible; use `union_by_name = true` to unify by column name instead (see §1.4).

### 1.3 `read_parquet` / `parquet_scan` parameter table

Named parameters passed after the file argument. Copied from the DuckDB Parquet overview and Hive-partitioning pages.

| Parameter | Type | Default | Meaning / accepted values |
|---|---|---|---|
| `binary_as_string` | `BOOL` | `false` | Load `BYTE_ARRAY`/binary columns as `VARCHAR`. Needed for legacy files written without the UTF8 logical-type flag. |
| `can_have_nan` | `BOOL` | `false` | Account for `NaN` values in `FLOAT`/`DOUBLE` filter pushdown so predicates over float columns stay correct when NaNs are present. |
| `filename` | `BOOL` | `false` | Add a `filename` column giving each row's source path. **Kept for compatibility** — since v1.3.0 the `filename` virtual column is available on globbed reads without setting this. |
| `file_row_number` | `BOOL` | `false` | Add a `file_row_number` column with the row's ordinal position within its source file. |
| `hive_partitioning` | `BOOL` | auto-detected | Interpret `key=value` directory segments as partition columns and expose them as columns. Auto-detected from the path; force with `true`/`false`. |
| `hive_types` | `STRUCT` (map literal) | auto | Override the logical type of specific partition columns, e.g. `hive_types = {'release': DATE, 'orders': BIGINT}`. |
| `hive_types_autocast` | `BOOL`/`0`/`1` | `1` (on) | Auto-cast partition column values. Set to `0` to keep all partition columns as `VARCHAR`. Auto-cast covers `DATE`, `TIMESTAMP`, and `BIGINT` only. |
| `union_by_name` | `BOOL` | `false` | Unify multi-file schemas **by column name** rather than by position; missing columns are filled with `NULL`. |
| `encryption_config` | `STRUCT` | — | Parquet encryption configuration, e.g. `{footer_key: 'key256'}`. See §6. |
| `schema` | `MAP` | `NULL` | Custom schema mapping keyed by **field ID**, used to rename, recast, reorder, or inject default-valued columns at read time (requires field IDs in the file). See §1.5. |

> **Footgun — `hive_types_autocast` only knows three types.** Auto-cast promotes partition values to `DATE`, `TIMESTAMP`, or `BIGINT`; every other logical type stays `VARCHAR` unless you name it explicitly in `hive_types`. If a partition key is a decimal or a non-standard timestamp, cast it yourself.

### 1.4 `union_by_name` (schema evolution across files)

```sql
-- files with drifting columns; absent columns become NULL
SELECT *
FROM read_parquet('flights*.parquet', union_by_name = true);
```

`union_by_name` has a cost: DuckDB must read every file's footer to compute the union schema before scanning, so it is slower than positional reads. Use it only when schemas genuinely differ.

### 1.5 `schema` parameter — rename / recast / add columns on read

```sql
SELECT * FROM read_parquet('integers.parquet', schema = MAP {
    0: {name: 'renamed_i',  type: 'BIGINT',   default_value: NULL},
    1: {name: 'new_column', type: 'UTINYINT', default_value: 43}
});
```

The map is keyed by the Parquet **field ID**. Each entry supplies a `name`, a target `type`, and a `default_value` used when the field is absent from a given file. This is the read-side counterpart to `FIELD_IDS` on write (§2.3).

### 1.6 Reading over HTTP(S) / object storage

```sql
SELECT * FROM read_parquet('https://some.url/some_file.parquet');
```

For `s3://` / `r2://` / `gcs://` you need `httpfs` and credentials via the Secrets Manager. Detailed in [07_httpfs_s3_r2.md](07_httpfs_s3_r2.md).

---

## 2. Writing Parquet with `COPY … TO`

### 2.1 Basic form

```sql
COPY (SELECT * FROM tbl) TO 'result.parquet' (FORMAT parquet);

-- a whole table
COPY tbl TO 'result.parquet' (FORMAT parquet);
```

`FORMAT parquet` is inferred from a `.parquet` extension but stating it is the safe, explicit habit.

### 2.2 General `COPY … TO` options (apply to all formats)

Copied verbatim from the `COPY` statement reference.

| Option | Type | Default | Description |
|---|---|---|---|
| `FORMAT` | `VARCHAR` | auto | Copy function to use; default selected from the file extension. |
| `USE_TMP_FILE` | `BOOL` | auto | Write to a temporary file first, then rename, if the target already exists (atomic-ish replace). |
| `OVERWRITE_OR_IGNORE` | `BOOL` | `false` | Allow overwriting files if they already exist. **Only effective with `PARTITION_BY`.** |
| `OVERWRITE` | `BOOL` | `false` | Remove all existing files inside targeted directories. **Only effective with `PARTITION_BY`.** |
| `APPEND` | `BOOL` | `false` | Regenerate paths so no existing files are overwritten. **Only effective with `PARTITION_BY`.** |
| `FILENAME_PATTERN` | `VARCHAR` | auto | Filename pattern; may contain `{uuid}`, `{uuidv4}`, `{uuidv7}`, or `{i}` (incrementing index). |
| `FILE_EXTENSION` | `VARCHAR` | auto | File extension for generated file(s). |
| `PER_THREAD_OUTPUT` | `BOOL` | `false` | Generate one file per thread rather than a single file. |
| `FILE_SIZE_BYTES` | `VARCHAR` or `BIGINT` | (empty) | If set, writes a **directory** of multiple files, rolling to a new file when this size is exceeded (accepts `'100MB'` etc.). |
| `PARTITION_BY` | `VARCHAR[]` | (empty) | Columns to partition by using a Hive partitioning scheme. See §4. |
| `PRESERVE_ORDER` | `BOOL` | (config-dependent) | Preserve row order during the copy. |
| `RETURN_FILES` | `BOOL` | `false` | Include the created filepath(s) in the query result. |
| `RETURN_STATS` | `BOOL` | `false` | Return files **and their per-column statistics** written as part of the COPY. |
| `WRITE_PARTITION_COLUMNS` | `BOOL` | `false` | Write the partition columns into the files as well as the directory names. **Only effective with `PARTITION_BY`.** |

> By default, `PARTITION_BY` columns are **not** stored inside the Parquet files (their values live only in the directory names) — set `WRITE_PARTITION_COLUMNS true` if a downstream reader that does not understand Hive layout needs the values in-file.

### 2.3 Parquet-specific write options

Copied verbatim from the `COPY` statement reference.

| Option | Type | Default | Description |
|---|---|---|---|
| `COMPRESSION` | `VARCHAR` | `snappy` | One of `uncompressed`, `snappy`, `gzip`, `zstd`, `brotli`, `lz4`, `lz4_raw`. |
| `COMPRESSION_LEVEL` | `BIGINT` | `3` | Level `1` (lowest) to `22` (highest). **Only applies to `zstd`.** |
| `FIELD_IDS` | `STRUCT` | (empty) | Field ID per column; pass `auto` to infer. Enables the read-side `schema` mapping (§1.5). |
| `ROW_GROUP_SIZE` | `BIGINT` | `122880` | Target rows per row group. Minimum effective granularity is 2,048 (the vector size). |
| `ROW_GROUP_SIZE_BYTES` | `BIGINT` | `row_group_size * 1024` | Target bytes per row group (accepts human-readable strings). Whichever of rows/bytes is hit first closes the row group. |
| `ROW_GROUPS_PER_FILE` | `BIGINT` | (empty) | Start a new Parquet file after this many row groups (writes a directory of files). |
| `CHUNK_SIZE` | `BIGINT` | `122880` | Alias for `ROW_GROUP_SIZE`. |
| `PARQUET_VERSION` | `VARCHAR` | `V1` | Parquet format version, `V1` or `V2`. |
| `KV_METADATA` | `STRUCT` | (empty) | Custom key-value metadata embedded in the file footer. |
| `STRING_DICTIONARY_PAGE_SIZE_LIMIT` | `BIGINT` | 1 MB | Max size of a string dictionary page. |
| `DICTIONARY_SIZE_LIMIT` | `BIGINT` | `ROW_GROUP_SIZE / 5` | Max dictionary size for dictionary encoding. |
| `WRITE_BLOOM_FILTER` | `BOOLEAN` | `true` | Write Bloom filters so readers can skip row groups on equality predicates. |
| `BLOOM_FILTER_FALSE_POSITIVE_RATIO` | `DOUBLE` | `0.01` | Target false-positive ratio of written Bloom filters. |
| `SHREDDING` | `STRUCT` | (empty) | Map `VARIANT` column names to types for typed (shredded) storage. |
| `GEOPARQUET_VERSION` | `VARCHAR` | `V1` | GeoParquet metadata version for geometry columns: `NONE`, `V1`, `V2`, `BOTH`. |
| `ENCRYPTION_CONFIG` | `STRUCT` | — | Encryption configuration, e.g. `{footer_key: 'key256'}`. See §6. |

> **Note on defaults conflicting across pages.** The overview page lists the `COMPRESSION` default as `snappy` in the `COPY` reference; some example text implies `zstd` for size-optimized writes. The authoritative default per the `COPY` statement reference is **`snappy`**. Explicitly state `COMPRESSION zstd` when you want the higher ratio.

### 2.4 Common write examples

```sql
-- zstd with a large row group (better compression, more parallel-read units)
COPY (FROM generate_series(100_000))
TO 'test.parquet'
(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100_000);

-- embed key-value metadata in the footer
COPY (SELECT 42 AS number) TO 'kv_metadata.parquet' (
    FORMAT parquet,
    KV_METADATA {number: 'Answer to life, universe, and everything'}
);

-- roll to a new file every 2 row groups
COPY (FROM generate_series(100_000))
TO 'output-directory'
(FORMAT parquet, ROW_GROUP_SIZE 20_000, ROW_GROUPS_PER_FILE 2);

-- capture the paths that were written
COPY tbl TO 'out' (FORMAT parquet, PARTITION_BY (dt), RETURN_FILES);

-- export an entire database as Parquet
EXPORT DATABASE 'target_directory' (FORMAT parquet);
```

---

## 3. Metadata & inspection functions

All accept a single file path or a glob.

### 3.1 `parquet_metadata(path)` — per-column-chunk statistics

The workhorse for zonemap inspection: one row per (row group × column) with the min/max stats that drive filter pushdown.

| Column | Type |
|---|---|
| `file_name` | `VARCHAR` |
| `row_group_id` | `BIGINT` |
| `row_group_num_rows` | `BIGINT` |
| `row_group_num_columns` | `BIGINT` |
| `row_group_bytes` | `BIGINT` |
| `column_id` | `BIGINT` |
| `file_offset` | `BIGINT` |
| `num_values` | `BIGINT` |
| `path_in_schema` | `VARCHAR` |
| `type` | `VARCHAR` |
| `stats_min` | `VARCHAR` |
| `stats_max` | `VARCHAR` |
| `stats_null_count` | `BIGINT` |
| `stats_distinct_count` | `BIGINT` |
| `stats_min_value` | `VARCHAR` |
| `stats_max_value` | `VARCHAR` |
| `compression` | `VARCHAR` |
| `encodings` | `VARCHAR` |
| `index_page_offset` | `BIGINT` |
| `dictionary_page_offset` | `BIGINT` |
| `data_page_offset` | `BIGINT` |
| `total_compressed_size` | `BIGINT` |
| `total_uncompressed_size` | `BIGINT` |
| `key_value_metadata` | `MAP(BLOB, BLOB)` |
| `bloom_filter_offset` | `BIGINT` |
| `bloom_filter_length` | `BIGINT` |
| `min_is_exact` | `BOOLEAN` |
| `max_is_exact` | `BOOLEAN` |
| `row_group_compressed_bytes` | `BIGINT` |

```sql
SELECT * FROM parquet_metadata('test.parquet');
SELECT * FROM parquet_metadata('data/*.parquet');  -- glob supported
```

### 3.2 `parquet_schema(path)` — physical schema tree

Reflects the Parquet-internal schema (including nested/converted/logical types). For a plain column list, prefer `DESCRIBE SELECT * FROM 'x.parquet'`.

| Column | Type |
|---|---|
| `file_name` | `VARCHAR` |
| `name` | `VARCHAR` |
| `type` | `VARCHAR` |
| `type_length` | `VARCHAR` |
| `repetition_type` | `VARCHAR` |
| `num_children` | `BIGINT` |
| `converted_type` | `VARCHAR` |
| `scale` | `BIGINT` |
| `precision` | `BIGINT` |
| `field_id` | `BIGINT` |
| `logical_type` | `VARCHAR` |

```sql
SELECT * FROM parquet_schema('test.parquet');
```

### 3.3 `parquet_file_metadata(path)` — file-level footer

| Column | Type |
|---|---|
| `file_name` | `VARCHAR` |
| `created_by` | `VARCHAR` |
| `num_rows` | `BIGINT` |
| `num_row_groups` | `BIGINT` |
| `format_version` | `BIGINT` |
| `encryption_algorithm` | `VARCHAR` |
| `footer_signing_key_metadata` | `VARCHAR` |
| `file_size_bytes` | `UBIGINT` |
| `footer_size` | `UBIGINT` |
| `column_orders` | `VARCHAR[]` |

```sql
SELECT file_name, num_rows, num_row_groups, format_version, created_by
FROM parquet_file_metadata('test.parquet');
```

### 3.4 `parquet_kv_metadata(path)` — custom footer key-value pairs

Keys and values are raw `BLOB`; decode with `decode(...)`/`CAST(... AS VARCHAR)` as needed.

| Column | Type |
|---|---|
| `file_name` | `VARCHAR` |
| `key` | `BLOB` |
| `value` | `BLOB` |

```sql
SELECT file_name, decode(key) AS k, decode(value) AS v
FROM parquet_kv_metadata('kv_metadata.parquet');
```

### 3.5 `parquet_full_metadata(path)` — everything, one row per file

Returns all four metadata views combined as nested `STRUCT[]` columns in a single row per file.

| Column | Type |
|---|---|
| `parquet_file_metadata` | `STRUCT(...)[]` |
| `parquet_metadata` | `STRUCT(...)[]` |
| `parquet_schema` | `STRUCT(...)[]` |
| `parquet_kv_metadata` | `STRUCT(...)[]` |

```sql
SELECT * FROM parquet_full_metadata('test.parquet');
```

### 3.6 `parquet_bloom_probe(path, column, value)` — Bloom-filter row-group skipping

Returns which row groups a Bloom filter can prove do **not** contain `value`. Supported for integer types, floating-point types, `VARCHAR`, and `BLOB`.

| Column | Type | Meaning |
|---|---|---|
| `file_name` | `VARCHAR` | File path |
| `row_group_id` | `BIGINT` | Row group ID |
| `bloom_filter_excludes` | `BOOLEAN` | `true` ⇒ this row group is guaranteed not to contain the value |

```sql
SELECT * FROM parquet_bloom_probe('my_file.parquet', 'my_col', 500);
```

---

## 4. Partitioned (Hive) writes

`PARTITION_BY` writes a Hive-partitioned folder hierarchy where each partition column becomes a `key=value` directory level.

```sql
COPY orders TO 'orders'
(FORMAT parquet, PARTITION_BY (year, month));
```

Resulting layout:

```
orders
├── year=2021
│    ├── month=1
│    │   ├── data_1.parquet
│    │   └── data_2.parquet
│    └── month=2
│        └── data_1.parquet
└── year=2022
     └── ...
```

### 4.1 Existing-directory handling

Writing partitions into a directory that already contains files errors by default. Choose one:

| Option | Behavior |
|---|---|
| `OVERWRITE_OR_IGNORE` | Allow overwriting existing files (local filesystems). |
| `OVERWRITE` | Remove **all** existing files inside the targeted partition directories first. |
| `APPEND` | Add new files alongside existing ones, regenerating UUIDs to avoid collisions. |

```sql
COPY orders TO 'orders'
(FORMAT csv, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE);
```

### 4.2 Filename patterns

`FILENAME_PATTERN` controls the per-partition filename. `{i}` → incrementing index; `{uuid}`/`{uuidv4}`/`{uuidv7}` → a 128-bit UUID.

```sql
COPY orders TO 'orders'
(FORMAT parquet, PARTITION_BY (year, month), FILENAME_PATTERN 'orders_{i}');
```

### 4.3 Slashes in partition values

Partition values containing `/` break the directory hierarchy. Percent-encode them with `url_encode(...)` before writing (and `url_decode(...)` on read).

### 4.4 Concurrency & sizing knobs

- `partitioned_write_max_open_files` (config, **default 100**) caps simultaneously open output files during a partitioned write. Highly-cardinality partitions that exceed this get written in multiple passes.
- **Guidance:** "Writing data into many small partitions is expensive. It is generally recommended to have at least `100 MB` of data per partition." Over-partitioning produces millions of tiny files, wrecking both write throughput and later object-storage list/read costs.

### 4.5 Reading a partitioned dataset back

```sql
SELECT *
FROM read_parquet(
    'orders/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT}
);
```

A predicate on a partition column (`WHERE year = 2022`) prunes whole directories before any file is opened — partition pruning, layered on top of the in-file row-group pushdown described next.

---

## 5. Projection & filter pushdown

Two mechanisms make Parquet dramatically cheaper to scan than row-oriented formats:

- **Projection pushdown** — only the columns referenced by the query are read from disk/object storage. A `SELECT a, b` over a 200-column file fetches two column chunks, not the whole file.
- **Filter pushdown** — `WHERE` predicates on Parquet columns are evaluated against each row group's **min/max zonemap** (and Bloom filters for equality). Row groups whose stats cannot satisfy the predicate are skipped entirely, so their column chunks are never fetched.

Combined with `httpfs`, these translate into **HTTP range requests**: DuckDB reads the footer, decides which row groups and columns it needs, then issues byte-range `GET`s for only those regions. A selective query over a large remote Parquet file transfers a small fraction of the object.

To maximize skipping, keep zonemaps tight and non-overlapping by sorting on your common filter columns before writing:

```sql
COPY (FROM 'events.parquet' ORDER BY event_time)
TO 'events-sorted.parquet'
(FORMAT parquet);
```

Verify skipping is possible by inspecting the stats the reader will use:

```sql
SELECT row_group_id, path_in_schema, stats_min_value, stats_max_value
FROM parquet_metadata('events-sorted.parquet')
WHERE path_in_schema = 'event_time';
```

---

## 6. Encryption

DuckDB supports Parquet Modular Encryption using in-session AES keys (128, 192, or 256-bit; raw or base64).

```sql
-- register keys for the session (in-memory only)
PRAGMA add_parquet_key('key128', '0123456789112345');
PRAGMA add_parquet_key('key256', '01234567891123450123456789112345');
PRAGMA add_parquet_key('key256base64', 'MDEyMzQ1Njc4OTExMjM0NTAxMjM0NTY3ODkxMTIzNDU=');

-- write encrypted
COPY tbl TO 'tbl.parquet' (ENCRYPTION_CONFIG {footer_key: 'key256'});

-- read encrypted (either spelling)
COPY tbl FROM 'tbl.parquet' (ENCRYPTION_CONFIG {footer_key: 'key256'});
SELECT * FROM read_parquet('tbl.parquet', encryption_config = {footer_key: 'key256'});
```

**Limitations / footguns:**

- **Uniform encryption only.** The footer and all columns are encrypted with the same key. Per-column keys are parsed but not implemented:
  ```sql
  -- throws: "Not implemented Error: Parquet encryption_config column_keys not yet implemented"
  COPY tbl TO 'tbl.parquet'
      (ENCRYPTION_CONFIG {footer_key: 'key256', column_keys: {key256: ['col0', 'col1']}});
  ```
- **~2.5× overhead** versus unencrypted reads/writes on typical datasets.
- **Interop:** DuckDB reads uniformly-encrypted files written by the Arrow C++ / PyArrow API when the same key is used for footer and columns.
- Keys are session-scoped and never persisted — re-register with `add_parquet_key` in every new connection.

---

## 7. Worked example — pushdown read + partitioned zstd write

```sql
-- 1. Selective read over a remote glob: only 'id' and 'amount' columns are
--    fetched, and only row groups whose 'event_date' zonemap overlaps the
--    range are range-requested from object storage.
CREATE VIEW recent AS
SELECT id, amount, event_date
FROM read_parquet('s3://bucket/events/**/*.parquet', hive_partitioning = true)
WHERE event_date >= DATE '2026-06-01'
  AND amount > 1000;

-- 2. Re-materialize sorted + partitioned as zstd Parquet for tight zonemaps
--    and cheap partition pruning downstream.
COPY (SELECT * FROM recent ORDER BY event_date)
TO 's3://bucket/curated/events'
(
    FORMAT parquet,
    COMPRESSION zstd,
    COMPRESSION_LEVEL 9,
    PARTITION_BY (event_date),
    ROW_GROUP_SIZE 122880,
    OVERWRITE_OR_IGNORE,
    FILENAME_PATTERN 'part_{uuidv7}'
);
```

---

## 8. Deprecations, renames, and footguns

- **`parquet_scan` is not deprecated** — it is a permanent alias of `read_parquet`. Prefer `read_parquet` for clarity.
- **`filename = true` is legacy.** Since v1.3.0 the `filename` virtual column is available on globbed reads without the parameter; the parameter is retained for backward compatibility.
- **`CHUNK_SIZE` == `ROW_GROUP_SIZE`.** Do not set both.
- **`OVERWRITE_OR_IGNORE` / `OVERWRITE` / `APPEND` / `WRITE_PARTITION_COLUMNS` are inert without `PARTITION_BY`.** They silently do nothing on a single-file write.
- **`COMPRESSION_LEVEL` is zstd-only.** Setting it under `snappy`/`lz4` has no effect.
- **Small row groups + many partitions = a tiny-file storm.** Keep ~100 MB per partition and enough rows per row group that each file has roughly `threads`-many row groups for parallel reads.
- **`union_by_name` reads every footer up front** — measurably slower than positional reads; use only for genuine schema drift.
- **Partition columns are directory-only by default** — a non-Hive-aware reader will not see them unless you set `WRITE_PARTITION_COLUMNS true`.

---

## 9. Cross-references

- [00_overview.md](00_overview.md) — editions, clients, versioning/release lines.
- [01_python_client.md](01_python_client.md) — `connect`/`execute`, relational API, replacement scans (Python-side `read_parquet`).
- [02_arrow_integration.md](02_arrow_integration.md) — zero-copy Arrow between DuckDB and Lance.
- [03_csv_import.md](03_csv_import.md) — the row-oriented counterpart; contrast with §5 pushdown.
- [06_configuration_memory_spill.md](06_configuration_memory_spill.md) — `memory_limit`, `threads`, `temp_directory` for out-of-core Parquet scans/writes.
- [07_httpfs_s3_r2.md](07_httpfs_s3_r2.md) — object-storage reads/writes and range requests.
- [08_secrets_manager.md](08_secrets_manager.md) — `CREATE SECRET` for S3/R2/GCS credentials.
- [13_lance_interop.md](13_lance_interop.md) — reading/writing Lance from DuckDB.

---

> **Relevance to core-x:** Parquet is **transport-only** in the core-x plane — ephemeral column-oriented Parquet is streamed through the DuckDB worker into Lance (`s3://data-sink/active/`, on Cloudflare R2), never a system of record. The pushdown behavior in §5 is why this beats CSV over R2: a selective read fetches only the needed column chunks and row groups via HTTP **range requests**, whereas a CSV read must pull the whole object and parse every byte. For large out-of-core stages, pair `ROW_GROUP_SIZE`/`ROW_GROUPS_PER_FILE` here with `memory_limit`/`temp_directory` spill ([06_configuration_memory_spill.md](06_configuration_memory_spill.md)) so hundreds-of-millions-of-rows projections stream to Lance without OOM. R2 credentials go through the Secrets Manager ([08_secrets_manager.md](08_secrets_manager.md)); the final zero-copy handoff to Lance uses Arrow ([02_arrow_integration.md](02_arrow_integration.md)), not an intermediate Parquet file on disk.

---

## Unverified / needs confirmation

- **`PRESERVE_ORDER` default** — the `COPY` reference marks it config-dependent (tied to `preserve_insertion_order`) rather than a fixed literal; treat as "on unless the session disabled insertion-order preservation."
- **`ROW_GROUP_SIZE` default (`122880`) vs. tips-page phrasing** — the tips page describes the default as 122,880 and the minimum as 2,048 (vector size); confirmed consistent across the `COPY` reference and tips page.
- **`SHREDDING` (VARIANT) and `GEOPARQUET_VERSION`** — present in the current `COPY` option table; exact accepted-value semantics for `SHREDDING` structs were not exhaustively documented on the fetched pages. Confirm against the target DuckDB build before relying on them.
- **`STRING_DICTIONARY_PAGE_SIZE_LIMIT` vs `DICTIONARY_SIZE_LIMIT`** — both appear in the current option set with different defaults (1 MB vs `ROW_GROUP_SIZE/5`); they govern the string-dictionary page and the overall dictionary respectively. Verify interaction on your build if tuning dictionary encoding.
