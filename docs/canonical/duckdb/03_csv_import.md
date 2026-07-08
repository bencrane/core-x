# CSV Import — read_csv, COPY, options (all_varchar, encoding, sample_size, ignore_errors, rejects)

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/stable/data/csv/overview (redirects to https://duckdb.org/docs/current/data/csv/overview.html) — read_csv / read_csv_auto / COPY FROM, full parameter table, defaults
> - https://duckdb.org/docs/stable/data/csv/auto_detection (→ /docs/current/data/csv/auto_detection.html) — the CSV sniffer: dialect/type/header detection, sample_size, auto_type_candidates, sniff_csv()
> - https://duckdb.org/docs/stable/data/csv/reading_faulty_csv_files (→ /docs/current/…) — ignore_errors, store_rejects, reject_scans / reject_errors table schemas, error types
> - https://duckdb.org/docs/stable/data/csv/tips (→ /docs/current/…) — practical tips: all_varchar, type overrides, sample_size=-1, union_by_name
> - https://duckdb.org/docs/current/sql/statements/copy.html — COPY … TO CSV writing options
> - https://github.com/duckdb/duckdb/releases + https://pypi.org/project/duckdb/ — current version confirmation

Scope: how DuckDB reads CSV into tables/relations (`read_csv`, `read_csv_auto`, `COPY … FROM`, `FROM 'file.csv'`), the complete option surface, the auto-detection sniffer, robust/faulty-CSV handling via `ignore_errors` and reject tables, and `COPY … TO` for CSV output — with a defensive `all_varchar` + `TRY_CAST` ingest pattern for messy government CSVs.

---

## Version ground truth (as of 2026-07-08)

- **Current stable line: DuckDB 1.5.x** — codenamed "Variegata", 1.5.0 released 2026-03-09; subsequent patches 1.5.2 (2026-04-13), 1.5.3 (2026-05-19), latest **1.5.4** (published 2026-06-17). The `duckdb` Python package tracks the same version numbers.
- **LTS line: 1.4.x** ("Andium"), still receiving updates; latest **1.4.5 LTS** released 2026-06-17. Under the release policy adopted at 1.4.0, every other minor line is LTS.
- The CSV reader is part of core DuckDB — **no extension needed** for base functionality. Extended text encodings (beyond utf-8 / utf-16 / latin-1) require the **`encodings`** core extension (documented under Core Extensions; `INSTALL encodings; LOAD encodings;`).
- Version-gated behavior called out inline below (e.g. `store_rejects` / `reject_scans` + `reject_errors` two-table model landed in 0.10.0; `strict_mode` and multi-file `files_to_sniff` are newer 1.x additions).

See sibling `00_overview.md` for release lines/versioning and `09_extensions_system.md` for how to `INSTALL`/`LOAD` the `encodings` extension.

---

## 1. Entry points — the four ways to read a CSV

```sql
-- 1. read_csv table function (auto-detection ON by default)
SELECT * FROM read_csv('flights.csv');

-- 2. read_csv_auto — legacy alias for read_csv with auto_detect=true (see note below)
SELECT * FROM read_csv_auto('flights.csv');

-- 3. COPY … FROM — load into an EXISTING table using that table's schema
CREATE TABLE flights (flight_date DATE, carrier VARCHAR);
COPY flights FROM 'flights.csv' (HEADER);

-- 4. FROM 'file.csv' shorthand — replacement scan, infers read_csv from the .csv extension
SELECT * FROM 'flights.csv';
FROM 'flights.csv';               -- FROM-first syntax, same thing
```

- **`read_csv`** vs **`read_csv_auto`**: `read_csv_auto` is the historical name for "read with the sniffer on." Today `read_csv` already runs the sniffer by default (`auto_detect=true`), so `read_csv_auto('x')` ≡ `read_csv('x', auto_detect=true)`. Prefer `read_csv`. `read_csv_auto` remains supported.
- **Globs & lists**: the file argument accepts a glob (`read_csv('data/*.csv')`) or a list of paths (`read_csv(['a.csv','b.csv'])`). Multi-file reads combine positionally by default; use `union_by_name=true` to combine by column name instead.
- **`COPY … FROM`** does **not** auto-detect types from scratch — it casts incoming fields to the target table's declared column types. This is the most robust load path when you already own the schema (it sidesteps sniffer type mistakes). It still auto-detects *dialect* (delimiter/quote) unless you pass options.
- **Replacement scan** (`FROM 'file.csv'`): DuckDB rewrites a bare file-path table reference into the matching reader based on extension. See `01_python_client.md` for replacement scans generally.

---

## 2. Full option reference — `read_csv(...)` / `COPY … FROM`

All options are passed as named arguments to `read_csv(path, opt=val, …)`, or in the parenthesized clause of `COPY tbl FROM 'f.csv' (OPT val, …)`. Types and defaults below are quoted from the current DuckDB CSV overview page.

| Option | Type | Default | Accepted values / notes |
|---|---|---|---|
| `all_varchar` | `BOOL` | `false` | Skip type detection entirely; every column is `VARCHAR`. Defensive-ingest workhorse. |
| `allow_quoted_nulls` | `BOOL` | `true` | Allow quoted strings (e.g. `""`) to be converted to `NULL`. |
| `auto_detect` | `BOOL` | `true` | Master switch for the sniffer (dialect + types + header). |
| `auto_type_candidates` | `TYPE[]` | see §3 | Which types the sniffer may infer, in priority order. |
| `buffer_size` | `BIGINT` | `16 * max_line_size` | Read-buffer size in bytes. Rarely tuned. |
| `columns` | `STRUCT` | (empty) | Fully specify names→types, e.g. `columns = {'id': 'INTEGER', 'name': 'VARCHAR'}`. **Disables auto-detection.** |
| `comment` | `VARCHAR` | (empty) | Line-comment initiator character; lines starting with it are skipped. |
| `compression` | `VARCHAR` | `'auto'` | `'auto'`, `'none'`, `'gzip'`, `'zstd'`. `auto` infers from extension (`.gz`, `.zst`). |
| `dateformat` (COPY alias `date_format`) | `VARCHAR` | (empty) | `strptime`/`strftime` format used to parse DATE columns. |
| `decimal_separator` | `VARCHAR` | `'.'` | Character used as the decimal point when parsing numerics. |
| `delim` / `sep` (COPY alias `delimiter`) | `VARCHAR` | `','` | Column delimiter, up to 4 bytes (supports multi-byte delimiters). |
| `encoding` | `VARCHAR` | `'utf-8'` | Core: `'utf-8'`, `'utf-16'`, `'latin-1'`. More via the `encodings` extension (adds CP1252 and others). |
| `escape` | `VARCHAR` | `'"'` | String that escapes a quote inside a quoted field. |
| `filename` | `BOOL` \| `VARCHAR` | `false` | `true` adds a `filename` column; a string sets that column's name. |
| `files_to_sniff` | `BIGINT` | `10` | Number of files sampled for schema detection in a multi-file read; `-1` = all. |
| `force_not_null` | `VARCHAR[]` | `[]` | Columns whose values must never be interpreted as `NULL` (empty string stays empty string). |
| `header` | `BOOL` | (auto) | Whether the first line is a header. Sniffer decides by default; set `header=true` to force. |
| `hive_partitioning` | `BOOL` | (auto) | Interpret the path as Hive-partitioned (`key=value/` dirs become columns). |
| `hive_types` | `STRUCT` | (empty) | Explicit types for Hive partition columns. |
| `hive_types_autocast` | `BOOL` | `true` | Auto-cast Hive partition column values. |
| `ignore_errors` | `BOOL` | `false` | Skip rows that fail to parse/cast instead of erroring the whole scan (see §4). |
| `max_line_size` / `maximum_line_size` | `BIGINT` | `2000000` | Max bytes per line before `LINE SIZE OVER MAXIMUM` error. (Docs also cite `2097152` in the faulty-file page — see Unverified note.) |
| `names` / `column_names` | `VARCHAR[]` | (empty) | Explicit column-name list (does not disable type detection). |
| `new_line` | `VARCHAR` | (auto) | `'\r'`, `'\n'`, or `'\r\n'`. Forces the record separator. |
| `normalize_names` | `BOOL` | `false` | Normalize column names (strip to alphanumeric, dedupe) — useful for dirty headers. |
| `null_padding` | `BOOL` | `false` | Pad short rows with `NULL` for missing trailing columns instead of erroring. |
| `nullstr` / `null` | `VARCHAR` \| `VARCHAR[]` | (empty) | String(s) that represent `NULL`, e.g. `nullstr = ['NA','N/A','']`. |
| `parallel` | `BOOL` | `true` | Enable the parallel CSV reader. |
| `quote` | `VARCHAR` | `'"'` | Quote character. |
| `rejects_limit` | `BIGINT` | `0` | Max faulty rows recorded per scan; `0` = unlimited (see §4). |
| `rejects_scan` | `VARCHAR` | `'reject_scans'` | Name of the temp table capturing per-scan metadata (see §4). |
| `rejects_table` | `VARCHAR` | `'reject_errors'` | Name of the temp table capturing per-error rows (see §4). |
| `sample_size` | `BIGINT` | `20480` | Rows the sniffer inspects. `-1` = whole file. |
| `skip` | `BIGINT` | `0` | Number of lines to skip at the top of the file (prefix/junk rows). |
| `store_rejects` | `BOOL` | `false` | Skip faulty rows AND record them in the reject tables (implies `ignore_errors`; see §4). |
| `strict_mode` | `BOOL` | `true` | Enforce strict RFC-ish parsing; when `true`, structural issues error. Loosen for messy files. |
| `thousands` | `VARCHAR` | (empty) | Thousands-group separator to strip when parsing numerics (e.g. `','`). |
| `timestampformat` (COPY alias `timestamp_format`) | `VARCHAR` | (empty) | `strptime` format used to parse TIMESTAMP columns. |
| `types` / `dtypes` / `column_types` | `VARCHAR[]` \| `STRUCT` | (empty) | Override inferred types — positional list or a name→type struct, e.g. `types = {'FlightDate': 'DATE'}`. |
| `union_by_name` | `BOOL` | `false` | Multi-file: align columns by name (missing → `NULL`) instead of by position. |

Notes:
- `columns` is the "I know the schema exactly" option and turns off both dialect-agnostic type detection and header inference for those columns. `types`/`names` are the surgical overrides that keep the rest of the sniffer running.
- `sep`, `delim`, and (in COPY) `delimiter` are the same knob under three spellings. Likewise `nullstr`/`null`, and `types`/`dtypes`/`column_types`.

> **Unverified / needs confirmation:** the overview page states `max_line_size` default `2000000` while the faulty-CSV page describes the `LINE SIZE OVER MAXIMUM` limit as `2,097,152` bytes (2 MiB). These may reflect a nominal default vs. an internal buffer bound, or a doc-version drift. Treat `2000000` as the documented `max_line_size` default and set it explicitly if the boundary matters.

---

## 3. Auto-detection (the CSV sniffer)

`auto_detect=true` (the default) runs a three-stage sniffer:

1. **Dialect detection** — picks delimiter, quote, and escape from candidate sets:
   - Delimiters tried: `,` `|` `;` `\t`
   - Quotes tried: `"` `'` or none
   - Escapes tried: `"` `'` `\` or none
   - The chosen dialect is the one yielding a **consistent column count** at the **maximum** columns-per-row.
2. **Type detection** — for each column, tries candidate types **in priority order**, first one that parses all sampled values wins. `VARCHAR` is the guaranteed fallback (everything casts to it).
   - **Default `auto_type_candidates` (sniffer priority order), per the auto_detection page:** `NULL, BOOLEAN, TIME, DATE, TIMESTAMP, TIMESTAMPTZ, BIGINT, DOUBLE, VARCHAR`.
   - The overview page's parameter table lists the `auto_type_candidates` *default value* as `['NULL','BOOLEAN','BIGINT','DOUBLE','TIME','DATE','TIMESTAMP','VARCHAR']` (no `TIMESTAMPTZ`, different ordering). **These two official pages disagree** — see Unverified note.
   - Users may extend the candidate set with `auto_type_candidates` to include `TINYINT, SMALLINT, INTEGER, DECIMAL, FLOAT`, e.g. `auto_type_candidates = ['BIGINT','DATE','VARCHAR']`.
   - Set `all_varchar=true` to disable type detection completely.
3. **Header detection** — compares the first row's inferred types against the rest; if they differ, row 1 is treated as a header. **All-`VARCHAR` files default to having a header** unless overridden — a classic footgun when every column is a string (set `header=true`/`false` explicitly).

**Date/timestamp formats**: DuckDB defaults to ISO 8601 and attempts to detect alternatives, resolving ambiguity (e.g. `01-02-2000`) via preference lists (ISO 8601, then `%Y-%m-%d`, then `%d-%m-%Y` for dates; ISO 8601 then `%Y-%m-%d %H:%M:%S` for timestamps). Pin `dateformat`/`timestampformat` when the data is unambiguous but non-ISO.

**Sampling**: `sample_size` defaults to **20480** rows. For on-disk files the sampler reads from multiple locations in the file; for compressed/streamed input it can only sample from the start (so type mistakes are more likely on compressed input — raise `sample_size` or use `sample_size=-1`).

### `sniff_csv()` — inspect what the sniffer decided

```sql
FROM sniff_csv('flights.csv');
FROM sniff_csv('flights.csv', sample_size = 100000);
```

Returns a single row describing the detected dialect. Columns include: `Delimiter`, `Quote`, `Escape`, `NewLineDelimiter`, `Comment`, `SkipRows`, `HasHeader` (BOOL), `Columns` (LIST of name/type STRUCTs), `DateFormat`, `TimestampFormat`, `UserArguments`, and **`Prompt`** — a ready-to-paste `read_csv(...)` call with `auto_detect=false` and every detected option filled in. Copy the `Prompt` value to freeze a fast, deterministic reader for a recurring file.

> **Unverified / needs confirmation:** the exact default contents and ordering of `auto_type_candidates` differ between the two official pages (overview vs auto_detection), as noted above. For deterministic pipelines, do **not** rely on the default candidate list — pass `auto_type_candidates` explicitly or use `all_varchar` + `TRY_CAST` (§6).

---

## 4. Faulty / robust CSV handling

Two escalating tools:

### `ignore_errors = true`
Silently **skips** rows that fail structural parsing or type casting; the scan completes with the good rows. You lose all visibility into what was dropped.

```sql
SELECT * FROM read_csv('dirty.csv', ignore_errors = true);
```

### `store_rejects = true` — skip AND capture
Setting `store_rejects=true` skips faulty rows and records every rejected row into two temporary reject tables so you can audit/clean. The docs state verbatim: "any errors in the file will be skipped and stored in the default rejects temporary tables" — i.e. it skips like `ignore_errors` and additionally captures. Multiple errors on one line produce multiple `reject_errors` entries.

```sql
SELECT * FROM read_csv('dirty.csv', store_rejects = true);

-- what got rejected, and why:
SELECT * FROM reject_errors;
SELECT * FROM reject_scans;
```

Reject-table controls:

| Option | Default | Purpose |
|---|---|---|
| `store_rejects` | `false` | Enable capture (implies row-skipping). |
| `rejects_scan` | `'reject_scans'` | Temp-table name for per-scan metadata. |
| `rejects_table` | `'reject_errors'` | Temp-table name for per-error rows. |
| `rejects_limit` | `0` | Max faulty rows recorded per scan; `0` = unlimited. |

**`reject_scans` schema** (one row per CSV scan/configuration):

| Column | Type |
|---|---|
| `scan_id` | `UBIGINT` |
| `file_id` | `UBIGINT` |
| `file_path` | `VARCHAR` |
| `delimiter` | `VARCHAR` |
| `quote` | `VARCHAR` |
| `escape` | `VARCHAR` |
| `newline_delimiter` | `VARCHAR` |
| `skip_rows` | `UINTEGER` |
| `has_header` | `BOOLEAN` |
| `columns` | `VARCHAR` |
| `date_format` | `VARCHAR` |
| `timestamp_format` | `VARCHAR` |
| `user_arguments` | `VARCHAR` |

**`reject_errors` schema** (one row per rejected value/line; join back on `scan_id`,`file_id`):

| Column | Type |
|---|---|
| `scan_id` | `UBIGINT` |
| `file_id` | `UBIGINT` |
| `line` | `UBIGINT` |
| `line_byte_position` | `UBIGINT` |
| `byte_position` | `UBIGINT` |
| `column_idx` | `UBIGINT` |
| `column_name` | `VARCHAR` |
| `error_type` | `ENUM` |
| `csv_line` | `VARCHAR` |
| `error_message` | `VARCHAR` |

**`error_type` values** (structural error classification):
- `CAST` — value could not be cast to the column type (e.g. non-date in a DATE column).
- `MISSING COLUMNS` — row has fewer columns than the schema.
- `TOO MANY COLUMNS` — row has more columns than the schema.
- `UNQUOTED VALUE` — improperly terminated quoted field.
- `LINE SIZE OVER MAXIMUM` — line exceeds `max_line_size`.
- `INVALID ENCODING` — bytes not valid for the declared encoding (utf-8/utf-16/latin-1).

**Footgun — projection pushdown hides errors:** the CSV parser is subject to projection pushdown. If you `SELECT` only some columns, cast/parse errors in **unselected** columns are never triggered, so they won't appear in the reject tables. To audit a file exhaustively, select all columns (`SELECT *` / `SELECT COUNT(*)` over `read_csv(..., store_rejects=true)`).

---

## 5. `COPY … TO` — writing CSV output

```sql
COPY (SELECT * FROM tbl) TO 'out.csv' (HEADER, DELIMITER ',');
COPY tbl TO 'out.csv.gz' (FORMAT csv, COMPRESSION gzip);
```

**CSV-specific write options:**

| Option | Type | Default | Notes |
|---|---|---|---|
| `FORMAT` | — | (from extension) | Set `csv` explicitly if the extension is ambiguous. |
| `HEADER` | `BOOL` | `true` | Write a header row. (Note: default `true` on write, sniffed on read.) |
| `DELIMITER` / `DELIM` / `SEP` | `VARCHAR` | `,` | Column separator. |
| `QUOTE` | `VARCHAR` | `"` | Quote character. |
| `ESCAPE` | `VARCHAR` | `"` | Quote-escape character. |
| `NULLSTR` / `NULL` | `VARCHAR` | (empty) | String written for `NULL`. |
| `DATEFORMAT` | `VARCHAR` | (empty) | Output DATE format. |
| `TIMESTAMPFORMAT` | `VARCHAR` | (empty) | Output TIMESTAMP format. |
| `FORCE_QUOTE` | `VARCHAR[]` | `[]` | Columns always quoted. |
| `NEW_LINE` | `VARCHAR` | `\n` | Row separator (use escaped strings). |
| `COMPRESSION` | `VARCHAR` | `auto` | `none`, `gzip`, `zstd`. |
| `PREFIX` | `VARCHAR` | (empty) | Emitted before rows; requires `SUFFIX` and `HEADER false`. |
| `SUFFIX` | `VARCHAR` | (empty) | Emitted after rows; requires `PREFIX` and `HEADER false`. |

**General `COPY … TO` options (all formats):** `USE_TMP_FILE`, `OVERWRITE_OR_IGNORE`, `OVERWRITE`, `APPEND`, `FILENAME_PATTERN` (supports `{uuid}`,`{uuidv4}`,`{uuidv7}`,`{i}`), `FILE_EXTENSION`, `PER_THREAD_OUTPUT` (default `false`), `FILE_SIZE_BYTES`, `PARTITION_BY`, `PRESERVE_ORDER`, `RETURN_FILES` (default `false`), `RETURN_STATS` (default `false`), `WRITE_PARTITION_COLUMNS` (default `false`).

```sql
-- Partitioned write, one directory tree, one file per partition thread
COPY tbl TO 'export/' (FORMAT csv, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE);
```

For Parquet output prefer `04_parquet.md`; CSV is transport-only in the core-x plane.

---

## 6. Defensive ingest — robust government CSV with `all_varchar` + `TRY_CAST`

Government / third-party CSVs routinely mix blank cells, `N/A` sentinels, thousands separators, mixed date formats, and stray non-UTF-8 bytes. The robust pattern: **read everything as text, then cast in SQL where you control failure semantics.** `TRY_CAST` returns `NULL` on failure instead of erroring the whole load (see `12_sql_essentials.md`).

```sql
-- Stage 1: read raw, no type inference, no whole-file failure on a bad cell
CREATE OR REPLACE TABLE raw_awards AS
SELECT *
FROM read_csv(
    's3://landing/awards_2026.csv',
    all_varchar   = true,          -- every column is VARCHAR; zero cast-time surprises
    header        = true,          -- force header (all-VARCHAR files mis-sniff this)
    nullstr       = ['', 'NA', 'N/A', 'NULL', '-'],
    normalize_names = true,        -- clean dirty header tokens into usable identifiers
    strict_mode   = false,         -- tolerate loose quoting/spacing
    store_rejects = true,          -- capture anything still unreadable
    encoding      = 'latin-1'      -- many gov exports are CP-1252/latin-1, not utf-8
);

-- Inspect what (if anything) was dropped at read time
SELECT error_type, count(*) FROM reject_errors GROUP BY 1;

-- Stage 2: typed projection with TRY_CAST — bad values become NULL, load never aborts
CREATE OR REPLACE TABLE awards AS
SELECT
    TRY_CAST(award_id AS BIGINT)                              AS award_id,
    recipient_name                                            AS recipient_name,
    TRY_CAST(replace(obligated_amount, ',', '') AS DOUBLE)   AS obligated_amount,
    TRY_CAST(strptime(action_date, '%m/%d/%Y') AS DATE)      AS action_date,
    TRY_CAST(fiscal_year AS INTEGER)                          AS fiscal_year
FROM raw_awards;

-- Audit rows where a critical field failed to cast (would-be silent NULLs)
SELECT count(*) AS unparseable_amounts
FROM raw_awards
WHERE obligated_amount IS NOT NULL
  AND TRY_CAST(replace(obligated_amount, ',', '') AS DOUBLE) IS NULL;
```

Why two stages beat letting the sniffer type the file directly:
- The sniffer only inspects `sample_size` rows; a bad value in row 500,000 can still abort a natively-typed load. `all_varchar` guarantees the read never fails on type.
- `TRY_CAST` moves cast failures into inspectable `NULL`s you can count and quarantine, rather than an opaque skipped-row count.
- You keep the original text (`raw_awards`) for forensic re-parsing.

### Encoding pitfalls
- Core supports only `utf-8` (default), `utf-16`, `latin-1`. A file that is actually Windows-1252 will read *mostly* fine as `latin-1` but corrupt smart-quotes/dashes/€. For correct CP1252 (and other encodings), `INSTALL encodings; LOAD encodings;` then pass `encoding='cp1252'` (see `09_extensions_system.md`).
- Reading UTF-8 data that contains a BOM is handled; but declaring the wrong `encoding` yields `INVALID ENCODING` rejects (with `store_rejects`) or an aborted scan.
- A silent partial-corruption (latin-1 read of CP1252) does **not** raise `INVALID ENCODING` because every byte is valid latin-1 — it just produces wrong characters. When mojibake appears in output, suspect the encoding, not the delimiter.

> Relevance to core-x: CSVs are transport-only in this plane — raw ephemeral input streamed through the DuckDB worker, never the system of record (Lance under `s3://data-sink/active/` is). The load-bearing move for messy upstream/government drops is `all_varchar` read → `TRY_CAST` projection → DuckDB streams typed Arrow directly into Lance (`to_arrow_reader` → `lance.write_dataset`, zero-copy; see `02_arrow_integration.md` and `13_lance_interop.md`). Out-of-core reads of hundreds-of-millions-of-row CSVs are bounded by `memory_limit` + `temp_directory` spill (`06_configuration_memory_spill.md`), and object-store paths (`s3://…`, R2) route through `httpfs` with R2 `storage_options`/secrets (`07_httpfs_s3_r2.md`, `08_secrets_manager.md`). Pin `encoding` explicitly on every gov ingest — a wrong-encoding read corrupts resolution keys before they ever reach the BTREE-indexed Lance columns.

---

## 7. Common footguns (quick list)

- **All-VARCHAR file → header mis-detected.** Set `header=true`/`false` explicitly.
- **Type mistakes from small sample** (esp. compressed input, which samples only from the start). Raise `sample_size` or use `sample_size=-1`, or `all_varchar`.
- **`ignore_errors` hides data loss.** Use `store_rejects=true` and audit `reject_errors` instead.
- **Projection pushdown hides cast errors** in unselected columns — audit with `SELECT *`.
- **`COPY … FROM` uses the target table schema**, not the sniffer's types — great for control, but a type mismatch surfaces as a cast error at load, not a sniff warning.
- **Encoding drift**: `HEADER` defaults to `true` on write but is *sniffed* on read; and `latin-1` silently mis-renders CP1252.
- **`read_csv_auto` is legacy** — use `read_csv` (auto-detect is already on).

---

## Related files
- `00_overview.md` — DuckDB editions, clients, versioning & release lines.
- `01_python_client.md` — Python `connect`/`execute`, relational API, replacement scans.
- `02_arrow_integration.md` — zero-copy Arrow (`to_arrow_reader`/`from_arrow`) — the CSV→Lance bridge.
- `04_parquet.md` — Parquet read/write/pushdown (preferred columnar transport).
- `05_json.md` — JSON reading and nested casting.
- `06_configuration_memory_spill.md` — `memory_limit`/`temp_directory` for out-of-core CSV reads.
- `07_httpfs_s3_r2.md` — reading CSV directly from S3/R2 object storage.
- `08_secrets_manager.md` — credentials for object-store CSV paths.
- `09_extensions_system.md` — installing the `encodings` extension for CP1252 et al.
- `12_sql_essentials.md` — `TRY_CAST`, `strptime`, types used in the defensive projection.
- `13_lance_interop.md` — writing the typed result into Lance.
