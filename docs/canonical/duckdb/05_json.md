# JSON — read_json/read_json_auto, formats, JSON functions, nested casting

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/current/data/json/overview — JSON extension overview, JSONPath vs JSON Pointer extraction, indexing conventions.
> - https://duckdb.org/docs/current/data/json/loading_json — `read_json` / `read_json_auto` full parameter table, `format`/`records` semantics.
> - https://duckdb.org/docs/current/data/json/json_type — the `JSON` logical type (physically `VARCHAR`), whitespace/key-order equality, `::JSON::STRUCT` casting.
> - https://duckdb.org/docs/current/data/json/json_functions — extraction/scalar/transform/aggregate/table functions.
> - https://duckdb.org/docs/current/data/json/creating_json — `to_json`, `json_quote`, `json_object`, `json_array`, `json_merge_patch`.
> - https://pypi.org/project/duckdb/ — current released `duckdb` Python package version.

Scope: How DuckDB reads JSON/NDJSON files (`read_json`/`read_json_auto` and every option), the full JSON function/operator surface, the `JSON` logical type and its physical `VARCHAR` representation, and the casting of `JSON` to native nested types (`STRUCT`/`LIST`/`MAP`) required before exporting to Arrow/Lance.

---

## Version ground truth (as of 2026-07-08)

- **DuckDB current stable / `duckdb` PyPI package: `1.5.4`** (released 2026-06-17). The 1.5.x line is codenamed "Variegata" (1.5.0 shipped 2026-03-09). The 1.4.x line ("Andium") is the current LTS.
- The `json` extension is a **core (built-in) extension shipped with virtually all DuckDB distributions** and is **auto-loaded on first use** — you do not normally need to `INSTALL json` / `LOAD json`. It provides the `JSON` logical type, the reader table functions, and all the functions below.

---

## 1. The `JSON` logical type

- The `JSON` logical type is **physically stored as `VARCHAR`** (text). Quoting the type page: "Physically, the data is stored as a `VARCHAR`." It is a semantic/validation wrapper over a string, not a shredded binary column type. This is the single most important fact for downstream Arrow/Lance work — see §7.
- **Values must be valid JSON.** Casting malformed text raises `Conversion Error: Malformed JSON at byte 0 of input: unexpected character.`
- **Whitespace is significant in equality.** `'{ "a": 5 }'::JSON` is NOT equal to `'{"a":5}'::JSON`.
- **Key order is significant in equality.** `'{"a":1,"b":2}'::JSON` is NOT equal to `'{"b":2,"a":1}'::JSON`.
- **Duplicate object keys are permitted and preserved.**
- **Indexing is 0-based for the JSON type** (unlike DuckDB `ARRAY`/`LIST`, which are 1-based, following PostgreSQL). JSONPath additionally supports negative indexing via `[#-1]` for the last element.

### Casting to/from native nested types

Casting is bidirectional and works for nested and scalar types alike:

```sql
-- JSON  -> native STRUCT
SELECT '{"duck": 42}'::JSON::STRUCT(duck INTEGER);   -- {'duck': 42}

-- native STRUCT -> JSON
SELECT {duck: 42}::JSON;                              -- {"duck":42}
```

The `::JSON::STRUCT(...)` chained-cast pattern (VARCHAR/JSON text → validated JSON → shredded native STRUCT) is the canonical way to promote a text/JSON column into a real nested column. See §6 and §7.

---

## 2. Reading JSON files: `read_json` / `read_json_auto`

`read_json(filename, ...)` and `read_json_auto(filename, ...)` are **aliases for the same table function.** Both auto-detect format, key names, and value types by default (`auto_detect = true`). `filename` may be a single path, a glob (`'dir/*.json'`), or a `LIST` of paths. Remote paths (`s3://`, `r2://`, `https://`) work when the `httpfs` extension is loaded (see `07_httpfs_s3_r2.md`).

```sql
-- Newline-delimited JSON (NDJSON), one object per line, from R2/S3:
SELECT * FROM read_json('s3://bucket/events/*.ndjson', format = 'newline_delimited');

-- Explicit column schema (skips type inference, most robust for pipelines):
SELECT * FROM read_json(
    's3://bucket/events/*.ndjson',
    format  = 'newline_delimited',
    columns = {id: 'BIGINT', ts: 'TIMESTAMP', payload: 'JSON'}
);
```

There is also a `COPY ... FROM ... (FORMAT json)` form and the `read_ndjson` / `read_ndjson_auto` convenience aliases (equivalent to `read_json(..., format = 'newline_delimited')`).

### 2.1 Full parameter table (verbatim from `loading_json`)

| Parameter | Type | Default | Accepted values / notes |
|---|---|---|---|
| `auto_detect` | `BOOL` | `true` | Whether to auto-detect the names of the keys and data types of the values automatically. When `false`, you must supply `columns`. |
| `columns` | `STRUCT` | *(empty)* | Explicit key names and value types, e.g. `{key1: 'INTEGER', key2: 'VARCHAR'}`. Setting this disables type inference for those keys. |
| `compression` | `VARCHAR` | `'auto_detect'` | `none`, `gzip`, `zstd`, `auto_detect`. Detected from file extension by default. (Upstream `loading_json` lists the literal keyword as `auto_detect`.) |
| `dateformat` | `VARCHAR` | `'iso'` | strptime-style format used when parsing `DATE` values. |
| `field_appearance_threshold` | `DOUBLE` | `0.1` | During auto-detection, fraction of records a field must appear in before it is treated as a `STRUCT` field rather than folded into a `MAP`. |
| `filename` | `BOOL` \| `VARCHAR` | `false` | Add a column with the source file path of each row. Passing a `VARCHAR` names the column. |
| `format` | `VARCHAR` | `'array'` | `auto`, `unstructured`, `newline_delimited`, `array`. See §2.2. |
| `hive_partitioning` | `BOOL` | *(auto-detected)* | Interpret the path as a Hive-partitioned path (`key=value/` directories become columns). |
| `ignore_errors` | `BOOL` | `false` | Ignore parse errors. **Only possible when `format = 'newline_delimited'`** — a malformed line is skipped instead of aborting the scan. |
| `map_inference_threshold` | `BIGINT` | `200` | Threshold on the number of distinct keys above which an object is inferred as a `MAP(VARCHAR, ...)` instead of a wide `STRUCT`. Set to `-1` to disable MAP inference (always produce STRUCT). |
| `maximum_depth` | `BIGINT` | `-1` | Maximum nesting depth to which automatic schema detection descends. `-1` fully detects nested JSON types. |
| `maximum_object_size` | `UINTEGER` | `16777216` | Maximum size of a single JSON object, in bytes (16 MiB default). Objects larger than this fail to parse. |
| `records` | `VARCHAR` | `'auto'` | `auto`, `true`, `false`. See §2.3. |
| `sample_size` | `UBIGINT` | `20480` | Number of sample objects used for automatic type detection. Set to `-1` to scan the entire input for inference. |
| `timestampformat` | `VARCHAR` | `'iso'` | strptime-style format used when parsing `TIMESTAMP` values. |
| `union_by_name` | `BOOL` | `false` | Unify schemas across multiple JSON files by column name (rather than requiring identical schemas / positional union). |

> Footgun: the **default `format` is `array`, not `auto`.** If you point `read_json` at an NDJSON file without setting `format`, the default expects a single top-level JSON array and can misparse. For NDJSON/line-delimited data always pass `format = 'newline_delimited'` (or use `read_ndjson`).

### 2.2 The `format` option

| Value | Meaning |
|---|---|
| `auto` | Automatically detect which of the three physical layouts below the file uses. |
| `newline_delimited` | NDJSON — exactly one JSON value per line (`\n`-separated). This is the only format for which `ignore_errors` works. |
| `array` | The file is a single JSON array `[ {...}, {...}, ... ]`; each element becomes a row. **This is the default.** |
| `unstructured` | Top-level JSON objects separated by whitespace (no array wrapper, not necessarily one-per-line). Each top-level value becomes a row. |

### 2.3 The `records` option

Controls how each top-level JSON value maps to output columns:

| Value | Meaning |
|---|---|
| `auto` | Detect whether values are objects (unpack into columns) or non-objects (single column). **Default.** |
| `true` | Each JSON value is a record (object); its keys become the output columns. |
| `false` | Each JSON value is emitted as a single column of type `JSON` (no unpacking). Use this to keep the raw value intact. |

### 2.4 `columns` vs auto-detection

- With `auto_detect = true` (default), DuckDB samples `sample_size` objects and infers a `STRUCT` schema. Fields appearing in fewer than `field_appearance_threshold` of records, or objects with more than `map_inference_threshold` distinct keys, may be inferred as `MAP` instead of `STRUCT`.
- With `columns = {...}` you pin the exact schema and value types. This is the recommended mode for production pipelines — it is deterministic, avoids full-file sampling cost, and lets you declare a value column as `'JSON'` to defer parsing.

---

## 3. Extraction functions and operators

DuckDB exposes two path dialects, both usable with `->` and with `json_extract`:

- **JSON Pointer** (RFC 6901): slash-separated, e.g. `'/duck/0'`.
- **JSONPath**: `$`-rooted with `.key` and `[index]`, e.g. `'$.duck[0]'`; supports negative indexing `[#-1]` (last element) and wildcards. DuckDB does not implement full JSONPath — it covers member/element lookup and wildcards, deferring richer transforms to SQL.

Remember: **JSON indexing is 0-based.**

| Function | Alias | Operator | Description (verbatim) |
|---|---|---|---|
| `json_exists(json, path)` | — | — | "Returns true if the supplied path exists in the json, and false otherwise." |
| `json_extract(json, path)` | `json_extract_path` | `->` | "Extracts JSON from json at the given path. If path is a LIST, the result will be a LIST of JSON." |
| `json_extract_string(json, path)` | `json_extract_path_text` | `->>` | "Extracts VARCHAR from json at the given path. If path is a LIST, the result will be a LIST of VARCHAR." |
| `json_value(json, path)` | — | — | "Extracts JSON from json at the given path. If the json at the supplied path is not a scalar value, it will return NULL." |

- `->` returns `JSON` (still text-typed). `->>` returns `VARCHAR` (already unwrapped/unquoted) — prefer `->>` / `json_extract_string` when the target is a scalar you want as a plain string.
- Passing a `LIST` of paths returns a `LIST` of extracted values in one call.

```sql
SELECT
    j ->> '$.name'          AS name,      -- VARCHAR
    j ->  '$.tags'          AS tags_json, -- JSON
    json_extract(j, '$.address.city') AS city,
    json_extract_string(j, '/scores/0') AS first_score   -- JSON Pointer, 0-based
FROM (SELECT '{"name":"a","tags":["x","y"],"address":{"city":"NYC"},"scores":[9,8]}'::JSON AS j);
```

---

## 4. Scalar / introspection functions

| Function | Description (verbatim) |
|---|---|
| `json_array_length(json[, path])` | "Return the number of elements in the JSON array json, or 0 if it is not a JSON array." |
| `json_contains(json_haystack, json_needle)` | "Returns true if json_needle is contained in json_haystack." |
| `json_keys(json[, path])` | "Returns the keys of json as a LIST of VARCHAR, if json is a JSON object." |
| `json_structure(json)` | "Return the structure of json. Defaults to JSON if the structure is inconsistent." |
| `json_type(json[, path])` | "Return the type of the supplied json, which is one of ARRAY, BIGINT, BOOLEAN, DOUBLE, OBJECT, UBIGINT, VARCHAR and NULL." |
| `json_valid(json)` | "Return whether json is valid JSON." |
| `json(json)` | "Parse and minify json." |

- `json_structure` produces the schema string that `json_transform` / `from_json` consume (see §5). It returns `"JSON"` for any subtree whose element types are inconsistent.
- `json_valid` is the safe gate before casting: `WHERE json_valid(raw)` filters out unparseable rows without aborting.

---

## 5. Transformation to native types: `json_transform` / `from_json`

These convert a `JSON` value into a native DuckDB nested value (`STRUCT`/`LIST`/`MAP`/scalars) driven by a **structure** argument (the same shape `json_structure` emits).

| Function | Description (verbatim) |
|---|---|
| `json_transform(json, structure)` | "Transform json according to the specified structure." |
| `from_json(json, structure)` | "Alias for json_transform." |
| `json_transform_strict(json, structure)` | "Same as json_transform, but throws an error when type casting fails." |
| `from_json_strict(json, structure)` | "Alias for json_transform_strict." |

- **`json_transform` / `from_json` are lenient**: a value that fails to cast to the requested type becomes `NULL`.
- **`json_transform_strict` / `from_json_strict` are strict**: a failed cast raises an error. Use strict in pipelines where a type mismatch must not be silently nulled.

```sql
-- Extract-and-type in one pass using a structure literal:
SELECT from_json(
    '{"id": 7, "coords": [1.5, 2.5], "meta": {"ok": true}}'::JSON,
    '{"id": "BIGINT", "coords": ["DOUBLE"], "meta": {"ok": "BOOLEAN"}}'
);
-- -> {'id': 7, 'coords': [1.5, 2.5], 'meta': {'ok': true}}
```

The chained-cast form `col::JSON::STRUCT(...)` (§1) is usually more ergonomic than a structure literal when the target type is known; `from_json` shines when the structure itself is data-driven or when you want lenient nulling.

---

## 6. Table (unnest-style) functions

- `json_each(json[, path])` — traverses the **top-level** members/elements at `path`.
- `json_tree(json[, path])` — **depth-first** traversal of the entire subtree.

Both return rows with columns: `key`, `value`, `type`, `atom`, `id`, `parent`, `fullkey`, `path`. They are typically used as lateral joins.

For native nested columns produced by casting, use SQL `unnest` directly (see `12_sql_essentials.md`):

```sql
-- Explode a JSON array into rows via native LIST + unnest:
SELECT unnest('["a","b","c"]'::JSON::VARCHAR[]) AS tag;
```

---

## 7. Casting `JSON` -> `STRUCT` / `LIST` / `MAP` (the load-bearing pattern)

Because the `JSON` type is **physically `VARCHAR`**, a column typed `JSON` (or plain `VARCHAR`) exports to Arrow/Parquet/Lance as a **string column**, not as a nested Arrow type. To get real nested Arrow output you must cast to a native nested type first.

```sql
-- text/JSON -> native nested types
SELECT '["a","b"]'::JSON::VARCHAR[]                       AS list_col;   -- LIST<VARCHAR>
SELECT '{"a":1,"b":2}'::JSON::MAP(VARCHAR, INTEGER)       AS map_col;    -- MAP
SELECT '{"id":1,"name":"x"}'::JSON::STRUCT(id INTEGER, name VARCHAR) AS struct_col; -- STRUCT
```

These native types map to Arrow's nested types on export (see `02_arrow_integration.md`):

| DuckDB type | Arrow type |
|---|---|
| `STRUCT(...)` | `Struct` |
| `LIST` / `T[]` | `List` |
| `MAP(K,V)` | `Map` |
| `JSON` (uncast) | `Utf8` / `LargeUtf8` (a **string**, not nested) |

### 7.1 Creating JSON (native value -> JSON text)

| Function | Description (verbatim) |
|---|---|
| `to_json(any)` | "Create JSON from a value of any type. Our LIST is converted to a JSON array, and our STRUCT and MAP are converted to a JSON object." |
| `json_quote(any)` | "Alias for to_json." |
| `array_to_json(list)` | "Alias for to_json that only accepts LIST." |
| `row_to_json(list)` | "Alias for to_json that only accepts STRUCT." |
| `json_array(any, ...)` | "Create a JSON array from the values in the argument lists." |
| `json_object(key, value, ...)` | "Create a JSON object from key, value pairs in the argument list. Requires an even number of arguments." |
| `json_merge_patch(json, json)` | "Merge two JSON documents together." (Second argument's values take precedence.) |

### 7.2 Aggregate functions

| Function | Description (verbatim) |
|---|---|
| `json_group_array(any)` | "Return a JSON array with all values of any in the aggregation." |
| `json_group_object(key, value)` | "Return a JSON object with all key, value pairs in the aggregation." |
| `json_group_structure(json)` | "Return the combined json_structure of all json in the aggregation." |

`json_group_structure` over a sample is a practical way to derive a structure string to feed `from_json` when the schema is unknown up front.

---

## 8. Worked example: NDJSON on R2 → native STRUCT → nested Arrow

```python
import duckdb, pyarrow as pa

con = duckdb.connect()
# httpfs + an R2 secret assumed configured; see 07_httpfs_s3_r2.md / 08_secrets_manager.md.

rel = con.sql("""
    SELECT
        id::BIGINT                                              AS id,
        ts::TIMESTAMP                                           AS ts,
        -- promote the raw JSON payload into a real nested STRUCT (NOT a text column):
        payload::JSON::STRUCT(
            actor VARCHAR,
            tags  VARCHAR[],
            attrs MAP(VARCHAR, VARCHAR)
        )                                                        AS payload
    FROM read_json(
        's3://data-sink/raw/events/*.ndjson',
        format  = 'newline_delimited',
        columns = {id: 'BIGINT', ts: 'TIMESTAMP', payload: 'JSON'},
        ignore_errors = true          -- valid only for newline_delimited
    )
""")

# Stream to Arrow with nested Struct/List/Map types preserved (zero-copy):
reader: pa.RecordBatchReader = rel.to_arrow_reader(batch_size=100_000)
# ... hand `reader` to lance.write_dataset(...) — see 13_lance_interop.md
# NOTE: use to_arrow_reader (not the deprecated fetch_arrow_reader) — see 02_arrow_integration.md §1.3.
```

The `payload` column arrives in Arrow as an Arrow `Struct` containing a `List<Utf8>` (`tags`) and a `Map<Utf8,Utf8>` (`attrs`) — queryable/indexable in Lance — rather than a single opaque JSON string.

---

## 9. Deprecations, renames, footguns

- **`format` default is `array`, not `auto`.** NDJSON without `format = 'newline_delimited'` misparses. (See §2.1.)
- **`ignore_errors` only works with `format = 'newline_delimited'`.** Setting it under `array`/`unstructured` has no effect on parse errors — those still abort the scan.
- **JSON indexing is 0-based**, but DuckDB `LIST`/`ARRAY` indexing is 1-based. Mixing `json_extract(j,'$.a[0]')` with `list[1]` in the same query is a common off-by-one.
- **`JSON` is text.** A `JSON`/`VARCHAR` column is a string on export — it does not become a nested Arrow/Parquet/Lance type. Cast to `STRUCT`/`LIST`/`MAP` first (§7).
- **`to_json` is not a parse.** `to_json`/`json_quote` serialize a native value *into* JSON text; to go the other way use casts or `from_json`/`json_transform`.
- **`from_json` (lenient) silently nulls bad casts.** Use `from_json_strict` / `json_transform_strict` when a mismatch must fail loudly.
- **`sample_size` (default 20480) can under-sample wide/heterogeneous files**, mis-inferring a field as absent or as `JSON`. Pin `columns` or set `sample_size = -1` for full-file inference.
- **`map_inference_threshold` (default 200)** silently converts wide objects to `MAP` instead of `STRUCT`. Set to `-1` to force `STRUCT`, or pin `columns`.

---

> Relevance to core-x: The Gen-3 system of record is LanceDB nested types on R2, never opaque text blobs. Any JSON payload landing through DuckDB must be promoted to native `STRUCT`/`LIST`/`MAP` via `payload::JSON::STRUCT(...)` (or `from_json_strict`) **before** it reaches Arrow — a `JSON`/`VARCHAR` column exports as an Arrow `Utf8` string and defeats columnar pushdown and any `BTREE` scalar index on a resolution key extracted from it. Extract and hard-type resolution keys out of the JSON (`payload ->> '$.id'` then `::BIGINT`) into their own columns so they can carry `BTREE` indices. Prefer explicit `columns = {...}` over auto-detection for deterministic, sampling-free schemas at hundreds-of-millions-of-rows scale; use `ignore_errors = true` only with `format = 'newline_delimited'`. Cast → native → zero-copy Arrow → `lance.write_dataset` keeps the pipeline out-of-core (pair with `memory_limit`/`temp_directory` from `06_configuration_memory_spill.md`).

---

## 10. Cross-links

- `00_overview.md` — DuckDB editions, clients, versioning & release lines.
- `01_python_client.md` — `connect`, `execute`, relational API, replacement scans.
- `02_arrow_integration.md` — `to_arrow_table`/`to_arrow_reader`, nested-type mapping, ADBC.
- `03_csv_import.md` — `read_csv` (the `all_varchar`/`sample_size`/`ignore_errors`/`rejects` analogues).
- `04_parquet.md` — `read_parquet`, `COPY TO`, partitioning, pushdown.
- `06_configuration_memory_spill.md` — `memory_limit`, `temp_directory`, out-of-core spilling.
- `07_httpfs_s3_r2.md` — reading JSON directly off S3/Cloudflare R2.
- `08_secrets_manager.md` — `CREATE SECRET` for R2/S3 credentials.
- `12_sql_essentials.md` — `TRY_CAST`, `STRUCT`/`LIST`/`MAP`/`VARIANT`, `unnest`.
- `13_lance_interop.md` — writing the resulting Arrow to Lance.

---

## 11. Unverified / needs confirmation

- **`json_pretty` / `json_merge_patch` presence in the operators table**: `json_merge_patch(json, json)` is confirmed on the *creating_json* page. A dedicated `json_pretty` function was **not** confirmed in the fetched function tables — do not assume it exists without checking the live `json_functions` page.
- **`compression` default label**: confirmed against the live `loading_json` page (2026-07-08) — the literal keyword is `auto_detect` (accepted values: `none`, `gzip`, `zstd`, `auto_detect`), corrected in §2.1. Behavior is auto-detection from the file extension.
- **`filename` type**: documented as `BOOL` default `false`; the string-column-naming variant (`filename = 'source_file'`) follows the same convention as `read_csv`/`read_parquet` but was not explicitly restated on the JSON loading page. Confirm if you rely on the named-column form.
- **`json_execute_serialized_sql` / `json_serialize_sql`**: these live on the *sql_to_and_from_json* page and concern SQL-statement (de)serialization, not value conversion — out of scope for data loading; noted here only to prevent confusion with `to_json`.
