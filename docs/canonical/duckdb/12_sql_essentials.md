# SQL Essentials for Pipelines — TRY_CAST, types (STRUCT/LIST/MAP/VARIANT), QUALIFY, window

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/current/sql/expressions/cast — CAST, TRY_CAST, `::` shorthand, casting rules, `cast_to_type`
> - https://duckdb.org/docs/current/sql/data_types/overview — the full built-in type table (primitives + nested + VARIANT + BIGNUM)
> - https://duckdb.org/docs/current/sql/data_types/struct — STRUCT construction/access, `struct_pack`, `{...}`, `row()`, `struct_update`, struct-field DDL
> - https://duckdb.org/docs/current/sql/data_types/map — MAP construction/access, `map()`, `MAP {...}`, `map_extract`, `map_from_entries`
> - https://duckdb.org/docs/current/sql/data_types/variant — the native VARIANT type, `::VARIANT`, `variant_typeof`, `variant_extract`
> - https://duckdb.org/docs/current/sql/query_syntax/qualify — QUALIFY (window-result filter), clause position, WINDOW-alias form
> - https://duckdb.org/docs/current/sql/query_syntax/unnest — UNNEST of LIST/STRUCT, `recursive`, `max_depth`, `keep_parent_names`
> - https://duckdb.org/docs/current/sql/functions/list — `list_value`/`[...]`, 1-based indexing, slicing, list comprehensions
> - https://duckdb.org/docs/current/sql/functions/text — `trim`/`ltrim`/`rtrim`, `replace`, `regexp_replace`, `lower`/`upper`, `strip_accents`
> - https://duckdb.org/docs/current/sql/functions/lambda — current `lambda x: ...` syntax; deprecated `x -> ...` arrow syntax
> - https://duckdb.org/docs/current/sql/dialect/friendly_sql — `EXCLUDE`/`REPLACE`, `COLUMNS()`, `GROUP BY ALL`, `ORDER BY ALL`, PIVOT/UNPIVOT, `FILTER`
> - https://duckdb.org/docs/current/sql/samples — `USING SAMPLE` (reservoir/system/bernoulli, `REPEATABLE`)
> - https://duckdb.org/2026/03/09/announcing-duckdb-150 — VARIANT GA in v1.5.0, GEOMETRY built-in, lambda-syntax change

Scope: The SQL surface a pipeline engineer actually reaches for when moving messy source rows through DuckDB into Arrow/Lance — safe casting (`TRY_CAST`), the primitive + nested type system (STRUCT/LIST/ARRAY/MAP/UNION/VARIANT), value construction/access/UNNEST, the "friendly SQL" clauses (QUALIFY, `* EXCLUDE/REPLACE`, `GROUP BY ALL`, `USING SAMPLE`, `FILTER`, PIVOT/UNPIVOT), and string/number cleaning idioms — with runnable examples.

---

## Version ground truth (as of 2026-07-08)

| Item | Value | Note |
|------|-------|------|
| Latest stable (non-LTS) | **DuckDB 1.5.4** ("Variegata"), released 2026-06-17 | The `/docs/current/` docs track the 1.5.x line. |
| Latest LTS | **DuckDB 1.4.5** ("Andium"), released 2026-06-17 | Long-term-support line; VARIANT is *experimental* here. |
| VARIANT type | **GA in 1.5.0** (2026-03-09); *experimental* since 1.4.0 | See [VARIANT](#variant--native-semi-structured-type) below. |
| Lambda arrow syntax `x -> ...` | **Deprecated** since v1.3; v1.5 is the **last** release supporting it without an explicit opt-in; **v2.0 disables it by default** | Use `lambda x: ...` in all new code. |
| Struct-field DDL (`ALTER TABLE ... ADD/DROP/RENAME COLUMN s.k`) | v1.3.0+ | See [STRUCT DDL](#altering-struct-fields-v130). |
| GEOMETRY as a built-in (not extension) | v1.5.0 | Out of scope here; noted for completeness. |

> Docs-URL note: `duckdb.org/docs/stable/...` 302-redirects to `duckdb.org/docs/current/...html`. If a `/stable/` fetch returns only "Redirecting…", refetch the `/docs/current/<path>.html` target. All URLs in this file are the resolved `/docs/current/` form.

---

## 1. Casting: `CAST`, `TRY_CAST`, and `::`

Source: https://duckdb.org/docs/current/sql/expressions/cast

DuckDB gives you two **equivalent** explicit-cast syntaxes plus a NULL-safe variant.

| Form | Syntax | On conversion failure |
|------|--------|-----------------------|
| Standard SQL cast | `CAST(expr AS TYPENAME)` | **Throws** a Conversion Error |
| PostgreSQL shorthand | `expr::TYPENAME` | **Throws** a Conversion Error (identical to `CAST`) |
| Safe cast | `TRY_CAST(expr AS TYPENAME)` | Returns **`NULL`** |

`TYPENAME` is any DuckDB type name or alias (see the [type table](#2-the-type-system)).

```sql
-- CAST and :: are the same thing:
SELECT CAST(i AS VARCHAR) AS i FROM generate_series(1, 3) tbl(i);
SELECT i::DOUBLE       AS i FROM generate_series(1, 3) tbl(i);
```

**Not every cast is legal.** Per the docs: "Not all casts are possible. For example, it is not possible to convert an `INTEGER` to a `DATE`." Illegal or unparseable values raise an error:

```sql
SELECT CAST('hello' AS INTEGER);
-- Conversion Error: Could not convert string 'hello' to INT32
```

### Why `TRY_CAST` for messy ingest

`CAST`/`::` abort the **entire query** on the first bad value. In an out-of-core scan over hundreds of millions of source rows, one malformed cell should not kill the batch. `TRY_CAST` converts what it can and yields `NULL` for what it cannot, so you can quarantine or default the failures downstream instead of crashing:

```sql
SELECT TRY_CAST('hello' AS INTEGER) AS i;   -- NULL, no error
SELECT TRY_CAST('42'    AS INTEGER) AS i;    -- 42
```

Pair it with a diagnostic count to measure ingest quality:

```sql
SELECT
    count(*)                                             AS rows_total,
    count(*) FILTER (WHERE TRY_CAST(amount AS DOUBLE) IS NULL
                       AND amount IS NOT NULL)           AS amount_cast_failures
FROM raw_source;
```

### `cast_to_type` (macro helper)

`cast_to_type(expr, type_template)` casts `expr` to the type of `type_template` — primarily useful inside macros to keep a generic operation's output type aligned with another column. Signature confirmed from the CAST page; exact behavioral edge cases beyond "matches another column's type" were not fully enumerated in the fetched page.

> Relevance to core-x: `TRY_CAST` is the correct primitive for the DuckDB → Arrow → Lance ingest path. Raw payloads are transport-only and frequently dirty; a single `CAST` failure would abort a full out-of-core spill run. Cast to the final resolution-key type with `TRY_CAST`, count NULLs to gate quality, and only then stream to Lance. Casting a `VARCHAR` resolution key to its final `BIGINT`/`UUID`/`VARCHAR` form with `TRY_CAST` before the Lance write is what keeps a `BTREE`-indexed key column clean.

---

## 2. The type system

Source: https://duckdb.org/docs/current/sql/data_types/overview

### 2.1 Primitive / general-purpose types

| Type | Aliases | Description |
|------|---------|-------------|
| `BIGINT` | `INT8`, `LONG` | Signed eight-byte integer |
| `INTEGER` | `INT4`, `INT`, `SIGNED` | Signed four-byte integer |
| `SMALLINT` | `INT2`, `SHORT` | Signed two-byte integer |
| `TINYINT` | `INT1` | Signed one-byte integer |
| `HUGEINT` | — | Signed sixteen-byte integer |
| `UBIGINT` / `UINTEGER` / `USMALLINT` / `UTINYINT` / `UHUGEINT` | — | Unsigned variants of the above |
| `BIGNUM` | — | Variable-length integer (arbitrary precision) |
| `DOUBLE` | `FLOAT8` | Double-precision floating point (8 bytes) |
| `FLOAT` | `FLOAT4`, `REAL` | Single-precision floating point (4 bytes) |
| `DECIMAL(prec, scale)` | `NUMERIC(prec, scale)` | Fixed-precision number |
| `VARCHAR` | `CHAR`, `BPCHAR`, `TEXT`, `STRING` | Variable-length character string |
| `BLOB` | `BYTEA`, `BINARY`, `VARBINARY` | Variable-length binary data |
| `BIT` | `BITSTRING` | String of 1s and 0s |
| `BOOLEAN` | `BOOL`, `LOGICAL` | Logical boolean (true / false) |
| `DATE` | — | Calendar date (year, month, day) |
| `TIME` | — | Time of day (no time zone) |
| `TIMESTAMP` | `DATETIME` | Combination of date and time |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMPTZ` | Timestamp interpreted against the current time zone |
| `INTERVAL` | — | Date / time delta |
| `UUID` | — | Universally unique identifier |
| `JSON` | — | Requires the `json` extension (text-backed; contrast with `VARIANT`) |

### 2.2 Nested / composite types

| Type | Description |
|------|-------------|
| `ARRAY` | An ordered, **fixed-length** sequence of values of the **same** type (e.g. `INTEGER[3]`) |
| `LIST` | An ordered, **variable-length** sequence of values of the same type (e.g. `INTEGER[]`) |
| `MAP` | A dictionary of key→value where **all keys share one type** and **all values share one type** |
| `STRUCT` | A dictionary of named fields where each key is a string but **each value may be a different type** |
| `UNION` | Stores exactly **one** of several alternative types per value |
| `VARIANT` | Semi-structured type where **each value is self-contained with its own type information** |

Case-sensitivity rule (from the overview page): "keys of `MAP`s are case-sensitive, while keys of `UNION`s and `STRUCT`s are case-insensitive." Nested types compose arbitrarily (`LIST` of `STRUCT`, `STRUCT` with a `MAP` field, etc.).

`ARRAY` vs `LIST`: `ARRAY` is fixed-length (declared `T[N]`) and maps to Arrow `FixedSizeList`; `LIST` is variable-length (declared `T[]`) and maps to Arrow `List`/`LargeList`. Use `ARRAY` for embeddings/vectors of known dimension, `LIST` for genuinely ragged data.

---

## 3. VARIANT — native semi-structured type

Sources: https://duckdb.org/docs/current/sql/data_types/variant • https://duckdb.org/2026/03/09/announcing-duckdb-150

**Status (verified):** `VARIANT` was introduced *experimentally* in **1.4.0** and became a **native, GA feature in 1.5.0** — the 1.5.0 release note states: "DuckDB now natively supports the VARIANT type, inspired by Snowflake's semi-structured VARIANT data type and available in Parquet since 2025." The docs do **not** label it preview/experimental in the 1.5.x line. On the **1.4.x LTS** line it is still experimental — do not assume GA behavior if you are pinned to LTS.

**What it is:** "The `VARIANT` type stores typed, binary data where each row is self-contained with its own type information." Unlike `JSON` (physically stored as text), VARIANT embeds type metadata per value, which gives it better compression and query performance for semi-structured data.

### Constructing VARIANT values

Cast any value to `VARIANT` with `::VARIANT`:

```sql
INSERT INTO events VALUES
    (1, 42::VARIANT),
    (2, 'hello world'::VARIANT),
    (3, [1, 2, 3]::VARIANT),
    (4, {'name': 'Alice', 'age': 30}::VARIANT);
```

### VARIANT functions

| Function | Behavior |
|----------|----------|
| `variant_typeof(v)` | Returns the underlying type as text, e.g. `INT32`, `VARCHAR`, `ARRAY(3)`, `OBJECT(name, age)` |
| `variant_extract(v, 'fieldname')` | Extracts a nested field; dot notation `v.fieldname` also works |

```sql
SELECT variant_typeof(data)            FROM events;   -- INT32 / VARCHAR / ARRAY(3) / OBJECT(name, age)
SELECT variant_extract(data, 'name')   FROM events WHERE id = 4;   -- 'Alice'
SELECT data.name                       FROM events WHERE id = 4;   -- 'Alice' (dot form)
```

### Parquet & Arrow mapping

- **Parquet:** DuckDB reads VARIANT from Parquet, **including shredding** (semi-structured values physically split into typed sub-columns so a scan reads only the fields it needs, in their native type, instead of one VARCHAR blob). VARIANT has been a Parquet type since 2025.
- **Arrow:** **Unverified / needs confirmation.** None of the fetched pages (the VARIANT data-type page, the 1.5.0 release note) specify an Apache Arrow type mapping for `VARIANT`. Do **not** assume a zero-copy Arrow round-trip for VARIANT columns. Before relying on VARIANT across a DuckDB→Arrow→Lance boundary, test the actual `to_arrow_table()` output type on your pinned DuckDB version, or avoid VARIANT at the Arrow boundary and materialize the fields you need into concrete STRUCT/primitive columns first. See `02_arrow_integration.md` and `13_lance_interop.md`.

> Relevance to core-x: VARIANT is attractive for ragged upstream payloads, but the Arrow bridge is where core-x lives (DuckDB → Arrow → Lance-on-R2). Until the Arrow mapping is confirmed on the pinned version, treat VARIANT as an intermediate compute type only and project it into typed STRUCT/primitive columns before the Lance write, so append-only fragments carry a stable, indexable schema.

---

## 4. Constructing and accessing nested values

### 4.1 STRUCT

Source: https://duckdb.org/docs/current/sql/data_types/struct

A STRUCT is an **ordered set of named fields**; "Each row in the `STRUCT` column must have the same keys." Keys are case-insensitive.

**Construct:**

```sql
-- struct_pack (named args with :=)
SELECT struct_pack(key1 := 'value1', key2 := 42) AS s;

-- literal {...}
SELECT {'key1': 'value1', 'key2': 42} AS s;

-- row() into a typed column
CREATE TABLE t1 (s STRUCT(v VARCHAR, i INTEGER));
INSERT INTO t1 VALUES (row('a', 42));

-- from a subquery's columns
SELECT d AS s FROM (SELECT 'value1' AS key1, 42 AS key2) d;
```

**Access:**

```sql
-- dot notation
SELECT a.x        FROM (SELECT {'x': 1, 'y': 2, 'z': 3} AS a);
SELECT a."x space" FROM (SELECT {'x space': 1} AS a);       -- quoted key with space

-- bracket notation (constant key expressions only)
SELECT a['x space'] FROM (SELECT {'x space': 1} AS a);

-- struct_extract()
SELECT struct_extract({'x space': 1}, 'x space');

-- explode to columns
SELECT unnest(a)          FROM (SELECT {'x': 1, 'y': 2} AS a);   -- columns x, y
SELECT a.* EXCLUDE ('y')  FROM (SELECT {'x': 1, 'y': 2} AS a);   -- star with exclusion
```

**Modify:** `struct_update` replaces/adds fields via named args:

```sql
SELECT struct_update({'a': 1, 'b': 2}, b := 3, c := 4) AS s;   -- {'a':1,'b':3,'c':4}
```

#### Altering STRUCT fields (v1.3.0+)

```sql
ALTER TABLE test ADD COLUMN s.k INTEGER;    -- add nested field
ALTER TABLE test DROP COLUMN s.i;           -- drop nested field
ALTER TABLE test RENAME s.j TO v1;          -- rename nested field
```

### 4.2 LIST / ARRAY

Source: https://duckdb.org/docs/current/sql/functions/list

Lists are **1-indexed** (not 0-indexed).

```sql
SELECT list_value(4, 5, 6);     -- creates a LIST; list_pack() is an alias
SELECT [4, 5, 6];               -- literal form
SELECT [4, 5, 6][3];            -- 6  (1-based index)
SELECT [4, 5, 6][2:3];          -- sublist via slice: list[begin[:end][:step]], negatives allowed
```

| Function | Description |
|----------|-------------|
| `list_value(arg, ...)` | Creates a LIST containing the argument values |
| `list_pack(arg, ...)` | Alias for `list_value` |
| `list[index]` | 1-based element access |
| `list[begin[:end][:step]]` | Slice; negative values accepted |

### 4.3 MAP

Source: https://duckdb.org/docs/current/sql/data_types/map

All keys share one type; all values share one type. MAPs may **not** have duplicate keys, and different rows may carry different key sets.

**Construct:**

```sql
SELECT MAP {'key1': 10, 'key2': 20, 'key3': 30};          -- MAP {...} literal
SELECT MAP(['key1', 'key2', 'key3'], [10, 20, 30]);       -- from two parallel lists
SELECT MAP {1: 42.001, 5: -32.1};                          -- integer keys
SELECT map_from_entries([('key1', 10), ('key2', 20)]);     -- from (k,v) tuples
CREATE TABLE tbl (col MAP(INTEGER, DOUBLE));               -- typed column
```

**Access:**

```sql
SELECT MAP {'key1': 5, 'key2': 43}['key1'];               -- 5
SELECT MAP {'key1': 5, 'key2': 43}['key3'];               -- NULL (missing key)
SELECT map_extract(MAP {'key1': 5, 'key2': 43}, 'key1');  -- [5]  (returns a list)
```

`map_keys()` / `map_values()` return the key list / value list respectively (referenced from the map/struct function pages; use `map_extract` for single-key lookup).

### 4.4 UNNEST

Source: https://duckdb.org/docs/current/sql/query_syntax/unnest

"The `unnest` special function is used to unnest lists or structs by one level." It only works in the `SELECT` clause.

```sql
SELECT unnest([1, 2, 3]);          -- 3 rows: 1, 2, 3
SELECT unnest({'a': 42, 'b': 84}); -- 2 columns: a, b

-- recursive: fully expand nested levels
SELECT unnest([{'a': 42, 'b': 84}], recursive := true);

-- max_depth: stop at a given depth
SELECT unnest([[[1, 2], [3, 4]]], max_depth := 2);

-- keep_parent_names: preserve field-name path during recursive unnest
SELECT unnest(col, recursive := true, keep_parent_names := true) FROM t;
```

| Named parameter | Type | Default | Effect |
|-----------------|------|---------|--------|
| `recursive` | BOOLEAN | `false` | Expand across all nesting levels, not just one |
| `max_depth` | INTEGER | (unbounded / one level) | Restrict how deep recursive unnest goes |
| `keep_parent_names` | BOOLEAN | `false` | Preserve the path to nested values as generated names |

Behavior: multiple lists unnest side-by-side (shorter lists pad with `NULL`); empty and `NULL` lists produce **zero** rows.

### 4.5 List comprehensions

Source: https://duckdb.org/docs/current/sql/functions/list (see also https://duckdb.org/2023/08/23/even-friendlier-sql)

DuckDB supports Python-style list comprehensions: `[expression FOR x IN list]`, optionally `IF cond`. Under the hood they translate to `list_apply`/`list_transform` and `list_filter`.

```sql
SELECT [x + 1 for x in [1, 2, 3]] AS l;                       -- [2, 3, 4]
SELECT [x + 1 for x in [1, 2, 3] if x >= 2] AS l;             -- [3, 4]
SELECT [lower(x) FOR x IN strings] AS strings
FROM (VALUES (['Hello', '', 'World'])) t(strings);            -- [hello, , world]
SELECT [upper(x) FOR x IN strings IF len(x) > 0] AS strings
FROM (VALUES (['Hello', '', 'World'])) t(strings);            -- [HELLO, WORLD]
```

### 4.6 Lambdas (for `list_transform` / `list_filter` / `list_reduce`)

Source: https://duckdb.org/docs/current/sql/functions/lambda

Use **Python-style** `lambda param1, param2, ...: expression`. The old single-arrow form (`x -> x + 1`) is **deprecated** as of v1.3; v1.5 is the last release that accepts it without an explicit opt-in, and v2.0 disables it by default.

```sql
SELECT list_filter([3, 4, 5], lambda x: x > 4);        -- [5]
SELECT list_transform([1, 2, 3], lambda x: x + 1);     -- [2, 3, 4]
SELECT list_reduce([1, 2, 3, 4], lambda acc, x: acc + x);  -- 10
```

---

## 5. Pipeline-handy clauses

Source: https://duckdb.org/docs/current/sql/dialect/friendly_sql (plus the QUALIFY and samples pages)

### 5.1 QUALIFY — filter on window-function results

Source: https://duckdb.org/docs/current/sql/query_syntax/qualify

QUALIFY is to window functions what `HAVING` is to aggregates: "The `QUALIFY` clause is used to filter the results of WINDOW functions." It "avoids the need for a subquery or WITH clause to perform this filtering."

**Clause position:** after the optional `WINDOW` clause, **before** `ORDER BY`. Logical evaluation order is roughly: `FROM → WHERE → GROUP BY → HAVING → WINDOW → QUALIFY → ORDER BY → LIMIT`.

```sql
-- filter by an inline window expression
SELECT schema_name, function_name,
    row_number() OVER (PARTITION BY schema_name ORDER BY function_name) AS function_rank
FROM duckdb_functions()
QUALIFY row_number() OVER (PARTITION BY schema_name ORDER BY function_name) < 3;

-- filter by a named WINDOW / by the window alias
SELECT schema_name, function_name,
    row_number() OVER my_window AS function_rank
FROM duckdb_functions()
WINDOW my_window AS (PARTITION BY schema_name ORDER BY function_name)
QUALIFY function_rank < 3;
```

Note from the docs: this filters based on WINDOW **functions**, not necessarily based on the `WINDOW` **clause** — you can QUALIFY on an inline `OVER(...)` even without naming a window.

### 5.2 `SELECT * EXCLUDE (...)` / `SELECT * REPLACE (...)`

- `EXCLUDE` drops named columns from the `*` expansion.
- `REPLACE` swaps specific columns for new expressions while keeping the rest of `*`.

```sql
SELECT * EXCLUDE (raw_blob, ingest_ts) FROM staging;
SELECT * REPLACE (TRY_CAST(amount AS DOUBLE) AS amount) FROM staging;
```

### 5.3 `COLUMNS()` — apply one expression to many columns

`COLUMNS()` runs the same expression over multiple columns, matched by regex, and supports `EXCLUDE`/`REPLACE` and lambdas.

```sql
-- trim every text column at once (pattern is illustrative; verify column set on your schema)
SELECT COLUMNS(c -> c LIKE '%_name') FROM people;
SELECT trim(COLUMNS('.*_name')) FROM people;   -- apply trim() across all *_name columns
```

### 5.4 `GROUP BY ALL` / `ORDER BY ALL`

- `GROUP BY ALL` infers the grouping columns from the non-aggregated items in `SELECT`.
- `ORDER BY ALL` orders on all output columns (handy for deterministic output before a write).

```sql
SELECT region, product, sum(amount) AS total
FROM sales
GROUP BY ALL;                    -- groups by region, product

SELECT * FROM sales ORDER BY ALL;
```

### 5.5 `FILTER (WHERE ...)` — per-aggregate filtering

Filter the rows that feed an individual aggregate without a `CASE` hack:

```sql
SELECT
    count(*)                                   AS n,
    count(*) FILTER (WHERE status = 'paid')    AS n_paid,
    sum(amount) FILTER (WHERE amount > 0)      AS gross_credits
FROM ledger;
```

### 5.6 `USING SAMPLE`

Source: https://duckdb.org/docs/current/sql/samples

| Method | Percentage | Fixed row count | Notes |
|--------|-----------|-----------------|-------|
| `reservoir` | yes | **yes** | Always outputs an exact count; the only method that supports a fixed number of rows |
| `system` | yes | no | Includes each **vector** with probability = sample %; fastest, coarse-grained |
| `bernoulli` | yes | no | Includes each **row** independently with probability = sample % |

```sql
SELECT * FROM tbl USING SAMPLE 5;                              -- 5 rows (reservoir, the default for a fixed count)
SELECT * FROM tbl USING SAMPLE 10%;                            -- ~10% (system by default for percentages)
SELECT * FROM tbl USING SAMPLE reservoir(50 ROWS) REPEATABLE (100);  -- exact 50 rows, seeded
SELECT * FROM tbl USING SAMPLE 20% (system, 377);             -- system, seed 377
SELECT * FROM tbl USING SAMPLE 10 PERCENT (bernoulli);        -- per-row 10%
```

`REPEATABLE (seed)` makes the sample reproducible. Use `reservoir(N ROWS) REPEATABLE (seed)` to pull a stable, exact-size profiling slice off a huge source without a full scan.

### 5.7 PIVOT / UNPIVOT

- `PIVOT` turns long tables into wide tables.
- `UNPIVOT` turns wide tables into long tables.

```sql
PIVOT sales ON product USING sum(amount) GROUP BY region;
UNPIVOT monthly_wide ON jan, feb, mar INTO NAME month VALUE amount;
```

### 5.8 Other friendly-SQL conveniences

- **Reusable column aliases** in the same `SELECT`: `SELECT i + 1 AS j, j + 2 AS k FROM range(0, 3) t(i)`.
- **`LIMIT n%`**: `SELECT * FROM t LIMIT 10%` returns 10% of results.
- **Trailing commas** are permitted in `SELECT` lists and `[...]` list construction.
- `GROUP BY CUBE` / `GROUP BY ROLLUP` / `GROUPING SETS` for multi-level aggregation.

---

## 6. String / number cleaning idioms

Sources: https://duckdb.org/docs/current/sql/functions/text • https://duckdb.org/docs/current/sql/expressions/cast

| Function | Signature | Behavior |
|----------|-----------|----------|
| `trim` | `trim(string[, characters])` | Remove any of `characters` from **both** sides; `characters` defaults to space |
| `ltrim` | `ltrim(string[, characters])` | Same, **left** side only |
| `rtrim` | `rtrim(string[, characters])` | Same, **right** side only |
| `replace` | `replace(string, source, target)` | Replace all occurrences of `source` with `target` (literal, not regex) |
| `regexp_replace` | `regexp_replace(string, regex, replacement[, options])` | Replace matches of `regex`; `options` is a flags string (e.g. `'g'` for global) |
| `lower` | `lower(string)` | Lowercase |
| `upper` | `upper(string)` | Uppercase |
| `strip_accents` | `strip_accents(string)` | Remove diacritics |

`nullif(a, b)` (returns `NULL` when `a = b`, else `a`) and `coalesce(a, b, ...)` (first non-NULL) are standard SQL conditionals — they live on the general/conditional-expressions pages, not the text-functions page, but are the two other core cleaning tools.

**Common cleaning patterns:**

```sql
-- normalize whitespace + case
SELECT lower(trim(name)) AS name_norm FROM raw;

-- turn empty-string sentinels into real NULLs, then safely cast
SELECT TRY_CAST(nullif(trim(qty), '') AS INTEGER) AS qty FROM raw;

-- strip currency formatting, then cast money text to DOUBLE
SELECT TRY_CAST(regexp_replace(price, '[$,]', '', 'g') AS DOUBLE) AS price_usd FROM raw;

-- collapse blanks to NULL, coalesce to a default
SELECT coalesce(nullif(trim(region), ''), 'UNKNOWN') AS region FROM raw;
```

---

## 7. Worked examples

### 7.1 Window dedup with `QUALIFY row_number()`

Keep the newest row per business key without a subquery or CTE:

```sql
SELECT *
FROM raw_events
QUALIFY row_number() OVER (
    PARTITION BY entity_id
    ORDER BY event_ts DESC
) = 1;
```

Deterministic ordering matters when `event_ts` ties — add a stable tiebreaker (e.g. `ORDER BY event_ts DESC, source_file, row_id`) so the same row wins on every run, which keeps append-only fragments reproducible.

### 7.2 `TRY_CAST` a money string to `DOUBLE`

```sql
SELECT
    id,
    raw_amount,
    TRY_CAST(
        regexp_replace(trim(raw_amount), '[$,\s]', '', 'g')   -- strip $ , and whitespace
        AS DOUBLE
    ) AS amount_usd
FROM raw_invoices;
-- '$1,234.50' -> 1234.5 ; 'N/A' -> NULL (no error, quarantine-able downstream)
```

### 7.3 Both together — clean + dedup in one pass

```sql
SELECT
    entity_id,
    lower(trim(name))                                                  AS name,
    TRY_CAST(regexp_replace(amount, '[$,]', '', 'g') AS DOUBLE)        AS amount_usd,
    event_ts
FROM raw_events
QUALIFY row_number() OVER (
    PARTITION BY entity_id
    ORDER BY event_ts DESC, source_file
) = 1;
```

This is the shape of the projection that feeds `to_arrow_reader()` → `lance.write_dataset(...)`: typed, deduped, one row per key, ready to append as an immutable fragment.

---

## 8. Deprecations & footguns

- **Lambda arrow syntax `x -> expr` is deprecated** (since v1.3). v1.5 is the last release accepting it implicitly; v2.0 disables it by default. Use `lambda x: expr`.
- **Lists are 1-indexed.** `[4,5,6][1]` is `4`, and `[...][0]` is not element zero — porting 0-indexed logic silently shifts every access. Slicing is `list[begin:end:step]`.
- **`CAST`/`::` abort the whole query on one bad value.** In batch ingest use `TRY_CAST`. Never assume a `::` in a scan over dirty source is safe.
- **`map_extract` returns a LIST**, not a scalar (`[5]`, not `5`); bracket access `m['k']` returns the scalar. Missing keys: bracket returns `NULL`.
- **MAP keys are case-sensitive; STRUCT/UNION keys are case-insensitive.** A `MAP {'Key': 1}['key']` lookup returns `NULL`.
- **VARIANT Arrow mapping is unconfirmed** — see §3. Do not build a zero-copy Arrow→Lance contract on VARIANT columns without testing on your pinned version.
- **VARIANT is experimental on the 1.4.x LTS line**, GA only from 1.5.0. Check your pin.
- **`unnest` only works in `SELECT`**, and `NULL`/empty lists yield zero rows — an inner join through `unnest` silently drops those parent rows.

---

## 9. Unverified / needs confirmation

- **VARIANT → Apache Arrow type mapping.** Not stated on any fetched page. Verify empirically (`duckdb.sql("SELECT ...::VARIANT").to_arrow_table().schema`) on the exact DuckDB version in your pipeline before crossing the Arrow/Lance boundary with VARIANT.
- **`cast_to_type(expr, type_template)`** — signature confirmed; full edge-case behavior (nested-type templates, failure mode vs. `TRY_CAST`) not exhaustively documented on the fetched CAST page.
- **`map_keys` / `map_values` exact signatures** — referenced but not quoted verbatim from a fetched page; confirm on the map-functions page if a precise signature is load-bearing.
- **Default sampling method when neither method nor `ROWS`/`%` disambiguates** — the docs state reservoir is the only fixed-count method and system is vector-based for percentages; the precise default-method selection rules were not quoted verbatim.

---

## Cross-links (sibling files in this domain)

- `00_overview.md` — DuckDB overview, editions, clients, versioning & release lines (LTS vs. non-LTS context for §"Version ground truth")
- `01_python_client.md` — Python `connect`/`execute`, relational API, replacement scans
- `02_arrow_integration.md` — `to_arrow_table`/`to_arrow_reader`, `from_arrow`, `register`, ADBC (the Arrow boundary referenced in §3, §7)
- `03_csv_import.md` — `read_csv`, `all_varchar`, `ignore_errors`, `rejects` (dirty-ingest source for `TRY_CAST`)
- `04_parquet.md` — `read_parquet`, VARIANT-in-Parquet shredding context for §3
- `05_json.md` — JSON functions & nested casting (contrast with VARIANT in §3)
- `06_configuration_memory_spill.md` — `memory_limit`, `threads`, `temp_directory`, out-of-core spill
- `07_httpfs_s3_r2.md` — httpfs, S3 API & Cloudflare R2
- `08_secrets_manager.md` — `CREATE SECRET` (s3/r2/gcs/azure/http)
- `09_extensions_system.md` — `INSTALL`/`LOAD`, autoloading, signing
- `10_core_extensions_catalog.md` — official core-extension list
- `11_quack_extension.md` — extension template internals
- `13_lance_interop.md` — DuckDB ↔ Lance read/write reality (destination for the §7 projections)
