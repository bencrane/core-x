# Profiling & Index Pitfalls — proving a query is fast (EXPLAIN ANALYZE, ART indexes, memory)

> Canonical upstream reference. Folded from the committed talk-transcript corpus (docs/youtube-transcripts/, docs/batches/) and verified against live upstream docs where they exist (July 2026). Talk-reported claims are attributed inline; upstream-verified facts cite the doc URL.
>
> Primary sources:
> - docs/youtube-transcripts/clean/2025-09-24_indexes-are-not-all-you-need-common-duckdb-pitfalls.clean.md — "Indexes Are Not All You Need — Common DuckDB Pitfalls", 2025-09-24, Tanya Bogajage (DuckDB Labs)
> - https://duckdb.org/docs/current/dev/profiling.html — profiling pragmas: `enable_profiling`, `profiling_output`, `profiling_coverage`, `disable_profiling` and their values/defaults
> - https://duckdb.org/docs/current/guides/meta/explain_analyze.html — `EXPLAIN ANALYZE`, physical operators, EC vs actual cardinality
> - https://duckdb.org/docs/current/sql/indexes.html — ART (Adaptive Radix Tree) indexes, PK/FK/UNIQUE, in-memory-during-creation constraint
> - https://duckdb.org/docs/current/guides/performance/indexing.html — index-scan eligibility, threshold formula, `index_scan_percentage` / `index_scan_max_count`
> - https://duckdb.org/2024/07/09/memory-management — `duckdb_memory()` columns and tag values (`ART_INDEX`, `IN_MEMORY_TABLE`, `BASE_TABLE`)
> - https://duckdb.org/docs/current/configuration/pragmas — `PRAGMA database_size` fields; buffer manager excludes index memory and small intermediates

Scope: how to prove a DuckDB query is actually fast — enabling the profiler, reading `EXPLAIN ANALYZE` to distinguish an index scan from a sequential scan, understanding the memory and maintenance cost of ART secondary indexes, and inspecting memory with `duckdb_memory()`. The through-line thesis: there is no rule of thumb — profile and benchmark.

---

## 0. The thesis

The talk's central claim, verbatim: **"there's no rule of thumb if you use DuckDB … because we try to choose kind of the best defaults anyways, there's not an obvious 'you have to do this and this and this' to make your system fast, because ideally it's already fast. So if it's not, then you really have to profile and benchmark to track down which operator is slow"** (Indexes Are Not All You Need, 2025-09-24 — docs/youtube-transcripts/clean/2025-09-24_indexes-are-not-all-you-need-common-duckdb-pitfalls.clean.md).

DuckDB ships sensible preconfigured defaults, which makes it hard to *accidentally* land a slow query — but also means it is deliberately **not** as tunable as systems like Oracle, so once a query is slow, getting it fast is not a matter of flipping a documented knob (talk-reported, same source). The method below is the substitute for a rule of thumb.

---

## 1. Enabling the profiler

DuckDB has two ways to get run-time numbers: `EXPLAIN ANALYZE` (readable, one-shot) and the **profiler** (richer metrics, forwardable to observability tooling). The talk notes the profiler "gives you a bit more metrics than the EXPLAIN ANALYZE, and maybe also makes them more usable, because we can output them for some clients already directly on the connection, so you can forward them to some observability tool" (talk, same source).

### 1.1 Pragmas (UPSTREAM-VERIFIED)

Verified against https://duckdb.org/docs/current/dev/profiling.html. `PRAGMA` and `SET` forms are interchangeable.

```sql
-- Turn profiling on. Value controls output format.
PRAGMA enable_profiling = 'json';   -- machine-readable; forward to observability tools
-- or: SET enable_profiling = 'json';

-- Profile every statement, not just SELECT.
PRAGMA profiling_coverage = 'all';

-- Write the profile to a file instead of the console.
PRAGMA profiling_output = '/path/to/profile.json';

-- Run your query, then read profiling_output.

-- Turn it off.
PRAGMA disable_profiling;
```

The talk's setup line: **"we will be using profiling coverage all, which means we want to profile every query, not just SELECT statements, and we want to have the output in JSON"** (talk, same source) — i.e. `enable_profiling = 'json'` + `profiling_coverage = 'all'`.

| Setting | Accepted values | Default | Notes |
|---|---|---|---|
| `enable_profiling` | `query_tree`, `json`, `query_tree_optimizer`, `no_output` | `query_tree` | Use `json` for programmatic / observability consumption. |
| `profiling_coverage` | `SELECT`, `ALL` | `SELECT` | `ALL` profiles non-SELECT statements too (INSERT/UPDATE/etc.). |
| `profiling_output` | file path string | console | When set, the profile is written to the file rather than printed. |
| `disable_profiling` | (no value) | — | Turns profiling off. |

Any of these can be reverted with `RESET <setting_name>;` (upstream: profiling doc).

> Version-gating: exact JSON keys emitted (per-operator timings, peak buffer memory, bytes read from file) are an actively expanding surface — the talk explicitly frames the profiler and its metrics as work in progress ("which we are actively working on expanding"). Treat the *set* of JSON metric fields as version-dependent; the pragma names above are stable and verified.

### 1.2 Reading the JSON

The JSON profile carries per-operator metrics the talk names as available or in-flight: average latency, **peak buffer memory**, per-operator timings (e.g. distinguishing a scan from an index-probing operation), and bytes read from a file (talk, same source — the file-read metric attributed to "Maya … working very actively on getting cool metrics in there, like how much did you read from a file"). Feed the JSON to an observability tool to track query-time regressions against traffic peaks; the talk cites this as the highest-leverage practice for production systems.

---

## 2. EXPLAIN vs EXPLAIN ANALYZE — is the index actually being used?

`EXPLAIN` shows the *planned* physical operators without running the query. `EXPLAIN ANALYZE` **runs** the query and annotates every operator with run-time numbers plus estimated cardinality (EC) and actual cardinality (UPSTREAM-VERIFIED: https://duckdb.org/docs/current/guides/meta/explain_analyze.html).

```sql
EXPLAIN ANALYZE
SELECT count(*) FROM data WHERE id = 42;
```

Output is an operator tree annotated with EC, actual cardinality, and per-operator wall-clock time.

### 2.1 The core footgun: a filter that *looks* indexed can still run SEQ_SCAN

The single most important verification in this file. From the talk: **"maybe you have added your index, but DuckDB chose not to. So you can first … have a look with EXPLAIN ANALYZE at your query plans, and you can see, in the left side, this is using a sequential scan on your table. So somehow DuckDB is not using your index scan. That, for example, could explain why, even though you thought your query would be fast, it ended up not being fast"** (talk, same source).

**Do not assume the index is engaged because it exists.** Read the operator type:

| Operator in plan | Meaning |
|---|---|
| `SEQ_SCAN` | Sequential table scan — reads all rows. Index (if any) was **not** used. |
| Index scan (`INDEX_SCAN` / ART index-scan operator) | The ART index was probed; only matching row IDs fetched. |

The talk's rule: first confirm you see an index-scan operator (not `SEQ_SCAN`) in the plan, *then* trust that the index is doing work. Always confirm with `EXPLAIN ANALYZE` rather than inferring from index existence.

### 2.2 When DuckDB will and won't pick an ART index scan (UPSTREAM-VERIFIED)

Per https://duckdb.org/docs/current/guides/performance/indexing.html, an ART index scan applies only to:

- **equality** and **`IN (...)`** conditions,
- on a **single-column** index **without expressions**.

Multi-column indexes and expression indexes **cannot** use an index scan. This is a common reason a "filter that looks indexed" degrades to `SEQ_SCAN` — the predicate shape or index shape disqualifies it before selectivity is even considered.

---

## 3. The selectivity threshold — why the index gets skipped

Even a legal single-column equality filter only uses the index if the result is a **tiny** fraction of the table.

### 3.1 Default threshold (UPSTREAM-VERIFIED)

DuckDB uses the ART index scan only when the estimated match count is below:

```
MAX(2048, 0.001 * table_cardinality)
```

(https://duckdb.org/docs/current/guides/performance/indexing.html). ART indexes are documented as "mainly used to ensure primary key constraints and to speed up point and **very highly selective (i.e., < 0.1%)** queries" (https://duckdb.org/docs/current/sql/indexes.html). Above the threshold, DuckDB falls back to `SEQ_SCAN` on purpose — a scan is cheaper than the random access of chasing many row IDs.

The talk frames the mechanism the same way: the default is to try to scan one data chunk (~2048 rows) via the index and stop if there are more, because it's judged "too expensive"; and the payoff exists only for high selectivity — **"your filters only get you like four rows out of 10 million or 100 million. In these cases … you do want to add an index"** (talk, same source).

### 3.2 Tuning the threshold (UPSTREAM-VERIFIED names)

Two settings control the threshold; set either (or both) to zero to disable index scans entirely (https://duckdb.org/docs/current/guides/performance/indexing.html):

```sql
-- Cap the fraction of the table an index scan will consider.
SET index_scan_percentage = 0.001;   -- fraction (0.001 = 0.1%)

-- Cap the absolute row count an index scan will consider.
SET index_scan_max_count = 2048;

-- Disable index scans: set to zero.
SET index_scan_percentage = 0;
SET index_scan_max_count = 0;
```

| Setting | Meaning | Disable |
|---|---|---|
| `index_scan_percentage` | fraction-of-table ceiling for choosing an index scan | `0` |
| `index_scan_max_count` | absolute row-count ceiling for choosing an index scan | `0` |

The talk describes exactly this knob without naming it — "if you know that your system gets slower once your index starts scanning too much … you could decrease the maximum count … I never want to scan more than five values, maybe" (talk, same source). The *behaviour* is talk-reported; the **setting names and the MAX(2048, 0.001·N) formula are upstream-verified** above.

---

## 4. The cost of ART secondary indexes

An explicit `CREATE INDEX` (or a `PRIMARY KEY` / `UNIQUE` constraint) makes DuckDB keep a **secondary copy** of the data it must maintain (UPSTREAM-VERIFIED: https://duckdb.org/docs/current/sql/indexes.html — ART indexes "are automatically created for columns with a `UNIQUE` or `PRIMARY KEY` constraint"; a `FOREIGN KEY` constraint does **not** auto-create an ART index). The talk enumerates three costs.

### 4.1 Maintenance is not parallelized — bulk appends slow dramatically

TALK-REPORTED benchmark (as stated in the talk; the specific millisecond figures are not independently verified). Appending 10k integer rows:

| Table state | Append time |
|---|---|
| No index | 7 ms |
| Three indexes on the column | ~400 ms |

Cause (talk): a plain table can bulk-append by writing to the end, but each index value must be placed into the tree, **and index maintenance is not parallelized** — "for every value, we need to go onto our tree and need to find out where we need to add it. And at the moment, that's also not parallelized" (talk, same source).

Upstream corroborates the *direction* without the numbers: "Changes on indexed tables perform worse than their non-indexed counterparts"; define indexes **after** bulk-loading because adding them beforehand "is detrimental to load performance" (https://duckdb.org/docs/current/guides/performance/indexing.html).

### 4.2 Index memory is roughly 2× table memory

TALK-REPORTED: measuring with `duckdb_memory()`, "our index memory is actually **twice the size** of our table memory," because the secondary structure copies the entire column, its row IDs, plus tree-node metadata (talk, same source; the 2× ratio is as stated in the talk and not independently verified — magnitude is workload-dependent). Upstream corroborates the concern directionally: "indexes can take up a significant portion of DuckDB's available memory, potentially affecting the performance of memory-intensive queries" (https://duckdb.org/docs/current/guides/performance/indexing.html).

### 4.3 No eviction strategy yet; must fit in memory during creation

- **No eviction (TALK-REPORTED, roadmap intention):** index memory is buffer-managed (tracked) but "we don't have an eviction strategy yet. So if you have an index and even if you lazily load it … all of that memory is active, and DuckDB is not yet evicting it" — flagged as hopefully fixed "in some of the upcoming releases" (talk, same source; not independently verified — treat as version-dependent). Under high concurrency, active index pages add memory pressure on top of live queries and can starve a long-running query of memory. The talk's guidance: benchmark it case by case.
- **Must fit in RAM during creation (UPSTREAM-VERIFIED):** "ART indexes must currently be able to fit in memory during index creation" — avoid creating an ART index that will not fit in RAM while being built (https://duckdb.org/docs/current/sql/indexes.html).

### 4.4 When an index is still worth it

Legitimate cases (talk, same source): (1) you have no choice — integrity constraints (PK/FK/UNIQUE) require it; (2) a **repeated, highly selective** filter query (a handful of rows out of tens/hundreds of millions). Otherwise the append and memory overhead usually outweighs the point-lookup gain. A plain scan has **predictable** cost — always the whole table, independent of match count — which the talk offers as a valid deliberate choice.

---

## 5. Inspecting memory with `duckdb_memory()`

`duckdb_memory()` is a table function that breaks live memory down by tag (UPSTREAM-VERIFIED: https://duckdb.org/2024/07/09/memory-management).

### 5.1 Columns and tags (UPSTREAM-VERIFIED)

Columns: `tag` (VARCHAR), `memory_usage_bytes` (BIGINT), `temporary_storage_bytes` (BIGINT). Tag values include `BASE_TABLE`, `IN_MEMORY_TABLE`, `ART_INDEX`, `HASH_TABLE`, `COLUMN_DATA`, `METADATA`, `OVERFLOW_STRINGS`, `PARQUET_READER`, `CSV_READER`, `ORDER_BY`, `ALLOCATOR`, `EXTENSION`.

```sql
-- Full breakdown.
FROM duckdb_memory();

-- Table vs index footprint, in GB (mirrors the talk).
SELECT tag, memory_usage_bytes / 1e9 AS gb
FROM duckdb_memory()
WHERE tag IN ('IN_MEMORY_TABLE', 'ART_INDEX');
```

The talk's exact usage: "I want the in-memory table tag, which is giving us the memory for our now created table, and the ART memory tag, which gives us the index memory … dividing it by 10 to the nine, which gives me the gigabytes" (talk, same source) — i.e. tags `IN_MEMORY_TABLE` and `ART_INDEX`, divided by `1e9`.

### 5.2 Caveat on what's counted

`PRAGMA database_size` reports `memory_usage` for the database buffer manager but **does not** include index memory or small intermediates (UPSTREAM-VERIFIED: https://duckdb.org/docs/current/configuration/pragmas — "memory usage of indexes is not currently counted by the buffer manager, neither is memory usage of small intermediates"). Use `duckdb_memory()`'s `ART_INDEX` tag for index footprint, not `database_size`. Poll `duckdb_memory()` regularly in production to see "how's my memory doing? What is being allocated at the moment?" (talk, same source).

---

## 6. The method, distilled

1. Enable the profiler (`enable_profiling='json'`, `profiling_coverage='all'`, optional `profiling_output`) or use `EXPLAIN ANALYZE` for a readable one-shot.
2. Read the operator tree. **Confirm `INDEX_SCAN`, not `SEQ_SCAN`.** If it's `SEQ_SCAN`, the index was skipped — check predicate shape (equality/`IN`, single-column, no expression) and selectivity against `MAX(2048, 0.001·N)`.
3. Check memory attribution with `duckdb_memory()` — watch `ART_INDEX` vs `IN_MEMORY_TABLE`.
4. Weigh index cost: non-parallel append maintenance, ~2× memory (talk-reported), no eviction yet (talk-reported), must fit in RAM at creation (verified). Index only for constraints or repeated high-selectivity filters.
5. There is no rule of thumb — **profile and benchmark**; for production, wire the JSON profiles into an observability tool and correlate query time with traffic.

---

## Relevance to core-x

> This is the verification method for confirming that structured queries over the platform's Lance/lakehouse data actually engage indices and pushdown rather than silently full-scanning. In the out-of-core DuckDB→Arrow→Lance-on-R2 path, a filter that *looks* indexed but plans as `SEQ_SCAN` means every predicate pull reads the whole dataset off R2 — the exact cost profile to avoid at object-store latency. Before trusting that a resolution-key filter is cheap, run `EXPLAIN ANALYZE` and confirm the index-scan operator; treat `MAX(2048, 0.001·N)` selectivity and the equality/`IN`/single-column eligibility rules as hard gates on whether a DuckDB ART index (or a Lance scalar index) can be engaged at all. Pair with ../lance/09_scanning_filtering.md for Lance-side pushdown and predicate scanning, and with 06_configuration_memory_spill.md for the memory-budget interaction — ART index memory (roughly 2× table, no eviction yet) competes with query working memory and can force spill on long-running scans.
