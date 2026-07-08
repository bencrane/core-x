# Configuration — memory_limit, threads, temp_directory, out-of-core spilling

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/current/configuration/overview.html — the authoritative list of configuration options, their defaults, types, and scope; how to SET/RESET/query settings.
> - https://duckdb.org/docs/current/configuration/pragmas.html — PRAGMA statements (memory_limit, threads, temp_directory, version, database_size, checkpoint, object cache, progress bar).
> - https://duckdb.org/docs/current/guides/performance/environment.html — environment/hardware guidance (RAM per thread, disk type, thread limiting).
> - https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads.html — larger-than-memory execution, which operators spill, preserve_insertion_order, parallelism tuning.
> - https://duckdb.org/docs/current/clients/python/overview.html — the `config=` dict on `duckdb.connect()`.
> - https://github.com/duckdb/duckdb/releases and https://pypi.org/project/duckdb/ — current released versions.

Scope: How to configure DuckDB's memory, threads, and temp/spill directory, and how DuckDB executes larger-than-memory (out-of-core) workloads by spilling blocking operators to disk — the exact knobs a constrained/ephemeral worker needs.

---

## Current released versions (as of 2026-07-08)

| Component | Version | Notes |
|---|---|---|
| DuckDB (stable) | **1.5.4** (codename *Variegata*), released 2026-06-17 | Latest non-LTS stable release line. |
| DuckDB (LTS) | **1.4.5** (codename *Andium*), released 2026-06-17 | Long-Term Support line; bugfix-only. |
| `duckdb` (PyPI) | tracks the DuckDB release (1.5.x / 1.4.x LTS) | `pip install duckdb` |

The two lines ship in lockstep on the same date: pick **1.5.x** for newest features, **1.4.x LTS** for a frozen, patch-only surface. Configuration option names and defaults below are stable across both lines unless a footgun note says otherwise.

> Sibling files in this domain: `00_overview.md` (editions, clients, versioning), `01_python_client.md` (`connect`/`execute`/relational API — full `connect()` signature lives there), `02_arrow_integration.md`, `07_httpfs_s3_r2.md`, `08_secrets_manager.md`, `13_lance_interop.md`.

---

## 1. Setting configuration

Four interchangeable ways to set an option. `SET` and `PRAGMA` are functionally equivalent for configuration options.

```sql
-- SET (two accepted forms; '=' and TO are equivalent)
SET memory_limit = '8GB';
SET memory_limit TO '8GB';

-- PRAGMA (equivalent to SET for config options)
PRAGMA memory_limit = '8GB';

-- Reset an option to its default
RESET memory_limit;
```

Set options **at connect time** with the `config` dictionary (Python shown; same concept in other clients):

```python
import duckdb

con = duckdb.connect(
    database=":memory:",
    config={
        "threads": 4,
        "memory_limit": "8GB",
        "temp_directory": "/mnt/nvme/duck.tmp",
    },
)
```

> The exact full `duckdb.connect(database, read_only, config, ...)` signature is documented in `01_python_client.md`. The overview page verbatim states: *"The `duckdb.connect()` accepts a config dictionary, where configuration options can be specified."* Example given: `duckdb.connect(config = {'threads': 1})`.

### Query current values

```sql
-- Single option
SELECT current_setting('memory_limit') AS mem;

-- Full settings table (name, value, description, input_type, scope)
SELECT * FROM duckdb_settings() WHERE name = 'memory_limit';
SELECT name, value, scope FROM duckdb_settings() ORDER BY name;
```

`duckdb_settings()` is a table function returning columns: **`name`, `value`, `description`, `input_type`, `scope`**.

### Scope: GLOBAL vs SESSION

The `SET`/`RESET` statement supports two **runtime** scopes (per the SET statement page, https://duckdb.org/docs/current/sql/statements/set):

- **GLOBAL** — *"used (or reset) across the entire DuckDB instance."*
- **SESSION** — *"used (or reset) only for the current session attached to a DuckDB instance."*

> **Correction (verified 2026-07-08):** `LOCAL` is **not** a working scope. The SET statement page lists `LOCAL` only as a keyword marked *"Not yet implemented"* — `SET LOCAL …` is not usable. Do not treat `SESSION` as "an alias for `LOCAL`"; `SESSION` is the real per-connection scope and `LOCAL` is unimplemented.

When no scope is specified, the option's default scope is used (GLOBAL for most options). The `scope` column of `duckdb_settings()` reports each option's default scope (an internal enum that can read `GLOBAL` or `LOCAL`) — this is metadata about the option, distinct from the `GLOBAL`/`SESSION` keywords accepted by the `SET` statement. Resource knobs like `memory_limit`, `threads`, and `temp_directory` are **GLOBAL** — set them once per instance. You may target a scope explicitly:

```sql
SET GLOBAL memory_limit = '8GB';
SET SESSION default_collation = 'nocase';   -- SESSION = current-connection scope
```

---

## 2. Memory + compute knobs (with real defaults)

Defaults below are copied from the configuration overview page. `memory_limit` and `max_memory` are aliases; `threads` and `worker_threads` are aliases.

| Option | Alias | Default | Type | What it controls |
|---|---|---|---|---|
| `memory_limit` | `max_memory` | **80% of RAM** | VARCHAR | Buffer-manager memory budget. |
| `threads` | `worker_threads` | **# CPU cores** | BIGINT | Number of worker threads for parallel execution. |
| `temp_directory` | — | `⟨database_name⟩.tmp` (persistent) or `.tmp` (in-memory) | VARCHAR | Directory DuckDB spills to when exceeding `memory_limit`. |
| `max_temp_directory_size` | — | **90% of available disk space** | VARCHAR | Cap on how much DuckDB may write to `temp_directory`. |
| `preserve_insertion_order` | — | **true** | BOOLEAN | Preserve row order on read/write; set `false` to reduce memory for large exports. |
| `external_threads` | — | **1** | UBIGINT | Threads for processing external work (outside the main pool). |
| `allocator_flush_threshold` | — | **128.0 MiB** | VARCHAR | Threshold at which the allocator flushes freed memory back to the OS. |
| `allocator_bulk_deallocation_flush_threshold` | — | **512.0 MiB** | VARCHAR | Threshold for bulk-deallocation flush to the OS. |
| `allocator_background_threads` | — | **false** | BOOLEAN | Whether the allocator runs a background thread to return memory to the OS. |
| `enable_object_cache` | — | **false** | BOOLEAN | Cache objects (e.g. Parquet metadata) across queries. |

### Notes on the memory knobs

- **`memory_limit` applies to the buffer manager only.** The pragmas page states verbatim: *"The specified memory limit is only applied to the buffer manager"* — it does **not** cover per-query vectors, materialized query results, or certain complex aggregate functions. Actual RSS can exceed `memory_limit`; budget headroom on a hard-capped worker.
- Accepted size units follow the human-readable form: `'8GB'`, `'8GiB'`, `'512MB'`, `'80%'` (percentage of RAM). Use an explicit absolute value on a constrained worker rather than a percentage — a container cgroup limit may differ from what DuckDB detects as "RAM."
- **`threads`** defaults to the detected core count. On a small worker DuckDB *"may launch too many threads"*; the tuning guide explicitly advises `SET threads = X` to cap it. Each thread needs memory (see §5).
- **`external_threads`** (default 1) is separate from the worker pool and rarely needs tuning.

---

## 3. Out-of-core / larger-than-memory execution

DuckDB is designed to process datasets larger than RAM by **spilling intermediate state to `temp_directory`** when a query would otherwise exceed `memory_limit`. This is the mechanism that makes hundreds-of-millions-of-rows pipelines run on small workers.

### Which operators spill

The workload-tuning guide lists the **blocking operators** that support larger-than-memory (out-of-core) processing:

- **`GROUP BY`** (hash aggregation)
- **`JOIN`** (hash join)
- **`ORDER BY`** (sort)
- **`OVER (PARTITION BY … ORDER BY …)`** (windowing)

When these operators' working set exceeds the buffer-manager budget, DuckDB streams the overflow to files under `temp_directory` and reads them back — no OOM, at the cost of disk I/O.

### Known limitations (from the guide, verbatim points)

- *"Multiple blocking operators … may still throw an out-of-memory exception"* — a single query with several concurrent spilling operators can still exceed the budget.
- Aggregate functions like **`list()`** and **`string_agg()`** do **not** support disk offloading — they buffer their full result in memory.
- Aggregate functions that **use sorting** need all inputs before aggregation can start.
- **`PIVOT`** inherits the `list()` limitation.

### `preserve_insertion_order` for large exports

For import/export of datasets larger than memory, the guide recommends:

```sql
SET preserve_insertion_order = false;
```

This lets DuckDB reorder results, *"potentially reducing memory usage."* The tradeoff: output row order is no longer guaranteed to match input order. For a pipeline that writes to an unordered sink (e.g. Arrow → Lance fragments), this is usually free memory savings.

### Guidance for constrained / ephemeral workers

On a memory-capped or ephemeral worker, **always set both** `memory_limit` and `temp_directory`:

1. `memory_limit` — cap the buffer manager below the container's hard memory limit so the OS OOM-killer never fires.
2. `temp_directory` — point spill at fast local disk (NVMe/SSD), sized for the largest intermediate state.
3. `max_temp_directory_size` — optionally cap spill so a runaway query fails cleanly instead of filling the disk.
4. `threads` — cap to keep per-thread memory (§5) within budget.

Disk guidance from the environment page: **SSD/NVMe strongly preferred** (HDD "supported but poor performance"); on Linux **XFS** is recommended; **avoid** DuckDB's native format in read-write mode on network-attached storage (NAS). For a Modal/ephemeral worker, that means the local NVMe scratch, not a mounted network volume.

---

## 4. Useful pragmas

| Statement | Purpose |
|---|---|
| `SET memory_limit = '8GB';` / `PRAGMA memory_limit = '8GB';` | Set buffer-manager budget. |
| `SET threads = 4;` / `PRAGMA threads = 4;` | Set worker thread count. |
| `SET temp_directory = '/mnt/nvme/duck.tmp/';` | Set spill directory. |
| `PRAGMA enable_progress_bar;` / `PRAGMA enable_print_progress_bar;` | Turn on the query progress bar (CLI/interactive). |
| `PRAGMA disable_progress_bar;` | Turn it off. |
| `PRAGMA database_size;` / `CALL pragma_database_size();` | Storage/size info for the attached database(s). |
| `PRAGMA version;` / `CALL pragma_version();` | DuckDB library version + source id. |
| `PRAGMA platform;` / `CALL pragma_platform();` | Platform string (used for extension resolution). |
| `PRAGMA force_checkpoint;` | Force a checkpoint (flush WAL into the main DB file), even with active transactions. |
| `PRAGMA enable_checkpoint_on_shutdown;` / `PRAGMA disable_checkpoint_on_shutdown;` | Control checkpoint at shutdown. |
| `PRAGMA enable_object_cache;` / `PRAGMA disable_object_cache;` | Toggle cross-query object cache (e.g. Parquet metadata). Default off. |
| `PRAGMA table_info('t');` / `CALL pragma_table_info('t');` | Column metadata: `cid, name, type, notnull, dflt_value, pk`. |
| `PRAGMA storage_info('t');` | Low-level storage/column-segment info for a table. |
| `PRAGMA database_list;` | List attached databases. |
| `PRAGMA show_tables;` / `PRAGMA show_tables_expanded;` | List tables (expanded adds schema/column detail). |
| `PRAGMA disable_optimizer;` | Turn off the query optimizer (debugging only). |

```sql
-- Inspect version and size at the start of a job
PRAGMA version;
PRAGMA database_size;

-- Force a checkpoint after a large load into a persistent DB
PRAGMA force_checkpoint;
```

> **Footgun:** the standalone `PRAGMA memory_limit=X` and `PRAGMA threads=X` forms are legacy but still supported; prefer `SET`. `enable_object_cache` defaults to **false** — for repeated scans over the same Parquet files, enabling it avoids re-reading footer metadata each query, but it is off by default. `PRAGMA enable_object_cache` / `disable_object_cache` remain documented on the current pragmas page as the object-cache toggle. Note (verified 2026-07-08): upstream PR #15129 introduced a parquet-extension setting `parquet_metadata_cache` (set via `SET parquet_metadata_cache = true`) as the more specific Parquet-metadata cache; it is not yet reflected on the current pragmas page. If a target build does not recognize `enable_object_cache`, check for `parquet_metadata_cache`.

---

## 5. RAM-per-thread sizing (environment guide)

The environment page gives concrete memory budgeting rules — critical when co-sizing `memory_limit` and `threads`:

- **Hard minimum: 125 MB of memory per thread.** Below this DuckDB cannot run reliably.
- **Recommended: 1–4 GB memory per thread.**
  - Aggregation-heavy workloads: **1–2 GB/thread**.
  - Join-heavy workloads: **3–4 GB/thread**.

Implication for a fixed `memory_limit`: `threads ≈ memory_limit / (1–4 GB)`. Setting more threads than the memory budget supports pushes work to spill (slower) or risks OOM on the operators that don't spill.

**Parallelism floor:** parallelism kicks in per row group. The guide notes DuckDB needs to scan at least **k × 122,880 rows** to use `k` threads. Small inputs won't parallelize regardless of `threads`. Row group size is set at attach time: `ATTACH '/tmp/file.db' AS db (ROW_GROUP_SIZE 16384);`.

---

## 6. Example: 8 GiB ephemeral worker on NVMe

Target: a worker with **8 GiB RAM cap** and a **local NVMe scratch mount** at `/mnt/nvme`, running an out-of-core scan/join/sort/aggregate over hundreds of millions of rows.

### SQL / CLI

```sql
-- Cap buffer manager below the 8 GiB container limit (leave headroom for
-- vectors/results, which memory_limit does NOT cover).
SET memory_limit = '6GB';

-- 6 GB / ~3 GB-per-thread (join-heavy) -> keep threads modest.
SET threads = 2;

-- Spill to fast local NVMe, not a network volume.
SET temp_directory = '/mnt/nvme/duck.tmp';

-- Fail cleanly instead of filling the scratch disk.
SET max_temp_directory_size = '40GB';

-- Large export where row order doesn't matter -> save memory.
SET preserve_insertion_order = false;
```

### Python (config at connect time — recommended for ephemeral workers)

```python
import duckdb

con = duckdb.connect(
    database=":memory:",
    config={
        "memory_limit": "6GB",
        "threads": 2,
        "temp_directory": "/mnt/nvme/duck.tmp",
        "max_temp_directory_size": "40GB",
        "preserve_insertion_order": False,
    },
)

# Verify the applied settings
print(con.sql("""
    SELECT name, value
    FROM duckdb_settings()
    WHERE name IN ('memory_limit','threads','temp_directory',
                   'max_temp_directory_size','preserve_insertion_order')
    ORDER BY name
""").fetchall())
```

Rationale: `memory_limit` sits below the 8 GiB cap so the OS OOM-killer never fires even though DuckDB's real RSS can exceed the buffer budget; `threads=2` respects the ~3 GB/thread join budget out of 6 GB; spill goes to NVMe; `max_temp_directory_size` bounds the blast radius; `preserve_insertion_order=false` trims memory on the write path.

---

## 7. Unverified / needs confirmation

- **`enable_progress_bar` default value** — the overview settings table did not list an explicit default for `enable_progress_bar`; the pragmas page documents `PRAGMA enable_progress_bar` / `disable_progress_bar` as toggles. Treat the enabled/disabled default as environment-dependent (interactive CLI vs. embedded) and confirm via `SELECT current_setting('enable_progress_bar')` in the target build rather than assuming.
- **Exact `duckdb.connect()` full signature** — the Python API-reference page redirected and the full `connect(database, read_only, config, ...)` parameter list was not captured here; it is documented in `01_python_client.md`. The `config={...}` dict form is confirmed from the Python overview page.
- **`allocator_flush_threshold` unit rendering** — confirmed default `128.0 MiB`; exact accepted input forms (bytes vs `MiB` string) not exhaustively enumerated on the fetched page.

---

> **Relevance to core-x:** On Modal workers running the out-of-core `DuckDB → Arrow → Lance-on-R2` pipeline, `memory_limit` + `temp_directory` are the two load-bearing knobs. Set `memory_limit` explicitly **below** the Modal container's memory cap (percentage auto-detection can misread cgroup limits and trigger the OOM-killer), and point `temp_directory` at the worker's **local NVMe scratch** (never a network/R2-backed mount — DuckDB spill is random-access and must be local). Cap `threads` to the `memory_limit / 3 GB` join budget, and set `preserve_insertion_order = false` on the write path into Lance fragments — Lance fragments are append-only and unordered, so preserving DuckDB insertion order buys nothing and costs memory. The blocking operators that spill (`GROUP BY`, `JOIN`, `ORDER BY`, window) are exactly the ones a large resolution/dedup pass hits; `max_temp_directory_size` bounds a runaway job to a clean failure instead of a full-disk crash. See `07_httpfs_s3_r2.md` for R2 read/write and `13_lance_interop.md` for the zero-copy Arrow hand-off into Lance.
