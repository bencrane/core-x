# Lance — Canonical Reference

> Canonical, source-cited reference for **Lance** — the columnar data/table format, the low-level `pylance` Python SDK (`import lance`), and the higher-level `lancedb` embedded database. Built from live upstream sources fetched **2026-07-08** and adversarially re-verified. Upstream behavior as-is, **not** core-x doctrine. See [`../README.md`](../README.md) for the whole library and the authoritative canonical-source table.

## Read this first: two packages, two layers

| | Low-level format | High-level database |
|---|---|---|
| **PyPI package** | `pylance` | `lancedb` |
| **Import** | `import lance` | `import lancedb` |
| **Version (2026-07-08)** | **8.0.0** (2026-07-01) | **0.34.0** (2026-07-02) |
| **Core object** | `lance.LanceDataset` | `lancedb.DBConnection` → `Table` |
| **core-x uses** | **this** — `write_dataset`, fragments, scalar indices, time travel | for completeness / table-API use cases |

**`lancedb` does not re-export `lance`.** Installing `lancedb` does not give you `import lance`. A worker that calls `lance.write_dataset(...)` must depend on `pylance`. Full detail in [`00_overview.md`](00_overview.md).

## Canonical sources (verified 2026-07-08)

- **Repo:** `github.com/lance-format/lance` — the old `github.com/lancedb/lance` path **301-redirects** here.
- **Docs:** `lance.org` (current home) — the older `lancedb.github.io/lance/` host still renders the same content.
- **LanceDB:** `github.com/lancedb/lancedb` · `lancedb.github.io/lancedb/` · `docs.lancedb.com`.
- **Packages:** `pypi.org/project/pylance` · `pypi.org/project/lancedb`.

(Some files below cite the older `lancedb/lance` / `lancedb.github.io` URLs in their source blocks — they resolve to the same content via the redirect.)

## Files

| # | File | Covers |
|---|---|---|
| 00 | [`00_overview.md`](00_overview.md) | Packaging (`pylance` vs `lancedb`), ecosystem, bindings, versions, hello-world, library map |
| 01 | [`01_file_format.md`](01_file_format.md) | The `.lance` columnar file format (v2/2.1), encodings; on-disk dataset layout — fragments, manifests, `_versions/`, `_transactions/`, `_indices/`, deletion vectors, row ids |
| 02 | [`02_python_dataset_api.md`](02_python_dataset_api.md) | `lance.dataset()`, `lance.write_dataset()` (full signature + every param), the `LanceDataset` read surface (`scanner`, `to_table`, `take`, `count_rows`, …) |
| 03 | [`03_writes_appends_upserts.md`](03_writes_appends_upserts.md) | `create`/`append`/`overwrite`, `merge_insert` (SQL-MERGE upsert), `delete`/`update`, `add_columns`/`alter_columns`, `LanceOperation` + `commit`, optimistic concurrency |
| 04 | [`04_versioning_time_travel.md`](04_versioning_time_travel.md) | `versions()`, time travel (`version=`/`asof=`/`checkout`), `restore`, tags, `cleanup_old_versions` |
| 05 | [`05_scalar_indices.md`](05_scalar_indices.md) | `create_scalar_index`: `BTREE`, `BITMAP`, `LABEL_LIST`, `INVERTED`/FTS, `NGRAM`; index management + prefilter pushdown |
| 06 | [`06_vector_search.md`](06_vector_search.md) | `create_index` vector types (`IVF_FLAT/PQ/SQ`, `IVF_HNSW_*`), ANN query (`nearest`, `nprobes`, `refine_factor`), multivector |
| 07 | [`07_storage_object_stores.md`](07_storage_object_stores.md) | `storage_options` keys for S3 / **Cloudflare R2** / GCS / Azure; credential resolution; commit stores for concurrent writers |
| 08 | [`08_compaction_maintenance.md`](08_compaction_maintenance.md) | `dataset.optimize.compact_files`, `optimize_indices`, fragment sizing, deletion materialization |
| 09 | [`09_scanning_filtering.md`](09_scanning_filtering.md) | `scanner(...)` params, the SQL-like filter language, projection/predicate pushdown, `take()`, streaming `to_batches` |
| 10 | [`10_duckdb_arrow_interop.md`](10_duckdb_arrow_interop.md) | Arrow zero-copy; reading Lance from DuckDB / Polars / pandas / engines |
| 11 | [`11_lancedb_table_api.md`](11_lancedb_table_api.md) | `lancedb.connect`, table lifecycle, `.search()` (vector/FTS/hybrid + rerankers), DB-layer indexing, embedding registry |

## core-x load-bearing path

`DuckDB Arrow` → [`07_storage_object_stores.md`](07_storage_object_stores.md) (R2 `storage_options`) → [`02`](02_python_dataset_api.md)/[`03`](03_writes_appends_upserts.md) (`write_dataset` / append / `merge_insert`) → [`05_scalar_indices.md`](05_scalar_indices.md) (`BTREE` on every resolution key, `BITMAP` on categoricals) → [`04`](04_versioning_time_travel.md)/[`08`](08_compaction_maintenance.md) (`cleanup_old_versions` + compaction to bound R2 growth). Read-back into DuckDB: [`10_duckdb_arrow_interop.md`](10_duckdb_arrow_interop.md).
