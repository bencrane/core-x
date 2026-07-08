# DuckDB — Canonical Reference

> Canonical, source-cited reference for **DuckDB** — the in-process, columnar-vectorized, out-of-core SQL engine that performs 100% of the transform in the core-x plane. Built from live upstream sources fetched **2026-07-08** and adversarially re-verified. Upstream behavior as-is, **not** core-x doctrine. See [`../README.md`](../README.md) for the whole library.

## Versions (verified 2026-07-08)

- **Stable: 1.5.4** (2026-06-17, v1.5 line codename `variegata`).
- **LTS: 1.4.5** (2026-06-17, codename `Andium`).
- Package: `pypi.org/project/duckdb`. Docs: `duckdb.org/docs/stable` (also `/docs/current` for the in-development line, `/docs/lts` for LTS).

Version-gated APIs are pinned per-file (e.g. the `fetch_arrow_*` deprecation, the `VARIANT` type, the native `lance` extension).

## Canonical sources

- **Repo:** `github.com/duckdb/duckdb` · **Docs:** `duckdb.org/docs/stable` · **Package:** `pypi.org/project/duckdb`.
- **`lance` DuckDB extension:** `github.com/lance-format/lance-duckdb` (`INSTALL lance; LOAD lance;`) — see [`13_lance_interop.md`](13_lance_interop.md).

## Files

| # | File | Covers |
|---|---|---|
| 00 | [`00_overview.md`](00_overview.md) | In-process OLAP model, editions, clients, versioning/release lines, install |
| 01 | [`01_python_client.md`](01_python_client.md) | `duckdb.connect`, `execute`/`sql`, parameterized queries, the relational API, replacement scans, `register` |
| 02 | [`02_arrow_integration.md`](02_arrow_integration.md) | `to_arrow_table`/`to_arrow_reader` (+ the deprecated `fetch_arrow_*`), `from_arrow`, ADBC, zero-copy & the Arrow C Data Interface |
| 03 | [`03_csv_import.md`](03_csv_import.md) | `read_csv`/`COPY`, the sniffer, `all_varchar`, `encoding`, `sample_size`, `ignore_errors`, `store_rejects` |
| 04 | [`04_parquet.md`](04_parquet.md) | `read_parquet`, `COPY … TO` (compression/row-group/partitioning), metadata functions, glob reads, pushdown |
| 05 | [`05_json.md`](05_json.md) | `read_json`/`read_json_auto`, formats, JSON functions, casting `JSON → STRUCT`/`MAP`/`LIST` |
| 06 | [`06_configuration_memory_spill.md`](06_configuration_memory_spill.md) | `memory_limit`, `threads`, `temp_directory`, `max_temp_directory_size`, `preserve_insertion_order`, **out-of-core spilling** |
| 07 | [`07_httpfs_s3_r2.md`](07_httpfs_s3_r2.md) | `httpfs`, the S3 API, **Cloudflare R2** (`r2://` vs `s3://` + `url_style`), credential chain |
| 08 | [`08_secrets_manager.md`](08_secrets_manager.md) | `CREATE SECRET`, temporary vs persistent, provider types (s3/r2/gcs/azure/http), `SCOPE`, `which_secret` |
| 09 | [`09_extensions_system.md`](09_extensions_system.md) | `INSTALL`/`LOAD`, autoloading, core vs community repos, signed/unsigned, `duckdb_extensions()` |
| 10 | [`10_core_extensions_catalog.md`](10_core_extensions_catalog.md) | The full official core-extension catalog with per-extension purpose |
| 11 | [`11_quack_extension.md`](11_quack_extension.md) | The `quack` extension **and** the extension template — the real state of extension development (incl. the `quack()`→`waddle()` demo rename) |
| 12 | [`12_sql_essentials.md`](12_sql_essentials.md) | `CAST`/`TRY_CAST`, the type system (`STRUCT`/`LIST`/`MAP`/`VARIANT`), `QUALIFY`, `EXCLUDE`/`REPLACE`, window/dedup idioms |
| 13 | [`13_lance_interop.md`](13_lance_interop.md) | **DuckDB ↔ Lance** — the native `lance` extension (`COPY … (FORMAT lance)`, `lance_vector_search`) *and* the pyarrow zero-copy bridge |
| 14 | [`14_ducklake_lakehouse.md`](14_ducklake_lakehouse.md) | **DuckLake** — the open lakehouse format (catalog + metadata as a SQL DB), `ATTACH 'ducklake:…'`, time travel, metadata-only schema evolution, multi-table ACID, inlining |
| 15 | [`15_ducklake_tuning.md`](15_ducklake_tuning.md) | DuckLake tuning — **`SET SORTED BY` clustering**, partitioning, row-group sizing, ZSTD, inlining thresholds, compaction, **R2 specifics** |
| 16 | [`16_profiling_and_pitfalls.md`](16_profiling_and_pitfalls.md) | Profiling & index pitfalls — `EXPLAIN ANALYZE`, index-scan vs seq-scan, ART index costs, `duckdb_memory()` |
| 17 | [`17_search_over_lakehouse.md`](17_search_over_lakehouse.md) | Search-first retrieval over a lakehouse (Alter Table / Duck Search) — **vendor-reported**, awareness only |

Files 14–17 are folded from the committed talk-transcript corpus ([`../../youtube-transcripts/`](../../youtube-transcripts/), [`../../batches/`](../../batches/)) and verified against live upstream; talk-reported claims are attributed inline.

## core-x load-bearing path

Defensive ingest [`03_csv_import.md`](03_csv_import.md) (`all_varchar` + `TRY_CAST` from [`12`](12_sql_essentials.md)) / [`05_json.md`](05_json.md) (cast to nested types) → configure out-of-core [`06_configuration_memory_spill.md`](06_configuration_memory_spill.md) (`memory_limit` + `temp_directory`) → export zero-copy [`02_arrow_integration.md`](02_arrow_integration.md) (`to_arrow_reader` at scale) → hand to Lance ([`../lance/`](../lance/)). DuckDB-side R2 reads: [`07`](07_httpfs_s3_r2.md)/[`08`](08_secrets_manager.md) — **separate** from Lance `storage_options`. Both engines meet in [`13_lance_interop.md`](13_lance_interop.md).

## Optimizing structured queries over the served data

For multi-hop analytical lookups (point-lookup by key → filtered join → aggregate), the load-bearing sequence: **push predicates into the Lance scanner before DuckDB joins** — DuckDB-over-registered-Lance does *not* engage Lance's indices ([`../lance/10_duckdb_arrow_interop.md`](../lance/10_duckdb_arrow_interop.md), the index-pushdown footgun) → **prove the plan** with `EXPLAIN ANALYZE` / profiling — DuckDB silently falls back to seq-scan ([`16_profiling_and_pitfalls.md`](16_profiling_and_pitfalls.md)) → know the clustering analogue — DuckLake `SET SORTED BY` file/row-group skipping vs Lance BTREE-on-resolution-keys ([`15_ducklake_tuning.md`](15_ducklake_tuning.md), [`../lance/05_scalar_indices.md`](../lance/05_scalar_indices.md)).
