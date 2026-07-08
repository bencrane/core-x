# Canonical Reference Library — Lance & DuckDB

> **What this is.** A canonical, source-cited reference library for the two engines the core-x data plane is built on: **Lance** (the columnar format + `pylance` SDK + LanceDB) and **DuckDB** (the transform engine). Every file was built **from live upstream sources** — official documentation sites, the projects' own source repositories, and PyPI release metadata — fetched **2026-07-08**, and then adversarially re-verified against those same sources.
>
> **What this is *not*.** This is **not** core-x doctrine and not a description of how any particular pipeline is wired. It documents *upstream behavior as-is*. For the opinionated core-x rules (what the SAM.gov worker does, which format version is mandated, the "Python is I/O only" law, etc.) see the numbered doctrine docs under [`../reference/`](../reference/) — but where those disagree with this library on an API signature, parameter, default, or version number, **the fetched-date facts here are ground truth** and the doctrine should be re-verified.

---

## How to use this library

- **Doing Lance work?** Start at [`lance/00_overview.md`](lance/00_overview.md) (packaging + the `pylance` vs `lancedb` split — read it first, it is the #1 source of confusion), then jump to the topic file.
- **Doing DuckDB work?** Start at [`duckdb/00_overview.md`](duckdb/00_overview.md), then the topic file.
- **Building the core-x plane (DuckDB → Arrow → Lance-on-R2)?** The load-bearing path is: [`duckdb/06_configuration_memory_spill.md`](duckdb/06_configuration_memory_spill.md) (out-of-core) → [`duckdb/02_arrow_integration.md`](duckdb/02_arrow_integration.md) (zero-copy export) → [`lance/07_storage_object_stores.md`](lance/07_storage_object_stores.md) (R2 `storage_options`) → [`lance/03_writes_appends_upserts.md`](lance/03_writes_appends_upserts.md) (write/append/upsert) → [`lance/05_scalar_indices.md`](lance/05_scalar_indices.md) (`BTREE` on resolution keys). The two engines meet in [`duckdb/13_lance_interop.md`](duckdb/13_lance_interop.md) and [`lance/10_duckdb_arrow_interop.md`](lance/10_duckdb_arrow_interop.md).

Each file is self-contained: it opens with its own primary-source block and a one-line scope, gives exact signatures with full parameter tables, runnable examples, and an explicit "Deprecations / footguns" and "Unverified / needs confirmation" section where applicable. Files marked **"Relevance to core-x"** call out where an upstream capability is load-bearing for the R2/Lance/DuckDB plane.

---

## Verified current versions (as of 2026-07-08)

| Component | Version | Released | Notes |
|---|---|---|---|
| `pylance` (`import lance`) | **8.0.0** | 2026-07-01 | Low-level Lance format + `Dataset` API. `requires-python >=3.9`. |
| `lancedb` (`import lancedb`) | **0.34.0** | 2026-07-02 | High-level embedded vector DB. `requires-python >=3.10`. |
| DuckDB (stable) | **1.5.4** | 2026-06-17 | v1.5 line codename `variegata`. |
| DuckDB (LTS) | **1.4.5** | 2026-06-17 | LTS line codename `Andium`. |
| `lance` DuckDB extension (`lance-format/lance-duckdb`) | **v0.5.4** | — | Third-party, distributed via the DuckDB core-extension repo (`INSTALL lance`). Verify the tag matching your DuckDB build. |

Version numbers are pinned per-file where an API is version-gated; the table above is the reconciled current state confirmed by the completeness audit against PyPI / the DuckDB releases API.

---

## Authoritative canonical sources

These are the verified upstream locations as of 2026-07-08. Individual files may cite an older-but-redirecting URL in their source blocks; this table is the reconciled truth.

| Project | Source repository | Documentation | Package |
|---|---|---|---|
| **Lance** (format + `pylance`) | `github.com/lance-format/lance` — the older `github.com/lancedb/lance` path **301-redirects** here | **`lance.org`** (current home) — the older `lancedb.github.io/lance/` host still renders the same content | `pypi.org/project/pylance` → `import lance` |
| **LanceDB** (embedded DB) | `github.com/lancedb/lancedb` | `lancedb.github.io/lancedb/` · `docs.lancedb.com` | `pypi.org/project/lancedb` → `import lancedb` |
| **DuckDB** | `github.com/duckdb/duckdb` | `duckdb.org/docs/stable` (also `/docs/current`, `/docs/lts`) | `pypi.org/project/duckdb` |
| **`lance` DuckDB extension** | `github.com/lance-format/lance-duckdb` | DuckDB core-extensions page + repo `docs/` | `INSTALL lance; LOAD lance;` |

> **`pylance` ≠ `lancedb`.** `pip install pylance` gives you `import lance` (the low-level format/dataset API that core-x uses for `write_dataset`). `pip install lancedb` gives you `import lancedb` (the high-level DB) and **does not** re-export the `lance` module. This is the single most common footgun — see [`lance/00_overview.md`](lance/00_overview.md).

---

## Contents

### Lance — [`lance/`](lance/) · [index](lance/README.md)

| File | Covers |
|---|---|
| [`00_overview.md`](lance/00_overview.md) | What Lance/LanceDB are, `pylance` vs `lancedb` packaging, ecosystem, versions, hello-world |
| [`01_file_format.md`](lance/01_file_format.md) | The Lance columnar file format (v2/2.1) & on-disk dataset layout (fragments, manifests, `_versions`/`_transactions`/`_indices`) |
| [`02_python_dataset_api.md`](lance/02_python_dataset_api.md) | `lance.dataset`, `lance.write_dataset`, the `LanceDataset` read surface — full signatures |
| [`03_writes_appends_upserts.md`](lance/03_writes_appends_upserts.md) | Write modes, `merge_insert`, `delete`/`update`, `add_columns`, `LanceOperation` + `commit`, OCC |
| [`04_versioning_time_travel.md`](lance/04_versioning_time_travel.md) | Versions, time travel, tags, `cleanup_old_versions` |
| [`05_scalar_indices.md`](lance/05_scalar_indices.md) | `create_scalar_index`: `BTREE`, `BITMAP`, `LABEL_LIST`, `INVERTED`/FTS, `NGRAM` |
| [`06_vector_search.md`](lance/06_vector_search.md) | Vector indices & ANN search (`IVF_PQ`/`HNSW`, `nprobes`, `refine`, multivector) |
| [`07_storage_object_stores.md`](lance/07_storage_object_stores.md) | `storage_options` for S3 / **Cloudflare R2** / GCS / Azure; commit stores |
| [`08_compaction_maintenance.md`](lance/08_compaction_maintenance.md) | Compaction, index optimization, fragment management |
| [`09_scanning_filtering.md`](lance/09_scanning_filtering.md) | Scanner, filter-expression language, projection/predicate pushdown, `take()` |
| [`10_duckdb_arrow_interop.md`](lance/10_duckdb_arrow_interop.md) | Arrow / DuckDB / Polars interop; reading Lance from query engines |
| [`11_lancedb_table_api.md`](lance/11_lancedb_table_api.md) | LanceDB (the DB): `connect`, tables, `add`/`search`, FTS, hybrid, cloud/remote |

### DuckDB — [`duckdb/`](duckdb/) · [index](duckdb/README.md)

| File | Covers |
|---|---|
| [`00_overview.md`](duckdb/00_overview.md) | What DuckDB is, editions, clients, versioning/release lines |
| [`01_python_client.md`](duckdb/01_python_client.md) | `connect`, `execute`, the relational API, replacement scans |
| [`02_arrow_integration.md`](duckdb/02_arrow_integration.md) | `to_arrow_table`/`to_arrow_reader`, `from_arrow`, `register`, ADBC, zero-copy |
| [`03_csv_import.md`](duckdb/03_csv_import.md) | `read_csv`/`COPY`, `all_varchar`, encoding, `sample_size`, `ignore_errors`, rejects |
| [`04_parquet.md`](duckdb/04_parquet.md) | `read_parquet`, `COPY TO`, metadata, partitioning, pushdown |
| [`05_json.md`](duckdb/05_json.md) | `read_json`, formats, JSON functions, nested `STRUCT`/`MAP`/`LIST` casting |
| [`06_configuration_memory_spill.md`](duckdb/06_configuration_memory_spill.md) | `memory_limit`, `threads`, `temp_directory`, **out-of-core spilling** |
| [`07_httpfs_s3_r2.md`](duckdb/07_httpfs_s3_r2.md) | `httpfs`, the S3 API & **Cloudflare R2** |
| [`08_secrets_manager.md`](duckdb/08_secrets_manager.md) | `CREATE SECRET`, types (s3/r2/gcs/azure/http), persistence |
| [`09_extensions_system.md`](duckdb/09_extensions_system.md) | `INSTALL`/`LOAD`, autoloading, core vs community, signing |
| [`10_core_extensions_catalog.md`](duckdb/10_core_extensions_catalog.md) | The full official core-extensions catalog |
| [`11_quack_extension.md`](duckdb/11_quack_extension.md) | The `quack` extension & the extension template (how DuckDB extensions work) |
| [`12_sql_essentials.md`](duckdb/12_sql_essentials.md) | `TRY_CAST`, types (`STRUCT`/`LIST`/`MAP`/`VARIANT`), `QUALIFY`, window functions |
| [`13_lance_interop.md`](duckdb/13_lance_interop.md) | **DuckDB ↔ Lance** — the native `lance` extension *and* the pyarrow bridge |
| [`14_ducklake_lakehouse.md`](duckdb/14_ducklake_lakehouse.md) | **DuckLake** — the open lakehouse format (catalog + metadata as a SQL DB) |
| [`15_ducklake_tuning.md`](duckdb/15_ducklake_tuning.md) | DuckLake tuning — `SET SORTED BY` clustering, partitioning, inlining, R2 specifics |
| [`16_profiling_and_pitfalls.md`](duckdb/16_profiling_and_pitfalls.md) | Profiling & index pitfalls — `EXPLAIN ANALYZE`, ART index costs, memory |
| [`17_search_over_lakehouse.md`](duckdb/17_search_over_lakehouse.md) | Search-first retrieval over a lakehouse (**vendor-reported**, awareness only) |

---

## The talk-transcript corpus (primary-source provenance layer)

A committed corpus of 17 talk/video transcripts (three layers each: verbatim ASR → **faithful clean** → editorialized) plus 3 reproduced articles lives at [`../youtube-transcripts/`](../youtube-transcripts/) and [`../batches/`](../batches/), indexed by [`../INDEX.md`](../INDEX.md). It is the primary-source layer behind `duckdb/14–17` and the corpus-folded additions to `duckdb/00`, `10`, `11`, `12` and `lance/10` (DuckCon #7, the Quack launch, DuckLake, extension workshops — much of it newer than any published docs). Canonical files cite it by transcript path; the **clean** layer is the citable one. Talk-reported claims are always qualified as such — where upstream docs exist, the canonical file verifies against them and the upstream citation wins.

## Maintenance

These are point-in-time snapshots (core library fetched 2026-07-08; corpus fold-in July 2026). Upstream moves — signatures get added, defaults change, versions ship. When you rely on a file for a load-bearing decision, re-verify the specific signature/default against the source URL in that file's header. Regenerate any file by re-fetching its cited primary sources; a file's "Unverified / needs confirmation" section names exactly what was not pinned at authoring time.
