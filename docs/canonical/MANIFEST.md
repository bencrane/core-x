# MANIFEST — Canonical Lance/DuckDB Knowledge Area

Flat, objective inventory of every file in the canonical reference library and its primary-source corpus. Paths are repo-relative. No recommendations or reading order here — see the READMEs for navigation. Facts only.

**Entry points:** [`README.md`](README.md) (library index) · [`lance/README.md`](lance/README.md) · [`duckdb/README.md`](duckdb/README.md) · [`../INDEX.md`](../INDEX.md) (corpus index) · this file (inventory).

**Provenance vocabulary** (used inside every file):

| Tag | Meaning |
|---|---|
| upstream-verified | Confirmed against an official documentation site or source repository; the URL is cited at the claim |
| talk-reported | Stated by a speaker in a transcript; attributed to talk + date + transcript path; not independently confirmed upstream |
| vendor-reported | Claimed by a third-party vendor about its own product; no neutral confirmation exists |
| Unverified / needs confirmation | Explicitly flagged section naming what could not be pinned at authoring time |

**Dates:** core library authored from live upstream fetched **2026-07-08**. Corpus fold-in performed **July 2026**. Versions pinned at authoring: pylance 8.0.0 · lancedb 0.34.0 · duckdb 1.5.4 stable / 1.4.5 LTS.

---

## 1. Canonical library — Lance (`docs/canonical/lance/`, 13 files)

| Path | Title (verbatim H1) | Source basis |
|---|---|---|
| `lance/README.md` | Lance — Canonical Reference | index file |
| `lance/00_overview.md` | Lance & LanceDB — Overview, Ecosystem, Packaging & Versions | upstream (lance.org, GitHub, PyPI) |
| `lance/01_file_format.md` | The Lance Columnar File Format & On-Disk Dataset Layout | upstream (format spec, protos, repo docs) |
| `lance/02_python_dataset_api.md` | pylance Python SDK — lance.dataset, lance.write_dataset, LanceDataset | upstream (API ref + source at v8.0.0) |
| `lance/03_writes_appends_upserts.md` | Writing Data — modes, append, merge_insert, delete, update, add_columns, LanceOperation & commits | upstream (API ref + source) |
| `lance/04_versioning_time_travel.md` | Versioning, Time Travel, Tags & cleanup_old_versions | upstream (API ref + source) |
| `lance/05_scalar_indices.md` | Scalar Indices — BTREE, BITMAP, LABEL_LIST, INVERTED/FTS, NGRAM (and any others) | upstream (API ref + source) |
| `lance/06_vector_search.md` | Vector Indices & ANN Search — IVF_PQ / HNSW, nprobes, refine, multivector | upstream (API ref + docs) |
| `lance/07_storage_object_stores.md` | Object Store Configuration — storage_options for S3 / Cloudflare R2 / GCS / Azure | upstream (docs + source) |
| `lance/08_compaction_maintenance.md` | Dataset Maintenance — compaction, index optimization, fragment management | upstream (API ref + source) |
| `lance/09_scanning_filtering.md` | Scanning, Filtering, Projection Pushdown & take() | upstream (API ref + source) |
| `lance/10_duckdb_arrow_interop.md` | Interop — Apache Arrow, DuckDB, Polars/pandas; reading Lance from query engines | upstream + corpus fold-in (index-pushdown note; 2023 DuckCon #3 historical note) |
| `lance/11_lancedb_table_api.md` | LanceDB (the database) — connect, tables, add/search, FTS, cloud/remote | upstream (lancedb docs + PyPI) |

## 2. Canonical library — DuckDB (`docs/canonical/duckdb/`, 19 files)

| Path | Title (verbatim H1) | Source basis |
|---|---|---|
| `duckdb/README.md` | DuckDB — Canonical Reference | index file |
| `duckdb/00_overview.md` | DuckDB — Overview, Editions, Clients, Versioning & Release Lines | upstream + corpus fold-in (2.0 roadmap, DuckLabs rename, adoption stats — talk-reported) |
| `duckdb/01_python_client.md` | DuckDB Python Client — connect, execute, relational API, replacement scans | upstream (duckdb.org clients/python) |
| `duckdb/02_arrow_integration.md` | Apache Arrow Integration — to_arrow_table/to_arrow_reader, from_arrow, register, ADBC | upstream (duckdb.org guides + stubs) |
| `duckdb/03_csv_import.md` | CSV Import — read_csv, COPY, options (all_varchar, encoding, sample_size, ignore_errors, rejects) | upstream (duckdb.org data/csv) |
| `duckdb/04_parquet.md` | Parquet — read_parquet, COPY TO, metadata, partitioning, pushdown | upstream (duckdb.org data/parquet) |
| `duckdb/05_json.md` | JSON — read_json/read_json_auto, formats, JSON functions, nested casting | upstream (duckdb.org data/json) |
| `duckdb/06_configuration_memory_spill.md` | Configuration — memory_limit, threads, temp_directory, out-of-core spilling | upstream (duckdb.org configuration + performance) |
| `duckdb/07_httpfs_s3_r2.md` | httpfs, S3 API & Cloudflare R2 — reading/writing object storage | upstream (duckdb.org core_extensions/httpfs) |
| `duckdb/08_secrets_manager.md` | Secrets Manager — CREATE SECRET, types (s3/r2/gcs/azure/http), persistence | upstream (duckdb.org secrets manager) |
| `duckdb/09_extensions_system.md` | Extension System — INSTALL/LOAD, autoloading, core vs community, signing | upstream (duckdb.org extensions) |
| `duckdb/10_core_extensions_catalog.md` | Core Extensions Catalog — the full official list with purpose | upstream + corpus fold-in (ducklake row expanded; counts — talk-reported) |
| `duckdb/11_quack_extension.md` | The quack Extension & the DuckDB Extension Template (how DuckDB extensions work) | upstream + corpus fold-in (protocol benchmarks, multi-writer, Quack-as-DuckLake-catalog, install channel) |
| `duckdb/12_sql_essentials.md` | SQL Essentials for Pipelines — TRY_CAST, types (STRUCT/LIST/MAP/VARIANT), QUALIFY, window | upstream + corpus fold-in (function chaining) |
| `duckdb/13_lance_interop.md` | DuckDB ↔ Lance Interop — the verified reality of reading/writing Lance from DuckDB | upstream (duckdb.org lance extension page, lance-duckdb repo) |
| `duckdb/14_ducklake_lakehouse.md` | DuckLake — The Open Lakehouse Format (catalog + metadata as a SQL database) | corpus fold-in, upstream-verified where docs exist (ducklake.select) |
| `duckdb/15_ducklake_tuning.md` | DuckLake Tuning & Sorted Tables — going fast on object storage (incl. R2) | corpus fold-in, upstream-verified where docs exist (ducklake.select) |
| `duckdb/16_profiling_and_pitfalls.md` | Profiling & Index Pitfalls — proving a query is fast (EXPLAIN ANALYZE, ART indexes, memory) | corpus fold-in, upstream-verified (duckdb.org profiling/indexes) |
| `duckdb/17_search_over_lakehouse.md` | Search-First Retrieval over a Lakehouse (Alter Table / Duck Search — VENDOR-REPORTED) | corpus only — vendor-reported throughout; no neutral upstream exists |

## 3. Library index files (`docs/canonical/`, 2 files)

| Path | Contents |
|---|---|
| `README.md` | Library-wide index: authoritative source table, verified version matrix, per-file contents tables, corpus-provenance section, maintenance notes |
| `MANIFEST.md` | This file |

## 4. Primary-source corpus (`docs/youtube-transcripts/`, 53 files; `docs/batches/`, 3 files)

Indexed in full by [`../INDEX.md`](../INDEX.md) (17-row talk table with dates and descriptions; layer-selection rules).

| Location | Count | Layer | Fidelity |
|---|---|---|---|
| `youtube-transcripts/raw/*.raw.txt` | 17 | Verbatim ASR + timestamps | Literal record; ASR-corrupted terms |
| `youtube-transcripts/clean/*.clean.md` | 17 | Faithful cleanup | Every substantive word kept; ASR errors fixed. **The citable layer** — canonical files cite these paths |
| `youtube-transcripts/*.md` | 17 | Editorialized digest | Restructured/summarized; not quotable as speaker's words |
| `youtube-transcripts/clean/_METHOD.md` | 1 | — | The cleaning spec + ASR-correction glossary the clean layer followed |
| `youtube-transcripts/raw/README.md` | 1 | — | Raw-layer usage notes |
| `batches/*.md` | 3 | Reproduced articles | Near-verbatim reproductions of published web articles (the official Quack blog post, Definite blog, Medium article) |

## 5. Relationship between the two areas

- Canonical files `duckdb/14`–`17` were authored **from** the corpus (verified upstream where possible); `duckdb/00`, `10`, `11`, `12` and `lance/10` received additive corpus-sourced sections. All other canonical files were authored purely from live upstream sources.
- Direction of trust: where a canonical file and a transcript disagree, the canonical file's upstream-verified statement governs; the transcript remains the record of what was said.
- Every corpus-sourced claim inside a canonical file cites its transcript path; transcripts never cite canonical.

## 6. Distinct-repo doctrine note

`docs/reference/01_duckdb_processing.md`, `02_lancedb_storage.md`, `03_modal_compute.md`, `04_trigger_orchestration.md` are **core-x doctrine** (opinionated internal rules), not part of this canonical area. Per [`README.md`](README.md): where doctrine and this library disagree on an upstream API signature, parameter, default, or version, the canonical library governs and the doctrine requires re-verification.
