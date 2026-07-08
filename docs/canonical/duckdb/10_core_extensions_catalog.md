# Core Extensions Catalog — the full official list with purpose

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/docs/current/core_extensions/overview.html — the current authoritative roster of core extensions (name, description, maintainer, support tier, aliases). `/docs/stable/core_extensions/overview` and `/docs/lts/core_extensions/overview` redirect here.
> - https://duckdb.org/docs/current/extensions/overview.html — the extension *system*: built-in vs installable, INSTALL/LOAD, the autoloading mechanism, core vs community.
> - https://www.aidoczh.com/duckdb/docs/archive/1.0/extensions/core_extensions.html — archived 1.0 core-extensions table, the last version that rendered the explicit **Autoloadable** + **Aliases** columns verbatim; used here to pin per-extension autoload flags and alias names.
> - https://duckdb.org/community_extensions/ — the separate community-extensions catalog (`INSTALL … FROM community`).
> - https://duckdb.org/docs/current/core_extensions/vss — vss / HNSW index syntax.
> - https://github.com/duckdb/duckdb/releases + https://duckdb.org/release_calendar — release lines / current versions.

Scope: The complete list of official DuckDB **core** extensions as of 2026-07-08, each with a one-to-two-line purpose, its built-in-vs-installable and autoload status, plus the exact INSTALL/LOAD surface and introspection queries needed to use them from a pipeline.

---

## 0. Version ground truth (2026-07-08)

| Line | Latest release | Notes |
|------|----------------|-------|
| **1.5.x** (feature line, codename *Ossivalis* / *Variegata*) | **1.5.4** | 1.5.0 released 2026-03-09. Feature line, community support only. |
| **1.4.x LTS** (codename *Andium*) | **1.4.5 LTS** (2026-06-17) | LTS line; ~1 year community support. From v1.4.0 onward, every other DuckDB version is an LTS edition. |

Extensions are **versioned and distributed independently of the DuckDB engine** — they are downloaded from the extension repository keyed by the running engine version + platform, not shipped inside every binary. Two DuckDB builds on the same version can therefore differ in which extensions are already resident, because only a small built-in set is statically linked (see §2).

> Docs URL structure: `/docs/stable/…` currently redirects to `/docs/current/…` (the released version), and `/docs/lts/…` points at the LTS line. All three resolve to the same core-extensions content today. When a fetch of `/docs/stable/…` returns only a "Redirecting…" stub, refetch the `/docs/current/…` target.

---

## 1. The full core-extensions roster (fetched 2026-07-08)

Core extensions are built, signed, and distributed from DuckDB's **core** repository and are maintained by the DuckDB team (a handful are third-party-maintained but still listed on the core page). This is the complete set enumerated on the current core-extensions overview page, alphabetical:

The five columns below (Name / Description / Maintainer / Support tier / Aliases) reproduce the upstream overview table verbatim. The two extra columns — **Autoloadable** and **Built-in?** — are *not* on the upstream page; they are annotations sourced separately (see footnotes 1 and 2) and must be verified per build.

**Roster note:** The set differs by line. The **1.5 feature line** (28 rows) includes `odbc` and `quack`; the **1.4 LTS** roster (26 rows) omits both. `arrow` is **not** on either roster — see "Also frequently seen" below. Rows present only on 1.5 are marked (1.5-only).

| Extension | Purpose (1–2 lines) | Maintainer | Support tier | Autoloadable¹ | Built-in?² | Aliases |
|-----------|---------------------|------------|:---:|:---:|:---:|---------|
| **autocomplete** | Shell tab-completion of SQL keywords/identifiers in the CLI. | DuckDB | Secondary | yes | yes (CLI) | — |
| **avro** | Read Apache Avro (`.avro`) files. | DuckDB | Secondary | — | no | — |
| **aws** | Features that depend on the AWS SDK (e.g. credential-chain lookup used with httpfs S3). | DuckDB | Secondary | yes | no | — |
| **azure** | Filesystem abstraction for Azure Blob Storage (`az://` / `abfss://`). | DuckDB | Secondary | yes | no | — |
| **delta** | Read Delta Lake tables. | DuckDB | Secondary | yes | no | — |
| **ducklake** | DuckLake lakehouse format support (catalog + Parquet data files). | DuckDB | Secondary | — | no | — |
| **encodings** | Adds the text encodings available in the ICU data repository (for reading non-UTF-8 files). | DuckDB | Secondary | — | no | — |
| **excel** | Read/write Excel (`.xlsx`) files; also Excel-style number/format strings. | DuckDB | Secondary | yes | no | — |
| **fts** | Full-text search indexes (BM25 over text columns). | DuckDB | Secondary | yes | no | — |
| **httpfs** | Read/write files over HTTP(S) and S3-compatible object storage (S3, R2, GCS-XML, MinIO). | DuckDB | Primary | yes | no | `http`, `https`, `s3` |
| **iceberg** | Read Apache Iceberg tables. | DuckDB | Secondary | no | no | — |
| **icu** | Time zones, collations, and calendar/date functions via the ICU library. | DuckDB | Primary | yes | no | — |
| **inet** | `INET` data type + IP-address functions (host/netmask/network ops). | DuckDB | Secondary | yes | no | — |
| **jemalloc** | Overwrites the system allocator with jemalloc for better fragmentation behavior. | DuckDB | Secondary | no | platform (§2) | — |
| **json** | JSON parsing, `read_json*`, JSON path/extract functions, nested casting. | DuckDB | Primary | yes | yes | — |
| **lance** | Read/write Lance tables (third-party, listed on the core page). | third-party (LanceDB) | — | — | no | — |
| **motherduck** | Connect a DuckDB session to the MotherDuck cloud service. | third-party (MotherDuck) | — | — | no | `md` |
| **mysql** | Read from / write to a live MySQL database (attach as a catalog). | DuckDB | Secondary | no | no | `mysql_scanner` |
| **odbc** (1.5-only) | Access remote databases over ODBC drivers. | DuckDB | Secondary | — | no | `odbc_scanner` |
| **parquet** | Read/write Apache Parquet; predicate/projection pushdown, metadata, Hive partitioning. | DuckDB | Primary | n/a (built-in) | **yes** | — |
| **postgres** | Read from / write to a live PostgreSQL database (attach as a catalog). | DuckDB | Secondary | yes | no | `postgres_scanner` |
| **quack** (1.5-only) | DuckDB-Quack protocol for remote access. | DuckDB | Secondary | — | no | — |
| **spatial** | Geospatial types + functions (GEOS/GDAL/PROJ), `ST_*`, spatial file formats. | DuckDB | Secondary | no | no | — |
| **sqlite** | Read from / write to SQLite database files (attach as a catalog). | DuckDB | Secondary | yes | no | `sqlite_scanner`, `sqlite3` |
| **tpcds** | Generate the TPC-DS benchmark dataset and run its queries. | DuckDB | Secondary | yes | no | — |
| **tpch** | Generate the TPC-H benchmark dataset and run its queries. | DuckDB | Secondary | yes | no | — |
| **ui** | Local web UI / notebook interface for DuckDB (third-party, MotherDuck). | third-party | — | — | no | — |
| **unity_catalog** | Connect to Databricks Unity Catalog. | DuckDB | Secondary | — | no | `uc_catalog` |
| **vortex** | Read/write the Vortex columnar format (third-party). | third-party | — | — | no | — |
| **vss** | Vector similarity search — HNSW index on `ARRAY`/`FLOAT[N]` columns. | DuckDB | Secondary | no | no | — |

**Support tier** (upstream column, DuckDB-maintained extensions only): *Primary* = covered by DuckDBLabs' community support policy (`httpfs`, `icu`, `json`, `parquet`); *Secondary* = best-effort support, still shipped/updated with each release. Third-party extensions have no tier.

¹ **Autoloadable** = DuckDB will silently `INSTALL` + `LOAD` it the first time a query references its functionality (see §3). This column is **not** on the upstream overview page; `yes`/`no` values are carried from the archived 1.0 core-extensions table (the last version exposing an explicit Autoloadable column). `—` means that older table did not list the (mostly newer) extension — treat as "manually install" and verify with `duckdb_extensions()` on your target build. See **Unverified / needs confirmation** (§8).

² **Built-in?** = statically linked into the standard binary distribution (resident with no download). Also not an upstream column. See §2 for the authoritative, platform-qualified answer.

### Also frequently seen but NOT on the core list

- **`arrow`** — Zero-copy Arrow↔DuckDB integration. **NOT on the current (1.5) or LTS (1.4) core-extensions roster** as of the 2026-07-08 fetch, despite being widely referenced. There is an `arrow` entry that surfaces in `duckdb_extensions()` output (shown `installed = false`), but it is not listed as a maintained core extension on the overview page. For core-x, the load-bearing Arrow path is the **in-process** Python interop (`to_arrow`/`from_arrow` on a DuckDB relation), which requires **no DuckDB extension** — see §6 and `02_arrow_integration.md`. Do not write `INSTALL arrow;` into a bootstrap assuming it is a core roster entry; verify against `duckdb_extensions()` on your build first.
- **`substrait`** — Substrait query-plan integration. Present on older (≤1.0) core lists; on current builds it is not on the primary core roster. Verify via `duckdb_extensions()`; if absent from core it lives in community.
- **`core_functions`** — an *internal* built-in module that carries the bulk of DuckDB's scalar/aggregate functions. It appears in `duckdb_extensions()` output and is always resident; it is not something you install.

---

## 2. Built-in / statically-linked extensions

The DuckDB distribution is kept lightweight, so **only a few essential extensions are statically linked**; everything else is downloaded on first use. Upstream states: *"only a few essential extensions are built-in, varying slightly per distribution."* There is no single universal list — it is platform-qualified — so the ground truth for **your** binary is always the introspection query below, not this doc.

Reliably built-in across the standard builds:

- **`parquet`** — always built in (documented "(built-in)"); no INSTALL/LOAD needed to read/write Parquet.
- **`json`** — built in on the standard distributions.
- **`autocomplete`** — built into the CLI client.
- **`core_functions`** — always resident (internal).
- **`icu`** — built into most standard distributions (the Python wheel and CLI ship it); confirm per build.

Platform-qualified:

- **`jemalloc`** — statically linked, and *"cannot be installed or loaded during runtime."* Per the current jemalloc core-extension page: *"Linux distributions of DuckDB ship with the `jemalloc` extension"* (all Linux, not just AMD64 — the AMD64-only wording is older phrasing); the macOS build does **not** ship it but can be built from source to include it; on Windows it is **not available**.

Authoritative per-binary check — run this on the exact build you deploy:

```sql
-- Which extensions are resident/installed/loadable on THIS binary:
SELECT extension_name, loaded, installed, install_mode, description
FROM duckdb_extensions()
ORDER BY loaded DESC, installed DESC, extension_name;
```

`install_mode` distinguishes `STATICALLY_LINKED` (built-in) from `REPOSITORY` (downloaded). This is the only non-lying answer to "is X built into my binary?".

> Relevance to core-x: on the Modal/worker images that run the out-of-core → Arrow → Lance pipeline, do not assume `httpfs`, `aws`, or `azure` are resident — none are statically linked. Bake `INSTALL httpfs;` (and `aws` if using the credential chain) into image build or session bootstrap so cold workers do not pay a first-query download, and so an offline/locked-down worker fails at build time rather than mid-pipeline. `parquet` and `json` are safe to assume; everything touching R2 is not.

---

## 3. The autoloading mechanism

Upstream: *"DuckDB contains an autoloading mechanism which can install and load the core extensions as soon as they are used in a query."* Example: `SELECT * FROM 'https://…/file.csv'` triggers `httpfs` to be auto-installed and auto-loaded with no explicit statement.

Two independent flags gate this (both default **true**):

```sql
SET autoinstall_known_extensions = true;  -- may download a known core extension on demand
SET autoload_known_extensions   = true;   -- may load an already-installed extension on demand
```

Not everything autoloads. Upstream: *"Not all extensions can be autoloaded"* — some make sweeping changes to the running instance (so autoload is technically not yet possible), and for others an explicit user opt-in is preferred. The `no`-flagged rows in §1 (`iceberg`, `mysql`, `spatial`, `vss`, `jemalloc`, …) must be installed/loaded by hand:

```sql
INSTALL spatial;
LOAD spatial;
```

Footguns:
- **Air-gapped / firewalled workers**: with default autoinstall on, a query silently reaches out to `extensions.duckdb.org`. In a locked-down environment set `SET autoinstall_known_extensions = false;` and pre-install deterministically, or the first S3 query throws a network error deep in execution.
- **Autoload ≠ pinned version**: an autoloaded extension is fetched for the running engine version. Reproducible pipelines should pin by pre-installing at image build, not rely on lazy fetch.

---

## 4. INSTALL / LOAD — the exact surface

Two distinct operations (per the extension-system overview):
- **Installation** = *"downloading the extension binary and verifying its metadata."* Persists on disk (the local extension directory); survives across sessions.
- **Loading** = *"dynamically loading the binary into a DuckDB instance."* Per-connection/session.

```sql
-- Core extension (default repository):
INSTALL httpfs;
LOAD httpfs;

-- Community extension (separate signed repo — see §5):
INSTALL h3 FROM community;
LOAD h3;

-- Force a re-download of an already-installed extension:
FORCE INSTALL httpfs;

-- Install from a specific repository / version / URL:
INSTALL httpfs FROM 'https://extensions.duckdb.org';
```

Python client (same semantics; run the SQL on the connection):

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
```

Introspection:

```sql
SELECT extension_name, loaded, installed, description
FROM duckdb_extensions();                 -- table function: full catalog + status
PRAGMA version;                           -- engine version / source id
```

> Relevance to core-x: `LOAD` is per-session, so every fresh DuckDB connection in a worker must re-`LOAD` (not re-`INSTALL`) `httpfs` before touching R2. Bootstrap the connection with `INSTALL httpfs; LOAD httpfs;` once at open — `INSTALL` no-ops if already on disk, `LOAD` is cheap. See `07_httpfs_s3_r2.md` and `08_secrets_manager.md`.

---

## 5. Community extensions are a separate catalog

Community extensions are **not** in this catalog. They are contributed by external authors and, per upstream, *"created by external contributors and not maintained by DuckLabs,"* but are still **built, signed, and distributed centrally** by DuckDB's CI (cryptographically signed to prove provenance), analogous to Homebrew: *"code will reside in your own repository, but build and distribution is centralized."*

- Catalog + list: https://duckdb.org/community_extensions/ and its "List of Community Extensions" page.
- Endpoint: `http(s)://community-extensions.duckdb.org`.
- Install syntax differs — the `FROM community` clause is mandatory:

```sql
INSTALL h3 FROM community;
LOAD h3;
```

Notable community extensions include `h3` (Uber H3 geospatial indexing), `prql`, `shellfs`, `crypto`, `lindel`, and many others. Do not assume any community extension has the stability, platform coverage, or support guarantees of a core extension.

See `09_extensions_system.md` for signing, repositories, and the full install/trust model, and `11_quack_extension.md` for how an extension is actually built (the `quack` template).

---

## 6. Deep dives on the core-x load-bearing extensions

Short pointers to the sibling files that carry the real API surface. These are the ones the R2 → Arrow → Lance data plane depends on. Note `arrow` here means the in-process Python interop, **not** a core extension (see §1).

- **httpfs** — the *only* path from DuckDB to Cloudflare R2. Provides the `s3://` filesystem; R2 is addressed via S3-compatible endpoint + region `auto`. Not built-in — must be installed/loaded. Full endpoint/credential/URL-style config: **`07_httpfs_s3_r2.md`**; credential provisioning via `CREATE SECRET`: **`08_secrets_manager.md`**.
- **json** — `read_json` / `read_json_auto`, format detection (`array`/`newline_delimited`/`unstructured`), JSON extract functions, nested→typed casting. Built-in on standard builds. Full surface: **`05_json.md`**.
- **encodings** — supplies non-UTF-8 text encodings (from the ICU data repo) so `read_csv(..., encoding=...)` can decode legacy source files before they land in Lance. Not built-in; install when a source drop is not UTF-8. CSV encoding options: **`03_csv_import.md`**.
- **arrow** — zero-copy Arrow↔DuckDB bridge; underpins `to_arrow_table` / `to_arrow_reader` / `from_arrow` used to stream record batches into `lance.write_dataset` without materializing intermediates. **Not a listed core extension** (see §1 "Also frequently seen"): the interop core-x relies on is the **in-process** Python path (methods on a DuckDB relation), which requires no `INSTALL`/`LOAD` at all. Extension-vs-in-process paths: **`02_arrow_integration.md`**; Lance side: **`13_lance_interop.md`**.
- **parquet** — always built-in; `read_parquet`, `COPY … TO … (FORMAT parquet)`, projection/predicate pushdown, Hive partitioning, metadata. This is the transport format for raw drops before compute. Full surface: **`04_parquet.md`**.
- **vss** — HNSW vector index inside DuckDB (see §6.1). Relevant only if similarity search runs *inside* DuckDB; Lance carries its own vector index in the SoR. **`13_lance_interop.md`** for where the vector index actually lives.

### 6.1 vss / HNSW — exact syntax

```sql
INSTALL vss;
LOAD vss;

-- Basic HNSW index on a fixed-size vector column (ARRAY / FLOAT[N]):
CREATE INDEX my_hnsw_index ON my_vector_table USING HNSW (vec);

-- Choose the distance metric:
CREATE INDEX my_hnsw_cosine_index ON my_vector_table
  USING HNSW (vec) WITH (metric = 'cosine');
```

`WITH (metric = …)` accepted values (verbatim from the vss page):

| metric | Meaning | Default |
|--------|---------|:---:|
| `l2sq` | Euclidean (squared L2) distance | ✅ default |
| `cosine` | Cosine similarity distance | |
| `ip` | Negative inner product | |

Persistence footgun — HNSW indexes are **in-memory only** unless you opt in:

```sql
SET hnsw_enable_experimental_persistence = true;
```

Upstream flags this **experimental** because *"WAL recovery is not yet properly implemented for custom indexes"* — risk of data loss on unexpected shutdown. Do not enable in production. The index column must be a fixed-size vector type (`FLOAT[N]` / `ARRAY`), not a variable-length `LIST`.

> Relevance to core-x: the system-of-record vector index lives in **Lance on R2** (fragment-level ANN), not in a DuckDB HNSW index. Use vss only for ad-hoc in-DuckDB experiments; do not stand up a production persistence path on an experimental, WAL-unsafe custom index. Resolution-key lookups against Lance use BTREE scalar indices, a separate concern from HNSW.

---

## 7. Deprecations, renames, and footguns

- **Alias renames** — the historical `postgres_scanner` / `sqlite_scanner` / `mysql_scanner` names are aliases; the canonical extension names are `postgres` / `sqlite` / `mysql`. Old install scripts using `INSTALL postgres_scanner;` still resolve via alias but should be updated.
- **`spatial` never autoloads** — a query using `ST_*` without a prior `LOAD spatial;` fails; there is no lazy-load rescue. Same for `iceberg`, `vss`, `mysql`.
- **`LOAD` is per-connection** — installing once does not load into new sessions. Re-`LOAD` on every fresh connection (see §4).
- **Silent network fetch** — default `autoinstall_known_extensions = true` means a first S3/HTTP query on a fresh worker reaches the network. Pre-install for determinism; disable autoinstall in air-gapped runs (§3).
- **Built-in set is not universal** — never assume an extension is resident because it was on one machine; `jemalloc` alone varies by OS/arch. Query `duckdb_extensions()` on the actual target build (§2).
- **`hnsw_enable_experimental_persistence`** — experimental, WAL-unsafe (§6.1). Not for production.
- **Extension version ⟂ engine version** — extensions are fetched per engine version + platform. Upgrading the DuckDB engine can invalidate cached extension binaries and force a re-download on first use.

---

## 8. Unverified / needs confirmation

- **Per-extension `Autoloadable` flags for newer entries** — `avro`, `ducklake`, `encodings`, `odbc`, `quack`, `unity_catalog`, `lance`, `ui`, `vortex`, `motherduck` are not on the archived 1.0 table that exposed the explicit column, and the current overview page renders columns *Name / Description / Maintainer / Support tier / Aliases* — **not** an Autoloadable/Autoinstallable column. Their autoload flags in §1 are marked `—`. Confirm the true value per build with `SELECT extension_name, install_mode FROM duckdb_extensions();` and by observing whether a bare reference triggers an auto-install. Do not assume autoload for any `—` row in a locked-down pipeline.
- **Exact built-in set for a given wheel/CLI build** — §2 lists the reliably-built-in extensions, but the precise statically-linked set "varies slightly per distribution" per upstream. The Python wheel, the CLI, and the various platform binaries can differ. The only authoritative answer is `install_mode = 'STATICALLY_LINKED'` from `duckdb_extensions()` on the exact artifact you ship.
- **`substrait` core-vs-community placement on 1.4/1.5** — it was on ≤1.0 core lists but is not on the current primary core roster fetched here. Verify against your target build before depending on `INSTALL substrait;` (core) vs `INSTALL substrait FROM community;`.
- **Persistent-HNSW GA status** — the vss page still describes disk persistence as experimental as of the 2026-07-08 fetch; upstream did not state a version in which it became non-experimental. Treat as experimental until confirmed on your engine version.
