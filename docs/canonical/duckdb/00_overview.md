# DuckDB — Overview, Editions, Clients, Versioning & Release Lines

> Canonical upstream reference — fetched 2026-07-08 from official DuckDB documentation and source repositories. Documents upstream behavior as-is; this is NOT core-x doctrine. Treat the version numbers and API signatures here as fetched-date ground truth.
>
> Primary sources:
> - https://duckdb.org/ — project homepage: tagline, quickstart snippets, client list, current version banner.
> - https://duckdb.org/docs/current/ (canonical target of the `/docs/stable/` redirect) — documentation landing page: client-API index, data-import topics.
> - https://duckdb.org/docs/current/connect/overview.html — connection & storage model (`:memory:` vs persistent file, out-of-core spilling, cross-version file compatibility).
> - https://duckdb.org/docs/current/clients/python/overview.html — Python client: `duckdb.connect()` usage, in-memory vs persistent semantics.
> - https://pypi.org/project/duckdb/ — Python package: current version, release date, supported Python versions, install command.
> - https://duckdb.org/release_calendar — release calendar: current stable, LTS lines, codenames, versioning scheme, upcoming releases.
> - https://duckdb.org/docs/lts/dev/release_cycle — release cycle: cadence, LTS support policy, branch model.
> - https://endoflife.date/duckdb — per-release EOL/support table (independent tracker corroborating the release calendar).
>
> Talk-transcript sources (committed clean corpus layer — `docs/youtube-transcripts/clean/*.clean.md`; these are speaker-reported, not upstream docs, and are attributed as such wherever folded in below):
> - `docs/youtube-transcripts/clean/2026-07-11_duckcon-7-state-of-the-duck.clean.md` — DuckCon #7 keynote, "State of the Duck" (Amsterdam, 2026-07-11). Source of the DuckDB 2.0 "year of the server" roadmap, the 2.0 "cinnamon teal" codename, the DuckDB Labs → DuckLabs company rename, and the >1M installs/day + >160M extension installs/month adoption figures.
> - `docs/youtube-transcripts/clean/2026-02-02_duckdb-extensions-the-past-the-present-and-the-future.clean.md` — conference talk by Sam Ansmink, "DuckDB Extensions: The Past, the Present, and the Future" (2026-02-02). Source of the 32 core / 145 community extension counts and the ~27M core + ~500k community extension downloads/week figures, and the stable-C-extension-API roadmap (author-dated internally to 1.6).

Scope: What DuckDB is, its current released versions and release-line/LTS model as of 2026-07-08, the full set of client APIs and where each is documented, the in-memory-vs-persistent storage model with direct-file querying, and installation — the orientation layer for every other file in this library.

---

## 1. What DuckDB is

DuckDB is an **in-process (embedded) SQL OLAP database management system**. The homepage (fetched 2026-07-08) leads with *"Run analytics where your data lives"* and describes DuckDB as *"a SQL database that runs everywhere"* — *"Simple, feature-rich, fast & open source."*

Defining properties:

- **In-process / embedded — no server.** DuckDB runs inside the host process (your Python interpreter, the CLI binary, a JVM, a browser via WASM). There is no separate database server to install, configure, connect to over a socket, or keep running. Contrast with client/server systems (Postgres, MySQL). The closest analogue is SQLite, but DuckDB is built for analytics rather than transactional single-row access.
- **Columnar-vectorized execution engine.** Data is processed in column-oriented batches ("vectors") rather than row-at-a-time, which is what makes analytical scans, aggregations, and joins fast on wide tables.
- **Out-of-core (larger-than-memory) execution.** Both persistent and in-memory databases support *"spilling to disk to facilitate larger-than-memory workloads (i.e., out-of-core-processing)."* A query over a dataset that does not fit in RAM does not immediately fail — the engine spills intermediates to a temp directory. (Configured via `memory_limit` / `temp_directory`; see [`06_configuration_memory_spill.md`](06_configuration_memory_spill.md).)
- **OLAP, not OLTP.** Optimized for analytical queries (scan-heavy, aggregation-heavy, wide) over transactional workloads (high-frequency single-row insert/update/delete).
- **Open source**, MIT-licensed, maintained by DuckDB Labs (renamed **DuckLabs** in 2026 — see note below) and the DuckDB Foundation.

> **Company rename — DuckDB Labs → "DuckLabs" (talk-reported).** As announced in the DuckCon #7 keynote, the company that maintains DuckDB rebranded from *DuckDB Labs* to **DuckLabs**, to reflect that it now ships more than just DuckDB — *"we have DuckDB and we have DuckLake and we have Quack, [so] we actually decided to change our company name … the company is now called DuckLabs"* (DuckCon #7 keynote, State of the Duck, 2026-07-11 — `docs/youtube-transcripts/clean/2026-07-11_duckcon-7-state-of-the-duck.clean.md`). This reconciles the naming already used elsewhere in this library (e.g. [`10_core_extensions_catalog.md`](10_core_extensions_catalog.md), [`13_lance_interop.md`](13_lance_interop.md), which quote upstream "DuckLabs" wording). Attribute historically per source: pages/quotes predating the rename read "DuckDB Labs"; the current company name is "DuckLabs". As of the fetch date (2026-07-08) `duckdb.org` had not been re-scraped for the new name, so treat the rename as talk-reported; not independently verified against the site.

> Relevance to core-x: DuckDB is the compute layer of the plane. Raw Parquet/CSV lands ephemerally; DuckDB reads it, projects/DISTINCTs/casts, and streams Arrow out to Lance. Its out-of-core spill is the mechanism that lets a single worker process hundreds-of-millions-of-rows datasets without a cluster — see [`06_configuration_memory_spill.md`](06_configuration_memory_spill.md) for `memory_limit` and `temp_directory`, and [`13_lance_interop.md`](13_lance_interop.md) for the zero-copy Arrow handoff to Lance.

---

## 2. Current versions (as of 2026-07-08)

| Facet | Value | Source |
|---|---|---|
| **Current stable release** | **1.5.4** | homepage / PyPI / release calendar |
| Stable release date | 2026-06-17 | PyPI / release calendar |
| Stable codename | **Variegata** (1.5 line; Paradise shelduck, *Tadorna variegata*) | release calendar |
| **Current LTS line** | **1.4.x** — latest patch **1.4.5** (2026-06-17) | endoflife.date / release calendar |
| 1.4 LTS codename | **Andium** | release calendar |
| 1.4 LTS release date | 2025-09-16 | endoflife.date |
| 1.4 LTS community-support expiry | 2026-09-16 (one year) | endoflife.date |
| Python package (`duckdb` on PyPI) | **1.5.4** (2026-06-17) | PyPI |
| Supported Python versions | 3.10, 3.11, 3.12, 3.13, 3.14 (`requires-python >= 3.10.0`) | PyPI |
| Next patch (scheduled) | 1.5.5 — 2026-07-20 | release calendar |
| Next major (scheduled) | 2.0.0 — Fall 2026 | release calendar |

**Reading the "1.4.5 LTS" vs "1.5.4 stable" pairing:** The homepage advertises both a current stable (1.5.4) and the latest LTS patch (1.4.5) simultaneously. These are two different release *lines*, not conflicting version numbers:
- **1.5.x** is the newest minor line — the current stable, receiving the latest features.
- **1.4.x** is the LTS line — 1.4 was designated Long-Term Support, and 1.4.5 is its most recent patch. Production deployments that want a year of guaranteed patch support pin to the 1.4 LTS line; deployments that want the newest engine run 1.5.

The Python package on PyPI tracks the stable line, so `pip install duckdb` currently installs **1.5.4**. To stay on LTS, pin explicitly (`pip install "duckdb==1.4.5"`).

---

## 3. Release-line & versioning model

DuckDB follows **semantic versioning**: *"larger new features are introduced in minor versions, while patch versions mostly contain bugfixes."*

- **Cadence:** *"Minor versions are released approximately every 4 months."*
- **Codenames:** Major and minor versions receive a codename based on a duck species. This has been the scheme since **v0.4.0** (between v0.2.2 and v0.3.3 every release including patches got a codename; from 0.4.0 onward only major/minor do). Examples: 1.0.0 "Nivis", 1.1.0 "Eatoni", 1.2.0 "Histrionicus", 1.3.0 "Ossivalis", 1.4.0 "Andium", 1.5.0 "Variegata".
- **Branch model:** The `vx.y-codename` branch produces all `vx.y.z` patch releases for that minor line. Features for the next minor merge onto `main` mid-cycle, then a codename branch is cut, then feature-freeze (bug fixes only) before release.

### LTS policy

> *"Starting with 1.4, every other DuckDB version is going to be a Long-Term Support (LTS) edition."*
> *"For LTS DuckDB versions, the support period for community support is currently a year after the release."*
> *"Non-LTS releases become end-of-life once a newer release (LTS or not) is available."*

- **LTS = every other minor**, starting at 1.4. So 1.4 is LTS, 1.5 is not, and the next LTS is expected at 1.6 (with 2.0 as the subsequent major). *(Flagged below under Unverified — the exact next LTS minor is not stated on the fetched pages, only the "every other minor" rule.)*
- **Non-LTS EOL is aggressive:** a non-LTS minor goes end-of-life the moment *any* newer release ships. This is why 1.3, 1.2, 1.1, 1.0 all show "Ended" support on the EOL tracker.
- **Commercial support** by DuckDB Labs is available for older LTS releases after their community-support year expires.

### Release / EOL table

| Release | Codename | Released | Latest patch | Support status (2026-07-08) | Community EOL |
|---|---|---|---|---|---|
| **1.5** (stable) | Variegata | 2026-03-09 | 1.5.4 (2026-06-17) | Active | ongoing (non-LTS; EOL when 1.6 ships) |
| **1.4 (LTS)** | Andium | 2025-09-16 | 1.4.5 (2026-06-17) | Active (LTS) | 2026-09-16 |
| 1.3 | Ossivalis | 2025-05-21 | 1.3.2 (2025-07-08) | Ended | 2025-09-16 |
| 1.2 | Histrionicus | 2025-02-05 | 1.2.2 (2025-04-08) | Ended | 2025-05-21 |
| 1.1 | Eatoni | 2024-09-09 | 1.1.3 (2024-11-04) | Ended | 2025-02-05 |
| 1.0 | Nivis | 2024-06-03 | 1.0.0 | Ended | 2024-09-09 |

**Storage-format stability:** Since **v0.10**, DuckDB maintains backward compatibility for the on-disk database file — *"newer versions can read files created by earlier versions."* Upgrading the engine does not require rewriting existing `.duckdb` files. (This governs the native storage format only; Parquet/CSV/JSON/Lance are external formats handled by their own readers.)

> Relevance to core-x: For pipelines pinned in Modal images or requirements files, choose deliberately between the **1.4 LTS** line (year of patch support, September 2026 expiry) and the **1.5 stable** line (newest features). Because non-LTS minors go EOL as soon as the next minor ships, pinning to a bare `duckdb>=1.x` on a non-LTS line means silently running an unsupported build. Pin exactly, and prefer the LTS line for long-lived orchestrators.

### DuckDB 2.0 roadmap (talk-reported — not shipped)

> **Provenance.** Everything in this subsection is **talk-reported** from the DuckCon #7 keynote (State of the Duck, 2026-07-11 — `docs/youtube-transcripts/clean/2026-07-11_duckcon-7-state-of-the-duck.clean.md`), delivered by Mark. It describes intended 2.0 features and syntax that had **not shipped** as of the 2026-07-08 fetch date. Do not treat any of it as current API. Upstream-verified facts (against `duckdb.org/release_calendar`, fetched 2026-07-08): **2.0.0 is scheduled for Fall 2026**, and the release calendar lists **no official codename** for 2.0. The codename below is talk-reported only.

- **Codename "cinnamon teal", targeted fall 2026.** *"I want to talk to you about the next DuckDB release, which is DuckDB 2.0. It's going to be named after the cinnamon teal … the release is scheduled for the fall."* The Fall 2026 target matches the release calendar; the **codename is talk-reported and not independently verified** — the release calendar page (fetched 2026-07-08) publishes no codename for 2.0.
- **Positioning: "the year of DuckDB as a server".** The prior DuckCon framed 2025 as "the year of the lakehouse"; 2.0's theme is DuckDB run long-lived as a server — *"now I think we're going to go for the year of DuckDB as a server,"* bringing focus onto observability (better metrics/logs), stability, and multi-tenant use of DuckDB's existing ACID/MVCC/transaction-isolation machinery.

Planned 2.0 features (all talk-reported, **not shipped**; syntax shown is as presented in the talk and **not verified** against upstream docs — do not treat as canonical SQL):

| Feature | What the talk said | Status |
|---|---|---|
| **`CONNECT` statement** | New syntax to directly forward queries to another DuckDB over the Quack protocol, **replacing** the preview `remote.query($$…$$)` form the speaker called out as unsatisfactory: *"with 2.0 we're going to have new syntax, the connect statement, that you can basically directly forward queries to the other side."* | Roadmap; syntax not verified — talk-shown only |
| **Triggers** | SQL-level triggers firing on events (e.g. *"after I insert into this table, I'm going to insert into my audit table"*) — audit tables / logs are the headline use case; also used internally for other features. | Roadmap |
| **Async I/O** | Asynchronous IO so the input layer scales separately from query processing, for much faster remote (object-store) reads — *"we're planning it first for Parquet files, but it's also coming to other file formats, and also … the DuckDB file format."* Parquet first. | Roadmap |
| **Partition-aware execution** | Query planning/optimization made partitioning-aware for lakehouse formats (DuckLake, Iceberg) and partitioned Parquet on S3, to take full advantage of partition pruning. | Roadmap |
| **C++ V2 / stable C extension API** | Broaden the **stable C extension API** so extensions are written/built/published **once** and keep working across releases, replacing today's build-against-unstable-C++-API model — *"write them only once, build them only once, publish them only once, and then it will be available essentially until the end of time."* (See the extensions talk below, which dates the internal target for this to **1.6**, not 2.0.) | Roadmap |
| **New parser** | Replace the long-standing Postgres parser with a new, modern parser that eases extension-defined SQL syntax — *"we're ripping out the old parser and we're making a new parser."* Intended to be **Postgres-compatible**: *"it should be compatible with the old one … if [you notice a difference], please file an issue."* (The talk says only "new modern parser" — it does **not** name it. Upstream-verified separately: the new parser is a **PEG** (Parsing Expression Grammar) parser that shipped as *experimental* in 1.5 and is slated to become the default in 2.0; the old parser is a fork of the Postgres YACC parser. Source: `duckdb.org/docs/current/sql/peg_parser`, fetched 2026-07-08.) | Roadmap (parser swap); PEG parser itself experimental-in-1.5 (upstream-verified) |
| **JSON type backed by VARIANT** | Plan to back the `JSON` type with `VARIANT` in the engine so JSON gains VARIANT's storage/execution optimizations transparently — explicitly flagged as *"we probably won't make for 2.0, but don't hold my words to it."* | Roadmap (may slip past 2.0) |

> **VARIANT is already GA in 1.5 (upstream-verified line, talk-corroborated).** Distinct from the JSON-backing plan above, the `VARIANT` type itself already shipped in the **1.5** stable line — *"the variant type … is something that's actually already in DuckDB v1.5"* (DuckCon #7, 2026-07-11). VARIANT is documented in this library's SQL essentials file — see [`12_sql_essentials.md`](12_sql_essentials.md). The talk frames it as *"JSON on steroids … imagine if JSON were fast,"* with schema patterns extracted for storage compression and execution speed; 2.0 is slated to add further VARIANT improvements. Verify specific VARIANT behavior against `12_sql_essentials.md` / upstream before relying on it.

### Adoption ground-truth (talk-reported)

Round-number adoption/usage figures spoken in the talks. These are **speaker-stated** (spoken round numbers, not audited counters) and **not independently verified** against a published dashboard; attributed exactly:

- **>1,000,000 DuckDB installs/day** — *"we are beyond 1 million installs every day"* (DuckCon #7, State of the Duck, 2026-07-11 — `docs/youtube-transcripts/clean/2026-07-11_duckcon-7-state-of-the-duck.clean.md`).
- **>160,000,000 extension installs/month** — *"we're now at over 160 million extension installs every month"* (same source; traffic donated by Cloudflare per the talk).
- **32 core / 145 community extensions** — *"By now we have 32 core extensions and 145 community extensions"* (Extensions: past/present/future, 2026-02-02 — `docs/youtube-transcripts/clean/2026-02-02_duckdb-extensions-the-past-the-present-and-the-future.clean.md`).
- **~27,000,000 core + ~500,000 community extension downloads/week** — *"the core extensions are downloaded over 27 million times every week … the community extensions are downloaded over 500,000 times a week"* (same 2026-02-02 source). Note these are **downloads/week** from the extensions talk (2026-02-02), whereas the 160M/month figure above is **installs/month** from the later keynote (2026-07-11) — different metrics and dates; do not conflate.

These are additive context only and do **not** revise the version matrix in §2 or the release/EOL table in §3.

---

## 4. Clients / APIs

DuckDB ships official client APIs across the languages below. All are documented under `https://duckdb.org/docs/current/clients/<name>` (the `/docs/stable/` path redirects there).

| Client | What it is | Docs path (under `duckdb.org/docs/current/clients/`) |
|---|---|---|
| **CLI** | Standalone `duckdb` command-line shell (REPL + scriptable) | `cli/overview` |
| **Python** | `duckdb` PyPI package; relational API, Arrow/Pandas/Polars interop, replacement scans | `python/overview` |
| **R** | `duckdb` CRAN package; DBI-compatible, dplyr backend | `r` |
| **Java (JDBC)** | JDBC driver | `java` |
| **Node.js** | Node client "node-neo" — primary package `@duckdb/node-api` (high-level), plus lower-level `@duckdb/node-bindings` | `node_neo/overview` |
| **WebAssembly (WASM)** | DuckDB compiled to WASM, runs in-browser | `wasm/overview` |
| **C** | The stable C API — the foundation most other bindings wrap | `c/overview` |
| **C++** | Native C++ API | `cpp` |
| **Rust** | `duckdb` crate | `rust` |
| **Go** | Go bindings | `go` |
| **ODBC** | ODBC driver | `odbc/overview` |
| **ADBC** | Arrow Database Connectivity — Arrow-native columnar transport | `adbc` |

Additional community/first-party bindings exist (Swift, Julia, C#/.NET, Dart) but the list above is the set surfaced on the homepage and docs client index as the primary supported APIs. The **C API** is the stable ABI layer that the language bindings build on.

For pipeline work the two load-bearing clients are:
- **Python** — the orchestration surface. See [`01_python_client.md`](01_python_client.md).
- **ADBC / Arrow** — the columnar transport for zero-copy handoff. See [`02_arrow_integration.md`](02_arrow_integration.md).

---

## 5. Storage model

### In-memory (`:memory:`) vs persistent file

DuckDB has exactly two storage modes, selected at connection time:

- **In-memory:** Pass the special value `:memory:` as the database path, or omit the path argument entirely. *"All data is lost when the process finishes."* When you call the Python module-level `duckdb.sql(...)`, it *"operates on an in-memory database, i.e., no tables are persisted on disk"* — that database is *"stored globally inside the Python module."*
- **Persistent:** Pass a file path. *"DuckDB will open or create a database at that location as needed."* *"Any data written to that connection will be persisted, and can be reloaded by reconnecting to the same file, both from Python and from other DuckDB clients."* Common extensions: **`.duckdb`**, **`.db`**, **`.ddb`** (all are just conventions — the extension is not enforced).

The persistent database is a **single file** (the DuckDB native storage format). Out-of-core spill applies to *both* modes: even a `:memory:` connection can process larger-than-memory queries by spilling intermediates to the temp directory.

### Querying external files directly — no import step

A defining DuckDB ergonomic: you can query CSV / Parquet / JSON files **directly in a `FROM` clause** without first creating a table or running an `IMPORT`/`COPY` step. The file path (local or remote) is treated as a table:

```sql
-- Query a Parquet file directly, no table creation
SELECT * FROM 'data.parquet' WHERE amount > 100;

-- Query a CSV directly (types auto-detected)
SELECT count(*) FROM 'events.csv';

-- Glob multiple files as one logical table
SELECT * FROM 'logs/2026-*.parquet';

-- Remote object storage (with httpfs extension loaded)
SELECT * FROM 's3://bucket/prefix/*.parquet';
```

Behind the scenes these dispatch to `read_parquet()`, `read_csv()` / `read_csv_auto()`, and `read_json()` / `read_json_auto()`. See [`03_csv_import.md`](03_csv_import.md), [`04_parquet.md`](04_parquet.md), [`05_json.md`](05_json.md). Remote reads over S3/R2 require the `httpfs` extension — see [`07_httpfs_s3_r2.md`](07_httpfs_s3_r2.md).

> Relevance to core-x: This is why raw is transport-only. There is no "load into DuckDB" step — DuckDB reads the ephemeral Parquet/CSV in place, and the output streams to Lance. A `:memory:` connection with a configured `temp_directory` is the normal shape for a stateless worker: nothing is persisted in a `.duckdb` file; the system of record is Lance on R2.

---

## 6. Install & minimal example

### Python

```bash
pip install duckdb                # installs current stable (1.5.4 as of 2026-07-08)
pip install 'duckdb[all]'         # with all optional dependencies
pip install "duckdb==1.4.5"       # pin to the 1.4 LTS line
```

Minimal usage (module-level in-memory, then an explicit connection):

```python
import duckdb

# Module-level in-memory database (global, ephemeral)
duckdb.sql("SELECT 42").show()

# Explicit in-memory connection
con = duckdb.connect()

# Persistent single-file database
con = duckdb.connect("file.db")

# With configuration
con = duckdb.connect(config={'threads': 1})
```

> **`duckdb.connect()` full signature:** The prose docs page does **not** print a typed signature — it shows only the usage forms above (`duckdb.connect()`, `duckdb.connect("file.db")`, `duckdb.connect(config={...})`). The complete parameter list (`database`, `read_only`, `config`) with types and defaults is documented and quoted verbatim in [`01_python_client.md`](01_python_client.md); do not reconstruct it from the usage forms here.

### CLI

Install the standalone command-line shell:

```bash
curl https://install.duckdb.org | sh
```

Then run it as a REPL or against a database file:

```bash
duckdb                 # in-memory REPL
duckdb my.duckdb       # open/create a persistent database
```

### SQL / Python parity

The same query runs identically across clients. Homepage example:

```sql
SELECT station_name, count(*) AS num_services
FROM train_services
GROUP BY ALL
ORDER BY num_services DESC
LIMIT 3;
```

```python
import duckdb
duckdb.sql("""SELECT station, count(*) AS num_services
FROM train_services
GROUP BY ALL
ORDER BY num_services DESC LIMIT 3;""")
```

---

## 7. Map of this library (sibling files)

| File | Covers |
|---|---|
| **00_overview.md** (this file) | Overview, editions, clients, versioning & release lines |
| [`01_python_client.md`](01_python_client.md) | Python client — `connect`, `execute`, relational API, replacement scans (full `connect()` signature lives here) |
| [`02_arrow_integration.md`](02_arrow_integration.md) | Apache Arrow — `to_arrow_table`/`to_arrow_reader`, `from_arrow`, `register`, ADBC |
| [`03_csv_import.md`](03_csv_import.md) | CSV import — `read_csv`, `COPY`, options (`all_varchar`, `encoding`, `sample_size`, `ignore_errors`, rejects) |
| [`04_parquet.md`](04_parquet.md) | Parquet — `read_parquet`, `COPY TO`, metadata, partitioning, predicate/projection pushdown |
| [`05_json.md`](05_json.md) | JSON — `read_json`/`read_json_auto`, formats, JSON functions, nested casting |
| [`06_configuration_memory_spill.md`](06_configuration_memory_spill.md) | Configuration — `memory_limit`, `threads`, `temp_directory`, out-of-core spilling |
| [`07_httpfs_s3_r2.md`](07_httpfs_s3_r2.md) | httpfs, S3 API & Cloudflare R2 — reading/writing object storage |
| [`08_secrets_manager.md`](08_secrets_manager.md) | Secrets Manager — `CREATE SECRET`, types (s3/r2/gcs/azure/http), persistence |
| [`09_extensions_system.md`](09_extensions_system.md) | Extension system — `INSTALL`/`LOAD`, autoloading, core vs community, signing |
| [`10_core_extensions_catalog.md`](10_core_extensions_catalog.md) | Core extensions catalog — the full official list with purpose |
| [`11_quack_extension.md`](11_quack_extension.md) | The `quack` extension & the DuckDB extension template |
| [`12_sql_essentials.md`](12_sql_essentials.md) | SQL essentials — `TRY_CAST`, types (STRUCT/LIST/MAP/VARIANT), `QUALIFY`, window functions |
| [`13_lance_interop.md`](13_lance_interop.md) | DuckDB ↔ Lance interop — verified reading/writing Lance from DuckDB |

---

## 8. Footguns & deprecations

- **`pip install duckdb` follows the stable line, not LTS.** As of 2026-07-08 that is 1.5.4 (non-LTS). If you need the year-of-support LTS guarantee, pin `duckdb==1.4.5` (or the latest 1.4.x). Do not assume `pip install duckdb` gives you a supported-for-a-year build.
- **Non-LTS minors go EOL immediately** when the next minor ships. Running an unpinned non-LTS line means you can silently end up on an unsupported build after an upgrade.
- **`:memory:` still spills to disk.** An in-memory connection is not RAM-only for query execution — it will write intermediates to `temp_directory` for larger-than-memory queries. Ensure that directory has space and is configured on out-of-core workers.
- **The `/docs/stable/` URLs are redirects.** Canonical documentation content lives at `/docs/current/...` (and versioned archives at `/docs/archive/<version>/`). When bookmarking or scripting doc fetches, expect the redirect and target `/docs/current/`.
- **Module-level `duckdb.sql()` uses a hidden global in-memory database.** Tables created via the module-level API are not persisted and are shared process-globally. For anything stateful or persistent, use an explicit `duckdb.connect(path)`.

---

## 9. Unverified / needs confirmation

- **Full `duckdb.connect()` typed signature** — the Python overview page fetched here shows only usage forms, not a typed signature with parameter defaults. The authoritative signature is captured in [`01_python_client.md`](01_python_client.md); treat that file as the source, not this one.
- **Exact next LTS minor version** — the policy is "every other minor starting at 1.4," which implies 1.6 is the next LTS, but the fetched pages do not name it explicitly. The release calendar names 1.5.5 (2026-07-20) and 2.0.0 (Fall 2026) as scheduled, without stating which is LTS.
- **`1.3.2` patch date** — endoflife.date lists 1.3.2 as 2025-07-08; cross-check against the release calendar if an exact date is load-bearing (the two trackers were consistent on all other rows).
- **Complete official client list** — beyond the primary set listed in §4, additional first-party/community bindings (Swift, .NET, Julia, Dart) exist but were not enumerated on the fetched homepage/docs-index pages; confirm against `duckdb.org/docs/current/clients/` if a specific niche client matters.
- **DuckDB 2.0 "cinnamon teal" codename** — talk-reported from DuckCon #7 (2026-07-11); the release calendar (fetched 2026-07-08) confirms the **Fall 2026** target for 2.0.0 but publishes **no codename**. Treat the codename as not independently verified until the calendar/announcement lists it.
- **DuckDB 2.0 roadmap features + syntax** (`CONNECT` statement, triggers, async I/O, partition-aware execution, stable C extension API / C++ V2, new parser, JSON-backed-by-VARIANT) — all talk-reported and **not shipped** as of the fetch date; SQL syntax shown in §3 is talk-shown, not verified against upstream docs. Re-verify against `duckdb.org` docs once 2.0 ships before treating any as canonical. (Exception: the parser's identity as a **PEG parser** is upstream-verified — it shipped experimental in 1.5 per `duckdb.org/docs/current/sql/peg_parser`; only the 2.0 *default-swap* is roadmap. The talk itself did not use the term "PEG.")
- **Company rename DuckDB Labs → "DuckLabs"** — talk-reported from DuckCon #7 (2026-07-11); not re-verified against `duckdb.org` at fetch time (2026-07-08). Sibling files in this library already use "DuckLabs" in quoted upstream wording (see §1 note).
- **Adoption figures** (>1M installs/day, >160M extension installs/month; 32 core / 145 community extensions; ~27M core + ~500k community extension downloads/week) — spoken round numbers from the two talks (§3 "Adoption ground-truth"); not independently verified against a published counter.
