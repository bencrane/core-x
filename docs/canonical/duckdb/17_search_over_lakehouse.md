# Search-First Retrieval over a Lakehouse (Alter Table / Duck Search — VENDOR-REPORTED)

> Canonical upstream reference. Folded from the committed talk-transcript corpus (docs/youtube-transcripts/, docs/batches/) and verified against live upstream docs where they exist (July 2026). Talk-reported claims are attributed inline; upstream-verified facts cite the doc URL.
>
> Primary sources:
> - docs/youtube-transcripts/clean/2026-07-11-grep-your-lakehouse-duckcon7.clean.md — "Grep your lakehouse: Search-first retrieval for DuckDB-powered agents", DuckCon #7, Sylvain Utard (co-founder & CEO, Alter Table). Talk delivered 2026-06-24 (Amsterdam); recording published 2026-07-11.
> - https://duckdb.org/events/2026/06/24/duckcon7/ — verifies the talk title, speaker, and event/date.
> - https://duckdb.org/docs/current/core_extensions/full_text_search — verifies what DuckDB CORE full-text search actually provides (contrast baseline; the talk's syntax is NOT this).
> - https://duckdb.org/docs/current/core_extensions/overview — verifies DuckDB's extension system, on which the talk's product is layered.

Scope: One third-party extension's pitch — as presented at a conference talk — for full-text + semantic search that rides a DuckLake lakehouse's storage layer, so agents can "grep" data the way coding agents grep a codebase.

---

## ⚠️ DISCLAIMER — READ BEFORE ANYTHING ELSE

**This file documents a THIRD-PARTY extension, not a shipped core capability.**

- The subject is **Alter Table** (a.k.a. "Duck Search"), a commercial product presented by **Sylvain Utard** at **DuckCon #7** (talk 2026-06-24, Amsterdam; recording published 2026-07-11). Everything here is described **as stated in that talk**.
- It is **NOT a DuckDB core capability.** DuckDB core ships a different, more limited full-text search extension (see the contrast table below) and has **no** built-in semantic/vector search operator in that extension.
- It is **NOT a Lance capability.** Lance has its own independent FTS/inverted index and vector search (see `../lance/05_scalar_indices.md`, `../lance/06_vector_search.md`); those are unrelated to this extension.
- An official Alter Table DuckDB extension repo exists (`altertable-ai/duckdb-altertable`, https://github.com/altertable-ai/duckdb-altertable), but it is a **connectivity** extension (query Alter Table's remote lakehouse over Arrow Flight) and **does not document the search SQL surface** shown in the talk (no `@@` operator, no `CREATE INDEX ... USING`, no semantic operator, no star-column search). **No official or neutral upstream documentation for the talk's search syntax could be located** (July 2026 web search). Every search SQL example, operator, and architectural detail below is therefore **vendor-reported / talk-shown and not independently verified.** Do not treat any of it as canonical syntax.
- **core-x does not run this.** core-x has no FTS lane today (per the retrieval survey). This file exists for **awareness, not endorsement or installation guidance.**

---

## 1. The thesis: "search-first retrieval" for agents

**Talk-reported claim** (DuckCon #7, "Grep your lakehouse", Utard, 2026-06-24 — docs/youtube-transcripts/clean/2026-07-11-grep-your-lakehouse-duckcon7.clean.md):

- Coding agents (Claude Code, Cursor) grep first: *"their first instinct, the first thing they do just after your ask, your prompt, is search. They want to grab your code base... They don't care about the structure, they don't care about the folders, they just want to grab everything."*
- Data-analyst agents are strong at **text-to-SQL** — *"they know better than anyone... how to write a good SQL query"* — but SQL is structured (tables, columns), so the agent must still know *"what table, what column to use for what use case."*
- A **semantic layer** can document that away, but per the speaker *"this is not happening often times."*
- The gap the pitch targets: *"What's really missing in SQL is a schema-agnostic retrieval mode."*

Speaker background as stated in the talk: 13 years building search engines; C++ engineer at a French Google competitor; first employee and VP of Engineering at Algolia; 4 years in B2C; since ~2025 co-founder & CEO of Alter Table.

## 2. The pitch: share the lakehouse storage layer ("we hate pipelines")

**Talk-reported claim** (same source):

- The motivating pain: keeping a database and a separate search engine in sync. *"You want to synchronize a database with a search engine... That pipeline, a nightmare. I hated it."*
- Alter Table's stated answer: a new retrieval mode over the lakehouse that *"could rely on the same storage layer than DuckLake, that DuckLake already uses, for search and semantic search."*
- Two retrieval families offered, both riding the same storage:
  - **Full-text search** — an inverted index mapping words → document IDs; AND of two words is a list intersection. Built on **Tantivy** (talk shout-out: *"shout out to Tantivy, that has been delivering amazingly so far"*).
  - **Semantic search** — word embeddings → vectors; a graph structure (*"one simple graph structure"*) queried by embedding the query and finding semantically similar objects.
- Positioning phrases from the talk: a *"data runtime for this AI era on top of the lake"*, a *"lakehouse with federation"*, a knowledge graph, and AI agents. Recurring refrain: *"we hate pipelines"* / *"I hate pipelines, and you should too."*

## 3. The SQL surface — AS SHOWN IN THE TALK (talk-shown, NOT verified upstream)

> ⚠️ Every construct in this section is **talk-shown / vendor-reported**. No upstream doc confirms this syntax. The talk explicitly noted it was **simplifying** ("I've been simplifying a lot how search is actually working within DuckDB"). Do **not** copy these as canonical DuckDB or Lance syntax. Reproduced here only to characterize the surface the speaker described.

The talk described extending DuckDB's SQL dialect (**talk-reported**, source as above):

- **Index creation** — `CREATE INDEX ... USING (full-text | semantic)`. The speaker: *"it was obvious that we had to implement `CREATE INDEX` for DuckDB using either a full-text search index or a semantic search index."*
- **Full-text operator `@@`** (double-at). *"We have been plugging the double-at operator that is already used by some engines for full-text search."* Used in a `WHERE` clause where *"you expect a string... to be found in a field."*
- **Relevance score via a virtual column.** *"DuckDB... has this virtual column mechanism that we have been using in order to reflect the score of what the search results look like."*
- **Search all columns with `*`.** The dialect was extended to *"allow anyone to use star as the column... you are searching across all columns."* Framed as agent-friendly: *"in a lot of use cases, that's very useful for agents so that they don't do the big OR between all those queries."*
- **Semantic-search operator.** A separate operator *"that I believe is fairly elegant and also used in some engines"* — used to find rows (e.g. tickets whose title/message/comments) with semantic similarity to a phrase such as `"GDPR EU residency"`.

**Query execution as described (talk-reported):** at query time the extension *"rewrite[s] the query in a way that will be efficient for DuckDB to scan the table, scan the Parquet files, based on what the search results have retrieved."* The vector-search / full-text-search functions *"are returning row IDs and scores, and then... it's all just SQL."*

## 4. Architecture — AS DESCRIBED IN THE TALK (talk-reported, not verified)

**Talk-reported claims** (same source):

- Built as **"extensions on an extension"** — layered on DuckDB's extension system (which the speaker credits: *"one of the extraordinary powers of DuckDB, is its extension system"*). The DuckDB extension system itself is real and upstream-documented (https://duckdb.org/docs/current/core_extensions/overview); the *Alter Table* extensions on top of it are the vendor product.
- **DuckLake model as restated in the talk:** *"The lake is mostly a catalog and some Parquet files. Usually, you store them on a distributed storage, and we are using Postgres for now for the catalog."*
- **Index placement:** the search indices are *"small indices... very close to the Parquet files"*, so they *"fit very nicely with all the snapshotting system, with all the time-travel system that the lakehouse is bringing."* → i.e. the indices ride DuckLake snapshots / time-travel.
- **Metadata:** *"a few metadata tables in order to understand what are the indices that are created, what are the files, where are the files"*, plus *"a few table functions to explain how to search."*
- **Rust/C++ bridge:** Tantivy is Rust, DuckDB is C++ — *"a little bit of a game to make sure that this works well."* The Tantivy index *"actually relies on the file system from DuckDB."*
- **"Postgres catalog for now"** — the speaker flags the Postgres catalog as a current-state choice, implying it may change.
- Closing invitation to the DuckDB maintainers (Hannes, Mark, Pedro) to build FTS extensions, with an offer to contribute — signalling this is a vendor extension, not an upstream feature.

## 5. CRITICAL DISTINCTION — do not conflate these three things

There are **three distinct search stacks** in play. They are not the same and must not be conflated:

| Stack | What it is | FTS | Vector / semantic | Storage | Status in this repo's world |
|---|---|---|---|---|---|
| **Alter Table / "Duck Search"** (this file) | Third-party extension layered on DuckDB, presented at DuckCon #7 | `@@` operator, `CREATE INDEX ... USING` full-text index, `*` as the search column (talk-shown; exact grammar not spelled out) | separate semantic operator (talk-shown) | rides DuckLake Parquet + snapshots; Postgres catalog "for now" | **Vendor-reported. Not installed, not endorsed, not in core-x.** |
| **DuckDB core FTS** (`fts` extension) | First-party DuckDB extension | `match_bm25()` + `PRAGMA create_fts_index` (verified) | **none** in this extension (verified) | in-database inverted index under an `fts_<schema>_<table>` schema; **not auto-updated; must be rebuilt on data change** (verified) | Baseline for contrast; unrelated to Alter Table. |
| **Lance native search** | Lance's own indices | inverted/FTS index — see `../lance/05_scalar_indices.md` | vector search — see `../lance/06_vector_search.md` | inside the Lance dataset on R2 | The core-x system of record; **independent of both above.** |

**Verified contrasts against DuckDB core FTS** (https://duckdb.org/docs/current/core_extensions/full_text_search):

- DuckDB **core** FTS does **not** provide a `@@` operator. Retrieval is the `match_bm25()` macro (Okapi BM25). The `@@` operator shown in the talk is an **Alter Table** addition, not core DuckDB.
- DuckDB **core** FTS has **no vector/semantic search** in the extension. Semantic search shown in the talk is entirely the vendor extension.
- DuckDB **core** FTS *does* accept `'*'` in `create_fts_index` to index **all VARCHAR columns** — but note this is a **build-time** convenience for the `fts` extension, semantically different from the talk's **query-time** "use star as the column / search across all columns" behavior (the talk demonstrated `*` as the searched column but did not spell out the exact operator grammar). Do not equate them.
- **Footgun (verified):** DuckDB core FTS *"will not update automatically when the input table changes"* and *"needs to be rebuilt when the underlying data has been modified."* The talk's pitch (indices riding lakehouse snapshots) is presented as avoiding a separate sync pipeline, but that claim is **vendor-reported and not verified** — do not assume the Alter Table indices are incrementally maintained.

## 6. Version / attribution gating and footguns

- **Talk vs. product:** the speaker explicitly said the talk **simplified** the real behavior. Treat all syntax as illustrative, not a spec.
- **No independent verification:** as of July 2026 no official Alter Table docs (the published `altertable-ai/duckdb-altertable` extension covers remote connectivity only, not this search surface), DuckDB docs, or neutral third-party docs confirm the `@@` operator, `CREATE INDEX ... USING` full-text/semantic index, the semantic operator, or the star-column search as implemented. Marked throughout **"as stated in the talk; not independently verified."**
- **Date footgun:** the transcript filename/header carries **2026-07-11** (recording publish date). The talk was **delivered 2026-06-24** at DuckCon #7 in Amsterdam (verified: https://duckdb.org/events/2026/06/24/duckcon7/). Cite the delivery date for the claim, the publish date for the transcript artifact.
- **"Postgres catalog for now"** is a stated current-state qualifier, not a durable architectural guarantee.
- **Do not confuse `USING` semantics with DuckDB core.** DuckDB core `CREATE INDEX` builds ART/B-tree indices; `CREATE INDEX ... USING FTS` is not a core DuckDB grammar and is vendor syntax as shown.

## 7. Relevance to core-x

> **Load-bearing only where it touches the DuckDB → Arrow → Lance-on-R2 plane.** core-x already resolves structured queries over Lance on R2 and has **no FTS lane**. This extension is **not** a component of that plane and is **not** being adopted here — it is a competing, vendor-hosted retrieval model over DuckLake, whereas core-x's system of record is LanceDB under `s3://data-sink/active/`. If a schema-agnostic / semantic retrieval mode is ever wanted in core-x, the native path is **Lance's own FTS/inverted index and vector search** (`../lance/05_scalar_indices.md`, `../lance/06_vector_search.md`) directly on the Lance datasets — not a DuckLake-side third-party extension that would introduce a second storage/catalog surface (Parquet + Postgres catalog) alongside the Lance SoR. The one genuinely portable idea worth noting: the talk's "search returns row IDs + scores, then it's all just SQL" pattern mirrors how a retrieval step can hand row IDs back to a DuckDB scan — but in core-x that scan targets Lance, not DuckLake Parquet.

## 8. See also

- `../lance/05_scalar_indices.md` — Lance's own scalar / inverted (FTS) indices (the native, in-repo path).
- `../lance/06_vector_search.md` — Lance vector search (the native semantic path).
- DuckDB core FTS: https://duckdb.org/docs/current/core_extensions/full_text_search (verified contrast baseline).
- DuckDB extension system: https://duckdb.org/docs/current/core_extensions/overview (the mechanism the vendor extension builds on).
- Talk landing: https://duckdb.org/events/2026/06/24/duckcon7/ (title, speaker, date).
