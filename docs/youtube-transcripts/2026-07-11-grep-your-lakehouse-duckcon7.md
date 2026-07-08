# Grep Your Lakehouse: Search-First Retrieval for DuckDB-Powered Agents — DuckCon #7

**Speaker:** Sylvain Utard (co-founder & CEO, Alter Table) — DuckCon #7, July 11, 2026.

*Talk transcript. Cleaned from an auto-generated transcript ("Alta Table" → Alter Table, "TenTen TV"/"Tenny TV" → Tantivy, "DuckLake" spelling normalized; wording lightly smoothed, meaning preserved).*

---

I hope you liked the formal talk with the agentic taste, because we are going to speak about agents again. We're going to speak about search, about lakehouse, and DuckLake.

If you're using coding agents today — whether through Claude Code or Cursor — you've probably experienced that their first instinct, the first thing they do after your prompt, is **search**. They want to grab your code base, find something across everything. They don't care about the structure, the folders — they just want to grab everything.

That same pattern is coming to data. And yet, if you've been playing with the data-analyst agents introduced a few quarters ago, what they're really good at right now is **text-to-SQL**. They know better than anyone how to write a good SQL query. But that's not enough, because SQL is very structured — it's about tables and columns — so they also need to understand **what table and what column to use for what use case.**

In the BI space the big word is **semantic layer**. If you have one, maybe you don't even need this because everything is documented. But I can tell you this doesn't happen often. What's really missing in SQL is a **schema-agnostic retrieval mode.**

## Who I am

I'm Sylvain. I spent 13 years building search engines. I started as a C++ engineer at a French startup competing with Google at the time — they lost the battle. Then, as the first employee and VP of Engineering at **Algolia**, a search-as-a-service API. I spent 4 years in B2C where I really experienced what large-scale data means. Since last year I'm co-founder and CEO of **Alter Table**, building a **data runtime for this AI era on top of the lake**.

We built a lakehouse with federation, and — as you'll understand — **we hate pipelines**. The platform comes with a knowledge graph and AI agents, because we really believe that's what's happening next. The TL;DR: **we have extended DuckLake with search capabilities.**

## Why add search to a lakehouse?

When you're building/using a lakehouse, you have all the data already there. And I'm not sure if you've been there, but I've done it so many times: you want to **synchronize a database with a search engine** — because the database does one thing and the search engine does another. That pipeline is a nightmare. I hated it, and we've all seen it often in the data world.

What we wanted for Alter Table was a new retrieval mode for our lakehouse, so we could rely on **the same storage layer that DuckLake already uses** for search and semantic search.

## A quick reminder: what indexing and search are

- **Full-text search:** indexing converts input objects into their reversed version — you split the text, maybe normalize it, and build an **inverted index**: an optimized data structure mapping the words in documents to their document IDs. At query time, full-text search is very efficient — you get the inverted list of document IDs, and querying two words with an AND is just an **intersection**.
- **Semantic search:** something very similar. Instead of tokenization and simple normalization, it goes further with **word embeddings** — creating vectors that reflect document content. Indexing builds a highly optimized data structure (we chose a **graph** structure). At query time you embed the query into a vector, and with the optimized graph you can search all objects with **semantic similarity**.

## Extending DuckDB

We were very inspired by what the DuckDB team did a few years ago. One of DuckDB's extraordinary powers is its **extension system**. For us it was obvious we needed it — so we've been **building extensions on an extension**. We didn't start from scratch; we used a bunch of other open-source technologies. Shout out to **Tantivy**, which has been delivering amazingly.

A wise man once told us: **"It's all just SQL."** That's exactly what we wanted to do — extend DuckDB's SQL surface.

- **`CREATE INDEX`:** the database world has indices — usually B-trees in transactional/non-lake databases. For us it was obvious to implement `CREATE INDEX` for DuckDB using either a **full-text search index** or a **semantic search index** — something very similar.
- **Query syntax:** in your `WHERE` clause you expect a string (here with a typo) to be found in a field. We're all lazy, so we prefer operators — we plugged in the **`@@` operator** already used by some engines for full-text search.
- **Scoring:** that's not enough — you want scoring. Like Google, you stop at the first result because it's the most relevant. Every search has a **score** reflecting relevancy. DuckDB has a **virtual column mechanism** that we use to reflect the score of the search results.
- **Search across all columns:** instead of searching only the `message` column, we extend the DuckDB SQL dialect to allow **`*` as the column** — searching across *all* columns. In a lot of use cases this is very useful for agents, so they don't have to do a big OR between all those per-column queries.
- **Semantic search:** we introduced an operator (fairly elegant, also used in some engines). Once you have that, it opens a whole world of possibilities — e.g. running an analysis on top of a **subset of candidates**: tickets where anywhere (title, message, comments) has semantic similarity with "GDPR EU residency."

## Implementation

Once we had the SQL surface, it was time to implement — and again, "it's all just SQL" guided the design.

You probably know how DuckLake works, but to repeat: the lake is mostly a **catalog plus some Parquet files**. Usually you store the files on distributed storage; we're using **Postgres for the catalog** for now. Adding search to DuckDB consists of bringing those small **indices** (the optimized data structures we built earlier) **very close to the Parquet files** — so it fits nicely with the lakehouse's **snapshotting and time-travel** systems.

Under the hood:

- We added a few **metadata tables** to understand which indices are created, what the files are, and where they are.
- A few **table functions** to explain how to search.
- **Bridging with Tantivy:** one world is Rust, one world is C++, so it was a bit of a game to make it work well without reinventing the wheel on both sides. The Tantivy index we use for search actually relies on the **file system from DuckDB**, and a lot of things happen like that.
- **At query time** (a very simplified version): we rewrite the query so it's efficient for DuckDB to scan the table / the Parquet files based on what the search results retrieved. The DuckDB vector-search / full-text-search functions return **row IDs and scores** — and then, well, it's all just SQL.

## Takeaway

DuckDB has been "it's all just SQL," and so is **Duck Search**. Hannes, Mark, Pedro — if you're looking for the next super-secret big thing, please be my guest and build search extensions for DuckDB. We love open source, and if that happens we'd love to contribute.

For the sake of the talk I've simplified a lot of how search actually works within DuckDB. But what you should remember: **I hate pipelines, and you should too.** Thank you.
