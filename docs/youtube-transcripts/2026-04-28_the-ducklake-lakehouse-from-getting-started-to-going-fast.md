# The DuckLake Lakehouse: From Getting Started to Going Fast

**Format:** MotherDuck live webinar — 2026-04-28.
**Speakers:** Gerald (MotherDuck marketing, host) and Alex (MotherDuck DevRel; co-author, *DuckLake: The Definitive Guide*, O'Reilly).
**Topic:** What DuckLake is, how it relates to MotherDuck, a getting-started demo, and performance tuning — plus extended Q&A.

*Webinar transcript. Cleaned from an auto-generated transcript ("Duck Lake" → DuckLake; wording lightly smoothed, meaning preserved.)*

---

**Gerald:** Welcome to our livestream on the DuckLake lakehouse — today we go from getting started to going fast. I'm Gerald on the marketing team at MotherDuck, joined by Alex. (Opening polls: most attendees have played with DuckLake a little; a few run it in production; familiarity split roughly evenly across Iceberg, Delta Lake, and DuckLake.)

Agenda: what DuckLake is and why it's exciting; what MotherDuck is and how it relates; a short getting-started demo from Alex; then tuning your DuckLake to make it faster; and Q&A throughout — drop questions in chat anytime.

**Alex:** Howdy, I'm Alex. Background in industrial systems engineering, then a lot of data — time at Intel, part-time at DuckDB Labs on docs/blogging, and ~3 years at MotherDuck (customer support → DevRel). I've read just about every PR in the DuckDB repo since ~2020. I'm a co-author on the upcoming O'Reilly book **DuckLake: The Definitive Guide** — two chapters are out (link in chat), with chapter 3 (architecture deep dive) and chapter 4 (performance tuning) coming.

## What is DuckLake?

DuckLake is an **open table lakehouse specification**. It has "duck" in the name but it's a fully open spec — implementations exist beyond DuckDB, in **Spark, Trino, and DataFusion**. The whole design goal is **simplicity** — not a word you hear as the #1 feature of any other lakehouse. Start simple and elegant, and that elegance leads to speed and scalability.

The key architectural decision: keep **both catalog and metadata inside a SQL database** — DuckDB, SQLite, Postgres, or MotherDuck, mix and match.

Getting started is three commands: **install** the DuckDB extension, **attach** a catalog, **start using it**.

More about DuckLake:
- **Partitioning** scales to petabyte data while retaining **high ingestion speed** — bring data in as-is, compact later in the background.
- **Bring your own blob storage** (MotherDuck) or full control (open source), using industry-standard **Parquet** and blob storage.
- Because of the SQL-database architecture, **query latency is very low** — milliseconds/hundreds of milliseconds instead of multiple seconds — and you can ingest very frequently (e.g. 30×/second).

**By analogy to Iceberg** (Delta is similar, fewer layers): at the bottom, **data files** — Parquet in both, columnar, indexed metadata, strict typing (DuckLake's format is almost identical to Iceberg — we didn't reinvent the wheel). Above that, the **metadata layer** (which Parquet files, which tables, which versions, change history). Above that, the **catalog** (multiple tables, concurrency) — in Iceberg that's a web API with a database behind it. DuckLake says: **if you have a database in the stack, use it for everything it's good at** — relational databases excel at high-concurrency transactional/metadata operations.

With **Iceberg**, querying takes **four sequential round trips** (catalog → metadata file → manifest list → manifest file → then data), all against object storage (great throughput, poor latency, ~100 ms each) — so even the fast case is often seconds. With **DuckLake**, you talk to **one relational database** (single-digit ms), then grab the Parquet files you need. Still Parquet on object storage, still schema evolution and time travel — just much simpler.

### Q&A (intro)

- **On-prem / K8s Helm chart?** No barrier — DuckLake is amenable to on-prem; you're managing a database (likely Postgres) and DuckDB compute, both Kubernetes-manageable.
- **How do tables map to Parquet files (I deleted rows but see one file)?** DuckLake is **append-style**: adding data creates new files; deleting adds a **delete file** (following the Iceberg spec, currently V2; V3 in testing) to preserve time travel. Merge/clean up with a metadata function that rewrites a new file, then a cleanup maintenance op removes it (if you don't need to time-travel back). Also, **inlining** (default for inserts ≤10 rows) keeps small inserts in the catalog database rather than writing Parquet — try inserting 100 rows to see the expected behavior.

## What is MotherDuck?

A **serverless cloud data warehouse** with **DuckDB** at its heart — "infrastructure for answers," optimized for **time to insight**. Connect via the DuckDB drivers in a couple of lines.

Architecture: **serverless** (spin up on connect, spin down aggressively when idle — pay for what you use, unlike lakehouses with a long-lived cluster). **Unabashedly single-node** — each query runs on one machine, but many queries can run on many machines; single-node avoids expensive shuffle/broadcast joins across thousands of nodes. And **dual execution** — run queries in the cloud, locally, or a mix (great for caching, follow-up analysis, and local development).

### Ways to deploy DuckLake

Three components: **storage** (Parquet files — where?), **metadata** (catalog + metadata in a relational DB — many choices), **compute** (local or cloud).

- **Lightweight & local:** files/folders on your laptop SSD, DuckDB as the local metadata DB, laptop CPU.
- **Cloud persistence, local compute:** any object store (S3/GCS/Azure Blob) + a **multiplayer** metadata DB (Postgres recommended); everyone runs compute on their own laptop.
- **Your own cloud DuckLake:** server-side compute you run.
- **MotherDuck fully managed:** MotherDuck-managed bucket + MotherDuck catalog + elastic serverless compute (with dual execution to the laptop). Or **bring your own bucket** — your Parquet files, an industry-standard format that'll last 50 years, keeping the openness.

## Demo

The **DuckDB UI** (download the CLI via a single curl command, run `duckdb -ui`) opens a browser UI on localhost — fully open-source DuckDB; it looks like the MotherDuck UI because MotherDuck contributed it back to the community for free.

`INSTALL`/`LOAD` DuckLake (happens automatically on use — downloads the extension from an S3 bucket into a common location and loads it into memory). Then connect with an `ATTACH` (SQLite-style):

```sql
ATTACH 'ducklake:catalog_one.ducklake' AS lake (DATA_PATH 'lake_data');
USE lake;
```

`ducklake:` marks it a DuckLake catalog; you can optionally specify the DB type (SQLite/DuckDB/Postgres) — default DuckDB, so it's omitted. The catalog is a file on disk (technically a `.duckdb` file — I name it `.ducklake` so I remember what it is). `DATA_PATH` names the Parquet folder (created if missing; later can point to S3). **40 milliseconds** and I have a lakehouse on my laptop.

Create a `testing` table and query it (`FROM testing` == `SELECT * FROM testing`) — two columns, one insert. The **catalog is already visible in the UI** — all the metadata: `ducklake_table` (one table `testing`, a UUID), `ducklake_column` (columns creatively named `i` and `j`). You can **join** the metadata tables — join the data-file table to the table to see how many Parquet files exist: one so far (inserted 100 rows, over the inlining threshold). Insert more → **two files with different snapshots** → time travel to a snapshot/timestamp.

Join `ducklake_column_stats` (or the column/table metadata) to see the **min/max per file** — the main way to do performance tuning. Column `i` is ordered 0–99; column `j` is random. For a filter `WHERE i < 20`, only one file needs reading — decided with a single SQL query filtering `value BETWEEN min AND max`.

**Compaction / maintenance** — DuckLake needs less maintenance than other formats, but you still want at least one background job to **merge adjacent files**: two files → one, ranging 0–999.

To do the same in **MotherDuck**, click "sign into MotherDuck," log in, and use the slightly different syntax — a `CREATE DATABASE ... TYPE ducklake` instead of `ATTACH` — then create tables and push/pull data as before. (You also get stored notebooks and the built-in visualization platform, **Dives**.)

## Tuning your DuckLake

Three settings you'll almost always want (via `CALL my_ducklake.set_option(...)`, `my_ducklake` = your DuckLake DB name):

1. **Parquet version 2** — better compression for common data patterns, near-free performance-wise. Only downside: ecosystem compatibility (make sure other tools can read Parquet V2).
2. **Zstandard compression** instead of the default Snappy — best-in-class for Parquet. Only downside: compatibility.
3. **Larger row-group size** *if running on the cloud* (keep the default on a laptop). Object storage has high latency but high throughput, so request bigger batches. Per a TU Munich benchmark, the diminishing-returns chunk size for object storage DB reads is **8–16 MB** per chunk; DuckDB reads a column-row-group chunk at a time, so set `row_group_size_bytes` so each column is ~8 MB (e.g. ~80 MB total for a 10-column table). Don't go giant.

**Keep compute and storage close** — most queries filter/aggregate, and you don't want to download everything to your laptop. Run cloud compute, and put it in the **same cloud and region** as your storage bucket.

Then **workload-specific** (optional) tuning:

**Fast ingest:**
- **Data inlining** (default ≤10 rows) — batch small inserts as rows in the catalog DB, flush to larger Parquet later. Reads still see the inlined rows, fully transactional — like Kafka's buffering but with lakehouse transactionality, and a big win over tons of tiny files. Lets you ingest multiple times/second.
- For **bulk**: enable **per-thread output** (each thread writes its own Parquet → blast object storage faster; trade-off: more files → compact more often), and **lower the retry wait time** (DuckDB uses optimistic concurrency control; default retry wait is 100 ms — long for a transactional DB — shrink it to insert faster).

**Fast read** — read as little data as possible, at two layers:
- **File-level statistics** in the catalog — keep the min/max ranges as narrow as possible for the columns you filter on. Key tool: **partitioning** (by time buckets, or domain-specific like customer). Don't over-partition (a million customers → a million tiny files); use **bucket partitioning** (Goldilocks — say 1,000 buckets), somewhere in the hundreds-to-thousands range.
- **Within a file**, read only the columns and **row-group chunks** that match your filters. Improve with **sorting/clustering** — `SET SORTED BY (...)` with any expression(s) (e.g. customer then date). Aim for ~**10 row groups per file** → up to a 10× boost; with the ~80 MB example that's ~1 GB files (a good lakehouse file size, ~0.5–1 GB). *(Sorting is Alex's main contribution to DuckDB so far.)*

## Extended Q&A (highlights)

- **Multiplayer catalog in MotherDuck** — MotherDuck brings DuckDB's single-player engine into the multiplayer realm (multiple connections to the same DB); ownership tends to be by user, sandboxed (nice in the agentic era); recommend **service accounts** so multiple humans manage a central resource.
- **Security / access governance** — multiple patterns. Traditional: manage access on the **object store** (folders by table/partition, grant folder access; DuckLake stores Hive-partitioned by default, can be disabled). Unique to DuckLake: **encryption** — one boolean, and every Parquet file is encrypted with a different key stored in the metadata catalog, so protecting the catalog (row-level security) gates the keys. Keys are long-lived/built into the file, so MotherDuck adds credential-lifecycle management.
- **Worker count** — depends on engine. CPU-limited: match threads to CPU threads (e.g. 32 for 16 cores + hyperthreading). **Network-bound** (common in lakehouses): multiply that by **2–5×**.
- **Import/export vs Iceberg** — importing Iceberg → DuckLake is **metadata-only** (same Parquet format), so 100 TB of data copies as just its metadata. Exporting back to Iceberg is still being worked on because DuckLake allows **multiple snapshots within one file** (a separate snapshot-ID column enables row-level time travel — unique to DuckLake), which Iceberg doesn't model. Alex has a side project on an **Iceberg REST API wrapper** around DuckLake (`alex@motherduck.com`).
- **Who writes the Parquet?** The engine (DuckDB, or Spark/Daft/Pandas/Polars). Optimistic concurrency: the file is written to object storage first (invisible), then a **commit registers it in the catalog** — so you can write files with any engine and register them later.
- **Local caching** — DuckDB caches Parquet pieces in memory while querying; community extensions (e.g. one by Peter Boncz at MotherDuck) extend caching to SSD.
- **Primary/foreign keys** — hard to store in a lakehouse while keeping the other benefits; workarounds include moving data selectively to MotherDuck. **Non-enforced informational primary keys** are on the DuckLake roadmap.
- **Speeding up catalog queries (indexes)?** On MotherDuck, rarely needed. On Postgres, profile first (Postgres is already far faster than 3× object-store round trips); consider **indexes on `table_id`** and, if very large, **Postgres partitions by `table_id`**. Standard Postgres tuning, not DuckLake-specific.
- **Inline flush threshold** — reads pull the whole inline table and filter in memory (catalogs like Postgres filter slower than DuckDB), so keep it to **< ~1 million rows** per flush, and wait until Parquet files reach a reasonable size. Inlining and compaction can be combined.
- **`SET SORTED BY` at table creation?** Today it's a separate command after creation (deduplicated, so re-running is a no-op — safe to always run in a dbt hook). `CREATE TABLE ... sorted (...)` syntax exists in DuckDB and is on the DuckLake roadmap.
- **dbt / Metabase** — MotherDuck are co-maintainers of the dbt-duckdb repo; file bugs. Metabase works; MotherDuck's **Postgres endpoint** (pretend to be Postgres to your BI tool) can smooth it.
- **Self-hosted MotherDuck?** Not today — MotherDuck is database-as-a-service only; **open-source DuckLake in your own cloud account** is the current answer (unlimited concurrency, many compute containers on one bucket/catalog). VPC deployment is a ways out.
- **BYO bucket / R2 vs MotherDuck-managed cost** — keep the bucket close. **R2** charges no egress (great billing model) but is a bit slower, so tune for larger files/chunks; storage is cheap. MotherDuck storage is competitively priced. Most lakehouse cost (~80–90%) is **compute**, so optimize for fastest query response — benchmark whole-system cost, not just storage.

**Gerald:** With that, we'll wrap up — thanks for all the great questions. Join our community Slack (Alex hangs out there, plus a DuckDB channel). Cheers, everyone.
