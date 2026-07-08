# DuckDB: Not Quack Science — Ubuntu Summit 26.04

**Speaker:** Gabor Sarnyas (DuckDB Labs, Developer Advocate) — Ubuntu Summit 26.04, London.
**Published:** 2026-05-28.

*Talk transcript. Cleaned from an auto-generated transcript ("clock" → Quack, "Duck Lake" → DuckLake, etc.); wording lightly smoothed for readability, meaning preserved.*

---

Hello from London. Thanks for coming, or thanks for tuning in online. My name is Gabor Sarnyas. For a long time I was a database researcher, and I work at a company called DuckDB Labs as a developer advocate. Today I'm here to convince you that DuckDB is not quack science after all.

## What is DuckDB at a glance?

DuckDB is an open-source analytical database system, available under the MIT license. It's a relational database that speaks a dialect of Postgres — a standard-ish SQL dialect. Code-wise it's about a million lines of C++ with no external dependencies, which means we can compile it to a single binary. It's about 50 MB and you can put it just about anywhere. You don't need root rights, sudo, or anything — just drop the DuckDB binary and start using it right away.

My favorite thing about DuckDB is how versatile it is. I've selected three conceptual levels: **command-line tool**, **portable database**, and **database server**.

## 1. Command-line tool

In a Unix-like environment, to install it (apologies — we don't have a package) you `curl` a script that you can inspect, and you get DuckDB. Put it on your path, type `duckdb`, and you get a read-eval-print loop.

You can use DuckDB as a glorified calculator — e.g. `sin(pi/2)` = 1, or the number of days since the latest Ubuntu LTS release. But you don't need a million lines of C++ for a calculator, so let's do something more interesting.

There's a high-speed train line between Amsterdam and London. I encoded its stations as a TSV file (tab-separated values — a tiny bit easier for humans to read than CSV). Station name, country name, ordered alphabetically. To list the unique countries the train passes through:

**In bash**, out of habit I'd start with `cat`, pipe through `tail` (chop the header), `cut` (pick the second column, the country), then `sort -u`. You get four countries, which aligns with my real-life experience taking this train.

**In DuckDB:**

```sql
SELECT DISTINCT country FROM stations.tsv;
```

Almost like an English sentence. But there's something to unpack: in most SQL systems you have to create a table, define the schema, and load the data. With DuckDB, an **auto-detection mechanism** sees the filename, loads the file, determines the header names and field types, loads it into a temporary table, and returns the distinct countries.

DuckDB also works as a Unix CLI tool — pass the query with `-C`, yielding the same results. And we like composability, so you can pipe into DuckDB (spelling out that you're reading CSV from stdin) and out to stdout. DuckDB put right in the middle of a Unix pipeline.

Take a slightly more complicated table, `trains.tsv`: two Eurostar trains from last Friday — ID, timestamp, station name — operating over two time zones. DuckDB's CSV loader detects the first column is `VARCHAR`, the second is `TIMESTAMP WITH TIME ZONE`, and even converts everything to our local time zone (UTC+1).

Now combine the two tables — which stations of these services are in which countries:

```sql
SELECT * FROM trains.tsv JOIN stations.tsv USING (station);
```

DuckDB has a nice trick: you can take the last result of whatever query you ran and keep operating on it — e.g. `COPY _ TO` another file with a specific delimiter. Get data into DuckDB fast, get the result out fast, and then DuckDB gets out of the way. It doesn't have to run all the time.

Implementing the same in a standard Unix environment with no database is a tiny bit painful — joins, `tail`, `awk`, `sort`, pre-sorting columns, chopping and stitching headers. It gets you there, but it's not pretty, and it gets harder as query complexity grows. This is a common trajectory I've observed: a bunch of CSV files, start with `grep`/`sed`/`sort`, then you hit a junction — keep evolving a complex shell script (GNU parallel, chunking) or bite the bullet and rewrite in Python (usually inefficient, with dependencies like Pandas). What's great about DuckDB is you drop in a single binary and get a composable, parallelizable, easy-to-debug pipeline.

### More powerful than the CLI tools

DuckDB's SQL is more powerful than a lot of command-line tools.

**PIVOT** (not standard SQL, tastefully borrowed from other systems) turns a long table into a wide one. Example: `distance_from_london.csv` encodes distances along the railway (London St. Pancras = 0 km, Amsterdam = 542 km). To build a distance matrix, first create all station pairs with a Cartesian product (put the table twice in the `FROM` clause), take the absolute difference — that's a long table, so long it doesn't fit on screen. Then pivot it on `station2` using the first occurrence of the distance. Now you have a nice wide table.

Converting km to miles the standard-SQL way means repeating `/1.61` and casting to integer for every column — a lot of typing for trains with many stations. DuckDB's **`COLUMNS(*)`** lists all available columns, so `COLUMNS(* EXCLUDE station1)` applies the mile-conversion to everything except `station1` — same result in four lines, scaling to any number of stations.

**UNPIVOT** turns a wide matrix back into a long table. Again `COLUMNS(*)` does it for you — a simple five-line expression.

To sum up the CLI approach: DuckDB works in pipelines, has error handling and type safety, needs no storage (in-memory mode works perfectly), runs your workload in parallel out of the box, and can spill to disk if the workload is larger than memory.

I've blogged about this — re-implementing most of the *Data Science at the Command Line* book with DuckDB. In December 2024 I claimed DuckDB is faster at counting lines than `wc`, and got shouted at. What happened: I ran it on macOS with the ancient BSD `wc`, which DuckDB beats. In a follow-up: back then GNU coreutils still lost to DuckDB, but the `uutils` Rust rewrite was already faster. That's changed — in 26.04, the Rust-rewrite `wc` plainly beats DuckDB. I ran the benchmarks a couple of days ago.

## 2. Portable database

Why do database work outside a client-server application? Databases are neat if you can import them quickly. DuckDB has drivers for many languages; import a driver and it attaches to either an in-memory database or a single `.db` file. You get a full-fledged database with no configuration and no client-server protocol setup, then implement application logic with transactions, primary keys, constraints — the whole declarative SQL toolkit.

DuckDB integrates really well in the data ecosystem — reads and writes just about any protocol or format. I often do not-really-database work with DuckDB. Once I had a Java program comparing a billion numbers to another billion expected results in a for loop; instead I imported DuckDB, put it in DuckDB, and wrote a simple debuggable SQL script. You'd never normally use a data warehouse for this, but because it's just a library you import it, do one operation, and leave it alone.

We have drivers for Python, Go, Rust, C, C++, Java, JavaScript, R — and DuckDB also runs in **WebAssembly**, so it runs in the browser.

**Scale example:** a data set of the last 6 years and 4 months of trains that ran in (or through) the Netherlands — every train stop of every service: **160 million records, ~21 GB of CSV.** Question: what's the average delay of trains in the Netherlands per year? Using **Marimo** (a Python notebook environment): import DuckDB in the first cell, connect to a local database file, then `FROM 'services*'` globs and loads all the files at **~1 GB/second** on a modern laptop — indeed 160 million rows.

The analysis query extracts the year, converts delays to seconds, averages, prints nicely. DuckDB results read into Python as Pandas data frames, so you click a chart icon, spend 30 seconds building a bar chart, and see trains were most punctual in 2020 (I wonder what happened in 2020). The trend since is not great, but the average delay is still under a minute. The whole analysis took **150 milliseconds** — more than 1 million rows per millisecond. Fast and economical.

### Friendly SQL

I like DuckDB's *friendly* SQL even more than its powerful SQL. It gets rid of some of SQL's rough edges:

- **Remote files in `FROM`:** put an HTTP URL in the `FROM` clause. DuckDB fetches the data from the remote endpoint, decompresses it, detects the CSV schema, and loads it into a temporary table.
- **Trailing comma** at the end of `SELECT` — most languages adopted this decades ago. Neat for interactive analytics: comment a line out without breaking the query.
- **`GROUP BY ALL`** — why can't the computer tell that `SELECT station, avg(delay)` needs to be aggregated by station? With `GROUP BY ALL`, DuckDB aggregates on all non-aggregate columns in the `SELECT`. A fan favorite — and a competitor favorite: other vendors have cloned it, including Postgres this year and a future version of the ISO SQL standard.

### Does it scale?

I took **TPC-H**, the gold standard in database benchmarking, at 1 TB, 10 TB, and 100 TB (measured as CSV size). 10–15 years ago this was big-iron territory: a $150K server plus a $150K database license. With DuckDB:

- **1 TB:** five Raspberry Pis (~$100 each) with 16 GB RAM, an SSD, a cooling box, Ubuntu — crunches it without much problem.
- **10 TB:** an ultrabook — I ran it on a maxed-out Framework 13 with 128 GB RAM.
- **100 TB:** used to be distributed-processing territory; runs on a single server with 1.5 TB of memory (large but not huge by today's standards) — spin it up in most cloud providers for a few hours.

Performance-wise, I ran a more complex version (updates, concurrent inserts/deletes). I'd heard the new scheduler might affect performance, so on the 300 GB data set: installed 24.04 with kernel v6, ran it, did a dist-upgrade — and found a **10% improvement on the composite score**. It's worth upgrading your Ubuntu; your database will thank you for the better scheduler.

### The single-player limitation

It's not all fun and games. DuckDB is in-process, which makes it fundamentally a **single-player experience**. You can connect to the database file in read-write mode, and connect multiple clients in read mode, but you cannot connect multiple clients in read-write mode — it would destroy our caching and hurt performance.

## 3. Database server — Quack

This is where the third part comes in. A couple of weeks ago we released **Quack** (of course, how ducks communicate). It's a **multiplayer mode**: the database still writes and reads a single file, but clients can connect for concurrent read-write access. Unlike a normal database like Postgres — a big database process with rather simple clients — here the **clients are also DuckDB instances**, so you can move the computation around wherever you see fit, and the clients are full-fledged database systems.

It's work-in-progress, planned to finalize by autumn. It uses HTTP. You authenticate with tokens (or bring your own) and authorize operations with callbacks (or bring your own). Currently beta, but already usable.

Usage is simple — set up two DuckDBs:

```sql
-- Server
CALL quack_serve('quack:localhost', token = '...');
-- create some data

-- Client
CREATE SECRET (TYPE quack, TOKEN '...');
ATTACH 'quack:localhost' AS remote;
SELECT * FROM remote.<table>;
```

That's the boring part — every database can do it. What's more interesting: because we now have databases everywhere, we can do **distributed processing** — a client on the left, a coordinator in the middle, servers on the right doing the heavy lifting. Lots of creativity possible, because you can move where your query (or parts of it) actually runs.

## Recap of the three use cases

- **Command-line tool** — like a combination of Unix tools and data frame libraries.
- **Portable database** — like SQLite, but geared for analytical workloads.
- **Database server** — like Postgres, but for analytics.

## Deep dive: storage and indexing

**Row vs. column storage.** Most databases (SQLite, Postgres, MySQL) use **row-based** storage — whatever is logically in the same row is physically in the same location on disk (think of the disk as a one-dimensional tape). That's not great for analytics: computing average delay per station means reading all the way through, hard to parallelize, lots of cache misses.

**Columnar storage** stores data by column, with unintended advantages. There's usually logic in how data is inserted — the date/timestamp column is often sorted or nearly sorted (here, all "28th of February," so encode it as one constant value). Most delays are zero, so apply **run-length encoding** ("zero times three, then two for the one delayed train"). This is what analytical databases like DuckDB use, and it's also what the **Parquet** format uses.

**Parquet** is far superior to CSV for passing data around: binary, columnar, and it encodes the schema plus high-level statistics. Parquet has **row groups** (~100,000 rows), and for each column in each row group it stores the minimum and maximum value. These **min-max indexes** let you prune efficiently: asking which stations had the worst delays on weekends of February 2025, you keep row group 1 (has February data) and immediately throw away row group 2 (minimum is March). You can often prune to a single percent of the data you need.

DuckDB chose columnar storage — similar to Parquet, but an **updatable format that holds multiple tables in a single file**. Load the 6+ years of services in 20 seconds; min-max indexes are created automatically during loading; the "worst delays" query runs in **30 milliseconds**. (It turns out the worst Dutch train stations are actually in Germany — longer travel time accumulates more delay; the top 25 or so are in Germany.)

It gets even better with remote endpoints. Column projection (reading just one column from a Parquet/DuckDB file) and **predicate pushdown** (leveraging min-max indexes to skip data) work over **HTTP range requests** — saving bandwidth, remote communication, and potentially egress costs. Works with DuckDB's format and with Parquet, no setup, out of the box.

## DuckDB in practice

38,500 GitHub stars. ~100,000 human website visits per week (excluding bots/crawlers). 10M+ PyPI downloads. Releases code-named after animals (I wonder where we got that from). LTS releases last 1 year — we move at a higher pace.

**Funding structure:** I work at **DuckDB Labs**, founded in 2021. Now that we have the DuckDB database, the Quack protocol, and the DuckLake lakehouse format, it made sense to call ourselves DuckDB Labs. We provide services around this stack. We're **bootstrapped** — no venture capital — sustaining ourselves from services, based in Amsterdam. The intellectual property is held by a separate organization, the **DuckDB Foundation**, a Dutch nonprofit whose sole purpose is keeping the project open source under the MIT license; it collects donations to that end.

**What is DuckDB used for?** Bluntly, the number one use case is to **save money**: fast local development (saves time and network costs), replacing proprietary systems (saves license cost), and replacing distributed systems (cuts complexity and compute cost). It's also great for **last-mile analytics** — I wouldn't use DuckDB on a 1-petabyte log (I'd spin up a Spark cluster and pre-aggregate), but once you have a 200 GB binary pre-aggregate, keep aggregating with DuckDB and build really fast dashboards, including in the browser. And it's great for **learning SQL** — I learned SQL from a proprietary evil database with bad syntax and don't have good memories of it; any open-source system is preferable, and DuckDB is easy to install with no DBA needed.

**Learning DuckDB:** documentation to get you up to speed, plus the DuckDB **library** — 100+ pieces of material: our scientific papers, papers by others building on DuckDB, podcasts, books, interviews. We've sponsored two university courses at the University of Tübingen: a basic SQL/relational-database course, and a detailed course on DuckDB internals and extension development.

## Summary

I hope I've convinced you that DuckDB is not quack science, and databases are not rocket science either — not clunky, intimidating software. You're more than welcome to try DuckDB; it only takes a few seconds.
