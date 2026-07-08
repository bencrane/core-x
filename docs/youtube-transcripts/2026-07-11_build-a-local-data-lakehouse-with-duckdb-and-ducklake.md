# Build a Local Data Lakehouse with DuckDB and DuckLake

**Speaker:** "DataGuy" (YouTube review-request video) — 2026-07-11.
**Topic:** Building a complete DuckLake lakehouse locally — time travel, schema evolution, multi-table transactions, and querying the metadata catalog directly.

*Video transcript. Cleaned from an auto-generated transcript ("Duck Lake"/"DuckDB Lake" → DuckLake; wording lightly smoothed, meaning preserved.)*

---

Hey y'all, DataGuy here, back with yet another review-request video — a continuation on the local data lakehouse theme, this time using **DuckDB / DuckLake**, which one of my followers wanted to see.

What I want to prove: in modern data lakehouses your actual data is a handful of nice clean **Parquet files**, but the bookkeeping — which files went to which table, which change in which version — normally lives in a sprawling tree of JSON, Avro, and manifest files. DuckLake has a cool solution: **embed the metadata in a database** — because databases have been the world's best tool for tracking metadata for 50 years.

Today we build a complete DuckLake lakehouse on your laptop with **time travel, schema evolution, and multi-table transactions**, and at the end I show that the entire metadata layer is also a database you can run a `SELECT` against. All run locally, driven by a `make demo` file.

## The plan

A lakehouse is two things: **data** and the **metadata** that turns a bag of files into a real table you can query. DuckDB keeps the data in Parquet files and stuffs all the metadata into a SQL database, making querying much simpler. Useful for anyone who wants lakehouse features locally without standing up a catalog service or Spark cluster.

One command kicks off a Python script that wipes any old lakehouse (always start clean), then runs six or seven chapters of SQL in order:

- **Chapter 1** — attach to the lakehouse; point DuckDB at a catalog file and a Parquet folder.
- **Chapter 2** — create two tables (`customers`, `orders`); rows get written out as Parquet; every statement records a snapshot in the catalog.
- **Chapter 3** — inspect storage.
- **Chapter 4** — time travel.
- **Chapter 5** — schema evolution.
- **Chapter 6** — a transaction changing two tables at once.
- **Chapter 7** — close DuckLake entirely and open the catalog as an ordinary database to read the metadata with our own eyes.

## Setup

A self-contained environment: a setup script creates a virtual environment and pre-downloads the DuckLake extension. A tiny Python snippet opens DuckDB, sets the extension directory to `duckdb/extensions`, and installs DuckLake — so the extension binary lands **in the project**, not in the usual `~/.duckdb` home folder. The repo is completely sealed.

The **Makefile** runs the setup and demo commands: `make setup`, `make demo`, `make shell`, plus `make reset` (reset the lakehouse) and `make clean` (nuke back to a fresh clone). If the venv Python doesn't exist, the rule runs setup first, and every other target depends on it.

The data is synthetic — customers and their orders (typical company use case). A `generate_data` script builds the CSV files via Python; you can run it repeatedly. `demo.py` is the conductor — it contains little SQL itself; `main` threads a **single connection** through all seven chapter scripts in order: create, load, inspect, time travel, schema evolution, transactions, metadata reveal, then a recap. The order on screen is the order of the story.

## The SQL

**Attach.** Set the extension directory to the project's `duckdb/extensions`, then `INSTALL ducklake; LOAD ducklake;`. Turning your database into a lakehouse is just two lines. Then:

```sql
ATTACH 'ducklake:<path-to-catalog.db>' AS lake (DATA_PATH 'lakehouse_data');
USE lake;
```

`ATTACH` this DuckLake catalog as `lake` — note the `ducklake:` prefix followed by a path to the catalog file. `DATA_PATH lakehouse_data` is where the Parquet goes. That single statement **is the entire architecture**: metadata in a SQL file here, data as Parquet over there — separation of church and state. `USE lake` makes sure you're using it.

**Create and load.** Create the `customers` and `orders` tables and insert the data from the CSVs. Nothing exotic.

**Inspect storage** — "where is my stuff physically?"
- `lake.table_info` (a DuckDB function) reports, per table, how many Parquet files back it and how many bytes each takes — two tables, one file each right now.
- `lake.snapshots` lists every version of the lakehouse so far: snapshot 0 is the catalog initializing, then a snapshot for creating `customers`, one for creating `orders`, one for loading each. **Every change is a row in a table** — the version history becomes a query instead of a folder of files you have to parse.
- Back in the demo file, after the queries it walks the actual lakehouse data folder and prints the real Parquet files with real byte sizes, plus the size of the catalog file itself. On screen: a couple of Parquet files holding the data, and one DuckDB file holding all the metadata.

**Time travel** — a big reason people like lakehouses, and DuckDB gets it for free. Simulate the next day's load (now 16 orders, revenue up to 924.3). The magic:

```sql
SELECT count(*), sum(revenue) FROM orders AT (VERSION => 4);
```

"Give me the `orders` table as it existed at snapshot 4, before today's batch" — you get back 12 and 706.919, the exact same numbers as before. Same table, same query, same point in history — no restoring a backup, no separate audit copy.

**Schema evolution** — the headline feature: schema changes in DuckLake are **metadata-only**; no Parquet file is ever rewritten.
- Add two columns to `customers` — `loyalty_tier` defaulting to `bronze`, `lifetime_value` defaulting to `0`. The existing 8 customers didn't have values, so the default gets **filled in at read time from the catalog**; the old Parquet files are never touched.
- Rename a column to `full_name` — the Parquet files still say `name` inside; DuckLake just records in the catalog that the column the world calls `full_name` maps to that data. A rename is a metadata edit.
- `SELECT` from `customers` gets `full_name` and the two new columns all populated; the same `SELECT` at a different version reflects the change. `table_info` shows the file count for `customers` is exactly what it was before all the `ALTER` statements — same files, same bytes, added and renamed a column, **DuckLake wrote zero data** (there wasn't any new data). That's the whole promise of schema evolution.

**Multi-table transactions** — where DuckLake really shines. Processing a real order: record the sale *and* update the customer's running total — either both happen or neither.

```sql
BEGIN TRANSACTION;
INSERT INTO orders VALUES (1017, ...);
UPDATE customers SET lifetime_value = ..., loyalty_tier = 'gold' WHERE id = 3;
COMMIT;
```

Two tables, one atomic unit. How do we know it was atomic? The newest snapshot shows **both tables move together** — one snapshot, two tables. In Iceberg/Delta, each table versions on its own, so a clean atomic commit across two is awkward; a multi-table transaction is coordination the format doesn't hand you. Here it's just `BEGIN`/`COMMIT`, because the catalog is a real database. A check: customer 3 now shows `gold` and lifetime value 88 — sale and customer update land together.

**Rollback** — open a transaction, do something dumb (re-tier every customer to platinum, delete every paid order), then `ROLLBACK` instead of committing — it never happened. Counting tiers and paid orders shows them untouched.

## Querying the metadata — the whole point

Querying the metadata opens up new use cases and is much easier. Imagine the DuckLake connection is closed; we open only a **raw metadata connection** — plain DuckDB, no DuckLake extension — pointed at the catalog file.

- List every table whose name starts with `ducklake` → ~**30 tables**. That's the pile of metadata that Iceberg/Delta keep as files on disk, here as queryable tables.
- Version history: `SELECT * FROM ducklake_snapshot` — the snapshot log we saw via the fancy function earlier.
- Every Parquet file: `SELECT * FROM ducklake_data_file` — the pointers from logical tables to physical files, as a table you can query.
- One of the cooler ones: `ducklake_column` filtered to `snapshot IS NULL` gives the current live version of every column — `full_name`, `loyalty_tier`, `lifetime_value`, all as plain rows. The rename isn't magic; it's just an update to a metadata table.

That's the whole thesis of **DuckLake and SQL**. This is how you get started with DuckLake on your own machine — really useful for demoing personal projects and even production use cases. Check it out, and have a great rest of your day.
