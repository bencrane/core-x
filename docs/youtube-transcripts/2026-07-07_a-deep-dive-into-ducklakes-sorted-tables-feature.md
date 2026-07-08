# A Deep Dive into DuckLake's Sorted Tables Feature

**Format:** YouTube video (sponsored by MotherDuck) — 2026-07-07.
**Topic:** How DuckLake sorted tables speed up high-cardinality queries via Parquet min/max pruning, and how sort interacts with inlining, flushing, and compaction.

*Video transcript. Cleaned from an auto-generated transcript ("Ducklake"/"Douglake" → DuckLake, "paret" → Parquet; wording lightly smoothed, meaning preserved.)*

---

So you've built a lakehouse and you're sending a lot of data to it. Your queries run fast — until they don't. Suddenly they crawl: you're scanning millions of rows looking for a single user ID, and your query engine has to open every Parquet file to do it. Today I'll show how **DuckLake sorted tables** fix this with one line of config, and why the **order you do things** actually matters.

## Why sorting matters for high-cardinality columns

Picture unsorted data (left) vs. sorted data (right). A lot of event data works like the left: many users clicking at different times, and event systems are usually first-in-first-out, so data arrives ordered by **timestamp**. That's easy to query at small scale. But push millions of rows daily and you get a lot of unsorted data in your Parquets.

That's a real problem if you need a specific **user ID** and the sequence of that user's events in a time range. In lakehouses, data is stored in Parquet files, and **Parquet files have min/max statistics in their footers** — that's what a lakehouse uses to decide which files it needs. If a user is active one day and again a week later, their data could land in one, two, three, four — potentially *all* — of your Parquets. That's a big inefficiency: you want to **reduce the number of Parquet files scanned**.

Better: fewer files where data that should be close together *is*, so you can skip other files. Sorted Parquet data on the right has user IDs 100–299 in the first file and 300–499 in the second — versus unsorted, where all user IDs appear in both files.

**Row groups and predicate pushdown.** Parquet files also have **row groups**. When a query comes through, the planner looks at all files; if the Parquets are sorted it reads only the one it needs and skips the rest (an optimization right at the start). Then, within the file it opens, the sorted data is also sorted **in row groups** — so it can skip row group 1 (which has some other user) and keep just row group 0 (which has the user we want). Being able to skip what you don't need helps enormously on large tables. Without sorted data, it would read through potentially thousands of Parquets and all their row groups; with it, maybe one file and one row group. A very useful lakehouse optimization.

## DuckLake's approach — "click it and forget it"

DuckLake is meant to be click-it-and-forget-it: you don't have to keep re-sorting down the road. Create a table, data starts streaming in, traffic spikes, volume goes crazy — the queries that were fine with small data keep their performance, and you're not getting a 2 a.m. text that your pipeline crashed overnight.

*(Substack with all the walkthrough code linked in the description.)*

## Unsorted table walkthrough

Start a DuckDB instance, `LOAD ducklake`, and attach a catalog:

```sql
ATTACH 'ducklake:sorted.db' AS my_ducklake (DATA_PATH 'data');
USE my_ducklake;   -- alias so you don't write my_ducklake every time
CREATE TABLE events (user_id VARCHAR, event_name VARCHAR, ts TIMESTAMP);
```

Created a table — **unsorted up front; that's how it works when you create a table**. Insert data: user IDs, event names, and timestamps. The timestamps are in order (events come in from different users but are stored in timestamp sequence — created first, into the lakehouse first — not grouped by user ID).

Look at the data folder: **no Parquet files yet.** Why? The number of rows inserted is **below DuckLake's inlining threshold**. DuckLake has a default threshold of **10 rows per insert**: at or below 10, data stays **inlined** in the metadata catalog (the DuckDB database); above 10, it's written to a Parquet. We inserted exactly 10, so it's inlined — no Parquet yet.

## Adding a sort order (not retroactive)

```sql
ALTER TABLE events SET SORTED BY (user_id, ts ASC);
```

`SET SORTED BY` is DuckLake-specific — reorder first by `user_id`, then `ts` ascending. Now insert five more rows (also unordered as inserted). Peek at the `events` table: the **first 10 rows are unordered**, but the **next five are ordered** by ascending user ID, and within a user by ascending timestamp (e.g. user 5618 goes 13:41:29 → 13:41:31).

What's happening? **Sort order is not retroactive.** If you dump unsorted data in and then add a sort order, it only affects data *after* that. This is a big reason to think carefully up front about what you want the table to achieve.

> **Sponsor — MotherDuck.** A data warehouse built on DuckDB ("infrastructure for answers"). It answers DuckDB's single-node question with **hybrid tenancy** (multiple users on DuckDB in a warehouse setting), has its own **MCP server**, and serves software engineers, data scientists/engineers, content creators — data warehousing *and* customer-facing analytics. Tiers scale from Pulse → Standard → Jumbo → Mega → Giga. The MCP server connects directly to an LLM and emits **"dives"** — analytical interfaces you host inside MotherDuck. Worth checking out if you're deploying a DuckLake to production.

**Resetting a sort:** `ALTER TABLE events SET SORTED BY ()` clears it — nice when you set the wrong order and want it back.

## Sorted table from the start

Drop and recreate `events`, then set the sort order **before** inserting. Setting a sort order affects three things: **inserts**, **flushing to Parquet**, and **compaction**. Insert the same data — now reading the table, the data is **sorted immediately at insert**: user IDs ascending, relocating each user's rows together, timestamps ascending within a user. Clean data at insert, so you don't have to think about it in your query — no `ORDER BY` in your initial CTE, no view/materialized table workaround for unsorted data. One line, one config, and every insert from now on is done.

## Inlining, flushing, and inspecting metadata

Since inserts of ≤10 rows don't create a Parquet, we haven't yet seen the file-skipping optimization — all data is **inlined**. Confirm with `ducklake_list_files` → **zero files** (files = Parquet files).

View the sort metadata: `SELECT * FROM metadata.ducklake_sort_expression` (with the `metadata` prefix) shows the sort order — and the **history** of sort orders (there was a sort ID on the first table, and a second one). Great for observability: did someone change or reset something? The table has a table ID (not name), but you can join `ducklake_table` to get the `events` table name. Using metadata you can see exactly who did what on which tables — one of my favorite parts of DuckLake.

**Flushing** means writing the inlined rows (saved in the metadata catalog) to a Parquet file: the inlined rows are removed from the catalog and written out, and **if a sort order is set, the flush sorts the data too**. Call `ducklake_flush_inlined_data` → "10 rows flushed from the events table" — and a **Parquet file finally appears**. Reading `events` shows nothing changed (data now comes from the Parquet instead of inline). `ducklake_list_files` now shows **one file** (before: 0).

**Parquet statistics** — you can view these with DuckDB (not DuckLake-specific, but convenient here): the file we created has user-ID min/max of **101–4731** (all the data we had) in a single row group (row group 0). If a query needed a user above 4731 not in this file, the planner skips this file and looks at the next, using the statistics.

## Sort options: turn off sort-on-insert

You have options. **Turn off sorting on insert** — there's overhead to sorting during insert. It makes sense to sort at a **batch** level (a lot of data at once), but for a **single event**, it may be better to insert unsorted and let it get sorted later at flush/compaction:

```sql
CALL my_ducklake.set_option('...');   -- turn off sort on insert
```

Now: flush orders, compaction orders, but a plain insert does **not** order. Insert five more rows — the first 10 (already in a Parquet) stay ordered; the new five are unordered (user 3892 then 10001). Still only one file — DuckLake reads from **inline data and Parquet at the same time**, in the background, blazing fast.

**Flush again** — the five unordered inline rows get sorted on flush and written to a **second Parquet**. Reading the table: the first insert (10 rows) is ordered, and the second insert is now ordered by user ID then timestamp. But note: the **whole table wasn't sorted together** — it's sorted **per Parquet**. Statistics show two files: first has 101–4731, second has 101–5618. Querying just user 5618, the engine skips the first Parquet (5618 not in it) and reads the second, row group 0. A big real-time optimization.

## Compaction

Ordered Parquets exist, but the table isn't globally ordered. **Compaction** condenses multiple Parquets into fewer — here, two into one — via the `checkpoint` statement. Run it → a new Parquet is created, and reading `events` shows **everything ordered together, 101–5618, in one table**.

`ducklake_list_files` now shows **only one** file. DuckLake walks the snapshots in its metadata to decide which files matter — the other two are now **orphaned** old files (still on disk, not used). Clean them manually with `ducklake_cleanup_old_files` at the DuckLake level → "two files were removed," leaving one file. Statistics on the compacted file: one row group, users 101–5618, all data in one sorted file.

## Takeaway

At scale this sets you up for success: high-cardinality queries won't crawl or hit memory limits and error out — saving time, money, and sleep. DuckLake's sorted tables offer a clear way to understand it, and the metadata tables give you observability. Lakehouses are a wonderful architecture, and DuckLake does a nice job. Thanks for watching.
