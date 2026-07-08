# Indexes Are Not All You Need — Common DuckDB Pitfalls

**Speaker:** Tanya (DuckDB Labs) — 2025-09-24, conference talk (DuckCon-style).
**Topic:** When tree-based indexes help vs. hurt in DuckDB, and how to profile slow queries.

*Talk transcript. Cleaned from an auto-generated transcript ("duct DB"/"DTDB"/"DDB" → DuckDB; wording lightly smoothed, meaning preserved.)*

---

Hi everyone, I'm Tanya. I'm going to talk about "**Indexes are not all you need**" — exploring ways to look into DuckDB pitfalls, find them, and see what to do from there.

A bit of background on me: I studied computer science in Germany, moved to Amsterdam for an internship at **CWI**, met some amazing database folks there, and eventually after my master's came back to work at **DuckDB Labs** (by then a company).

## DuckDB pitfalls and how to find them

That's a big promise in the title — am I going to give you the magic formula for why your queries are slow and how to fix it? Sadly no, but I'll try to give starting points for looking into slow queries and what DuckDB offers to help find the cause.

By default, DuckDB tries to choose sensible **preconfigured defaults** (check the docs), so it's hopefully not that easy to get into a slow query. The flip side: once you *have* a slow query, making it fast can be tricky, because DuckDB **isn't designed to be super tunable** like some other systems that match exactly your use case.

To analyze query performance, we'll use **indexes** as a case study — something I've worked a lot on, and one of those things you can either really gain from or lose a lot of performance from. We'll look at profiling and at a function to monitor database memory.

## A step back on indexes

We're talking specifically about **tree-based indexes** (e.g. B-trees) — not zone maps / min-max statistics.

- **Traditional (transactional) systems:** indexes are inherent. Think of a web shop — a series of point lookups, updates on a single tuple, all very "pointy." Indexes are great because you can quickly jump to that place in your data. And you have a **fixed set of queries**, so you know what needs to be fast and can fine-tune. (Oracle basically built a business model around consulting on how to make the system fast for your workload.)
- **Analytical workloads:** initially counterintuitive. No longer point lookups — you want fast **big table scans** and complex aggregations, so the focus is efficient scan performance, and indexes fell short. Many analytical systems said "we don't need that," and some still don't support tree-based indexes. And with ad-hoc "click-together" queries (as in the previous talk), you may not even know what you'd be indexing.

**So where do indexes still matter?** Even analytical systems aren't standalone — data often comes from another system, and **integrity constraints** may still have to hold (guarantees on your data). And beyond scans, you see **filters** everywhere ("give me exactly value 500, then join it to something"). For filters:

- **Zone maps / min-max indexes** help — but only if the data is **pre-clustered**, so you can skip sections. Without clustering / correlation to sorted attributes, the zone map won't help.
- **Data size matters** — once you scan the whole data set to find a few tuples, even well-tuned analytical scans get expensive. That's a case where an index is still useful.
- **Versatility:** in production / pipeline use, you can't assume only heavy analytical queries. You may query metadata, or run non-analytical background chores regularly. For DuckDB to be a versatile tool, good index support matters.

## Indexes in DuckDB

It took time from the ~2018 state to now, but DuckDB now has **primary keys, foreign keys, unique constraints**, and explicit indexes (via `CREATE INDEX`). In all these cases, DuckDB maintains a **secondary copy** of your data in the background.

## Profiling walkthrough

**Setup table:** a simple table of integers, ~100 million rows, with pseudo-random-but-deterministic values (so we know how many duplicates to expect — controlled with a modulo). The data is **unclustered**, which keeps scans from cheating. The index in all examples is a non-unique `CREATE INDEX` on the single `id` column.

**Enable profiling.** Most know `EXPLAIN ANALYZE` (query plan output, JSON or picture). DuckDB also has a **profiler** (actively being expanded) giving more metrics and outputting them for some clients directly on the connection — so you can forward them to an observability tool, track average query latency, peak buffer memory, or per-operator timings (scan vs. index probe). For the talk: `PRAGMA profiling coverage = 'all'` (profile every query, not just `SELECT`) with JSON output. (Maya is also here, working on cool metrics like how much you read from a file.)

### Index maintenance cost

Bulk-append 10k rows (10k integers), comparing **no index** vs. **three indexes** (same index created three times):

- **No index:** ~7 ms
- **Three indexes:** ~400 ms

Why? DuckDB can bulk-append to a table by writing to the end. But for **each value**, every index must traverse the tree to find where to insert it — and that's **not parallelized** yet. A lot of work for three indexes.

### Index memory

Maybe you say: ingestion happens at night, during the day I only care about `SELECT`. Then there's **index memory** — how much memory the indexes use once running. Use `pragma_database_size` / the `duckdb_memory()` function, which gives memory per tag. Compare the `IN_MEMORY_TABLE` tag (table memory) with the `ART_INDEX` tag (index memory), divided by 10⁹ for gigabytes:

- **Index memory is ~2× the table memory** — significant. The index is a secondary structure: it copies the entire column, its row IDs, plus index metadata (tree nodes).

Also, memory is **tracked but not yet buffer-managed with eviction** — if you lazily load an index, once used a lot, all that memory stays active (hopefully fixed in upcoming releases). Even *with* eviction, under high concurrency with many index parts active for index scans, indexes create **additional memory pressure** on top of your queries — and for a long-running query, that extra memory can't be used for the query, potentially hurting performance. So even with eviction, use indexes **case by case**.

### When to still use an index

- You **can't not** use it — integrity constraints.
- **Highly selective filters** — your filter returns ~4 rows out of 100 million, and it's a repeated query. Then add an index for that specific case.
- Two configurable settings: if the system slows once the index scans too much, **decrease the max scan count** ("use an index, but never scan more than 5 values"). Now you're a bit in the land of tuning DuckDB, but some use cases end up there. The default: try to scan a data chunk; if there's more, stop (getting too expensive).

### Point query

Scan all rows with a given `id` and count them (in your case you might join to another table). Using `EXPLAIN ANALYZE` (more readable than the profiler output here):

- **First check:** am I actually using the index scan? You may have added an index but DuckDB chose not to use it — the plan may show a **sequential scan**, which could explain why a query you thought would be fast wasn't. When it works, you see an **index scan** node.
- **How much faster:** for a few chunks (~4,000 values), index probing is ~1 ms max. But once you fetch **a lot of chunks**, DuckDB does extra work: you're no longer scanning sequentially — you scan the index (which gives row IDs), then **jump around in the data** to find the matching rows. That's a lot of **random access** instead of one sequential pass.
- If you use **no index**, scanning always gives the **same, predictable** performance — you go over the entire table regardless of how many values you fetch. Even at 40,000 values you're still fetching a very small percentage of the table, so **high selectivity is important**.

## Final thoughts

- **There's no rule of thumb** with DuckDB — it tries to choose the best defaults anyway, so there's no obvious "do X, Y, Z to make it fast." If it's not fast, you have to **profile and benchmark** to find which operator (or, in concurrent scenarios, which combination of queries) is slow.
- **Good observability** for bigger production systems is very useful — see "peak traffic → these query times go up." The profiler aims to make profiling relevant metrics more feasible, and you can use `duckdb_memory()` to regularly ask "how's my memory doing, what's allocated?"

Check out the performance guide and the DuckDB configuration page. That was my talk — thanks and cheers.
