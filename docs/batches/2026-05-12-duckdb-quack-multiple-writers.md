# DuckDB Just Changed the Game: Meet Quack, the Protocol That Unlocks Multiple Writers

**Siddique Ahmad** · 6 min read · May 12, 2026

> The duck that always swam alone can now swim in flocks.

*Photo by Glen Carrie on Unsplash*

For years, the single biggest complaint about DuckDB has been the same one-liner: "It only supports one writer at a time."

Data engineers would discover DuckDB, fall in love with its blazing columnar SQL, and then hit this wall the moment they needed more than one process writing to the same database. The workarounds multiplied — custom RPC wrappers, Arrow Flight SQL shims, routing writes through PostgreSQL via pg_duckdb, or just giving up and switching to a heavier system.

On May 12, 2026, DuckDB's team answered definitively. They introduced **Quack** — a native client-server protocol that finally lets multiple DuckDB instances talk to each other, and more importantly, write to each other concurrently.

This is not a minor release note. This is an architectural shift.

## Why Multiple Writers Were Hard (And Why DuckDB Waited)

DuckDB was designed from the ground up as an in-process analytical engine. No separate server process, no network roundtrip, just a library you embed directly into your Python, R, Java, or Go application. This gave it remarkable speed and simplicity — query a Parquet file on S3 in two lines of Python, no daemon to start, no connection string gymnastics.

The downside was fundamental: DuckDB keeps a significant amount of state in RAM. If two separate processes tried to write simultaneously, they'd each have their own in-memory state, and reconciling those would require distributed coordination — exactly the complexity DuckDB was built to avoid.

So for years, DuckDB enforced a strict rule: one writer process, many reader processes. Within a single process, you could have multiple writer threads using MVCC (Multi-Version Concurrency Control), and appends would never conflict. But cross-process writes? Not supported.

The community responded by building everything from simple RPC wrappers to MotherDuck's entire cloud platform to paper over this gap. When the DuckDB team saw that ecosystem of workarounds, they took it as a signal. The people had spoken.

## Enter Quack: DuckDB Talks to DuckDB

> "What do two (or more) ducks do if they want to talk to each other? They quack!" — The DuckDB Team

The Quack protocol is elegantly simple in concept: one DuckDB instance acts as a server, another acts as a client. Both are full DuckDB instances. The server holds the mutable state, serializes all writes, and answers queries. The client attaches to it remotely and treats it like a local attached database.

Here's the minimal setup:

### Server Side (DuckDB #1)

```sql
INSTALL quack FROM core_nightly;
LOAD quack;

CALL quack_serve(
  'quack:localhost',
  token = 'super_secret'
);

CREATE TABLE telemetry AS
FROM VALUES ('event_1', NOW()) v(name, ts);
```

### Client Side (DuckDB #2 — or #3, #4, #N…)

```sql
INSTALL quack FROM core_nightly;
LOAD quack;

CREATE SECRET (
  TYPE quack,
  TOKEN 'super_secret'
);

ATTACH 'quack:localhost' AS remote;

-- Write from this process
INSERT INTO remote.telemetry VALUES ('event_2', NOW());

-- Query it
FROM remote.telemetry;
```

For complex queries you want executed server-side:

```sql
FROM remote.query(
  'SELECT name, COUNT(*) as cnt FROM telemetry GROUP BY 1'
);
```

That last one is important for large datasets — push the heavy computation to the server, pull back only the result.

## The Protocol Itself: HTTP Done Right

The DuckDB team had the rare luxury of designing a database protocol from scratch in 2026, with no legacy constraints. Their choice? Build Quack directly on top of HTTP.

This is a pragmatic and brilliant decision:

- Every load balancer, firewall, and auth proxy already knows HTTP. Zero new infrastructure skills required.
- DuckDB-Wasm can speak Quack natively — meaning a DuckDB instance running inside a browser can directly connect to a DuckDB server on an EC2 instance.
- HTTP/2 and HTTP/3 optimizations apply automatically, including parallel fetch of large result sets from multiple threads.

The interaction model is classic request-response driven by the client: connection requests with token auth, query execution requests, and chunked result fetch messages for large payloads.

## New Use Cases This Unlocks

This is the part data engineers should pay close attention to. Quack doesn't just fix an old limitation — it opens up patterns that weren't previously possible with DuckDB at all.

### 1. Multi-Process Telemetry Ingestion with Live Dashboards

The canonical example from the DuckDB team themselves: dozens of processes collecting telemetry while a dashboard queries the same tables in real time.

Previously you'd need Kafka → some OLAP store → BI tool. Now:

```
[Worker 1] → INSERT INTO quack_server.events
[Worker 2] → INSERT INTO quack_server.events
[Worker 3] → INSERT INTO quack_server.events
[Dashboard] → SELECT * FROM quack_server.events WHERE ts > NOW() - INTERVAL '5 min'
```

All against the same DuckDB server. No intermediate queue needed for moderate-volume workloads.

### 2. Parallel ETL Writers, Single Analytical Store

Classic ETL scenario: you have 8 parallel Python workers each processing a partition of your data. Previously you'd have each worker write to a separate DuckDB file, then merge them later. Now they all write to the same server concurrently:

```python
# In each parallel worker
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL quack FROM core_nightly; LOAD quack;")
conn.execute("CREATE SECRET (TYPE quack, TOKEN 'my_token');")
conn.execute("ATTACH 'quack:etl-server:5432' AS warehouse;")

# Each worker writes its partition
conn.execute(f"""
    INSERT INTO warehouse.processed_orders
    SELECT * FROM read_parquet('s3://bucket/partition_{worker_id}/*.parquet')
""")
```

No merge step. No temporary files. Writers contend at the server, server serializes commits.

### 3. Microservices Sharing an Analytical Backend

If you're building a lightweight SaaS or internal tool with multiple services, Quack lets each service maintain its own DuckDB for local queries while also reading/writing a shared analytical layer:

```
[Auth Service]   → local DuckDB + reads from quack_server.user_events
[Billing Service] → local DuckDB + writes to quack_server.transactions
[Analytics API]  → local DuckDB + reads quack_server.* for dashboards
```

The "EleDucken" pattern (DuckDB inside Postgres) was the hack for this before. Quack is the native solution.

### 4. Browser-to-Server Analytics (The Wasm Angle)

This one is genuinely new territory. Because DuckDB-Wasm speaks Quack natively, you can build analytics applications where:

- The browser runs DuckDB-Wasm for local query acceleration
- It connects directly via Quack to a server-side DuckDB for shared/persistent data
- No REST API layer needed between the frontend and the data

This is an interesting architecture for internal tools, notebooks, or collaborative data apps.

### 5. Edge-to-Central Data Collection

For IoT or distributed data collection scenarios:

```
[Edge Node A]    → DuckDB in-process → periodically flushes to quack:central-server
[Edge Node B]    → DuckDB in-process → periodically flushes to quack:central-server
[Central Server] → DuckDB serving Quack → runs scheduled rollup queries
```

DuckDB's small footprint makes it viable on edge hardware. Quack completes the picture with a native sync mechanism.

## What This Means for Your Stack

Let's be direct about the trade-offs.

**Latency:** Because Quack moves data over the network (even if it's just localhost), you lose the "zero-latency" advantage of the purely in-process model. For massive bulk loads, the blog's advice to use `remote.query()` is critical — it tells the server to do the work rather than pulling all the raw data to the client first.

Quack is not a replacement for PostgreSQL in transactional workloads. If you have high-frequency OLTP with many small transactions, DuckDB remains optimized for bulk analytical operations. The DuckDB team acknowledges this openly.

Quack is also not the same as DuckLake + PostgreSQL catalog — that's DuckDB's answer for multi-writer access to a lakehouse format with conflict arbitration via Unity Catalog or PostgreSQL. Quack is for direct client-server access to a single DuckDB server instance.

The sweet spot is analytical workloads that previously required either: (a) a heavier OLAP database, or (b) a complex pipeline with merge steps. If your write frequency is moderate-to-high but not microsecond-level transactional, and your queries are analytical, Quack lands squarely in that gap.

For data engineers building multi-entity reporting platforms, ERP analytics, or telemetry systems on lean infrastructure, this is particularly relevant — DuckDB's low operational overhead combined with Quack's multi-writer capability is a compelling alternative to standing up ClickHouse or Redshift for mid-scale workloads.

## Getting Started Today

Quack ships in DuckDB v1.5.2 and lives in the `core_nightly` extension repository:

```sql
-- On both client and server
INSTALL quack FROM core_nightly;
LOAD quack;
```

Full documentation is at: `duckdb.org/docs/current/quack/overview`

## The Bigger Picture

DuckDB started as a research project that wanted to be the "SQLite of analytics." That framing still holds, but Quack signals a maturation: DuckDB is now willing to be a server when the use case demands it, without abandoning its in-process roots.

The in-process model remains the default. Quack is an opt-in extension you add when you genuinely need multi-process writes. That's the right engineering philosophy — simple by default, extensible when needed.

The duck grew up. It can swim in a flock now.

---

*Tags: DuckDB, Data Engineering, SQL, Data Analytics, In-Process*
