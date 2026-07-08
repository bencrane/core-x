# Using DuckDB Quack as the DuckLake catalog

**Source:** Mike Ritchie — May 17, 2026 (updated June 8, 2026) · 12 min read — <https://www.definite.app/blog/duckdb-quack-ducklake-catalog>

A DuckLake lakehouse has two moving parts: Parquet files in object storage, and a catalog database that records what those files mean. The data layer is the easy part. The catalog is where the interesting decisions live, and for most of DuckLake's short life the default answer has been "point it at Postgres."

We think that default is wrong, or at least about to be. Definite runs DuckLake in production and in our new on-prem product, and we have spent the last few weeks running the catalog on DuckDB itself, served over the new Quack protocol. This post is about what broke, what got faster, and why a DuckDB catalog turns out to be the natural choice instead of the clever one.

First, some vocabulary, because three things in this post have confusingly similar names:

- **DuckDB** is the engine: the in-process analytical database.
- **DuckLake** is the lakehouse format: Parquet files for data, plus a SQL catalog database for metadata (schemas, snapshots, file lists, statistics). DuckLake does not care what that catalog database is, as long as it speaks SQL, supports ACID transactions, and has primary keys.
- **Quack** is a protocol, shipped as a DuckDB extension, that turns a DuckDB instance into a server other DuckDB instances connect to over HTTP. It was released by the DuckDB team on May 12, 2026.

The thesis: use DuckDB as the DuckLake catalog, and use Quack to make that catalog reachable and safe for concurrent writers. It is DuckDB end to end, no Postgres in the path.

## The problem with Postgres as a catalog

DuckLake's catalog can be any ACID SQL database. In practice the early DuckLake deployments, ours included, reached for Postgres. It is the obvious choice: it is a real database, it handles concurrent connections, everyone has one. But running it in anger surfaced three problems that are not really Postgres's fault. They are the friction of bolting a row-store transactional database onto a DuckDB-native lakehouse.

### 1. Type conflicts at the seam

DuckLake's catalog stores real data, not just pointers. Schemas, column definitions, file-level statistics (min/max per column, used to skip Parquet files at query time), and, as we will get to, inlined row data. All of that originates in DuckDB and has DuckDB types.

DuckDB has types Postgres does not. `UBIGINT`, `HUGEINT`, `UTINYINT`, the unsigned integer family, and DuckDB's nested types (`STRUCT`, `MAP`, `LIST`) and `VARIANT`. When the catalog is Postgres, DuckLake has to encode those types into something Postgres can hold. The DuckLake docs are explicit about this: with a Postgres catalog, `UBIGINT` is mapped to `VARCHAR` and stored as text; nested types are stored as strings and converted back on read; and `VARIANT` column inlining is not supported on Postgres at all, because the type information does not survive the round trip through a string representation.

None of this is catastrophic, but it is a class of bug that exists only because there is an impedance mismatch in the middle of the system. Every encode and decode is a place a value can be misread, a statistic can be wrong, a file can be wrongly skipped. When the catalog is DuckDB, the mapping is the identity function. A `UBIGINT` is a `UBIGINT`. A `STRUCT` is a `STRUCT`. The whole category of "DuckDB type does not fit in the catalog" disappears, because the catalog is DuckDB.

### 2. The connection ceiling

A DuckLake catalog is small by design. It holds metadata, not bulk data. So the natural instinct is to provision a small Postgres for it, which is exactly what we did.

A small Postgres has a small `max_connections`. And every process that touches the lakehouse — every query worker, every ingestion job, every dashboard refresh — holds a stateful Postgres connection to the catalog for as long as it is attached. Those connections fan out fast. We hit catalog connection exhaustion in production: enough concurrent workers, each holding a catalog connection, to saturate the pooler and start refusing new ones. The catalog is a few megabytes of metadata, and it fell over on connection count, not load.

You can fix this with a bigger Postgres, or a connection pooler, or careful pool tuning in every client. But notice what you are doing: operating a stateful database tier, and capacity-planning its connection count, to hold metadata for a lakehouse whose entire pitch is that storage and compute are separate and cheap.

### 3. The catalog that was supposed to stay small

The other half of the connection problem is size. "The catalog stays small" is true right up until you turn on data inlining, and inlining is one of the best features DuckLake has.

Here is how inlining works, because it matters for the rest of this post. Normally a DuckLake write does two things: write a Parquet file to object storage, then write a row to the catalog pointing at that file. For a large append that is great. For a small one, a five-row `INSERT`, it is wasteful: you get a tiny Parquet file, an object-store round trip, and metadata overhead, all to store five rows.

Data inlining skips the Parquet step. Instead of writing a file and a pointer, DuckLake writes the rows directly into a table inside the catalog database. The default is on: `ducklake_default_data_inlining_row_limit` ships at 10 rows, and you can raise it per connection (`DATA_INLINING_ROW_LIMIT` on `ATTACH`) or persist it per table or schema. Later, `ducklake_flush_inlined_data()` rolls the accumulated inlined rows up into proper Parquet files. The payoff is real: small, frequent writes (the streaming-ingestion, telemetry, CDC pattern) stop generating object-store garbage and stop paying object-store latency. They commit at the speed of a local insert.

But look at where those rows live. They live in the catalog. Turn the inlining limit up, run a high-frequency write workload, and the catalog that was "just metadata" is now also holding your most recent data. In our own DuckLake clone of a customer's lake, a single Shopify orders table showed an 835,000-row gap between the snapshot row count and the Parquet-backed row count. That gap was inlined data: over three thousand `ducklake_inlined_data_*` tables of rows that had not been flushed yet.

So the small Postgres has to be not so small. And now you are running, monitoring, backing up, and connection-capping a stateful Postgres tier that holds real data, for a lakehouse architecture whose whole premise is that you should not have to.

To summarize: Postgres as a DuckLake catalog works, but it forces a type-translation layer you do not want, and it reintroduces a stateful, capacity-planned database tier exactly where DuckLake was trying to remove one.

## Why DuckDB is the catalog you actually want

If the catalog can be any ACID SQL database, and the data and the engine are both DuckDB, the obvious move is to make the catalog DuckDB too. DuckLake supports this directly: the FAQ calls out using "DuckDB for both your DuckLake entry point and your catalog database."

The upside is everything the Postgres section was complaining about, inverted:

- **No type translation.** DuckDB types store as themselves. Inlined nested types and `VARIANT` work without the string-encoding caveats. The seam is gone because there is no seam.
- **It is fast.** The catalog is now a DuckDB file, read and written by the same engine running the queries. Catalog lookups, snapshot reads, statistics scans: all local DuckDB operations on a small file, no network database protocol in the loop.
- **Inlining gets better, not scarier.** This is the part the DuckDB team specifically calls out: a DuckDB catalog is expected to "greatly improve performance, especially with inlining." Inlined rows live in a DuckDB table read by a DuckDB engine. The most recent data in your lake is now sitting in the fastest possible place for DuckDB to query it.

There is exactly one problem, and it is a big one.

DuckDB has no multi-writer story. It is an in-process database. One process holds the database file, keeps a lot of state in memory, and a second process cannot just open the same file and start writing: there is no mechanism to synchronize the in-memory state between them. The DuckDB team is blunt about this in the Quack announcement: an in-process system works "less well" for modifying the same database from multiple processes at once, and there are "very good technical reasons" for that, mostly the in-memory state.

A DuckLake catalog with one writer is not a lakehouse. It is a single-process database with extra steps. Every ingestion job, every transformation, every interactive write needs to commit to the catalog, and they cannot all be the one process that owns the file.

This is the real reason Postgres became the default DuckLake catalog. Not because Postgres is a good fit, but because Postgres answers the one question DuckDB could not: how do many writers safely share one catalog?

## Quack: the missing multi-writer layer

That is exactly the gap Quack closes.

Quack turns a DuckDB instance into a server. One DuckDB process loads the `quack` extension, calls `quack_serve(...)`, and now other DuckDB instances connect to it over HTTP and run SQL against its databases as if they were local. Attach a remote catalog with `ATTACH 'quack:host:9494' AS remote`, and remote tables read and write like local ones. Transactions are forwarded over the wire.

The important move: there is still only one process that owns the catalog file (the Quack server), but every other process is now a client of that process instead of a competing owner of the file. The single-writer constraint becomes a single-server design, which is a completely different and much more workable thing. The server serializes writes through DuckDB's normal MVCC and write-ahead log, the same concurrency machinery DuckDB already uses; clients just issue SQL and get answers.

Three properties make this a real solution and not a workaround:

**It is HTTP.** Quack rides on plain HTTP (HTTPS behind a reverse proxy). No bespoke wire transport to operate. Load balancers, firewalls, TLS termination, and auth all work the way they already work for every other HTTP service you run. Connections are HTTP keep-alive, not stateful database sessions, which directly defuses the connection-ceiling problem from the Postgres catalog: you are not fanning out a fixed pool of precious Postgres connections, you are making HTTP requests.

**It is fast.** Quack uses DuckDB's own serialization format (`application/duckdb`, the same primitives behind the WAL), not an interchange format, so complex types cross the wire losslessly with no translation. A query is a single round trip after the handshake. In the DuckDB team's own benchmarks, Quack moved 60 million rows in under 5 seconds, faster than Arrow Flight SQL and far faster than the Postgres wire protocol, and it beat Postgres on small-write throughput up to 8 threads (around 5,500 transactions per second).

**It is the DuckDB team's own plan for DuckLake.** This is not us bending Quack into a shape it was not meant for. The Quack announcement says it directly: "we are going to integrate Quack into DuckLake, so that it becomes possible to use a remote DuckDB server as a DuckLake catalog." A DuckDB-served DuckLake catalog is on the DuckDB roadmap. We are early to it, not off-road.

One thing Quack does not solve, and we should say so plainly: it is still one writer process. The Quack server serializes everything; there is no horizontal write scaling and the server is a single point of failure until the DuckDB team's planned replication protocol ships. For a single-tenant lakehouse this is a fine trade. For a large multi-tenant fleet it is a real constraint to design around.

## What this looks like in practice

Net architecture, with all three pieces in place:

- **Data plane:** Parquet files in object storage (S3, GCS, Azure Blob, MinIO). Unchanged. DuckLake already did this well.
- **Catalog plane:** a DuckDB database file, owned by one Quack server process, on a local or attached disk.
- **Everything else:** query workers, ingestion jobs, transformation steps, all become Quack clients. They `ATTACH` the catalog over HTTP. They never open the catalog file directly and never need a Postgres connection.

A useful side effect: the catalog server can hold the object-store credentials, so clients do not have to. A client connects to Quack with an auth token, issues SQL, and the server reads and writes object storage on its behalf. Credentials live in one place instead of being distributed to every job.

And the catalog becomes, in effect, a serverless database. Storage and compute separate cleanly: the catalog data is a file on a disk, the catalog compute is the Quack process, and the clients are stateless HTTP callers. The catalog is no longer a database tier you provision, size, and connection-cap. It is a file plus a process.

## Our spike numbers

We validated this end to end. The setup: a Quack server on Kubernetes, the DuckLake catalog as a DuckDB file on an attached SSD, Parquet data on object storage.

**Concurrent writers.** Eight parallel writer processes, each a Quack client, inserting into a shared DuckLake table (disjoint key ranges), 250,000 rows each. Two million rows total, zero errors. Aggregate write rate was about 222,000 rows/sec, higher than the single-writer baseline of about 192,000 rows/sec, not lower. The "5,500 TPS at 8 threads" ceiling from the Quack benchmarks applies to tiny single-row transactions; for DuckLake-style commits, where each commit is a real Parquet write, the serialization point did not bind.

- One gotcha worth repeating: `CREATE TABLE IF NOT EXISTS` raced across writers. `IF NOT EXISTS` did not short-circuit; the DuckLake transaction layer caught the concurrent creation and aborted all but one writer. Pre-create the table, then start the parallel writers.

**A warm catalog server.** We cloned a real customer DuckLake catalog into the Quack-served setup and ran the same queries against it and against our existing production stack (per-request DuckDB behind an API, catalog on Postgres). The Quack-served version was consistently faster, and the reason matters more than any single number: the Quack server holds the catalog attached and warm in one long-lived process, while the per-request stack provisions a fresh DuckDB on every call. Most of the gain is warm-versus-cold, not Quack the protocol. That is itself an argument for the model: a persistent catalog server beats cold-start-per-request.

These are spike results, single-tenant, with auth and isolation stripped down. They are a ceiling, not a production SLA. But the direction is unambiguous.

## How Definite deploys it

Definite's on-prem product is a single-tenant, self-hosted distribution of our analytics stack: lakehouse, query engine, semantic layer, automations, an AI agent, and a web UI, deployed as one Helm chart onto the customer's own Kubernetes, object store, and database.

The lakehouse component is DuckDB plus Quack plus DuckLake. Parquet data on the customer's object store; the DuckLake catalog as a DuckDB file on a persistent volume; Quack as the server in front of it. The API, the job runner, and the query path all reach the lakehouse as Quack clients. Internally we call that server component "the lakehouse pod," and Quack is what lets a single stateful pod serve every other component without each one needing object-store credentials or a direct handle on the catalog file.

We chose this over a shared Postgres catalog deliberately. It means one fewer stateful service for the customer to run, and it removes the catalog connection ceiling as a thing anyone has to think about. In the on-prem context, where every operational dependency is something the customer's own team has to babysit, "the catalog is a file and a process, not a database to operate" is a real simplification.

We are clear-eyed about the current limits. The lakehouse pod is single-replica today: one Quack writer, no high-availability story yet. That is fine for a single-tenant deployment and squarely on our roadmap to harden. The longer-term direction, both for our cloud and on-prem, is the same one the DuckDB team is heading toward: a DuckDB-served catalog as the default, with Quack's planned replication protocol providing the HA story when it lands.

## The takeaway

Postgres as a DuckLake catalog was never the right design. It was the only design, because DuckDB could not do multi-writer and the catalog needs many writers. That forced a stateful database tier and a type-translation seam into a lakehouse built to avoid exactly those things.

Quack removes the constraint. With a DuckDB-served catalog you get DuckDB end to end, no type conflicts, inlining without the string-encoding caveats, HTTP instead of a stateful connection pool, and a catalog that behaves like a serverless database: a file plus a process, not a tier to operate.

It is early. Quack is a beta extension, the DuckLake integration is still landing, and there is no built-in HA yet. But the destination is clear, and the DuckDB team has said as much. We are building Definite's on-prem lakehouse on it now because we would rather be early to the right architecture than comfortable on the wrong one.

## Sources

- DuckDB team, "Quack: The DuckDB Client-Server Protocol" (DuckDB blog, May 12, 2026): <https://duckdb.org/2026/05/12/quack-remote-protocol.html>
- DuckDB docs, "Quack Remote Protocol" (overview, reference): <https://duckdb.org/docs/current/core_extensions/quack>
- DuckLake docs, "Data Inlining": <https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining>
- DuckLake docs, data types / catalog database support: <https://ducklake.select/docs/stable/specification/data_types>
- DuckLake FAQ (catalog database options, multiplayer DuckDB): <https://ducklake.select/faq>
- DuckDB team, "Analytics-Optimized Concurrent Transactions" (MVCC and WAL background): <https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions.html>
