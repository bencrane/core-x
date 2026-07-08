# Quack w/ Hannes Mühleisen (DuckDB)

**Published:** 2026-05-27.
**Source:** Practical Data "Lunch and Learn" with Joe Reis (YouTube) — <https://www.youtube.com/watch?v=ACOMAyOEFYU>

*Video transcript. Cleaned from an auto-generated transcript — heavy speech-to-text artifacts corrected (Quack, DuckDB, DuckLake, Postgres, etc.); wording lightly smoothed for readability, meaning preserved. Speakers: Hannes Mühleisen (DuckDB Labs), Joe Reis (host), Ramona C. Truta.*

---

**Joe Reis:** Welcome everybody to the Practical Data Lunch and Learn. This time last year my good friend Hannes Mühleisen gave a talk on DuckLake, and now he's got something new in store. This was announced a couple of weeks ago — we're going to talk about Quack, and lastly something new. Hannes, take it away.

**Hannes Mühleisen:** Excellent. Thanks for the invitation, we're really happy to be here. It's been interesting — two weeks ago we were in San Francisco and we talked about Quack. I can give you the first reactions as well. But before I do, I want to show a bit of what I've been showing at AI Council as motivation. Then my plan was to do an extended demo — something I wouldn't dare do on stage, but here I feel a bit more emboldened because I know the internet's going to work.

## Motivation

I presume you've heard of DuckDB — a universal data-wrangling tool that runs everywhere, including on your neighbor's battery. DuckDB has been absolutely excellent at running on a single computer. Over the years we've added a bunch of stuff: DuckDB can talk to Postgres, MySQL — actually anything with an ODBC driver. So you can have a database living somewhere else and talk to it from DuckDB. We accidentally made one of the best Postgres clients — one of those funny things that happens sometimes. We've also had this integration with object stores — S3, Azure, Google — where you store Parquet files. That all works really well.

**However, DuckDB could not talk to another DuckDB, and that was by design.** When we started DuckDB we felt very strongly about the single-node in-process architecture, because it simplified a huge amount of things. With a single node you don't have to manage a cluster. With an in-process architecture you don't have to manage a server process or a Docker container or whatever. But the consequence was that it's not trivial to talk from one in-process database to another.

Then we looked at the internet and saw a bunch of GitHub repos of people implementing ways for DuckDB to talk to DuckDB — for something outside the process to talk to DuckDB. Gizmo may be one of the better-known ones. So many repos kept popping up that we were like, okay, we probably have to do something about that. If your architectural decisions are being second-guessed by everybody out there, you have to come back and reconsider them.

One use case I want to highlight is **observability**: you have a bunch of nodes that measure something, or collect logs, or run out in the field collecting telemetry, and you want to centralize all that information in a central authoritative place. That's a use case DuckDB couldn't do, and it kept people from solving their problems with DuckDB. We're not out there to be right — from an academic perspective, we try to solve people's data problems. This was one of the examples where we just couldn't help, so we had to reconsider — and swallow our pride.

## What Quack is

If you wonder where the name came from: the way ducks talk to each other is by quacking. So if we make something that makes ducks talk to each other, it had to be **Quack**. Quack is a way for DuckDB instances to talk to each other — on the same computer, over the network, between planets, who knows. It's a database client-server protocol specific to DuckDB.

Here's an example. On the left we have two DuckDB instances — one green, one blue — which can be different processes, computers, or planets. On the green side you say "I want to start serving this database that I'm in, using Quack." You call `quack_serve`, which starts an internal built-in server. Say on the green side you created a table. On the blue side you can `ATTACH` this database and start querying it. You can use our implicit magic — `FROM remote.foo` (in DuckDB `SELECT *` is optional, so `FROM` means the same as `SELECT * FROM`) — or you can say `FROM remote.query('... FROM foo')` to explicitly specify the query to run on the other side, and the result is shipped back.

When I presented this at AI Council we still had some preview things to deal with, but since DuckDB 1.5.3 released last week, all of this just works out of the box, because Quack is implemented as a DuckDB extension. It automatically installs once you start using it. Very elegant — you don't have to do anything.

## How Quack works internally

At the bottom is TCP/IP — totally shocking, we need a network protocol and we're not going to invent a new one (although that would be very DuckDB style — we could invent our own network protocol and screw everybody in the process). No, it's just TCP.

On top of that it's **HTTP**, which is a bit more controversial — we got some Hacker News comments about how you'd ever dare to base a powerful protocol on HTTP. The simple truth is it's a great choice, because DuckDB also runs in the browser, and the browser can only speak HTTP. By basing Quack on HTTP, you can talk to a DuckDB server from the browser, which is very elegant. There's also the fun fact that all the infrastructure out there is optimized to make HTTP fast — every firewall knows it, the security people know it, it's not something that pops up in your firewall log the first time you try Quack.

On top of HTTP is a simple request-response scheme. Then — somewhat more controversial — how do you encode the structure of these messages, which need to contain complicated things like data types, tables, and queries? Wouldn't you just use Arrow? We thought about it at length and made a conscious decision **not** to use Arrow for Quack's internal protocol, but to use something DuckDB has anyway — the **serializer**, an internal facility that already existed. We use it to turn multi-dimensional objects into a single-dimensional data stream, which is required to shove into an HTTP message. On top of that it's a simple protocol — about four message types: "I want to connect," "I want to send a query," "I want to fetch results." That's basically it. We don't want a lot of complexity there.

### Authentication and authorization

We've lived in a blessed world where we didn't have to deal with authentication, because DuckDB was in-process — if you have access to the process you can do whatever you want, so there was no point defending against a client in the same process. Now with Quack we have to deal with it, and this is an endless well of complexity, so we deflect some of it using our ecosystem. Quack ships with a very basic token-based authentication — you have the token, you can come in — but you can **override this from an extension**. If you have some crazy Kerberos-based authentication your 80-year-old CTO insists on, ship an extension with your own authentication method and you're done. You can even write a SQL function to do authentication — read a file, check whether you're allowed.

It gets more complicated for **authorization** — who gets to do what — another endless well of complexity. Right now we don't do any of it, but we let you override it. If you have a crazy authorization scheme — say row-level access control on some criteria — you plug in a function that looks at the query, can remember the protocol, and says yes or no. It can even rewrite the incoming queries. Very flexible, and we didn't have to implement the definitive version ourselves.

## Benchmarks

Full disclosure: many years ago Mark and I wrote a paper on how terrible database client protocols were. In the plot on the first page, all the database protocols were at least 10× worse than `netcat` (which just copies bytes to the socket) — everybody was at least 20×, some 1200× worse. So I was in the wonderful position of having to design a database client protocol that wouldn't suffer from this.

**Setup:** two computers (one client, one server), both on Amazon EC2 in the same availability zone, normal-sized servers. Two experiments: bulk transfer of rows, and small transactions (the observability case).

**Bulk transfer** — cranking the number of rows up to 60 million (a big dataset):

- **Quack:** ~4 seconds
- **Arrow Flight:** ~20 seconds
- **Postgres:** ~3 minutes

That's a big difference, and it makes sense — Postgres hasn't changed their protocol even since we wrote the paper; they have huge overhead. Quack is blowing everything away, as it should since we optimized it for bulk transfer. What surprised us a bit was doing so much better than Arrow Flight, for some interesting technical reasons.

**Small transactions** — same two-computer setup, a stream of inserts, increasing the number of parallel processes. Threads on the x-axis, transactions per second on the y-axis (higher is better). Surprisingly, at eight parallel threads we **outperformed Postgres** on the transactional case — even though Postgres is designed for exactly this. Arrow Flight, really designed for bulk transfer, did not do well here. We were very happy about that.

## What you can do with it

The simplest thing, which wasn't possible before: a client and a server talking via Quack, both ways. You can pull a large amount of data from the server or push a large amount to it. For cases where DuckLake is a bit too heavy and you just want to centralize data in an authoritative place — maybe with small transactions — you can use Quack. You could imagine crazier things: a coordinator that does round-robin of queries or sharding of data, talking to instances both ways. At DuckDB we've learned people build much crazier things than you can imagine when you just build infrastructure — you build pieces and people do stuff with them.

### OLTP vs OLAP — yesterday, today, tomorrow

- **Yesterday** (before Quack): common wisdom said use Postgres for OLTP transactions and something like DuckDB for analytics. But that's not quite true. Really, you had excellent transactional databases like TigerBeetle and excellent analytical databases like DuckDB, and Postgres was in the middle — "general-purpose transaction processing" — good at pretty much nothing but okay at a bunch of use cases, so still useful.
- **Today** (with Quack): DuckDB moves much more into the middle, into general-purpose territory, useful for more use cases because it now has client-server capability.
- **Tomorrow:** who knows. (Slightly cheeky.)

## Q&A

**Ramona C. Truta:** Do you outperform Postgres on transactions because you started from scratch and benefited from all the learnings — whereas anyone who started building 20–30 years ago is carrying that legacy and can't redesign everything?

**Hannes:** Great question, and you're absolutely right. I've spent time with Wireshark on the Postgres protocol. For example, it transfers the schema for *every row* in a result set over the wire — you'd argue the schema is fixed for the whole result set, but that has to do with Postgres's roots. That's a huge overhead in the bulk-transfer case, which is why it's so hopeless there. For the transactional case it's less obvious why we outperform, but I'm not going to complain. Designing a protocol now gives you a lot of hindsight — people generally don't touch these protocols. We had the advantage of a green field in 2026. We talked to the MotherDuck folks, who built a client protocol about four years ago, and they told us what they regretted, so we could avoid it.

**[On unlocks he's excited about]** In the last two weeks people have already built four, five, six different clients for Quack in languages completely independent of DuckDB. We'd thought the elegance was that both client and server are DuckDB, so we don't have to provide a JavaScript client — but somebody built a standalone client anyway. Exciting to see. The unlock I'm most excited about: **DuckDB as a remote catalog server for DuckLake**, which already works.

**Andrew:** Do the performance gains apply to external tables?

**Hannes:** It depends on how fast you can read those external tables. If DuckDB sits in front of a Postgres server, you're limited by how fast you can read from Postgres. In front of an S3 bucket with a slow network, there's no magic — we can't beat the I/O behind DuckDB. But on a fast SSD without crazy queries, you'd usually be bound by the client-side I/O bandwidth, which is a good place to be. We do use DuckDB's internal column compression on the Quack wire, which helps a lot — e.g. not transferring a constant vector with a million identical values, just sending it once.

**Dan:** For speed of transactions, is it best to use a Quack DuckDB catalog for DuckLake instead of Postgres?

**Hannes:** In general, yes — it's a great idea to use Quack and DuckDB as a catalog server for DuckLake. DuckLake has this wonderful **inlining** feature (buffering rows in the catalog instead of going straight to Parquet files, great for small inserts), and we were running into limitations of Postgres with that already. So that's a pattern I do recommend looking into.

## Demo

*(Live, end-to-end.)* I launch an empty Amazon EC2 instance — default boring Ubuntu, nothing on it, no DuckDB. Then `curl` install from `install.duckdb.org` — and DuckDB's already running, `SELECT 42`, wonderful. Now I want this EC2 box as a Quack server, so I use `quack_serve`. It yells at me because I'm binding to `0.0.0.0`, which I override for the demo.

A note: Quack is meant to be served over HTTPS, but the Quack server itself doesn't speak HTTPS. There's a whole guide on the website for serving it properly — a reverse proxy like nginx that terminates SSL and reverse-proxies to the local Quack server. You should generally not bind to any external interface without setting that up.

I open the port (default `9494`), then from my local DuckDB I `CALL quack_query(...)` with the hostname and the auto-generated token — and it works. From scratch, including clicking around Amazon for five minutes. I create a `hello` table on the server, insert "hello I'm server," read it back over the wire — great. It's a bit clunky to type `quack_query` every time, so I use `ATTACH` with the token, then `FROM remote.hello` works, and I can `INSERT INTO remote.hello VALUES ('hello I'm the client')` — data goes both ways.

Now let me stress it. I load TPC-H via the benchmark extension on the EC2 machine — a few million rows of `lineitem`, `SELECT count(*)` = 6 million. Pulling 100,000 rows across the Atlantic (Amsterdam → us-east-1) finishes in ~2 seconds. A million rows — I guessed 20 seconds — took **4 seconds**. One cool thing: Quack **auto-parallelizes** row retrieval. If it detects the result will be big, it spawns a bunch of threads that pull the result in parallel, which hides the fairly big latency between me and the box.

**The reveal:** DuckDB as a remote catalog server for DuckLake. Previously if the DuckLake catalog was remote, it always had to be something like Postgres. We don't hate Postgres — we think there are improvements possible. In a Colab notebook I `pip install duckdb` (Colab ships 1.3.2, you need 1.5.3), call `quack_serve` to start a server in that process (Pedro from our team demonstrated running Quack within the same process — which is why the token is his dog's name), then `ATTACH` through DuckLake with the `ducklake:` prefix pointing the catalog at the Quack server. Create a DuckLake instance, insert a table, do time travel — it all uses the Quack server running in that process.

## More Q&A

**Jose:** What had to fundamentally change in DuckDB to handle concurrent reads/writes?

**Hannes:** DuckDB itself has always handled concurrent reads/writes — but you had to be in the same process, with restrictions. For example, we couldn't handle concurrent writes while a checkpoint was running. (Fault tolerance works by writing changes to a journal — the write-ahead log — forced to disk, so after a crash the database file can be recovered to the last committed state; checkpointing folds those journal changes back into the main file so the journal doesn't grow forever. SQLite rewrites the database file every commit, which is inefficient.) Mark recently worked on lifting the "can't commit while checkpointing" restriction, so DuckDB can do that now. But **for Quack to work, nothing fundamentally had to change in DuckDB** — I don't think I committed any change to DuckDB core as part of implementing Quack. Quack is just an extension. It benefits from a lot of recent work to make DuckDB more efficient for concurrent access, but it didn't require it.

**[On] high availability for a DuckLake catalog** using DuckDB Quack instead of Postgres CDC: You can do failover, load balancing, etc. because **Quack runs over HTTP** — serve it over nginx and all the usual HTTP tricks work. One thing we haven't done yet but is planned: shipping journal entries to a secondary standby replica over Quack. We already have the infrastructure, because our journal also uses the serializer, so we can reuse those components to ship log entries to a secondary replica and get HA. The neat thing about Quack being built on HTTP: people know how to do operations on HTTP services, so it's not weird to manage. If you want a Postgres-protocol proxy you're writing a bunch of code; if you want a Quack proxy, you already have one.

**Joe Reis:** What about fleets of Quacker DBs / distributed DuckDB?

**Hannes:** Exciting, not something we've worked on yet. You can have a coordinator that speaks to other DuckDB instances; the tricky bit is splitting up the tasks — if you want truly scaled-out you have to deal with task splitting, one of the big multi-decade challenges in databases. But it's possible. What I'd find interesting immediately: replicas with load balancing over them. If you want to `UNION` the results of a bunch of DuckDB instances, that works — attach these second-level nodes to a centralized coordinator and query the union, all DuckDB. Also, because in Quack **both client and server are DuckDB, you can do query processing on both ends** — the client isn't dumb. If you want to re-aggregate or filter a result you pulled from the server, you can, which is pretty unique architecturally. The holy grail of endless scaling is maybe future work.

**Tune:** OAuth support?

**Hannes:** Somebody already wrote a Quack-over-OAuth extension that overrides Quack's authentication to use OAuth. That's the beauty of community — you make something pluggable and people build all sorts of things. We're thinking about making the protocol itself pluggable so you can register new message types, and client-side we're planning a new statement to explicitly forward queries to the remote side.

**Joe Reis:** What are you excited about with Quack? And remind us of the download numbers.

**Hannes:** DuckLake launched a year ago and is already in second place among Iceberg, DuckLake, and Delta in our extension downloads — roughly 3 million Iceberg, 2.5 million DuckLake, 2.3 million Delta. In a year DuckLake has become a thing people bet the farm on. Firebolt just announced they're replacing lots of internal structures with DuckLake. Someone already made a "Quack + DuckLake on Cloudflare" project. I can't keep up with all of it — a lovely problem to have. I hope Quack also inspires changes in other projects; in the end I care about data systems getting better, whether it's my code or not.

**[What's coming up]** Today we changed our email addresses (a terrifying thing in Google Workspace — actually impressed by how it works). Quack as a catalog server for DuckLake is really exciting. DuckDB 2.0 is coming with big changes: **async I/O finally**, a whole new extensible-syntax parser (just about finished — close enough to put in the release), and things around the extension API. Even in our small company I don't know exactly what every last person is cooking up.

**Joe Reis:** I want to check out the peer-to-peer data-sharing abilities of Quack — could be really interesting for data teams.

**Ramona C. Truta:** You're eating someone's lunch with this, yes?

**Hannes:** I'd disagree. VCs always talk about "the oxygen," and I disagree — the data space is huge and growing, so you can grow without eating somebody else's lunch. Maybe you don't take over the world or become Oracle, and that's fine — that's not what we're aiming at. I'd say we're **unlocking things that didn't work before**. With DuckDB we put analytical query processing in places nobody had thought about — people run DuckDB on batteries; you couldn't run Spark on that. We're not taking away from any Spark deployment, just expanding possibilities. That's also what Quack is about. I don't see it so much as eating lunch as unlocking possibilities.

**Ramona:** So not purposely, but when you unlock it, you give people a choice.

**Hannes:** Of course. I like databases — what can I say.

**Joe Reis:** It unlocks a lot — decentralized data, data meshes people have been talking about for a long time. When you started the Jupyter notebook I thought, data teams could totally use this behind the scenes to share data without SFTP or sending it over email. Huge possibilities. Hannes, thank you very much — this was a bit spur-of-the-moment. This'll be up on YouTube as well.

**Hannes:** Thanks so much for the invite, and thanks everybody for the questions — really enjoyed them.
