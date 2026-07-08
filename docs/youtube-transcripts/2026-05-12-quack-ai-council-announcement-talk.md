# Quack: The DuckDB Client-Server Protocol — AI Council Announcement

**Event:** AI Council (DuckDB Labs announcement talk).
**Companion blog:** <https://duckdb.org/2026/05/12/quack-remote-protocol>

*Conference talk transcript (DuckDB Labs).*

---

## Previously on DuckDB

Hello everybody. This is quite interesting — thank you so much for the invitation. And I'm still amazed I got away with this talk proposal, because I basically just said "this is what we wanted to surprise you with." It wouldn't have been a surprise if it had been in the program half a year ago.

Let me start with the story so far — a small recap, like in a good TNG two-parter, where we start with a "previously."

So, DuckDB. I've spent the last decade or so working on DuckDB together with many other wonderful people. And since we're always looking for superlatives — it's the *friendliest* SQL database. I think that's fair. We're not going to get into the whole game of who has the fastest and whatever. It's the friendliest.

DuckDB is a universal data wrangling tool. That's how we like to call it, because you can use it for everything under the sun as long as it's vaguely tabular. It's used quite everywhere — it's wild to see. I'm still totally amazed that our little project from university became such a big thing. It runs in space. It runs in a battery. It can run fully local in a process. It's ideal for tasks you don't trust your intern with — the ones where you send the intern to do it and hope they don't blow up the database. DuckDB is quite useful for this because you can run all of that locally, and you don't end up with a massive Snowflake bill. (I like the Snowflake guys, but you know what I mean.)

DuckDB has crazy adoption at the moment. We're running at about **40 million downloads per month** of DuckDB itself — and that's just the Python package. That's more than 1 million each day. And the extensions (plugins for DuckDB) are downloaded about **160 million times each month**, which is also totally wild. All these downloads must be humans, right? We're absolutely sure. We'll run out of Americans in about 10 months. *[laughter]*

## DuckLake

But we went beyond DuckDB. Almost exactly one year ago I stood on this stage and hinted in my last slide at something new. We announced **DuckLake** about a week after the conference — a lakehouse format that admits metadata is best stored in a database. The preview was released in May, about a year ago.

DuckLake is the simplest way to a data lake. We have a slightly aggressive slide where we show the bits and pieces of Iceberg on the left — you need a catalog, then metadata files, manifest lists, manifest files, data files, and it's all a bit complex. And on the right we have DuckDB, which admits metadata should probably be stored in a database, so you get a much simpler structure.

This hit a nerve. We actually expected criticism but got an overwhelmingly positive response — lots of praise, on a *preview*. People said things like "DuckDB and DuckLake — why we bet the company on the Duck stack." We banged out a preview and people said "let's bet a company on it." That's only the stuff I can talk about in public.

A few weeks / a month ago we released **DuckLake 1.0** — the first version we think is production-ready. This time we got press coverage, which was nice. People said "DuckDB is the thing only the DuckDB people love, the real people use something else." But we can count extension downloads. DuckDB has extensions for Iceberg, for DuckLake, and for Delta Lake. Within less than a year — and only 4 weeks after declaring 1.0 — DuckLake is at 2.5 million downloads a month, Iceberg is at 2.9 million, and Delta is at 2.3 million. It's amazing to see how many people are happy with this.

## But DuckDB can't talk to DuckDB

But that's not what I want to talk about today. *[the clicker stops working — dramatic pause]*

Today is really not about DuckDB. We also worked on other things this last year — less public than DuckDB. It's insane speed for any database project to run these gigantic initiatives, finish them within a year, and do something else in the meantime. Pedro, one of our guys, ended up taking over DuckDB so I could do something else, and others took over project management. This is something I've worked on personally, so I'm kind of nervous to talk about it.

Single-node DuckDB works great — I'd even say extremely great. You have a single computer, and it will just keep crunching until you run out of disk space, which is a pretty good boundary. DuckDB can also talk to almost everything under the sun: Postgres (via the postgres scanner), MySQL, SQLite, random ODBC drivers — pulling data back and forth. It talks to object stores, all the data formats (Parquet and its various competitors), catalogs like S3 Tables. All wonderful. Little check marks everywhere.

**But somehow DuckDB cannot talk to DuckDB.** That's a bit annoying, isn't it? And we're not the first to notice. There's a large number of GitHub repos — running at about one per week popping up — where people add functionality for DuckDB to talk to DuckDB. Here are four of them; there are many more. Maybe it's easy to vibe-code in an afternoon, which is why so many appear. At some point I have to take off my academic hat of being right (which I love being) — because at some point it's about solving people's problems. Clearly there's a need out there.

A year ago I stood here talking about the benefits of single-node in-process databases. And I still fully think making a single-node database is a great idea. But there are lots of use cases an in-process database cannot cover.

One example: the **real-time analytics** use case. You have a fleet of nodes doing whatever, trying to store telemetry in a central authoritative location — a flood of fairly small inserts. What do you do with your in-process database? It's really difficult to map that, and it's unfortunate because it excludes a lot of exciting use cases. From the academic side, we have to admit this is also part of analytics — the academic world has long treated *change* in analytical databases as someone else's problem to solve.

The crazy thing is this even happens on localhost. People have multiple tools they want to talk to the same database — a DBeaver or DataGrip on a DuckDB database, and then the CLI to do something else, or the DuckDB UI. This has been a long-standing feature request. You can kind of do it already with DuckLake, but that's a lot of additional operational complexity, and performance for small inserts is bad — lakehouse formats really aren't made for tons of tiny inserts.

## Enter Quack

So that was the motivation. Now — this is just the happiest duck. I found this picture and was so happy I get to show you this little happy ducky. So let's say we want ducks to talk to each other. What do they do? They **quack**.

Today I'm very happy to show you **Quack** — a DuckDB extension that extends DuckDB with quite powerful capabilities to communicate with other DuckDBs. Maybe we can quack together — we do this in our company quite a lot. Quack quack quack quack quack! *[audience joins]* Excellent. You made me very happy.

So what is Quack? It's a client-server protocol for DuckDB in the traditional sense, implemented as a DuckDB extension. The cool thing is that **both sides are just DuckDB** — there's no separate server, no separate client. It's just DuckDB. You have two DuckDBs (here, one green and one blue).

On the left you say "please serve this local database that I'm currently in on this localhost," and maybe create a table there. On the other side you say `ATTACH` — which is DuckDB's way of connecting to other things (Postgres, SQLite, DuckLake, whatever). Then you can type, for example, `FROM remote.foo` and magically the query runs on the other side, the result is shipped back, and displayed on the blue side. You can also write an explicit `FROM remote.query('... FROM foo')`, and we're working on something really cool there with DuckDB 2.0 to make it nice in a user interface.

This is a minimum example, but you can do anything reachable from SQL — everything. All the extensions, all the data types, everything just works. And it works *today*: if you have DuckDB 1.5.2, you can run this script and it already does all of this. Client-server right out of the box. It works for all DuckDB distributions — Python, shell, WebAssembly, Windows, macOS, Linux — we don't care.

This is being released today as a beta. We expect the production release in a couple of months with DuckDB 2.0 (coming in fall, in case you haven't heard). We'll do some simplification on how you specify queries that should run remotely, which will give a great experience writing queries for other servers — it's just not there yet, and I didn't want to show you syntax that doesn't work today.

## How Quack works internally

At the bottom is TCP/IP — no choice, we're not using UDP. On top of that? HTTP. We need a protocol on top of HTTP because of WASM — DuckDB runs in the browser, and we want to talk to a server from the browser, so we don't have a lot of choice.

Using HTTP for a database protocol you build in 2026 is also great because all the firewalls like it, the cyber people don't freak out, you can slap TLS on it and it's encrypted. And it's actually quite impressive — we benchmarked it, and because HTTP is so ubiquitous, all the hardware is optimized for it. Counterintuitively (I guess because of YouTube), HTTP is faster than anything else — the hardware prioritizes it and knows how to deal with it. If you use another protocol, it's actually slower.

On top of HTTP we need a serialization format to encode queries and results. We already have that in DuckDB — a serializer for the write-ahead log — so we just use that. Lossless serialization of all the internal structures: types, columns, data chunks, all that stuff.

On top of that we need an interaction protocol. It's straightforward — messages with different types, like every other database RPC. Request-response: execute a statement, fetch more results, and so on.

### Authentication and authorization

Now we have some side requirements — everybody's favorite: **authentication**. (Who here loves this? No one. Me neither.) Once you have client-server, you can't hide behind the in-process authentication model anymore. Authentication is terrifying. But we realized we can't solve it for everybody, so we made it **pluggable through DuckDB extensions**. Anyone can override the authentication method with whatever they think is best. There's a default one based on tokens, but you can decide — glue it to your LDAP server if you want. With DuckDB's community of extension writers, we can rely on them to cover more use cases, and big corps can make their own. You can even just write a SQL function if you can express your authentication in SQL.

The even harder thing is **authorization**. We've authenticated somebody — now how do we decide permissions? Table-level, column-level, row-level — a huge feature set. We've managed to avoid it so far, and again we don't want to solve it once and for all. We give you the flexibility: there's a callback that sees what the user wants to do, and you can decide whether to allow it, and even modify the queries the user is running. Quack is giving you the tools to build something exciting — not trying to solve everything from the get-go.

## Experiments

Because I'm a database person and can't live without a performance experiment. Back in 2017 we wrote a paper on how terrible database client-server protocols are — every database protocol was worse than `netcat` by a factor of 10. Those insights made us strong believers in in-process. So it's extra terrifying to build a client-server protocol when you've been the one complaining about everyone else's. We better get this right.

**Setup:** client and server, both on AWS VMs — fairly small (32 GB RAM, 8 CPUs) but with fast networking (15 GB/s) in the same availability zone. We tried three things: **Postgres** (everybody likes Postgres), **Quack** (of course), and **Arrow Flight SQL** (via Gizmo Data, one of the projects that showed us people want this). Two experiments: bulk transfer and small inserts.

**Bulk transfer** — transferring millions of rows (100K, 1M, 10M, 60M), measuring wall-clock time (lower is better):

- **Quack:** ~5 seconds for 60 million rows
- **Postgres protocol:** ~3 minutes
- **Arrow Flight:** ~20 seconds (better than Postgres, still much slower than Quack)

**Small inserts** — single-insert transactions with an increasing number of client threads (1, 2, 4, 8...), measuring completed transactions per second:

- **Arrow Flight:** not doing well — designed for bulk, and only bulk
- **Postgres:** doing okay — it was designed for this use case
- **Quack:** surprisingly did really well — ~5,000 transactions/second at eight clients

We optimized the protocol a little to make this work. So it's fast both for bulk and for small transactions.

## What you can do with it

You can glue two DuckDBs together — the classic case. Because you have DuckDB on *both* sides, you can do really cool things: post-process a result coming from the server, or aggregate your local data before inserting it into the central server. You can do crazier things — proxy a bunch of shards living in separate databases through one coordinator node and send that to a client through Quack. Shards, replicas, or both — the Duck Stack in action. Your imagination is the only limiting factor.

We're also planning to add **WAL replication** to Quack, so you can have a read replica for DuckDB that's automatically kept up to date. We've been surprised so many times by what people build with DuckDB.

## OLTP vs OLAP — yesterday, today, tomorrow

Let me zoom out to the endless OLTP-vs-OLAP debate. Where is DuckDB on the spectrum? You'd argue pretty far on the analytics side.

- **Yesterday (common wisdom):** OLTP is Postgres land, DuckDB is OLAP, and somewhere in the middle is the elusive HTAP. But this is wrong. Actually, **Postgres is the middle** as a general-purpose system, and there are hardcore OLTP systems like **TigerBeetle** that run circles around Postgres in transactions. Postgres isn't really great at any single task, but good enough for a lot of use cases.
- **Today (with Quack):** We've been working behind the scenes on concurrent transactions, concurrent checkpointing, and commits while checkpointing. Still work to do — and we just announced Quack today. But this moves DuckDB much closer to the middle **without compromising anything on analytics** — we haven't made anything slower. DuckDB is becoming general-purpose too, coming from the analytics side.
- **Tomorrow:** Who knows. Maybe it's time to use a database built after 2000 at some point.

## Mission

Our mission at DuckDB Labs is to empower people to deal with data and build amazing systems with confidence. Quack is an important ingredient — it greatly expands where DuckDB can be useful. We've had this restriction that DuckDB was in-process only and couldn't deal with more complex deployment models. That's changing, and I'm excited we've built something that's also fast.

## Demo

*(Medium terrifying, as you can imagine.)* Here's a browser. I put a magic URL in — and it works. What happened: I loaded a DuckDB instance **in the browser** (already mind-blowing that this is possible). Then `INSTALL quack FROM core_nightly` (it's currently in a separate repository because it's still moving), then `CREATE SECRET` — DuckDB's way of handling authentication tokens (we use secrets for everything, like S3 credentials). You can copy-paste my token if you're quick.

Then I run a query — this hits a node running in a CloudFormation stack on Amazon, a small T3 micro. I `ATTACH` the remote database as an alias `remote`. Now I can look at a table living on that other server (hopefully the Wi-Fi doesn't let me down)... Yes! *[laughter]* Quack talked to the server sitting in EC2 from this laptop — because this runs in the browser, and it's all good.

You can also say `FROM remote.query('SELECT * FROM line_item LIMIT 10')`. This is equivalent, but the second one was faster — why? Because the first one transferred the whole table, so we had to wait a little. This is exactly what Quack lets you choose: where computation happens — either ship the query to the remote side, or run it locally with the virtual catalog attachment. This works as of today. You can start building stuff with Quack.

## Closing

We're really excited about Quack because it solves this long-standing issue and expands the use cases where DuckDB is useful. And if everything goes well, the blog URL is live now — Gabo, our DevRel, heroically stayed up in Amsterdam to click the publish button during the talk.

This is the next frontier for DuckDB, where it becomes more of a general-purpose system without compromising its strong in-process roots — and without losing them. All the use cases DuckDB has been solving still work perfectly well; we're just adding to that.

Thank you so much, and I'm happy to answer some questions. *[applause]*
