# State of the Duck — DuckCon #7

**Event:** DuckCon #7 — 2026-07-11 (Amsterdam), opening keynote.
**Speakers:** Hannes Mühleisen (state of the project), then Mark Raasveldt (DuckDB 2.0 roadmap), then Q&A.

*Talk transcript. Cleaned from an auto-generated transcript ("Docker Con" → DuckCon, "Docker TV" → DuckDB, "qa.dockercon.org" → qa.duckcon.org; wording lightly smoothed, meaning preserved.)*

---

## State of the Duck (Hannes Mühleisen)

Welcome everybody. It's amazing to see everybody — I say this every time, but we would never have thought that our puny little GitHub repo would ever lead to something like this. This is **DuckCon number seven**, and as is tradition we'll start with the **State of the Duck** — our overview of what has happened and what will happen in DuckDB land.

We're live-streaming DuckCon again — good evening China and India, good afternoon Europe and Africa, good morning America. It's a growing global community, and we want people to participate even if they can't come to Amsterdam. For the talks today we're again using the **Slido** system for questions and upvotes at `qa.duckcon.org`. (The screenshot will steal all your Bitcoin and also get you to this website.) *[laughter]*

Big thanks to our sponsors: **MotherDuck** (gold) and **Spire** (silver). *[applause]* Without sponsors we wouldn't be able to have a free event in a lovely room like this — with ice cream.

**Our mission:** to give you confidence when working with data. There's so much anxiety about working with data — here's a folder of 1,500 wonky JSON files I just found; here's a CSV somewhere; here's a big Parquet file that seems untractable. Our mission is to give you the confidence to work with data of any shape and size and not worry too much about it.

**DuckDB** is the piece of software that started it all — years ago, in a bar not far from here, where we sat down and said it needs to happen. It's our biggest project and doing extremely well. **DuckDB is loved by agents** — from our download statistics, something built to do well with humans turns out to do well with non-humans too. Who would have thought?

DuckDB has become more than an in-process SQL library. It's "batteries included" now — all these protocols and storage systems we can talk to, other database systems (we may have accidentally built a **universal database client** for other databases), all these platforms DuckDB runs on (every day we hear about some strange platform someone got DuckDB running on, and we applaud in the office), integrations for almost every programming language, and all the data formats under the sun. Somebody had to become a real expert in each of those — we have people who've become big experts in CSV, Parquet (every last word of the spec), even Avro, sometimes against their will.

**Adoption:** we're now beyond **1 million installs every day**, and over **160 million extension installs every month** (shout out to our hosting company Cloudflare, who donates the traffic). Plus GitHub stars, LinkedIn followers, and — most and least importantly — the DB-Engines ranking. *[laughter]*

**DuckLake:** it's only a little more than a year since we published the 0.1 "wild idea," and just a couple of weeks ago we published **DuckLake 1.0** — from idea to production-ready in a bit more than a year. DuckLake took the idea of a lakehouse and **radically simplified it, DuckDB-style** — we don't need 15 layers of indirection or 15 layers of files. Adoption has been great: DuckLake is now up there with Iceberg and Delta in DuckDB extension downloads, equalizing in popularity within a year. There's the "why we bet the company on the Duck Stack" blog post — mildly terrifying, a lot of responsibility. DuckLake is now a grown-up project inside the company alongside DuckDB. You can use DuckDB to talk to DuckLake, but there are lots of other implementations too.

**Quack:** most recently (only about 4 weeks ago, though it feels like an eternity) we presented **Quack**, a communication protocol that extends DuckDB to talk to other DuckDBs. We thought that was that — but the world said "no, we're going to build another client for it," and people have already built **standalone clients** for Quack. The idea: two DuckDB instances communicating via the extension — `quack_serve` on one side, `ATTACH` on the other. We looked at the `FROM remote.query($$...$$)` syntax and said "this cannot be it," so I'm happy to tease that **2.0 will have new syntax — the `CONNECT` statement** to directly forward queries to the other side. Quack gives you a more traditional client-server pattern for DuckDB — something you didn't think we'd do, but people asked for it so persistently that we did.

**Company rename:** because we now have DuckDB, DuckLake, and Quack, we've changed the company name — it's now called **DuckLabs**, acknowledging that there's more than just DuckDB, and it rolls off the tongue better. Some people have been calling us DuckLabs for ages, so we adjusted our name to reality.

Now I'm happy to hand over to Mark for what's going to happen. *[applause]*

## DuckDB 2.0 roadmap (Mark Raasveldt)

Thanks for spending this beautiful sunny day indoors with us — it brings me back to my teenage years spent inside behind the computer.

I want to talk about the next release, **DuckDB 2.0**, named after the **cinnamon teal** (we name all releases after duck species). It's scheduled for the **fall**, and we have some big plans. Quack — released early as a preview — is going to be a central part of our strategy going forward.

Last year we announced the **year of the lakehouse** (DuckLake, plus Iceberg and Delta work). This year we're going for the **year of DuckDB as a server**. Quack is pivotal — it lets you run DuckDB as client-server — but the server side brings new challenges. Running DuckDB long-term and sustained (versus spinning up, running queries, tearing down in a script) means we're looking at **better metrics, logs, observability**, **stability**, and the **multi-tenant scenario**.

Interestingly, we built DuckDB since day one as an analytical *and* transactional database that can do multiple clients — full ACID properties, transactions, multiple connections, transaction isolation, MVCC. Most people weren't using that (single-user perspective), but now with client-server it becomes pivotal. A feature we've had for a long time is becoming much more useful to the community at large.

**DuckDB is not that bad at transactions.** If you've worked with analytical systems you may assume DuckDB can't do many deletes/insertions per second. Compared to some competing lakehouse formats (maybe a few transactions per second in many situations), DuckDB is actually not bad — you can run **hybrid workloads**, and some of our customers already do. Hannes's slide when he first revealed Quack showed **transaction performance of DuckDB/Quack versus Postgres**: they're not doing badly. Postgres does better on many transactional workloads, but DuckDB can be competitive with general-purpose databases like Postgres.

### Some 2.0 features

(Hard to fit on one slide — we have a lot of people working on DuckDB now; anyone wearing the company-logo shirt works for us, so reach out and chat.)

- **Variant type** — already in DuckDB v1.5. Think of it as "**JSON on steroids**" — imagine if JSON were fast. It behaves like JSON (store data in any row), but it's no longer just text: DuckDB looks at the JSON schema, extracts common structural patterns, and optimizes both at the **storage layer** (compressed, stored efficiently) and the **query-execution layer** (very cool performance). We plan (probably not for 2.0 — don't hold me to it) to **replace the JSON type backend with variant** so you get these advantages while still using the regular JSON type. Variant works very well with long-running log collection: real-time ingestion of streaming JSON logs (different sources, schema changes over time) into DuckDB while still getting engine benefits the current JSON type doesn't offer. Already in v1.5, with a lot of 2.0 improvements coming.
- **Triggers** — a SQL event mechanism: "if something happens in the system, trigger another event." Common use: audit tables / logs (e.g. after insert into a table, insert into an audit table). Works nicely with long-running services; we'll use it internally for other features, and it's exposed at the SQL level so you can build your own.
- **Object stores** — interacting with S3 and similar is often central to the DuckDB experience (DuckDB usually isn't your source of truth — though that might change with client-server). We've read object stores and parallelized for a long time, but there were limitations around **synchronous file reads**. DuckDB 2.0 brings **asynchronous I/O**: you can scale your input layer separately from query processing, using much more parallelism for remote reads — making remote/network reads a lot faster. First for Parquet, then other formats and the DuckDB format. Helps a bit with local storage, but network storage is where the big gains are.
- **Partition-aware execution** — DuckDB's native format doesn't really support partitioning, but lakehouse formats (DuckLake, Iceberg, Parquet files on S3) do, and it's often critical for optimal performance. We're making query planning and optimization **partitioning-aware** for much faster processing on partitioned data sets.
- **C++ V2 / broader stable extension API** — currently extensions (including ours) are almost always built against the *unstable* C++ API, so you must target a specific DuckDB version and constantly rebuild/update as new versions ship. Community extensions can disappear on the next release if the author doesn't keep up. We're **broadening the scope of our stable API** so extensions are written, built, and published **once** and remain available essentially forever — easier for developers and us, and more reliable for the community. (And with AI tooling it's now much easier to build extensions — give it a go.)
- **New parser** — DuckDB has famously always used the **Postgres parser**. Enough is enough — we're building our own **modern parser**. It ties into extensions, letting them integrate new parser constructs much more easily (so expect more extensions exposing new SQL syntax). You shouldn't notice anything — it should be compatible with the old one; if you do, please file an issue.

That's a quick show of some 2.0 features — many more to come. Time for Q&A — scan the QR code, ask or upvote questions. *[applause]*

## Q&A

**Q: How do you feel about major industry leaders like Databricks building next-gen engines (e.g. Raiden) modeled off DuckDB?**
We're very flattered — what else would we say? Imitation is the sincerest form of flattery. We've seen competitors feel required to build an in-process analytical database, and we're really happy about that.

**Q: Plans to bring the spatial extension closer to PostGIS, since you often compare with Postgres?**
(Ask Mark in the break.) Postgres compatibility is high on our priority list, and the spatial extension is one of those areas. As a rule, unless we have good reason, we don't want to deviate from Postgres.

**Q: [totally not passive-aggressive] Why is the Google Cloud integration so limited?**
Great question — ask your Google sales rep. We're very happy to work with them to make it better. *[laughter]*

**Q: Where do you see the DuckDB ecosystem in five years?**
Our vision has always been world domination, of course — five years is a good timeline for that, though it's always five years away (like fusion). "**DuckDB everywhere**" is a nice mantra we use internally — putting DuckDB in as many places as possible and expanding the use cases, like now with client-server. That's the goal.
