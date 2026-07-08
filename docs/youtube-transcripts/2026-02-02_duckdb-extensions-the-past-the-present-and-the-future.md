# DuckDB Extensions: The Past, the Present, and the Future

**Speaker:** Sam Ansmink (DuckDB Labs) — 2026-02-02, conference talk (followed a hands-on extensions workshop by "Rusty").
**Topic:** A high-level tour of the DuckDB extension mechanism and ecosystem — how it came to be, the current state, and where it's going (the stable C API).

*Talk transcript. Cleaned from an auto-generated transcript ("DuctTb"/"DTB"/"ductb" → DuckDB, "duck lake" → DuckLake, "hpfs" → httpfs; wording lightly smoothed, meaning preserved.)*

---

Hi, my name is Sam. I'll be talking about DuckDB extensions. I hope many of you enjoyed Rusty's workshop this morning, where he went really in depth — I'll take a step back and do more of a high-level view. I'll fly through the history of DuckDB extensions, where we are now, and the future.

Today's menu: introduction, the past, the present, and then the future. In the **past** I'll give a flight through the key points in DuckDB extension development — not just the extensions but the whole framework and ecosystem. In the **present** I'll look at the current state and best practices — if you want to get into extensions now, what's there. In the **future** I'll give a sneak peek at what we're cooking up.

## Introduction

I'm **Sam Ansmink** (for non-Dutchies, just "Sam"). I've worked at DuckDB Labs for over four years. Before that I did my master's thesis at **CWI** — not far from here, the birthplace of DuckDB — in the Database Architectures group, on **encrypted query execution in DuckDB**. Then I joined DuckDB Labs (then a small startup) and worked on various extensions, but also extensively on the **extension framework itself**: the extension template, the different APIs, and the CI/CD pipelines to deploy and test them.

*(Audience poll: nearly everyone has **used** DuckDB extensions; ~50/50 have **written** one — impressive, probably thanks to Rusty.)*

### What are DuckDB extensions?

A way to **add or alter functionality** to the core DuckDB feature set — table functions, types, file systems, catalogs, cryptographic modules, and more. Examples you may know: the **JSON** extension (read JSON files), the **Postgres** extension (integrate with Postgres), the wonkier **Google Sheets** community extension (read/write directly from Google Sheets), and some obscure ones (that's how science works).

### Why have extensions at all?

Four key reasons for us:

1. **Binary size** — DuckDB is an embedded database meant to run everywhere, including storage/memory-restricted environments. A 3 GB binary is a problem. Extensions let users choose the trade-off between feature set and binary size.
2. **No external dependencies** — DuckDB is a **zero-dependency** system, but we *want* to use great libraries. The solution: push them into extensions, so DuckDB core stays dependency-free and you're only exposed to a dependency if you use the feature that needs it.
3. **Functional incompatibility** — e.g. different, functionally incompatible SQL dialects — extensions are a great mechanism to handle that.
4. **Different maintainers** — different people can maintain different parts of the code that runs in DuckDB.

### Using extensions

DuckDB **autoloads and installs** extensions. A `SELECT * FROM <json file over the network>` transparently installs and loads two required extensions — you won't notice except that two extensions are now installed. You can also install/load manually.

**Where do they come from?** Two **extension repositories**: **core** (extensions DuckDB actively maintains) and **community** (community-maintained). Straight from SQL: `INSTALL <ext>` (default core) or `INSTALL <ext> FROM community`.

## The Past

A (slightly wonky) timeline: the **first DuckDB commit in 2018 at CWI**, through the first release and the first stable **v1.0.0**, up to **v1.4.4** released last Monday.

Key points in the extension mechanism/ecosystem:

- **2020** — the **extension mechanism** itself (DuckDB's ability to put code in an extension and load it). Pretty old by now, still how DuckDB loads extensions today.
- **A year later** — the **core repository** (mechanism for DuckDB to auto-install extensions via `INSTALL`).
- **~Two years later** — the **C++ extension template** — DuckDB Labs saying "not only can *we* build these, *you* can too, and here's how." Now used for most core and community extensions; also a guideline and a promise of how it works.
- **A year later** — the **community repository** — giving people an easy place to deploy extensions so users install them as easily as core ones.
- **Right after** — two more templates: the **Rust** and **C** extension templates (they come back later — keep an eye on them).

A few notable extensions: **ICU** (a big library) was the direct reason to build the extension mechanism ("we want this but it doesn't fit — extensions it is"). Over the years many familiar extensions arose, and last year the **DuckLake** extension (our open lakehouse format).

## The Present

Extensions are **ubiquitous**. Stats:

- **32 core** extensions, **145 community** extensions.
- Core extensions downloaded **>27 million times/week**; community **>500,000/week**.
- Serious numbers relative to client downloads (e.g. the DuckDB Python client) — extensions are integral to using DuckDB, explained by autoloading and reliance on features like Parquet and httpfs.

### How extensions are built today

- **C++ template** (first on the timeline) — builds against DuckDB's **unstable** API. The **recommended** way today.
- **Rust template** — experimental, nice on paper but has shortcomings; currently codes against an unstable API (may change).
- **C template** — can build both C and C++, and is the **first that works with a stable API**.

### Who maintains extensions

- **Core repository:** **primary** core extensions (top DuckDB Labs support), **secondary** core extensions (more experimental / less support), and **third-party** core extensions (maintained by DuckDB Labs partners).
- **Community repository:** maintained by the community.

## The Future

**The challenge:** the recommended template uses an **unstable C++ API** — it can change every version, so you can't rely on functions being the same next release. Downsides:

- Must **rebuild all extensions for every DuckDB release** — a burden.
- **Heavy maintenance** — when a core engineer changes a widely-used API, it breaks many extensions, and every maintainer has to figure out what changed and fix it.
- **Hard to document** — the API is a moving target on purpose (so DuckDB can move fast).

**The solution:** a **stable C API** (which is where the other templates come in). It brings **stability** and, being a C API, **good interoperability** with languages like Rust. Compile an extension once and it keeps working across multiple versions (ideally forever) — maintainers can build and largely forget.

**Goals**, all focused on the stable C extension API and switching the **default** from the unstable C++ API to the stable C API:

- **Expand the C extension API** as much as possible so we can move our own extensions over (not all functionality is there yet — Mo has done great work adding features, and we're getting close).
- **Stabilize** both the Rust and C extension templates, leveraging the increasingly powerful APIs.
- **Migrate as many extensions as possible**, deciding which move to Rust vs. C/C++.

### Takeaways

- DuckDB has an extensive, heavily-downloaded extension ecosystem and an ever-growing community of maintainers offering functionality we never dreamed of.
- Two main APIs: the **C++ API** (recommended today, reaches deep into DuckDB but comes at a price) and the **new stable C API** (improves life mostly for simpler extensions by bringing stability).

## Q&A (highlights)

- **Roadmap dates?** "Dateless roadmap" — internally the top two points target **v1.6** (v1.5 in a few weeks, v1.6 a couple of months after — roughly summer).
- **Private/company extension repositories?** Already possible — the only reason to use core/community repos is that those extensions are **signed**. Put DuckDB in **unsigned mode** to install/load from anywhere, or compile your own DuckDB with your closed-source extension built in.
- **Iceberg hive/hidden partition writes?** (Forwarded to **Tom**, top contributor to DuckDB Iceberg.) Top of mind for **V2**, then V3 support; would like V1.5, else a bug-fix release. Note: bug-fix releases apply to core DuckDB, but extensions ship small features in them — a side effect of tying extension releases to DuckDB releases under the unstable API.
- **Silent (auto) extension downloads — security risk?** The default mode optimizes for ease of use. For security-critical/critical-infrastructure systems, do mitigations yourself: see the "Securing DuckDB" docs page; **statically compile** extensions into your own DuckDB build and **disable extension installation** entirely.
- **Vulnerability reporting / scoring for published extensions?** Security is critical when the community builds and deploys. Important: **autoloading only works for extensions under the DuckDB core team's control — community extensions are never autoloaded** (that would be a hazard). Community-extension security is handled like other package managers — the burden is on users to trust what they install (like random npm packages); they're all **open source** (no straight binaries in the community repo), and DuckDB also actively checks for malicious extensions.

Thank you.
