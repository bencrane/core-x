# Building Local-First Analytics Apps with SQLRooms — DuckCon #7

**Event:** DuckCon #7 — 2026-07-11.
**Topic:** SQLRooms — an open-source framework (and CLI) for local-first data analytics apps on DuckDB.

*Talk transcript. Cleaned from an auto-generated transcript; wording lightly smoothed, meaning preserved.*

---

Thanks for the great intro, Gabor. Hi everyone. I'd like to start with a question: **what if a DuckDB file could contain an entire analytics workspace?** Do you think it would be a good idea or not? I'm not sure myself — but let me tell you the story of how I came to this idea.

## SQLRooms the framework

I created **SQLRooms**, an open-source framework for building data analytics applications that benefits from DuckDB's powerful capabilities. It's supported by **Foursquare** (my current employer), but it doesn't belong to Foursquare — we **donated it to the OpenJS Foundation**.

It has a lot of useful building blocks for making data analytics applications:

- **DuckDB integration** with different connectors — it can work with **WebAssembly DuckDB**, **MotherDuck**, or an **embedded native DuckDB** as a Python server.
- **UI components** for querying DuckDB, looking at the schema, and similar tasks.
- **UI and layout capabilities** you can build your applications on top of: collapsible, resizable panels, tabs, and a grid layout for dashboards.
- A **modular system for saving your application state**.
- A concept of **artifacts** — configurable, pluggable, composable artifacts you can add to your project. They can be very different things; the framework provides some, but you can bring your own.

It's been used by a few projects. It started with the **Flow Map City** project I built before joining Foursquare. At Foursquare we use it for our spatial desktop general-purpose geo-visualization / geo-analytics tool, and for several internal applications. Other companies use it too. It comes originally from the geospatial space, but it's now used in other domains as well.

## SQLRooms the CLI — the single-file idea

A framework is a good thing, but I thought it could have better reach and be more useful as a **tool people could use without having to code**. So — it's early days and a bit rough — I'm working on a **CLI tool called SQLRooms** that you install with the **UV** package manager.

The idea: you have a **single file which is your project file**, and it's a **DuckDB database** where we store both the **data and the application state**. This way it becomes a **shareable, portable artifact** — very convenient. It's experimental, and you can make it even more experimental by specifying `--experimental` to see all the artifacts it currently offers.

When you load it, you get a UI where you can add data and create artifacts. For instance:

- Create a **dashboard based on Mosaic** (mentioned in the previous talk — there's a Mosaic integration in the SQLRooms framework). Build it manually, or talk to the assistant via the AI-assistant integration in SQLRooms.
- Create a **Notion-like block document** — you define which blocks you want to support (charts or text), and you can get help from the system to write an analysis.
- Use **Pyodide cells** to execute Python code, with a bridge to talk to the host DuckDB instance and get the data from it.
- Use a **free-form HTML application builder** that creates an embedded app you can run as an artifact within your project.

All of this is saved as part of your **single file**, with all your artifacts and all the data.

## Closing

That's it. I'm not sure if I convinced you this is promising — I think it is. But my hope is that it can be as **lightweight, portable, and local-first as DuckDB itself.**
