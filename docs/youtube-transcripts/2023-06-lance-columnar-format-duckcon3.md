# Bringing AI to DuckDB with Lance Columnar Format for Multi-Modal AI — DuckCon #3 (San Francisco)

> ⚠️ **DATED SOURCE — JUNE 2023.** This transcript is from DuckCon #3 (San Francisco), **June 2023**. It reflects the state of the Lance format, LanceDB, DuckDB, and the surrounding AI tooling at that time — an early, forward-looking pitch, not current documentation. It is therefore **at minimum incomplete, and quite possibly inaccurate today**: APIs, type support, performance claims, DuckDB/Arrow interop, and roadmap items have almost certainly changed. Treat it as **coarse historical context and framing only**. Any agent relying on this file **must verify every concrete claim against more complete and more recent sources** before acting on it.

**Event:** DuckCon #3, San Francisco (June 2023).
**Topic:** Lance columnar format / LanceDB for multi-modal AI in DuckDB.

*Video transcript. Cleaned from an auto-generated transcript ("land CB" → LanceDB, "ductdb" → DuckDB, "Nessa" → nested, wording lightly smoothed; meaning preserved).*

---

I have to admit that when I saw the lineup I nearly decided to cancel, because I used to work with Josh for four years at Cloudera and always hated going after him. Todd — before Julia's talk — encouraged me to keep going. And once upon a time I was one of the early co-authors of the pandas library, so I'm glad to hear the kind words.

Now I'm working on the **Lance columnar format** and **LanceDB**. Lloyd's talk went the furthest back in time, Josh's talk was certainly the funniest — I'm going to claim to have the craziest talk: how to build a full AI tooling stack using DuckDB, or how to bring that into the DuckDB system. So picture me as the *It's Always Sunny* conspiracy-theory meme.

## The problem with ML / MLOps / AI data

The problem I've seen isn't just that your data is not rectangular — the data is really **irregular**. You have annotations, labels, and bounding boxes; you have lots of deeply nested data; and you also have really large blobs, up to dozens of megabytes or sometimes gigabytes when you have 3D scans of things.

So what ends up happening in data lakes for AI-heavy shops: you have your single-source-of-truth format (maybe Parquet), but then you have **another copy** that stores something like TFRecords for training, and then **yet another copy** for debugging or evals or some other thing. This creates a lot of complexity for the tooling and pipelines you build on top of it, and it really blows up the cost of data storage and compute.

## Lance columnar format

That's what we try to solve with the Lance columnar format, which we're working on with the LanceDB team. It's a high-performance new columnar format optimized for AI, designed to **unify storage** across embeddings, text, metadata, tabular data, images — everything you can think of. Out of the box it's compatible with our favorite tools like DuckDB, pandas, Polars, PyTorch, and more.

It comes with **zero-copy schema evolution and versioning**, so you can always roll back to the previous good state if your model eval shows bad results. And because of the optimizations we've done in the data format, we can achieve **several orders of magnitude faster performance than Parquet on random access**, and reduce overall training time by at least **2–3×** with faster shuffling, faster filtering, and faster data loading.

## The rest of the AI tool chain in DuckDB

But the data format itself is not enough to have a full-fledged AI tooling chain in DuckDB. You need the models to run in SQL, you want UDFs, and you want data exploration tools. So:

- A **data layer** — hopefully Lance format.
- **DuckDB extensions** for PyTorch or TensorFlow.
- **Scanners for specialized data** — e.g. using FFmpeg or OpenCV to build a frame scanner for videos.
- **UDFs on top** to do image transformations like crop and rotation — all pluggable with CUDA integration.

All of that is possible because DuckDB is written in C++ and works with all of those tools. I put a toy version of all that together in a blog post called "**Peking Duck**" (with two e's — "peeking," not hosting).

## Types, types, types

The key to a successful business is location, location, location. I think the key to a successful data system is **types, types, types**. Arrow and DuckDB types are maybe **80% interoperable**; unfortunately, AI falls into that missing 20%. It comes in roughly three buckets:

1. **Nested types** — for annotations, bounding boxes, labels.
2. **Extension types** — for images, embeddings, videos, point clouds, and things like that.
3. **ML-specific types** — like `bf16`, which should be fairly easy to add.

The problems are going to be how to make sure all these different layers agree with each other and can talk to each other.

Two other small problems in the **push-down layer**: DuckDB pushes down to Arrow using the PyArrow compute expressions, but that's not a standard across different Arrow implementations. Lance format uses Rust, for example — we use DataFusion to handle predicate push-downs — so there are some pain points there. Possibly **Substrait** can serve as a long-term interface, but who knows.

## Wrap-up

I have about ten seconds left. You can find us on GitHub — if you don't like it, tell us how we suck; if you like it (and don't hate it), give us a star please. I'm really focused on the data layer, but as you can see there's lots to build for a full AI tool chain, so if you're interested, please — I'd love to collaborate on it with you. Thank you.
