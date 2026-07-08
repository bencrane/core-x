# docs/ — file index

What lives under `docs/`, organized by bucket, with the **type** of every file. Two buckets:

- **`youtube-transcripts/`** — 17 talks/videos, each stored in **three layers** (verbatim → faithful → editorialized).
- **`batches/`** — 3 written web sources (blog posts / article), reproduced as Markdown. **Single layer** (they were pasted text, not ASR, so there is no raw/clean split).

```
docs/
├── INDEX.md                         ← this file
├── batches/                         ← written sources (3 files, 1 layer each)
│   └── <name>.md
└── youtube-transcripts/             ← talks/videos (17 items, 3 layers each)
    ├── <name>.md                    ← LAYER 3: editorialized digest
    ├── clean/
    │   ├── <name>.clean.md          ← LAYER 2: faithful cleanup
    │   └── _METHOD.md               ← spec the faithful cleanup followed
    └── raw/
        ├── <name>.raw.txt           ← LAYER 1: verbatim ASR (system of record)
        └── README.md                ← explains the raw layer
```

---

## Bucket 1 — `youtube-transcripts/` (the 3-layer corpus)

Every talk has the **same basename** across three files. Pick the layer by need:

| Layer | Path pattern | Type | Fidelity | Read it when |
|---|---|---|---|---|
| **1 — Verbatim** | `raw/<name>.raw.txt` | Untouched ASR export **with timestamps** | Literal record; **ASR-corrupted** (terms/code mistranscribed) | You need exact wording, a precise number, or to cross-ref the video by timestamp |
| **2 — Faithful** | `clean/<name>.clean.md` | ASR errors + filler fixed, natural prose, **every word of substance kept, original order** | High; near-verbatim (≈87–106% of raw word count) | **Default read.** You want the real content correctly readable |
| **3 — Editorialized** | `<name>.md` | Restructured/summarized digest with headings, bullets, reordering | Meaning-faithful but **rewritten & lossy** | You want a fast skim/overview, not the speaker's actual words |

Support files: `clean/_METHOD.md` (the exact cleaning rules + ASR glossary the Layer-2 pass followed) and `raw/README.md` (raw-layer usage notes).

### The 17 talks (each = `.md` + `clean/*.clean.md` + `raw/*.raw.txt`)

| Date | Basename | What it is |
|---|---|---|
| 2023-06 | `2023-06-lance-columnar-format-duckcon3` | DuckCon #3 talk — Lance columnar format for multi-modal AI. ⚠️ **Dated June 2023 source**; file carries a stale-content caveat |
| 2024-08-24 | `2024-08-24_duckdb-function-chaining-the-simpler-sql` | YouTube tutorial — function chaining with the dot operator |
| 2025-09-24 | `2025-09-24_indexes-are-not-all-you-need-common-duckdb-pitfalls` | Talk (Tanya, DuckDB Labs) — index pitfalls & query profiling |
| 2026-01-30 | `2026-01-30_duckdb-extension-development-workshop-part-1` | Hands-on workshop (Query Farm) — building a scalar function |
| 2026-01-30 | `2026-01-30_duckdb-extension-development-workshop-part-2` | Hands-on workshop (Query Farm) — table functions, threading, publishing |
| 2026-02-02 | `2026-02-02_duckdb-extensions-the-past-the-present-and-the-future` | Talk (Sam Ansmink, DuckDB Labs) — extension ecosystem + stable C API |
| 2026-04-28 | `2026-04-28_the-ducklake-lakehouse-from-getting-started-to-going-fast` | MotherDuck webinar (Gerald + Alex) — DuckLake intro, demo, tuning, Q&A |
| 2026-05-12 | `2026-05-12-quack-ai-council-announcement-talk` | DuckDB Labs announcement talk (AI Council) — Quack reveal + demo |
| 2026-05-13 | `2026-05-13_duckdb-quack-motherduck-video-transcript` | MotherDuck video — "Quack explained" |
| 2026-05-27 | `2026-05-27_quack-hannes-muhleisen-interview` | Interview — Hannes Mühleisen × Joe Reis lunch-and-learn |
| 2026-05-28 | `2026-05-28_duckdb-not-quack-science-ubuntu-summit` | Talk (Gabor Sarnyas) — Ubuntu Summit 26.04 |
| 2026-06-25 | `2026-06-25_ai-generated-data-pipelines-keep-breaking` | YouTube — why AI-generated data pipelines break (DuckDB/MotherDuck) |
| 2026-07-07 | `2026-07-07_a-deep-dive-into-ducklakes-sorted-tables-feature` | YouTube (MotherDuck-sponsored) — DuckLake sorted tables |
| 2026-07-11 | `2026-07-11-grep-your-lakehouse-duckcon7` | DuckCon #7 talk (Sylvain Utard, Alter Table) — search-first retrieval |
| 2026-07-11 | `2026-07-11_build-a-local-data-lakehouse-with-duckdb-and-ducklake` | YouTube how-to ("DataGuy") — local DuckLake lakehouse |
| 2026-07-11 | `2026-07-11_duckcon-7-building-local-first-analytics-apps-with-sqlrooms` | DuckCon #7 talk — SQLRooms |
| 2026-07-11 | `2026-07-11_duckcon-7-state-of-the-duck` | DuckCon #7 keynote (Hannes + Mark) — State of the Duck |

*Naming note:* most basenames use `YYYY-MM-DD_slug`; three older ones use `YYYY-MM-DD-slug` (hyphen) — cosmetic only, the three layers still share the exact basename.

---

## Bucket 2 — `batches/` (written sources, single layer)

Pasted web text reproduced as Markdown. These are **not** transcripts — the `.md` closely reproduces the original article, so there is no raw/clean/editorialized split.

| Date | File | Type |
|---|---|---|
| 2026-05-12 | `2026-05-12-duckdb-quack-multiple-writers.md` | Medium article (Siddique Ahmad) — Quack overview |
| 2026-05-12 | `2026-05-12-quack-remote-protocol-blog.md` | **DuckDB official blog post** — the canonical Quack announcement |
| 2026-05-17 | `2026-05-17-duckdb-quack-as-ducklake-catalog.md` | Definite blog (Mike Ritchie) — Quack as the DuckLake catalog |

---

## Totals

- **17** talks × 3 layers = **51** transcript files, + `_METHOD.md` + `raw/README.md`.
- **3** written-source articles in `batches/`.
- **1** index (this file).

## For a future agent

1. **Default to Layer 2** (`clean/<name>.clean.md`) — correct content, correct terms, readable.
2. **Drop to Layer 1** (`raw/<name>.raw.txt`) only to verify exact wording/numbers or to find a timestamp. Do **not** trust raw wording of product names, code, or technical terms — those are ASR-corrupted; the clean layer has the corrections.
3. **Layer 3** (`<name>.md`) is a convenience digest — do not cite it as the speaker's words.
4. `batches/` files are near-verbatim reproductions of published articles; cite normally.
5. **Extracted knowledge lives in [`canonical/`](canonical/README.md)** — this corpus has been folded into the canonical Lance/DuckDB reference library (new files `canonical/duckdb/14`–`17`, plus additions to `canonical/duckdb/00/10/11/12` and `canonical/lance/10`), with every folded fact citing its transcript here. For **facts**, read canonical first; come back here for the speaker's full words and context.
