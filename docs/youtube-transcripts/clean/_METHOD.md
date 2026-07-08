# Cleaning method — faithful transcript layer (`clean/`)

This folder holds the **faithful cleanup** layer of the transcript corpus. Three layers exist per talk:

| Layer | Path | What it is |
|---|---|---|
| Verbatim | `raw/<name>.raw.txt` | Untouched ASR + timestamps. System of record. |
| **Faithful (this folder)** | `clean/<name>.clean.md` | **Every word of substance preserved.** ASR errors + filler fixed only. Reads as natural language. **No editorializing.** |
| Editorialized | `<name>.md` | Restructured/summarized editorial rewrite (readable digest). |

## The one hard rule: KEEP EVERY WORD OF SUBSTANCE

The faithful layer is a **near-verbatim transcription cleanup**, NOT a rewrite and NOT a summary. Output length should be close to the raw's spoken content. If you find yourself shortening, condensing, or "improving" phrasing, STOP — that's the editorialized layer's job, not this one.

**Do (mechanical cleanup only):**
1. **Correct ASR mistranscriptions** — proper names, product names, technical terms, and words the ASR clearly garbled (see glossary below, and use context for others).
2. **Remove pure filler and disfluencies only:** "um", "uh", "you know", filler "like", filler "kind of"/"sort of", tic "right?", false starts, and immediate stutters/word-repetitions ("the the", "we we"). Nothing else.
3. **Fix punctuation, capitalization, sentence boundaries, and obvious grammar** (verb agreement, tense, dropped articles) so it reads as correct, natural English.
4. **Add light paragraph breaks** for readability. For interviews/Q&A/multi-speaker talks, keep or add speaker labels / `Q:`/`A:` **only where the raw already indicates a speaker or question turn** — do not invent structure.
5. **Drop the `HH:MM:SS - HH:MM:SS` timestamp lines** (they live in the raw layer).

**Do NOT (this is editorializing — forbidden here):**
- Do NOT summarize, condense, or shorten. Keep every sentence, aside, tangent, joke, repetition-for-emphasis, demo narration, and Q&A exchange.
- Do NOT paraphrase or "improve" word choices. Preserve the speaker's actual phrasing.
- Do NOT reorder or regroup content. Keep the original order exactly.
- Do NOT add thematic section headings that restructure the talk, TL;DRs, bullet-point summaries, commentary, or interpretation. (A single title line at the top is fine; light `##` headers are allowed ONLY if they mirror something the speaker explicitly announces, e.g. "let's get into the demo" — when in doubt, use plain paragraphs.)
- Do NOT bullet-ize prose that was spoken as prose.
- Keep all numbers, code, commands, and claims exactly as spoken.

## Worked example

**Raw ASR:**
> So DuckDB, right? I've spent the last decade or so working on DuckDB together with many other wonderful people. And since we're always looking for superlatives, you know, it's the friendliest SQL database. I think that's fair, right? friendliest.

**✅ Faithful (this layer):**
> So, DuckDB. I've spent the last decade or so working on DuckDB together with many other wonderful people. And since we're always looking for superlatives, it's the friendliest SQL database. I think that's fair — friendliest.

*(Removed "right?", "you know"; fixed capitalization/punctuation. Kept every substantive word, including the trailing "I think that's fair — friendliest.")*

**❌ Editorialized (WRONG for this layer):**
> DuckDB is the friendliest SQL database.

*(Summarized — dropped words and voice. This belongs only in the `<name>.md` layer.)*

## ASR correction glossary (non-exhaustive; use context for the rest)

- **Quack** — not: clock, cork, quark, quirk, Quaid
- **DuckDB** — not: ductb, DuctTb, DTB, DDB, DUTDB, ductibi, ductivity, "Docker TV", "Dr B", "Dr DB", "Doug TB", "dact db", "duct tb"
- **DuckLake** — not: "duck leg", "duck lake", "DuckDB Lake", Douglake, "dark lake"
- **DuckCon** — not: "Docker Con", Duckon
- **DuckDB Labs** / **DuckLabs** — keep as the speaker says (State-of-the-Duck announces a rename to "DuckLabs"; elsewhere it's "DuckDB Labs")
- **MotherDuck** — not: Moderndesk, "Mod AI", "mother duck"
- **Postgres / PostgreSQL** — not: phosphorus, phosphorous, poscess, postgorous, proscars
- **Parquet** — not: paret, parque, "park files"
- **SQLite** — not: "SQL light", "sequel light"
- **Tantivy** — not: "TenTen TV", "Tenny TV", Tenny
- **httpfs** — not: hpfs · **WASM / WebAssembly**
- **Arrow Flight SQL**, **Arrow**, **ADBC** — not: "error flight", "narrow flight"
- **Iceberg**, **Delta Lake**, **Hudi**, **GizmoSQL / GizmoData**, **pg_duckdb**, **EleDucken**
- **MVCC**, **WAL** (write-ahead log), **Zstandard/zstd**, **Snappy**, **TPC-H**, **lineitem**
- **cardinality** — not: cardality · **struct** — not: strruct · **vectorized**
- **Tera** / **MiniJinja** (Query Farm templating extensions) — not: terra, "mini ginga"
- **Stochastic** — not: stocastic · **ccache** — not: ccash · **vcpkg**, **CMake**, **Ninja**, **PEG parser**
- **Marimo**, **Pyodide**, **Mosaic**, **SQLRooms**, **Foursquare**, **Dives** (MotherDuck viz)
- **CWI** · Names: **Hannes Mühleisen**, **Mark Raasveldt**, **Gabor Sarnyas**, **Sam Ansmink**, **Joe Reis**, **Ramona C. Truta**, **Pedro**, **Carlo**, **Peter Boncz**, **Maya**, **Tom**

## Output format

Start each file with a minimal header, then the cleaned prose:

```
# <Title> — faithful transcript

*Faithful cleanup of the ASR transcript: every word of substance preserved; only filler removed and clear mistranscriptions corrected. Not editorialized. Verbatim source: `raw/<name>.raw.txt`.*

**Published:** <date>.  **Source:** <url or venue>

---

<cleaned prose — full length, original order>
```

Get the title / date / source from the header of the sibling editorialized `<name>.md` (metadata only — do NOT use its body as a model; its body is editorialized).
