# Raw transcripts — verbatim ASR, uncorrected

These `*.raw.txt` files are the **verbatim auto-generated (ASR) transcripts** as exported, with original timestamps and **no edits**. They are the **system of record** for the `docs/youtube-transcripts/` corpus.

## Pairing

Each raw file shares its basename with the cleaned Markdown one level up:

```
docs/youtube-transcripts/<name>.md          ← cleaned, editorial rewrite (default read)
docs/youtube-transcripts/raw/<name>.raw.txt  ← verbatim ASR (audit / verify fallback)
```

## How to use these (for agents)

- **Read the cleaned `.md` first.** It corrects entity errors (`clock`→Quack, `phosphorus`→Postgres, `Docker Con`→DuckCon, `ductibi`→DuckDB, etc.), reconstructs code, and structures the content.
- **Drop to the `.raw.txt` only to verify** — exact wording, a precise number, a disputed claim, or a "did they really say X." The raw files keep timestamps for cross-referencing the source video.

## Important caveats about the raw text

- It is **heavily ASR-corrupted**: product names, code, and technical terms are frequently mistranscribed. Do **not** treat the raw wording of any noun/term as authoritative — the cleaned version is the corrected reading.
- The cleaned `.md` files are **editorial rewrites**, not verbatim: grammar was rebuilt, filler removed, and some dense Q&A/demo sections were summarized. Where cleaned and raw disagree on wording, raw is the literal record and cleaned is the intended meaning.

## Not included here

The three files in `docs/batches/` (the Medium article, the DuckDB blog post, and the Definite blog post) are **written sources pasted verbatim**, not ASR transcripts — their `.md` reproduces the original text, so there is no separate raw layer.
