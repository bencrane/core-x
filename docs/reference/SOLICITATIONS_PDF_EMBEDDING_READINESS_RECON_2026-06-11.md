# Solicitations PDF Content & Embedding-Readiness Recon

**Read-only.** No OCR, no extraction, no vector generation, no Lance mutation, no
file move/copy. All numbers are live R2 ground truth probed **2026-06-11** with
`pylance 7.0.0 / duckdb 1.5.3 / pyarrow 24.0` over `core-x/prd` R2 creds. Reads were
`count_rows()` (+ filter pushdown), projection-only `to_table(columns=…)` of scalar
columns, and DuckDB aggregates. Embedding vectors were **never materialized into the
client** — null fraction was measured via `count_rows(filter="embedding IS NULL")`.

---

## 0. Verdict (read first)

The directive's premise — "raw `.pdf` binaries, OCR/extraction still ahead" — is
**stale by three stages**. The award-anchored 90-day track is **built through chunking**:
bytes downloaded, text extracted, content chunked into uniform ≤1,200-char windows with
keys denormalized to the prime award. **The only thing missing is the vectors.**

> **The `embedding` column exists (`fixed_size_list<float>[1024]`) and is 100% NULL on
> all 2,391,042 embeddable chunks. No embed pipeline exists in-repo.** The remaining work
> is a single, isolated embed-and-write motion, not a chunk-and-embed build.

Join spine is **zero-loss**: `contract_award_unique_key` resolves the winners manifest to
`contract_prime_txn` at **100.0%** (41,963/41,963). The keys are already denormalized onto
the chunk rows, so embedding→prime-award is a **direct single-key join** at query time.

The binding constraint is **coverage, not quality**: of 1,119,355 distinct prime awards in
the trailing-90-day window, only **9,730 (0.87%)** have embeddable text — a front-of-funnel
loss (82.7% of prime awards carry no `solicitation_identifier`), not an extraction defect.

---

## 1. Corpus Location & State

All datasets are LanceDB on R2 under `s3://data-sink/active/`. **State = post-extraction,
chunked, pre-embedding.** Both a raw-binary layer and an extracted-text layer exist.

| Layer | Dataset (R2 URI) | Rows | State |
|---|---|--:|---|
| **Raw bytes (CAS)** | `sam_attachment_blobs_90day/<resource_id>` | 126,901 obj | raw `.pdf`/`.docx`/`.xlsx`/… binaries, content-addressed |
| Raw download ledger | `sam_attachment_files_90day/` | 127,576 | SoR for byte layer (status, sha256, size_downloaded, stored_uri) |
| **Join-spine manifest** | `sam_opps_attachment_manifest_90day_winners/` | 155,183 | pointer layer + **pre-resolved award/sol/notice keys** |
| Scope gate | `sam_attachment_gtm_scope_90day/` | 126,901 | per-resource GTM in/out-of-scope decision |
| Content dedup | `sam_attachment_content_dedup_90day/` | 126,901 | sha256_raw → canonical map (120,887 canonical) |
| Text extraction event ledger | `sam_attachment_extraction_90day/` | 243,801 | per (resource, stage, attempt) — text_chars, yield, OCR routing |
| **Scope text chunks** | `govcon_scope_vectors_90day/` | **1,348,983** | chunked text + **`embedding[1024]` = 100% NULL** |
| **Unknown text chunks** | `govcon_unknown_90day/` | **1,042,059** | chunked text + **`embedding[1024]` = 100% NULL** |
| Pricing (structured) | `govcon_pricing_90day/` | 102,809 | extracted table cells (no embedding column) |
| Inner files (zip expand) | `sam_attachment_inner_files_90day/` | **ABSENT** | container expansion **never ran** |
| Prime feed (join target) | `usaspending_api_fresh/contract_prime_txn/` | 1,518,807 | txn grain; 1,247,391 distinct awards; 297 cols |

**Embedding-shaped, not embedded.** `govcon_scope_vectors_90day` and `govcon_unknown_90day`
each declare `embedding: fixed_size_list<item: float>[1024]`. Measured null count equals
row count exactly on both (1,348,983/1,348,983 and 1,042,059/1,042,059). The 1024-dim is a
schema reservation; a repo-wide grep for any embed writer (`text-embedding`/`voyage`/
`cohere`/`embed_`/model ref) returns **nothing** — the populating stage is unbuilt.

---

## 2. Volume Metrics

### 2.1 Raw byte layer (`sam_attachment_files_90day`)
| status | files | real GB |
|---|--:|--:|
| downloaded | 126,901 | **213.72** |
| gone (400/410) | 673 | — |
| failed | 2 | — |

Real file size (true `size_downloaded`, not corrupt manifest `size_bytes`): **mean 1.68 MB,
median 0.19 MB, p90 2.55 MB, max 431 MB.** Heavily right-skewed.

mime (sniffed, downloaded): `pdf` 88,638 · `zip` 34,828¹ · `ole` 1,939 · `jpg` 754 ·
`txt` 419 · `rtf` 136 · `png` 100.
¹ `zip` = magic-byte PK; conflates true `.zip` archives with OOXML (`.docx`/`.xlsx`).

### 2.2 Extraction outcomes (latest terminal state per distinct resource_id)
| terminal state | resources | → sink |
|---|--:|---|
| skipped_out_of_scope | 81,887 | (GTM scope gate — dominant filter) |
| extracted_unknown | 16,700 | govcon_unknown_90day |
| extracted_scope | 8,377 | govcon_scope_vectors_90day |
| dropped_duplicate | 6,014 | (sha256 dedup) |
| dropped_content_noise | 4,653 | (boilerplate header) |
| dropped_boilerplate | 4,614 | (filename rule) |
| extracted_pricing | 2,174 | govcon_pricing_90day |
| **requires_ocr** | **1,241** | (deferred — no text) |
| skipped_non_text | 1,170 | (pure image/binary) |
| routed | 55 | (stuck intermediate) |
| extract_failed | 16 | (re-attemptable) |

**27,251 files extracted to text/structured.** Extraction is robust (16 hard failures /
0.01%). Extractor mix (rows with text): `pdfium` 24,565 · `python_docx` 6,799 ·
`openpyxl` 2,302 · `txt` 110 · `libreoffice+*` 113.

### 2.3 Per-file extracted text length (informs chunking/context budget)
`text_chars` over files with text>0 (n=33,889): **mean 84,826 (~14.1k words), median 8,465
(~1.4k words)**, p10 585, p90 178,402, **p99 1,493,300, max 4,113,269** (~685k words).
Extreme right tail — a minority of mega-documents dominate total text volume.

### 2.4 Embeddable chunk volume (vectors pending)
| sink | chunks | embedding | distinct resources | distinct awards |
|---|--:|---|--:|--:|
| govcon_scope_vectors_90day | 1,348,983 | **100% NULL** | 8,377 | 4,890 |
| govcon_unknown_90day | 1,042,059 | **100% NULL** | 16,477 | 7,755 |
| **embeddable total** | **2,391,042** | **0 vectors** | — | 10,214 (union) |
| govcon_pricing_90day | 102,809 | (structured, n/a) | — | — |

Chunk sizing is **clean and uniform**: `char_len` mean ~1,191, median 1,196, **max 1,200,
p99 1,199, zero chunks > 8k**. The 4.1M-char monster files were already split safely. Chunks
per award: median 28, p90 648, **max 13,325** (one procurement's full attachment set).

---

## 3. Linkage Blueprint

### 3.1 Lineage (keys denormalized at every layer)
```
govcon_scope_vectors_90day (chunk grain)
  ├─ resource_id ───────────────► sam_attachment_files_90day.resource_id ──► blob CAS
  │                                  (stored_uri = .../sam_attachment_blobs_90day/<resource_id>)
  ├─ resource_id ───────────────► sam_opps_attachment_manifest_90day_winners.resource_id
  │                                  (download_url, notice_type, attachment_order, sol_norm,
  │                                   is_primary_target, award_keys[], award_count)
  ├─ notice_id ─────────────────► sam-gov-opps universe (notice metadata)
  ├─ solicitation_number ───────► Sol# spine (lifecycle link across notices)
  └─ contract_award_unique_key ─► contract_prime_txn.contract_award_unique_key   ◄── PRIME JOIN
```

The winners manifest already **picked the solicitation-bearing notice** (87% of citations
are `Solicitation` / `Combined Synopsis/Solicitation`; only 8,173 `Award Notice`), so the
chunks point at where the SOW/PWS actually lives — not the award announcement. Because
`contract_award_unique_key`, `notice_id`, and `solicitation_number` are denormalized onto
every chunk row, **no intermediate hop is needed at query time.**

### 3.2 Join verification (live)
- `contract_award_unique_key` is the canonical key on both sides. Manifest → prime
  resolution: **41,963 / 41,963 = 100.0%**, zero loss.
- Prime window column is **`last_modified_date`** (API-fresh bound): span **2026-03-09 →
  2026-06-07**, 1,518,807 txns → **1,247,391 distinct awards**, 1,119,355 in trailing-90d.
- `solicitation_identifier` is present on prime (216,069 awards / 17.3% carry it) but is the
  *secondary* path; prefer `contract_award_unique_key`. Sol# matching needs alnum
  normalization (`upper(); strip [^A-Z0-9]`) — the manifest's `sol_norm` already holds it.

### 3.3 Canonical SQL — solicitation chunk → prime award (90-day, award-grain)
```sql
-- contract_prime_txn is TXN grain; collapse to one row per award before joining,
-- else every modification fans the chunk out. Hold CUI off any external embed egress.
WITH prime_award AS (
  SELECT * FROM (
    SELECT *,
           row_number() OVER (PARTITION BY contract_award_unique_key
                              ORDER BY TRY_CAST(last_modified_date AS DATE) DESC,
                                       TRY_CAST(modification_number AS INTEGER) DESC) AS rn
    FROM contract_prime_txn
    WHERE TRY_CAST(last_modified_date AS DATE) >= CURRENT_DATE - INTERVAL 90 DAY
  ) WHERE rn = 1
)
SELECT  c.chunk_id, c.chunk_ix, c.text, c.char_len, c.header_class, c.content_marking,
        c.notice_id, c.solicitation_number,
        a.contract_award_unique_key, a.award_id_piid, a.recipient_uei, a.recipient_name,
        a.awarding_agency_name, a.naics_code, a.action_date, a.last_modified_date
FROM    govcon_scope_vectors_90day AS c
JOIN    prime_award                AS a USING (contract_award_unique_key)
WHERE   len(c.content_marking) = 0;   -- no marking detected; route marked rows to isolated/self-hosted embed
```

### 3.4 Coverage funnel (trailing-90-day prime awards)
| stage | distinct awards | rate |
|---|--:|--:|
| Prime awards (last_modified_date ≥ today−90d) | 1,119,355 | 100% |
| …carry a `solicitation_identifier` | 216,069 | 17.3% |
| …have ≥1 harvested attachment (winners manifest) | 40,418 | 3.6% |
| **…have extracted, embeddable text chunks** | **9,730** | **0.87%** |

Dominant loss is **Stage 1** (no Sol# on the award) then the GTM scope gate
(81,887 files dropped out-of-scope). Extraction itself is near-lossless.

---

## 4. Embedding Red Flags

1. **Vectors are 100% NULL and there is no embed code.** 2,391,042 chunks carry an empty
   `fixed_size_list<float>[1024]`; no in-repo pipeline writes it. *Standard "chunk-and-embed"
   assumes chunking is the work — here chunking is done and embedding is the entire remaining
   build.* (1024-dim is a reservation; the model is unfixed — pin it before first write so the
   column dim and the model agree.)

2. **Control-marking egress.** **42,307 chunks** carry a detected control marking
   (`len(content_marking) > 0`; scope 22,271 · unknown 19,244 · pricing 792). Sending these to a
   third-party embedding API risks a Controlled-Unclassified-Information spill. The `content_marking`
   list is the gate — it must be honored: embed marked rows on a self-hosted model or exclude them from
   external egress. (Field renamed `sensitivity`→`content_marking` and retyped `string`→`list<string>`
   on 2026-06-12; legacy detected rows carry the sentinel `['unspecified']` — the specific caveat was
   not persisted pre-rename and was deliberately not backfilled. Go-forward extraction captures the
   actual caveats, e.g. `['itar','dist_stmt_c']`. `[]` = none detected, which is **not** proof of
   public — see flags 4 and the 2,000-char/­scan blind spots.)

3. **Container expansion never ran.** Zero `expanded_container` events, zero non-null
   `parent_resource_id` across 243,801 events, and `sam_attachment_inner_files_90day` is
   absent. True multi-file `.zip` bundles were not unpacked, so their nested SOW/PWS docs are
   missing from the text layer. **Bounded:** OOXML (`.docx`/`.xlsx`, which also sniff as zip)
   *were* extracted directly (the `python_docx`/`openpyxl` counts), and only **55** resources
   sit stuck at intermediate `routed` — this is a real but small content hole, not 34,828 lost
   archives.

4. **Scanned / image-only PDFs have no text.** 1,241 resources terminal `requires_ocr`, plus
   1,170 `skipped_non_text` and 754 `jpg` / 100 `png` attachments. These contribute zero
   vectors until OCR runs (explicitly out of scope here). Exclude from the embed worklist or
   they pollute it with empty text.

5. **Extreme document-length skew.** Per-file text to **4.11M chars** (p99 1.49M); per-award
   to **13,325 chunks** (median 28). Chunking already tamed this (uniform ≤1,200-char windows),
   so the embed step is safe — but batch sizing and any per-award rerank/retrieval must expect a
   single award to fan out to ~13k vectors.

6. **Coverage is the real ceiling (planning fact, not a bug).** Only 0.87% of 90-day prime
   awards reach embeddable text. The embed corpus is GTM-scope-gated and Sol#-gated, not the
   full award set. Any downstream "every recent award has scope text" assumption is false by
   two orders of magnitude.

---

## 5. Reproduction
Probe stack: `doppler run -- .venv/bin/python` (creds `R2_ACCESS_KEY_ID`,
`R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT` from `core-x/prd`). Pattern:
```python
import os, lance, duckdb
so = {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
      "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
      "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}
d = lance.dataset("s3://data-sink/active/govcon_scope_vectors_90day/", storage_options=so)
d.count_rows(), d.count_rows(filter="embedding IS NULL")   # → (1348983, 1348983)
```
Disposition / text-length / join / content_marking queries: project only scalar columns via
`to_table(columns=[…])` into DuckDB; never pull `embedding`. Join proof in §3.2 is
`SELECT count(DISTINCT w.contract_award_unique_key) … LEFT JOIN prime USING(contract_award_unique_key)`.
