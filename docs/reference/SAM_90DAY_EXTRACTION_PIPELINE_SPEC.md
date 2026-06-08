# 90-Day SAM.gov Attachment — Text Extraction Pipeline (Architecture & Execution Spec)

**Status:** specification. **Do not implement from memory — implement from this document.** A secondary
engineering agent should be able to build the pipeline end-to-end from §2–§9 without architectural
ambiguity. This is Stage 4 of the GovCon substrate pipeline (Stage 3 = byte download, complete:
`SAM_ATTACHMENT_90DAY_HARVEST_AND_FORENSIC_RECORD.md`).

**Inputs (immutable, already on disk):**
- Bytes: `s3://data-sink/active/sam_attachment_blobs_90day/<resource_id>` — 126,901 CAS objects / 213.72 GB.
- Download ledger (read-only SoR): `s3://data-sink/active/sam_attachment_files_90day/` — one row per
  `resource_id`, columns incl. `file_name, mime_declared, mime_sniffed, content_length, size_downloaded,
  sha256, stored_uri, status, notice_id, solicitation_number, naics_code`.
- Filename taxonomy + contamination analysis: `SAM_90DAY_FILENAME_TAXONOMY_SIZING.md` (token-boundary
  regexes, lane sizing).

**Ground-truth population (measured 2026-06-08):** 114,901 text-extractable files (`mime_declared ∈
{pdf,docx,doc,txt}`); 12,000 non-text binaries; 11,195 high-confidence scope (token-boundary);
~6,355 safely-droppable boilerplate; remainder generic.

---

## 1. Architectural decisions (read before building)

| # | Decision | Rationale |
|---|---|---|
| **D1** | **State lives in a NEW append-only event ledger `sam_attachment_extraction_90day`, NOT as mutable flags on `sam_attachment_files_90day`.** | The download ledger is an immutable SoR. Lance has no cheap in-place cell update; `update`/`merge_insert` rewrite whole fragments. ~115k files × ~3 transitions = rewrite/version-bloat storm + single-writer-lock contention with the worker pool + lifecycle conflation. An append-only event log is O(append), idempotent, and audit-complete. "Current state" = a deterministic resolution view (D2). This **is** immutable state tracking in Lance — done correctly. |
| **D2** | **Current state = latest terminal event per `resource_id`** (resolve by `max(attempt)`, tie-break `completed_at`, terminal states win over intermediate). | Append-only logs need a resolution rule. Terminal states (§2.2) are absorbing; resume reads the resolution view and skips. |
| **D3** | **Parallel extract, serialized write.** N CPU-bound worker processes extract and *return results*; a single writer process batches all Lance appends. | Lance is single-writer-per-dataset; concurrent appends from a pool corrupt/conflict. Mirrors the proven Stage-3 collector pattern. |
| **D4** | **`pypdfium2` is the PDF engine only.** Dispatch by mime: PDF→pdfium; DOCX→`python-docx`; TXT→decode; DOC(legacy OLE)→LibreOffice-headless convert→pdfium, else `extract_failed`. pdfium is also the sole rasterizer for OCR (§5). | pypdfium2 is PDF-only; ~24k DOCX + ~1.4k DOC + ~0.4k TXT are in the population and need their own permissive-license extractors. Licenses: pdfium **BSD-3**, python-docx **MIT**, Tesseract **Apache-2.0** — clean profile, no PyMuPDF (AGPL/commercial). |
| **D5** | **Three sinks, separated by purpose.** Scope text → `govcon_scope_vectors_90day` (chunk grain). Pricing text → `govcon_pricing_90day` (kept out of the scope vector index to avoid polluting semantic scope search). Boilerplate → discarded (state only). | Clean retrieval surface. |
| **D6** | **Embedding is a defined but separate sub-stage (Phase 2.5).** Phase 2 writes *text chunks*; embedding + vector index run after, decoupled from the model choice. | Avoids coupling the CPU text pass to GPU/model availability; chunks are immediately useful. |
| **D7** | **OCR is an isolated asynchronous stage (Phase 3), separate process/run.** | Blast-radius containment: OCR is ~100× slower than text extraction; it must not block or crash the fast pass. |

If the operator overrides D1 and insists on flags on the download ledger: it is achievable via Lance
`merge_insert` upserts, but document the cost (full-fragment rewrite per batch, version compaction
required, no concurrent pool writes). Not recommended.

---

## 2. Data models

### 2.1 `sam_attachment_files_90day` — INPUT, read-only
Unchanged. Joined on `resource_id`. No schema change (per D1).

### 2.2 `sam_attachment_extraction_90day` — NEW, append-only state ledger (Lance v2.1)
One row per processing **event** (route, text_pass, ocr). Immutable appends.

| column | type | meaning |
|---|---|---|
| `resource_id` | string | join key → download ledger + CAS blob |
| `lane` | string | `L1_scope` · `L2_drop` · `L3_triage` · `non_text` (assigned at routing) |
| `stage` | string | `route` · `text_pass` · `ocr` · `embed` |
| `state` | string | see enum below |
| `extractor` | string | `pdfium` · `python_docx` · `txt` · `libreoffice+pdfium` · `pdfium+tesseract` |
| `n_pages` | int32 | page/section count (null for txt) |
| `text_chars` | int64 | extracted character count (post-normalize) |
| `text_yield_ratio` | double | `text_chars / max(1,n_pages)` — rasterization detector |
| `header_class` | string | first-page classification: `scope` · `pricing` · `boilerplate` · `unknown` |
| `n_chunks` | int32 | chunks emitted to a vector/pricing sink |
| `sha256_text` | string | sha256 of normalized extracted text (idempotency + dedup) |
| `attempt` | int32 | retry counter |
| `worker_id` | string | process identity (debug) |
| `run_id` | string | `extract-<UTC ISO>` batch id |
| `error` | string | failure detail |
| `started_at` / `completed_at` | timestamp(us, UTC) | event trail |

**`state` enum (terminal unless noted):**
- `routed` *(intermediate — L1/L3 awaiting text pass)*
- `dropped_boilerplate` *(L2; never opened)*
- `skipped_non_text` *(non-text mime; out of text scope)*
- `extracted_scope` *(text pass → scope vectors)*
- `extracted_pricing` *(text pass → pricing sink)*
- `dropped_content_noise` *(opened, header = boilerplate, text discarded)*
- `requires_ocr` *(intermediate — low text yield, handed to Phase 3)*
- `ocr_extracted_scope` / `ocr_extracted_pricing` / `ocr_dropped_noise` *(Phase 3 terminals)*
- `extract_failed` / `ocr_failed` *(terminal failure; re-attemptable on resume)*

Indices: `BTREE(resource_id)`, `BTREE(sha256_text)`, `BITMAP(lane, stage, state)`. Build once at end of each run.

**Resolution view (current state):** `row_number() OVER (PARTITION BY resource_id ORDER BY (terminal
desc), attempt desc, completed_at desc) = 1`, where `terminal` flags non-`routed`/`requires_ocr` states.

### 2.3 `govcon_scope_vectors_90day` — NEW, chunk grain (Lance v2.1)

| column | type | meaning |
|---|---|---|
| `chunk_id` | string | **deterministic**: `<resource_id>:<chunk_ix:04d>` (idempotent re-write key) |
| `resource_id` | string | parent file |
| `chunk_ix` | int32 | order within file |
| `text` | string | normalized chunk text |
| `char_len` | int32 | chunk length |
| `header_class` | string | parent classification (`scope`/`unknown`) |
| `notice_id` / `solicitation_number` / `naics_code` | string | carried from download ledger (filter/label) |
| `source_extractor` | string | `pdfium` · `pdfium+tesseract` · `python_docx` |
| `embedding` | fixed_size_list&lt;float32&gt;[D] | **nullable**; populated in Phase 2.5 (D parameterized) |
| `run_id` | string | provenance |
| `created_at` | timestamp(us, UTC) | |

Indices: `BTREE(resource_id)`; vector `IVF_PQ` on `embedding` built in Phase 2.5 after population.

### 2.4 `govcon_pricing_90day` — NEW (Lance v2.1)
Same shape minus `embedding`; holds `extracted_pricing` text (wage determinations, SCA/DBA, price
schedules) for structured downstream parsing. Not vector-indexed.

### 2.5 `ops.sam_extraction_90day_runs` — NEW Postgres run ledger
Per-run roll-up (mirrors `ops.sam_attachment_download_90day_runs`): `run_id, phase, lane,
files_in, extracted_scope, extracted_pricing, dropped_boilerplate, dropped_content_noise,
requires_ocr, extract_failed, total_chars, total_chunks, sustained_files_per_s, status, error,
started_at, completed_at`. Written on every terminal state.

---

## 3. Phase 1 — Routing Gate (pre-extraction, pure metadata)

**Goal:** assign each `resource_id` a lane and append a routing event. No blobs opened.
**Source:** download ledger `status='downloaded'` LEFT-ANTI-JOIN the resolution view (idempotent — route
only the not-yet-routed). **Matching uses token-boundary regex, NOT substring `ILIKE`** (per the
contamination finding — substring `%rep%`/`%rate%`/`%spec%` misroutes drawings, corporate docs, inspections).

**Lane assignment (precedence order — first match wins):**
1. `mime_declared NOT IN ('pdf','docx','doc','txt')` → **`non_text`**, state `skipped_non_text`.
2. `regexp_matches(lower(file_name), SCOPE_RX)` → **`L1_scope`**, state `routed`. *(scope wins over drop — never drop a filename-confirmed SOW)*
3. `regexp_matches(lower(file_name), DROP_RX)` → **`L2_drop`**, state `dropped_boilerplate` *(terminal; never opened)*.
4. else → **`L3_triage`**, state `routed`.

```
SCOPE_RX = (^|[^a-z])(sow|pws|p\.?w\.?s|s\.?o\.?w|soo|statement of work|performance work statement|
            scope of work|statement of objectives|specifications?|drawings?|salient charact)([^a-z]|$)
DROP_RX  = (^|[^a-z])(sf ?1449|sf ?30|sf ?33|sf ?18|ppq|past performance questionnaire|
            representations? and certifications?|cdrl)([^a-z]|$)
```
`DROP_RX` is deliberately conservative (explicit forms only — no bare `rep`/`cert`/`clause`). A borderline
file goes to L3 triage (opened, content-checked), never silently dropped.

**Expected lane sizes (re-derived at run time; ~):** L1 ≈ 11,195 · L2 ≈ 6,355 · L3 ≈ 97,351 · non_text ≈ 12,000.

**Output:** append routing events to `sam_attachment_extraction_90day`. L2/non_text rows are terminal here.

---

## 4. Phase 2 — High-speed text pass (multiprocess pool)

**Scope:** lanes `L1_scope` (priority, run first) then `L3_triage`. Architecture per D3 (parallel extract,
single writer).

### 4.1 Concurrency model
- `ProcessPoolExecutor(max_workers = cpu_count − 2)`. Work items = `resource_id` batches, sharded by
  `hash(resource_id)` for even distribution.
- Each worker is **pure compute + read I/O**: `get_object` blob from R2 (in-memory; spill to
  `SpooledTemporaryFile` if `content_length > 16 MB`) → mime-dispatch extract → classify → chunk →
  **return** a result struct `{resource_id, state, metrics, header_class, chunks[]}`. Workers never touch Lance.
- A **single writer** (main process) drains results from the pool, and every `K=500` results appends a
  batch to `sam_attachment_extraction_90day`, and every `M=2000` chunks appends to the sink datasets.
  Also writes a per-result line to a JSONL checkpoint (`/tmp/sam_extract_90day.ckpt.jsonl`).
- **Idempotent/resumable:** at start, load the resolution view ∪ checkpoint → done-set of terminal
  `resource_id`s → skip. `requires_ocr` and `extract_failed` are NOT in the skip-set (re-attemptable).

### 4.2 Per-file extraction logic (mime dispatch, D4)
- **pdf** → pdfium: open document; for each page extract text via the text page API; accumulate
  `text_chars`, `n_pages`. Release page/textpage/document handles explicitly (no leaks).
- **docx** → `python-docx`: concatenate paragraph + table text. `n_pages = null`.
- **txt** → decode (utf-8 → latin-1 fallback). `n_pages = null`.
- **doc (legacy OLE)** → LibreOffice headless `--convert-to pdf` (or `txt`) to a temp path → pdfium →
  cleanup; on convert failure → `extract_failed`.
- Any exception (corrupt file, decrypt-required, pdfium error) → `extract_failed` (caught per file; the
  pool never dies).

### 4.3 Threshold detection — rasterized/flattened-image PDFs → `requires_ocr`
Compute `text_yield_ratio = text_chars / max(1, n_pages)`. Flag `requires_ocr` when **both**:
- `text_yield_ratio < OCR_RATIO_THRESHOLD` (default **80** chars/page — a born-digital page rarely
  yields < 80 chars; a scanned page yields ~0), AND
- `text_chars < OCR_ABS_FLOOR` (default **n_pages × 80**, with a hard floor of 200) — prevents OCR'ing a
  legitimately terse text doc.
- Mixed docs: if `fraction_of_pages_below(40 chars) > 0.5` → also `requires_ocr`.
- `pdf` mime only (docx/txt are born-digital; never OCR). `requires_ocr` is intermediate → Phase 3.

### 4.4 Content triage (header classification on EXTRACTED TEXT — ground truth, not filename)
Run header regex on the normalized first-page text (first ~2,000 chars), case-insensitive:
- **scope** (`PERFORMANCE WORK STATEMENT|STATEMENT OF WORK|STATEMENT OF OBJECTIVES|SCOPE OF WORK|
  \bPWS\b|\bSOW\b|SPECIFICATIONS?|TECHNICAL REQUIREMENTS|SALIENT CHARACTERISTICS`) → `extracted_scope`.
- **pricing** (`WAGE DETERMINATION|SERVICE CONTRACT ACT|DAVIS[- ]BACON|\bSCA\b|\bWD\b|PRICE SCHEDULE|
  SCHEDULE OF PRICES|\bCLIN\b`) → `extracted_pricing`.
- **boilerplate** (`STANDARD FORM 1449|SOLICITATION/CONTRACT/ORDER FOR COMMERCIAL|AMENDMENT OF SOLICITATION|
  PAST PERFORMANCE QUESTIONNAIRE|REPRESENTATIONS AND CERTIFICATIONS`) → `dropped_content_noise` (text discarded).
- **unknown** (no header hit, but has real text) → **recall-biased default → `extracted_scope`** with
  `header_class='unknown'` (generic docs frequently embed scope inline; tag so downstream can filter).

**Lane interaction:**
- **L1_scope**: filename is high-confidence scope → bypass the boilerplate-drop branch (never drop), run
  threshold (may still → `requires_ocr`) → `extracted_scope` (or `requires_ocr`).
- **L3_triage**: full decision tree above.

### 4.5 Vector / pricing staging
- `extracted_scope` (and `unknown`) → chunk → append to `govcon_scope_vectors_90day`.
- `extracted_pricing` → chunk → append to `govcon_pricing_90day`.
- **Chunking:** normalize whitespace; split on paragraph/page boundaries; window **~1,200 chars with
  ~180-char overlap** (≈256-token target). `chunk_id = <resource_id>:<ix:04d>` (deterministic →
  re-extraction is idempotent via dedup-on-`chunk_id` / `merge_insert`). Carry `resource_id, notice_id,
  solicitation_number, naics_code, header_class, source_extractor`. `embedding` left null.

---

## 5. Phase 3 — Isolated OCR queue (asynchronous)

Separate script/run (D7). **Target:** resolution view `state='requires_ocr'`.

- **Engine:** Tesseract via `pytesseract` (Apache-2.0).
- **In-memory rasterization handoff (no intermediate images on disk):** for each page —
  `pdfium page.render(scale = TARGET_DPI/72)` → pdfium bitmap → `.to_pil()` (or numpy buffer) → hand the
  in-memory image object **directly** to `pytesseract.image_to_string(img, lang='eng', config='--psm 1')`
  → accumulate text. Explicitly `del` the bitmap/image and `page.close()` after each page (one page in
  RAM at a time per worker). `TARGET_DPI` default **300** (OCR quality vs. memory).
- **Post-OCR:** run the same §4.4 header triage on the OCR text → `ocr_extracted_scope` /
  `ocr_extracted_pricing` / `ocr_dropped_noise`; chunk scope/pricing to the same sinks
  (`source_extractor='pdfium+tesseract'`). OCR yielding near-zero text → `ocr_failed`.
- **Concurrency:** smaller pool (OCR is heavy) — `max_workers = max(2, (cpu_count−2)//2)`; same
  single-writer + checkpoint + idempotent-skip model as §4.1.

---

## 6. Phase 2.5 — Embedding + vector index (defined, deferred)

After scope chunks land: batch-read `govcon_scope_vectors_90day` where `embedding IS NULL` → embed with
the chosen model (dimension **D**, model TBD by operator; instruction/retrieval embedder) → `merge_insert`
on `chunk_id` to populate `embedding` → build Lance `IVF_PQ` vector index. Idempotent on `chunk_id`.
Decoupled from Phase 2 so the model/GPU choice never blocks text extraction.

---

## 7. Execution sequence (ordered)

0. **Create datasets** (idempotent): `sam_attachment_extraction_90day`, `govcon_scope_vectors_90day`,
   `govcon_pricing_90day` with explicit pyarrow schemas (§2); apply `ops.sam_extraction_90day_runs` DDL.
1. **Phase 1 routing** → append lane events; L2/non_text terminal. Verify lane counts vs. expected (§3).
2. **Phase 2 on L1_scope** (priority — fastest GTM value, ~11k files / 44.9 GB) → extraction events +
   scope chunks. Smoke first (`--max-files 200` to throwaway sinks), confirm yield + 0 crashes, then full.
3. **Phase 2 on L3_triage** (~97k files) → triage + extraction + chunks. Daemonize (`os.setsid`, per the
   Stage-3 pattern) — this is the long pass.
4. **Phase 3 OCR** on `requires_ocr` → OCR terminals + chunks.
5. **Phase 2.5 embedding** + `IVF_PQ` index (when model chosen).
6. **Reconcile** (§8). 7. **Acceptance** (§8). Write terminal `ops` rows per phase.

Daemonization, JSONL checkpoint, and resume-on-relaunch are mandatory for steps 3–4 (multi-hour),
reusing the Stage-3 proven mechanics.

---

## 8. Reconciliation & acceptance

**Reconcile (read-only, post-run):**
- **Coverage:** every `resource_id` with `mime_declared ∈ {pdf,docx,doc,txt}` in the download ledger
  (114,901) has exactly one terminal state in the resolution view. `0` rows in intermediate
  (`routed`/`requires_ocr`) after Phase 3.
- **Vector integrity:** `count(distinct resource_id)` in `govcon_scope_vectors_90day` ==
  `count(state IN ('extracted_scope','ocr_extracted_scope'))`. No orphan chunks (every chunk's
  `resource_id` has a matching terminal scope state); no missing (every scope file has ≥1 chunk).
- **Pricing integrity:** analogous for `govcon_pricing_90day`.

**Acceptance criteria (definition of done):**
- `extract_failed / 114,901 < 0.01` (each failure class logged with `error`).
- `requires_ocr` fully resolved by Phase 3 (0 unresolved).
- Reconcile coverage = 100%, vector orphans = missing = 0.
- `ops.sam_extraction_90day_runs` terminal rows present for every phase.
- Chunk-id uniqueness holds (no dup `chunk_id`).

---

## 9. Failure modes & blast-radius containment

| Risk | Containment |
|---|---|
| Corrupt/encrypted PDF kills a worker | per-file try/except → `extract_failed`; pool isolation |
| Concurrent Lance writers conflict | single-writer (D3); workers return data only |
| OCR slowness stalls fast pass | OCR is a separate stage/run (D7); never inline |
| State-flag rewrite storm | append-only event ledger (D1); no in-place updates |
| Re-run redundancy / partial crash | resolution-view ∪ checkpoint skip; deterministic `chunk_id` |
| Memory blowup on large/scanned PDFs | in-memory blob cap + spill; OCR renders one page at a time |
| Misrouting via filename substrings | token-boundary regex routing (§3) + content-truth triage (§4.4) |
| Pricing pollutes scope retrieval | separate `govcon_pricing_90day` sink (D5) |

---

## 10. Tunable parameters (defaults)

`POOL_WORKERS = cpu_count−2` · `OCR_WORKERS = max(2,(cpu_count−2)//2)` · `OCR_RATIO_THRESHOLD = 80
chars/page` · `OCR_ABS_FLOOR = max(200, n_pages×80)` · `MIXED_PAGE_FRACTION = 0.5` · `CHUNK_CHARS =
1200` · `CHUNK_OVERLAP = 180` · `LEDGER_FLUSH_K = 500` · `CHUNK_FLUSH_M = 2000` · `BLOB_SPILL = 16 MB` ·
`TARGET_DPI = 300` · `EMBED_DIM D = TBD`.

---

## 11. Implementation artifacts (to be built — names fixed here for the executor)
- `pipelines/sam_gov/sam_attachment_extract_90day.py` — Phase 1 routing + Phase 2 text pass (`--lane`, `--max-files`, `--daemon`, `--resume`).
- `pipelines/sam_gov/sam_attachment_ocr_90day.py` — Phase 3 OCR queue.
- `pipelines/sam_gov/sam_attachment_embed_90day.py` — Phase 2.5 embedding + index.
- `pipelines/sam_gov/sam_attachment_extract_reconcile_90day.py` — §8 reconcile.
- `pipelines/sam_gov/ops_sam_extraction_90day_runs.sql` — canonical ops DDL.
- Runtime: `doppler run --project core-x --config prd -- uv run --with pylance --with pyarrow --with pypdfium2 --with python-docx --with pytesseract --with boto3 --with 'duckdb>=1.5,<2' --with 'psycopg[binary]' python …` (Tesseract binary required on host for Phase 3).
