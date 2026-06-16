# 90-Day SAM.gov Attachment — Text & Structured Extraction Pipeline (Architecture & Execution Spec, v2)

> **Historical-name note (2026-06-16):** `90day` is the original rolling-window name from when the
> corpus was a trailing 90-day slice. The corpus is now **cumulative** — the Subaward Scope-Enrichment
> lift folded in older solicitations and the chunk sinks accumulate — so the `90day` suffix on every
> `*_90day` dataset/module is a **historical artifact, NOT a freshness guarantee**. A physical rename
> was scoped and deliberately not executed (it is a supervised, prod-affecting cutover of the deployed
> `gtm-mcp` gateway); see `docs/plans/SAM_GOVCON_90DAY_RENAME_MIGRATION.md` for the decision record.

**Status:** specification — **CANONICAL, supersedes v1** (`SAM_90DAY_EXTRACTION_PIPELINE_SPEC.md`).
**Implement from THIS document.** A secondary engineering agent should build the pipeline end-to-end
from §3–§13 with no architectural ambiguity. **Do not write code from memory; follow the spec.**

v2 incorporates the verified remediations from `SAM_90DAY_EXTRACTION_PIPELINE_SPEC_ADVERSARIAL_REVIEW.md`
(adversarial multi-agent review `wf_4be318e0-c69`: 33 surviving findings, 7 refuted). The v1 core
architecture (append-only event ledger, resolution view, parallel-extract/single-writer, purpose-separated
sinks) survived review and is retained. Every change below cites the review finding it closes.

**Inputs (immutable, on disk):**
- Bytes: `s3://data-sink/active/sam_attachment_blobs_90day/<resource_id>` — 126,901 CAS objects / 213.7 GB.
- Download ledger (read-only SoR): `s3://data-sink/active/sam_attachment_files_90day/` — columns incl.
  `file_name, mime_declared, mime_sniffed, mime_match, content_length, size_downloaded, sha256, stored_uri,
  status, notice_id, solicitation_number, naics_code`.
- Winners manifest (read-only): `s3://data-sink/active/sam_opps_attachment_manifest_90day_winners/` — carries
  `resource_id` (unique, 155,183 distinct), `contract_award_unique_key`, `award_keys[]`. Join key = `resource_id`.

**VERIFIED GROUND TRUTH (live ledger, 2026-06-08):** 126,901 downloaded / 213.7 GB.
mime_declared: pdf 88,669 (184.67 GB); docx 24,382 (3.96); xlsx 8,909 (3.09); doc 1,437 (0.39);
zip 1,379 (16.46); jpg 645; xls 510; txt 413; pptx 173; jpeg 109; rtf 104; png 101; mp4 29 (1.98);
mov 17; ppt 10; xlsm 4; bmp 3; accdb 3; xlsb 3; csv 1.
Spreadsheets (xlsx+xls+xlsm+xlsb) = 9,426 / 3.21 GB. zip = 1,379 / 16.46 GB.
sha256 dedup: 120,887 distinct of 126,901 → **5,627 byte-identical duplicate files (4.9%) / 5.48 GB**
(`.doc` decomposes to ~1,395 OLE / 32 rtf / 9 zip / 1 pdf).

---

## 1. Changelog v1 → v2 (each maps to a review finding)

| # | Change | Closes |
|---|---|---|
| C1 | Pin embedding `D=1024` + `metric=cosine` + write-time L2-normalize; Phase 4 (was 2.5) reclassified **REQUIRED**; local self-hosted embedder (CUI). `chunk_id` stays UNINDEXED until Phase 4. | #17,#18,#24,#21 |
| C2 | `.doc` lane = **serialized**, outside the pool; LibreOffice declared in §15 + `SOFFICE_BIN`; startup fail-fast assert; post-convert output check; rtf/zip/pdf sniff pre-pass. | #8,#7,#27 |
| C3 | boto3 client **per worker process** via pool `initializer`; force `spawn`; daemonize-before-pool; `max_pool_connections=4`. | #5 |
| C4 | OCR → **`--psm 3`**; `OCR_PSM` tunable; pin `eng.traineddata` (osd only if OSD used). | #13 |
| C5 | DOCX traversal via `iter_inner_content()` + `id(cell._tc)` merged-span dedup + delimited rows; defined docx first-page surrogate. | #6 |
| C6 | New **`L4_structured`** spreadsheet lane (openpyxl/LibreOffice, cell-delimited); **Phase 1.5 zip-expansion**; **pdfplumber** table extraction for pricing-classed PDF pages; states `extracted_spreadsheet`/`expanded_container`. | #2,#4 |
| C7 | `unknown` header class → separate **`govcon_unknown_90day`** sink + labor-lexicon admission gate; mandatory `where header_class='scope'` retrieval prefilter + `BITMAP(header_class)`. | #1 |
| C8 | New **Phase 5 structured field extraction** → `govcon_labor_demand_90day` (labor_category/headcount/clearance/PoP/place/wage), per-document, bridged to award. | #3 |
| C9 | **Content-canonical dedup pre-pass** (raw sha256) + `sha256_text` chunk dedup + fan-out map `sam_attachment_content_dedup_90day`. | #9 |
| C10 | **Control-marking detector** in triage → captures matched caveats into the `content_marking list<string>` column on chunks (gate for any external/egress path; `[]`=none detected). | #10 |
| C11 | Phase 2 chunk write = **`merge_insert` on `chunk_id`**; checkpoint-after-chunk-durability ordering; §12 per-`resource_id` `n_chunks==COUNT(*)` assertion. | #19,#20 |
| C12 | **Compaction** (`compact_files` + `cleanup_old_versions`) before index build; corrected Lance rationale in D1/D3/§13. | #22,#23,#11 |
| C13 | pdfium opened **from spilled file path** (not re-read bytes) + >50 MB semaphore; sniff-aware dispatch override; charset-normalizer txt decode; pdfium stream-order caveat + optional reading-order flag. | #12,#14,#15,#25 |
| C14 | Inline `contract_award_unique_key` on the three text sinks (join winners manifest on `resource_id`); §10 throughput model; smoke-derived `requires_ocr` estimate + `OCR_MAX_PAGES`. | #14,#26,#24 |

---

## 2. Architectural decisions

| # | Decision | Rationale |
|---|---|---|
| **D1** | State in a NEW append-only event ledger `sam_attachment_extraction_90day` (NOT mutable flags on the download SoR). | An append-only log is O(append), idempotent, audit-complete; "current state" = resolution view (D2). **(Corrected mechanic, #23):** per-cell Lance `update` accumulates deletion vectors + tiny new fragments (compaction debt); `merge_insert` rewrites only matched columns — neither is a full-fragment rewrite. The event log still wins: it avoids the read-modify-write commit, per-mutation single-writer serialization, and compaction debt of per-transition mutation, and keeps the download SoR untouched. |
| **D2** | Current state = latest terminal event per `resource_id` (resolve by terminal-first, then `max(attempt)`, then `completed_at`). | Resume reads the resolution view and skips terminal `resource_id`s. `extract_failed`/`ocr_failed`/`requires_ocr` are re-attemptable (not skipped). |
| **D3** | Parallel extract, **single committing process** per dataset. | **(Corrected, #11):** Lance appends are individually conflict-compatible; the real constraint is that the pipeline commits directly to R2 with **no `commit_lock`/external manifest store**, so concurrent committers to R2 race. One committing process ⇒ zero concurrent committers ⇒ safe. If true write parallelism is ever needed, use `lance.write_dataset(commit_lock=...)`, not abandoning Lance. |
| **D4** | `pypdfium2` is the **PDF text engine only**; mime dispatch handles the rest (D8). pdfium is also the sole OCR rasterizer. | pdfium is PDF-only. Licenses (re-verified): pdfium **BSD-3**, python-docx **MIT**, openpyxl **MIT**, xlrd **BSD**, pdfplumber **MIT**, striprtf **BSD-3**, charset-normalizer **MIT**, pytesseract+Tesseract **Apache-2.0**. **No PyMuPDF (AGPL), no antiword (GPL).** |
| **D5** | Purpose-separated sinks: scope→`govcon_scope_vectors_90day`, pricing→`govcon_pricing_90day`, **unknown→`govcon_unknown_90day`** (C7), boilerplate discarded, sensitive→quarantine (C10). | Clean, un-polluted retrieval surface. |
| **D6** | Embedding is **Phase 4, REQUIRED** (no longer "deferred"), run after text chunks land; decoupled only in execution order. | Text chunks are immediately useful; embedding is the GTM-completing step, not optional. |
| **D7** | OCR is an isolated asynchronous stage (Phase 3), separate process/run. | Blast-radius containment (OCR ≈100× slower than text). |
| **D8** | **Worker model:** `ProcessPoolExecutor(initializer=…)` with `mp.set_start_method('spawn')`; each worker creates its **own module-global boto3 client** in the initializer; daemonize **before** pool creation. | #5: boto3 clients are unpickleable and fork-unsafe on macOS; per-process client is the only safe contract. |
| **D9** | **Structured extraction (Phase 5)** is the product terminal, not the vector index. | #3: chunks/embeddings answer "what does it say," not "who needs to hire what / how many / where / what rate." |
| **D10** | **Containers expanded** (Phase 1.5): zips streamed open, inner files content-addressed + re-injected through routing. | #4: zips are 71%-text-by-bytes; the largest non-text byte tier wraps scope/drawings. |

---

## 3. Data models

### 3.1 `sam_attachment_files_90day` — INPUT, read-only, unchanged (per D1).

### 3.2 `sam_attachment_extraction_90day` — append-only state event ledger (Lance v2.1)
One row per processing event. Columns: `resource_id, parent_resource_id (null unless inner-of-zip), lane,
stage, state, extractor, n_pages, text_chars, text_yield_ratio, header_class, content_marking, n_chunks,
sha256_raw, sha256_text, codec, attempt, worker_id, run_id, error, started_at, completed_at`.

**`state` enum** — terminal unless noted:
`routed`(int) · `dropped_boilerplate` · `dropped_duplicate`(non-canonical, C9) · `skipped_non_text` ·
`expanded_container`(C6/D10) · `extracted_scope` · `extracted_pricing` · `extracted_spreadsheet`(int,C6 — re-routed via §7.5 to a scope/pricing/unknown terminal) ·
`extracted_unknown`(C7) · `dropped_content_noise` · `requires_ocr`(int) ·
`ocr_extracted_scope` · `ocr_extracted_pricing` · `ocr_extracted_unknown` · `ocr_dropped_noise` ·
`extract_failed` · `ocr_failed`.

`header_class ∈ {scope, pricing, boilerplate, unknown}`. `content_marking` is a `list<string>` TAG (column) of the control-marking caveats literally detected in the head (`cui`/`fouo`/`itar`/`ear`/`export_controlled`/`dist_stmt_b..f`); `[]` = none detected (NOT proof of public), null = not scanned. It is NOT a state — a marked file keeps its normal terminal state and is embedded locally (§7.4/§9). `extractor ∈ {pdfium, python_docx, openpyxl, libreoffice+pdfium, libreoffice+xlsx, striprtf, txt, pdfium+tesseract}` (closed set; keys `ops.by_extractor`).
Indices (built ONCE at run end): `BTREE(resource_id)`, `BTREE(sha256_raw)`, `BTREE(sha256_text)`,
`BITMAP(lane, stage, state)`.
**Resolution view:** `row_number() OVER (PARTITION BY resource_id ORDER BY is_terminal DESC, attempt DESC,
completed_at DESC) = 1`.

### 3.3 `govcon_scope_vectors_90day` — chunk grain (Lance v2.1)
`chunk_id` (string, deterministic `<resource_id>:<chunk_ix:04d>`) · `resource_id` · `chunk_ix` · `text` ·
`char_len` · `header_class` · `content_marking` (list<string>) · `notice_id` · `solicitation_number` · `naics_code` ·
**`contract_award_unique_key`** (C14, joined from winners manifest) · `source_extractor` ·
`reading_order_conf` (C13) · `embedding fixed_size_list<float32>[1024]` (**nullable**; populated Phase 4) ·
`run_id` · `created_at`.
Indices: `BTREE(resource_id)`, `BTREE(contract_award_unique_key)`, `BITMAP(header_class)`. (`content_marking` is a `list<string>`, not bitmap-indexable directly — gate egress via `len(content_marking)` or a derived `has_content_marking` boolean if a bitmap is needed.)
**`chunk_id` MUST remain UNINDEXED until Phase 4 `merge_insert`s complete (#21).** Vector `IVF_PQ` built in Phase 4.

### 3.4 `govcon_pricing_90day` — same shape minus `embedding`, plus `cells` (string, cell-delimited table rows
from pdfplumber/openpyxl, C6). Not vector-indexed.

### 3.5 `govcon_unknown_90day` (C7) — same shape as 3.3 (with nullable `embedding`) plus **`lexicon_hit` (bool)**;
holds ALL `header_class='unknown'` chunks — both lexicon-hit and lexicon-miss (§7.4; never silently dropped).
`lexicon_hit=false` rows are cheaply excludable via `BITMAP(lexicon_hit)`. Kept physically separate from scope so the scope index stays clean.

### 3.6 `govcon_labor_demand_90day` (C8/Phase 5) — structured grain
`demand_id` (deterministic `<resource_id>:<n>`) · `resource_id` · `contract_award_unique_key` · `notice_id` ·
`solicitation_number` · `naics_code` · `labor_category` · `headcount` (int, nullable) · `clearance_level` ·
`pop_start` · `pop_end` · `place_of_performance` · `wage_floor` · `source_chunk_ids list<string>` · `extractor` ·
`confidence` · `run_id` · `created_at`. Indices: `BTREE(resource_id, contract_award_unique_key)`, `BITMAP(naics_code, clearance_level)`.

### 3.7 `sam_attachment_content_dedup_90day` (C9) — fan-out map
`resource_id` · `sha256_raw` · `canonical_resource_id` · `is_canonical` (bool) · `notice_id` · `solicitation_number`.
Lets duplicate-solicitation labels survive without duplicate extraction/vectors.

### 3.8 `ops.sam_extraction_90day_runs` (Postgres) — per-run roll-up
`run_id, phase, lane, files_in, extracted_scope, extracted_pricing, extracted_spreadsheet, extracted_unknown,
dropped_boilerplate, dropped_duplicate, dropped_content_noise, content_marked, expanded_container,
requires_ocr, extract_failed, by_extractor jsonb, total_chars, total_chunks, sustained_files_per_s,
sustained_mbps, cpu_wait_ratio, status, error, started_at, completed_at`. (`by_extractor` = per-engine
success/fail roll-up, #27.)

---

## 4. Phase 0 — dataset creation
Create 3.2/3.3/3.4/3.5/3.6/3.7 with explicit pyarrow schemas (`data_storage_version="2.1"`); apply the §3.8 DDL
(`CREATE … IF NOT EXISTS`). Do **not** declare the IVF_PQ index or any `chunk_id` index yet (#21).

## 5. Phase 1 — Routing gate + content-canonical dedup

**5.1 Content-canonical dedup pre-pass (C9, #9).** Over `sam_attachment_files_90day` (status='downloaded'):
group by `sha256` (raw bytes; 0 nulls — no re-hash needed); pick `canonical_resource_id = min(resource_id)`
per cluster; write the full map to `sam_attachment_content_dedup_90day`. Only canonical files are extracted;
non-canonical → state `dropped_duplicate` (terminal). Saves 5,627 extractions (4,889 OCR-eligible).

**5.2 Routing (token-boundary regex, NOT substring `ILIKE` — #1 contamination).** Over canonical files,
LEFT-ANTI-JOIN the resolution view (idempotent). Precedence (first match wins):
1. `mime_declared='zip'` → **`container`** → Phase 1.5 queue (state `routed`, stage `route`).
2. `mime_declared IN ('xlsx','xls','xlsm','xlsb')` → **`L4_structured`** (state `routed`). *(C6)*
3. `mime_declared NOT IN ('pdf','docx','doc','txt')` → **`non_text`**, state `skipped_non_text` (terminal). *(images/av/cad/accdb/pptx — pptx may be added to L4 later; out of v2 text scope.)*
4. `regexp_matches(lower(file_name), SCOPE_RX)` → **`L1_scope`** (state `routed`).
5. `regexp_matches(lower(file_name), DROP_RX)` → **`L2_drop`**, state `dropped_boilerplate` (terminal).
6. else → **`L3_triage`** (state `routed`).

```
SCOPE_RX = (^|[^a-z])(sow|pws|p\.?w\.?s|s\.?o\.?w|soo|statement of work|performance work statement|
            scope of work|statement of objectives|specifications?|drawings?|salient charact)([^a-z]|$)
DROP_RX  = (^|[^a-z])(sf ?1449|sf ?30|sf ?33|sf ?18|ppq|past performance questionnaire|
            representations? and certifications?|cdrl)([^a-z]|$)
```
Expected (canonical, ~): L1 ≈ 11k · L2 ≈ 6.3k · L3 ≈ 95–97k · L4 ≈ 9.4k · container ≈ 1.4k · non_text ≈ 10.6k.

## 6. Phase 1.5 — Zip expansion pre-stage (C6/D10, #4)
For each `container` (`zip`): stream-open from CAS **in memory** (no disk extract); enumerate entries; for each
inner file whose **sniffed** mime ∈ `{pdf,docx,doc,txt,xlsx,xls}`: content-address by its own raw sha256 into the
CAS layout, register a synthetic row id `<resource_id>::<inner_path>` carrying parent `notice_id/solicitation_
number/naics_code` (set `parent_resource_id`), and **re-inject through §5.2 routing** (including §5.1 dedup on
inner sha256). Parent zip → state `expanded_container` (terminal); the synthetic `<resource_id>::<inner_path>` IS the `resource_id`
value carried into every sink and the structured table, so `chunk_id` stays well-formed as
`<resource_id>::<inner_path>:<ix:04d>` (the `::` container delimiter and the final `:` chunk delimiter remain
distinguishable). Route `.dwg/.dgn/.rvt` and other binaries to
`non_text` / a drawings-OCR backlog (do NOT feed to pdfium text). **Guards:** recursion depth ≤ 2; per-container
uncompressed-bytes ceiling `ZIP_MAX_UNCOMPRESSED` (zip-bomb containment; v1 had no ceiling); skip encrypted entries
(state `extract_failed`, reason `zip_encrypted`). §12 reconcile counts expanded inner files as a separate population.

## 7. Phase 2 — High-speed text pass (multiprocess pool)

**7.1 Concurrency (D8, #5).** `mp.set_start_method('spawn')`; **daemonize (double-fork + `os.setsid`) BEFORE pool
creation**; `ProcessPoolExecutor(max_workers=POOL_WORKERS, initializer=_init_worker)`. `_init_worker` creates a
module-global boto3 client (`max_pool_connections=4`) — the **sole** place a client is born; never inherit/pass one.
Workers are pure compute+read I/O: read blob → dispatch-extract → classify → chunk → **return** a result struct.
Workers never write Lance. A **single writer** (main proc) drains results, flushes the extraction ledger every
`LEDGER_FLUSH_K=500` and chunks every `CHUNK_FLUSH_M=2000`. **Idempotency/resume:** done-set = resolution view ∪
per-result JSONL checkpoint; `requires_ocr`/`extract_failed` not skipped.

**7.2 Mime dispatch (D4, C2/C5/C13).** Pre-dispatch **sniff override (#14):** `engine = sniff_engine(mime_sniffed)
if (mime_match=false AND mime_sniffed ∈ {pdf,zip,ole,rtf,txt}) else declared_engine` (zip→docx, ole/rtf→libreoffice,
pdf→pdfium, txt→decode). Then:
- **pdf → pdfium**, opened **from the spilled `SpooledTemporaryFile` file object/path** when `content_length >
  BLOB_SPILL=16MB` (NOT a re-read bytes blob — #12), so the OS page cache holds bytes. Iterate pages via the text
  API; accumulate `text_chars`, `n_pages`; release `textpage/page/document` handles per page/doc. A semaphore caps
  concurrent extraction of files >50 MB to `BIG_FILE_CONC=4` (537 such files). pdfium returns **content-stream
  order, not reading order (#15)** — acceptable for single-column govt docs; set `reading_order_conf='low'` via a
  cheap x-column-cluster signal from `get_charbox` on multi-column pages (used to optionally trigger a `pdftext
  --sort` re-pass on the flagged minority, not the header regex).
- **docx → python-docx**, traversing **`Document.iter_inner_content()`** (paras+tables in document order, #6); for
  tables `for row in table.rows: for cell in row.cells`, de-dup merged spans on **`id(cell._tc)`**, recurse
  `cell.iter_inner_content()` for nested tables; emit rows with `" | "` cell + `\n` row delimiters. `n_pages=null`.
- **doc → SERIALIZED LibreOffice lane (#7), OUTSIDE the pool**: a single serialized converter (or one warmed
  `soffice` listener) processes all `.doc` sequentially via `--convert-to pdf`→pdfium; **post-convert existence/size
  check** (exit 0 with no output = retriable, not terminal `extract_failed`). Per-worker profile not needed because
  the lane is serialized. **Sniff pre-pass (#8):** rtf→`striprtf`, zip→python-docx, pdf→pdfium; residual OLE→soffice.
- **txt → charset decode (#25):** utf-8(strict) → BOM sniff (utf-16/utf-8-sig) → `charset-normalizer` best guess →
  cp1252(`errors='replace'`); record `codec` + replacement-char ratio; tag high-replacement chunks low-confidence.
- Any exception → `extract_failed` (caught per file; pool never dies).

**7.3 Threshold detection → `requires_ocr` (pdf only).** `requires_ocr` iff `text_yield_ratio
(=text_chars/max(1,n_pages)) < OCR_RATIO_THRESHOLD=80` AND `text_chars < OCR_ABS_FLOOR (=max(200, n_pages×80))`;
OR `fraction_of_pages(<40 chars) > 0.5`. docx/txt/spreadsheet never OCR.

**7.4 Content triage on EXTRACTED text (#1 gate, C10 control markings).** Run on normalized first ~2,000 chars
(docx surrogate = first 2,000 chars of in-order concat incl. leading table):
- **Control-marking scan FIRST (C10, #10):** detect each caveat present in the head and **capture the actual
  matched tokens** into the `content_marking list<string>` TAG (a column, NOT a diverting state). Per-caveat
  patterns: `cui`=`CONTROLLED UNCLASSIFIED INFORMATION|\bCUI\b`, `fouo`=`FOR OFFICIAL USE ONLY|\bFOUO\b`,
  `export_controlled`=`EXPORT CONTROLLED`, `itar`=`\bITAR\b`, `ear`=`\bEAR\b`, `dist_stmt_<b..f>`=`DISTRIBUTION
  STATEMENT ([B-F])`. `content_marking=[]` when none match (absence of evidence within the window, NOT proof of
  public). Marked files continue through normal classification/chunking and are **embedded locally** (§9); the tag
  — not a pipeline diversion — gates outbound/GTM consumption (`WHERE len(content_marking)=0`). This suffices because
  v2 has no external embedding path (§9 is self-hosted); a physical quarantine sink would add no security and would
  break the §9/§12 `embedding IS NULL == 0` gate. (Caveat: head-only + literal-text detection has false negatives —
  scans/OCR-deferred and >2,000-char-body markings are not seen; the pattern set is an engineering choice, not the
  authoritative NARA/32 CFR 2002/DoDI 5200.48 standard.)
- **scope** headers (`PERFORMANCE WORK STATEMENT|STATEMENT OF WORK|STATEMENT OF OBJECTIVES|SCOPE OF WORK|\bPWS\b|
  \bSOW\b|SPECIFICATIONS?|TECHNICAL REQUIREMENTS|SALIENT CHARACTERISTICS`) → `extracted_scope`.
- **pricing** (`WAGE DETERMINATION|SERVICE CONTRACT ACT|DAVIS[- ]BACON|\bSCA\b|\bWD\b|PRICE SCHEDULE|SCHEDULE OF
  PRICES|\bCLIN\b`) → `extracted_pricing` (+ pdfplumber table pass, §7.5).
- **boilerplate** (`STANDARD FORM 1449|SOLICITATION/CONTRACT/ORDER FOR COMMERCIAL|AMENDMENT OF SOLICITATION|PAST
  PERFORMANCE QUESTIONNAIRE|REPRESENTATIONS AND CERTIFICATIONS`) → `dropped_content_noise`.
- **unknown** (no header hit): **labor-lexicon admission gate over the FULL body (#1)** —
  `labor categor|\bLCAT\b|\bFTE\b|headcount|clearance|certification|period of performance|place of performance|
  wage|\bSCA\b|wage determination` → state `extracted_unknown` → **`govcon_unknown_90day`** sink (NOT the scope
  index). Lexicon-miss → `extracted_unknown` to the unknown sink as well (do NOT silently drop) but flagged
  `lexicon_hit=false` so it can be excluded cheaply. **L1_scope** bypasses the boilerplate-drop branch (never drop a
  filename-confirmed SOW); still subject to CUI + threshold.

**7.5 Spreadsheet extraction (L4, C6, #2).** xlsx/xlsm/xlsb → `openpyxl read_only=True`; legacy `.xls`/unreadable →
the §7.2 LibreOffice path (`--convert-to xlsx`/`csv`). Emit **per-sheet, cell-and-row-delimited** text (header row +
`" | "` cells, `\n` rows) — never a flattened blob. State `extracted_spreadsheet`. Route the emitted text through
the §7.4 classifier (→ scope vs pricing vs unknown), do **not** dump all spreadsheet text into pricing.

**7.6 Vector / pricing / unknown staging (C7/C11/C14).** `extracted_scope`→`govcon_scope_vectors_90day`;
`extracted_pricing`→`govcon_pricing_90day` (+ pdfplumber `cells` for tabular PDF/spreadsheet pages, MIT);
`extracted_unknown`→`govcon_unknown_90day`. **Chunking:** normalize whitespace; split on paragraph/page; window
`CHUNK_CHARS=1200`, `CHUNK_OVERLAP=180`; `chunk_id=<resource_id>:<ix:04d>`. **Write via `merge_insert` on `chunk_id`
(#19)** — the idempotency floor. **Carry `contract_award_unique_key`** by left-joining the winners manifest on
`resource_id` at staging (#14). **Ordering invariant (#19/#20):** a result's per-result checkpoint line is written
only **after** its chunks durably reach the sink — never batch the checkpoint — so resume never loses chunks.
`sha256_text` is the secondary dedup key (collapses text-identical re-saves, C9).

## 8. Phase 3 — Isolated OCR queue (async, D7)
Target resolution-view `state IN ('requires_ocr')`. Tesseract via `pytesseract` (Pillow is a hard dep of
pytesseract — auto-installed). **In-memory handoff (no disk images):** `pdfium page.render(scale=TARGET_DPI/72)` →
`.to_pil()` → `pytesseract.image_to_string(img, lang='eng', config=f'--psm {OCR_PSM}')`; `del` bitmap/image +
`page.close()` per page. **`OCR_PSM=3` (#13)** (no `osd.traineddata` dep); `OCR_MAX_PAGES` cap + large-raster
down-scale for the long tail (observed max 1,580 pages, #24). Post-OCR → §7.4 triage → `ocr_extracted_scope/
pricing/unknown` / `ocr_dropped_noise` / `ocr_failed`; chunk to the same sinks (`source_extractor='pdfium+tesseract'`).
`OCR_WORKERS=max(2,(cpu_count−2)//2)`; same single-writer + checkpoint + idempotent-skip model. Expected queue
~5,900–7,000 PDFs (~8%, smoke-confirmed §10).

## 9. Phase 4 — Embedding + vector index (REQUIRED, D6, C1)
Embedder: **self-hosted instruction-retrieval model, `D=1024`** (e.g. `bge-large-en-v1.5`), run **locally — no
external API** (CUI posture, C10). Batch-read each vector sink where `embedding IS NULL` → embed → **L2-normalize at
write** → `merge_insert` on `chunk_id`. Then build Lance `IVF_PQ` with **`metric='cosine'`**, `num_sub_vectors=64`
(`D/16`), `num_partitions≈sqrt(n_vectors)`. `chunk_id` was kept unindexed precisely so the merge_insert path avoids
lancedb #3177 (#21). Assert `COUNT(*) WHERE embedding IS NULL == 0` before indexing. (`nprobes`/`refine_factor` are
query-time, documented in a retrieval-config note, not the build.) ALL chunks including control-marked ones
(`len(content_marking)>0`) are embedded by the **local** model — there is no external API in this pipeline, so nothing
is sent off-host and the §12 `embedding IS NULL == 0` gate applies uniformly. The `content_marking` tag gates outbound
consumption, not embedding.

## 10. Phase 5 — Structured field extraction (THE PRODUCT, D9, C8, #3)
Over the Stage-4 substrate, run a **deterministic per-document pass grouped by `resource_id`** across its classified
scope+pricing+spreadsheet chunks (exhaustive — NOT gated on a vector similarity query, which is lossy). Extract into
`govcon_labor_demand_90day`: `labor_category, headcount, clearance_level, pop_start, pop_end,
place_of_performance, wage_floor, source_chunk_ids`. Key by `resource_id`; carry `contract_award_unique_key` (joined
via the winners manifest / `PRIME_AWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md` + `SUBAWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md`
— these sinks do not carry the award key directly). Extractor = regex+LLM hybrid; record `confidence`. This table —
not the vector index — answers "list awards needing ≥N cleared electricians in <place>, base+options."

## 11. Execution sequence
0. **Provision** (§15): assert `soffice` + `eng.traineddata` present (fail fast, #8/#13). Create datasets (§4).
1. **Phase 1** dedup pre-pass (§5.1) + routing (§5.2). Verify lane counts.
2. **Phase 1.5** zip expansion (§6) — re-injects inner files into routing.
3. **Phase 2 smoke**: `--max-files 200` to throwaway sinks; record sustained files/s, MB/s, CPU-vs-wait, and the
   **observed `requires_ocr` fraction** (feeds §10 OCR sizing). 0 crashes required.
4. **Phase 2 on L1_scope** (priority) → then **L4_structured** → then **L3_triage** (daemonized, §7.1).
5. **Phase 3 OCR** (§8).
6. **Compaction (#22):** `compact_files(target_rows_per_fragment=1_048_576)` + `cleanup_old_versions()` on the
   extraction ledger and every sink **before** building indices (compaction invalidates index coverage of rewritten
   fragments). Then build the §3.2/§3.3/§3.4/§3.5 indices (still **no `chunk_id` index**).
7. **Phase 4** embedding + IVF_PQ (§9).
8. **Phase 5** structured extraction (§10).
9. **Reconcile** (§12); write terminal `ops` rows per phase.
Daemonization + JSONL checkpoint + resume are mandatory for steps 4–5/7–8.

## 12. Reconciliation & acceptance
**Reconcile (read-only):**
- **Coverage:** every canonical `resource_id` with text/spreadsheet/container mime (denominator now =
  `{pdf,docx,doc,txt,xlsx,xls,xlsm,xlsb,zip}` minus `dropped_duplicate`, **plus expanded inner files**) has exactly
  one terminal state. 0 `routed`/`requires_ocr` after Phase 3.
- **Vector integrity (#20):** for every terminal scope/pricing/unknown `resource_id`, `ledger.n_chunks ==
  COUNT(*)` of its `chunk_id`s in the matching sink (hard assertion; offending ids logged). No orphan chunks; no
  missing. Chunk-`chunk_id` uniqueness holds after Phase 2 (not only Phase 4).
- **Dedup:** non-canonical files all `dropped_duplicate`; fan-out map row count == non-canonical count.
- **Embedding:** `COUNT(*) WHERE embedding IS NULL == 0` per vector sink before IVF_PQ.

**Acceptance:**
- `extract_failed / denominator < 0.01`, **decomposed per `extractor`** via `ops.by_extractor` (#27); a missing
  `soffice`/`eng.traineddata` is a startup failure, not a silent gate breach.
- `requires_ocr` fully resolved (0 unresolved).
- Coverage = 100%; vector orphans = missing = 0; per-`resource_id` `n_chunks` assertion passes.
- `govcon_labor_demand_90day` populated for ≥1 row per `extracted_scope` document that contains a labor-lexicon hit.

## 13. Failure modes & blast-radius containment
Per-file try/except → `extract_failed` (pool isolation). Single committer (D3) — no concurrent R2 commits. OCR
isolated (D7). Append-only ledger (D1) — no in-place updates. Resume = resolution-view ∪ checkpoint; deterministic
`chunk_id` + `merge_insert`. In-memory blob spill + pdfium-from-spill (#12) + >50 MB semaphore. `.doc` serialized
(#7). Zip-bomb ceiling + recursion cap (§6). `content_marking` tag (captured caveats) + local-only embedding — no external path exists (#10). Token-boundary routing +
content-truth triage (#1).

## 14. Tunables (defaults)
`POOL_WORKERS=cpu_count−2` · `OCR_WORKERS=max(2,(cpu−2)//2)` · `BIG_FILE_CONC=4` · `OCR_RATIO_THRESHOLD=80` ·
`OCR_ABS_FLOOR=max(200,n_pages×80)` · `MIXED_PAGE_FRACTION=0.5` · `OCR_PSM=3` · `OCR_MAX_PAGES=400` (down-scale
beyond) · `TARGET_DPI=300` · `CHUNK_CHARS=1200` · `CHUNK_OVERLAP=180` · `LEDGER_FLUSH_K=500` · `CHUNK_FLUSH_M=2000`
· `BLOB_SPILL=16MB` · `ZIP_MAX_UNCOMPRESSED=2GB` · `ZIP_MAX_DEPTH=2` · `EMBED_DIM D=1024` · `EMBED_METRIC=cosine` ·
`COMPACT_TARGET_ROWS=1_048_576` · `start_method=spawn` · `max_pool_connections=4`.

## 15. Host provisioning / prereqs (#8/#13)
- **LibreOffice** installed; `SOFFICE_BIN` env set; **assert at startup** (`which $SOFFICE_BIN` → fail fast).
- **Tesseract** + **`eng.traineddata`** (assert); `osd.traineddata` only if OSD pre-step enabled.
- Python deps (uv, permissive licenses verified): `pylance pyarrow pypdfium2(BSD-3) python-docx(MIT) openpyxl(MIT)
  xlrd(BSD) pdfplumber(MIT) striprtf(BSD-3) charset-normalizer(MIT) pytesseract(Apache-2.0, pulls Pillow) boto3
  duckdb 'psycopg[binary]'` + the local embedder runtime. **No PyMuPDF, no antiword.**
- Runtime: `doppler run --project core-x --config prd -- uv run --with … python pipelines/sam_gov/<script>.py`.

## 16. Throughput model (#26)
Phase 2 wall-clock ≈ `max( R2 concurrent egress of ~189 GB across POOL_WORKERS, CPU extraction time )`; the single
writer issues <~1,000 commits total → effectively free, **not** the ceiling. The Stage-3 4.625 MB/s figure was
WAF-rate-bound (8 req/s on sam.gov) and does NOT transfer to R2 reads. The §11 step-3 smoke records sustained
files/s, bytes/s, and CPU-vs-wait so the binding term is measured, not assumed; a live monitor compares running
`sustained_files_per_s` to the smoke projection.

## 17. Implementation artifacts (names fixed for the executor)
- `pipelines/sam_gov/sam_attachment_extract_90day.py` — Phase 1 (dedup+route), 1.5 (zip), 2 (text+L4 spreadsheet), serialized `.doc` lane (`--phase`, `--lane`, `--max-files`, `--daemon`, `--resume`).
- `pipelines/sam_gov/sam_attachment_ocr_90day.py` — Phase 3.
- `pipelines/sam_gov/sam_attachment_embed_90day.py` — Phase 4 (local embedder + IVF_PQ).
- `pipelines/sam_gov/sam_labor_demand_extract_90day.py` — Phase 5 structured extraction.
- `pipelines/sam_gov/sam_attachment_extract_reconcile_90day.py` — §12 reconcile + compaction.
- `pipelines/sam_gov/ops_sam_extraction_90day_runs.sql` — canonical ops DDL.

## 18. Provenance
v1: `SAM_90DAY_EXTRACTION_PIPELINE_SPEC.md` (superseded). Review: `SAM_90DAY_EXTRACTION_PIPELINE_SPEC_ADVERSARIAL_REVIEW.md`
(`wf_4be318e0-c69`, 33 surviving / 7 refuted). Ground truth: live `sam_attachment_files_90day`, 2026-06-08.
