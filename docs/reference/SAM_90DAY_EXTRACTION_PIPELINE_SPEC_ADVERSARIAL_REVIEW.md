# Adversarial Review — 90-Day SAM.gov Attachment Text Extraction Pipeline Spec

**Reviewed artifact:** `docs/reference/SAM_90DAY_EXTRACTION_PIPELINE_SPEC.md` (Stage 4 of the GovCon substrate pipeline)
**Ground-truth basis:** live 90-day ledger `sam_attachment_files_90day`, measured 2026-06-08 (126,901 downloaded files / 213.7 GB; 114,901 text-extractable / 189.0 GB).
**Review method:** adversarial multi-agent workflow (`wf_4be318e0-c69`, 2026-06-08) — 6 independent Opus dimension critics → per-finding adversarial Opus verification → Opus synthesis; 47 agents / 679 tool-calls; 40 findings raised, **33 surviving / 7 refuted**. Each surviving finding was re-verified against the live ledger, library/LanceDB docs, or a precise spec contradiction.

## Verdict

The pipeline's **core state architecture is sound and should ship as designed**: append-only event ledger (D1), latest-terminal resolution view (D2), parallel-extract/serialized-write (D3), and purpose-separated sinks (D5) are the right primitives, and the most alarming prior concerns (resolution-view masking a success, concurrent-merge conflicts, Pillow missing, DPI-tag accuracy loss, header-regex substring contamination) were **refuted** under verification. However, the spec is **not buildable end-to-end as written** and **does not meet its own stated GTM goal** without changes in three classes: (1) **strategic data-loss** — spreadsheets (9,426 / 3.21 GB) and zips (1,379 / 16.46 GB) carrying the densest labor/pricing signal are hard-routed to a terminal `non_text` skip, and there is **no structured field-extraction stage**, so the substrate is generic RAG, not a "who needs to hire what" product; (2) **runtime provisioning gaps** that will crash or silently corrupt at build time — LibreOffice is undeclared and unmanaged for the `.doc` lane, `boto3` clients are shared across a fork pool, and `--psm 1` requires an undeclared `osd.traineddata`; (3) **retrieval-quality defects** — the `unknown → extracted_scope` recall-biased default pollutes the scope vector index with ~40k non-scope docs. The embedding model/dimension `D` is `TBD`, blocking the IVF_PQ index. Before any code is written, the spec must close the spreadsheet/zip routing, pin a default `D`, add provisioning declarations, gate the `unknown` class out of the scope index, and add the four concurrency/extractor hardening rules below.

---

## Severity-Ranked Summary (surviving findings, deduped)

Severities below are the **verifier-corrected** values, not the original reviewer self-ratings.

| # | ID | Finding | Severity | Dimension |
|---|----|---------|----------|-----------|
| 1 | triage_pollution-1 | `unknown → extracted_scope` default pollutes scope vector index (~40k non-scope docs) | **High** | retrieval |
| 2 | strat-2 / strat-5 / extract_spreadsheet_zip_gtm-1 / scope-exclusion-8 | Spreadsheets (9,426 / 3.21 GB) + tabular PDF scope discarded; no table-aware extractor | **High** | strategic |
| 3 | strat-3 | No structured field-extraction stage — chunking answers "what does it say", not "who needs to hire what" | **High** | strategic |
| 4 | strat-4 | ZIPs (1,379 / 16.46 GB) wrapping SOW/drawings discarded unopened | **High** | strategic |
| 5 | boto3_fork-1 | ProcessPool workers share/inherit unpicklable boto3 clients; macOS fork abort | **High** | concurrency |
| 6 | extract_docx_tables-1 | DOCX paragraph-only read drops table-borne scope (43.7% of a sampled SOW) | **High** | extractors |
| 7 | libreoffice_conc-1 | 1,437 `.doc` files collide on shared LibreOffice profile under 14 workers | **High** | concurrency |
| 8 | extract_doc_libreoffice-1 | LibreOffice/`soffice` not installed; entire `.doc` lane fails the §8 gate | **Medium** | extractors |
| 9 | dedup-1 | No pre-extract sha256 dedup: 5,627 byte-identical files re-extracted + duplicate chunks | **Medium** | datamodel |
| 10 | strat-1 | Award→winner join key not denormalized onto chunk sinks (recoverable via `resource_id`) | **Low** | strategic |
| 11 | strat-7 | No CUI/FOUO/export-controlled/PII handling; external-API embedding exposure | **Medium** | strategic |
| 12 | lance_state-2 | D3 single-writer rationale is factually wrong (appends are conflict-compatible) | **Medium** | lance_state |
| 13 | mem_blowup-1 | 16 MB spill bounds transport bytes only, not pdfium parse memory | **Medium** | concurrency |
| 14 | extract_mime_dispatch_sniff-1 | Dispatch ignores `mime_sniffed`; ~20 mislabeled files hard-fail their engine | **Low** | extractors |
| 15 | extract_pdf_reading_order-1 | pdfium returns stream order, not reading order (multi-column tail risk) | **Low** | extractors |
| 16 | ocr_psm1_osd-1 | `--psm 1` needs undeclared `osd.traineddata`; no PSM tunable | **Medium** | ocr |
| 17 | strat-6 | Embedding model/`D` and OCR compute unsized; Phase 2.5 un-instantiable | **Low** | strategic |
| 18 | ivfpq-underspec-6 | IVF_PQ default metric is L2; semantic search needs cosine + normalization | **Low** | datamodel |
| 19 | idempotency-2 | Phase 2 appends chunks (only 2.5 uses merge_insert); resume edge cases | **Low** | datamodel |
| 20 | reconcile-holes-4 | §8 reconcile lacks per-`resource_id` `n_chunks == COUNT(*)` check | **Low** | datamodel |
| 21 | merge-insert-index-bug-5 | Unfixed Lance bug if `BTREE(chunk_id)` + `optimize()` ever added | **Low** | datamodel |
| 22 | lance_state-3 | Compaction omitted; analogous engine already at 256 fragments un-compacted | **Low** | lance_state |
| 23 | lance_state-4 | D1 "rewrite whole fragments → storm" mechanic is inaccurate | **Low** | lance_state |
| 24 | embed-blocker-3 | Vector dataset is text-only until `D` chosen (overlaps strat-6/ivfpq-6) | **Low** | datamodel |
| 25 | ocr_volume_unsized-1 / ocr_planning_gap-1 | `requires_ocr` queue unsized (~5,900–7,000 PDFs, page-weighted) | **Low** | ocr |
| 26 | extract_txt_encoding-1 | latin-1 fallback never raises; masks cp1252 mojibake (413 files) | **Low** | extractors |
| 27 | writer_not_ceiling-1 | Writer is not the throughput ceiling; egress/CPU model absent | **Low** | concurrency |
| 28 | acceptance-7 | `<1%` extract_failed bar not decomposed per failure class | **Low** | datamodel |

---

## High-Severity Findings

### 1. `unknown → extracted_scope` default pollutes the scope vector index (triage_pollution-1)

**Spec location:** §4.4 (recall-biased default), D5, §2.3.

**Problem.** §4.4 routes the entire `unknown` header class (text present, no header hit) into `extracted_scope`, and §4.5/§2.3 write those chunks into the **same** index, `govcon_scope_vectors_90day`, alongside genuine-header scope. The spec's mitigation — "tag so downstream can filter" — is inert: a LanceDB vector query with no `where` clause scans the whole index regardless of `header_class`. Prefilter is opt-in per query, and the spec never specifies a query-time filter. D5 explicitly establishes "clean retrieval surface" as a design value and carves out pricing for exactly this reason — but leaves the much larger `unknown` class to pollute.

**Verified evidence.** Re-deriving §3 lanes on the live ledger: `L3_triage` = 98,131 (85.4% of 114,901). A deterministic 150-PDF L3 sample classified by the exact §4.4 regexes: scope=16, pricing=11, boilerplate=30, unknown=70 (46.7%), requires_ocr=23. Of scope-index entrants (scope+unknown), **81.4% are the `unknown` default**. Projection onto verified lane sizes: ~35k–40k L3 PDFs enter the scope index via the `unknown` default; adding L3 DOCX (21,629) pushes the polluting population **past 45k** — 2–3× the genuine-scope population. Verbatim `unknown` examples pulled live: "Pre Proposal Conference Registration Form.pdf", "Mandatory Contractor Training DOL Posters 2024.pdf", "VAAR Class Deviation 852.222-71…pdf" — unambiguously non-scope.

*(Severity corrected critical→high: the "~85% non-scope" figure is the L3-PDF entrant composition, not the whole index, which also holds ~12k filename-confirmed L1_scope files; this is a precision/recall defect, not corruption or data loss.)*

**Remediation (concrete).**
- **Preferred (option a):** route `header_class='unknown'` chunks to a **separate `govcon_unknown_90day` sink** (mirror of the D5 pricing carve-out). Add a distinct terminal target so the scope index stays clean; a max-recall query can `UNION` the unknown sink explicitly. Update §8 vector-integrity reconcile to account for the third sink.
- **Secondary (option b), GTM-targeted:** gate admission of `unknown` to any scope surface on a **positive labor-demand lexicon hit over the FULL body** (not the first 2,000 chars): `labor category | LCAT | FTE | headcount | clearance | certification | period of performance | place of performance | wage | SCA | wage determination`. Lexicon-hit → scope-adjacent sink; lexicon-miss → unknown sink. **Do not silently drop.**
- **Do NOT** "partition IVF_PQ by `header_class`" — IVF_PQ partitions are vector-centroid clusters, not metadata partitions. The correct stopgap is a mandatory query-time `where header_class='scope'` prefilter plus a `BTREE`/`BITMAP` scalar index on `header_class`; this is strictly weaker than (a)/(b) because it leaves unknown rows physically co-resident.

---

### 2. Spreadsheets + tabular PDF scope discarded — the densest labor/pricing signal (strat-2, strat-5, extract_spreadsheet_zip_gtm-1, scope-exclusion-8)

**Spec location:** §3 lane rule 1 (`mime NOT IN {pdf,docx,doc,txt} → non_text`, `skipped_non_text`, terminal); D4; §4.2 (pdfium flat text as sole PDF representation).

**Problem.** Two compounding losses of the exact GTM payload (labor categories, headcount, rates, wage floors):
1. **Spreadsheets hard-skipped.** §3 rule 1 terminally routes all xlsx/xls/xlsm/xlsb to `non_text` and never opens them. In federal solicitations the *structured* labor signal — CLIN/price schedules, staffing matrices, LCAT rate cards, cost-volume workbooks — lives disproportionately in `.xlsx`. The spec invests an entire ~100×-slower OCR stage to recover scanned PDFs while discarding the cheapest, densest structured source — a priority inversion against the stated goal. xlsx is OOXML (zip-of-XML); there is no license or engine barrier.
2. **Tabular PDF scope mangled.** pdfium has no layout/table/column analysis (maintainer Discussion #290: "does not expose APIs for layout analysis such as detecting words, lines and paragraphs/columns"). CLIN tables and staffing matrices embedded in SOW PDFs are returned in content-stream order, destroying the row/column adjacency (which rate ↔ which labor category) that §2.4's "structured downstream parsing" promise depends on.

**Verified evidence.** Live ledger: spreadsheets (xlsx 8,981 + xls 515 + xlsm 4 + xlsb 3) = **9,426 files / 3.211 GB**; 42–44% carry price/labor/clin/wage/staff/bid/schedule tokens in the filename. Sampled names: "Attachment 2 - CLIN Pricing Sheet and Equipment List.xlsx", "Staffing Plan_N6660425Q0168.xlsx", "Attachment 5 - Cost Volume Workbook.xlsx". mime_sniffed: 8,900 sniff as `zip` (OOXML, openpyxl-readable), 521 as `ole` (legacy, xlrd). Licenses re-verified at source: **openpyxl = MIT, xlrd = BSD** — consistent with D4's permissive bar. pdfium reading-order facts confirmed against the maintainer thread and pdftext's `--sort` workaround.

*(Severity corrected critical→high: PDFs at 184.67 GB / 88,669 files remain the dominant substrate and their lanes are the primary payload; spreadsheets are a high-density structured supplement — 7.4% of files, 1.5% of bytes. The pdfium tail is medium on its own but bundled here as the same "tabular signal" gap.)*

**Remediation (concrete).**
- **Add lane `L4_structured`** in §3 (a spreadsheet branch before the `non_text` catch-all): xlsx/xlsm/xlsb → **openpyxl `read_only=True`** (8,981 files); legacy `.xls`/`.xlsb` → route through the **LibreOffice path the spec already provisions for `.doc`** (`--convert-to xlsx`/`csv`) — zero new dependency. Emit per-sheet **cell-and-row-delimited** text (header row + cells, `" | "` cell delimiter, row delimiter), **not** a flattened blob, so labor/rate grids retain column alignment. Add `extracted_spreadsheet` to the §2.2 state enum and `source_extractor='openpyxl'/'libreoffice+xlsx'`.
- **Route spreadsheet text through the existing §4.4 classifier** so output splits into `govcon_scope_vectors_90day` vs `govcon_pricing_90day` like the PDF/DOCX path — do **not** dump all spreadsheet text into pricing (over-couples; violates D5 purpose separation).
- **For PDF pages classified `pricing`** (or showing tabular signals — high digit density / aligned numeric columns), run a **table-aware extractor (pdfplumber, MIT)** to emit cell-structured rows into `govcon_pricing_90day`; keep pdfium flat text for prose scope. Add a per-row/cell structured column to §2.4. Scope this to the pricing/table subset, **not** all 88k PDFs.
- **Extend §8 reconcile** coverage to include the spreadsheet mimes (the current denominator hardcodes 114,901 / `{pdf,docx,doc,txt}`).

---

### 3. No structured field-extraction stage (strat-3)

**Spec location:** §4.4 (`header_class` only), §4.5 (chunking), §6 (embedding), whole pipeline.

**Problem.** The terminal artifacts are ~1,200-char text chunks + an IVF_PQ embedding index. The only semantic label produced anywhere is `header_class ∈ {scope,pricing,boilerplate,unknown}`. A `grep` across the spec for `labor|headcount|clearance|wage|place/period-of-performance` returns **zero** schema columns or stages. A vector index over chunks supports "find passages semantically similar to query" — it cannot answer "list awards needing ≥10 cleared electricians in San Diego, base+4 option years" as filterable structured facts. The pipeline stops one defined stage short of the actual product. The asymmetry is internal: §2.4 explicitly stages pricing "for structured downstream parsing," but the scope text — carrying the labor payload — terminates in a generic vector index with no structured successor.

**Verified evidence.** Confirmed against the spec text and sibling docs: `GOVCON_PROFILE_MATERIALIZATION_SCHEMA.md` is the company-profile (UEI/address) layer, not labor-field extraction; `SAM_GOVCON_SUBSTRATE_AGENT_NOTES.md` names the end-state as a vector substrate. No documented Phase 4 exists. Chunk-volume re-derived: ~1.1M–1.2M chunks / ~280M–307M embed tokens.

*(Severity corrected critical→high: the specified pipeline is a coherent, necessary precursor substrate, not wasted work; chunking is a correct **input** to structured extraction, not "the wrong terminal primitive.")*

**Remediation (concrete).**
- **Add a net-new Stage 5 — structured extraction** over the Stage-4 substrate: a deterministic **per-document** LLM/regex pass over the classified scope+pricing chunks **grouped by `resource_id`** (and the L4 spreadsheet grids), emitting a `govcon_labor_demand_90day` table with `labor_category, headcount, clearance_level, pop_start, pop_end, place_of_performance, wage_floor, source_chunk_id`.
- **Dataflow correction:** do **not** gate structured extraction on a vector similarity query (lossy). The substrate already holds every classified chunk per `resource_id`; run the structured pass as an exhaustive per-document pass over the held text. Vector search remains the ad-hoc human/agent retrieval surface.
- **Keying:** key the structured table by `resource_id` and bridge to the award via the existing prime/subaward attachment bridges (`PRIME_AWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md` / `SUBAWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md`) — these sinks do not carry `contract_award_unique_key` directly.

---

### 4. ZIPs (1,379 / 16.46 GB) discarded unopened (strat-4)

**Spec location:** §3 lane rule 1 (`zip → non_text`, `skipped_non_text`, terminal).

**Problem.** On a construction-NAICS substrate, zips are containers of exactly the scope/labor payload the pipeline targets — bundled SOW attachments, full drawing sets, bid packages. Skipping the container discards the contained pdf/docx scope wholesale, contradicting the spec's own §4.4 recall-bias principle ("recover scope hidden in generic files"). This is the **largest single non-text byte tier**.

**Verified evidence.** Live ledger: 1,379 zip / 16.457 GB. Cracked open two independent samples directly from CAS blobs: Sample A (32 zips) inner entries are **58.6% text-extractable pdf/docx/doc/txt (71.2% of inner bytes)**; CAD/drawings only 10% of entries. `SOW_Attachments.zip` → 70 PDF + 1 docx; the Requirements zip → 64 xlsx + 37 docx + 21 doc + 19 pdf. Sample B (50 fresh random zips): 50/50 opened cleanly, **0 encrypted, only 2/50 nested**, 86% contain ≥1 text doc. Distribution: only 10 zips ≥100 MB (1.82 GB); 1,025 are <10 MB. NAICS-23 is the largest bucket (7.79 GB / 288 zips).

**Remediation (concrete).**
- **Add a zip-expansion pre-stage** (after Phase 1 routing, before Phase 2): stream-unzip in memory, enumerate entries, content-address each inner blob by its own sha256 into the existing CAS layout, and route inner files **back through Phase 1** under synthetic id `<resource_id>::<inner_path>`, carrying parent `notice_id/solicitation_number/naics_code`.
- **Inner-file dedup:** content-address on inner sha256 so the existing dedup (finding #9) extends to inner files (drawing sets and boilerplate are heavily duplicated across solicitations).
- **State + reconcile interaction (load-bearing):** add a terminal **`expanded_container`** state for the parent zip row (so it is not left intermediate), and **extend the §8 reconcile denominator** to count expanded inner files as a separate population — otherwise the pipeline can never satisfy its own definition-of-done.
- **Hardening:** cap recursion depth (≤2 is sufficient per sampling — handles CDRL bundles with inner zips), cap total uncompressed bytes per archive (zip-bomb containment), only re-inject members whose sniffed mime ∈ `{pdf,docx,doc,txt,xlsx,xls}`. Note the spec has **no 1 GB ceiling** (it specifies `BLOB_SPILL = 16 MB`); add an explicit per-container size ceiling. Route `.dwg/.dgn/.rvt` to `non_text`/a drawings-OCR backlog — do not feed them to pdfium text extraction.

---

### 5. ProcessPool workers share unpicklable boto3 clients (boto3_fork-1)

**Spec location:** §4.1 (`get_object` per worker), D3 (`ProcessPoolExecutor`), §11 runtime.

**Problem.** §4.1 mandates each worker call R2 `get_object` but never specifies where the boto3 client is created. boto3 clients are **not pickleable** (SSL context + thread locks) and must not be shared across processes. Under `ProcessPoolExecutor`'s pickle-based dispatch, a parent-created client captured in a worker closure **fails outright**; a fork-inherited threaded client causes SSL response-ordering corruption or the macOS objc `fork()` abort the codebase already documents (`sam_attachment_download_90day.py` lines 103-105). D3 tells implementers to "mirror the proven Stage-3 collector pattern" — but that pattern is **ThreadPoolExecutor + single shared client**, which does NOT carry to a process pool. The spec gives workers zero client-lifecycle contract.

**Verified evidence.** boto3 clients not pickleable confirmed (botocore #636, boto3 #2311); official guidance: create a new client inside each child. macOS default start method is `spawn` since Python 3.8; "fork is considered not safe on macOS." §7 step 3 mandates double-fork daemonization (`os.setsid`) AND a ProcessPoolExecutor, with ordering unpinned.

**Remediation (concrete).** Add to §4.1:
- Each worker process lazily creates its **own module-global boto3 client** via the `ProcessPoolExecutor(initializer=...)` hook — never inherits or receives one as an argument.
- Force `mp.set_start_method('spawn')`; keep `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` as backstop.
- **Pin ordering:** daemonization (double-fork + `os.setsid`) must complete **before** the pool is created; the pool `initializer` is the sole place the client is born.
- Size `max_pool_connections ≈ 2–4` per client (each process issues one `get_object` at a time), not the Stage-3 value of 32.

---

### 6. DOCX paragraph-only read drops table-borne scope (extract_docx_tables-1)

**Spec location:** §4.2 ("docx → python-docx: concatenate paragraph + table text"), §4.4 header triage.

**Problem.** python-docx `Document.paragraphs` does **not** include table-cell text, and `Document.tables` does not include nested (cell-embedded) tables. The literal phrase "paragraph + table text" is ambiguous; an executor using `doc.paragraphs` (a documented common pitfall) silently loses every table — and in govt SOW/PWS docs the CLIN/labor-category/deliverable line items live in tables. For §4.4, if a form opens with a table, the leading-table header is invisible to first-page classification → misclassification. Iterating `row.cells` also re-emits merged-cell text, inflating `text_chars`.

**Verified evidence.** Reproduced on python-docx 1.2.0: `doc.paragraphs` excludes table text; top-level `doc.tables` misses nested tables; `row.cells` duplicates merged spans (2×). On the cited live blob `0ce70cc417b84da0975cce548c6829a4` ("Att 02_SOW (250626).docx"): paragraphs-only = 1,140 chars, table-cell = 867 chars → **table share 43.2%** (reviewer 43.7%); the table content is the genuine deliverable grid ("Repair spalling of about 6 inches by full diameter of primary spillway pipe…"). 1,391 docx files carry `sow`/`pws` in the filename; total docx = 24,382. PDF lane unaffected.

**Remediation (concrete).** Specify exact traversal in §4.2:
- Walk **`Document.iter_inner_content()`** (the method is on the `Document` proxy in python-docx 1.2.0, NOT `doc.element.body`) to emit paragraphs and tables in document order.
- For tables: `for row in table.rows: for cell in row.cells:`, de-duping merged spans keyed on **`id(cell._tc)`**; recurse `cell.iter_inner_content()` for nested tables (`Table` has no `iter_inner_content`).
- Emit table rows with a stable cell delimiter (`" | "`) and a row delimiter so CLIN/labor grids retain structure for downstream parsing.
- Define the docx §4.4 "first-page" surrogate as the first N chars of the in-order concatenation, explicitly including any leading table. Keep `n_pages=null`; OCR-skip stays mime-based (already correct in §4.3).

---

### 7. `.doc` files collide on the shared LibreOffice profile under 14 workers (libreoffice_conc-1)

**Spec location:** D4; §4.2 (`doc → LibreOffice headless --convert-to pdf`), runs inside the Phase 2 ProcessPool (`POOL_WORKERS = cpu_count−2 = 14`).

**Problem.** LibreOffice headless `--convert-to` is not safe to invoke concurrently against the default shared user profile (`~/.config/libreoffice`): concurrent `soffice` instances contend on the profile lockfile and either silently produce no output (subprocess returns 0, no PDF) or crash. With 1,437 `.doc` files across 14 workers, multiple `soffice` instances launch simultaneously; a large fraction silently fail → mis-recorded `extract_failed`. The spec provides no profile isolation, no serialization, no output check.

**Verified evidence.** LibreOffice bug 82775 (headless segfault on concurrent requests) and 106134 (headless disallows concurrent jobs); gotenberg #94 documents the silent no-output mode and resolved it via serialization; Ask LibreOffice / unoconv confirm even separate `soffice --convert-to` CLI processes collide on the shared default profile. 1,437 `.doc` confirmed in ledger. **Acceptance-gate math:** the 1,437 `.doc` population is *larger than the entire §8 failure budget* (1,149 = 1% of 114,901); 75% silent failure (1,078) by itself nearly busts the bar before any other failure source. A `soffice` convert is multi-second vs sub-second pdfium, so concurrent overlap across ~103 conversions/worker is effectively certain.

**Remediation (concrete).** Make explicit in D4/§4.2:
- **Preferred:** route all `.doc` to a **single serialized conversion lane** outside the parallel pool (1,437 files is small), or a reused warm `soffice` listener processing `.doc` sequentially — removes contention entirely.
- **Alternative:** per-worker `-env:UserInstallation=file:///tmp/lo_profile_<worker_pid>`, established and **warmed once at worker init** (a cold custom profile created on first conversion can itself fail), plus `-env:SingleAppInstance=false` (Linux).
- **Non-optional in either path:** post-convert **existence/size check** on the output PDF — `soffice` can return exit 0 with no output; zero/absent output is a retriable condition, not immediate terminal `extract_failed`.

---

## Medium-Severity Findings

### 8. LibreOffice/`soffice` not installed; `.doc` lane fails the gate (extract_doc_libreoffice-1)

**Spec location:** §1 D4; §4.2; §11 runtime.

**Problem.** The only specified extractor for the `.doc` lane is a `soffice` subprocess, but `soffice` is absent from the runtime host, and §11 declares only the Tesseract binary as a host prereq — never `soffice`. An executor building "from this document" will not provision it; every legacy-OLE `.doc` falls to `else → extract_failed`.

**Verified evidence.** Host probe: `which soffice libreoffice` → both not found; no `/Applications/LibreOffice.app` or `/opt/homebrew/bin/soffice`. Tesseract 5.5.0 IS present (the one declared prereq is satisfied; the undeclared one is missing). The runtime is local (§7 step 3 daemonizes via double-fork + `os.setsid` per Stage-3, not Modal), so this host is representative. `doc` rows decompose to 1,395 OLE / 32 rtf / 9 zip / 1 pdf. **1,395 / 114,901 = 1.214%** — this class alone breaches the §8 `<0.01` gate if all OLE `.doc` fail.

**Remediation (concrete).**
- Declare LibreOffice as an explicit §11 host prereq with an install step and `SOFFICE_BIN` env var, exactly as Tesseract is declared, plus the per-worker profile from finding #7.
- Add a **sniff-based pre-pass** so misfiled rows skip `soffice`: 32 rtf → **striprtf (BSD-3, not MIT)**; 9 zip → python-docx; 1 pdf → pdfium. The residual ~1,395 true OLE go through `soffice` (highest fidelity); do **not** use `docx2txt` for them — it reads only OOXML `.docx`, not legacy OLE. Avoid `antiword` (GPL — conflicts with the permissive posture).

### 9. No pre-extract sha256 content dedup (dedup-1)

**Spec location:** §2.3 (`chunk_id = resource_id:ix`), §4 (per-file extract), §4.5, §9.

**Problem.** `chunk_id` keys on `resource_id`, so byte-identical files under different `resource_id`s produce different `chunk_id`s and are never deduplicated. Each is fully extracted (CPU + OCR) and emits near-identical chunks into the scope index, skewing nearest-neighbor toward boilerplate. The spec defines `sha256_text` with a BTREE index "for dedup" but **never wires it** to skip extraction, suppress chunks, or collapse vectors — a dangling label.

**Verified evidence.** Among 114,901 text-extractable files: **109,274 distinct sha256 → 5,627 redundant byte-identical files (4.9%), 5.48 GB**; top clusters 50/47/38/32 copies. Corpus-wide: 126,901 / 120,887 distinct = 6,014 dupes; sha256 has 0 nulls (dedup computable with no re-hash). Of the 5,627 redundants, 4,889 are OCR-eligible PDFs; only 163 match `DROP_RX`; cluster contents include W-9 (×26), 52.212-3 reps&certs (×29), and "rrc sow march 2022 final.pdf" (×31) — the last is L1_scope, which §4.4 never drops, so all 31 copies enter the scope index.

*(Severity corrected high→medium: L2 filename-drop + §4.4 boilerplate suppression catch a fraction; impact bounded at 4.9% index inflation — an efficiency/quality defect, not data loss.)*

**Remediation (concrete).**
- Insert a **content-canonical pre-pass before Phase 2**: pick `min(resource_id)` per **raw-byte sha256** cluster, extract/OCR only canonical files (saves 5,627 extractions incl. 4,889 OCR-eligible).
- Use the existing **`sha256_text`** as the final chunk-dedup key (it also collapses byte-different/text-identical re-saves — a strictly larger dedup set). Wire it; do not add a new key.
- Carry the non-canonical `resource_id → sha256 → notice_id/solicitation_number` fan-out in a **separate mapping table** so duplicate-solicitation labels/filters survive without duplicate vectors.
- Out of scope: near-identical (semantic) dedup.

### 10. CUI / export-controlled / PII handling absent (strat-7)

**Spec location:** whole spec; cf. prior `SAM_ATTACHMENT_DOWNLOAD_EXECUTION_PLAN.md §9` (`export_controlled=false` gate).

**Problem.** Govt solicitation attachments carry CUI/FOUO markings and export-controlled technical data (esp. construction/defense drawings/specs). §6 ships ~280M tokens of extracted text to an **external embedding API**, and the GTM use (outbound pitching) compounds exposure. The prior download plan explicitly gated `export_controlled=true` (724 rows); the 90-day manifest **dropped that column**, and this spec adds no replacement — no CUI detection in §4.4, no quarantine, no on-prem-embedding constraint.

**Verified evidence.** Prior plan §9 lists "Do not download … export_controlled = true (724 rows)." Live schema probe: neither `sam_opps_attachment_manifest_90day_winners` nor `sam_attachment_files_90day` carries an `export_controlled` column; the manifest builder (`sam_opps_attachment_manifest_90day_winners.py:281-285`) captures `accessLevel` but never an EC field. The existing `access_level='public'` gate already excludes 3,037 private rows (public 152,146 / private 3,037), so residual EC risk is publicly-posted-but-marked documents. *(One reviewer citation — "agent notes §0: 'No export_controlled column'" — does not exist in the repo; the underlying fact is verified independently.)*

**Remediation (concrete).**
- **Add a content-based sensitivity classifier to §4.4** (the only implementable control on bytes already on disk): scan extracted first-page text for `CONTROLLED UNCLASSIFIED INFORMATION|CUI|FOR OFFICIAL USE ONLY|FOUO|EXPORT CONTROLLED|ITAR|EAR` and DoD `DISTRIBUTION STATEMENT [B-F]`. Route flagged files to a **quarantined sink excluded from the external Phase 2.5 embedding path** (embed locally or skip); tag chunks with a `sensitivity` column so GTM consumers can filter.
- The reviewer's "re-join `export_controlled` from the source notice" is **not feasible as written** (column absent everywhere; never captured) — treat a SAM-API EC backfill as optional secondary, not the primary gate. Content detection also catches mismarked-public docs a metadata gate would miss.

### 11. D3 single-writer rationale is factually wrong (lance_state-2)

**Spec location:** §1 D3; §9 ("Concurrent Lance writers conflict").

**Problem.** D3 justifies single-writer with "Lance is single-writer-per-dataset; concurrent appends from a pool corrupt/conflict." This is **false** as a statement of Lance semantics: appends are explicitly conflict-compatible with each other (optimistic concurrency + rebase). The design conclusion (serialize commits) is correct, but for an **unstated** reason — the pipeline commits directly to R2 with no `commit_lock` / external manifest store, so concurrent committers to R2 are unsafe. A future engineer who "knows appends are fine" could wire concurrent appends and hit silent R2 commit races.

**Verified evidence.** Lance transaction docs: "the append operation … is designed to be compatible with most other operations, even itself." `lance==7.0.0` confirmed installed. The Stage-3 commit path D3 mirrors (`sam_attachment_download_90day.py` lines 121-130, 494-500) calls `lance.write_dataset(..., storage_options=so)` with **no `commit_lock`**, **no `s3+ddb://`** manifest store.

**Remediation (concrete).** Rewrite D3's rationale: *"Lance appends are individually conflict-compatible, but the pipeline commits directly to R2 with no commit lock / external manifest store; a single committing process means zero concurrent committers, so no atomic manifest compare-and-swap is needed. If true write parallelism is ever required, the mechanism is `lance.write_dataset(commit_lock=...)` (or DynamoDB for AWS S3), not abandoning Lance."* Fix the §9 cell in lockstep: "Concurrent Lance writers conflict" → "Concurrent Lance committers to R2 race (no commit lock wired)."

### 12. 16 MB spill bounds transport bytes, not pdfium parse memory (mem_blowup-1)

**Spec location:** §4.1 (`BLOB_SPILL = 16 MB`); §9 (memory containment); §10.

**Problem.** The §9 one-liner conflates transport-buffer spill with extraction-time memory. pdfium's `FPDF_LoadMemDocument` requires the input buffer to stay resident for the document's lifetime; if a worker re-reads the spilled temp file into a `bytes` object to feed pdfium (the spec never says it streams from the spilled path), the raw bytes are re-materialized on the heap, so spill does NOT bound extraction-time RAM. No per-worker memory budget or large-file serialization rule exists.

**Verified evidence.** Live PDF ledger: n=88,669; >16 MB=2,377; >50 MB=537; >100 MB=58; max=237.7 MB; p99=36.0 MB; median=0.31 MB. Host: 16 logical CPUs → 14 workers, 48 GiB. *(Severity corrected high→medium: pdfium loads pages lazily; text-only extraction holds the page tree + one text-page, not "structures that dwarf raw bytes" — that's the rendering case. Worst realistic per-worker resident ≈ a few hundred MB to ~1 GB; 14× ≈ 7–14 GB, not 48 GB. Whole-host OOM needs ~3.4 GB/worker, unreached. Probability of all 14 hash-sharded workers simultaneously on >100 MB files (58/88,669) is negligible. Real risk = individual worker spikes / swap pressure + absence of any extraction ceiling.)*

**Remediation (concrete).**
- For files above the spill threshold, **open pdfium directly from the spilled `SpooledTemporaryFile`'s path/file object** (not a re-read bytes blob) so the OS page cache holds the bytes — the single change that closes the gap.
- Optional defense-in-depth: a semaphore capping concurrent extraction of files >50 MB to 2–4 (only 537 such files exist; do not build a separate single-worker lane).
- Move explicit per-page `textpage/page/document` handle release into §4.2 as a memory control.
- **Do not** drop `POOL_WORKERS` below 14 for the text pass — it would slow the dominant <16 MB population.
- Fix the §9 wording to distinguish transport spill from extraction memory.

### 13. `--psm 1` requires undeclared `osd.traineddata`; no PSM tunable (ocr_psm1_osd-1)

**Spec location:** §5 (`config='--psm 1'`), §10 (no PSM tunable).

**Problem.** PSM 1 invokes Orientation and Script Detection, loading `osd.traineddata` — a separate file from `eng.traineddata`. If absent, Tesseract logs "Failed loading language 'osd'" and (depending on config/TESSDATA_PREFIX) can return empty → `ocr_failed`. Even when present, PSM 1 provides no OCR benefit over PSM 3 (the OSD result is computed internally, not returned). §5 thus adds a fragile host dependency for zero gain; §10 exposes no PSM tunable; §11 documents neither `osd` nor `eng` packs.

**Verified evidence.** §5 line 210 confirmed verbatim. PSM 1 = auto-segmentation **with** OSD (official tessdoc); `osd.traineddata` is a distinct file (tesseract #1132/#1133/#1463, RedHat 1068910, ropensci/tesseract #43). PyImageSearch PSM guide: "I don't think it's worth applying --psm 1." OCR targets ~12,000 non-text files / 24.7 GB; §8 requires `requires_ocr` fully resolved (0 unresolved). *(Severity corrected high→medium: the catastrophic empty-queue outcome is config/version-dependent, not guaranteed when `lang='eng'` is present; the reliable harm is a fragile dependency for zero gain plus a per-host empty-failure risk.)*

**Remediation (concrete).**
- Switch to **`--psm 3`** (default, no OSD dependency) for full-page document OCR; expose `OCR_PSM` as a §10 tunable. PSM 3 over PSM 4 — the OCR text feeds §4.4 full-page reading-flow triage; PSM 4 single-column can fragment multi-column SOW layouts.
- If rotation correction is genuinely needed, run `pytesseract.image_to_osd` as a separate try/except-guarded pre-step and declare `osd.traineddata` as a §11 prereq.
- Regardless, **pin `eng.traineddata`** as a §11 host prereq ("Tesseract binary required" alone does not guarantee the eng data on minimal images).

---

## Low-Severity Findings

### 14. Award→winner key not denormalized onto chunk sinks (strat-1)
**Spec location:** §2.3/§2.4 columns; §4.5. The chunk sinks carry `notice_id/solicitation_number/naics_code` but not `contract_award_unique_key`. **Verified:** the lead is **not** lost — `resource_id` is preserved end-to-end and is unique in the winners manifest (155,183/155,183 distinct); 100% of 114,901 text-extractable files LEFT JOIN to a winner row on `resource_id` alone. *(Severity corrected critical→low: a denormalization-for-convenience gap, not data recovery.)* **Remediation:** at §4.5 staging, left-join the winners manifest on `resource_id` and carry `contract_award_unique_key` as an inline label on both new sinks with `BTREE(contract_award_unique_key)`. Do **not** write the key into the read-only download ledger (violates D1/§2.1). `award_keys[]` (list type) and recipient UEI stay resolved via the manifest join.

### 15. pdfium stream order ≠ reading order (extract_pdf_reading_order-1)
**Spec location:** D4; §4.2; §4.4. pdfium returns content-stream order with no layout model; the spec presents output as ground truth with no caveat. **Verified:** real library fact, but live testing of 10 real PDFs across SOW/PWS/wage/clause-table classes flagged **0 multi-column pages**; govcon attachments are overwhelmingly single-column flowing text. Wage determinations (`ELECTRICIAN……$ 20.96`) and CLIN schedules extract with adjacency intact. *(Severity corrected medium→low: sparse, fail-safe tail; §4.4 `unknown→scope` default means a garbled header fails safe.)* **Remediation:** add a §4.2 caveat that pdfium output is unsorted stream order, acceptable for single-column govt docs; add an **optional** `low_reading_order_confidence` chunk flag on L1_scope via a cheap x-column-cluster signal from the existing `get_charbox` API; use that flag (not the header regex) to trigger optional re-OCR-with-layout or a pdftext `--sort` re-pass on only the flagged minority. (Overlaps the table-aware fix in finding #2 — implement once.)

### 16. Dispatch ignores `mime_sniffed` (extract_mime_dispatch_sniff-1)
**Spec location:** §3, §4.2. Dispatch keys on `mime_declared` only; the ledger carries `mime_sniffed` + a populated `mime_match` bool that the routing never reads. **Verified:** real, but yield is overstated — truly recoverable ≈ **20 files** (not 90+): pdf-lane zip→docx(1)+txt(3); docx-lane pdf→pdfium(1)+ole→libreoffice(14)+txt(1). The 29 NULL-sniff pdf rows give no alternate signal; the `.doc` lane already uses content-tolerant LibreOffice (the "42 mis-engined .doc" claim is false). 20/114,901 = 0.017%. **Remediation:** one-line pre-dispatch override in §4.2 — `engine = sniff_engine(mime_sniffed) if (not mime_match and mime_sniffed in {'pdf','zip','ole','rtf','txt'}) else declared_engine` (zip→python-docx, ole/rtf→libreoffice, pdf→pdfium, txt→decode); keep §9 try/except as the corruption backstop; keep declared default when sniff is NULL.

### 17. Embedding model/`D` and OCR compute unsized (strat-6, embed-blocker-3)
**Spec location:** §6, §10 (`EMBED_DIM D = TBD`); §5. The `fixed_size_list<float32>[D]` column cannot be schema-instantiated without `D`, and IVF_PQ cannot be built — but D6 deliberately decouples embedding so the text pass is unblocked, and the column is nullable. *(Severity corrected high→low: defensible deferral; a single pinned default away from executable.)* **Remediation:** pin a concrete default `D` (768 or 1024 instruction-retrieval embedder) so §2.3 is instantiable, state derived `num_sub_vectors=D/16` and projected footprint (~3.4 GB raw float32 at D=768 × 1.1M vecs pre-PQ), and **reclassify Phase 2.5 from "deferred" to "required for the GTM deliverable."** If the column must stay deferred, create the table without it and `add_columns` at Phase 2.5 — do not declare an uninstantiable `[D]` at §7 step 0. For OCR sizing: instrument Phase 2 to emit the measured `requires_ocr` count + sampled per-page timing rather than committing a speculative wall-clock.

### 18. IVF_PQ default metric is L2; semantic search needs cosine (ivfpq-underspec-6)
**Spec location:** §2.3, §6, §10. IVF_PQ is named with no metric/params. **Verified:** Lance default metric = **L2** and default `num_sub_vectors = D/16`; instruction/retrieval embedders are tuned for cosine/dot and are frequently not unit-normalized — silently inheriting L2 degrades ranking. The "D-TBD blocks `num_sub_vectors`" framing is wrong (Lance auto-derives a divisor-safe default). **Remediation:** in §10/§6 (deferred with Phase 2.5), specify **`metric='cosine'` AND require unit-normalization of vectors at write time** (stating cosine without normalization is incomplete); only require `D % 16 == 0` (or `% 8`) to avoid the degenerate 1-subvector fallback; `nprobes/refine_factor` are query-time params — document them in a retrieval-config note, not the build DDL.

### 19. Phase 2 appends chunks; only 2.5 uses merge_insert (idempotency-2)
**Spec location:** §4.1, §4.5, §6, §8. `lance.write_dataset(mode='append')` has no key dedup (verified on lance 7.0.0); only Phase 2.5 uses `merge_insert`. **Verified partial:** the headline duplicate-`chunk_id` scenario is largely prevented because the resume done-set is the **union** of resolution view ∪ per-result JSONL checkpoint (§4.1 line 155); the genuine residual is an **unpinned ordering invariant** and a **missing-chunk** window (a checkpointed result whose chunks were still in the M-buffer at crash is skipped → chunks lost). *(Severity corrected high→low.)* **Remediation:** (a) make the Phase 2 chunk write a **`merge_insert` on `chunk_id`** as the idempotency floor (matches §4.5's stated intent); (b) **pin the invariant** in §4.1: write a result's per-result checkpoint line only after (or atomically with) its chunks durably reach the sink, and never batch the checkpoint; (c) add the §8 chunk-uniqueness check after Phase 2, not just 2.5. Drop option (c) "same transaction across datasets" — Lance has no cross-dataset transaction.

### 20. §8 reconcile lacks per-`resource_id` `n_chunks == COUNT(*)` (reconcile-holes-4)
**Spec location:** §8 vector integrity. **Verified partial:** the count-equality + "every scope file has ≥1 chunk" + "no orphan chunks" invariants already catch the 0-chunk and orphan cases; the only genuinely uncovered hole is **partial-batch loss** (≥1 chunk survives but fewer than recorded `n_chunks`) under the dual-cadence flush (ledger K=500 vs chunks M=2000). **Remediation:** add one hard assertion — per `resource_id` with a terminal scope/pricing state, `ledger.n_chunks == COUNT(*)` of matching `chunk_id`s in the sink; non-empty diff fails acceptance with offending ids logged. Better structural fix: flush the chunk batch **before** (or atomically with) the ledger event recording its `n_chunks`, eliminating the window rather than detecting it. The finding's anti-joins #1/#2 merely restate invariants §8 already mandates.

### 21. Lance merge_insert silent-skip bug if `BTREE(chunk_id)` + `optimize()` added (merge-insert-index-bug-5)
**Spec location:** §6, §2.3. lancedb #3177 (OPEN, 0.30.0/lance 3.0.0) silently skips `when_matched_update_all()` when a scalar index exists on the merge key AND `optimize()` runs between merges. **Verified partial:** the spec **does not** trigger this — §2.3 indexes `BTREE(resource_id)`, NOT `chunk_id` (the merge key), and there is **no `optimize()` call** in the spec; §6 already builds the index after population. *(Severity corrected medium→low: an additive hardening, not a present bug.)* **Remediation:** keep only — (c) pin lance/lancedb in §11 away from 0.30.0/3.0.0 and add a §8 assertion `COUNT(*) WHERE embedding IS NULL == 0` before building IVF_PQ; plus a one-line §6/§2.3 note that **`chunk_id` must remain UNINDEXED** until Phase 2.5 merge_inserts complete (so a future implementer doesn't add `BTREE(chunk_id)` under the D1 convention and trip #3177). Drop remediations (a)/(b) (already satisfied) and (d) (discards deterministic-`chunk_id` idempotency).

### 22. Compaction omitted (lance_state-3)
**Spec location:** D1, §2.2, §7, §10. Frequent small appends (K=500, M=2000) create one fragment + one version per flush; the new ledger projects to ~486 fragments. **Verified:** the analogous proven Stage-3 ledger sits at **256 fragments / 260 versions, never compacted**; empirically 10 appends → 10 fragments, `compact_files()` collapses to 1. *(Verifier corrected to low.)* **Remediation:** add an explicit §7 compaction step between extraction and index build — `lance.dataset(uri).optimize.compact_files(target_rows_per_fragment=1_048_576)` on the extraction ledger and each chunk sink, **before** (re)building indices (compaction invalidates index coverage of rewritten fragments); add `cleanup_old_versions()` and a §10 compaction tunable. *(Note: this interacts with finding #21 — compaction must run before any `chunk_id` index ever exists.)*

### 23. D1 "rewrite whole fragments → storm" mechanic is inaccurate (lance_state-4)
**Spec location:** §1 D1; §9; override note (lines 34-36). **Verified:** `update` does NOT rewrite whole fragments — it marks rows via a deletion vector and writes one tiny new fragment (reproduced on lance 7.0.0: a one-cell update left frag data files byte-identical, added a deletion vector + a 1-row fragment); `merge_insert` rewrites only matched columns. **Remediation:** correct the mechanic in D1, §9, and the override note: *"per-cell `update` accumulates deletion vectors + small new fragments (compaction debt); `merge_insert` rewrites only matched columns; neither is a full-fragment rewrite. The append-only event log avoids the read-modify-write commit, per-mutation single-writer serialization, and the compaction debt of per-transition mutation, and is audit-complete."* Keep the D1 decision. (Drop the claim that §4.5/§6 merge_insert is justified on this premise — they stand on idempotency alone.)

### 24. `requires_ocr` queue unsized (ocr_volume_unsized-1, ocr_planning_gap-1)
**Spec location:** §5, §7 step 4, §8, §10, D7. Every other lane is numerically sized; `requires_ocr` is left to emerge at run time. **Verified:** measured trigger rate ~8% blended (L1 9.17%, L3 8.00%) → **~5,900–7,000 PDFs**, page-weighted (8% of flagged docs ≥20 pages carry ~38% of OCR page-load; observed max 1,580 pages). Cited examples ("Attachment 5 Fall Protection Drawings.pdf" 29p; "…Montrose WC Final Drawings…" 37p) confirmed. *(Verifier corrected to low: a sizing omission with a contained operational consequence; memory is already bounded.)* **Remediation:** fold a `requires_ocr` estimate into the existing §7 step-2 smoke (extend it to emit the observed OCR fraction + projected page count = fraction × PDF count × avg `n_pages`); add an **`OCR_MAX_PAGES`** per-document cap / large-raster down-scale in §10 to bound the 1,580-page tail against the §8 "0 unresolved" gate. Do **not** blanket-exclude CAD drawings (SCOPE_RX routes `specifications?` + `drawings?` to L1; image-only spec sheets carry labor signal) — deprioritize by ordering instead.

### 25. latin-1 txt fallback masks cp1252 mojibake (extract_txt_encoding-1)
**Spec location:** §4.2 (`txt → decode utf-8 → latin-1 fallback`). **Verified:** latin-1 maps all 256 byte values so it never raises; cp1252 punctuation bytes (0x80–0x9F: smart quotes, em dash, bullet) silently decode to C1 control chars. But blast radius is the smallest lane (413 files / 0.02 GB), GTM signal is ASCII and survives intact, and §4.4 regexes are ASCII so triage is unaffected. The Part-2 claim (pdf/docx sniff-as-txt files have "no decode route") is **refuted** — the spec dispatches on `mime_declared`, so they go to pdfium/python-docx with an `extract_failed` catch-all. **Remediation:** replace the bare latin-1 fallback with `utf-8(strict) → BOM sniff (utf-16/utf-8-sig) → charset-normalizer best guess → cp1252 errors='replace'`; record the chosen codec + replacement-char ratio in the event row; tag high-replacement chunks low-confidence. (charset-normalizer over the unmaintained chardet.)

### 26. Writer is not the throughput ceiling; egress/CPU model absent (writer_not_ceiling-1)
**Spec location:** D3; §4.1 (K=500/M=2000); §7. **Verified:** the writer does only ~230 ledger + ~235–705 chunk commits over the run (<~1,000 total) — single-digit to low-double-digit minutes aggregate, negligible vs a multi-hour pass; the writer design is correctly sized. The Stage-3 4.625 MB/s figure was WAF-rate-bound (8 req/s on sam.gov) and does NOT transfer to Phase 2's R2 reads. **Remediation (doc only):** add a §7 throughput model — Phase 2 wall-clock ≈ `max(R2 concurrent egress of 189 GB across the pool, CPU extraction time)`, writer treated as free; have the §7 step-2 smoke record sustained files/s, bytes/s, and CPU-vs-wait ratio so the binding term is visible. Note the L1 smoke (~4.0 MB avg) is a conservative lower bound for the lighter L3 pass; add a live monitor comparing running `sustained_files_per_s` against the smoke projection.

### 27. `<1%` extract_failed bar not decomposed per class (acceptance-7)
**Spec location:** §8, §4.2. The 1% bar (1,149 files) is a single aggregate; `doc=1,437` alone is 1.25% of the denominator. **Verified:** the §8 gate already says "each failure class logged with `error`," and §2.2 persists both `extractor` and `error` per event, so per-class decomposition is a trivial GROUP BY — the "not diagnostic" framing is a misread. **Remediation:** the one net-new fix is to **assert LibreOffice binary presence at Phase-2 startup (fail fast)** so a missing-binary env bug doesn't silently convert all 1,437 `.doc` into `extract_failed` and breach the gate (ties to findings #7/#8); optionally add a per-extractor roll-up to `ops.sam_extraction_90day_runs` and carry both a global `<1%` and a per-extractor sanity cap. (Overlaps findings #7/#8 — implement the startup assertion once.)

---

## Prioritized Remediation Roadmap (change in the spec BEFORE implementation)

Ordered by dependency and blast radius. Items 1–5 are **build blockers**; 6–10 are required for the spec to meet its own stated goal and acceptance gate; 11+ are correctness/quality hardening.

1. **Pin embedding `D` + Phase 2.5 status** (findings #17, #18, #24-embed): choose a default `D` (768/1024), state `metric='cosine'` + write-time normalization, mark Phase 2.5 **required**. Unblocks §2.3 schema instantiation and IVF_PQ. *Blocks §7 step 0.*
2. **Declare + manage `.doc` extraction** (findings #8, #7, #27): add LibreOffice as a §11 host prereq + `SOFFICE_BIN`; route `.doc` to a **serialized lane** (or per-worker warmed profile) with a post-convert output check; assert `soffice` presence at startup. Add the rtf/zip/pdf sniff pre-pass. *Without this the §8 gate is unmeetable.*
3. **Fix the boto3 worker contract** (finding #5): per-process client via `initializer`, force `spawn`, pin daemonize-before-pool ordering, small `max_pool_connections`. *Without this Phase 2 crashes on the macOS host.*
4. **Switch OCR to `--psm 3` + declare traineddata** (finding #13): change §5 config, expose `OCR_PSM`, pin `eng.traineddata` (and `osd` only if OSD is used). *Without this Phase 3 can empty-fail the queue.*
5. **Specify DOCX in-order table traversal** (finding #6): `Document.iter_inner_content()` + `id(cell._tc)` merged-span dedup + delimited rows; define the docx §4.4 first-page surrogate. *Without this ~24k DOCX silently lose scope.*
6. **Close the spreadsheet/zip/structured-PDF data loss** (findings #2, #4): add `L4_structured` (openpyxl/LibreOffice) + zip-expansion pre-stage + pdfplumber table extraction for pricing pages; add `extracted_spreadsheet`/`expanded_container` states; extend §8 reconcile denominator. *Without this the substrate omits the densest GTM signal.*
7. **Gate `unknown` out of the scope index** (finding #1): separate `govcon_unknown_90day` sink and/or labor-lexicon admission gate; mandatory `where header_class='scope'` prefilter as stopgap.
8. **Add the structured field-extraction Stage 5** (finding #3): `govcon_labor_demand_90day` keyed by `resource_id`, per-document pass over held chunks, bridged to award via existing bridges. *This is the actual product.*
9. **Add content-canonical sha256 dedup + wire `sha256_text`** (finding #9): canonical extraction pre-pass + final chunk dedup + duplicate-fan-out mapping table.
10. **Add CUI/sensitivity classifier + quarantine** (finding #10): content-based detection routing flagged files away from external embedding.
11. **State stream + reconcile invariants** (findings #19, #20): Phase 2 `merge_insert` on `chunk_id`, checkpoint-before-chunk-flush ordering, per-`resource_id` `n_chunks==COUNT(*)` assertion, chunk-uniqueness after Phase 2.
12. **Add compaction + correct Lance rationale** (findings #22, #23, #11): §7 `compact_files()` before index build, `cleanup_old_versions()`, §10 tunable; rewrite D1/D3/§9 mechanics (commit-coordination, not append corruption; deletion-vector cost, not full-fragment rewrite); keep `chunk_id` unindexed until 2.5 (finding #21).
13. **Memory + dispatch + encoding + pdfium polish** (findings #12, #14, #15, #25): stream pdfium from spilled path; sniff-aware dispatch override; cp1252/charset-normalizer txt decode; pdfium stream-order caveat + optional reading-order flag.
14. **Denormalize award key + add throughput/OCR-sizing model** (findings #14-strat-1, #26, #24): inline `contract_award_unique_key` on the two new sinks; §7 egress/CPU model; smoke-derived `requires_ocr` estimate + `OCR_MAX_PAGES`.

---

## Appendix — Refuted Findings (challenged and dismissed)

These were raised in earlier review passes and **do not survive verification**. Listed so the reader sees what was tested and rejected.

| ID | Title | Why refuted |
|----|-------|-------------|
| **lance_state-1** | Resolution view masks a successful extraction when a later-attempt failure is logged | The SQL tie-break weakness is real in isolation, but the triggering data state is **unreachable**: per §4.1, any durable attempt-1 `extracted_scope` is in the resolution view → in the resume done-set → the resource is skipped → attempt 2 is never produced. If the success was lost in a crash, there is no row to mask. The one dangerous variant (orphan chunks from partial flush) is a different concern already backstopped by the §8 "no orphan chunks" invariant. |
| **extract_merge_insert_concurrency-1** | Chunk merge_insert contradicts single-writer/append and conflicts with concurrent embedding | Misreads the spec: chunk sinks are written by **append** (§4.1/§4.5), only Phase 2.5 uses `merge_insert`. Verified Lance conflict matrix: **Append does NOT conflict with Update or Merge Insert** — the cited merge-vs-merge conflict requires both writers to use merge_insert, which never co-occurs. Phases are temporally serialized (D6, §7). No D1-vs-§4.5 inconsistency (D1 concerns the download ledger, §4.5 the separate sinks). |
| **ocr_topil_pillow-1** | OCR `to_pil()` crashes because Pillow is absent | Factually wrong: `pytesseract` declares `Pillow>=8.0.0` as a **hard dependency**; installing `--with pytesseract` auto-installs `pillow==12.2.0`. Verified in isolated uv env — the §5 handoff path works, no `ModuleNotFoundError`. pypdfium2 not requiring Pillow is irrelevant since pytesseract drags it in. |
| **ocr_dpi_metadata-1** | Missing DPI tag → Tesseract assumes 70 dpi → degraded OCR | Mechanical sub-claim true (no `info['dpi']`, "Invalid resolution 0 dpi, using 70"), but the **material claim is false**: the Tesseract maintainer states xres/yres "have virtually no impact unless resolution is so poor as to make error rate very high" — recognition is driven by actual pixel height, which the `scale=300/72` render fully delivers regardless of the metadata tag. Cosmetic stderr warning, not an accuracy regression. |
| **pricing_header_contamination-1** | §4.4 pricing regex re-imports the 46% substring contamination | Mechanism mismatch: the 46% figure was measured on **substring `ILIKE`** (`%SCA%` catching TESCAN/scanning); §4.4 uses **word-boundary `\bSCA\b`**. All six taxonomy false positives tested → none match the `\b`-anchored regex. The scope branch is also evaluated before pricing (first-match-wins), and `unknown→scope` is recall-biased, and pricing is a retained queryable sink (not /dev/null). |
| **merge_insert_indexed-1** | merge_insert on `chunk_id` against a BTREE-indexed scope table is a known Lance failure | Wrong column: §2.3 indexes `BTREE(resource_id)`, not `chunk_id`. The cited bug (#2285) requires the index and merge key on the same column, was fixed >1 year ago, and current pylance is 7.0.0. The open variant (#3177) requires an `optimize()` call the spec never makes. The remediation (build indices after merge) is already what §6 does. *(See surviving finding #21 — the residual hardening note is preserved there.)* |
| **chunk_resume_race-1** | Writer-crash window lets resume re-chunk → duplicate `chunk_id`s | The done-set unions the resolution view with the **per-result checkpoint written on drain before chunks flush**, so flushed chunks imply those ids were already checkpointed and are skipped on resume — no duplicate `chunk_id`s. The genuine residual (the *reverse*: missing chunks) is captured in surviving finding #19/#20. |
