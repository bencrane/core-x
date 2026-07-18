# Labor-category extraction — pipeline & methodology audit

**Mode:** READ-ONLY recon (code archaeology + R2 ground-truth probes). **Snapshot:** 2026-06-21 (UTC).
**Question:** provenance of `govcon_award_scope_requirements.labor_category_values` before building GTM campaigns on it — given prior noise in *equipment* extractions.
**Probes (reproducible, read-only):** [`scripts/archive/labor_category_pulse_probe.py`](scripts/archive/labor_category_pulse_probe.py) (§4 pulse), [`scripts/archive/award_requirements_provenance_probe.py`](scripts/archive/award_requirements_provenance_probe.py) (extractor-tag distribution). Raw JSON: `/tmp/labor_pulse.json`, `/tmp/req_prov.json`.

---

## BLUF — `labor_category_values` is the opposite of noisy

The field the operator fears is, in fact, the **cleanest requirement type in the entire pipeline**. It is a **closed 36-token controlled vocabulary**, enforced at *both* extraction lanes, **100% conformant across all 58,464 live serving occurrences** (0 free-form strings, 0 junk). The equipment-noise problem does **not** transfer to labor.

| Requirement type | rows (resource grain) | distinct values | cardinality | character |
|---|---:|---:|---:|---|
| **labor_category** | 21,770 | **36** | **0.17%** | **controlled vocabulary — pristine** |
| equipment_capability | 2,385 | 2,231 | **93.5%** | free-form LLM text — **noisy (the prior problem)** |
| past_performance | 2,389 | 1,644 | 68.8% | free-form |
| deliverable | 19,937 | 10,934 | 54.8% | free-form |
| standard_compliance | 163,572 | 8,727 | 5.3% | semi-controlled (norm hints) |
| vehicle_constraint (set-aside) | 15,343 | 171 | 1.1% | controlled-ish |

**The real risk is the inverse of noise: lossy recall / false negatives** — a closed vocabulary that is construction/trades/facilities/security/medical/logistics-heavy and carries **zero IT/cyber/software roles**. For the IT vertical (DA01) the labor signal is structurally thin. **Do not rewrite the labor prompt to denoise it — it isn't noisy. The open question is vocabulary coverage, not precision.** (Details in §5.)

---

## 1. The extraction architecture

### 1a. Attachment sourcing & storage
Raw attachments are pulled from SAM.gov's per-resource file-bytes endpoint `https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{resource_id}/download` by [`sam_attachment_download_90day.py`](pipelines/sam_gov/sam_attachment_download_90day.py) (residential IP, concurrency 6, ~8 req/s, WAF circuit breaker). Gate: `access_level='public' AND file_name IS NOT NULL AND size_bytes >= 1` (`:73`). Bytes land content-addressed in R2 at `s3://data-sink/active/sam_attachment_blobs/<resource_id>`; lineage ledger at `s3://data-sink/active/sam_attachment_files/` (Lance SoR).

### 1b. Text extraction engine — multi-engine, routed by sniffed MIME ([`sam_attachment_extract_90day.py`](pipelines/sam_gov/sam_attachment_extract_90day.py))
| Format | Engine | Note |
|---|---|---|
| PDF | **pypdfium2** (`pdfium.PdfDocument` → `get_textpage().get_text_range()`, `:959-974`) | primary text path; **not** pdfplumber/pypdf/PyMuPDF |
| DOCX | **python-docx** (`docx.Document`, `:1021-1025`) | document order |
| XLSX | **openpyxl** | head/schema retained on truncation |
| legacy DOC/RTF/OLE | **LibreOffice (`soffice`)** conversion | out-of-process |
| pricing PDFs | **pdfplumber** table pass | secondary only, capped `PDFPLUMBER_MAX_PAGES=50` |

**No OCR / tesseract in this phase.** Low-text-yield (scanned) PDFs are *deferred* to a separate Phase 3 via a `requires_ocr` state (`:5-6`) — meaning **image-only solicitations contribute no labor signal in the current serving data.**

### 1c. Chunking strategy — fixed-size sliding **character** window (the crux)
Not semantic, not page-based, not token-based, not "first-N-pages." The **full** normalized document text is windowed deterministically:
- `CHUNK_CHARS=1200`, `CHUNK_OVERLAP=180`, window end snapped to a whitespace boundary (`_chunk_text`, `:925-951`).
- Whitespace pre-normalized (CRLF→LF, runs collapsed) before windowing (`:925-929`).

**Hard truncation that drops later content:**
- `MAX_EXTRACT_CHARS=4_000_000` per file — once hit, **later PDF pages / docx blocks / xlsx rows are dropped** (`:982-984`, `:1101-1102`).
- `MAX_CHUNKS_PER_FILE=4000` — hard backstop (`:939-951`).

Documented to keep >99% of docs whole; the tail loss hits data-dump appendices. **Implication for labor recall:** a labor table buried past the 4M-char mark in a very large SOW is silently dropped.

### 1d. Chunk selection — none at extraction (recall-preserving)
**Every** chunk of an admitted document is emitted; there is no top-K, embedding-similarity, or keyword gate on chunks before extraction (`_build_chunks`, `:1300-1318`). Selection happens only at two coarser levels:
- **Document-lane routing by filename regex** (`:631-636`): `SCOPE_RX→L1_scope` (SOW/PWS/specs), `DROP_RX→L2_drop` (SF1449/SF30/PPQ/reps&certs boilerplate — never parsed), else `L3_triage`.
- **Chunk classification** (triage tags scope/pricing/unknown + a labor-lexicon boolean) — but a lexicon **miss still lands in the `unknown` sink** (`:1142-1145`); chunks are never dropped for failing a labor query.

Embeddings (BAAI/bge-large-en-v1.5, D=1024) are a **downstream Phase 4** (`sam_attachment_embed_90day.py`) used for retrieval, **not** for selecting extraction input.

---

## 2. The explicit logic / prompting — **two lanes, one sink**

`labor_category` rows are written by **two distinct engines** into the same `govcon_award_requirements` sink, keyed by an `extractor` tag. The serving rollup blends both (it does not filter by extractor).

### Lane 1 — deterministic REGEX (Phase 1, authoritative for the bulk) — [`sam_labor_demand_extract_90day.py`](pipelines/sam_gov/sam_labor_demand_extract_90day.py)
A curated **36-token labor lexicon** compiled to word-boundary regexes, **context-gated** by a demand cue, with negation suppression and span-containment dedup. Every match is mapped to its canonical vocabulary token — that is *why* the output is intrinsically in-vocab.

```python
# :409-447  canonical token -> body regex
LABOR_TERMS: dict[str, str] = {
    "electrician": r"electricians?", "plumber": r"plumbers?",
    "pipefitter": r"pipe\s?fitters?", ... "truck_driver": r"truck\s+drivers?",
    "dispatcher": r"dispatchers?", }
# :587-591  each entry -> Pattern gated by DEMAND_CUE_RX (context cue)
for canon, body_rx in LABOR_TERMS.items():
    pats.append(Pattern("labor_category", "labor_category",
                        re.compile(rf"\b(?:{body_rx})\b", I),
                        _make_labor_handler(canon), DEMAND_CUE_RX))
# :392-406  handler clamps every match to the canonical token
def _make_labor_handler(canon):
    def h(m, t): ... return {"value_norm": canon, "headcount": n,
                             "wage_floor": wage, "labor_category": canon}
```
Writes `extractor="regex:labor_category@v1"`, `confidence=1.0`, `validated=true`. **There is no free-text path — a regex-lane labor value is one of the 36 tokens by construction.**

### Lane 2 — LLM via session-agent **handoff** (Phase 2), engine `session-fable`/`session-opus`
**Not an in-code API call.** The harness emits self-contained per-resource **task files** (embedding the prompt template, vocabulary, output schema, exact `result_path`, and the chunk texts); an external agent session reads each and writes a result JSON; a committer daemon validates + lands it (`:1172-1175`, task-file builder `:1580-1602`; harness [`p2b_extract_grind_workflow.js`](pipelines/sam_gov/reference/p2b_extract_grind_workflow.js)). No `anthropic`/`openai` SDK is imported in the extraction module. `prompt_version="v1"`; `prompt_hash = sha256(prompt_template.md + vocabulary.json + output_schema.json)` (`:1217-1223`).

**The exact labor instruction given to the LLM** ([`prompt_template.md:37-38`](pipelines/sam_gov/reference/govcon_llm_lane_v1/prompt_template.md)):
> **labor_category rows**: `requirement_value` MUST be a member of `vocabulary.labor_categories`. If a labor category in the text has no vocabulary member, **skip it (do not invent values).**

Reinforced by the prompt's honesty contract — *"Precision over recall: every stored row becomes citable evidence in live outreach"* — and rule 5: shall/must only; negated requirements ("no clearance required") produce no row. **This is a precision-maximizing, recall-sacrificing design by intent.**

---

## 3. The validation / filtering rules

### Regex lane (deterministic, in-line)
Context-cue gate (`DEMAND_CUE_RX` must be near the term), negation suppression, `_drop_contained_labor` span-containment (drops `equipment_operator` when nested inside `heavy_equipment_operator`, `:640-642`), family handlers. `validated=true` is set literally — **no vocab recheck is needed because the handler only ever emits the 36 canonical tokens.**

### LLM lane deterministic validator (`validate_result`, `:1782-1899`) — rows failing are counted, never stored
1. **Citation substring-match** — `evidence_quote` must appear VERBATIM (whitespace-normalized) inside a cited chunk body, else `reject("quote_mismatch")`. Quote capped at 300 chars.
2. **Controlled-vocab membership (the labor gate)** — `labor_set = set(vocab["labor_categories"])`; `if rtype=="labor_category" and value_norm not in labor_set: reject("labor_category_out_of_vocab")` (`:1830-1832`). **This is the second guarantee of 100% conformance.**
3. **Clearance enum** check; `requirement_id` dedup; mandatory-language rule.

### Post-processing at serving roll-up ([`materialize_award_scope_requirements.py`](pipelines/serving/materialize_award_scope_requirements.py))
Pure assembly — **no extraction here.** Per award: `DISTINCT requirement_value WHERE requirement_type='labor_category' AND validated`, **alpha-sorted, capped at 25** (`:190-196`); `req_lists_truncated` flags any clip. **No extractor filter → both lanes are surfaced.** No string-length or junk-char filter is applied to labor (vocabulary membership already is the filter); the stored Lance type is plain `string`.

---

## 4. The data-reality pulse check (live R2)

### 4a. Provenance distribution — `govcon_award_requirements` (243,901 rows, 100% validated)
| extractor family | rows | share |
|---|---:|---:|
| **regex** | 190,155 | 78.0% |
| **llm** (session-opus + session-fable) | 53,746 | 22.0% |

**Both lanes are live in production.** (Note: a pure code reading would conclude the LLM lane was "staged, not landed" — the R2 data refutes that. The LLM lane has landed 53,746 validated rows, including **100% of `equipment_capability` (2,385)** and **100% of `past_performance` (2,389)** — the two types the regex lane provably cannot emit.)

**`labor_category` (21,770 rows, 36 distinct, 100% validated):**
| extractor | rows | share | distinct |
|---|---:|---:|---:|
| `regex:labor_category@v1` | 19,339 | 88.8% | 36 |
| `llm:session-opus@v1` | 1,867 | 8.6% | 36 |
| `llm:session-fable@v1` | 564 | 2.6% | 33 |

Regex-dominant, LLM-blended — **both clamped to the same 36 tokens, so the blend is invisible in the output (100% in-vocab either way).**

### 4b. The "Big Three" eyeball — R499 / R425 / DA01
`govcon_award_scope_requirements` carries **no PSC**, so the filter joins to `govcon_active_awards` on `contract_award_unique_key`.

**Coverage reality (a hard GTM limit):** of **7,564** active Big-Three awards, only **854 (11.3%)** have any scope-requirements annotation, and only **317** carry a labor category. The other ~88.7% have no harvested/extracted solicitation — **the labor signal is absent, not empty, for most of these awards.**

**Top exact strings across the Big Three (405 occurrences, 23 distinct, 100% in-vocab):**
| labor_category | award occurrences | normalized? |
|---|---:|:--:|
| security_guard | 247 | ✓ |
| program_manager | 43 | ✓ |
| project_manager | 32 | ✓ |
| instructor | 19 | ✓ |
| licensed_practical_nurse | 13 | ✓ |
| medical_assistant | 12 | ✓ |
| electrician | 7 | ✓ |
| safety_officer / interpreter | 4 each | ✓ |
| mason / welder | 3 each | ✓ |
| custodian, translator, registered_nurse, dispatcher, hvac_technician, equipment_operator | 2 each | ✓ |
| carpenter, food_service_worker, truck_driver, quality_control_manager, painter, surveyor | 1 each | ✓ |

Per-PSC texture: **R499** dominated by `security_guard` (235); **R425** by `program_manager`/`project_manager`; **DA01** (IT services) by `project_manager`/`program_manager`/`security_guard`/`instructor` — **generic management/guard roles only, because no IT labor token exists in the vocabulary.**

**Noise-to-signal verdict: there is no noise.** 23/23 distinct Big-Three strings and 36/36 global strings are clean snake_case vocabulary members. The signal-quality problem is **coverage and IT-blindness**, not junk.

### 4c. Global texture (lane characterization) — 58,464 occurrences / 25,185 awards
Construction/facilities/security/logistics dominate: `security_guard` 20,482, `truck_driver` 15,860, `program_manager` 5,583, `project_manager` 3,391, `quality_control_manager` 1,145, `instructor` 1,101, `licensed_practical_nurse` 1,000, then the trades (welder, electrician, plumber, mason, carpenter, …). (Serving occurrences exceed the 21,770 resource-grain rows because one attachment fans out to many awards via `source_resource_ids`.)

---

## 5. GTM implications & recommendation

1. **Use `labor_category_values` as-is for trades / facilities / security / medical / logistics verticals.** It is high-precision, controlled-vocabulary, citation-backed (every LLM row carries a verbatim `evidence_quote`). **No prompt rewrite is warranted to reduce noise — there is none.**
2. **Do NOT use `labor_category` as the primary demand signal for IT (DA01) or professional/financial/legal verticals.** The 36-token vocabulary contains **zero** IT/cyber/software roles (absent: software_engineer, systems/network/cloud engineer, sysadmin, devops, data scientist/analyst, cybersecurity/SOC analyst, help-desk/IT support, DBA, ISSO/ISSM, penetration tester, professional engineer). For an IT-services award, **absence of a labor token is not absence of labor demand — it is vocabulary blindness (systematic false negative).** Drive IT targeting from PSC (DA01/DG11/7A21/DA10), NAICS, `scope_summary`, and `capability_tags` (`it_services`, `cybersecurity_services`, `software_development`) instead.
3. **If labor-level IT targeting is required, this is a vocabulary-expansion task, not a denoising task.** Add the IT/professional tokens to `vocabulary.json` *and* `LABOR_TERMS`, bump `prompt_version`/`prompt_hash`, and re-run both lanes. The prompt's precision logic is already correct.
4. **Mind two recall ceilings already in the pipeline:** (a) the 4M-char / 4000-chunk truncation drops labor tables in the tail of very large SOWs; (b) the regex lane requires a *demand cue* adjacent to the term — bare mentions are intentionally not captured. Both are precision-by-design, but they cap recall.
5. **Coverage is the dominant limiter, not extraction quality:** only ~11% of active Big-Three awards have any scope annotation. Expanding attachment harvest/extraction coverage will move the GTM needle far more than touching the labor extractor.

---

## 6. Methodology note (faithful reporting)
The four-reader code archaeology inferred from source that the LLM lane was "staged but not landed." The live R2 extractor-tag probe **corrected that**: the LLM lane has landed 53,746 validated rows (22% of all requirements; 11.2% of labor_category; 100% of equipment_capability/past_performance). Conclusions here are anchored to the R2 ground truth, not to code inference. Reproduce both probes with the commands in their docstrings under `doppler run -p core-x -c prd`.
