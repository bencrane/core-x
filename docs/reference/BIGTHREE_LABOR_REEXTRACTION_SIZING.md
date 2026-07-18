# Big-Three labor re-extraction — compute & API-cost sizing

**Mode:** READ-ONLY recon (R2 ground-truth probes + pipeline code archaeology). **Snapshot:** 2026-06-21 (UTC).
**Question:** size the compute / API cost / wall-clock to re-run the LLM labor lane with an *uncapped (free-form job-title)* prompt over the active **Big-Three** IT/Professional cohort `psc_code ∈ ('DA01','R425','R499')`, to decide greenlight.
**Probes (reproducible, read-only, zero spend):**
[`scripts/archive/bigthree_reextract_sizing_recon.py`](../../scripts/archive/bigthree_reextract_sizing_recon.py) (schema lock),
[`scripts/archive/bigthree_reextract_sizing_probe.py`](../../scripts/archive/bigthree_reextract_sizing_probe.py) (cohort funnel + bytes),
[`scripts/archive/bigthree_reextract_chunks_probe.py`](../../scripts/archive/bigthree_reextract_chunks_probe.py) (sink-direct chunk inventory),
[`scripts/archive/bigthree_reextract_census_probe.py`](../../scripts/archive/bigthree_reextract_census_probe.py) (exact LLM-lane token census — imports the pipeline's own `select_chunks`/`compute_input_set`).
Raw JSON: `/tmp/bigthree_{sizing,chunks,census}.json`.

---

## BLUF — the run is trivially cheap; the only real constraints are *time* and *recall*, not dollars

The default lane re-prompts a **budgeted, vertical-neutral selection of 9,117 chunks across 799 staged task-files** (NOT the 101,950-chunk raw inventory — the LLM lane caps each doc at 8,000 tokens). Measured input is **5.26M tokens**; with estimated output the run is **~$50–155 economic API-equivalent** (and **~$0 marginal cash** — `session-opus` is the Max-subscription agent-handoff lane), **~1–1.5 h pure compute / ~3–6 h wall-clock**, fully resumable.

**Greenlight: yes — cost is a non-factor at any scenario.** The two decisions that actually matter:
1. **"Uncap the prompt" ≠ "uncap the budget."** Holding the default 8,000-tok/doc selection, **122 of 799 docs (15%) are budget-truncated** — large IT SOWs lose tail content. Maximum IT-labor recall requires *also* raising `GOVCON_LLM_DOC_TOKEN_BUDGET` (Scenario B), still cheap.
2. **197 cohort docs carrying CUI/`content_marking` are hard-bracketed out** of the external agent lane (egress safety, Decision A) and will **not** be re-extracted regardless of the prompt.

---

## 1. The addressable cohort size

`govcon_active_awards` is already award-grain and active-by-construction (membership = `GREATEST(pop_current_end, pop_potential_end) ≥ as_of OR both NULL`). The cohort is three nested rings — **do not collapse them**:

| Ring | Definition | Count |
|---|---|---:|
| **TAM** (demand universe) | active-substrate awards, `psc_code ∈ (DA01,R425,R499)` | **16,016** |
| — strictly future-dated | `active_current OR active_potential` | 7,564 |
| — NULL-PoP (active but no end date) | `pop_unknown` | 8,452 |
| **Reachable** | cohort awards present in the SAM attachment manifest | 597 |
| **Run-sizing cohort** | cohort awards with ≥1 **successfully downloaded** attachment | **550** |

Per-PSC (total members / future-dated):

| PSC | Members | Future-dated | NULL-PoP |
|---|---:|---:|---:|
| R499 (Professional/Admin/Mgmt support — other) | 9,473 | 4,276 | 5,197 |
| DA01 (IT & Telecom — facility ops/maintenance) | 4,293 | 1,796 | 2,497 |
| R425 (Engineering & Technical services) | 2,250 | 1,492 | 758 |
| **Total** | **16,016** | **7,564** | **8,452** |

> **16,016 / 7,564 answer "how much labor demand exists."** The re-run cost is governed solely by the supply of downloaded, chunk-bearing text gated to the **550** awards that actually have attachments — only ~3.4% of the cohort surfaced a downloadable solicitation attachment.

## 2. The file & chunk volume

Join path `govcon_active_awards.contract_award_unique_key → sam_opps_attachment_manifest_winners.{contract_award_unique_key} → sam_attachment_files.resource_id (status='downloaded')`. File/byte/chunk aggregates are over the **DISTINCT** cohort `resource_id` set (a solicitation maps to >1 award; an award to many files).

| Metric | Value |
|---|---:|
| Distinct manifest attachment resources (cohort) | 4,986 *(5,331 incl. `award_keys[]` fan-out, +7%)* |
| **Distinct successfully-downloaded files** | **4,738** |
| **True byte volume** (`Σ content_length`) | **3.29 GB** (3,292,213,525 B) |
| — avg / max per file | 695 KB / 74.2 MB; 0 zero-byte |
| — by type | pdf 3,142 files / 2.73 GB · zip 1,531 / 0.53 GB · ole 52 · jpg 8 · rtf/txt/html 5 |
| Files that produced chunks (scope+pricing+unknown sinks) | 1,359 (28.7% of 4,738) |
| **Raw chunk inventory** (sink-direct, exact `length(text)`) | **101,950 chunks / 121.0M chars** |

The other 3,379 downloaded files (71%) yield **zero** LLM input — routed to boilerplate-drop (SF1449/SF30), skipped out-of-scope, dropped as duplicate/noise, or `requires_ocr` (image-only, no text). Chunk inventory by sink (resources are document-disjoint across sinks):

| Sink | Chunks | Files | Payload chars | avg chars/chunk |
|---|---:|---:|---:|---:|
| `govcon_scope_vectors` | 34,413 | 342 | 40.9M | 1,189 |
| `govcon_unknown` | 61,664 | 937 | 73.1M | 1,186 |
| `govcon_pricing` | 5,873 | 80 | 7.0M | 1,187 |
| **Total** | **101,950** | **1,359** | **121.0M** | ~1,187 |

avg ~1,187 chars/chunk confirms the fixed **1,200-char** strategy (`CHUNK_CHARS=1200`, `CHUNK_OVERLAP=180`).

### ⚠ Chunks fed to the LLM ≠ the 101,950 inventory

The LLM lane does **not** feed the inventory whole. `select_chunks()` ([`sam_labor_demand_extract_90day.py:1242`](../../pipelines/sam_gov/sam_labor_demand_extract_90day.py)) applies a **hard 8,000-token/doc budget** (`DOC_TOKEN_BUDGET`, chars/4) via priority tiers — tier 0 = opening 6 chunks, tier 1 = `SCOPE_HDR_RX` (PWS/SOW/SOO/specs), tier 2 = `LABOR_LEXICON_RX` (`labor categor|LCAT|FTE|headcount|clearance|wage|SCA|…`). Only `sel["selected"]` is embedded in the task file. The staged input set (Decision A, `compute_input_set`) is **scope-ALL ∪ pricing-ALL ∪ (unknown ∩ lexicon/regex-hit)** minus `marked` (CUI) resources.

**Measured exactly for the cohort** (census probe imports the real functions — equivalent to `--phase census`, cohort-restricted):

| Stage | Count |
|---|---:|
| Files with chunks | 1,359 |
| → Decision-A input set ∩ cohort | 996 |
| → minus `marked` (CUI/`content_marking`, egress-blocked) | −197 |
| **→ staged LLM task-files (= API calls)** | **799** (scope 249 / pricing 75 / unknown 475) |
| **→ chunks actually fed (after 8K-tok tiered cap)** | **9,117** |
| docs truncated at the 8,000-tok budget | 122 (15%) |

**Chunks fed to the LLM under the default lane = 9,117** (an 11× reduction from the 101,950 inventory).

## 3. API cost & time estimation

Token convention is the pipeline's own: **chars / 4** (`estimate_tokens`). Engine `session-opus` → harness `model: 'opus'` (`claude-opus-4-8`), **CONC=5** concurrent agents, ~10 task-files/batch, all-fail circuit breaker at the session cap, fully resumable (per-file result-exists skip). Fixed per-task overhead = `prompt_template.md` + `vocabulary.json` + `output_schema.json` = **12,142 B ≈ 3,036 tok**, re-paid per task (isolated task files).

### Token volume (Scenario A — prompt-uncap, default 8K selection)

| Component | Tokens |
|---|---:|
| Selected chunk payload (**measured**) | 2,676,737 |
| Fixed prompt/vocab/schema overhead (799 × 3,036) | 2,425,764 |
| Task-file scaffold + per-chunk wrappers | 157,344 |
| **Total INPUT (measured)** | **5,259,845** |
| **Output** (estimated; free-form uncap lifts labor rows) | ~1.0M (band 0.6–1.6M) |
| **Total** | **~6.3M** |

### Cost — economic API-equivalent (marginal cash ≈ $0 on the subscription lane)

Priced at list. *The current Opus 4.8 rate card should be confirmed against Anthropic's price sheet — it swings the figure ~3×, but the run is trivial under either:*

| Scenario | INPUT tok | OUTPUT tok | @ Opus $5/$25 per M | @ legacy $15/$75 per M | Pure compute |
|---|---:|---:|---:|---:|---:|
| **A — prompt-uncap, default 8K selection** *(realistic default)* | **5.26M** | ~1.0M | **~$51** | ~$154 | ~1–1.5 h |
| **B — also uncap the doc budget** (full eligible inventory of the 799 staged docs) | ~18.3M | ~1.5M | ~$129 | ~$387 | ~2–3 h |
| **C — full inventory, all 1,359 files, no staging/budget caps** *(ceiling)* | ~35.7M | ~1.8M | ~$224 | ~$671 | ~3–4 h |

Output ≈ input in dollar terms at these volumes (output billed 5× input). Prompt-caching the 12,142-B fixed prefix saves single-digit dollars — **not worth engineering** on a sub-$200 run.

### Wall-clock

- **Pure compute (Scenario A):** 799 small calls (~6.4K in / ~1.3K out each) at CONC=5 → **~1–1.5 h**.
- **Realistic wall-clock:** Max-plan rolling-window rate limits + circuit-breaker cooldowns dominate → **~3–6 h** (half-day), fully resumable.
- **Context window:** non-issue — 8K-budgeted tasks are far under Opus's window. *Only Scenario C re-introduces a context-split tail for the largest SOWs (max inventory ~3,000 chunks ≈ 0.9M tok).*

---

## Engineering caveats (the decision, not the dollars)

1. **Cost is not the constraint.** Even the uncapped ceiling (C) is ~$224–671 economic / ~$0 cash. Greenlight is unconditional on budget; size the decision on **recall** and **time**.
2. **Selection is vertical-neutral — IT labor IS reachable.** Tier-1 keys on PWS/SOW structure; tier-2 on `LCAT/FTE/headcount/clearance/wage` (generic labor-demand cues that fire on IT/professional-services docs), **not** the 36-token construction trade list. So a prompt-only uncap does recover IT job titles in selected chunks. The recall limiter is the **8,000-tok budget truncating 122/799 docs (15%)**, not lexicon bias.
3. **"Uncap prompt" vs "uncap budget" are different jobs.** If max IT-labor recall is the goal, run Scenario B (`GOVCON_LLM_DOC_TOKEN_BUDGET`↑). Still cheap; recovers the truncated tail.
4. **197 CUI/`marked` cohort docs are permanently bracketed out** of the external agent lane (Decision A egress safety). They are excluded from the 799 and will not be re-extracted by any prompt change — an inherent recall floor, not a tuning knob.
5. **Pricing chunks stay in.** The lane stages all 80 pricing-sink files unconditionally; wage-determination / LCAT × wage grids are labor *signal* for this run, not noise.
6. **Output is the cost driver.** Free-form labor multiplies requirement rows; output tokens (and thus cost) are the soft, run-dependent figure — the input 5.26M is measured-exact, output ~1.0M is the estimate carrying the band.

## Method notes / provenance

- All figures are read-only against the Gen-3 Lance SoR under `s3://data-sink/active/`; zero writes, zero LLM spend.
- **Chunk count is measured two ways and reconciled:** the `sam_attachment_extraction` event ledger is an append-only multi-stage log (a separate `marking_fullbody` audit pass re-emits `n_chunks` over the same resources), so its per-resource sums are **not** the LLM-fed count. The authoritative inventory (101,950) is counted **sink-direct**; the authoritative *fed* count (9,117 / 5.26M tok) is computed by importing the pipeline's own `select_chunks`/`compute_input_set` — not re-implemented.
- Cohort join uses the scalar `contract_award_unique_key`; the `award_keys[]` list path widens the resource set ~7% (4,986 → 5,331), an upper-bound sensitivity that does not change the greenlight.
- Adversarially verified (3-way independent re-derivation + reconciliation): the verification caught the original draft's BLOCKER — modeling input on the 121.0M-char inventory instead of the 8K-budgeted selection (a ~7× over-estimate) — which the census then measured exactly.
