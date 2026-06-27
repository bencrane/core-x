# BUILD PLAN — Open-Biddable Designation & Subcontracting-Plan Detection (v2)

**Date:** 2026-06-27 (UTC) · **Repo:** `core-x` (data/compute plane) · **v2:** adversarial review applied (see §10)

Detect the socioeconomic **designation surface** (HUBZone / 8(a) / SDVOSB / WOSB / EDWOSB / VOSB / SDB / …) **and** the FAR 52.219-9 subcontracting-plan requirement across the **full attachment text** of SAM.gov solicitations currently **open for bidding**, then materialize a per-notice profile.

**Pipeline:** `manifest (residential) → download bytes (residential) → extract text + detect (Modal, R2→R2) → materialize (Lance SoR)`

**Core principle — download broad, detect narrow & iteratively.** The byte download is the expensive, rate-limited, residential-bound stage; detection is cheap, Modal-eligible, re-runnable on stored bytes. Scope the download once; iterate the lexicon freely afterward. The keyword/clause spec is **not** a download dependency — it gates scope selection and detector validation, not byte I/O.

**v2 governing rule — PRESENCE ≠ TRUTH.** A token in solicitation text means *the solicitation references it*, not *the opportunity requires/qualifies for it*. The deliverable's ground-truth spine is the structured `set_aside_code` (authoritative, free, already in the opps feed); attachment-text signals are **enrichment**, explicitly namespaced apart, never collapsed into the structured flags.

---

## 0. Current state — Cycle 1 (manifest) DONE

| | |
|---|---|
| Dataset | `s3://data-sink/active/sam_opps_attachment_manifest_open_biddable/` (Lance, indexed) |
| Scope | 10,297 open-biddable notices (Solicitation / Combined Synopsis, `response_deadline >= now`) |
| Shape | 50,367 citations → **27,158 distinct files** → 10,076 notices with ≥1 attachment (99%) |
| Code | `pipelines/sam_gov/sam_attachment_manifest.py --scope-open-biddable` (PR #756, on `main`) |

Worklist sizing (gates: non-phantom · public · not-EC · text mime); **the unrestricted∩text figure is already validated by the sizing probe** (§4 codifies the exact join):

| Gate | Distinct files | GB (declared **lower bound**; real ~2–4×) |
|---|--:|--:|
| Downloadable | 20,437 | 47.3 |
| Text-extractable (pdf/docx/doc/txt) | 18,726 | 43.3 |
| **Unrestricted ∩ text** (subcontracting target) | **9,462** (5,946 notices) | **23.5** |

---

## 1. Phase 0 — Designation lexicon (the spec; zero bandwidth)

**Artifact:** `pipelines/sam_gov/reference/designation_lexicon.json` — frozen, versioned.

**Spine = canonical verbatim flag *stems*** from `sam_business_type_code_dict` / `govcon_subawardee_designations` (see `docs/reference/SUBAWARDEE_DESIGNATIONS.md`). The 12 stems:
`service_disabled_veteran_owned_business`, `veteran_owned_business`, `women_owned_small_business`, `economically_disadvantaged_women_owned_small_business`, `woman_owned_business`, `historically_underutilized_business_zone_hubzone_firm`, `c8a_program_participant`, `self_certified_small_disadvantaged_business`, `minority_owned_business`, `joint_venture_women_owned_small_business`, `joint_venture_8a`, `small_disadvantaged_business`.

**FLAG TAXONOMY (the anti-conflation fix — P0-1).** Every stem materializes under an explicit namespace; the bare stem is never emitted:
- `set_aside__<stem>` — derived from the structured `set_aside_code` (opps feed). **Authoritative ground truth.** Free; this is the deliverable's spine.
- `opp_text_ref__<stem>` — derived from attachment text. Means "the solicitation text references this designation." **Enrichment, not truth.** Carries a `reference_context` ∈ {`binding`, `listed`, `negated`} from the §5 negation pass.
- A consumer joins to the entity datasets (`govcon_subawardee_designations`, firm-attribute) **deliberately** via the shared stem — the prefix forces the join author to choose which semantic they mean. Document the recipe in §6.

**Per designation:** boundary-aware regex surface forms + FAR clause numbers. Example:
```json
{ "stem": "historically_underutilized_business_zone_hubzone_firm", "short": "HUBZone",
  "patterns": ["\\bHUBZone\\b", "\\bHUB[\\s-]?Zone\\b", "\\bHistorically Underutilized Business Zone\\b"],
  "far_clauses": ["19.13", "52.219-3", "52.219-4"] }
```

**Subcontracting-plan detector group (presence ≠ requirement — P0-2).** Emit *separate* signals, not one boolean:
- `subk_plan_clause_present` — `\b52\.219-9\b` / `\bsubcontracting plan\b` / `\bSmall Business Subcontracting Plan\b` token present (honest; high recall, low precision — clause matrices list it as boilerplate).
- `subk_plan_goals_table_present` — fill-in goals rows detected (`Small Business`/`SDB`/`WOSB`/`HUBZone`/`SDVOSB` adjacent to a `\d+(\.\d+)?\s?%`).
- `subk_plan_section_l_submit_directive` — Section L / "Instructions to Offerors" directive to *submit* a plan (`submit .{0,40} subcontracting plan`).
- `subk_plan_required` (derived) := `clause_present AND (goals_table_present OR section_l_submit_directive)`. **Presence alone never sets `_required`.** (v1 detects goals-table *presence*; parsing the actual percentages is deferred to v2.)

**Match policy (in the lexicon):** case-insensitive; **word-boundary regex, never naive `LIKE`**; whitespace-collapsed multi-word forms; **minimal proximity-negation in v1 (P1-8)** — a stem hit within N chars (default 120) of `not set aside` / `unrestricted` / `does not apply` / `full and open` / `all offerors` downgrades `reference_context` to `negated`, captured in the evidence snippet. JV stems (`joint_venture_*`, "8(a) JV", "joint venture") are high-collision — require extra-tight patterns at peer review. `small_disadvantaged_business` is **NULL in the entity dataset** (program folded into 8(a)); a text-derived SDB flag has no entity counterpart — document as text-only, no parity implied.

**Acceptance:** all 12 stems + subk group present; loader + unit tests on a fixture string set (positive, negated, and acronym-collision cases).

---

## 2. Phase 1 — Regex-mechanics sanity gate ONLY (zero bandwidth)

**Demoted (P0-3).** This gate proves the regex *mechanics* — patterns fire on positives, don't collide on `8a`-in-token, negation downgrades correctly — against a hand-built fixture set. **It does NOT validate recall or attachment-text precision** and must not be read as doing so.

Why (verified on live data): of notices with a *known* structured set-aside, the matching token appears in the `description` only **1.1% (8a) / 21.9% (SDVOSB) / 26.7% (WOSB) / 44.4% (HUBZone)** of the time. Synopsis text massively under-represents designations that are ground-truth-true, and is a different distribution from SOW / Section-L / clause-matrix prose. So a `description` hit-count measures the synopsis's poverty, not the detector's quality. Real validation is Phase 1.5, on real attachment text.

**Gate:** fixtures pass → proceed.

---

## 3. Phase 1.5 — Sampled-attachment detector validation (the real de-risk; small bandwidth)

**New (P0-4).** Prove precision/recall on the TRUE corpus before committing the full pull.

1. **Stratified sample** ~300–500 distinct files from the worklist, stratified by (`set_aside_code` present vs absent) × mime. Reuse `sam_attachment_download.py --max-files` to throwaway URIs (residential).
2. Run the full extract+detect path (§5) over the sample.
3. **Hand-label** the sample: per file, true designations referenced + whether a subcontracting plan is genuinely required (goals table / Section-L).
4. **Measure:** precision (esp. the **boilerplate false-positive rate on unrestricted notices** — the central risk), and recall against the structured set-aside ground truth on the set-aside stratum.

**Gate (operator):** precision/recall clear the §7 bar (e.g. structured-set-aside recall ≥ 0.9 on the labeled stratum; unrestricted-notice FP rate ≤ agreed X) → authorize the full download. Else refine the lexicon (free, re-run on the already-downloaded sample) and re-measure. **This gate, not Phase 1, is what justifies the 50–90 GB spend.**

---

## 4. Phase 2 — Scoped residential byte download (Cycle 2)

**Egress (MANDATORY):** residential / local, **never Modal** (SAM WAF 429s datacenter IPs). Public backend, no api_key. ~4 req/s single-threaded, detached, resume-safe. Per `docs/reference/SAM_ATTACHMENT_DOWNLOAD_DIRECTIVE.md`.

**Worklist — the manifest lacks `set_aside_code`, so it MUST re-join the opps feed (P0-5):**
```sql
-- materialize opps FIRST (timestamptz can't push into the Lance scan), then:
LEFT JOIN sam-gov-opps/active AS o ON manifest.notice_id = o.notice_id
unrestricted := (o.set_aside_code IS NULL OR trim(o.set_aside_code) IN ('','NONE'))
-- NULL (4,492) + 'NONE' (1,454) = 5,946 unrestricted notices. Treating only NULL as
-- unrestricted silently drops 1,454; reconcile to 5,946 notices / 9,462 files at run time.
```
Gates: `file_name IS NOT NULL AND size_bytes >= 1` · `access_level='public'` · `export_controlled=false` · `mime_type IN ('pdf','docx','doc','txt')` · unrestricted.

**Recommended scope: unrestricted ∩ text = 9,462 files (~23.5 GB declared; ~50–90 GB true). ← OPERATOR-CONFIRMED AT THIS GATE.** Declared sizes are lower bounds; enforce the real 50 MB/file ceiling on Content-Length at fetch. Reuse `sam_attachment_download.py` (CAS by distinct `resource_id`).

**Output:** `s3://data-sink/active/sam_attachment_blobs/<resource_id>` + `s3://data-sink/active/sam_attachment_files/` ledger. **Acceptance:** `downloaded / (worklist − gone) ≥ 0.99`; reconcile orphans = missing = corrupt = 0.

---

## 5. Phase 3 — Text extraction + detection (Modal-eligible, R2→R2; **NO OCR**)

Reads R2 blobs — no SAM egress, Modal-eligible. Reuse `pipelines/sam_gov/sam_attachment_extract.py` text pass. **OCR lane DROPPED entirely.**

**Per file:**
1. Born-digital text extract by sniffed type: pdfium → pdf, python-docx → docx, striprtf → rtf, serialized soffice → legacy doc.
2. **ZERO-TEXT = DENSITY FLOOR, not a 1-char short-circuit (P1-6).** Reuse the engine's existing constants (`sam_attachment_extract.py` §7.3): `OCR_ABS_FLOOR = max(200, n_pages*80)`; classify **blind spot** if `text_chars < OCR_ABS_FLOOR` OR `fraction_of_pages(<40 chars) > 0.5`. `has_extractable_text := NOT blind_spot`. A scanned PDF with a stray header stamp (>1 char but ~0 useful) is correctly bucketed as blind spot — `X=1` would misclassify it as text-bearing and emit a confident **false** "no designation." (The short-circuit optimization is dropped — density needs the page read — but pdfium on scanned pages returns empty cheaply, so cost is unaffected and no OCR is invoked.)
3. Apply `designation_lexicon` over extracted text (only when `has_extractable_text`): per-(notice, file) `opp_text_ref__<stem>` hits with `reference_context` (binding/listed/negated) + the subk signals + an evidence snippet **with page + char offset** (P2, for human FP adjudication without re-extract).

**Output:** append-only ledger `s3://data-sink/active/sam_attachment_designation_hits/` — one row per (resource_id, stem) with `notice_id`, `reference_context`, `evidence` (page/offset), `has_extractable_text`. Idempotent (merge_insert on hit key); single committer; workers pure compute.

---

## 6. Phase 4 — Materialize per-notice profile (Lance SoR)

`s3://data-sink/active/govcon_open_biddable_designations/` — **1 row per `notice_id`**, three explicit flag families:
- **`set_aside__<stem>` (12, bool)** — from structured `set_aside_code`. **Authoritative.**
- **`opp_text_ref__<stem>` (12, bool) + `opp_text_ref__<stem>__context`** — from attachments (binding/listed/negated).
- **Reconciliation:** `set_aside_vs_text_agreement`, and `text_only_designations` (referenced in text, no structured set-aside — the enrichment signal, flagged as *reference*, not qualification).
- **Subcontracting:** `subk_plan_clause_present`, `subk_plan_goals_table_present`, `subk_plan_section_l_submit_directive`, `subk_plan_required` (derived conjunction).
- **Coverage provenance:** `n_attachments`, `n_files_with_text`, `n_files_zero_text`, `all_attachments_zero_text` (per-notice blind-spot flag), `manifest_snapshot_date`, `opps_snapshot_date` (P1-7).
- **Indexes:** BTREE `notice_id`; BITMAP the `set_aside__*` + `subk_plan_required` + `all_attachments_zero_text`.

**Join recipe (documented):** to compare opportunity references against certified firms, join `opp_text_ref__<stem>` (here) to `govcon_subawardee_designations.<stem>` (firm-attribute) **explicitly** — the namespace prefix forces an intentional choice and prevents the firm-vs-opportunity conflation. Lance v2.1, idempotent snapshot-overwrite.

---

## 7. Phase 5 — Acceptance, coverage report, land

**Numeric acceptance (P2 — not just mechanical coverage):**
- Structured-set-aside recall ≥ 0.9 on the Phase-1.5 labeled stratum; unrestricted-notice FP rate ≤ agreed bar.
- Coverage: notices / files / `n_files_zero_text` ("N of M target PDFs had no extractable text layer — OCR blind spot, not processed").
- **`subk_plan_required` incidence vs `subk_plan_clause_present` incidence vs the `description`-only floor (7.3% on unrestricted synopses)** — show the precision the conjunction-gate buys over bare presence.
- **Drift (P1-7):** notices in the manifest no longer `response_deadline >= now()`; sampled `/resources` head-check for attachment-set changes since harvest. Don't present a stale profile as "currently open."

**Git lifecycle:** own it — branch off latest `main`, PR, squash-merge, pull into operator checkout, verify on disk.

---

## 8. Guardrails & non-goals (hard stops)

- **PRESENCE ≠ TRUTH.** Structured `set_aside_code` is ground truth; attachment-text flags are namespaced enrichment (`opp_text_ref__*`), never collapsed into `set_aside__*`.
- **`subk_plan_required` requires clause + (goals-table OR Section-L), never bare clause presence.**
- **No OCR.** Zero-text = **density-floor**-measured blind spot, never silent, never `X=1`.
- **Boundary-aware regex, never naive `LIKE`.** Proximity-negation in v1.
- **Verbatim canonical *stems*** (namespaced) — joins to entity datasets are deliberate, never accidental.
- **Download residential only; extraction Modal-eligible. Detection re-runnable on stored bytes.**
- **Re-derive all counts from the live manifest + opps join at run time** — figures here drift daily.
- **Blast-radius isolation:** per-phase ledgers; download failure ≠ extraction corruption ≠ SoR corruption.

## 9. Data model / URIs

| Artifact | URI / path | Grain |
|---|---|---|
| Scoped manifest (Cycle 1 ✓) | `s3://data-sink/active/sam_opps_attachment_manifest_open_biddable/` | notice-attachment citation |
| Designation lexicon | `pipelines/sam_gov/reference/designation_lexicon.json` | stem → patterns |
| File ledger (download SoR) | `s3://data-sink/active/sam_attachment_files/` | physical file (`resource_id`) |
| Blob CAS | `s3://data-sink/active/sam_attachment_blobs/<resource_id>` | bytes |
| Designation hits ledger | `s3://data-sink/active/sam_attachment_designation_hits/` | (resource_id, stem) |
| **Per-notice profile (deliverable)** | `s3://data-sink/active/govcon_open_biddable_designations/` | `notice_id` |

## 10. Operator decision gates

1. **Phase 0→1:** lexicon patterns peer-reviewed.
2. **Phase 1→1.5:** regex fixtures pass (mechanics only).
3. **Phase 1.5→2:** detector precision/recall on hand-labeled real-attachment sample clears the §7 bar — **the gate that justifies the full bandwidth.**
4. **Phase 2 scope:** confirm download scope (recommended: unrestricted ∩ text, 9,462 files) — **currently pending.**

## 11. Adversarial review applied (v1→v2)

P0: (1) flag-namespace split — firm-attribute vs opportunity-reference; (2) `subk_plan_required` = clause + goals/Section-L, not bare presence; (3) Phase 1 demoted to regex-mechanics (description doesn't predict attachment performance — verified 1.1–44.4% structured-recall in synopsis); (4) **new Phase 1.5** sampled-attachment precision/recall gate before the full pull; (5) explicit opps re-join for "unrestricted" (NULL ∪ 'NONE' = 5,946; manifest has no `set_aside_code`). P1: (6) zero-text density floor (reuse engine constants), not `X=1` + short-circuit; (7) snapshot/amendment provenance + drift report; (8) proximity-negation in v1. P2: numeric precision/recall acceptance; SDB has no entity counterpart; JV high-collision patterns; evidence page/offset.
