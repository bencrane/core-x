# BUILD PLAN — Open-Biddable Designation & Subcontracting-Plan Detection

**Date:** 2026-06-27 (UTC) · **Repo:** `core-x` (data/compute plane)

Detect the full socioeconomic **designation surface** (HUBZone / 8(a) / SDVOSB / WOSB / EDWOSB / VOSB / SDB / …) **and** the FAR 52.219-9 subcontracting-plan requirement across the **full attachment text** of every solicitation currently **open for bidding** on SAM.gov — then materialize a per-notice designation profile that joins, with zero vocabulary drift, to the existing entity-level designation datasets.

**Pipeline:** `manifest (residential) → download bytes (residential) → extract text + detect (Modal, R2→R2) → materialize (Lance SoR)`

**Core principle — download broad, detect narrow & iteratively.** The byte download is the expensive, rate-limited, residential-bound stage; detection is cheap, Modal-eligible, and re-runnable on stored bytes. Scope the download once to the target notices' full text; iterate the lexicon freely afterward without a second crawl. The keyword/clause spec is **not** a download dependency — it gates *scope selection* and *detector validation*, not the byte I/O.

---

## 0. Current state — Cycle 1 (manifest) DONE

Scoped attachment-pointer manifest for the open-biddable universe is built, indexed, and durable:

| | |
|---|---|
| Dataset | `s3://data-sink/active/sam_opps_attachment_manifest_open_biddable/` (Lance, indexed) |
| Scope | 10,297 open-biddable notices (Solicitation / Combined Synopsis, `response_deadline >= now`) |
| Shape | 50,367 citations → **27,158 distinct files** → 10,076 notices with ≥1 attachment (99%) |
| Code | `pipelines/sam_gov/sam_attachment_manifest.py --scope-open-biddable` (PR #756, on `main`) |

Worklist sizing already computed (gates: non-phantom · public · not-export-controlled · text mime):

| Gate | Distinct files | GB (declared **lower bound**) |
|---|--:|--:|
| Downloadable | 20,437 | 47.3 |
| Text-extractable (pdf/docx/doc/txt) | 18,726 | 43.3 |
| **Unrestricted ∩ text** (subcontracting target) | **9,462** | **23.5** |
| Unrestricted ∩ high-value-named only | 728 | 1.6 |

---

## 1. Phase 0 — Designation lexicon (the spec; zero bandwidth)

**Artifact:** `pipelines/sam_gov/reference/designation_lexicon.json` — frozen, versioned (same pattern as `govcon_llm_lane_v1/vocabulary.json`).

**Spine = the canonical verbatim flag names** already decoded in `sam_business_type_code_dict` / `govcon_subawardee_designations` / `govcon_active_awards` (see `docs/reference/SUBAWARDEE_DESIGNATIONS.md`). Text-derived flags MUST reuse these names verbatim so the solicitation-text profile joins cleanly to the entity-level designation datasets. **Do not invent parallel names.**

The 12 canonical designations (anchor):
`service_disabled_veteran_owned_business`, `veteran_owned_business`, `women_owned_small_business`, `economically_disadvantaged_women_owned_small_business`, `woman_owned_business`, `historically_underutilized_business_zone_hubzone_firm`, `c8a_program_participant`, `self_certified_small_disadvantaged_business`, `minority_owned_business`, `joint_venture_women_owned_small_business`, `joint_venture_8a`, `small_disadvantaged_business`.

**Per designation:** boundary-aware regex surface forms + FAR clause numbers. Example:

```json
{
  "flag": "historically_underutilized_business_zone_hubzone_firm",
  "short": "HUBZone",
  "patterns": ["\\bHUBZone\\b", "\\bHUB[\\s-]?Zone\\b", "\\bHistorically Underutilized Business Zone\\b"],
  "far_clauses": ["19.13", "52.219-3", "52.219-4"]
}
```

**Subcontracting-plan detector group** (separate, derived flag):
`patterns`: `\bsubcontracting plan\b`, `\b52\.219-9\b`, `\bSmall Business Subcontracting Plan\b`; `support`: `52.219-8`, `52.219-16`.

**Match policy (baked into the lexicon):** case-insensitive; **word-boundary regex, never naive `LIKE`** (avoids `8a`-in-token, `SB`/`WOB`/`SDB`-collide, "minority"-in-prose false positives); whitespace-collapsed multi-word forms. v1 = boundary-aware presence; precision (negation/context like "unrestricted", "not a set-aside") refined iteratively — cheap, re-runs on stored text.

**Acceptance:** all 12 designations + subk group present; patterns peer-reviewed; loader + unit tests on a fixture string set.

---

## 2. Phase 1 — Cheap validation on `description` (zero bandwidth; GATE)

De-risk the detector before spending bandwidth. Run the lexicon over the `description` synopsis field of the 10,297 open-biddable notices (already in Lance) — no download required.

**Metrics:**
- Per-designation hit counts on `description`.
- **Precision sanity:** co-occurrence of each text-designation hit with the structured `set_aside_code` (e.g., HUBZone text-hit ∩ `HZC` set-aside; SDVOSB text-hit ∩ `SDVOSBC`). Divergence flags either pattern noise or genuine text-only signal.
- False-positive spot sample (N=50 manual).

**Gate:** discrimination acceptable → proceed to Phase 2. Else refine lexicon and re-run (loop is free). This is the cheap detector proof the synopsis field exists for.

> Note: `description` is synopsis-only and a known floor — the authoritative clause text lives in the attachments. Phase 1 validates the *detector mechanics*, not final recall.

---

## 3. Phase 2 — Scoped residential byte download (Cycle 2)

**Egress constraint (MANDATORY):** residential / local, **never Modal** (SAM WAF 429s datacenter IPs). Public backend, no api_key. Single-threaded ~4 req/s, detached, resume-safe. Per `docs/reference/SAM_ATTACHMENT_DOWNLOAD_DIRECTIVE.md`.

**Worklist** — re-derived from the scoped manifest at run time (counts drift daily):
- Gates: `file_name IS NOT NULL AND size_bytes >= 1` (drops phantoms) · `access_level='public'` · `export_controlled=false` · `mime_type IN ('pdf','docx','doc','txt')`.
- **Recommended scope: unrestricted ∩ text = 9,462 files (~23.5 GB declared LB; ~50–90 GB true).** Every text doc on the 5,946 unrestricted biddable notices — complete recall for 52.219-9 (clause is not SOW-bound; high-value-named slice under-recalls). **← OPERATOR-CONFIRMED AT THIS GATE.**
- Reuse `pipelines/sam_gov/sam_attachment_download.py` (CAS blobs + enriched Lance ledger; iterate distinct `resource_id`, not citations).

**Declared sizes are LOWER BOUNDS** (corrupted mod-10 MB for ≥10 MB files); enforce the real 50 MB/file ceiling on post-redirect Content-Length at fetch (`oversize`). Bandwidth-bound; ETA = max(request budget, bandwidth budget) from the smoke MB/s.

**Output:** `s3://data-sink/active/sam_attachment_blobs/<resource_id>` (R2 CAS) + `s3://data-sink/active/sam_attachment_files/` (Lance ledger SoR). **Acceptance:** `downloaded / (worklist − gone) ≥ 0.99`; reconcile orphans = missing = corrupt = 0.

---

## 4. Phase 3 — Text extraction + designation detection (Modal-eligible, R2→R2; **NO OCR**)

Reads blobs from R2 — **no SAM egress, Modal-eligible**. Reuse/extend `pipelines/sam_gov/sam_attachment_extract.py` high-speed text pass. **OCR (that spec's Phase 3) is DROPPED entirely** — simpler, faster, single lane.

**Per file:**
1. Born-digital text extract by sniffed type: pdfium → pdf, python-docx → docx, striprtf → rtf, serialized soffice lane → legacy doc/xls.
2. **ZERO-TEXT TEST (pragmatic — no char counting):**
   - `has_extractable_text = (extracted_text_length > X)`, **X default 1** (configurable; raise to ~16–32 if stray-glyph false-positives appear on scanned PDFs).
   - **SHORT-CIRCUIT:** stop accumulating the moment length crosses X — do **not** read the full text layer to count. A text PDF resolves on page 1 and stops; a scanned PDF's empty text layer is read but pdfium returns empty per page *without OCR*, so cost stays bounded.
   - `has_extractable_text = false` ⇒ scanned/image-only ⇒ **the accepted, measured OCR blind spot** (skipped, counted, never silently "no designations").
3. Apply `designation_lexicon` over the extracted text (only when `has_extractable_text`): per-(notice, file) designation hit set + subcontracting-plan hit + a short evidence snippet per hit.

**Output:** append-only event ledger `s3://data-sink/active/sam_attachment_designation_hits/` — one row per (resource_id, designation) with `notice_id`, `flag`, `matched_pattern`, `evidence_snippet`, `has_extractable_text`, provenance. Idempotent (merge_insert on hit key). Single committing process; workers pure compute.

---

## 5. Phase 4 — Materialize per-notice designation profile (Lance SoR)

`s3://data-sink/active/govcon_open_biddable_designations/` — **1 row per `notice_id`**:
- **Designation flags (12, bool):** verbatim canonical names (OR over that notice's files' hits).
- `subcontracting_plan_required` (bool), `designation_count` (int), `any_socioeconomic_designation` (bool).
- **Coverage provenance:** `n_attachments`, `n_files_with_text`, `n_files_zero_text`, `all_attachments_zero_text` (bool — the per-notice blind-spot flag), evidence references.
- **Indexes:** BTREE `notice_id`; BITMAP the 12 flags + `subcontracting_plan_required` + `all_attachments_zero_text`.
- Joins to the opps dataset (`notice_id`) and to `govcon_subawardee_designations` / `govcon_active_awards` (shared verbatim flag vocabulary).

Lance v2.1, idempotent snapshot-overwrite, rebuild-safe.

---

## 6. Phase 5 — Acceptance, coverage report, land

**Coverage report (explicit, no silent caps):**
- Notices processed / files extracted / files with text vs **`n_files_zero_text` ("N of M target PDFs had no extractable text layer — OCR blind spot, not processed")**.
- Designation distribution across the open-biddable set.
- **Subcontracting-plan incidence from full text vs the `description`-only floor** (compare to the 4.4% synopsis proxy — quantify the lift the attachment pass buys).
- Notices flagged `all_attachments_zero_text` (where the answer is genuinely unknowable without OCR).

**Git lifecycle:** own it end-to-end — branch off latest `main`, PR, squash-merge, pull into the operator checkout, verify on disk.

---

## 7. Guardrails & non-goals (hard stops)

- **No OCR.** Zero-text PDFs = accepted, **measured** blind spot — never silently "no designations."
- **Boundary-aware regex, never naive `LIKE`.** Acronym collisions are the primary false-positive risk.
- **Verbatim canonical flag names** — zero vocabulary drift; text flags must join to the entity datasets.
- **Download residential only; extraction Modal-eligible.** Egress constraint binds Phases manifest+download, not detection.
- **Detection is re-runnable on stored bytes** — refine the lexicon without re-downloading.
- **Re-derive all counts from the live manifest at run time** — the figures here drift daily.
- **Blast-radius isolation:** each phase has its own ledger; a download failure cannot corrupt extraction, an extraction failure cannot corrupt the SoR.

## 8. Data model / URIs

| Artifact | URI / path | Grain |
|---|---|---|
| Scoped manifest (Cycle 1 ✓) | `s3://data-sink/active/sam_opps_attachment_manifest_open_biddable/` | notice-attachment citation |
| Designation lexicon | `pipelines/sam_gov/reference/designation_lexicon.json` | designation → patterns |
| File ledger (download SoR) | `s3://data-sink/active/sam_attachment_files/` | physical file (`resource_id`) |
| Blob CAS | `s3://data-sink/active/sam_attachment_blobs/<resource_id>` | bytes |
| Designation hits ledger | `s3://data-sink/active/sam_attachment_designation_hits/` | (resource_id, designation) |
| **Per-notice profile (deliverable)** | `s3://data-sink/active/govcon_open_biddable_designations/` | `notice_id` |

## 9. Operator decision gates

1. **Phase 0→1:** lexicon patterns reviewed.
2. **Phase 1→2:** detector validated on `description` (discrimination acceptable).
3. **Phase 2 scope:** confirm download scope (recommended: unrestricted ∩ text, 9,462 files) — **currently pending**.
