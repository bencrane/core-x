# Stage-2 SB>$500K attachment-link harvest — RUN RECORD

**Executed:** 2026-06-21 (UTC). **Mode:** LIVE crawl + R2 write (not a probe). **Plan:** [`docs/plans/STAGE2_LINK_HARVEST_WAVE_PLAN.md`](STAGE2_LINK_HARVEST_WAVE_PLAN.md).
**Outcome:** SUCCESS — 2,093/2,093 target notices crawled, 13,005 attachment-link rows landed, +293 SB>$500K awards moved crawlable→covered, 3,213 net-new files handed to Stage-3.

---

## What ran (P0 → P4)

| Phase | Command | Result |
|---|---|---|
| **P0 resolve** | `scripts/stage2_target_resolve_sb500k.py` → `s3://data-sink/active/_stage2_target_sb500k/` | 2,093 target notices frozen (9 active / 2,084 archived); reproduced the plan exactly: 5,052 crawlable Sol# → **597 in-universe** → 2,093 notices; 0 already in any manifest |
| **Smoke** | `sam_attachment_manifest.py --do-remaining --max-notices 40` → `_smoke_stage2_sb500k/` | 40 notices, 446 rows, **0 WAF blocks** — connectivity + residential IP + schema validated |
| **P1 crawl** | `sam_attachment_manifest.py --do-remaining --resume --inter-call-sleep 0.12 --checkpoint-every 200`, `SAM_OPPS_LANCE_URI=_stage2_target_sb500k`, `SAM_ATTACH_MANIFEST_URI=sam_opps_attachment_manifest_sb500k` | see below |
| **P2 land** | (harvester writes + indexes) → `s3://data-sink/active/sam_opps_attachment_manifest_sb500k/` | 13,005 rows; BTREE(notice_id, resource_id, naics_code) + BITMAP(trigger_relevant, mime_type, access_level) |
| **P3 verify** | `scripts/stage2_sb500k_verify.py` | coverage uplift below |
| **P4 handoff** | same probe | 3,213 download-pending → Stage-3 |

**No harvester code edits were required** — narrowing `SAM_OPPS_LANCE_URI` to the frozen 2,093-notice universe made `--do-remaining` crawl exactly those notices.

## P1 crawl — actuals (`ops.sam_attachment_manifest_runs`)

| Metric | Value |
|---|---:|
| Notices targeted / covered | 2,093 / **2,093** (uncovered 0) |
| API calls (`/resources` GETs) | 2,093 (1 per notice — no retry storms) |
| Wall-clock | **7 min 0 s** (15:22:41 → 15:29:41 UTC; ~0.33 s/notice ≈ 3 req/s single-stream) |
| WAF blocks (403/429) | **0** |
| Attachment rows written | **13,005** |
| Notices with ≥1 attachment | 1,194 |
| Zero-attachment notices (gettable-but-empty) | **899** (43%) — recorded, not retried |
| Declared bytes (LOWER BOUND, ≥10 MB corrupted) | ~48.1 GB — **not** a Stage-3 budget (use ledger `size_downloaded`) |

## P3 — coverage uplift (`scripts/stage2_sb500k_verify.py`)

| Metric | Value |
|---|---:|
| New manifest rows / **distinct files** / notices / solicitations | 13,005 / **3,213** / 1,190 / 275 |
| New solicitations net-new vs all prior manifests | 274 / 274 (100% net-new) |
| SB>$500K `covered` **before → after** | 3,062 → **3,355** |
| **SB>$500K awards newly covered** | **+293** |

**Why +293, not the ~631 resolvable ceiling:** coverage counts a Sol# only if it yielded ≥1 attachment row. 899 of the 2,093 target notices were attachment-empty, and some Sol#s' siblings were all empty, so 274 of 597 resolvable Sol# produced attachments → 293 awards. This is the honest, expected result (many SAM solicitation notices host no public attachments, or the package sits on a sibling outside our universe). Acceptance was explicitly bounded to "the resolvable set that yields attachments," not the headline award count.

## P4 — Stage-3 handoff

| Metric | Value |
|---|---:|
| New distinct files (`resource_id`) | 3,213 |
| Already downloaded | 0 |
| **Net-new Stage-3 download-pending** | **3,213** |

These 3,213 files are now in the Stage-3 download backlog ([`STAGE3_EXTRACTION_BACKLOG_WAVE_PLAN.md`](STAGE3_EXTRACTION_BACKLOG_WAVE_PLAN.md) P1). The three coverage/backlog probes now union `sam_opps_attachment_manifest_sb500k/`, so future coverage measurement includes this harvest automatically.

## Datasets written
- `s3://data-sink/active/sam_opps_attachment_manifest_sb500k/` — **canonical** new vertical manifest (13,005 rows, indexed). Blast radius contained: existing manifests untouched.
- `s3://data-sink/active/_stage2_target_sb500k/` — scratch worklist (underscore = non-SoR).
- `s3://data-sink/active/_smoke_stage2_sb500k/` — smoke scratch (disposable).

## Scope ceiling (restated, unchanged)
The other **4,455 of 5,052** SB>$500K crawlable Sol# carry an FPDS `solicitation_identifier` that matches **no** SAM notice — un-crawlable by Sol# at any effort (Stage-1 discovery limit: FPDS-only ids / pre-FY2019 / non-SAM vehicles). This harvest closes the resolvable slice; the un-resolvable floor is out of reach for this pipeline and would require a non-SAM document source.

## Next action
Run the Stage-3 plan over the 3,213 new download-pending files (priority-tiered) to convert these links into `insurance_bonding` / `labor_category` serving signals — that is where the surety/bonding GTM payload is realized.
