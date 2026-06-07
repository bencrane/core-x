# SAM.gov Attachment Pipeline — Terminology, Data Model, and State Reference

**Purpose:** eliminate loose terminology. This document defines every term precisely,
explains how SAM.gov models procurements, describes the 3-stage pipeline, and records
the measured state as of **2026-06-07** with **reproduction queries** so any claim can
be verified independently.

**Audience:** an AI agent or engineer who needs to reason about this system without
prior context. Read Part 1 (definitions) before Part 3 (state) — the numbers only make
sense once the grains and keys are clear.

> Convention in this doc: **DURABLE** facts (data model, schema, architecture) do not
> change. **SNAPSHOT (2026-06-07)** facts are point-in-time measurements; re-run the
> query in Part 6 to refresh them.

---

## Part 1 — Terminology (precise definitions)

### 1.1 The procurement lifecycle and the "notice"
A **notice** is the atomic record SAM.gov publishes. Key: **`notice_id`** — a 32-char
hex string, unique per notice. One real-world procurement is represented by **several
notices over time**, each a separate `notice_id`, all sharing one **`solicitation_number`**.

A typical lifecycle (each line = a distinct notice / distinct `notice_id`):

```
Sources Sought / Presolicitation     (optional early market-research notices)
        │   (same solicitation_number)
        ▼
Solicitation  OR  Combined Synopsis/Solicitation
        │        ← THE ATTACHMENTS (SOW/PWS/specs PDFs) LIVE ON THIS NOTICE,
        │          posted while bidding is open
        ▼   (bids submitted, evaluated)
Award Notice
                 ← carries award_number (PIID), awardee, award_date, award_amount;
                   it is an ANNOUNCEMENT and usually has FEW or NO attachments
```

**This is the single most important fact for avoiding confusion:** the **document
package lives on the Solicitation notice**, while the **award metadata lives on the
separate Award notice**. They are different rows with different `notice_id`s, **linked
only by `solicitation_number`**. To go from "a company won award X" to "the scope
document for X," you must join Award → `solicitation_number` → Solicitation notice →
its attachments.

### 1.2 "Solicitation" vs "Award" — the pairing, precisely
- A **solicitation** and its resulting **award** are two phases of one procurement,
  joined by **`solicitation_number` (Sol#)**.
- The pairing is **NOT 1:1 and NOT guaranteed**:
  - Many solicitations are **open** (bidding in progress) → **no award yet**.
  - Some are **cancelled** → **no award ever**.
  - Awarded ones span **all time** → most awards are not recent.
- Therefore: *holding a solicitation's PDF does not imply a recent award exists.* You
  determine "recent award" from the **Award notice's `award_date`**, then join back to
  the solicitation's PDF by Sol#.

### 1.3 "Active" vs "Awarded" — status vs event (commonly conflated)
- **Active** = the `Active` field = `Yes` (a lifecycle **status**). A notice is Active
  until its `ArchiveDate` passes; then SAM flips it to **Archived**. It means "still
  posted on sam.gov, not yet taken down." Applies to **all notice types** (most Active
  notices are open solicitations with **no award at all**). The Active window length
  varies per notice (driven by its own archive date) — it is **not a time window**.
- **Awarded in the last N days** = a filter on **`award_date`** (an **event date**),
  independent of Active/Archived status.
- They overlap only partially:
  | situation | Active? | awarded ≤N days? |
  |---|---|---|
  | open solicitation, no award yet | yes | no |
  | awarded 30 days ago, archived fast | **no** | yes |
  | awarded 18 months ago, long archive window | yes | no |
  | awarded 40 days ago, still posted | yes | yes |
- **Consequence:** a correct "recent awards" pull must filter `award_date` across **both
  the Active and the Archived universes**, because many fresh awards archive quickly and
  drop out of Active.

### 1.4 Two grains, two keys (do not conflate)
- **Attachment citation** — grain = one (notice, attachment) pair. Key =
  **`attachment_id`**. The same physical file cited by many notices appears as many
  citations.
- **Physical file** — grain = one actual file. Key = **`resource_id`**. The bytes are
  identity-addressed in R2 at `<blob_prefix>/<resource_id>`, so a file is downloaded
  **exactly once** no matter how many notices cite it.
- Empirically (DURABLE, from the manifest module): ~331,401 citations → ~118,739
  distinct files across the original active universe — repetition lives in citations,
  not files.

### 1.5 The two "URL/link" things (the ambiguity behind "solicitation urls and links")
- **Notice page link** — column `link` (universe) / `ui_link` (manifest) =
  `https://sam.gov/opp/{notice_id}/view`. The human-readable opportunity page.
  **Present for every notice in the universe.**
- **Attachment download URL** — column `download_url` (manifest only) =
  `https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{resource_id}/download`.
  The direct file-bytes endpoint. **Only exists for notices that have been *harvested*
  (Stage 2).**
- When someone says "the solicitation urls/links substrate," they almost always mean the
  **attachment `download_url`s** (the pointer layer needed to fetch PDFs), not the notice
  page link.

### 1.6 "Substrate" (Phase 1) vs "PDF content" (Phase 2)
- **Substrate / links / manifest (Phase 1)** = the **pointer layer**: one row per
  attachment citation with `download_url`, `resource_id`, `file_name`, `mime_type`,
  `size_bytes`, plus the notice/solicitation/NAICS keys. Built by *harvesting* the
  `/resources` endpoint per notice. **No bytes.**
- **PDF content / bytes (Phase 2)** = the **actual file bytes** fetched from each
  `download_url`, stored in R2, recorded in the ledger. Built by the *downloader*.
- These are sequential, independent stages. You can hold a complete manifest (Phase 1)
  and have downloaded **zero** bytes (Phase 2).

### 1.7 `size_bytes` is CORRUPTED (a known SAM defect — DURABLE)
The manifest's `size_bytes` (from SAM's attachment-list API) is **exact only for files
< 10 MB**. For files ≥ 10 MB, SAM returns `((true_size − 1) mod 10,000,000) + 1` — the
true size with an unknown multiple of 10 MB subtracted. So `size_bytes` is a **LOWER
BOUND**, not the true size, and is **not invertible**. Examples (declared → real):
`5,066,771 → 45,066,771`; `2,253,038 → 32,253,038`; `10,000,000 → ~210 MB`. **True size
is known only after download** (`size_downloaded` in the ledger). Never treat
`size_bytes` as a true size, cap, or storage budget above 10 MB.

### 1.8 `trigger_relevant` (DURABLE)
A boolean flag set during harvest: `naics_code` starts with `"23"` (Construction) **OR**
`classification_code` (PSC) ∈ {`N063`, `C1AZ`}. It only sets **harvest ordering** and a
filter flag — it does **not** restrict coverage when the harvester runs with
`do_remaining=True` (the default), which sweeps every notice in its universe. Source:
`sam_attachment_manifest.py` (`naics.startswith("23")`, `TRIGGER_PSC = ("N063","C1AZ")`).

### 1.9 Download "tiers" (DURABLE — gates applied at download time)
The downloader (`sam_attachment_download.py`) builds its worklist by applying a **tier
predicate** over a manifest, then deduping to one row per `resource_id`. Tiers (exact,
from `_TIER_PRED`):
- Universal floor `_UNIV` (every tier): `size_bytes>=1 AND file_name IS NOT NULL AND
  access_level='public' AND export_controlled=false`.
- `_SIZECAP`: `size_bytes>=10000 AND size_bytes<50000000` (declared band — a prefilter,
  not a real-size bound).
- `_TEXT`: `mime_type IN ('pdf','docx','doc','txt')`.
- `_HV`: filename contains sow/pws/"statement of work"/spec/etc.
- `_TRIG`: `trigger_relevant = true`.

| tier | predicate | meaning |
|---|---|---|
| T0 | `_UNIV ∧ _TRIG ∧ _HV ∧ _SIZECAP` | trigger, high-value docs |
| T1 | `_UNIV ∧ _TRIG ∧ _TEXT ∧ _SIZECAP` | trigger, all text |
| T2 | `_UNIV ∧ _TRIG ∧ attachment_order=1 ∧ _TEXT` | trigger, first attachment |
| T3 | `_UNIV ∧ _HV ∧ _SIZECAP` | **all-sector** high-value docs |
| T4 | `_UNIV ∧ _TEXT ∧ _SIZECAP` | **all-sector** all text |

**None of the tiers filter by `naics_code` or by `award_date`.** Tier gating is about
file type/quality, not sector or recency. NAICS/entity scoping happens *upstream* (which
manifest you point the downloader at), not in the tier.

The downloader enforces the **real 50 MB ceiling at fetch** (post-redirect
Content-Length + streaming byte cap → `status=oversize`, no store), because `size_bytes`
cannot be trusted (§1.7).

---

## Part 2 — Pipeline architecture (3 stages, DURABLE)

All storage is **LanceDB datasets on Cloudflare R2** (`s3://data-sink/...`). Compute is
DuckDB. Runs are local/in-session from a residential IP (SAM 429s datacenter egress).

```
STAGE 1 — UNIVERSE (the notice rows)
  sam_opps_bulk.py        → s3://data-sink/sam-gov-opps/active/    (currently-active notices, daily snapshot)
  sam_opps_archived_bulk.py → s3://data-sink/sam-gov-opps/archived/ (inactive notices, per fiscal year)
  Columns include: notice_id, solicitation_number, naics_code, award_number, award_date,
                   awardee, posted_date, archive_date, link, ...
        │
STAGE 2 — MANIFEST / SUBSTRATE (the attachment pointer layer)   [Phase 1]
  sam_attachment_manifest.py / sam_play1_harvest.py
    reads a universe → GET /opportunities/{notice_id}/resources per notice
    → s3://data-sink/active/sam_opps_attachment_manifest*/...
  grain = one attachment citation; carries download_url, resource_id, file_name,
          mime_type, size_bytes, + notice/solicitation/naics keys. NO bytes.
        │
STAGE 3 — BYTE DOWNLOAD (the PDF content)                        [Phase 2]
  sam_attachment_download.py
    apply tier gate over a manifest → dedup to resource_id → fetch bytes
    → blobs:  s3://data-sink/active/sam_attachment_blobs/<resource_id>  (R2 content-addressed)
    → ledger: s3://data-sink/active/sam_attachment_files/  (one row per physical file:
              status, http_status, sha256, size_expected, size_downloaded, mime_*,
              stored_uri, run_id, worklist_tier, completed_at)
```

**Ledger statuses:** `downloaded` (200, stored) · `oversize` (real ≥50 MB, not stored)
· `restricted` (401/403) · `gone` (400/410) · `failed` (other). Resume skips
`downloaded`/`restricted`/`gone`; retries `failed`.

### 2.1 The entity-anchored "Play-1" selection chain (DURABLE)
"Play 1" = profile companies (PE rollup / award-performance use cases) by their federal
record. The selector (`sam_play1_target_select.py`) uses **USASpending FPDS as the clean
entity spine** because SAM `awardee` is a name string, never a UEI:

```
QUALIFY  award_search rows in a NAICS set with positive obligation and
         action_date ≥ now−Q years      → set of recipient_uei  ("winning govt $")
FOOTPRINT those UEIs' contract activity over now−F years (transaction_search_fpds)
                                         → distinct PIID + solicitation_identifier
JOIN     SAM universe (active+archived) on award_number=PIID OR
         solicitation_number=solicitation_identifier  → target notice_ids
HARVEST  Stage 2 over those notices     → vertical manifest
DOWNLOAD Stage 3 (tier gate)            → bytes for that vertical
```
Key join: **PIID (`award_number`) is the clean key**; Sol# is the secondary join.
FPDS sources: `s3://data-sink/active/usaspending/award_search/` (~78.4M award rows) and
`.../transaction_search_fpds/` (~107.3M transaction rows); both carry clean
`recipient_uei`, `naics_code`, `action_date`, `piid`, and (FPDS txns) `solicitation_identifier`.

---

## Part 3 — Measured state (SNAPSHOT 2026-06-07)

### 3.1 Universe (Stage 1)
| dataset | rows | notes |
|---|--:|---|
| `sam-gov-opps/active/` | 77,683 | daily snapshot of currently-active notices (100% `Active=Yes`); count drifts as SAM refreshes |
| `sam-gov-opps/archived/` | **2,839,948** | FY2019–2026, all `Active=No`, 6 scalar indices |

The **active full-CSV extract is active-only** — its `WHERE Active='YES'` filter drops
nothing because the file already contains only active rows. Historical/inactive notices
come from a **separate per-fiscal-year archived dataset** that Stage 1's
`sam_opps_archived_bulk.py` ingests. (This corrected an earlier wrong assumption that the
"full" extract contained history.)

### 3.2 Manifests / substrate (Stage 2) — what attachment links we hold
| manifest | unique files (`resource_id`) | citations | notices w/ attachments | how scoped |
|---|--:|--:|--:|---|
| `sam_opps_attachment_manifest_play1/shard_000..005/` (A&D) | 340,569 | 915,069 | 212,722 | entity-anchored A&D/gov-svcs, 2y/6y |
| `sam_opps_attachment_manifest_remediation/shard_000/` | 36,438 | 106,115 | 17,806 | entity-anchored remediation, 2y/6y |
| `sam_opps_attachment_manifest_equipment_rental/shard_000/` | 3,733 | 6,994 | 2,409 | entity-anchored equip-rental, 2y/6y |
| `sam_opps_attachment_manifest/` (original) | — | ~331,401 | ~79K (active trigger sweep) | active universe, trigger-ordered |

"Entity-anchored 2y/6y" = qualified entities won govt $ in that NAICS set within 2 years,
expanded to their 6-year footprint (Part 2.1). Qualified UEIs measured: A&D **29,025**;
remediation **4,763**; equipment-rental **1,076**.

### 3.3 PDF content / bytes (Stage 3) — what we actually downloaded
**TOTAL stored: 25,755 files / 57.7 GB real bytes** (status=`downloaded`), + 53
`oversize` (real ≥50 MB, skipped) — **and actively climbing**: the remediation Stage-3
download was still running at snapshot, so the total had already reached **28,755 files /
61.1 GB** minutes later and continues toward the ~29,234-file remediation worklist.
Re-measure (Part 6) for the current figure. By NAICS sector of the file's notice (at the
25,755 read):

| sector | files downloaded | source run |
|---|--:|---|
| 23 Construction | 18,280 | original trigger tiers (T0/T2/T3/T1; NAICS 23 was the trigger) |
| 53 Rental/Leasing | 3,042 | equipment-rental vertical (T4) |
| 33 Manufacturing | 1,319 | all-sector tier spillover (T3/T4) |
| 56 Admin/Waste/Remediation | 789+ | remediation vertical (T4) — **in progress** at snapshot |
| 54 / 81 / 48 / 32 / other | ~1,400 | all-sector spillover |

~18,355 of the downloaded files are `trigger_relevant` (the original active-universe
runs). **The downloaded corpus is scoped by NAICS/tier/entity-footprint — never by
award date.** Most are **solicitations** (open/active opportunity documents), not awards.

**Real-size calibration (from the ledger's true `size_downloaded`):** mean **2.45 MB**,
**median 0.31 MB**, p90 6.1 MB, max ~50 MB (the cap). The distribution is heavily
right-skewed — most files are small; bytes concentrate in a minority of large files.
Use `files × ~2.45 MB` to estimate real GB; treat manifest declared sums as lower bounds.

### 3.4 The "awarded in the last 120 days" question (SNAPSHOT)
Filtering the universe (active+archived) to `award_date ≥ 2026-02-07`:
- **25,696** distinct award notices in the last 120 days (all sectors). Dominant sector:
  33 Manufacturing (17,052); 23 Construction = **1,219**.
- **Phase 1 (substrate):** only **3,765 / 25,696 = 15%** of these award notices have
  their attachment links in any harvested manifest. The other **21,931 (85%) are
  unharvested** — because our harvests were *entity-vertical*-scoped, not
  *recent-award*-scoped.
- **Phase 2 (content):** only **152** of these recent-award notices have a downloaded
  PDF (mostly the 138 construction ones, a side-effect of the trigger runs).
- **Caveat:** these counts match on the **award notice's OWN attachments**, which
  undercounts the true reachable set, because the documents live on the **solicitation
  sibling notice** (Part 1.1). The correct measure joins Award→Sol#→Solicitation
  notice→attachments; that join had not been run as of this snapshot.

---

## Part 4 — Common confusions, stated plainly

1. **"We have 18,280 construction PDFs, so we have recent construction awards covered."**
   No. Those are mostly **solicitation** documents harvested because they were *active
   construction opportunities*. Only ~138 are tied to a notice *awarded in the last 120
   days* (and even that undercounts; see the Sol# join caveat). Document supply ≠ recent
   awards.
2. **"Active means recent."** No. Active = "still posted," a status set by each notice's
   archive date. It includes open solicitations (no award) and can include awards months
   old. Recent = an `award_date` filter, a different thing.
3. **"Solicitation and award are the same notice."** No. Same procurement, **two
   different notices** sharing a Sol#. Attachments on the solicitation; award metadata on
   the award notice.
4. **"`size_bytes` tells us the download size."** Only under 10 MB. Above that it's a
   lower bound (Part 1.7). Real size is known only post-download.
5. **"The tiers filter by sector/recency."** No. Tiers filter by file type/quality only.
   Sector/entity/recency scoping is chosen by *which manifest* you feed the downloader
   and *which notices* you harvest.

---

## Part 5 — Canonical R2 locations (DURABLE)
| what | URI |
|---|---|
| Active universe | `s3://data-sink/sam-gov-opps/active/` |
| Archived universe | `s3://data-sink/sam-gov-opps/archived/` |
| Original active manifest | `s3://data-sink/active/sam_opps_attachment_manifest/` |
| A&D manifest (6 shards) | `s3://data-sink/active/sam_opps_attachment_manifest_play1/shard_000..005/` |
| Remediation manifest | `s3://data-sink/active/sam_opps_attachment_manifest_remediation/shard_000/` |
| Equipment manifest | `s3://data-sink/active/sam_opps_attachment_manifest_equipment_rental/shard_000/` |
| Downloaded-file ledger | `s3://data-sink/active/sam_attachment_files/` |
| Downloaded blobs (CAS) | `s3://data-sink/active/sam_attachment_blobs/<resource_id>` |
| FPDS award_search | `s3://data-sink/active/usaspending/award_search/` |
| FPDS transactions | `s3://data-sink/active/usaspending/transaction_search_fpds/` |
| Per-vertical target worklists | `s3://data-sink/active/_play1_target_universe{,_remediation,_equipment_rental}/` |

Code (repo `core-x`, `pipelines/sam_gov/`): `sam_opps_bulk.py`,
`sam_opps_archived_bulk.py`, `sam_attachment_manifest.py`, `sam_play1_target_select.py`,
`sam_play1_harvest.py`, `sam_attachment_download.py`.

---

## Part 6 — Verification (reproduce every SNAPSHOT number)

Run with R2 creds in env (Doppler `core-x/prd` provides `R2_*`). Pattern: open a Lance
dataset, query with DuckDB. Storage options:
```python
import os, lance, duckdb
so = {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
      "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
      "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
      "region": "auto"}
def t(uri, cols=None): return lance.dataset(uri, storage_options=so).to_table(columns=cols)
```

- **Archived universe count:** `lance.dataset(".../sam-gov-opps/archived/", storage_options=so).count_rows()` → 2,839,948.
- **Total downloaded files + bytes + status:**
  ```python
  con = duckdb.connect(); con.register("led", t(".../active/sam_attachment_files/"))
  con.execute("SELECT status, count(*), sum(size_downloaded)/1e9 FROM led GROUP BY status").fetchall()
  ```
- **Downloaded by sector** (join ledger `resource_id` → any manifest `naics_code`): union the manifest datasets, `GROUP BY substr(naics_code,1,2)`.
- **Real-size calibration:** `SELECT avg(size_downloaded), median(size_downloaded), quantile_cont(size_downloaded,0.9), max(size_downloaded) FROM led WHERE status='downloaded'`.
- **Awards last 120d + substrate/PDF coverage:** filter universe `TRY_CAST(award_date AS DATE) >= current_date - INTERVAL 120 DAY`; left-join distinct `notice_id` against the union of manifest `notice_id`s (Phase 1) and against ledger `resource_id`s where status='downloaded' (Phase 2).
- **size_bytes corruption:** compare a manifest row's `size_bytes` to the ledger's
  `size_downloaded` for the same `resource_id` where the file is ≥10 MB — the ledger
  value will exceed the manifest value by a multiple of 10,000,000.

---

## Part 7 — One-paragraph summary
SAM publishes **notices**; one procurement is several notices sharing a **`solicitation_number`**.
The **document PDFs live on the Solicitation notice**; the **award metadata
(`award_date`, awardee, PIID) lives on the separate Award notice**. "Active" is a posting
**status**, not recency; "awarded in last N days" is an **`award_date` filter** spanning
both active and archived universes. The pipeline has three stages: **universe** (notice
rows), **manifest/substrate** (attachment `download_url`s — *Phase 1, links, no bytes*),
and **download** (the actual PDF bytes → ledger + blobs — *Phase 2*). As of 2026-06-07 we
hold a large manifest substrate for entity-anchored A&D/remediation/equipment verticals
and have downloaded **25,755 files / 57.7 GB**, but that corpus is **NAICS/tier/entity
scoped, never award-date scoped** — so only ~152 of it intersects "awarded in the last
120 days." Targeting recent awards is a distinct, small, fast build: filter `award_date`,
join Award→Sol#→Solicitation, harvest the gap, download the PDFs.
