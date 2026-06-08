# SAM.gov Attachment Substrate — Forensic Audit + 90-Day Stage-3 Harvest (Work Record)

**Date:** 2026-06-08. **Scope of this record:** two sequential directives executed in one session —
(1) a read-only forensic audit of the pre-existing attachment cache, and (2) an isolated Stage-3
byte-download harvest of the 90-day prime-winners manifest. Every figure below was verified against
live state (R2, the Postgres ops ledger, git) at the time of writing; provenance per claim is in §7.

This document states measured facts only. No projections except where explicitly labeled.

---

## 1. Forensic audit of the pre-existing cache (Directive 1)

A read-only audit was run to determine whether SAM.gov payloads already existed on disk and to trace
their provenance. Findings:

| Question | Finding (verified) |
|---|---|
| Loose files at `landing/sam_attachments/`? | **0 objects / 0 B** — empty. |
| Payloads elsewhere? | **`s3://data-sink/active/sam_attachment_blobs/`** — **54,952 objects / 82.474 GiB (88,555,646,717 B)**, content-addressed by `resource_id`. |
| Catalog ledger | `s3://data-sink/active/sam_attachment_files/` — 247 objects (Lance). |
| Vector / extracted-text tables (`govcon_scope_vectors`)? | **None exist.** |
| Run provenance | Postgres `ops.sam_attachment_download_runs`: 7 batches, 55,012 download events, first 2026-06-07 01:02:40Z, last 2026-06-08 02:09:15Z. (The 60-event gap vs. 54,952 stored objects = a 40-file smoke run to throwaway URIs + a 20-file warm-up; neither produced production blobs.) |

**Sourcing logic (from `ops.sam_attachment_download_runs.worklist_filter`, authoritative):** a tiered gate
over `sam_opps_attachment_manifest`, not random sampling and not a live keyword API search. Tiers:
T0+T2 (4,231 files), T1 (14,099), T3 (4,385), T4 (32,237). The "NAICS filter" is a precomputed
`trigger_relevant` boolean = NAICS 23 (construction) ∪ PSC N063/C1AZ, applied to T0+T2/T1/T3; T4 is
"all sectors, all text, 10 KB–50 MB" with no trigger gate. Executor: `pipelines/sam_gov/sam_attachment_download.py`.

**Lineage-loss finding (the reason the 90-day run was built differently):** the historical ledger schema
stores `resource_id` but **not** `file_name`. Of 54,952 stored files, 22,834 trace to the current
`sam_opps_attachment_manifest` and 5,195 to `sam_opps_attachment_manifest_90day_winners`; **27,878
(50.7%, the bulk of T4) resolve to neither** because their source manifest snapshot was overwritten
(`sam_opps_attachment_manifest` is written `mode="overwrite"`, re-snapshotted). Their bytes are intact;
their filenames are not recoverable from current datasets. No `sam_attachment_worklist_T4` snapshot was
persisted, so there is no surviving lineage record for that tier.

**Substrate classification (computed during the audit; filename keyword classifier; not re-run for this
record):** of the 27,074 files whose filenames resolved — Scope (SOW/PWS/SOO/spec/drawings) 6,405;
Pricing/Compliance (SCA/DBA/wage-det/FAR-cert) 1,670; Noise (SF1449/SF30/PPQ/amendment) 3,810;
Other/Unclassified 15,189. The remaining 27,878 are filename-unresolvable (T4, source overwritten) but
content-confirmed as documents by magic-byte sniff. Method caveat: keyword classification under-counts
scope content embedded in generically named files.

---

## 2. 90-Day Stage-3 harvest (Directive 2)

**Objective:** download the public attachment bytes for `sam_opps_attachment_manifest_90day_winners`,
isolated from the historical cache, for the staffing demand-side GTM motion.

### 2.1 Target (validated read-only before fetch)

`s3://data-sink/active/sam_opps_attachment_manifest_90day_winners/` — 155,183 rows; `access_level`
breakdown public 152,146 / private 3,037. Public + downloadable (named, `size_bytes ≥ 1`, distinct
`resource_id`): **127,576 files / 211,838,772,724 B declared (≈ 211.8 GB)**. `size_bytes` is uncorrupted
for this feed (ground-truth probe #324, n=998, declared == true Content-Length 100% on the ≥10 MB stratum),
so declared bytes are true bytes. All 127,576 public downloadable rows carry a non-null `file_name`.

### 2.2 Engine (`pipelines/sam_gov/sam_attachment_download_90day.py`, new)

- **Concurrency 6 + global token bucket capped at 8 req/s.** Concurrency is decoupled from the WAF-facing
  request rate: regardless of worker count or file-size mix, aggregate request initiation never exceeds
  the proven-safe residential envelope (probe #324: conc=6 @ 0.1 s → 0 WAF blocks / 1,000 req; conc ≥ 24
  trips the WAF in ~100 req). A circuit breaker aborts on clustered 429/403 (≥15 in 60 s or ≥25 consecutive).
- **Out-of-core fetch:** each file streams to a `SpooledTemporaryFile` (16 MB RAM, then NVMe spill) with
  incremental sha256 — no in-memory bytearray. Upload via `boto3 upload_fileobj` (`use_threads=False`).
  No 50 MB cap (the 213 GB target includes 718 files ≥50 MB); a 1 GB per-file ceiling + 600 s wall-clock
  bound pathological transfers.
- **Lineage-complete ledger:** persists `file_name`, declared mime, real `content_length`, `access_level`,
  `notice_id`, `solicitation_number`, `naics_code`, sha256, size_match, mime_match per file. Immune to the
  historical T4 lineage loss.
- **`os.setsid` double-fork daemon** (heavy imports post-fork; macOS fork-safe), resumable via Lance
  ledger ∪ JSONL checkpoint. Worklist persisted to `sam_attachment_worklist_90day/` (durable lineage source).

### 2.3 Calibration (`run_id=smoke-cal`, ops row present)

200 full downloads at conc=6 / 8 req/s to throwaway URIs (`_smoke_90day_*`, purged after): 200/200
downloaded, **0 WAF blocks**, 4.237 MB/s, 0 size mismatches, 1 mime mismatch. Confirmed the proven rate
holds for full multi-MB transfers (the probe used 1-byte Range GETs). Throwaway artifacts were deleted;
their 200 files / 262,775,472 B are **not** in the production sink.

### 2.4 Production run (`run_id=90day-full`)

Terminal record from `ops.sam_attachment_download_90day_runs` (status = success):

| Metric | Value |
|---|---|
| Attempted | 127,576 |
| **Downloaded** | **126,901** |
| Failed | 2 |
| Gone (HTTP 400/410, link-type, no body) | 673 |
| Oversize (>1 GB) | 0 |
| WAF 429 / 403 | **0 / 18** |
| **Bytes downloaded** | **213,723,358,445 B = 199.045 GiB = 213.72 GB** |
| Sustained throughput | 4.625 MB/s |
| size_mismatches (size_downloaded ≠ declared) | 76 (0.060%) |
| mime_mismatches (magic-byte ≠ declared ext) | 171 (0.135%) |
| Started → completed (UTC) | 2026-06-08 03:33:54 → 16:24:12 (**12.84 h**) |

**Acceptance:** downloaded / (attempted − gone − restricted − oversize) = 126,901 / 126,903 = **0.999984**
(≥ 0.99 bar). The 2 `failed` (retry-exhausted) are the only gap; they are re-attemptable via a `--resume`
pass (a `failed` row is not in the resume skip-set).

### 2.5 Reconciliation (post-completion, `--backfill`)

`sam_attachment_reconcile_90day.py --backfill`, run after confirming the daemon exited:
`blobs = 126,901 · ledger_downloaded = 126,901 · orphans = 0 · missing = 0 · size_mismatches = 0 ·
backfilled = 0 · consistent = True`. Every stored object has a catalog row and vice versa. No backfill was
needed (clean normal exit).

---

## 3. Durability model (built and exercised this session)

Three layers, verified live during the run:
1. **Blob bytes → R2 CAS, per file, immediately** on download completion (not on a timer). A session/host
   death cannot delete R2 objects.
2. **JSONL checkpoint** (`/tmp/sam_90day_ckpt.jsonl`) — per-file, line-buffered, local.
3. **Lance ledger** → R2 every 500 files.

A crash between ledger flushes leaves *orphan blobs* (bytes present, no catalog row) — never lost bytes.
`sam_attachment_reconcile_90day.py --backfill` reconstructs orphan ledger rows from
`sam_attachment_worklist_90day/` ⨝ the CAS listing, using R2 sources only. The worklist snapshot is in R2
and immune to manifest re-snapshot, so the historical T4 lineage loss cannot recur for this cache.
Mid-run, a read-only reconcile measured a 194-orphan window (bytes safe, recoverable) — that window closed
to 0 at clean completion.

A 30-minute monitor (session-local cron) tracked the run across its 12.84 h and was cancelled at completion.

---

## 4. Code shipped (merged to `main`, present on operator checkout `/Users/benjamincrane/core-x`)

| PR | Commit | Files |
|---|---|---|
| [#326](https://github.com/bencrane/core-x/pull/326) | `96eeea4` | `pipelines/sam_gov/sam_attachment_download_90day.py`, `pipelines/sam_gov/ops_sam_attachment_download_90day_runs.sql` |
| [#327](https://github.com/bencrane/core-x/pull/327) | `d17df65` | `pipelines/sam_gov/sam_attachment_reconcile_90day.py` |

Both PRs MERGED and pulled to the operator checkout (verified on disk).

---

## 5. Current state of the lake (verified 2026-06-08)

| Dataset (R2 `s3://data-sink/active/…`) | Objects | Size | Role |
|---|---|---|---|
| `sam_attachment_blobs_90day/` | 126,901 | 213,723,358,445 B (199.045 GiB) | **90-day CAS payload (new)** |
| `sam_attachment_files_90day/` | 782 | 50.277 MiB | 90-day ledger (Lance, lineage-complete) |
| `sam_attachment_worklist_90day/` | 3 | 12.991 MiB | 90-day worklist snapshot |
| `sam_attachment_blobs/` | 54,952 | 88,555,646,717 B (82.474 GiB) | historical CAS payload (pre-existing) |
| `sam_attachment_files/` | 247 | 15.719 MiB | historical ledger (no `file_name`) |
| `ops.sam_attachment_download_90day_runs` (Postgres) | 2 rows | — | smoke-cal + 90day-full |

- `landing/sam_attachments/`: empty (0 objects).
- No `govcon_scope_vectors*` or any vector/extracted-text dataset exists.
- Two attachment caches now exist and are **not merged** (per directive): historical (54,952 / 82.474 GiB)
  and 90-day (126,901 / 213.72 GB). Combined payload on disk: 181,853 objects ≈ 296 GB.

---

## 6. Where this leaves us / next step

1. **Text extraction is not built or run.** Both caches are raw bytes. The 90-day cache is staged for it:
   the extraction worklist is a single Lance pushdown (BITMAP on `mime_declared`) over
   `sam_attachment_files_90day/`:
   ```sql
   SELECT resource_id, file_name, mime_declared, stored_uri, naics_code, solicitation_number
   FROM   active/sam_attachment_files_90day/
   WHERE  status='downloaded' AND mime_declared IN ('pdf','docx','doc','txt')
   ```
   → fetch by `stored_uri` → extract → write `govcon_scope_vectors_90day` (does not yet exist). The
   ~25 GB of non-text binaries (zip/xlsx/mp4/images) are tagged and filterable out of the text path.
2. **2 failed files** in the 90-day run (0.0016%). Re-attemptable via
   `sam_attachment_download_90day.py --resume --run-id 90day-full` if 100% is wanted; acceptance bar is
   already met without them.
3. **76 size + 171 mime mismatches** are flagged in the ledger (`size_match=false` / `mime_match=false`),
   not failures — candidates for inspection before/within extraction.
4. **Historical-cache T4 lineage gap is unresolved by design** (out of this directive's scope). It is
   reconstructable only by re-deriving filenames from a fresh manifest join on the surviving `resource_id`s;
   not attempted here.

---

## 7. Verification basis (how each claim was confirmed, 2026-06-08)

- **Git / PRs / files on disk:** `git -C /Users/benjamincrane/core-x log`, `gh pr view 326/327`, `ls` — direct.
- **90-day run metrics:** `ops.sam_attachment_download_90day_runs` (Postgres, terminal row) — authoritative;
  cross-checked against the daemon log `SUMMARY` line and the live R2 blob count/bytes (exact match:
  126,901 objects / 213,723,358,445 B).
- **Reconciliation:** `sam_attachment_reconcile_90day.py --backfill` output (consistent=True).
- **Historical cache:** `rclone size` of `sam_attachment_blobs/` (54,952 / 82.474 GiB) and `ops.sam_attachment_download_runs`
  aggregate; forensic provenance/classification computed during the audit (filename keyword method; classification
  not re-run for this record — flagged inline in §1).
- **Lake state / absences:** `rclone size`/`lsf` of the named prefixes; `landing/sam_attachments/` = 0;
  no `govcon`/vector datasets in the `active/` census.
