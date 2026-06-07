# SAM.gov Solicitation Attachment — Content Download & Audit Ledger

**Execution plan. Self-contained. Another agent should be able to run this end-to-end.**

> **v2** — incorporates the adversarial review ([`SAM_ATTACHMENT_DOWNLOAD_PLAN_REVIEW.md`](SAM_ATTACHMENT_DOWNLOAD_PLAN_REVIEW.md)) §4 required edits: file-grain dedup (C1), append checkpointing (H1), size-based resume + status mapping (H2), bandwidth budgets (H3), durable blob tier (M1), sha256 demotion (M2), phantom-row gate (M3), reconciliation grain (M4), and the L1–L4 corrections.

Stage 3 of the GovCon scope-document pipeline: download the actual attachment
**bytes** (PDF/DOC/DOCX/…) for a prioritized slice of the attachment manifest,
land them in R2, and track every file in a verifiable, auditable ledger.

> **No LLM / no model in this stage.** Download is pure deterministic I/O
> (HTTP GET → bytes → hash → store → ledger row). Text extraction and embedding
> are *downstream* stages and are explicitly **out of scope** here.

---

## 0. Prerequisites & ground truth (already done — do not rebuild)

- **The manifest exists and is complete** (the "link substrate"):
  - URI: `s3://data-sink/active/sam_opps_attachment_manifest/` (Lance)
  - **331,401 rows** across **79,211 active solicitations** (100% coverage). **But note the grain (see §1.5):** a row is one *notice-attachment citation*, NOT one file. The 331,401 rows resolve to **118,739 distinct `resource_id`s** (physical files) over **65,331 notices**; `attachment_id` is unique per row (331,401, the citation key).
  - **~24.7% of rows are phantoms** — 81,770 rows have `size_bytes ∈ {0, NULL}` (81,753 also `file_name IS NULL`). **Downloadable rows** = `size_bytes >= 1 AND file_name IS NOT NULL`. Always quote downloadable-row / distinct-file counts, never the raw 331,401.
  - Manifest columns: `notice_id, solicitation_number, naics_code, psc_code, title, posted_date, ui_link, trigger_relevant, trigger_legs, attachment_id, resource_id, attachment_order, file_name, mime_type, size_bytes, access_level, export_controlled, download_url, harvested_at, snapshot_date`.
  - Indices: BTREE `notice_id` / `resource_id` / `naics_code`; BITMAP `trigger_relevant` / `mime_type` / `access_level`.
  - Built by [`pipelines/sam_gov/sam_attachment_manifest.py`](../../pipelines/sam_gov/sam_attachment_manifest.py).

- **The download endpoint is open and unauthenticated** (SAM public backend — NOT the metered `api.sam.gov` gateway):
  ```
  GET https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{resource_id}/download
  ```
  `200 application/octet-stream`, honors HTTP `Range`, **no api_key, no quota**. The full URL is stored per-row in `download_url`. **Observed (review F3/F4):** public files return bytes whose length equals `size_bytes` **for files < 10 MB only**; restricted files return **`401`** (JSON body); dead/removed resources return **`400`**. ⚠️ **`size_bytes` is corrupted for files ≥10 MB** (verified 2026-06-06): SAM reports `((true−1) mod 10,000,000)+1`, a lower bound — F3's 4-file sample was all < 1.7 MB and missed it. Use `size_downloaded` as the true size; enforce the 50 MB cap on real Content-Length at fetch.

- **Execution conventions (MANDATORY):**
  - **Run LOCAL / in-session, NOT on Modal** — SAM throttles datacenter egress IPs (429); residential IP is clean.
  - Creds via Doppler: `doppler run --project core-x --config prd -- …` → `R2_ACCESS_KEY_ID/SECRET/ENDPOINT`, `HQX_DB_URL_POOLED`. Deps via uv (`--with pylance pyarrow requests 'psycopg[binary]' boto3`).
  - **Launch detached** (`subprocess.Popen(..., start_new_session=True)`; macOS has no `setsid`), log to `/tmp/sam_download.log`, monitor with `pgrep -f sam_attachment_download` (the python process) + the log. Not harness-tracked → no auto-completion ping.
  - **Politeness:** single-threaded, ~4 req/s (0.15–0.25s sleep), backoff on 429/503. R2 `storage_options = {aws_access_key_id, aws_secret_access_key, endpoint, region:"auto"}`.
  - **Pin `data_storage_version="2.1"`** on all new datasets (per `02_lancedb_storage.md` §2.3 — do NOT copy the harvester's `"2.0"`).

---

## 1. Pick a prioritization tier (operator decision)

Funnel (downloadable rows only; counts are **rows** — see §1.5 for the distinct-file truth):

```
TOTAL rows                              331,401          (incl. 81,770 phantom rows)
downloadable (size>=1 & file_name set)  ~249,631 | 874.1 GB
public (access_level='public')           308,171 | 639.2 GB
  + text mime (pdf/docx/doc/txt)         199,974 | 589.3 GB
    + export_controlled=false            199,765 | 589.3 GB   (724 EC rows excluded)
        trigger                           65,825 | 443.3 GB | 7,550 notices
        non-trigger                      133,940 | 145.9 GB | 26,881 notices
```

`>50 MB` files are **1.3% of files / 40% of bytes** — always cap size.

**Candidate tiers** (still expressed as rows for comparability; the run iterates the **distinct-`resource_id`** collapse of the chosen tier — §1.5):

| Tier | Definition | Rows | GB | Notices |
|---|---|--:|--:|--:|
| **T0** | trigger · high-value name · 10KB–50MB | 5,363 | 20.1 | 3,362 |
| **T2** | trigger · primary doc (`attachment_order=1`) · text | 6,218 | 25.3 | 5,365 |
| **T3** | all sectors · high-value name · 10KB–50MB | 15,510 | 28.5 | 10,643 |
| **T1** | trigger · all text · `< 50MB` | 63,342 | 231.2 | 7,540 |
| **T4** | all sectors · all text · `< 50MB` | 196,812 | 356.3 | 34,404 |

**Gates:** `public` = `access_level='public'`; `text` = `mime_type IN ('pdf','docx','doc','txt')`; `not-EC` = `export_controlled=false`; `size cap` = `10_000 <= size_bytes < 50_000_000` (exclusive 50MB — matches counts); `non-empty` = `size_bytes >= 1 AND file_name IS NOT NULL` (M3, applied to every tier); `high-value name` = `lower(file_name)` contains any of `sow, pws, "statement of work", "performance work", "scope of work", "statement of objectives", "specification", soo`.

**Default:** **T0 ∪ T2** (precision + recall), validate extraction yield, then widen to T3/T1.

---

## 1.5 Data model — two grains, two keys (C1, the critical fix)

A row is a **citation**, not a file. Verified counts: 331,401 rows · 331,401 distinct `attachment_id` · **118,739 distinct `resource_id`** · 65,331 notices.

| Entity | Grain | Unique key | Where |
|---|---|---|---|
| **Manifest** (citation/junction) | one notice-attachment | `attachment_id` (= `notice_id`+`resource_id`) | existing |
| **File ledger** (download/storage) | one physical file | **`resource_id`** | this stage |

- **The downloader MUST iterate distinct `resource_id`,** not manifest rows. Collapse the chosen tier first:
  ```sql
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY resource_id ORDER BY attachment_order, notice_id) rn
    FROM <tier_filtered_manifest>
  ) WHERE rn = 1
  ```
  → one GET, one R2 object, one ledger row per file. (Re-publish the chosen tier's true budget as `count(distinct resource_id)` files / `sum(size_bytes)` over the deduped set.)
- **R2 path stays flat by file:** `s3://data-sink/active/sam_attachment_blobs/<resource_id>` — `resource_id` *is* the file's identity, so this is content/identity-addressed and naturally idempotent (the duplication is in citations, not files). See §2 / M1 for why `active/` not `landing/`.
- **Join** `manifest ⨝ ledger ON resource_id` intentionally fans one downloaded file out to its many citing notices.

---

## 2. Storage design

1. **Bytes → durable R2 blob tier** (M1): `s3://data-sink/active/sam_attachment_blobs/<resource_id>`. This is a **durable unstructured-blob tier whose system-of-record catalog IS the Lance ledger** — addressed as CAS by `resource_id`. (NOT `landing/`, which is the ephemeral Gen-2 zone subject to lifecycle sweeps. No TTL on this prefix.)

2. **File ledger → Lance `s3://data-sink/active/sam_attachment_files/`** — **one row per distinct `resource_id`** (object identity; 1→many to manifest citations). The auditable SoR for the download:

   | column | type | meaning |
   |---|---|---|
   | `resource_id` | string | object identity (1→many to manifest rows; NOT a manifest PK) |
   | `status` | string | `downloaded` / `failed` / `restricted` / `gone` |
   | `http_status` | int32 | actual response code |
   | `sha256` | string | content hash — integrity (+ opportunistic cross-object dedup; M2) |
   | `size_expected` / `size_downloaded` | int64 | manifest `size_bytes` (corrupted mod 10 MB for ≥10 MB — lower bound) vs actual bytes (true size) |
   | `size_match` | bool | bytes **modulo-10 MB consistent** with `size_expected` (not raw `==`); `false` ⇒ real anomaly |
   | `mime_claimed` / `mime_sniffed` / `mime_match` | string/string/bool | declared vs magic-byte |
   | `stored_uri` | string | exact R2 path |
   | `attempts`, `first_attempt_at`, `completed_at`, `error` | | trail |
   | `run_id`, `worklist_tier` | string | provenance |

   Build BTREE `resource_id` / `sha256`, BITMAP `status` / `worklist_tier` **once at the end**.

3. **Run ledger → Postgres `ops.sam_attachment_download_runs`** (one row per batch; same pattern as `ops.sam_*_runs`):
   ```sql
   CREATE TABLE IF NOT EXISTS ops.sam_attachment_download_runs (
     id bigserial PRIMARY KEY, run_id text NOT NULL, worklist_tier text, worklist_filter text,
     attempted int, downloaded int, failed int, restricted int, gone int,
     bytes_downloaded bigint, sustained_mbps numeric, size_mismatches int, mime_mismatches int,
     status text, error text, started_at timestamptz, completed_at timestamptz );
   ```

---

## 3. The downloader — build spec

New file `pipelines/sam_gov/sam_attachment_download.py` (pure python; module + `run_download(...)` + argparse `_cli()`; no Modal). Per **distinct-`resource_id`** worklist item:

1. **Resume skip (size-based, NOT hash — H2):** skip iff ledger row `status='downloaded'` AND `head_object(<resource_id>)` succeeds AND `ContentLength == size_downloaded`. (Hash is computed once at download and stored as an integrity record — it is never a resume gate, since re-hashing would require re-downloading.)
2. `GET download_url` streaming, polite headers, timeout 90s. **Status mapping (H2):**
   - `200` → write + ledger `downloaded`.
   - `401` (or `403` whose JSON body is `UNAUTHORIZED`) → `restricted`, **no retry**.
   - `400`/`410` on a previously-valid URL → `gone`, **no retry**.
   - `429` / `503` / `5xx` / network → exponential backoff (cap 120s, ~6×), then retry. **`403` is NOT auto-retried** unless the body is a WAF/throttle page.
3. Stream to buffer; compute `sha256`, `size_downloaded`; sniff magic bytes (`%PDF`, `PK\x03\x04`, `\xd0\xcf\x11\xe0`, …) → `mime_sniffed`; set `size_match` / `mime_match` (flagged, never silently accepted).
4. Write bytes to `s3://data-sink/active/sam_attachment_blobs/<resource_id>` via boto3 (R2 endpoint, s3v4). Append the ledger row to the in-memory buffer.
5. Sleep `inter_call_sleep`. **Checkpoint (H1):** every `--checkpoint-every` (e.g. 1000) `lance.write_dataset(buffer_arrow, ledger_uri, mode="append", data_storage_version="2.1", storage_options=so)` then clear the buffer. **Indices built once at the end** — never per checkpoint, never `mode="overwrite"`.

**CLI:** `--tier`, `--resume`, `--max-files N` (smoke), `--inter-call-sleep 0.2`, `--checkpoint-every 1000`, `--blob-prefix`, `--ledger-uri`, `--run-id`.

---

## 4. Verification & reconciliation (auditability)

Write-time: `sha256` + `size_match` + `mime_match` per file, flagged on mismatch.

**Reconciliation pass** (`pipelines/sam_gov/sam_attachment_reconcile.py`, standalone, re-runnable) — verified mechanically viable (review F2: boto3 PUT/LIST/HEAD/DELETE all succeed against core-x R2). Computed on **distinct `resource_id`** (M4):
1. `list_objects_v2` the blob prefix → stored `resource_id`s + sizes.
2. Load ledger.
3. Report: `orphans` (object, no `downloaded` row) · `missing` (`downloaded` row, no object / size 0) · `corrupt` (object size ≠ ledger `size_downloaded`, or sampled re-hash ≠ `sha256`).
4. **Store is provably consistent when `orphans = missing = corrupt = 0`.**

---

## 5. Legibility

- **Per-notice state:** `manifest ⨝ ledger ON resource_id` (one file → its citing notices).
- **Status snapshot** (read-only): distinct-file worklist total · downloaded · failed · restricted · gone · bytes · sustained MB/s · size/mime mismatches · reconciliation status.

---

## 6. End-to-end execution steps

1. **Confirm inputs** — re-derive manifest counts live (don't hard-code; daily re-snapshot).
2. **Confirm tier** (default T0 ∪ T2). **Materialize the distinct-`resource_id` worklist** (§1.5) as a versioned Lance subset `s3://data-sink/active/sam_attachment_worklist_<tier>/`; print **distinct-file count + GB**.
3. **Build** `sam_attachment_download.py` per §3. `py_compile`; unit-test the magic-byte sniffer, the size-based resume predicate, and the status mapping.
4. **Smoke:** `--max-files 40 --ledger-uri …/_smoke_attach_files/ --blob-prefix …/_smoke_blobs/`. Verify bytes land, hashes/flags set, ledger **appended** + indexed, ops row inserted, **and record sustained MB/s** (feeds H3 budget). 0 errors.
5. **Launch detached** (start_new_session, `/tmp/sam_download.log`, `--resume`). Confirm the python process is alive (`pgrep -f sam_attachment_download`).
6. **Monitor** log + `pgrep`; on death, relaunch identical command with `--resume`.
7. **Reconcile** (§4): require `orphans=missing=corrupt=0`.
8. **Acceptance** (§7); write terminal ops row (incl. `sustained_mbps`).
9. **Land code** (git lifecycle): commit downloader + reconcile script, PR → `main` off a **fresh branch off latest `main`**, squash-merge, pull into the operator `main` checkout, verify on disk.

---

## 7. Acceptance criteria (definition of done)

Computed on **distinct `resource_id`** (M4):
- **`downloaded / (worklist_distinct − gone) >= 0.99`** (the `gone` bucket carves out attachments removed between harvest and download — `400/410`).
- `size_match=false` and `mime_match=false` each **< 0.5%**, every such row inspected/explained. (`size_match` is modulo-10 MB consistency, not raw equality — SAM's ≥10 MB mod-10 MB corruption is excluded so it does not inflate this; a `false` is a genuine truncation/wrong-file anomaly.)
- Reconciliation: **`orphans = missing = corrupt = 0`.**
- `ops.sam_attachment_download_runs` terminal row present with counts + bytes + `sustained_mbps` + the exact `worklist_filter`.
- Ledger `sam_attachment_files` indexed (once, at end) and joins cleanly to the manifest on `resource_id`.
- Code merged to `main` and present on the operator checkout disk.

---

## 8. Throughput / time budget (H3 — request-rate is NOT the limiter at scale)

For each tier compute **both** and take the **max** as wall-clock:
- **Request budget** = distinct files ÷ req/s (~4/s).
- **Bandwidth budget** = tier GB ÷ measured sustained MB/s (record from the §6.4 smoke).

Large tiers are **bandwidth-bound:** T1 (231 GB) / T4 (356 GB) take *many hours of transfer* regardless of request count — do not quote a request-rate-derived ETA for them. T0∪T2 (~30–40 GB) is modest on both axes.

---

## 9. Guardrails & non-goals

- **No LLM / no extraction / no embedding** in this stage. Bytes in, bytes stored, ledger written.
- **Iterate distinct `resource_id`, not manifest rows** (C1). The ledger PK is `resource_id` (file), the manifest PK is `attachment_id` (citation) — different grains.
- **Checkpoint with `mode="append"`, never `"overwrite"`** (H1). Index once at end. Pin `"2.1"`.
- **Resume on size, never hash** (H2). Map `401`→restricted, `400/410`→gone; don't retry auth `401/403`.
- **Do not download** `access_level != 'public'` or `export_controlled = true` (724 rows).
- **Bytes → durable `active/sam_attachment_blobs/`** (Lance ledger is SoR; R2 = CAS blobs), **not** ephemeral `landing/`.
- **Apply the non-empty gate** `size_bytes>=1 AND file_name IS NOT NULL` to every worklist (M3 — ~24.7% of manifest rows are phantoms).
- **Not on Modal / any datacenter IP.** Local residential only, polite pacing.
- All §0/§1 counts are *as-of-harvest* — re-derive from the live manifest at run time.
