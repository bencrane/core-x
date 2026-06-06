# SAM.gov Solicitation Attachment — Content Download & Audit Ledger

**Execution plan. Self-contained. Another agent should be able to run this end-to-end.**

Stage 3 of the GovCon scope-document pipeline: download the actual attachment
**bytes** (PDF/DOC/DOCX/…) for a prioritized slice of the attachment manifest,
land them in R2, and track every file in a verifiable, auditable ledger.

> **No LLM / no model in this stage.** Download is pure deterministic I/O
> (HTTP GET → bytes → hash → store → ledger row). Text extraction and embedding
> are *downstream* stages and are explicitly **out of scope** here.

---

## 0. Prerequisites & ground truth (already done — do not rebuild)

- **The manifest exists and is complete** (the "link substrate"):
  - URI: `s3://data-sink/active/sam_opps_attachment_manifest/` (Lance v2.0)
  - **331,401 rows** (one per attachment) across **79,211 active solicitations** (100% coverage), **874.1 GB** total declared bytes, 65,331 notices carry ≥1 downloadable file.
  - Built by [`pipelines/sam_gov/sam_attachment_manifest.py`](../../pipelines/sam_gov/sam_attachment_manifest.py) (frontend method; on `main`).
  - Manifest columns: `notice_id, solicitation_number, naics_code, psc_code, title, posted_date, ui_link, trigger_relevant, trigger_legs, attachment_id, resource_id, attachment_order, file_name, mime_type, size_bytes, access_level, export_controlled, download_url, harvested_at, snapshot_date`.
  - Indices: BTREE `notice_id` / `resource_id` / `naics_code`; BITMAP `trigger_relevant` / `mime_type` / `access_level`.

- **The download endpoint is open and unauthenticated** (the SAM public website backend — NOT the metered `api.sam.gov` developer gateway):
  ```
  GET https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{resource_id}/download
  ```
  Returns `206 application/octet-stream`, honors HTTP `Range`, needs **no api_key**, **no quota**. The full URL is already stored per-row in `download_url`.

- **Execution conventions (MANDATORY — follow exactly):**
  - **Run LOCAL / in-session, NOT on Modal.** SAM throttles shared datacenter egress IPs (Modal got 429 on the first call); a residential IP is clean.
  - Credentials via Doppler: `doppler run --project core-x --config prd -- …` provides `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT`, `HQX_DB_URL_POOLED`.
  - Dependencies via uv: `uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' python …`
  - **Launch detached** so a session interruption can't kill it: Python `subprocess.Popen(..., start_new_session=True)` (macOS has no `setsid`), log to a stable path (`/tmp/sam_download.log`), monitor via `pgrep` + the log. The job is NOT harness-tracked, so there is no auto-completion ping — monitor explicitly.
  - **Politeness:** ~4 requests/sec (≈0.15–0.25s inter-call sleep), single-threaded. Back off on 429/403. (Validated: 80/80 → 200 at 4/s on the manifest crawl.)
  - **R2 storage_options** helper:
    ```python
    so = {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
          "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
          "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}
    ```

---

## 1. Pick a prioritization tier (operator decision)

The download set is a filtered subset of the manifest. Funnel (computed live):

```
TOTAL                                   331,401 files | 874.1 GB
public (access_level='public')          308,171 files | 639.2 GB   (drops 23,230 private)
  + text mime (pdf/docx/doc/txt)        199,974 files | 589.3 GB
    + not export_controlled             199,765 files | 589.3 GB   (only 209 ITAR-flagged)
        trigger slice                    65,825 files | 443.3 GB | 7,550 notices
        non-trigger                     133,940 files | 145.9 GB | 26,881 notices
```

Size concentration (within public+text+not-ec): **`>50 MB` files are 1.3% of files but 40% of bytes (2,601 files / 232.9 GB)** — always cap size.

**Candidate tiers (exact):**

| Tier | Definition | Files | GB | Notices |
|---|---|--:|--:|--:|
| **T0** | trigger · high-value name · 10KB–50MB | 5,363 | 20.1 | 3,362 |
| **T2** | trigger · primary doc (`attachment_order=1`) · text | 6,218 | 25.3 | 5,365 |
| **T3** | all sectors · high-value name · 10KB–50MB | 15,510 | 28.5 | 10,643 |
| **T1** | trigger · all text · ≤50MB | 63,396 | 231.2 | 7,540 |
| **T4** | all sectors · all text · ≤50MB | 197,164 | 356.3 | 34,416 |

**Gate definitions:**
- `public` = `access_level = 'public'`
- `text` = `mime_type IN ('pdf','docx','doc','txt')`
- `not export-controlled` = `export_controlled = false`
- `size cap` = `10_000 <= size_bytes < 50_000_000`
- `high-value name` = `lower(file_name)` contains any of: `sow, pws, "statement of work", "performance work", "scope of work", "statement of objectives", "specification", soo`
- (drop-list, for reference) `boilerplate` = name contains: `wage determination, sf1442, amendment, provisions, representations, sign-in, q&a`

**Default recommendation:** start with **T0 ∪ T2** (trigger high-value docs + each trigger notice's primary doc — precision + recall, ≈ 8–11k files / ≈ 30–40 GB), validate extraction yield, then widen to **T3** (all-sector scope docs) or **T1** (full construction) as needed. Operator confirms the tier before the full run.

---

## 2. Storage design (three artifacts, keyed on `resource_id`)

1. **Bytes → R2 objects.** Land at:
   ```
   s3://data-sink/landing/sam_attachments/<resource_id>
   ```
   (Flat by `resource_id` — globally unique, dedup-friendly. Do NOT inline bytes into Lance.)

2. **File ledger → Lance `s3://data-sink/active/sam_attachment_files/`** — one row per attempted file. **This is the auditable system of record for the download.** Schema:

   | column | type | meaning |
   |---|---|---|
   | `resource_id` | string | PK (joins manifest) |
   | `notice_id` | string | parent |
   | `status` | string | `downloaded` / `failed` / `skipped_dupe` / `restricted` |
   | `http_status` | int32 | actual response code |
   | `sha256` | string | content hash (integrity + dedup) |
   | `size_expected` | int64 | manifest `size_bytes` |
   | `size_downloaded` | int64 | actual bytes written |
   | `size_match` | bool | `size_downloaded == size_expected` |
   | `mime_claimed` | string | manifest `mime_type` |
   | `mime_sniffed` | string | magic-byte detection (`%PDF`, `PK\x03\x04`, `\xd0\xcf\x11\xe0`, …) |
   | `mime_match` | bool | claimed vs sniffed agree |
   | `stored_uri` | string | exact R2 path of the bytes |
   | `attempts` | int32 | retry count |
   | `first_attempt_at` / `completed_at` | timestamp(us, UTC) | timing |
   | `error` | string | failure detail (nullable) |
   | `run_id` | string | provenance (which batch) |
   | `worklist_tier` | string | which tier selected it |

   Indices after write: BTREE `resource_id`, `notice_id`, `sha256`; BITMAP `status`, `worklist_tier`, `size_match`, `mime_match`.

3. **Run ledger → Postgres `ops.sam_attachment_download_runs`** — one row per batch (provenance + reproducible scope). DDL:
   ```sql
   CREATE TABLE IF NOT EXISTS ops.sam_attachment_download_runs (
     id            bigserial PRIMARY KEY,
     run_id        text NOT NULL,
     worklist_tier text,
     worklist_filter text,         -- the exact predicate used (reproducibility)
     attempted     int,
     downloaded    int,
     failed        int,
     skipped_dupe  int,
     bytes_downloaded bigint,
     size_mismatches int,
     mime_mismatches int,
     status        text,           -- success / partial / error
     error         text,
     started_at    timestamptz,
     completed_at  timestamptz
   );
   ```

---

## 3. The downloader — build spec

New file: `pipelines/sam_gov/sam_attachment_download.py`. Mirror the harvester's structure
([`sam_attachment_manifest.py`](../../pipelines/sam_gov/sam_attachment_manifest.py)) — pure-python module + `run_download(...)` + argparse `_cli()` + `if __name__ == "__main__"`. No Modal wrapper.

**Core loop (per worklist row):**
1. If `--resume` and ledger row exists with `status='downloaded'` AND the R2 object exists AND its size matches → **skip** (idempotent).
2. If a prior ledger row with the same `sha256` is already `downloaded` → record `skipped_dupe` pointing at the existing `stored_uri` (content dedup); do not re-store.
3. `GET download_url` with `requests`, streaming, polite headers, timeout 90s.
   - On `429/403/503`: exponential backoff (cap 120s), retry up to ~6×.
   - On `5xx`/network error: short backoff, retry.
   - On hard `4xx`: record `failed` with `http_status`, continue.
4. Stream bytes to a temp buffer; compute `sha256` and `size_downloaded`; sniff magic bytes for `mime_sniffed`.
5. Write bytes to `s3://data-sink/landing/sam_attachments/<resource_id>` (via boto3/obstore or lance fs; reuse R2 creds).
6. Append the ledger row (status `downloaded`, hashes, size/mime match flags, `stored_uri`, `run_id`, `worklist_tier`).
7. Sleep `inter_call_sleep`. Every `--checkpoint-every` (e.g., 500) flush the ledger to Lance (overwrite full accumulated set, atomic) so a kill loses ≤ the in-memory window; `--resume` reloads it.

**CLI flags:** `--tier <T0|T1|T2|T3|T4|custom>` (or explicit gate flags), `--resume`, `--max-files N` (smoke), `--inter-call-sleep 0.15`, `--checkpoint-every 500`, `--landing-prefix`, `--ledger-uri`, `--run-id`.

**Worklist materialization:** read the manifest, apply the tier predicate (§1), optionally persist the chosen subset as a versioned Lance dataset `s3://data-sink/active/sam_attachment_worklist_<tier>/` for reproducibility, then iterate it.

---

## 4. Verification & reconciliation (what makes it auditable)

**Write-time (in the loop):** every file gets `sha256`, `size_match`, `mime_match`. Mismatches are **flagged in the row, never silently accepted** (a truncated file or an HTML error page saved as `.pdf` shows up as `size_match=false` / `mime_match=false`).

**Reconciliation pass** — standalone, re-runnable script `pipelines/sam_gov/sam_attachment_reconcile.py`:
1. List the R2 landing prefix (`obstore`/boto2 list, or lance fs) → set of stored `resource_id` + object sizes.
2. Load the ledger.
3. Emit a report:
   - `orphans` — objects in R2 with no `downloaded` ledger row.
   - `missing` — `downloaded` rows with no object (or size 0).
   - `corrupt` — object size ≠ ledger `size_downloaded`, or (sampled) re-hash ≠ `sha256`.
   - `flagged` — rows with `size_match=false` or `mime_match=false`.
4. **Acceptance:** the store is provably consistent when `orphans = missing = corrupt = 0`.

---

## 5. Legibility (operator-facing)

- **One join** answers per-notice state: `manifest ⨝ ledger ON resource_id` → for any notice: which docs, downloaded?, where, verified?, when, which run.
- **Status snapshot** (read-only query, run on demand): worklist total · downloaded · pending · failed · bytes landed · dedup savings · size/mime mismatch counts · reconciliation status.

---

## 6. End-to-end execution steps

1. **Confirm inputs.** Read `sam_attachment_manifest` (`count_rows` == 331,401-ish; the dataset may have re-snapshotted — re-derive counts, don't hard-code).
2. **Confirm tier** with the operator (default T0 ∪ T2). Materialize the worklist subset; print exact file count + GB.
3. **Build** `pipelines/sam_gov/sam_attachment_download.py` per §3. `python -m py_compile` it. Unit-test the magic-byte sniffer + the resume/dedup predicate.
4. **Smoke test:** `--max-files 40 --ledger-uri s3://data-sink/active/_smoke_attach_files/ --landing-prefix s3://data-sink/landing/_smoke_sam_attachments/`. Verify: bytes land, hashes computed, ledger written + indexed, ops row inserted, 0 errors.
5. **Launch full run detached** (start_new_session, `/tmp/sam_download.log`, `--resume`). Confirm 3 PIDs alive after ~15s.
6. **Monitor** via log + `pgrep`; if killed, relaunch identical command with `--resume`.
7. **Reconcile** (§4) after completion; require `orphans=missing=corrupt=0`.
8. **Acceptance** (§7) met → write final ops row.
9. **Land code** (git lifecycle): commit `sam_attachment_download.py` + `sam_attachment_reconcile.py`, PR → `main`, squash-merge, pull into the operator `main` checkout, verify on disk. (Open against `main` directly; fresh branch off latest `main` to avoid squash-divergence.)

---

## 7. Acceptance criteria (definition of done)

- Chosen tier's files: **≥ 99% `status='downloaded'`** (remainder are genuine `failed` with recorded `http_status`/`error`, or `restricted`).
- `size_match=false` and `mime_match=false` each **< 0.5%**, and every such row is inspected/explained.
- Reconciliation report: **`orphans = missing = corrupt = 0`.**
- `ops.sam_attachment_download_runs` has the terminal row with counts + bytes + the exact `worklist_filter`.
- Ledger `sam_attachment_files` is indexed and joins cleanly to the manifest on `resource_id`.
- Code merged to `main` and present on the operator checkout disk.

---

## 8. Guardrails & non-goals

- **No LLM, no embedding, no extraction in this stage.** Bytes in, bytes stored, ledger written. Extraction (PyMuPDF / python-docx / LibreOffice / OCR) and embedding are separate downstream plans.
- **Do not download** `access_level != 'public'` or `export_controlled = true`.
- **Do not inline bytes into Lance** — bytes → R2 objects; Lance holds the ledger (pointers + hashes + status).
- **Do not run on Modal / any datacenter IP** — SAM throttles them. Local residential IP only.
- **Respect pacing** — single-threaded ~4/s with 429/403 backoff. Getting the IP throttled defeats the run.
- **Idempotent** — re-runs must skip already-downloaded+verified files and never double-store identical content (dedup by `sha256`).
- Treat all counts in §0/§1 as *as-of-harvest* — re-derive from the live manifest at run time; the active set re-snapshots daily.
