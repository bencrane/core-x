# DIRECTIVE — SAM.gov Solicitation Attachment: Content Download & Audit Ledger

**You are a fresh executor agent. This file is self-contained. Read nothing else to execute it.**
**Working repo:** `core-x` (data/compute plane). **Stage:** download the actual attachment
**bytes** (PDF/DOC/DOCX/…) for a prioritized slice of an already-built attachment manifest,
store them durably in R2, and record every file in a verifiable, auditable ledger.

> **This stage is pure deterministic I/O. No LLM, no embedding, no extraction.**
> HTTP GET → bytes → hash → store → ledger row. Text extraction and embedding are
> separate downstream stages and are **out of scope**. Do not introduce any model.

This directive is the authoritative executable spec. (Provenance, not required reading:
it consolidates `SAM_ATTACHMENT_DOWNLOAD_EXECUTION_PLAN.md` + its adversarial review
`SAM_ATTACHMENT_DOWNLOAD_PLAN_REVIEW.md`; every number and behavior below was verified
live on 2026-06-06.)

---

## 1. Mission

The manifest (the "link substrate") already exists and maps every active solicitation to its
attachment download URLs. Your job: pick a prioritized subset, download the **distinct files**
in it from SAM's open backend, land the bytes in R2, and produce a ledger that lets anyone
later **prove** what was fetched, that each file is intact, and that the store matches the
ledger. Then land the code.

---

## 2. Operating constraints (MANDATORY — violating these breaks the run)

1. **Run LOCAL / in-session on an operator machine. NEVER on Modal or any datacenter IP.**
   SAM throttles shared datacenter egress (HTTP 429 on the first call); a residential IP is clean.
2. **Credentials via Doppler:** prefix every data command with
   `doppler run --project core-x --config prd -- …`. Provides `R2_ACCESS_KEY_ID`,
   `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT`, `HQX_DB_URL_POOLED`.
3. **Dependencies via uv:** `uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' --with boto3 python …`. No repo venv.
4. **Network/R2 calls run with the Bash sandbox disabled** (`dangerouslyDisableSandbox: true`).
5. **Politeness:** single-threaded, **~4 requests/sec** (0.15–0.25 s inter-call sleep). Back off on 429/503. Do not parallelize aggressively — getting the IP throttled defeats the run.
6. **Launch the full run DETACHED** so a session/terminal interruption cannot kill it:
   Python `subprocess.Popen(..., start_new_session=True)` (macOS has **no** `setsid`),
   stdout/stderr → `/tmp/sam_download.log`, `stdin=DEVNULL`. Monitor with
   `pgrep -f sam_attachment_download` (the python process) + the log. It is **not**
   harness-tracked, so there is no automatic completion signal — check explicitly.
7. **All new Lance datasets pin `data_storage_version="2.1"`.**
8. R2 `storage_options = {"aws_access_key_id":…, "aws_secret_access_key":…, "endpoint":R2_ENDPOINT, "region":"auto"}`. For per-object PUT/LIST/HEAD use a boto3 S3 client (`endpoint_url=R2_ENDPOINT`, `region_name="auto"`, signature v4).

---

## 3. Ground truth you can rely on (verified live)

**Manifest** — `s3://data-sink/active/sam_opps_attachment_manifest/` (Lance):
- **331,401 rows. A row is one _notice-attachment citation_, NOT one file.** Re-derive counts at run time (the active set re-snapshots daily); the figures here are as-of-harvest.
- Distinct-`resource_id` (physical files): **118,739**. Distinct `attachment_id` (citation key, no nulls): **331,401**. Notices with ≥1 attachment: **65,331**.
- **~81,770 rows (24.7%) are phantoms** — `size_bytes ∈ {0, NULL}` (81,753 also `file_name IS NULL`). They are not downloadable.
- Columns: `notice_id, solicitation_number, naics_code, psc_code, title, posted_date, ui_link, trigger_relevant, trigger_legs, attachment_id, resource_id, attachment_order, file_name, mime_type, size_bytes, access_level, export_controlled, download_url, harvested_at, snapshot_date`.

**Download endpoint** (open, unauthenticated — the SAM public backend, NOT `api.sam.gov`):
```
GET https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{resource_id}/download
```
- `200 application/octet-stream`, honors HTTP `Range`, **no api_key, no quota**. The full URL is stored per-row in `download_url`.
- **Verified behaviors you must code to:**
  - Public file → `200`; magic bytes match `mime_type`. Downloaded byte length equals `size_bytes` **only for files < 10 MB**. ⚠️ For files ≥10 MB, `size_bytes` is corrupted to `((true−1) mod 10,000,000)+1` (verified 2026-06-06) — a lower bound, not the real size. Check integrity by **modulo-10 MB consistency**, never raw equality; treat `size_downloaded` as the true size.
  - Restricted (`access_level!='public'` or `export_controlled=true`) → **`401`** with a JSON `UNAUTHORIZED` body.
  - Removed/dead resource → **`400`** (JSON `BAD_REQUEST`).

**R2 reconciliation is mechanically viable** — boto3 `put_object` / `list_objects_v2` / `head_object` / `delete_object` all succeed against core-x R2 with the provided creds. (Listing the prefix works; do not assume otherwise.)

**Postgres** — schema `ops` exists; follow the `ops.sam_*_runs` convention.

---

## 4. The data model you MUST honor — two grains, two keys

A manifest row is a **citation**; the same physical file is cited by many notices, so
`resource_id` repeats (331,401 citations → 118,739 files). Therefore:

| Entity | Grain | Unique key |
|---|---|---|
| **Manifest** (citation / junction) | one notice-attachment | `attachment_id` (≡ `notice_id`+`resource_id`) |
| **File ledger** (what you build) | one **physical file** | **`resource_id`** |

- **You download per distinct `resource_id`, NOT per manifest row.** One GET, one R2 object,
  one ledger row per file. (Iterating raw rows would re-fetch shared files up to N times.)
- The R2 object path is flat and file-identity-addressed: `…/sam_attachment_blobs/<resource_id>`
  — naturally idempotent (the repetition is in citations, not files).
- `manifest ⨝ ledger ON resource_id` intentionally fans one downloaded file out to its many
  citing notices — that is the per-notice view, and is correct.

---

## 5. Step 1 — choose and materialize the worklist (distinct files)

**5a. Gates** (apply ALL; predicates are exact):
- `non_empty`  = `size_bytes >= 1 AND file_name IS NOT NULL`  *(drops the 24.7% phantoms — apply to every tier)*
- `public`     = `access_level = 'public'`
- `not_ec`     = `export_controlled = false`  *(724 EC rows excluded)*
- `text`       = `mime_type IN ('pdf','docx','doc','txt')`
- `size_cap`   = `10_000 <= size_bytes < 50_000_000`  *(**declared-size prefilter, NOT a real-size bound** — `size_bytes` is corrupted mod 10 MB for ≥10 MB files, so a real ≥50 MB file can declare <50 MB and pass; the real 50 MB ceiling is enforced at fetch on post-redirect Content-Length + stream length → `oversize`. The "1.3% of files / 40% of bytes" split is itself computed from corrupted declared sizes and understates large-file bytes.)*
- `high_value` = `lower(file_name)` contains any of: `sow`, `pws`, `statement of work`, `performance work`, `scope of work`, `statement of objectives`, `specification`, `soo`

**5b. Tiers** (counts are *citation rows* as-of-harvest, for comparison; the run iterates the
**distinct-`resource_id`** collapse — see 5c):

| Tier | Definition | Rows | GB | Notices |
|---|---|--:|--:|--:|
| **T0** | trigger · high_value · size_cap | 5,363 | 20.1 | 3,362 |
| **T2** | trigger · `attachment_order=1` · text | 6,218 | 25.3 | 5,365 |
| **T3** | all sectors · high_value · size_cap | 15,510 | 28.5 | 10,643 |
| **T1** | trigger · text · size_cap | 63,342 | 231.2 | 7,540 |
| **T4** | all sectors · text · size_cap | 196,812 | 356.3 | 34,404 |

`trigger` = `trigger_relevant = true` (NAICS sector 23 ∪ PSC N063/C1AZ). **Default tier: T0 ∪ T2**
(precision + recall). Confirm the tier with the operator before the full run.

**5c. Collapse to distinct files and persist the worklist.** After applying the chosen tier's
gates, deduplicate to one row per `resource_id`, then write the worklist as a versioned Lance
dataset `s3://data-sink/active/sam_attachment_worklist_<tier>/`:
```sql
SELECT * FROM (
  SELECT *, row_number() OVER (PARTITION BY resource_id ORDER BY attachment_order, notice_id) AS rn
  FROM <tier_filtered_manifest>
) WHERE rn = 1
```
Print the **distinct-file count** and `sum(size_bytes)` GB. ⚠️ The byte total is a **lower bound** — `size_bytes` is corrupted mod 10 MB for ≥10 MB files, so it undercounts true storage. The real storage budget is `sum(size_downloaded)` over the ledger after the run; the request count (distinct files) is exact.

---

## 6. Step 2 — storage layout

1. **Bytes → durable R2 blob tier:** `s3://data-sink/active/sam_attachment_blobs/<resource_id>`.
   This is a durable unstructured-blob tier; its **system-of-record catalog is the Lance ledger**
   (§6.2), and the objects are content/identity-addressed by `resource_id`. **Use `active/`, not
   `landing/`** (landing is the ephemeral zone subject to lifecycle deletion). No TTL on this prefix.

2. **File ledger → Lance `s3://data-sink/active/sam_attachment_files/`**, **one row per distinct
   `resource_id`** — the auditable system of record for the download:

   | column | type | meaning |
   |---|---|---|
   | `resource_id` | string | file identity (1→many to manifest citations; not a manifest PK) |
   | `status` | string | `downloaded` / `failed` / `restricted` / `gone` |
   | `http_status` | int32 | actual response code |
   | `sha256` | string | content hash — integrity (+ opportunistic cross-file dedup) |
   | `size_expected` / `size_downloaded` | int64 | manifest `size_bytes` vs actual bytes |
   | `size_match` | bool | bytes are **modulo-10 MB consistent** with `size_expected` (not raw `==`, since `size_expected`/`size_bytes` is corrupted mod 10 MB for ≥10 MB files) — `false` ⇒ real truncation/wrong-file anomaly |
   | `mime_claimed` / `mime_sniffed` / `mime_match` | string/string/bool | declared vs magic-byte |
   | `stored_uri` | string | exact R2 object path |
   | `attempts` | int32 | retry count |
   | `first_attempt_at` / `completed_at` | timestamp(us, UTC) | timing |
   | `error` | string | failure detail (nullable) |
   | `run_id` / `worklist_tier` | string | provenance |

   Build indices **once, at the very end**: BTREE `resource_id`, `sha256`; BITMAP `status`, `worklist_tier`.

3. **Run ledger → Postgres `ops.sam_attachment_download_runs`** (one row per batch):
   ```sql
   CREATE TABLE IF NOT EXISTS ops.sam_attachment_download_runs (
     id bigserial PRIMARY KEY, run_id text NOT NULL, worklist_tier text, worklist_filter text,
     attempted int, downloaded int, failed int, restricted int, gone int,
     bytes_downloaded bigint, sustained_mbps numeric, size_mismatches int, mime_mismatches int,
     status text, error text, started_at timestamptz, completed_at timestamptz );
   ```

---

## 7. Step 3 — build the downloader

New file `pipelines/sam_gov/sam_attachment_download.py` — pure python (module +
`run_download(...)` + argparse `_cli()` + `__main__`; **no Modal**). Iterate the distinct-file
worklist (§5c). Per file:

1. **Resume skip — SIZE-BASED, never hash.** Skip iff a ledger row has `status='downloaded'`
   AND `head_object(<resource_id>)` succeeds AND `ContentLength == size_downloaded`. (Hash is
   computed once at download as an integrity record; re-hashing would require re-downloading, so
   it is **never** a resume gate.)
2. `GET download_url` (stream, polite headers, timeout 90 s). **Map status codes exactly:**

   | response | action |
   |---|---|
   | `200` | write bytes, ledger `downloaded` |
   | `401` (or `403` whose body is `UNAUTHORIZED`) | ledger `restricted`, **no retry** |
   | `400` / `410` on a previously-valid URL | ledger `gone`, **no retry** |
   | `429` / `503` / `5xx` / network error | exponential backoff (cap 120 s, ≤6×), then retry |
   | other hard `4xx` | ledger `failed`, **no retry** |

   Do **not** auto-retry plain `403`/`401` (permanent auth refusals — would burn 6×120 s each).
3. Stream to buffer; compute `sha256` and `size_downloaded`; sniff magic bytes
   (`%PDF` → pdf, `PK\x03\x04` → docx/xlsx/zip, `\xd0\xcf\x11\xe0` → legacy doc/xls, …) → `mime_sniffed`.
   Set `size_match` and `mime_match`. **Flag mismatches in the row — never silently accept**
   (catches truncations and HTML-error-page-saved-as-PDF).
4. `put_object` the bytes to `…/sam_attachment_blobs/<resource_id>` (boto3, R2 endpoint, s3v4).
   Append the ledger row to an in-memory buffer.
5. Sleep `inter_call_sleep`. **Checkpoint with APPEND, never overwrite:** every
   `--checkpoint-every` (default 1000) call
   `lance.write_dataset(buffer_arrow, ledger_uri, mode="append", data_storage_version="2.1", storage_options=so)`
   then clear the buffer. Indices are built **once at the end**, not per checkpoint.
   (Overwriting the whole dataset per checkpoint is O(n) write-amplification and version bloat — forbidden.)

**CLI flags:** `--tier`, `--resume`, `--max-files N` (smoke), `--inter-call-sleep 0.2`,
`--checkpoint-every 1000`, `--blob-prefix`, `--ledger-uri`, `--run-id`.

---

## 8. Step 4 — smoke test (before the full run)

```
doppler run --project core-x --config prd -- uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' --with boto3 \
  python pipelines/sam_gov/sam_attachment_download.py \
  --tier <chosen> --max-files 40 \
  --ledger-uri s3://data-sink/active/_smoke_attach_files/ \
  --blob-prefix s3://data-sink/active/_smoke_blobs/
```
Verify: bytes land in R2; `sha256`/`size_match`/`mime_match` populated; ledger **appended** then
indexed; `ops.sam_attachment_download_runs` row inserted; 0 unexpected errors; **record the
sustained MB/s** (feeds §11). Clean up the `_smoke_*` datasets/objects after.

---

## 9. Step 5 — launch full run (detached) + monitor

Launch via `Popen(..., start_new_session=True)` → `/tmp/sam_download.log`, with `--resume`.
Confirm the python process is alive: `pgrep -f sam_attachment_download`. Monitor the log.
**If the process dies, relaunch the identical command with `--resume`** — it skips
already-downloaded+size-verified files and continues; loss is bounded by the last checkpoint window.

---

## 10. Step 6 — reconcile (the audit closure), Step 7 — accept

**Reconciliation** (`pipelines/sam_gov/sam_attachment_reconcile.py`, standalone, re-runnable),
computed on **distinct `resource_id`**:
1. `list_objects_v2` the blob prefix → stored `resource_id`s + object sizes.
2. Load the ledger.
3. Report: `orphans` (object, no `downloaded` row) · `missing` (`downloaded` row, no object or size 0)
   · `corrupt` (object size ≠ ledger `size_downloaded`, or sampled re-hash ≠ stored `sha256`).

**Acceptance (definition of done):**
- `downloaded / (worklist_distinct − gone) >= 0.99` (the `gone` bucket carves out files removed
  between harvest and download — `400/410`).
- `size_match=false` and `mime_match=false` each **< 0.5%**, every such row inspected/explained.
- Reconciliation: **`orphans = missing = corrupt = 0`.**
- `ops.sam_attachment_download_runs` has the terminal row (counts, bytes, `sustained_mbps`, exact `worklist_filter`).
- Ledger indexed (once, at end) and joins cleanly to the manifest on `resource_id`.
- Code merged to `main` and present on the operator checkout disk (§12).

---

## 11. Throughput / time budget (do not quote a request-rate ETA for large tiers)

For the chosen tier compute **both**, take the **max** as wall-clock:
- **Request budget** = distinct files ÷ ~4/s.
- **Bandwidth budget** = tier GB ÷ measured sustained MB/s (from the §8 smoke).

Large tiers are **bandwidth-bound**: T1 ≈ 231 GB, T4 ≈ 356 GB take many hours of transfer
regardless of request count. T0 ∪ T2 (~30–40 GB) is modest on both axes.

---

## 12. Step 8 — land the code (git lifecycle — you own it end-to-end)

Commit `sam_attachment_download.py` + `sam_attachment_reconcile.py`. Open a PR against `main`
from a **fresh branch off latest `main`** (avoids squash-divergence). Squash-merge it yourself,
then `git pull` into the operator `main` checkout and verify the files are on disk
(`git log -1 --oneline`). Done = the operator's checkout reflects the merge.

---

## 13. Guardrails & non-goals (hard stops)

- **No LLM / no extraction / no embedding.** Bytes in, bytes stored, ledger written.
- **Iterate distinct `resource_id`, not manifest rows.** Ledger key = `resource_id` (file); manifest key = `attachment_id` (citation).
- **Checkpoint `mode="append"`, never `"overwrite"`; index once at end; pin `"2.1"`.**
- **Resume on size (`head_object` ContentLength), never hash.**
- **Status map:** `401`→restricted, `400/410`→gone, retry only `429/503/5xx/network`; never auto-retry auth `401/403`.
- **Skip `access_level != 'public'` and `export_controlled = true`.** Apply the non-empty gate.
- **Bytes → durable `active/sam_attachment_blobs/`** (Lance ledger is SoR; R2 = CAS blobs). Not `landing/`.
- **Local residential IP only, polite pacing.** Never Modal/datacenter.
- **Re-derive all counts from the live manifest at run time** — the figures in §3/§5 are as-of-harvest and drift daily.
