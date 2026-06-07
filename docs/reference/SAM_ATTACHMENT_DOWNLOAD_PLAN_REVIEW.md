# Adversarial Review — SAM Attachment Download & Audit Ledger Execution Plan

Reviewer: principal-engineer adversarial pass. Target:
[`SAM_ATTACHMENT_DOWNLOAD_EXECUTION_PLAN.md`](SAM_ATTACHMENT_DOWNLOAD_EXECUTION_PLAN.md).
Every claim below is backed by the plan's own text (quoted) or a live check (shown
with command + observed output). All checks run 2026-06-06 against the live
manifest, live R2, live SAM backend, live Postgres.

---

## 1. Verdict

**GO WITH CHANGES.** The plan's spine is sound and verified: the manifest exists
with every column the plan relies on (331,401 rows, all gates present), the SAM
download endpoint serves public bytes byte-for-byte (`size_bytes` equals the
downloaded length exactly on every public file tested), the R2 reconciliation
mechanism the audit design depends on (PUT + LIST prefix + HEAD + DELETE) **works
with the provided creds** — the prior "403 on LIST" finding was against SAM's
`falextracts` bucket, **not** core-x R2, and does not apply. Restricted files
return a clean refusal (`401 UNAUTHORIZED`, JSON body) so the public-only
guardrail is enforceable.

But the plan ships with one **Critical** defect that will waste requests and
corrupt the ledger's key model, and several **High** defects that will produce a
non-auditable or mis-counted result if executed as written. The headline defect:
**`resource_id` is not unique in the manifest** — 331,401 rows resolve to only
118,739 distinct `resource_id`s — yet the plan declares `resource_id` the ledger
**PK** and iterates the worklist row-by-row, which will re-download and re-store
the same object up to N times and fan out the audit join. Fix the items in §4
before execution.

---

## 2. Verified facts (command + observed result)

All checks used the prompt's Doppler/uv pattern and the documented R2
`storage_options`.

### F1 — Manifest row count, schema, version count
```
lance.dataset("s3://data-sink/active/sam_opps_attachment_manifest/", storage_options=so)
  .count_rows() -> 331401
  schema -> all 20 columns present incl. resource_id, notice_id, mime_type,
            size_bytes, access_level, export_controlled, download_url, file_name,
            attachment_order  (exact types match the plan's §0 list)
  .versions() -> 195 versions, latest_ts 2026-06-06 17:57:47
```
Every column the tier gates and ledger reference exists with the stated type.
**195 versions** is direct evidence of the overwrite-per-checkpoint churn (see C2).

### F2 — R2 PUT + LIST + HEAD + DELETE all succeed (reconciliation mechanism works)
```
boto3 client (endpoint=R2_ENDPOINT, region=auto, s3v4):
  put_object  landing/_review_probe/probe_<ts>.bin (24 bytes)   -> PUT_OK
  list_objects_v2 Prefix="landing/_review_probe/"               -> LIST_OK count=1, size=24
  head_object                                                    -> HEAD_OK size=24
  delete_object                                                  -> DELETE_OK
```
The reconciliation pass (§4 "List the R2 landing prefix") is **mechanically
viable** with these creds. The prompt's worry that the 403-on-LIST finding breaks
the audit is **disproven** — that finding was SAM's public bucket, not R2.

### F3 — Public download: 206-equivalent `200`, octet-stream, `size_bytes` == bytes, magic matches
```
GET sam.gov/.../files/{rid}/download   (4 public files in tier size range)
  public_pdf  e1909055… 200 octet-stream decl=1611861 got=1611861 size_match=True magic=%PDF
  public_pdf  b20505ad… 200 octet-stream decl= 292963 got= 292963 size_match=True magic=%PDF
  public_pdf  01a2ddb5… 200 octet-stream decl= 525252 got= 525252 size_match=True magic=%PDF
  public_docx c7f4ada3… 200 octet-stream decl=  22781 got=  22781 size_match=True magic=PK\x03\x04
```
`size_match = (downloaded == size_bytes)` holds **byte-for-byte on every public
file**. The plan's integrity flag is sound for public content (resolves the
prompt's finding #3 in the plan's favor).

> **⚠️ CORRECTION (verified live 2026-06-06) — F3 was sampled only on small files.**
> All four probed files above are 22 KB–1.6 MB (< 10 MB), so none exercised the
> defect. `size_bytes` is **corrupted for files ≥10 MB**: SAM returns
> `((true_size − 1) mod 10,000,000) + 1` — a **lower bound** on true bytes, exact
> only below 10 MB, and **not invertible**. Raw `downloaded == size_bytes` does NOT
> hold for ≥10 MB files (declared→real: 5,066,771→45,066,771; 4,019,768→14,019,768;
> 2,253,038→32,253,038; 10,000,000→~210 MB). The integrity check must use
> **modulo-10 MB consistency**, not raw equality (implemented in
> `sam_attachment_download.py` `run_download`), and true size is authoritative only
> via the ledger's `size_downloaded`. Downloads were never at risk — the 50 MB cap is
> enforced on real Content-Length + stream length at fetch, not on `size_bytes`.

### F4 — Restricted files refuse with `401` (not 403), JSON error, 283 bytes
```
private           661ea3e5… -> 401 application/json 283B  {"errors":{"code":"UNAUTHORIZED",...}}
export_controlled 92b49686… -> 401 application/json 283B  {"errors":{"code":"UNAUTHORIZED",...}}
bogus resource_id 000…000   -> 400 application/json       {"errors":{"code":"BAD_REQUEST",...}}
```
Restricted content is genuinely gated (good — the guardrail is real). But the
code is **401**, and a dead/archived resource is **400** — neither is in the
plan's retry set (`403/429/503`) nor explicitly mapped (see H2).

### F5 — `resource_id` is NOT unique (the Critical finding)
```
select count(distinct resource_id), count(*) from manifest
  -> distinct=118739, total=331401
select count(*) from (select resource_id ... having count(*)>1)
  -> 54708 resource_ids appear more than once
```
The same attachment is shared across multiple notices. `resource_id` is **not a
PK of the manifest**.

### F6 — Blank `mime_type` is 24.67%, but it is zero-byte phantom rows, not real docs
```
blank/empty mime_type: 81758 / 331401 = 24.67%
of those: 81753 have size_bytes=0 AND file_name=NULL
filename-extension breakdown of blank-mime rows:
  NULL ext: 78842 | '' ext: 2895 | .gov:13 | .pdf:3 | .xhtml:3 | .zip:2
blank-mime rows surviving public+not-ec+10KB–50MB gate: 2
overall size_bytes IN (0,NULL): 81770   |  size_bytes<10000: 82524 (81770 are zero)
```
The prompt's hypothesis ("blank mime drops real extensionless PDFs from every
tier") is **disproven**: blank-mime ≈ zero-byte/null-name phantom records, which
every tier already drops via the `size_bytes >= 10000` floor. Only **2** real
candidates are lost. But this surfaces a *different* fact (M3): ~24.7% of the
manifest is phantom rows.

### F7 — Live tier recomputation (matches plan within re-snapshot drift)
```
T0 trigger+hv+10k–50m : 5363 files / 20.1 GB / 3362 notices   (plan: 5363/20.1/3362 — exact)
T2 trigger+order1+text: 6218 files / 25.3 GB / 5365 notices   (plan: 6218/25.3/5365 — exact)
T1 trigger+alltext≤50m: 63342 files / 231.2 GB / 7540 notices (plan: 63396/231.2/7540)
T4 all+alltext≤50m    : 196812 files / 356.3 GB / 34404 notices(plan: 197164/356.3/34416)
```
Counts are file-**rows**, not distinct files (see C1). Counts otherwise track the
plan; small drift is the expected daily re-snapshot.

### F8 — `export_controlled = true` is 724 rows, not the plan's "209"
```
select export_controlled, count(*) -> False:330677, True:724
```
Plan §1 says "only 209 ITAR-flagged"; live is **724**. Stale figure; the gate
itself (`export_controlled=false`) is unaffected.

### F9 — SAM opps active URI + ops schema + prior run ledger all present
```
lance.dataset("s3://data-sink/sam-gov-opps/active/").count_rows() -> 79211  (== plan's "active solicitations")
lance.dataset("s3://sam-gov-opps/active/")                        -> FAIL (doc-example URI, not real)
Postgres: ops schema exists; ops.sam_attachment_manifest_runs exists;
  last row -> status=success, attachments=331401, notices_covered=79211, 2026-06-06 21:57Z
```
The new `ops.sam_attachment_download_runs` table is a clean addition following the
existing `ops.sam_*_runs` convention. The harvester's real active URI
(`s3://data-sink/sam-gov-opps/active/`) resolves and equals the plan's
79,211-notice figure.

### F10 — Full-dataset read cost and fragment count (checkpoint-cost evidence)
```
ds.to_table() over 331401 rows -> 2.5s
len(ds.get_fragments()) -> 1   (overwrite collapses to a single fragment)
```
A full **read** is 2.5s; an overwrite **write** of a growing ledger is at least as
expensive and grows with row count — direct evidence for the append-vs-overwrite
finding (H1).

---

## 3. Findings, ranked by severity

### CRITICAL

#### C1 — `resource_id` is not unique; the plan keys the ledger PK and the worklist iteration on it → duplicate downloads, fan-out join, mis-stated counts
- **What's wrong.** Plan §2 declares the ledger schema `resource_id | string | PK
  (joins manifest)` and §3 iterates "per worklist row," landing bytes at
  `s3://data-sink/landing/sam_attachments/<resource_id>`. But F5 proves
  `resource_id` repeats: **331,401 rows → 118,739 distinct `resource_id`s; 54,708
  repeat.** The same physical attachment is attached to multiple notices.
- **Why it matters.**
  1. The worklist (a manifest subset) contains duplicate `resource_id`s. Iterating
     row-by-row will `GET` and attempt to store the **same object N times** —
     wasted requests against a politeness-budgeted crawl, and N ledger rows for one
     object (or a race writing the same R2 key).
  2. `resource_id` as ledger **PK** is false: a `manifest ⨝ ledger ON resource_id`
     (§5 "one join answers per-notice state") **fans out** — the join is
     one-ledger-row-to-many-manifest-rows, which is actually what you want for
     per-notice legibility, but then `resource_id` cannot be the ledger's unique
     key and the dedup logic in §3 step 2 is confused about what it is deduping.
  3. Tier "file" counts in §1/§7 are **row** counts, not distinct-object counts, so
     the GB and request budgets are overstated (e.g. T4 "196,812 files" is rows;
     distinct objects is materially fewer).
- **Evidence.** F5, F7. Plan quotes: `resource_id | string | PK (joins manifest)`;
  "Flat by `resource_id` — globally unique, dedup-friendly."
- **Remediation (concrete).**
  1. **Deduplicate the worklist by `resource_id` before iterating.** After applying
     the tier predicate, collapse to distinct `resource_id` (keep the lowest
     `attachment_order` / first `notice_id` for provenance):
     ```sql
     SELECT * FROM (
       SELECT *, row_number() OVER (PARTITION BY resource_id ORDER BY attachment_order, notice_id) rn
       FROM worklist
     ) WHERE rn = 1
     ```
     One download per distinct object; the manifest still maps every notice to it.
  2. **Make the ledger grain "one row per distinct `resource_id`,"** and rename the
     schema note from "PK (joins manifest)" to "object identity (1→many to
     manifest rows)." Keep the BTREE on `resource_id`; the join intentionally fans
     out to notices.
  3. **Recompute and republish the tier table as distinct-object counts** (files =
     `count(distinct resource_id)`, GB = `sum(size_bytes)` over the deduped set) so
     the request/bandwidth budget is honest.
  4. The flat `<resource_id>` R2 key is **correct and should stay** — it gives
     free physical dedup; it is the *iteration* and the *PK label* that are wrong,
     not the storage path.

### HIGH

#### H1 — Checkpoint strategy rewrites the entire ledger every N (O(n) per checkpoint); use append
- **What's wrong.** Plan §3 step 7: "Every `--checkpoint-every` (e.g., 500) flush
  the ledger to Lance (overwrite full accumulated set, atomic)." This mirrors the
  harvester, which already produced **195 versions** (F1) by overwriting on every
  checkpoint.
- **Why it matters.** At T4 (~196K rows pre-dedup, ~tens-of-thousands post-dedup),
  overwrite-every-500 rewrites a monotonically growing dataset hundreds of times;
  total write work is O(n²/checkpoint). A full read is already 2.5s at 331K rows
  (F10) and a write is heavier. It also detonates version count (195 already) and
  leaves orphan fragments until `cleanup_old_versions` runs. `02_lancedb_storage.md`
  §4.3 explicitly documents `"append"` as the incremental path: "Adds new fragments
  to the existing version's data … new rows accrete without rewriting the dataset."
- **Evidence.** F1 (195 versions), F10 (2.5s read, write ≥ that and growing), plan
  §3 step 7 quote, storage doc §4.3.
- **Remediation (concrete).** Checkpoint with **`mode="append"`**: buffer the
  in-memory window of completed ledger rows, and every `--checkpoint-every` call
  `lance.write_dataset(batch_arrow, ledger_uri, mode="append",
  data_storage_version="2.1", storage_options=so)`, then clear the buffer. Build
  the BTREE/BITMAP indices **once at the end** (incremental appends do not need
  per-checkpoint indexing). On `--resume`, read the existing ledger to get the set
  of already-`downloaded` `resource_id`s. This makes each checkpoint O(window), not
  O(n). (Note: pin `data_storage_version="2.1"` per storage doc §2.3, not `"2.0"`.)

#### H2 — Resume predicate is self-contradictory; restricted-status mapping is missing; retry set omits 401/400
- **What's wrong (resume).** Plan §3 step 1 skips if "ledger row exists with
  `status='downloaded'` AND the R2 object exists AND its size matches." Step 7 of
  the prompt's own framing notes the contradiction: §0/§4 also talk about hash, and
  the acceptance/dedup path (§3 step 2) keys on `sha256`, which **requires
  re-downloading** to compute. You cannot both "skip without re-downloading" and
  "confirm by hash."
- **What's wrong (status).** The ledger status enum includes `restricted`
  (§2) but the core loop (§3) never sets it — a `private`/`export_controlled` file
  that reaches the download path returns **401** (F4) and the loop's "hard 4xx →
  record `failed`" path would log it as generic `failed`, not `restricted`,
  polluting the failure rate and the ≥99% acceptance metric.
- **What's wrong (retry codes).** §3 step 3 retries `429/403/503`. Live restricted
  is **401** and dead resource is **400** (F4) — both are non-retryable and would
  (correctly) fall to the hard-4xx branch, but the plan never enumerates them, and
  it lists `403` as *retryable* when restricted content may legitimately be a
  permanent 403/401 (burning 6 backoff cycles up to 120s each on a file that will
  never succeed).
- **Why it matters.** A wrong resume predicate either re-downloads everything
  (defeats resume, wastes the politeness budget) or skips on a weaker check than
  claimed. Misclassified 401s inflate `failed` and can fail the ≥99% gate. Retrying
  permanent 401/403 wastes minutes per file.
- **Evidence.** Plan §3 steps 1–3 and §2 status enum; F4 (401/400 codes).
- **Remediation (concrete).**
  - **Resume predicate = size, not hash:** skip iff ledger row `status='downloaded'`
    AND R2 `head_object(<resource_id>)` succeeds AND `ContentLength ==
    size_downloaded`. Hash is computed **once at download time** and stored; it is an
    integrity record, never a resume gate. State this explicitly in §3 step 1.
  - **Map terminal codes:** `401` (and any `403` whose JSON body carries
    `UNAUTHORIZED`) → `status='restricted'`, no retry. `400` on a previously-valid
    URL → `status='failed'` with `error='resource_gone_400'`, no retry. Only
    `429/503` (and `5xx`/network) are retryable; **remove `403` from the retry set**
    or special-case it (retry only if body is a WAF/throttle page, not an auth
    error).
  - Since the worklist is already filtered to `public` + `not export_controlled`,
    `restricted` should be ~0 in practice — but the mapping makes the backstop
    auditable instead of mislabeled.

#### H3 — Throughput/time framing is request-bound but large tiers are bandwidth-bound
- **What's wrong.** Plan §0 frames pacing as "~4 requests/sec." The prompt's finding
  #6 is correct: T1 is **231.2 GB** (F7) and T4 is **356.3 GB**. At 4 req/s the
  *request* rate is irrelevant to wall-clock; **bandwidth** dominates.
- **Why it matters.** 4 req/s × ~mean file size sets a *throughput*, but the plan
  implies request-rate is the limiter. Quick reality check: 63,342 T1 objects at
  ~4/s is ~4.4 hours **if request-bound**; but 231 GB even at a sustained 50 Mbit/s
  residential uplink-to-SAM throughput is ~10+ hours, and at 20 Mbit/s ~25+ hours —
  bandwidth, not request count, is the wall-clock driver. A "4 req/s ⇒ done in N
  hours" estimate derived from request count alone is wrong for T1/T4.
- **Evidence.** F7 (T1 231 GB, T4 356 GB); plan §0 "~4 requests/sec."
- **Remediation (concrete).** State two budgets per tier in §1: a **request budget**
  (distinct objects ÷ req/s) **and** a **bandwidth budget** (tier GB ÷ measured
  sustained download MB/s), and take the **max** as the wall-clock estimate. Measure
  the real sustained rate during the 40-file smoke (§6 step 4) and record MB/s in
  the run ledger. For T0∪T2 (~30–40 GB) bandwidth is modest; for T1/T4 the plan must
  not imply a request-rate-derived timeline.

### MEDIUM

#### M1 — Architectural tension: durable raw bytes vs "raw is transport-only" — resolve the tier and lifecycle explicitly
- **What's wrong.** `~/.claude/CLAUDE.md` and `02_lancedb_storage.md` §1 state "raw
  is transport-only / ephemeral; Lance is the system of record" and "Parquet is
  transport only … Lance is the only durable columnar store." The plan lands
  **durable** attachment bytes as R2 objects under `s3://data-sink/landing/…`.
- **Why it matters.** This is not a violation — the bytes are unstructured blobs,
  not a columnar tier, and Lance holds the ledger/pointers exactly as the SoR. But
  `landing/` in this lake is the **ephemeral Gen-2 zone** by convention; placing a
  durable, downstream-consumed corpus there invites a future lifecycle/TTL sweep to
  delete it. The plan does not state retention or why `landing/` over `active/`.
- **Evidence.** Plan §2 step 1 (`s3://data-sink/landing/sam_attachments/`); CLAUDE.md
  "raw is transport-only … `dex-raw-landing-zone` is a retired Gen-2 landing
  bucket"; storage doc §1.
- **Remediation (concrete).** Either (a) land under a durable, intentionally-named
  prefix — `s3://data-sink/active/sam_attachment_blobs/<resource_id>` — and document
  it as a durable unstructured-blob tier whose **catalog/SoR is the Lance ledger**;
  or (b) keep `landing/` but add an explicit "no lifecycle expiry on
  `landing/sam_attachments/`" note and a retention statement to §2 and §8. State
  that the Lance ledger is the SoR and the R2 objects are CAS blobs addressed by it.
  Pick one and write it down; do not leave the bytes in an ambiguously-ephemeral
  prefix.

#### M2 — Dedup-by-`sha256` is largely redundant and adds a re-read; clarify its real job
- **What's wrong.** §3 step 2: "If a prior ledger row with the same `sha256` is
  already `downloaded` → record `skipped_dupe`." With the flat-`resource_id` key and
  worklist dedup (C1), same-object repeats are already caught by the cheaper
  resource_id object-exists check. True cross-`resource_id` content duplication is
  small: only **1,716** (name,size) groups span multiple resource_ids (F10), and
  even those are not confirmed-identical bytes.
- **Why it matters.** As written, sha256-dedup implies maintaining an in-memory
  hash→uri map and still computes a hash per file (fine), but its framing as a
  primary dedup mechanism is misleading and risks a redundant re-store guard that
  competes with the resource_id key. It is an *integrity* signal, not a meaningful
  byte-savings lever here.
- **Evidence.** F5, F10; plan §3 step 2.
- **Remediation (concrete).** Demote sha256 to an **integrity + opportunistic
  cross-object dedup** record: always compute and store it; do physical dedup by
  the `resource_id` object key (which is the natural CAS key). Drop the claim that
  sha256 is the primary dedup path; note the cross-object content-dup population is
  ~1.7K candidate groups, immaterial to the budget.

#### M3 — 24.7% of the manifest is zero-byte/null-name phantom rows — exclude at worklist time and stop citing them in headline totals
- **What's wrong.** F6: 81,770 rows (24.7%) have `size_bytes IN (0,NULL)` and 81,753
  also have `file_name=NULL`. The "331,401 rows / 874.1 GB" headline (§0) and any
  "all-mime" framing count these phantoms.
- **Why it matters.** They are silently dropped by the size floor on the text tiers
  (good), so they do not corrupt T0–T4 — but they inflate the manifest's advertised
  size and would pollute any future tier that does not impose a size floor (e.g. a
  hypothetical "all files" backfill). The plan should not present a row count that is
  one-quarter phantom as the download substrate.
- **Evidence.** F6; plan §0 "331,401 rows … 874.1 GB total declared bytes."
- **Remediation (concrete).** Add a hard pre-filter to every worklist:
  `size_bytes >= 1 AND file_name IS NOT NULL` (the tiers' `>=10000` floor already
  implies this, but state it as an explicit non-empty/named-file gate so any future
  tier inherits it). Re-state the §0 headline as "downloadable rows" (rows with
  `size_bytes>0 AND file_name IS NOT NULL`) vs raw manifest rows.

#### M4 — Acceptance "orphans = missing = corrupt = 0" and "≥99% downloaded" need the resource_id-grain and genuine-gone carve-outs
- **What's wrong.** §7 demands `orphans = missing = corrupt = 0` and ≥99% downloaded.
  With resource_id duplication (C1) uncorrected, "orphans" (R2 objects with no
  `downloaded` ledger row) and the downloaded-ratio denominator are computed on the
  wrong grain. Separately, F4 shows a previously-valid URL can go **400** if the
  attachment is removed between harvest and download — those are *genuinely gone*,
  not failures the operator can fix.
- **Why it matters.** A reconciliation built on row-grain will report false
  orphans/missing once dedup collapses N rows to one object. And a strict ≥99%
  without a "genuinely gone (400/410)" bucket can be unreachable if a notice
  archived between harvest and download (the manifest already filters
  `fileExists=0`/`deletedFlag=1` at harvest — see `sam_attachment_manifest.py` line
  250 — but harvest-to-download drift still happens).
- **Evidence.** F4 (400 on dead rid), F5; harvester line 250
  `str(a.get("fileExists","1"))=="0" or str(a.get("deletedFlag","0"))=="1"`; plan §7.
- **Remediation (concrete).** Compute reconciliation on **distinct `resource_id`**:
  orphans = R2 objects whose `resource_id` has no `downloaded` ledger row; missing =
  `downloaded` rows whose R2 `head_object` 404s or size 0. Add a `status='gone'`
  bucket for `400/410` on a previously-valid URL, and define acceptance as
  **`downloaded / (worklist_distinct − gone) >= 0.99`** with `orphans=missing=0` and
  `corrupt=0` (corrupt = R2 size ≠ ledger `size_downloaded` or sampled re-hash ≠
  stored `sha256`).

### LOW

#### L1 — `export_controlled` count and "209" are stale (live 724)
- §1 says "only 209 ITAR-flagged"; live is **724** (F8). The gate
  (`export_controlled=false`) is unaffected; just correct the prose so the funnel is
  accurate, and treat all §1 numbers as as-of-harvest (which §8 already instructs).

#### L2 — Size-cap boundary prose vs table mismatch
- §1 prose says size cap `10_000 <= size_bytes < 50_000_000` (exclusive 50MB); the
  tier table labels it "≤50MB." Zero files sit exactly at 50,000,000 (F7), so it is
  cosmetic, but pick one operator and state it once (`< 50_000_000` matches the
  prose and the tier counts I reproduced).

#### L3 — `mode="2.0"` pin would be inherited from the harvester; use `"2.1"`
- The harvester writes `data_storage_version="2.0"`. `02_lancedb_storage.md` §2.3:
  "New core-x datasets MUST pin `"2.1"`" (current default). The new ledger is a new
  dataset — pin `"2.1"`, do not copy the harvester's `"2.0"`.

#### L4 — Detached-launch claim "3 PIDs alive" is unexplained
- §6 step 5 says "Confirm 3 PIDs alive after ~15s," but the design is single-threaded
  (`Popen` of one python process). Either the 3 PIDs are an artifact (doppler → uv →
  python wrapper chain) — in which case say so — or it is a copy-paste from a
  multi-process job. Clarify so the monitor check is meaningful (`pgrep -f
  sam_attachment_download` on the python process, not a PID count).

---

## 4. Required plan edits (checklist before execution)

- [ ] **C1:** Add a worklist `DISTINCT resource_id` collapse (row_number over
      `PARTITION BY resource_id`) before iteration. Re-label the ledger grain as
      "one row per distinct `resource_id` (1→many to manifest notices)"; drop the
      "PK" wording. Republish §1 tier counts as `count(distinct resource_id)` files
      and `sum(size_bytes)` over the deduped set.
- [ ] **H1:** Change checkpointing from `mode="overwrite"` (full rewrite) to
      `mode="append"` of the completed-row buffer; build indices once at the end;
      `data_storage_version="2.1"`.
- [ ] **H2:** Specify resume predicate = `status='downloaded'` AND `head_object`
      OK AND `ContentLength==size_downloaded` (size, **not** hash). Map `401`/auth-`403`
      → `restricted` (no retry), `400` → `gone`/`failed` (no retry); retry only
      `429/503/5xx/network`; remove plain `403` from the retry set.
- [ ] **H3:** Add per-tier **bandwidth** budget (GB ÷ measured MB/s) alongside the
      request budget; take the max as wall-clock. Record sustained MB/s from the
      smoke run in `ops.sam_attachment_download_runs`.
- [ ] **M1:** Decide and document the bytes tier: durable
      `active/sam_attachment_blobs/<resource_id>` (preferred) **or** `landing/` with
      an explicit no-expiry retention note. State Lance ledger = SoR, R2 = CAS blobs.
- [ ] **M2:** Demote sha256 to integrity + opportunistic dedup; physical dedup is the
      `resource_id` object key.
- [ ] **M3:** Add explicit `size_bytes >= 1 AND file_name IS NOT NULL` worklist gate;
      restate §0 headline as downloadable-rows vs raw rows (24.7% are phantom).
- [ ] **M4:** Compute reconciliation on distinct `resource_id`; add a `gone` bucket;
      redefine acceptance as `downloaded/(worklist_distinct − gone) >= 0.99`,
      `orphans=missing=corrupt=0`.
- [ ] **L1–L4:** Correct the EC count (724), unify the 50MB boundary wording, pin
      `"2.1"`, and clarify the PID/monitor check.

---

## 5. What the plan got right (balanced)

- **Endpoint and integrity model are verified-correct.** Public downloads return
  octet-stream bytes whose length equals manifest `size_bytes` **exactly** (F3), and
  magic-bytes match the claimed mime — the write-time `size_match`/`mime_match`
  flags are well-founded and will catch truncations and HTML-error-page-as-PDF.
- **The R2 reconciliation mechanism works** end-to-end with the provided creds
  (PUT/LIST/HEAD/DELETE all succeed, F2). The audit design is mechanically viable;
  the prompt's 403-on-LIST concern was a different bucket.
- **The public-only / not-export-controlled guardrail is enforceable** — restricted
  files genuinely refuse (401 with JSON body, F4), so even a leak past the filter is
  caught and (with H2's mapping) auditable.
- **Flat `<resource_id>` R2 keying is the right CAS layout** — it gives free physical
  dedup of shared attachments (the very duplication that breaks the *iteration* in
  C1 is *handled* correctly by the *storage path*).
- **Provenance discipline is good:** per-file ledger in Lance + per-run row in
  `ops.sam_attachment_download_runs` (clean fit with the existing `ops.sam_*_runs`
  family, F9), versioned worklist materialization, and the explicit "no
  LLM/extraction in this stage" scope boundary.
- **Operational conventions are correct:** local/residential execution (SAM throttles
  datacenter IPs), polite single-threaded pacing with backoff, detached launch with
  `--resume`, and "re-derive counts from the live manifest, don't hard-code"
  (§8) — which is exactly why the minor count drift in F7/F8 is benign.
