# HANDOFF — Land the award-summary API FRESH Lance (`contract_prime_award`), first cycle

**You are a fresh agent. This document is your complete brief. Do not assume any prior conversation.**
Everything you need to land ONE dataset is below: exact commands, exact column facts, exact recovery
procedures. Read the three reference files in §PRECONDITIONS before you launch anything, then execute
the runbook in §5 verbatim.

---

## 1. Objective & strict scope

Land, verified, exactly ONE Lance dataset:

```
s3://data-sink/active/usaspending_api_fresh/contract_prime_award/
```

This is the **award-grain FRESH leg** — contract + IDV **award summaries** pulled from USAspending's
`download/awards` async job, landed verbatim (all-VARCHAR, exact API column names, no renaming).

**That is the WHOLE job this cycle. Stop when the fresh pull verifies (§8).**

**DO NOT build the award canonical spine** (`award_search` ⊕ `contract_prime_award`). That is a
separate, LATER cycle. Do not touch `award_search`. Do not write any canonical/OBT/spine dataset.
See §9 (out-of-scope) — it is a hard boundary, not a suggestion.

---

## 2. Current state (what is committed; what landed; where you start)

- **Branch:** `feat/award-api-fresh`, commit `027331a`, pushed to `origin`. Built on top of `main`.
- **The pipeline files exist ONLY on that branch, not on `main`.** Your first action (§5.0) is to check
  it out. If you are on `main` these files will not be on disk.
- **Files on the branch:**
  - `pipelines/usaspending/usaspending_api_award_fresh.py` — Modal app `usaspending-api-award-fresh`.
    Local entrypoints: `init_ops`, `backfill(days=40, chunk_days=7, force=False)`, `daily(days=7,
    chunk_days=7)`, `verify_table`. (These are `@app.local_entrypoint()`; the remote functions they
    call are `run_backfill` / `run_daily` / `verify` / `apply_ops_ddl`.)
  - `pipelines/usaspending/ops_usaspending_api_award_fresh_runs.sql` — ledger DDL for
    `ops.usaspending_api_award_fresh_runs`.
- **The pipeline is COMPLETE and correct. You RUN it. Do not rewrite it.** It clones the txn fresh feed
  (`usaspending_api_fresh.py`) with the award-leg changes (endpoint `download/awards`, member
  `Contracts_PrimeAwardSummaries`, 500k-cap chunking).
- **Nothing has landed. Clean start.** A prior attempt (see §6) ran `backfill` with the default 7-day
  chunks; chunk 1 hung in USAspending's `download/awards` queue and was killed. `backfill` writes the
  Lance table only AFTER all chunks fetch, so `contract_prime_award` was **never created**. The R2 path
  above does not exist yet. `run_backfill` refuses to overwrite an existing table (`force=False`
  default), so a clean create is expected on first success.
- **No open PR yet** despite what an earlier note may say — the branch is pushed but `gh pr list` shows
  none. §10 covers opening/merging it, but **the data landing is the deliverable, not the PR.**

---

## 3. Locked facts (all live-verified 2026-07-04 — trust these; do NOT re-derive)

| Fact | Value |
|---|---|
| **Endpoint** | `POST https://api.usaspending.gov/api/v2/download/awards/` |
| **NOT** | `bulk_download/awards` — that emits ONLY the `PrimeTransactions` member (txn grain, 297c). The award-summary member lives on `download/awards`. Verified live, endpoints doc §8.2–8.3. |
| **date_type** | `last_modified_date` — **ACCEPTED (HTTP 200 confirmed live).** The `download/awards` contract does not document it, but the API accepts it. |
| **prime_awards types** | `["A","B","C","D","IDV_A","IDV_B","IDV_B_A","IDV_B_B","IDV_B_C","IDV_C","IDV_D","IDV_E"]` |
| **sub_awards** | `[]` (empty — we ignore subawards here) |
| **file_format** | `csv` |
| **Response ZIP file_name** | `PrimeAwardSummariesAndSubawards_*.zip` (**live-verified**) |
| **ZIP members (expected layout)** | `Contracts_PrimeAwardSummaries`, `Assistance_PrimeAwardSummaries`, `Contracts_Subawards`, `Assistance_Subawards`. **Caveat:** the endpoints doc (§5, §8.3, §9) live-confirms the ZIP `file_name` and the two member *types* (`*_PrimeAwardSummaries_*` + `*_Subawards_*`) and their grain; the **Contracts/Assistance four-way split is contract-EXPECTED, not live-measured**. Zero execution impact — the payload requests `prime_awards` with `sub_awards:[]`, and `_fetch_chunk` selects by the unique substring `Contracts_PrimeAwardSummaries` (cannot false-match `Assistance_*`), so whether the ZIP ships 2 or 4 members the correct single member is extracted. |
| **Member we KEEP (only 1)** | `Contracts_PrimeAwardSummaries` — **AWARD grain** |
| **Members we IGNORE** | Any `Assistance_*` member (assistance = out of scope); any `*_Subawards` member (subawards already covered by `usaspending_api_subaward_fresh.py` → `contract_subaward`). Selection is by `Contracts_PrimeAwardSummaries` substring, so ignoring is automatic regardless of exact member count. |
| **PK / dedup key** | `contract_award_unique_key` |
| **Reconciliation key** | `last_modified_date` |
| **Expected column count** | **≈286 verbatim PAS columns** (see width caveat below) |
| **THE 500k CAP** | `download/awards` caps each job at `download_request.limit` = **500,000 rows**. `bulk_download/awards` (the txn feed) is UNCAPPED — that is the key difference. The window MUST be chunked to stay under the cap. |
| **Cap tripwire** | Pipeline aborts a chunk if the job's reported `total_rows ≥ 490,000` (`CAP_GUARD`), instructing you to shrink `chunk_days`. |
| **Window** | 40 days on `last_modified_date` (backfill default). Rationale: the BULK leg `award_search` snapshot frontier is ~2026-06-05 (max `last_modified`); today ≈ 2026-07-04 → ~29-day gap; 40 days covers it + ~11-day overlap. |
| **Land shape** | Verbatim, all-VARCHAR (`read_csv(all_varchar=true)`), exact API names, no renaming, no projection. |

**Payload the pipeline sends (built in `_fetch_chunk`, per chunk window):**
```json
{
  "filters": {
    "prime_and_sub_award_types": {
      "prime_awards": ["A","B","C","D","IDV_A","IDV_B","IDV_B_A","IDV_B_B","IDV_B_C","IDV_C","IDV_D","IDV_E"],
      "sub_awards": []
    },
    "date_type": "last_modified_date",
    "date_range": {"start_date": "<chunk_start_iso>", "end_date": "<chunk_end_iso>"}
  },
  "file_format": "csv"
}
```

**Column-width caveat (read this, do not treat 286 as a hard invariant):** endpoints doc §8.3 records
that the physical `Contracts_PrimeAwardSummaries` header was **never extracted live** — every prior
`download/awards` job hung in queue before the CSV could be read. `286` is the catalog/contract
expectation, not a live-measured count. **The first successful landing IS the live confirmation of the
width.** `verify_table` returns the real column count. Treat anything in ~[270, 300] as sane-and-expected
and record the actual number; do NOT fail the cycle solely because it is not exactly 286. The BTREE index
builder is presence-filtered (`_build_indices` skips any absent column), so a slightly different width does
not break indexing.

---

## 4. Preconditions checklist (verify BEFORE launching)

- [ ] **On the branch:** `git rev-parse --abbrev-ref HEAD` → `feat/award-api-fresh`. Files present on disk.
- [ ] **Modal authed** as `bencrane`: `modal token list` (or `modal profile current`) succeeds.
- [ ] **Modal named secrets exist:** `r2-credentials` (R2_ENDPOINT or R2_ACCOUNT_ID, R2_ACCESS_KEY_ID,
      R2_SECRET_ACCESS_KEY) and `hqx-postgres` (HQX_DB_URL_POOLED). Check: `modal secret list` shows both.
      The pipeline mounts these on every remote function; no local env needed for the pull itself.
- [ ] **Doppler available** (`doppler` on PATH) only if you want to run the ledger query locally against
      Postgres (§5.4). The pull does not need it — Modal carries `hqx-postgres`.
- [ ] **`init_ops` has run** (creates `ops.usaspending_api_award_fresh_runs`). Idempotent DDL — safe to
      re-run. Do this once before `backfill` (§5.1). Without the ledger, `_record_run` logs a WARN and
      skips the audit row but the pull still proceeds.
- [ ] **`modal` CLI** at `/opt/homebrew/bin/modal` (confirmed on this host).

---

## 5. Execution runbook — exact commands, in order

All commands run from the repo root `/Users/benjamincrane/core-x`.

### 5.0 — Check out the branch (files live only here)
```bash
cd /Users/benjamincrane/core-x
git fetch origin
git checkout feat/award-api-fresh
git pull --ff-only origin feat/award-api-fresh
git rev-parse --abbrev-ref HEAD          # must print: feat/award-api-fresh
ls -la pipelines/usaspending/usaspending_api_award_fresh.py \
       pipelines/usaspending/ops_usaspending_api_award_fresh_runs.sql
```

### 5.1 — Create the ops ledger (once, idempotent)
```bash
modal run pipelines/usaspending/usaspending_api_award_fresh.py::init_ops
# → prints {'applied': True}
```

### 5.2 — Launch the backfill DETACHED with 20-day chunks (the key mitigation)
Use `--chunk-days 20`, NOT the default 7. This cuts a 40-day window into **TWO** `download/awards`
jobs instead of six — far less exposure to the backlogged queue (see §6). Run OFF-PEAK (US night).

```bash
modal run --detach pipelines/usaspending/usaspending_api_award_fresh.py::backfill --chunk-days 20
```

- `--detach` is mandatory: the job runs server-side and survives your terminal. **Do NOT rely on the
  attached stream as the completion signal** — a dropped stream is not a failed job. Monitor out-of-band
  (§5.3–5.4).
- Defaults you are NOT overriding: `days=40`, `force=False`. Leave them.
- The remote function has `retries=5` (fresh-IP retries — helps a 429 throttle; does NOT rescue a global
  queue backlog) and a 200-minute container timeout; each chunk self-limits at a 150-minute poll ceiling.

Record the returned Modal app/run id from the launch output for monitoring.

### 5.3 — Monitor out-of-band: Modal
```bash
modal app list                                    # find app: usaspending-api-award-fresh, state running
modal app logs usaspending-api-award-fresh        # stream logs (safe to attach/detach freely)
```
In the logs, each chunk prints:
```
download/awards job [<start>…<end>]: PrimeAwardSummariesAndSubawards_*.zip
  poll N: status=running rows=<total_rows>
  ...
  poll M: status=finished rows=<total_rows>
  extracted Contracts_PrimeAwardSummaries member(s): [...]
```
A chunk is **healthy** while `rows` climbs or `status` flips to `finished`. A chunk is **queue-stuck**
if `status=running` with a **flat** `rows` for many minutes (see §6/§7).

### 5.4 — Monitor out-of-band: the ops ledger (Postgres)
The audit row is written only at a terminal state (success or error), so an empty result mid-run is
normal. Poll it to confirm the final outcome.

**IMPORTANT — Doppler injects `HQX_DB_URL_POOLED` into the CHILD process, not your login shell.** If you
write `doppler run … -- psql "$HQX_DB_URL_POOLED"`, your login shell expands `$HQX_DB_URL_POOLED` to
**empty** BEFORE `doppler run` executes (the var is not in your shell env — Doppler only provides it to
the child). `psql` then gets an empty conninfo, falls back to a default local socket, and fails/hangs.
**Defer the expansion into the Doppler-managed child with `sh -c`** so `$HQX_DB_URL_POOLED` resolves
inside the process that actually has it:

```bash
doppler run -p core-x -c prd -- sh -c 'psql "$HQX_DB_URL_POOLED" -c "SELECT id, run_mode, window_start, window_end, rows_written, columns, table_rows_after, api_calls, write_mode, indices_built, status, left(coalesce(error_message,'\''''\''),160) AS err, started_at, executed_at FROM ops.usaspending_api_award_fresh_runs ORDER BY id DESC LIMIT 5;"'
```

(`psql` is at `/opt/homebrew/opt/libpq/bin/psql`; `doppler` is on PATH; the repo's `doppler.yaml` already
scopes `core-x`/`prd`. All confirmed present on this host.)

**This local ledger read is OPTIONAL/secondary.** The authoritative, secret-free read-back is §5.5
`verify_table` (Modal-native — no local Postgres, no Doppler). If the `psql` path gives you any trouble,
skip it and trust §5.5 as the sole terminal read-back.

Terminal success looks like: `run_mode='backfill'`, `write_mode='overwrite'`, `status='success'`,
`rows_written > 0`, `columns ≈ 286`, `indices_built` non-empty.

### 5.5 — Verify the landed table (independent read-back)
```bash
modal run pipelines/usaspending/usaspending_api_award_fresh.py::verify_table
```
Returns JSON:
```json
{"uri":"s3://data-sink/active/usaspending_api_fresh/contract_prime_award/",
 "rows": <>0>, "columns": <≈286>,
 "indices": [ ...index entries... ],
 "min_last_modified": "<iso>", "max_last_modified": "<iso>"}
```
Confirm: `rows > 0`; `columns` in the sane band (~270–300, record the exact value); the **substring**
`contract_award_unique_key` **appears somewhere in the `indices` output**; `max_last_modified` is a recent
date (frontier extends past the ~2026-06-05 BULK frontier toward ~2026-07-04). **When this passes, the
cycle is DONE. STOP (§8, §9).**

**`indices` display caveat — do NOT do an exact-string match.** The award `verify()` extracts index names
via `getattr(i, "name", str(i))` over `ds.list_indices()`, whose elements are plain **dicts** (lance's
`Index` is a runtime `TypedDict` — no `.name` attribute). So each entry renders as a **stringified dict**
(e.g. `{'name': 'contract_award_unique_key_idx', 'columns': [...], ...}`), NOT a clean name string. The
BTREE index IS built; only the display format is ugly. **Verify by substring** — `contract_award_unique_key`
present anywhere in the `indices` blob — never by `indices == ["contract_award_unique_key", ...]`. (This is
a cosmetic regression vs. the subaward sibling, whose `verify()` uses
`i.get('name') if isinstance(i, dict) else getattr(i, 'name', str(i))`. Do NOT stop to fix it — you RUN
this pipeline, you don't rewrite it. Index presence is what matters, and the substring check proves it.)

---

## 6. The `download/awards` queue reality + the mitigation

**The queue is the pace-setter — not our code, not the data volume.** `download/awards` runs each request
as an async job on USAspending's custom-download queue. Off-peak that queue can still be **backlogged**:
submissions succeed (HTTP 200, correct `file_name`, rows begin generating) and then the job plateaus at
`status=running` with a flat row count for 45+ minutes. This is an **EXTERNAL** problem, not a code bug —
the payload is accepted and rows generate before the stall.

**Mitigation — fewer, larger chunks:**
- `--chunk-days 20` → **2 jobs** for the 40-day window (vs 6 jobs at the default 7). Fewer submissions =
  less queue exposure.
- Row-density data point: ~70k rows per 7-day window (all members) → 20-day ≈ 150–300k, 40-day ≈ ~400k.
  Both are under the 490k `CAP_GUARD`. **20-day is the sweet spot** — safe on the cap, minimal submissions.
- **Run off-peak** (US night). Timing matters more than anything in our control.

**When to shrink chunks:** if a 20-day chunk **trips the cap guard** (`total_rows ≥ 490,000` → the
pipeline raises and tells you to reduce `chunk_days`), re-run with `--chunk-days 14` or `--chunk-days 10`.
Smaller chunks = more jobs = more queue exposure, so only shrink as far as the cap forces you.

**When to defer:** if chunks persistently hang even off-peak, the queue is globally backlogged; there is
no code fix. Defer to a quieter window and re-launch. `retries=5` gives fresh-IP retries (helps a 429
throttle) but does NOT rescue a global backlog.

**There is no faster award-summary bulk source.** `download/search` with `spending_level:["awards"]` is the
same download-job family / same queue. `spending_by_award` is synchronous but caps at ~10k rows → unusable
for bulk. `download/awards` chunked is the only viable path; the only levers are fewer/larger chunks,
off-peak timing, and patience.

---

## 7. Failure modes & recovery

| Signal | Diagnosis | Action |
|---|---|---|
| Chunk logs `status=running` with a **flat `rows`** for many minutes (e.g. plateau at 70k for 45+ min). | **Queue-stuck** (external backlog), not a bug. | Wait to the 150-min per-chunk poll ceiling — the pipeline self-limits and Modal resubmits (`retries=5`). If it re-hangs persistently, **kill and re-launch off-peak with a smaller `--chunk-days`** (14 → 10), or **defer**. `modal app stop usaspending-api-award-fresh` to kill. |
| Pipeline raises `chunk […] returned total_rows=… ≥ 490,000 (cap 500,000) — window too wide`. | **500k cap tripwire** — the chunk would truncate. | Re-launch backfill with a smaller chunk: `modal run --detach …::backfill --chunk-days 14` (then `10` if still tripping). Nothing landed (raise happens before write). |
| Submit returns **HTTP 429** (`_ThrottledError`). | Rate throttle. | The remote function raises so `modal.Retries` recycles the container with a **fresh IP** and retries (up to 5, exponential backoff). No action unless all 5 exhaust → defer and re-launch off-peak. |
| A chunk finishes with `total_rows: 0` / a window lands 0 rows. | For `backfill`, `run_backfill` treats **0 rows as a HARD FAILURE** (raises; nothing written — the table is not created on an empty pull). | Confirm the window genuinely spans modified awards (40 days on `last_modified_date` should never be empty). If truly 0, widen `--days` (e.g. `--backfill days` via `::backfill --chunk-days 20` with a larger `days` arg — pass `days` positionally: `::backfill --days 60 --chunk-days 20`) and re-launch. |
| Job status flips to `failed`. | Upstream job failure (rare; seen once in prior probing). | `modal.Retries` re-runs the whole function; if it persists, defer and re-launch off-peak. |
| Backfill dies mid-run / you kill it. | **Partial state is safe.** `backfill` fetches ALL chunks into a shared workdir and writes the Lance table **only once, after all chunks succeed.** A death before that write leaves **NO** `contract_prime_award` table — clean state. | Simply re-launch `::backfill --chunk-days 20`. Because the table doesn't exist, `force=False` still permits the create. |
| `verify_table` errors "dataset does not exist". | Backfill never reached the write. | Check the ledger (§5.4) for an `error` row and Modal logs for the failing chunk. Fix per the rows above, re-launch backfill. |
| Ledger row shows `status='error'` with an `error_message`. | Terminal failure captured. | Read `error_message` — it names the exact cause (cap trip / ceiling / 0-rows). Act per the matching row above. |

**Never** create the table with `force=True` to "get past" a failure — `force=True` overwrites an
existing table and is ONLY for a deliberate from-scratch recreate. On a clean first cycle you never need it.

---

## 8. Definition of done (exact, verifiable)

All of the following true:
1. `contract_prime_award` **exists** at `s3://data-sink/active/usaspending_api_fresh/contract_prime_award/`.
2. `verify_table` returns `rows > 0` and `columns ≈ 286` (record the exact live-measured count — this is
   the first physical confirmation of the PAS width; anything ~270–300 is sane).
3. `verify_table` `indices` output **contains the substring** `contract_award_unique_key` (the BTREE index
   is built; entries render as stringified dicts — check for the substring, NOT exact-string equality; see
   the §5.5 display caveat).
4. A fresh `ops.usaspending_api_award_fresh_runs` row: `run_mode='backfill'`, `write_mode='overwrite'`,
   `status='success'`, `rows_written > 0`, `indices_built` non-empty.
5. `max_last_modified` frontier is recent (extends past ~2026-06-05 toward ~2026-07-04) — sane, not stale.

**Confirm with the two read-backs in §5.4 (ledger) and §5.5 (verify_table). Then STOP.**

---

## 9. Explicit OUT-OF-SCOPE (hard boundaries)

- **DO NOT build the award canonical spine** (`usaspending_award_canonical` = `award_search` ⊕
  `contract_prime_award`). That is a later, separate cycle.
- **DO NOT touch, read-modify, or overwrite** `s3://data-sink/active/usaspending/award_search/`
  (the 78.64M-row / 154-col BULK leg). It is prod and belongs to the future spine cycle only.
- **DO NOT** write any OBT / canonical / crosswalk / bridge dataset.
- **DO NOT** run `daily` this cycle — `daily` is the ongoing append top-up, not part of the first landing.
- **DO NOT** re-derive the locked facts in §3 (endpoint, payload, member, cap) — they are live-verified.
- **STOP after `verify_table` passes (§8).** Landing `contract_prime_award` is the entire deliverable.

---

## 10. Ship (after the data lands and verifies)

The DATA landing is the deliverable. The PR is a visibility/CI artifact. Once §8 is satisfied:

```bash
# Only if you had to fix pipeline code (you should NOT have — you RUN it, don't rewrite it):
cd /Users/benjamincrane/core-x
git add -A
git commit -m "fix(usaspending): <what> on award-summary fresh pipeline

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
git push origin feat/award-api-fresh

# Open + merge the PR (none is open yet despite the branch being pushed):
gh pr create --base main --head feat/award-api-fresh \
  --title "feat(usaspending): award-summary API fresh pipeline (download/awards → contract_prime_award)" \
  --body "Lands contract_prime_award (Contracts_PrimeAwardSummaries, award grain, verbatim all-VARCHAR)
from download/awards on a last_modified_date window, chunked under the 500k cap. First cycle: API pull
only; the award canonical spine is a later cycle.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"

gh pr merge <num> --squash --delete-branch

# Pull into the operator's main checkout so disk truth matches the merge:
git checkout main
git fetch origin && git pull --ff-only origin main
git log -1 --oneline        # verify the squash commit is present on main
```

If no code changes were needed (the expected case), still open + merge the PR to land the committed
pipeline, then pull `main`. **"Merged" ≠ "done" — done = the operator's `main` checkout reflects the
commit on disk.**

---

## Appendix — reference files to read before launching

- `pipelines/usaspending/usaspending_api_award_fresh.py` — the pipeline you run (Modal app
  `usaspending-api-award-fresh`; entrypoints `init_ops` / `backfill` / `daily` / `verify_table`).
- `pipelines/usaspending/usaspending_api_fresh.py` — the txn-feed template it clones (submit/poll/unzip
  → verbatim write → Lance machinery). On `main`.
- `docs/reference/USASPENDING_AWARDS_API_ENDPOINTS_AND_GRAIN.md` — endpoint/grain contract; read §5
  (`download/awards`), §8.2–8.3 (live member/naming proof + the width-PENDING note), §9 (fresh-leg
  recommendation + cap rationale).
- `pipelines/usaspending/ops_usaspending_api_award_fresh_runs.sql` — the ledger DDL `init_ops` applies.
- **BULK leg for the LATER spine cycle (do NOT touch this cycle):**
  `s3://data-sink/active/usaspending/award_search/` (78.64M rows, 154 cols, award grain).
