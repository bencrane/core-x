# SAM.gov / GovCon Substrate — Agent Operating Notes

Hard-won, concrete operating knowledge for building prime-award → SAM.gov substrate
pipelines in `core-x`. Written for a future agent picking up this class of work
(bridge diagnostics, attachment harvests, the `govcon_scope_vectors` build). Every
note is a rule + the failure signature that earned it + the exact fix. Pair with
`PRIME_AWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md` (the funnel) and
`PRIME_AWARD_SUBSTRATE_HARVEST_RUNBOOK.md` (the executed Phase-1 harvest).

> **Corrections folded in (2026-06-07)** from an adversarial first-principles audit —
> full evidence in `SAM_GOVCON_SUBSTRATE_AGENT_NOTES_ADVERSARIAL_REVIEW.md`. Two
> claims were materially wrong as first written and are corrected in place: §8
> `size_bytes` corruption (does NOT apply to this endpoint's data) and §5 base_type
> ranking magnitude (overstated ~30×). The recommendation directions held; the numbers did not.

---

## 1. Runtime & secrets (start here every time)

`lance` / `duckdb` / `pylance` are **NOT** in system Python. Every script runs through
Doppler (for R2/SAM creds) + `uv` (for ephemeral deps):

```bash
doppler run --project core-x --config prd -- \
  uv run --quiet --with pylance --with pyarrow --with 'duckdb>=1.5,<2' --with requests \
  python /path/to/script.py
```

R2 storage_options idiom (the only way Lance reads/writes the lake):

```python
def r2_storage_options():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}
```

Relevant Doppler secrets (names only): `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`,
`R2_ENDPOINT`, `SAM_API_KEY`, `HQX_DB_URL_POOLED`.

---

## 2. Long live sweeps MUST survive session resume (the biggest trap)

**Failure:** a multi-hour live sweep launched via the harness background runner is
**killed on session resume** (app restart / laptop sleep). A 3.5 h harvest died at
12 %. The harness watcher process dies too.

**Fix — three compounding requirements:**

1. **Daemonize the worker in Python** (double-fork + `os.setsid()`), so it leaves the
   harness process group and outlives a resume. `setsid` the *binary* does **not**
   exist on macOS — use `os.setsid()`.
2. **Daemonize BEFORE importing threaded libs.** Forking a multi-threaded process on
   macOS crashes:
   `objc[...]: +[NSNumber initialize] may have been in progress in another thread when
   fork() was called. ... Crashing instead.`
   So: `import os,sys` → daemonize → *then* `import duckdb, requests`. Launch with
   `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` as a backstop. Guard the stdin redirect
   (`os.dup2(devnull,0)` raises `OSError: Bad file descriptor` under the uv launcher).
3. **Make the sweep resumable.** Append one JSON line per unit-of-work to a
   line-buffered checkpoint; on restart, load the done-set and skip. This made ~5
   resume-kills completely harmless.

```python
def _daemonize(logpath):              # call before importing duckdb/requests
    if os.fork() > 0: os._exit(0)
    os.setsid()
    if os.fork() > 0: os._exit(0)
    f = open(logpath, "a", buffering=1)
    os.dup2(f.fileno(), 1); os.dup2(f.fileno(), 2)
    try: os.dup2(open(os.devnull).fileno(), 0)
    except OSError: pass
```

**Separate the durable daemon (work) from the harness watcher (notification).** The
watcher is convenience only; never let completion depend on it. The checkpoint file is
the source of truth — poll `wc -l` on it.

The committed `sam_opps_attachment_manifest_90day_winners.py harvest --daemon` ports
this `_daemonize()` (heavy libs are lazily imported inside the stage fns, so the fork is
single-threaded). Without `--daemon` the harvest is resumable but NOT detached — for a
multi-hour run either pass `--daemon` or launch it yourself under `nohup … & disown`.

---

## 3. SAM.gov access — use the frontend, not the developer gateway

**`api.sam.gov` (developer gateway) is useless for sweeps.** SI-NONFED keys cap at
~5–10 req/day; the wall is:
`429 {"code":"900804","message":"Message throttled out","description":"You have
exceeded your quota. You can access API after <date> 00:00 UTC"}` (resets UTC
midnight). It also *requires* `postedFrom`/`postedTo` and rejects ranges >1 yr:
`400 "Date range must be null year(s) apart"`. Only use it for tiny ground-truth
spot-checks.

**Use the unauthenticated public frontend (no api_key, no quota):**

| Purpose | Endpoint |
|---|---|
| Search (solnum→notice) | `GET https://sam.gov/api/prod/sgs/v1/search/?index=opp&q=<solnum>&size=100&is_active=false` |
| Resources (attachments) | `GET https://sam.gov/api/prod/opps/v3/opportunities/{notice_id}/resources` |
| Download (bytes) | `GET https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{resourceId}/download` |

- **Search response:** `_embedded.results[]`; **notice_id is the `_id` field**; type in
  `type.value`; `solicitationNumber` per record. `page.totalElements` for the count.
- **Resources response:** `_embedded.opportunityAttachmentList[].attachments[]` →
  `{resourceId, name, mimeType, size, accessLevel, attachmentOrder}`.
- **`hal+json` quirk:** a strict `Accept: application/json` returns **406**. Send
  `Accept: application/json, text/plain, */*`.
- **Headers:** browser UA + `Origin: https://sam.gov` + `Referer: https://sam.gov/opp/{nid}/view`.
- **`opps/v2/opportunities/search` (frontend) is auth-gated (401)** — only `sgs/v1/search` is open.
- **Pacing:** single-threaded, **residential IP** (datacenter egress is 429'd).
  Proven safe at `0.12 s` inter-call (~5.5 req/s observed, 0.09 % error over 49 K calls).
  Backoff on 403/429/503/5xx; treat 404 as terminal.
- **Co-tenancy:** other sessions' jobs may be hitting sam.gov from the same IP
  (`sam_attachment_download.py`, etc.). Check `ps aux | grep sam` before a big sweep;
  it raises shared 429 risk.

---

## 4. Translation is an OFFLINE join, never a live search

The solnum→notice_id step needs **zero** live calls. Inner-join FPDS
`solicitation_identifier` (`usaspending_api_fresh/contract_prime_txn`) against
`sam-gov-opps` **active ∪ archived** on a normalized solnum. This reproduced the live
frontend hit rate within 2/500.

**Normalize both sides** — absorbs FPDS↔SAM dash/space drift; skipping it undercounts:

```sql
regexp_replace(upper(trim(<col>)), '[^A-Z0-9]', '', 'g')
```

---

## 5. Rank notices on `base_type`, NOT `notice_type` (correct key; modest impact)

`sam-gov-opps` carries both. **`notice_type` flips to "Award Notice" once a
solicitation is awarded** (in the bridge join: `notice_type='Award Notice'` = 40,540
vs `base_type='Award Notice'` = 24,293 — verified). **`base_type` preserves the original
posting type = the document-host identity** — and the PWS/SOW attachments live on the
original solicitation notice, so rank on `base_type`. *Measured impact (re-derived over
the real bridge):* ranking on `notice_type` instead changes **~5,738** winner selections,
of which only **~268** actually drop from a document-host tier to a non-host tier (15 to
Award Notice) — ~76% of the changed picks still land on a genuine Combined Synopsis/
Solicitation. So `base_type` is the correct key, but the blast radius is modest, not
"decision-critical": a one-shot run on `notice_type` would still be ~99.5% correct. The
earlier "~8,400 mis-demoted / 11,957-vs-3,554" framing was wrong against the artifact.

Hierarchy (lower = higher-value host): Combined Synopsis/Solicitation > Solicitation >
Presolicitation > Special Notice > Modification > Justification > Award Notice >
Sources Sought > other. Multiplicity is real and the tail is long — **observed max
8,365 siblings** for one solnum (government-wide vehicles / IDV-BPA umbrellas, e.g.
`47QSMD20R0001`); "51" wildly understates it. Pick the **min-rank** notice per solnum
(size the `row_number()` dedup + DuckDB spill for thousand-sibling solnums). Choose
Award Notice / Sources Sought only when no higher tier exists (flag those rows).

---

## 6. DuckDB + Lance footguns (each one cost an iteration)

- **Don't materialize the archived opps set (~2.84 M rows) WIDE — project first.** A
  full/wide materialize + aggregate stalled an audit agent (>10 min, ~0 % CPU; Arrow
  streaming stall). The nuance the first draft missed: the committed `build_bridge`
  materializes the *entire* active∪archived set with **8 narrow columns** and does NOT
  stall — column projection is what makes it tolerable. Rule: project to the few needed
  columns before materializing (or push a solnum IN-list into the scan); never pull wide.
- **Scalar BTREE pushdown only fires via `lance.dataset(...).scanner(filter=…)`**, NOT
  via a DuckDB-registered relation + SQL `WHERE` (that reads near-full columns over the
  laptop→R2 WAN).
- **Register/table alias collision = silent empty table:**
  `con.register('m', reader); CREATE TABLE m AS SELECT * FROM m` yields **0 rows**.
  Use distinct names (`register('msrc', …)` → `CREATE TABLE m AS SELECT * FROM msrc`)
  then `unregister`. This bug *passes* null-checks trivially (0 nulls in 0 rows) — so
  **always assert rowcount > 0 / == expected**, never trust "all checks pass" on a set
  you didn't size.
- **Read only needed columns** via `scanner(columns=[...])` — WAN transfer dominates.

---

## 7. Lance write / pyarrow schema gotchas

- **`posted_date` is a DATE/timestamp, not a string.** Declaring `pa.string()` and
  feeding the value yields `ArrowTypeError: Expected bytes, got datetime.datetime`.
  Coerce with `str(x)` or match the arrow type.
- Build the table with an **explicit `pa.schema`** for type control; `list_(pa.string())`
  (e.g. `award_keys[]`) writes fine.
- New dataset: `mode="overwrite"` is safe (atomic manifest commit). Build BTREEs
  **after** the write: `ds.create_scalar_index(col, index_type="BTREE")`; re-open the
  dataset to `list_indices()`. Use `data_storage_version="2.1"`, small
  `max_rows_per_file` (250 K) for R2-safe multipart.

---

## 8. SAM data-quality caveats (bake into every consumer)

- **`size_bytes` from `/resources` was TRUE (uncorrupted) as of 2026-06 — do not
  inherit the old corruption claim.** `sam_attachment_manifest.py`'s docstring says this
  field is `((true-1) mod 10_000_000)+1`-corrupted for ≥10 MB files; that did **NOT**
  reproduce here. The landed manifest holds 4,830 rows >10 MB (max 249 MB) — impossible
  under a formula bounded to [1, 10 M] — and a live Range probe (`bytes=0-0`, no full
  download) returned `Content-Length` exactly equal to the declared size up to 249 MB.
  Still confirm `Content-Length` at fetch as defense-in-depth, but the "5 MB may be
  45 MB" mechanism is false for this endpoint's data.
- **~2 % of attachments are `access_level='private'`** (auth-gated) — filter to
  `'public'` before any byte fetch.
- **~16 % of attachment rows are null-mime AND zero-size — the SAME rows (15.8 %)**,
  placeholder/link resources that still carry a `resourceId`. One population, not two;
  usually skip for the text/vector substrate.

---

## 9. Verification — triangulate, don't trust one lens

1. **Deterministic** internal-consistency read-back (counts, nulls, index presence,
   URL well-formedness, ranking fidelity) — but it can't catch parse/landing bugs.
2. **Live-vs-landed drift** — re-probe a random sample, compare `resourceId` sets to
   the landed table (caught nothing here = parsing correct; 15/15).
3. **Independent adversarial re-derivation** from raw sources (separate agents, fresh
   probes, different seeds) — re-derive the join, trace award linkage, re-probe live.
   65 independent samples, 0 violations confirmed the build.

---

## 10. Git lifecycle from a worktree

`gh pr merge <n> --squash --delete-branch` prints
`fatal: 'main' is already used by worktree at /Users/benjamincrane/core-x` — but the
**merge still succeeds server-side**. Confirm with `gh pr view <n> --json state,mergeCommit`.
Then **pull into the operator's main checkout** (`/Users/benjamincrane/core-x`), not
just the worktree: `git -C /Users/benjamincrane/core-x pull --ff-only origin main` →
verify `git log -1 --oneline`. The worktree branch ref can't be deleted while the
worktree sits on it (clears on worktree removal); the remote branch is deleted on merge.

---

## 11. Baseline funnel numbers (the shape to expect on re-runs)

90-day API-fresh prime feed → SAM substrate, measured 2026-06:

| Stage | Rate |
|---|---|
| Award-grain Sol# fill | **17.4 %** (rest are DLA `SPE*` / FSS / sole-source — no notice, structural ceiling) |
| Solnum → notice_id resolution | **33–34 %** |
| Substrate yield on resolved solnums | **85–90 %** |
| **End-to-end award → downloadable substrate** | **~4.9 %** (full-pop: 0.1736 × 0.332 × 0.852; the 5.3 % seen earlier used optimistic 172-sample yields) |

The narrowness is a **filter, not a leak** — it isolates the competed-procurement,
SOW-bearing segment. ~82 % of awards are unreachable by design. Re-run the bridge on
each monthly USAspending dump / as the fresh feed rolls forward.
