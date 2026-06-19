# `*_90day` Dataset Rename — MIGRATION PLAN (decision-ready, supervised)

**Date:** 2026-06-16 · **Status:** ✅ **EXECUTED 2026-06-19 via [#542](https://github.com/bencrane/core-x/pull/542)** — full plane-wide cutover (18 R2 datasets + 3 `ops.*` ledgers), not just the 3 chunk sinks. R2 server-side copy → Lance integrity verify (rows/version/indices, IVF_PQ vector smoke) → one-PR code flip → gtm-mcp Render redeploy (deploy `dep-d8qpju3bc2fs73e6bm70`, commit `0b1efba`, status live) → Postgres `ALTER TABLE RENAME` → old-prefix delete after parity verify. Zero data loss. Authored while finishing the Subaward
Scope-Enrichment reindex, in response to the directive "rename it to remove `90day` from the name
since that is brittle and gets inaccurate as time passes."
**Companions:** `SUBAWARD_SCOPE_ENRICHMENT_RUN_RECORD.md` (the lift this came out of),
`SAM_90DAY_EXTRACTION_PIPELINE_SPEC_V2.md` (the pipeline this names).

---

## TL;DR — why this is a migration, not a rename, and why it was NOT done blind overnight

The brittleness call is correct: `90day` encodes a rolling-window claim into a permanent dataset
**name**, and the data no longer honors it (the subaward lift folded in older solicitations; the sinks
accumulate). But `90day` is not one dataset's wart — it is the **naming convention for the entire
SAM.gov / govcon data plane** (~25 datasets + module filenames + the prime extraction ledger), and
the load-bearing chunk sinks are read by a **deployed production gateway**. Renaming them is a
coordinated, prod-affecting, hard-to-reverse cutover. It must be done **supervised**, with a human to
verify production retrieval after the cutover. Doing it blind while the operator slept would risk
silently breaking the live `gtm-mcp` search service — so it is teed up here instead, one-session
executable.

**The disqualifying fact (verified 2026-06-16):** `apps/gtm_mcp` is a **deployed Render Web Service**
(`gtm-mcp`, MCP Streamable HTTP at `/mcp`) whose dataset access goes through a **runtime registry that
lists `s3://data-sink/active/` and self-refreshes a `name → uri` map on a ~30-min TTL**
(`GTM_REGISTRY_TTL_S=1800`, `apps/gtm_mcp/src/database.py`). The scope-search tool hardcodes
`DATASET = "govcon_scope_vectors_90day"` (`apps/gtm_mcp/src/tools/govcon.py:40`). Therefore, the
moment the R2 dataset is renamed, the registry's next refresh (≤30 min, **no deploy required**) stops
resolving the old name and `search_govcon_scopes` returns nothing — a **silent production outage** —
until the gateway is redeployed with the new `DATASET`. The rename and the redeploy must be
sequenced together by someone watching the service.

---

## Scope of the convention (verified blast radius)

`90day` appears on the system-of-record datasets, their module filenames, and the prime ledger.
Source-code references to the **three chunk sinks** (the unit this plan migrates first) are few and
enumerable; the bulk of the 400+ raw grep hits are sibling git worktrees and session transcripts, not
live source.

### The three chunk sinks (R2, `s3://data-sink/active/`) — primary migration unit

| Physical dataset (current) | Recommended new name | Size (2026-06-16) | Objects |
|---|---|---|---|
| `govcon_scope_vectors_90day` | `govcon_scope_vectors` | 7.70 GB | 1,589 |
| `govcon_unknown_90day` | `govcon_unknown` | 6.53 GB | 1,630 |
| `govcon_pricing_90day` | `govcon_pricing` | 0.50 GB | 1,237 |
| **Total** | | **~14.7 GB** | **~4,456** |

### Live source references to the sink URIs (exact change points)

| File | Line(s) | Kind | Notes |
|---|---|---|---|
| `pipelines/sam_gov/sam_attachment_extract_90day.py` | 73-75 | **Canonical** `SCOPE_URI`/`PRICING_URI`/`UNKNOWN_URI`, env-overridable `SAM90_{SCOPE,PRICING,UNKNOWN}_URI` | **Imported by** `sam_labor_demand_extract_90day.py` (91-92, 124, 2175-2177) and `govcon_p0_uniqueness_preflight.py` (28-41) — change here propagates to both. This is the **prime extract pipeline** (writes all three sinks): changing the default write target functionally touches the prime pipeline. |
| `pipelines/sam_gov/sam_attachment_embed_90day.py` | 44-47 | **Duplicate** defaults, env `SAM90_EMBED_{SCOPE,UNKNOWN}_URI` (scope+unknown) | Defines its own URIs rather than importing the canonical ones — consolidate or update in lockstep. |
| `pipelines/sam_gov/sam_attachment_embed_modal.py` | 21-22 | **Hardcoded** dict (scope+unknown), no env override | Modal embed orchestrator (on-demand, no cron). Add env override while here. |
| `pipelines/sam_gov/subaward_scope_append.py` | 52-54 | Hardcoded (all three) | One-shot lift artifact (already run). Update for hygiene; no live effect. |
| `apps/gtm_mcp/src/tools/govcon.py` | 40 | `DATASET = "govcon_scope_vectors_90day"` (scope only) | **Deployed gateway.** Requires redeploy (see TL;DR). gtm-mcp references **only scope**, not unknown/pricing. |
| docstrings/specs | `sam_labor_demand_extract_90day.py:8-9`, `govcon_p0_uniqueness_preflight.py:4`, `sam_attachment_embed_90day.py` header, `SAM_90DAY_EXTRACTION_PIPELINE_SPEC_V2.md` | comment-only | Update for accuracy; no behavior. |

### Out of scope for this first cut (deliberately)
- **The prime extraction ledger `sam_attachment_extraction_90day`** and the other `sam_*_90day` /
  `govcon_*_90day` datasets (downloads, blobs, files, requirements, doc_scope, labor_demand,
  teaming_edges, sub_targeting, capability_profiles, …). They share the convention and should follow
  in a second wave, but the ledger is a fenced prime-owned system — its rename is the prime team's
  call and must not be bundled with a sink cutover.
- **Module filenames** (`*_90day.py`). Renaming files is pure churn with import-path blast radius;
  do it last, separately, if at all.

---

## Decision the operator must make (the only open question)

**Is the semantic-accuracy gain worth a ~15 GB prod-affecting cutover + a `gtm-mcp` redeploy?**
- **Option 1 — Execute the rename** (this plan). Recommended **only** with a maintenance window and
  someone watching `gtm-mcp` search post-cutover.
- **Option 2 — Annotate, don't rename.** Treat `90day` as a known historical artifact and add a
  one-line note to `SAM_90DAY_EXTRACTION_PIPELINE_SPEC_V2.md` ("`90day` is the original rolling-window
  name; the corpus is now cumulative — the suffix is historical, not a freshness guarantee"). Zero
  risk, zero churn, removes the *misleading* without the migration. **Reasonable default** if the only
  cost of the current name is aesthetic.

The recommendation is Option 2 unless there is a concrete consumer being misled by the name, in which
case Option 1 on a window. An alias layer (logical name → physical URI in the gtm-mcp registry) is
explicitly **not** recommended — it contradicts the "no catalog layer" architecture and leaves the
physical name (the thing flagged as inaccurate) unchanged.

---

## Execution runbook (Option 1) — supervised, single session

Pre: pick the new names (table above). Decide whether to migrate all three sinks (recommended — keep
the family coherent) or stage them. Do the cutover when the prime extract / embed pipelines are idle.

1. **Copy each dataset to its new R2 prefix (server-side, no egress).** For each sink, copy every
   object under `active/<old>/` to `active/<new>/` (CopyObject, 1000-key pages), preserving the full
   Lance layout (`_versions/`, `data/`, `_indices/`, manifests). The freshly-rebuilt `unknown` IVF_PQ
   (see run-record §8) travels with the copy — no re-index needed.
2. **Verify the copy is byte-faithful.** For each new dataset: `lance.dataset(new).count_rows()`
   equals the old; `version` matches; `list_indices()` shows the same `embedding_idx` + scalar
   indices; `index_stats("embedding_idx").num_unindexed_rows == 0`; a spot vector search returns
   sane neighbors. **Do not proceed to step 3 until every new dataset verifies.**
3. **Flip all source references in ONE PR** (the change-points table) → squash-merge → `git pull` in
   the operator checkout. Update the canonical `*_URI` in the extract module, the embed module's
   duplicate defaults, the Modal dict, the append script, `gtm_mcp/src/tools/govcon.py:40`, and the
   docstrings. Keep the `SAM90_*` env-var names (or rename them too — operator's call).
4. **Redeploy `gtm-mcp` (Render)** so the live service ships the new `DATASET`. Sequence this to land
   **within the registry TTL window** (≤30 min after the R2 rename) to minimize/avoid the
   search-resolution gap — or accept a brief gap and redeploy immediately. After deploy, **verify
   `search_govcon_scopes` returns results** against the new dataset (this is the human-in-the-loop
   check that makes the cutover safe).
5. **Delete the old prefixes** only after steps 2-4 pass and the gateway is confirmed healthy on the
   new name. Scoped boto3 delete with a per-key `startswith("active/<old>/")` assertion (the lift's
   Phase-J procedure); never `rm`.

### Rollback
Old prefixes are retained until step 5, so rollback = revert the PR, redeploy `gtm-mcp` with the old
`DATASET`, and the registry re-resolves the old name on its next refresh. Reversible up to the delete.

### DoD
For each renamed sink: new name resolves in the gtm-mcp registry; `count_rows`/`version`/indices
match the pre-rename dataset; `search_govcon_scopes` (and any unknown/pricing consumer) returns
correct results on the new name; old prefix deleted; prime extract + embed + labor + preflight all
read/write the new name (no `_90day` sink URI remains in live source).

---

## Why blind overnight execution was the wrong call (record)
Hard-to-reverse (15 GB copy + deployed-service redeploy) + outward-facing (live `gtm-mcp` retrieval) +
no human to verify post-cutover search quality + changes the fenced prime pipeline's write target.
The cost of waiting is one misleading-but-harmless name persisting; the cost of a botched blind
cutover is a silently broken production search gateway discovered by users, not by an operator. The
asymmetry mandates a supervised migration. The safe, authorized open item (the `govcon_unknown_90day`
reindex) was completed; this — correctly scoped — was prepared, not gambled.
