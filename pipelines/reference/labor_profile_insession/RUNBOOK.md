# naics_psc_labor_profile — In-Session Classification RUNBOOK

Durable, parameterized harness for running a batch of `(naics_code, psc_code)` combos through the
labor-classification pipeline **entirely in-session on Opus 4.8, with ZERO Anthropic API spend**.
This replaces the ephemeral `/tmp/nplp_*.py` scripts. A fresh agent in a new session runs from here.

Materializer module: `pipelines/reference/materialize_naics_psc_labor_profile.py`
(`prompt_version = labor_profile_v2`). There is **no Batches-API path** — `retrieve` only reads an
in-session `--agent-results` file.

---

## ⛔ HARD CONSTRAINTS (read before running anything)

1. **In-session Opus 4.8 / xhigh ONLY. Zero Anthropic API.** Classification runs exclusively as
   in-session subagents (`model: 'opus', effort: 'xhigh'`). Never call the Anthropic Message
   Batches API or any `anthropic` client. The dataset provenance is stamped
   `model_id = claude-opus-4-8:in-session`.
2. **Concurrency cap = 4.** A 285-way fan-out once tripped a shared-session **HTTP 429**
   ("Server is temporarily limiting requests"). The workflow slices the work into slices of 15
   and runs **waves of 4 concurrent agents**. Do NOT raise `NPLP_CONCURRENCY` above ~4.
3. **Fail-closed gate is over the NEW worklist's combo count** — whatever count the manifest built
   for *your* worklist CSV, not a hardcoded 8,690. `retrieve` refuses to write unless every
   manifest combo is present in the agent-results file.
4. **Import the exact module — no sys.path shadowing.** A stale copy of
   `materialize_naics_psc_labor_profile.py` on an old worktree's `sys.path` once silently shadowed
   the intended one. Every harness script imports via `_util.load_module()`, which loads the
   sibling file **by absolute path**. If you must point elsewhere, set
   `NPLP_MODULE_PATH=/abs/path/to/materialize_naics_psc_labor_profile.py`.
5. **Output normalization + validation.** Agents must emit the PSC key as `psc_code` (not `psc`);
   `assemble.py` normalizes `psc`→`psc_code` defensively. Every category object must carry all of
   `soc_code, off_pattern, sca_code, role_class, confidence`. **Retry (re-drive)** any call whose
   file is empty, contains `test`/placeholder/tag-only output, or is missing required fields —
   `validate.py` flags these; the fail-closed gate rejects a partial/invalid file.

### R2 reference-data prerequisites (must exist in `s3://data-sink/active/`)

All are load-bearing — the manifest and enrichment break without them:

| Dataset | Used by | Role |
|---|---|---|
| `bls_oews_2025` | `build_manifest`, `_vocabularies`, `retrieve` | detailed-SOC vocabulary + OEWS top-40 staffing-pattern candidates + `a_median` wage enrichment |
| `dol_sca_occupations` | `_vocabularies` | SCA labor-category vocabulary |
| `bls_ep_industry_occupation_matrix_2024_2034` | `build_manifest`, `retrieve` | EP 2024→34 growth enrichment |
| `naics_reference` | `build_manifest` | NAICS titles + descriptions grounding |
| `psc_reference` | `build_manifest` | PSC titles + full_description/includes/excludes grounding |
| `naics_psc_deliverable` | `build_manifest` | `prior_what_was_done` hint |
| `govcon_active_awards` | `build_manifest` | default worklist source **only** — bypassed when `NPLP_WORKLIST_CSV` is set |

---

## The external worklist

The operator hands you a **self-contained worklist** — a CSV (or JSONL you convert to CSV) with at
minimum these columns:

```
naics_code,psc_code
541511,D302
561210,S201
...
```

Optional extra columns `naics_description` and `psc_description` are used as award-desc fallbacks;
everything else (real NAICS/PSC titles, descriptions, OEWS candidates, vocabularies) is still pulled
from the R2 reference datasets. Point the manifest at it with `NPLP_WORKLIST_CSV`. All OEWS-candidate
and vocabulary machinery is unchanged; only the combo set changes.

---

## Environment

```
# Always wrap in Doppler for R2 + HQX creds:
doppler run -p core-x -c prd -- <command>

# Harness knobs (all optional, sane defaults):
export NPLP_WORKLIST_CSV=/abs/path/to/worklist.csv       # external worklist (else govcon_active_awards)
export NPLP_SCRATCH=/tmp/nplp                              # scratch root for system.txt/calls/results
export NPLP_SLICE_SIZE=15                                  # calls per slice
export NPLP_CONCURRENCY=4                                  # waves; DO NOT exceed ~4
export NPLP_CHECKPOINT_PREFIX=staging/nplp_insession       # R2 staging key prefix (no bucket)
# Optional: retarget the output datasets (defaults = the live SoR URIs):
export NPLP_MANIFEST_URI=s3://data-sink/active/_naics_psc_labor_profile_manifest/
export NPLP_PROFILE_URI=s3://data-sink/active/naics_psc_labor_profile/
export NPLP_CATEGORIES_URI=s3://data-sink/active/naics_psc_labor_profile_categories/
```

> To materialize into a SEPARATE dataset (e.g. a new-worklist batch that must not clobber the live
> SoR), set `NPLP_PROFILE_URI` / `NPLP_CATEGORIES_URI` / `NPLP_MANIFEST_URI` to fresh URIs before
> `manifest` and keep them set through `retrieve`.

Run scripts as modules from the repo root so the package-relative import works:
`python3 -m pipelines.reference.labor_profile_insession.<script>`.

---

## End-to-end sequence

### 1. Build the manifest over the external worklist

```
export NPLP_WORKLIST_CSV=/abs/path/to/worklist.csv
doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' --with 'pyarrow>=17' \
  --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
  python3 pipelines/reference/materialize_naics_psc_labor_profile.py manifest
```

Prints `worklist (external ...): N combos` and the resolution-level histogram, and freezes the
manifest dataset. **Note the combo/call counts — that N is your fail-closed target.**

### 2. Prep the in-session inputs

```
doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.prep
```

Renders `<scratch>/system.txt`, `<scratch>/calls/<cid>.txt`, and `cids/slices/npsc/expected` JSON.
Prints `calls / combos / slices / waves@C=4`.

### 3. Generate + run the sliced workflow in waves of 4

```
# Emit a workflow for all slices (or a sub-range <start> <end> to batch it):
python3 pipelines/reference/labor_profile_insession/gen_workflow.py            # all slices
python3 pipelines/reference/labor_profile_insession/gen_workflow.py 0 16       # slices 0..15 only
```

Run the emitted `<scratch>/workflow_<start>_<end>.mjs` in-session (the workflow runner injects the
`agent`/`parallel`/`log`/`phase` globals). Each agent reads `system.txt` once, then its slice's call
files, classifies every PSC, and Writes `<scratch>/results/<cid>.json`. It loops-until-complete
(3 re-drive rounds) within the batch. **Run in batches of a handful of slices and checkpoint between
them** so a crash resumes from R2.

### 4. Validate (repeat until clean)

```
doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.validate
```

Reports coverage toward N, plus any defects (missing/extra PSCs, missing required fields, out-of-vocab
SOC/SCA). **Re-drive** every flagged cid (regenerate a workflow limited to those slices, or delete
the bad `<scratch>/results/<cid>.json` and re-run its slice) and re-validate. Do not proceed while
`DEFECTS`, `absent`, `field-missing`, `SOC-bad`, or `SCA-bad` are non-zero.

### 5. Checkpoint to R2 (after each batch / before assembly)

```
doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.checkpoint_r2
```

Bundles all completed result files to `s3://data-sink/<NPLP_CHECKPOINT_PREFIX>/opus_results_latest.jsonl`
+ an immutable `snapshot_<N>calls.jsonl`. Restore = download the JSONL and re-explode each line
(keyed by its `custom_id`) back into `<scratch>/results/<cid>.json`.

### 6. Assemble the agent-results file

```
doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.assemble
```

Marshals `<scratch>/results/*.json` → `<scratch>/agent_results.json` (validates per-call
completeness, normalizes `psc`→`psc_code`). If it reports `incomplete`/`bad`, go back to step 3/4.

### 7. Materialize (fail-closed gate → both datasets + indexes + ledger)

```
doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' --with 'pyarrow>=17' \
  --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
  python3 pipelines/reference/materialize_naics_psc_labor_profile.py \
  retrieve --agent-results $NPLP_SCRATCH/agent_results.json
```

`retrieve` validates against the manifest + vocabularies, runs the **fail-closed N-combo gate**
(all combos present or NO write, exit 2), joins deterministic wage/growth enrichment, and writes
both Lance datasets + BTREE/BITMAP indexes + the `ops.naics_psc_labor_profile_runs` ledger.

### 8. Verify the materialized datasets

```
doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.verify_datasets
```

Checks counts, provenance (100% `claude-opus-4-8:in-session`), reconciliation (profiles ↔ categories
both directions; non-play == placeholder rows), on-disk enum validity, distributions, enrichment
coverage, and index presence.

---

## Re-drive recipe (when validate flags cids)

1. Identify the failing cids from `validate.py` output.
2. Delete their stale result files: `rm <scratch>/results/<cid>.json`.
3. Regenerate a narrow workflow. Either re-run `gen_workflow.py <start> <end>` for the slice range
   containing them, or the loop-until-complete in the existing workflow will re-drive them on the
   next run (up to 3 rounds). Keep concurrency at 4.
4. Re-run `validate.py`. Repeat until 0 defects / full coverage, then checkpoint → assemble → retrieve.
