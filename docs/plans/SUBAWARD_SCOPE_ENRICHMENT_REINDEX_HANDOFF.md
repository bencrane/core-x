# Subaward Scope-Enrichment — REINDEX HANDOFF (one open item)

**Date:** 2026-06-16 · **Status:** the lift is COMPLETE and delivered; ONE non-blocking housekeeping item remains (rebuild the `govcon_unknown_90day` vector index; confirm `govcon_scope_vectors_90day`'s). **No account/LLM tokens required** — this is a self-hosted, local Lance/MPS operation. Safe to do anytime.

> **Who this is for:** an agent picking up the subaward scope-enrichment lift after the operator's weekly account limit was hit mid-housekeeping. Everything that delivers the coverage lift is already banked in R2; you are NOT redoing the lift. You are finishing one degraded-but-correct index.

---

## TL;DR — the task
Phase G (embed) filled all the new vectors, but its `index` leg crashed building the **unknown** sink's IVF_PQ index. Fix the crash (1-line) and re-run the index for **unknown** (and verify **scope** got its index). That's it.

- The crash: `pyo3_runtime.PanicException: called Result::unwrap() on an Err value: RecvError(())` raised inside `ds.optimize.compact_files()` during `index --sink unknown`.
- Root cause: `index_sink`'s best-effort compact is wrapped in `except Exception` (`pipelines/sam_gov/sam_attachment_embed_90day.py:191`), but a **pyo3 Rust panic is a `BaseException`, not `Exception`**, so it escapes the guard and aborts the whole index command. (On the `scope` sink the same compact failed but as a normal `Exception` — "Repetition buffer too large" — and was caught/skipped, so scope proceeded to `create_index`.)

Until fixed: the new **unknown**-sink chunks are **embedded but not in the ANN index**, so vector search over them is brute-forced — **correct, just slower** (runbook risk R9). The capability-profile deliverable does NOT use vector search, so it is unaffected.

---

## What is already DONE (do not redo — all durable in R2)
- **Deliverable:** `govcon_subawardee_capability_profiles` rebuilt. `has_extracted_scope` **3,302 → 4,220 (+918 subs), 50.1% → 64.1%** of 6,586 bridge UEIs. CUI checks 0/0, `row_eq_universe`/`row_eq_distinct_uei` true, schema PASS, idempotency hash `ac5c523cefbbc4a7f1a056c457e99a61`.
- **Chunks appended** to the shared sinks (idempotent by `chunk_id`, re-run delta 0): scope `1,348,983→1,481,167`, unknown `1,042,059→1,310,223`, pricing `102,809→156,117` (total +453,656).
- **F marking** reconcile PASS (552 targets marked). **H1 regex** 3,107 docs / 9,282 requirement rows. **H2 LLM** 1,614 docs ground, ingest `run_pass_rate 0.984`, 3,269 requirement rows + doc_scope landed.
- **Embed vectors filled:** `embedding IS NULL == 0` for the UNMARKED set on BOTH scope and unknown (this is the embed-completion contract; it passed). Marked chunks (~326,866 scope / ~267k unknown) are **intentionally left NULL** (CUI bracket) and are **excluded from the ANN index by design** — do NOT embed them.
- **Prime extraction ledger** `sam_attachment_extraction_90day` is **verdict-intact** (v267): 0 route/expand/extract lift events; only 3,056 sanctioned `marking_fullbody` audit events. **Never write to this ledger.**
- **Phase J cleanup done:** all six `_sublift_*` throwaway datasets deleted (0 remaining under `active/_sublift_`).
- **Code PRs merged + pulled to `main`:** #478 `5ab7366` (id-filter+gate-bypass), #479 `a9a4435` (`--inner-uri`+reserved-word), #480 `29c502e` (append script), #481 `a62a758` (H2 CUI gate+scoping), #482 `0d3a171` (H2 scope-by-intersection).

---

## Environment / invocation
- Repo (operator checkout, on `main`): `/Users/benjamincrane/core-x`. cd there.
- R2 system of record: LanceDB under `s3://data-sink/active/`. Creds via Doppler.
- The embed module's deps (torch, sentence-transformers, the BAAI/bge-large-en-v1.5 model) are in the **main venv** `/Users/benjamincrane/core-x/.venv` (model cached at `~/.cache/huggingface`).
- Run embed with offline HF to avoid a Hub-stall: `EMBED_DEVICE=mps HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1`. On a GPU box use `EMBED_DEVICE=cuda` (the index build over ~1.3–1.5M 1024-d vectors is ~15–40 min/sink on MPS).
- Module: `pipelines/sam_gov/sam_attachment_embed_90day.py`. CLI is a **positional verb** (`status|embed|index|verify`) + `--sink scope|unknown|both` — NOT `--phase`.

---

## FIX — Option A (recommended: 1-line code fix, then re-run index)
**Edit `pipelines/sam_gov/sam_attachment_embed_90day.py:188-193`** so the best-effort compact also swallows a pyo3 Rust panic. Current:
```python
    try:
        ds.optimize.compact_files()
        ds = lance.dataset(uri, storage_options=so)
    except Exception as exc:  # noqa: BLE001
        log(f"[{name}] compact skipped (non-fatal Lance error): {str(exc)[:140]}")
        ds = lance.dataset(uri, storage_options=so)
```
Change the catch to also cover the panic (surgical — catch the pyo3 panic explicitly so KeyboardInterrupt is NOT swallowed):
```python
    try:
        from pyo3_runtime import PanicException
    except Exception:  # noqa: BLE001
        PanicException = ()  # type: ignore
    try:
        ds.optimize.compact_files()
        ds = lance.dataset(uri, storage_options=so)
    except (Exception, PanicException) as exc:  # compact is best-effort; a Lance/Rust panic must not abort indexing
        log(f"[{name}] compact skipped (non-fatal Lance error/panic): {str(exc)[:140]}")
        ds = lance.dataset(uri, storage_options=so)
```
(Minimal alternative if the import is awkward: `except BaseException as exc:` — broader, also catches the panic.)

Then ship it via the standard git lifecycle (branch off `origin/main` → PR → squash-merge → `git pull` in `/Users/benjamincrane/core-x` → `git log -1`), and re-run the index for both sinks:
```bash
cd /Users/benjamincrane/core-x
EMBED_DEVICE=mps HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
  doppler run --project core-x --config prd -- .venv/bin/python \
  pipelines/sam_gov/sam_attachment_embed_90day.py index --sink unknown
# confirm scope's index too (idempotent; create_index replace=True):
EMBED_DEVICE=mps HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
  doppler run --project core-x --config prd -- .venv/bin/python \
  pipelines/sam_gov/sam_attachment_embed_90day.py index --sink scope
```
`index_sink` first asserts `null_unmarked == 0` (already true), then compact (now skipped on panic), then `create_index IVF_PQ cosine, num_partitions≈round(sqrt(n)) (~1145 unknown / ~1217 scope), num_sub_vectors=64, replace=True`, then scalar BTREE/BITMAP. Long-running → run detached (`nohup … &`) if you won't babysit; it's resumable (`replace=True` is idempotent; re-running is safe).

## FIX — Option B (no code change: build the index directly, skipping compact)
If you'd rather not touch code, build the index from a one-off script (bypasses the panicking compact entirely). Run from the **repo root** (so module imports resolve), reusing the module's lease + storage helpers:
```bash
cd /Users/benjamincrane/core-x
EMBED_DEVICE=mps HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 LANCE_BYPASS_SPILLING=true \
doppler run --project core-x --config prd -- .venv/bin/python - <<'PY'
import math, lance
from pipelines.sam_gov.sam_attachment_embed_90day import _r2_storage_options, SINKS, IVF_SUB_VECTORS, SinkCommitLease
so=_r2_storage_options()
for name in ("unknown","scope"):                       # unknown first (the one that failed)
    uri=SINKS[name]; ds=lance.dataset(uri, storage_options=so)
    nun=ds.count_rows(filter="embedding IS NULL AND array_length(content_marking)=0")
    assert nun==0, f"{name}: {nun} unmarked NULL — do NOT index yet"
    n=ds.count_rows(); parts=max(1,round(math.sqrt(n)))
    with SinkCommitLease(uri, holder=f"embed-index:{name}", ttl_s=6*3600):
        ds.create_index("embedding", index_type="IVF_PQ", num_partitions=parts,
                        num_sub_vectors=IVF_SUB_VECTORS, metric="cosine", replace=True)
        cols=set(ds.schema.names)
        for c in ("resource_id","contract_award_unique_key"):
            if c in cols: ds.create_scalar_index(c, index_type="BTREE", replace=True)
        for c in ("naics_code","header_class"):
            if c in cols: ds.create_scalar_index(c, index_type="BITMAP", replace=True)
        if name=="unknown" and "lexicon_hit" in cols:
            ds.create_scalar_index("lexicon_hit", index_type="BITMAP", replace=True)
    print(f"{name}: indexed rows={n} partitions={parts}")
PY
```
(Option A is preferable: it leaves the module correct for the next run. Do Option B only as a stopgap.)

---

## DoD / verify (after either option)
```bash
EMBED_DEVICE=mps HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
  doppler run --project core-x --config prd -- .venv/bin/python \
  pipelines/sam_gov/sam_attachment_embed_90day.py verify
```
PASS criteria: for BOTH `scope` and `unknown` — `embedding IS NULL == 0` for the UNMARKED set, and a **vector (IVF_PQ) index present**. The `null_marked` count is expected NONZERO (CUI-bracketed, never embedded/indexed — correct). Optionally spot-check a vector search returns the new chunks.

---

## Guardrails (do not violate)
1. **Do NOT re-embed** — vectors are already filled (`null_unmarked==0`). Only the INDEX needs (re)building. `create_index(..., replace=True)` is idempotent.
2. **Do NOT embed/index the MARKED chunks** — they are deliberately NULL (CUI bracket) and excluded from the ANN index by design.
3. **Never write to the shared prime extraction ledger** `sam_attachment_extraction_90day` (it is verdict-intact; leave it).
4. **No account/LLM tokens needed** — embedding model is self-hosted (BGE on MPS/cuda). This is purely local compute + R2.
5. Indexing holds `SinkCommitLease(embed-index:<sink>)` — fine; just don't run two indexers on the same sink concurrently.
6. This does NOT affect the delivered capability profile (it reads requirements/doc_scope, not vectors). Lift coverage is locked at 64.1% regardless.

---

## Pointers
- Runbook: `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_EXECUTION_PLAN.md` (Phase G §9, risk R9).
- Embed module: `pipelines/sam_gov/sam_attachment_embed_90day.py` (`index_sink` ~L162-211; the compact guard to broaden is ~L188-193).
- Last embed run log (if still present): `/tmp/sublift_embed.log` (shows scope index built, unknown index panicked).
