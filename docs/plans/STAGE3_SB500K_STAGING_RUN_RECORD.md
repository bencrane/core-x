# Stage-3 SB>$500K task-file staging — RUN RECORD + backlog correction

**Executed:** 2026-06-21 (UTC). **Mode:** read-only sizing + deterministic pipeline phases (regex ledger no-op, bracket, census, select). No LLM spend; no crawl. **Outcome:** 134 task files staged + a material correction to the Stage-3 backlog sizing.

## What ran
| Step | Command (scoped to `/tmp/stage3_sb500k_stageready.ids`) | Result |
|---|---|---|
| Worklist + CUI check | `scripts/stage3_sb500k_stageready_ids.py` | 4,346 stage-ready ids; **0 unscanned chunks** (marking report covers them); 183 docs CUI-marked (excluded) |
| Regex extract | `--phase extract --resume` | **no-op** — all 4,346 already terminal in the regex ledger (regex ran before, found nothing) |
| Bracket (CUI Decision-A) | `--phase bracket` | idempotent (`rows_changed: 0`); marking gate PASS |
| Census | `--phase census` | **134 in-scope pending** (of 549 corpus-wide pending) |
| Select (stage) | `--phase select --staging-dir /tmp/g3_stage_sb500k` | **134 task files** written, 309,486 tokens, prompt_hash `f3567fc8…` |

## ⚠️ The backlog correction (the important finding)
The Stage-3 plan sized the LLM grind off **"files not in `govcon_award_requirements`" (~34K SB)**. That **overcounts the actual work**. Bracket reveals the true `llm_state` distribution corpus-wide:

| llm_state | count |
|---|---:|
| **pending** (needs LLM) | **549** |
| done (already processed) | 24,382 |
| excluded_marked (CUI) | 3,368 |
| excluded_out_of_scope | 7,606 |

**"Not in requirements" ≠ "needs LLM."** Most of those resources were already run through the LLM lane and produced an *empty* result (valid `done`, no rows). The genuinely-pending LLM worklist is **549 resources corpus-wide**; the SB>$500K cohort holds **134** of them. The LLM lane is **~94% complete**.

**Implication:** the multi-account / multi-session grind machinery is **overkill for the current pending queue** — 549 docs is a single-session job. The plan's wave/segmentation design remains correct *as a design*, but the population it was sized for does not exist as pending work.

## Where the real remaining leverage is (not "grind the pending queue")
1. **Download-pending tiers** — 8,584 SB>$500K not-downloaded + 3,213 newly-harvested (Stage-2 #600). These must be downloaded → chunked → regex'd before they become *new* pending LLM docs. This is the genuine backlog expansion; gated on crawl/download, not grind capacity.
2. **Deliberate v2-freeform re-extraction** — if the 24,382 `done` resources were processed under the v1 controlled-vocabulary lane, re-running them under **v2-freeform-labor** would recover IT/professional job titles the closed vocab could not express (the DA01/IT-labor gap). This is a real, large grind — but a strategic `reset-llm` + token-spend decision, not "staging the pending backlog."

## The 134 — ready to grind (single session suffices)
- Staging dir: `/tmp/g3_stage_sb500k/` (`tasks/<rid>.task.json` self-contained; `results/` empty).
- Prefix spread (first hex): c=1, d=38, e=51, f=44.
- Surety/bonding signal is NOT dependent on this grind — it lives in the **regex lane** (already run); this grind adds free-form **labor** recall only.

**Grind message (paste into a session):** see the response that accompanied this record / reuse the `bigthree_v2` NOTE pointed at `/tmp/g3_stage_sb500k`.

**Ingest (operator, after grind):**
```
GOVCON_LLM_FREEFORM_LABOR=1 doppler run -p core-x -c prd -- uv run --no-project --with pylance --with pyarrow --with duckdb --with 'psycopg[binary]' --with boto3 \
  python pipelines/sam_gov/sam_labor_demand_extract_90day.py --phase ingest \
    --staging-dir /tmp/g3_stage_sb500k --engine session-opus
# then: materialize_award_scope_requirements.py --cmd build   (rematerialize serving arrays)
```
prompt_hash at staging = `f3567fc823dadf6aa1cc5f7f8699467cd77f79cb6fd107751ca54ddfe7148866` (pass `--allow-prompt-hash <hash>` if the LLM artifacts change before ingest).

## Durable artifact
`scripts/stage3_sb500k_stageready_ids.py` — materializes the SB>$500K stage-ready allow-list AND verifies marking-pass (CUI) coverage. Reusable for the next cohort. (Task files themselves are ephemeral under `/tmp`.)
