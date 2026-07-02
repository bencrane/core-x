# naics_psc_labor_profile — Build Run Record

> ## ⚡ CLASSIFICATION RUNS ONLY AS IN-SESSION SUBAGENTS
> **Default model = Opus 4.8, effort = `xhigh`. ZERO Anthropic API spend.** There is no Batches-API
> path in the module. Materialize via `retrieve --agent-results <file>`. The end-to-end harness for
> running a NEW worklist lives at **`pipelines/reference/labor_profile_insession/RUNBOOK.md`** — a
> fresh agent starts there. Concurrency is capped at **4** (waves of 4 over slices of 15); the
> fail-closed gate is over the manifest's combo count for the worklist actually run.

**Version:** `labor_profile_v2` — richer NAICS/PSC grounding (full NAICS `description` + conditional PSC `full_description`/`includes`/`excludes`); supersedes v1 (title-only PSC, 300-char NAICS cap). The profile dataset now carries `naics_description` + `psc_full_description` columns.

**Datasets (Lance v2.1, R2 SoR):**
- `s3://data-sink/active/naics_psc_labor_profile/` — 8,690 rows (one per service NAICS×PSC combo)
- `s3://data-sink/active/naics_psc_labor_profile_categories/` — 26,705 rows (combo × ranked labor category)

**Grain:** one profile per distinct service `(naics_code, psc_code)` on `govcon_active_awards` (692 NAICS, 97,607 service award rows). Each profile carries the labor categories a winner must staff, selected by an LLM from two constrained vocabularies (830 detailed SOC occupations from OEWS national; 477 selectable SCA labor categories), grounded in the combo's real OEWS staffing pattern (top-40 candidates by `pct_total` at the resolved ladder level).

## Classification engine: all-Opus 4.8, in-session, zero API spend

The L2 classification was produced **100% by in-session Opus 4.8 / `xhigh` workflow subagents** (`model_id = claude-opus-4-8:in-session`, `prompt_version = labor_profile_v2`) — no Anthropic API billing. In-session subagents are the only supported classification lane; the module carries no API path.

**Why homogeneous all-Opus:** Opus at `xhigh` is well-calibrated — its `low` confidence lands on genuinely ambiguous cases (catch-all "other" PSCs; orthogonal NAICS×PSC pairings), and category depth scales with service complexity rather than padding. A single-model SoR also keeps the BITMAP-indexed `top_confidence` column on one calibration regime.

**Grounding (v2):** each call's prompt carries the **full** Census NAICS `description` (v1 truncated to 300 chars, dropping the Illustrative-Examples / Cross-References tail on ~73% of NAICS) plus, **conditionally**, the PSC `full_description` and any `includes`/`excludes` guardrails from `psc_reference` (v1 used the PSC title only). Rendered on 458 (includes) / 404 (excludes) / 345 (full-NAICS) calls, added only when the text exceeds the title (keeps prompts lean). Net effect: combo-level `top_confidence` high 3,551 → 3,674, categories 26,391 → 26,705.

## Pipeline (throttle-safe, checkpointed, fail-closed)

1. **manifest** — worklist + references + OEWS/EP candidates → `_naics_psc_labor_profile_manifest` (925 calls, 15 PSCs/call max).
2. **classify** — per-call prompts (shared `system.txt` vocab read once + slim per-call payload) run as sliced in-session subagents: **62 slices × 15 calls, waves of 4 concurrent, `opus`/`xhigh`**, loop-until-complete. Executed in 6 checkpointed batches. Concurrency capped at 4 after an initial 285-way fan-out tripped the shared session inference rate limiter (HTTP 429); C=4 held clean across all batches (0 limiter contact).
3. **checkpoint** — after each batch, all completed results bundled to `s3://data-sink/staging/nplp_opus_reclass_v2/` (durable, restore-on-crash; v1 checkpoints preserved at `nplp_opus_reclass/`).
4. **validate** — per-call completeness + SOC/SCA enum membership + required-field presence; 0 defects at 925/925.
5. **materialize** — union through the fail-closed 8,690-combo gate (all combos or no write) → both datasets + indexes + ops ledger.

## Verification (read back from R2)

- profiles 8,690 (distinct combos 8,690); categories 26,705
- provenance 100% `claude-opus-4-8:in-session`, `prompt_version = labor_profile_v2`
- reconciliation: 0 combos missing either direction; 718 non-play profiles ↔ 718 placeholder category rows (exact); all 7,972 play combos carry ≥1 real category
- on-disk enum validity: SOC out-of-vocab 0, SCA out-of-vocab 0
- new columns populated: `naics_description` 8,169/8,690 (avg 624 chars; v1 capped at 300), `psc_full_description` 6,727/8,690
- enrichment: `a_median` on all 25,987 real category rows; EP 2024→34 growth on 19,917
- indexes: profile BTREE(naics_code, psc_code) + BITMAP(is_labor_play, resolution_level, top_confidence); categories BTREE(naics_code, psc_code, soc_code, sca_code) + BITMAP(role_class, confidence, off_pattern, resolution_level)

## Rebuild / run a new worklist

Full end-to-end steps (manifest with external worklist → prep → sliced in-session workflow in waves
of 4 → validate/repair → checkpoint → assemble → retrieve → verify) are in
**`pipelines/reference/labor_profile_insession/RUNBOOK.md`**. Skeleton:

```
# 1. manifest (default = govcon_active_awards; set NPLP_WORKLIST_CSV for an external worklist)
doppler run -p core-x -c prd -- python3 pipelines/reference/materialize_naics_psc_labor_profile.py manifest
# 2-6. classify all calls in-session (Opus 4.8 / xhigh, slices of 15, waves of 4) -> per-call
#       result files -> validate -> checkpoint -> assemble into agent_results.json
# 7. materialize through the fail-closed combo gate:
... retrieve --agent-results <scratch>/agent_results.json
```
