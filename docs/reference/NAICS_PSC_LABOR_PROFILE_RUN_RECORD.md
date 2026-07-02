# naics_psc_labor_profile — Build Run Record

**Datasets (Lance v2.1, R2 SoR):**
- `s3://data-sink/active/naics_psc_labor_profile/` — 8,690 rows (one per service NAICS×PSC combo)
- `s3://data-sink/active/naics_psc_labor_profile_categories/` — 26,391 rows (combo × ranked labor category)

**Grain:** one profile per distinct service `(naics_code, psc_code)` on `govcon_active_awards` (692 NAICS, 97,607 service award rows). Each profile carries the labor categories a winner must staff, selected by an LLM from two constrained vocabularies (830 detailed SOC occupations from OEWS national; 477 selectable SCA labor categories), grounded in the combo's real OEWS staffing pattern (top-40 candidates by `pct_total` at the resolved ladder level).

## Classification engine: all-Opus 4.8, in-session, zero API spend

The L2 classification was produced **100% by in-session Opus 4.8 workflow subagents** (`model_id = claude-opus-4-8:in-session`, `prompt_version = labor_profile_v1`) — no Anthropic API billing. This supersedes an initial partial `claude-sonnet-4-6` Message Batches run (640 calls / 5,737 combos); that sonnet output is retained only as a cross-check reference, not materialized.

**Why homogeneous all-Opus:** a pilot showed Opus is better-calibrated than the sonnet baseline — its `low` confidence lands on genuinely ambiguous cases (catch-all "other" PSCs; orthogonal NAICS×PSC pairings), and category depth scales with service complexity rather than padding. A single-model SoR also keeps the BITMAP-indexed `top_confidence` column on one calibration regime.

## Pipeline (throttle-safe, checkpointed, fail-closed)

1. **manifest** — worklist + references + OEWS/EP candidates → `_naics_psc_labor_profile_manifest` (925 calls, 15 PSCs/call max).
2. **classify** — per-call prompts (shared `system.txt` vocab read once + slim per-call payload) run as sliced in-session subagents: **62 slices × 15 calls, waves of 4 concurrent, `opus`/`xhigh`**, loop-until-complete. Executed in 6 checkpointed batches. Concurrency capped at 4 after an initial 285-way fan-out tripped the shared session inference rate limiter (HTTP 429); C=4 held clean across all batches (0 limiter contact).
3. **checkpoint** — after each batch, all completed results bundled to `s3://data-sink/staging/nplp_opus_reclass/` (durable, restore-on-crash).
4. **validate** — per-call completeness + SOC/SCA enum membership + required-field presence; 0 defects at 925/925.
5. **materialize** — union through the fail-closed 8,690-combo gate (all combos or no write) → both datasets + indexes + ops ledger.

## Verification (read back from R2)

- profiles 8,690 (distinct combos 8,690); categories 26,391
- provenance 100% `claude-opus-4-8:in-session`
- reconciliation: 0 combos missing either direction; 675 non-play profiles ↔ 675 placeholder category rows (exact); all 8,015 play combos carry ≥1 real category
- on-disk enum validity: SOC out-of-vocab 0, SCA out-of-vocab 0
- enrichment: `a_median` on all 25,716 real category rows; EP 2024→34 growth on 19,700
- indexes: profile BTREE(naics_code, psc_code) + BITMAP(is_labor_play, resolution_level, top_confidence); categories BTREE(naics_code, psc_code, soc_code, sca_code) + BITMAP(role_class, confidence, off_pattern, resolution_level)

## Rebuild

```
doppler run -p core-x -c prd -- python3 pipelines/reference/materialize_naics_psc_labor_profile.py manifest
# classify all 925 calls in-session (sliced, waves of 4, opus/xhigh) -> per-call results
# retrieve unions in-session results through the fail-closed gate and materializes:
... retrieve --batch-ids "" --agent-results <results.json>
```
