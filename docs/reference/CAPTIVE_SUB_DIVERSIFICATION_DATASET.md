# captive_sub_diversification — dataset spec

> **⚠️ Renamed 2026-06-19 ([#542](https://github.com/bencrane/core-x/pull/542)):** the SAM/govcon `_90day` suffix was dropped plane-wide (window-as-data discipline — the acquisition window is a read-time predicate, not part of the stored entity's name). Every `*_90day` dataset/ledger referenced below now lives at its de-suffixed canonical name (e.g. `captive_sub_diversification_90day` → `captive_sub_diversification`, `govcon_scope_vectors_90day` → `govcon_scope_vectors`). R2 was server-side-copied, so all versions/indices/row-counts carry over unchanged; figures below predate the rename but remain valid under the new names.

**Built:** 2026-06-19 · **SoR:** `s3://data-sink/active/captive_sub_diversification_90day/` (Lance storage-format v2.1, snapshot-overwrite; live dataset-version probe = **v15**) · **CSV mirror:** `~/Desktop/captive_sub_diversification.csv`

> **All figures below are live-measured against the R2 SoR + hq-x Postgres ops ledger on 2026-06-19** (`lance.dataset(...).count_rows()/version`, DuckDB distinct counts, and `ops.captive_diversification_serving_runs` latest terminal row). The production rolling-window build is the operative live state.

## What it is
The full-universe, semantic version of the "captive sub → fresh award under a *different* prime" diversification play. For every single-prime **captive** subawardee, it lists the fresh prime awards (won by other primes) whose scope semantically matches what the sub actually does — the "you depend on one prime; here's where else you fit" target list. Replaces the deterministic `govcon_sub_targeting_90day` `capability_match` leg (which reached only 169 subs) with vector recall across all captive subs.

## Build method (`/tmp/build_div.py`)
1. **Captive segment** — over `usaspending_api_fresh/contract_subaward` (v12): `COUNT(DISTINCT prime)=1 ∧ COUNT(*)≥3 ∧ total ∈ [$500K, $50M]` → **3,156 subs** (100% have BGE vectors).
2. **Sub embedding** — mean-pooled, renormalized centroid of each sub's `govcon_sub_capability_vectors_90day` (v8) chunks (BGE-large 1024-d).
3. **Semantic match** — per-sub ANN (k=15, nprobes=30, refine_factor=20) over `govcon_scope_vectors_90day` (v286, 1.48M prime-solicitation scope chunks, IVF_PQ). Matched chunk → `contract_award_unique_key` (direct column, BTREE) → award. **No scope text is emitted** (CUI-safe); evidence is the sub's own public `subaward_description` + the FPDS award title.
4. **Award → prime** — resolved via `usaspending_api_fresh/contract_prime_txn` (v22), latest txn per award → `recipient_uei` (the new prime), NAICS, `action_date`, value, agency, set-aside, PoP end.
5. **Filters / rank** — drop `cand_prime = sole_prime`; best chunk per (sub, new-prime); cosine ceiling 0.55 distance. **Hybrid quality gate:** `naics2_aligned` / `naics4_aligned` flags (award NAICS sector/4-digit vs the sub's trade) — raw cosine alone produced domain-wrong matches (HR firm → highway construction); the NAICS gate is the usable filter.

## Coverage (measured)
| | rows | distinct captive subs | distinct new primes |
|---|---:|---:|---:|
| All semantic matches | 32,944 | 3,156 | 1,900 |
| **NAICS-sector aligned (`naics2_aligned`) — the usable list** | **9,002** | **2,412** | — |
| NAICS-4 aligned (high precision) | 3,280 | 1,403 | — |

avg match_score (cosine) 0.721. **Always filter `naics2_aligned = true`** (or `naics4_aligned`) — the unaligned rows are semantic noise.

> The table above is the all-dates **validation** build (historical). The standing worker writes a rolling **365-day** window, and that **production** build is the live operative dataset. **Measured 2026-06-19** (`captive_sub_diversification_90day` v15, DuckDB distinct counts + `ops.captive_diversification_serving_runs` latest row, `status=success`, `completed_at=2026-06-19 15:35 UTC`): **28,965 rows · 3,156 distinct captive subs scored · 1,541 distinct new primes · 1,862 distinct matchable awards** · avg cosine **0.7215**. Aligned tiers: **`naics2_aligned` = 8,118 rows / 2,340 subs / 993 distinct awards** (the usable list); **`naics4_aligned` = 2,973 rows / 1,341 subs** (high precision).

## Schema
`sub_uei, sub_name, sub_state, sub_total_dollars, sub_n_subawards, sub_naics, sole_prime_uei, sole_prime_name, cand_prime_uei, cand_prime_name, award_key, match_score, award_naics, award_naics_desc, award_agency, award_action_date, award_value, award_set_aside, award_pop_end, award_desc, naics2_aligned, naics4_aligned, sub_evidence, built_at`.
BTREE indices: `sub_uei`, `cand_prime_uei`, `award_key`.

## How to query (gtm-agent / execute_audience_query — auto-discovered on next registry refresh)
```sql
-- a captive sub's diversification bench, domain-aligned, freshest awards first
SELECT sub_name, sole_prime_name, cand_prime_name, award_naics_desc, award_agency,
       award_value, award_action_date, match_score, award_desc
FROM captive_sub_diversification_90day
WHERE naics4_aligned
  AND award_action_date >= '2026-03-21'        -- true "won in last 90 days"
ORDER BY sub_total_dollars DESC, match_score DESC
```

## Caveats (do not over-quote)
1. **Award side is bridge-bound.** The matchable-award **ceiling is 4,988 distinct awards** (measured 2026-06-19) — the only ones with harvested + award-linked solicitation scope text (`govcon_scope_vectors_90day` v286 = 1,481,167 chunks but just **4,988** distinct `contract_award_unique_key`), i.e. **0.40%** of the 1,247,391 distinct prime awards (`contract_prime_txn` v22 distinct `contract_award_unique_key`, measured 2026-06-19). Each run surfaces a subset of that ceiling (the all-dates validation run's ANN hit 2,298; the live production 365-day window resolved **1,862** distinct awards). Sub side is full (all 3,156 captives scored).
2. **Window mixes dates.** `award_action_date` ranges across the feed (mostly 2026, some older); filter by it for true freshness. The subaward feed is ~days stale (no daily Trigger cadence yet).
3. **Construction-skewed** — subaward + solicitation density concentrates in building/heavy construction; IT/professional-services subs get fewer aligned matches (sparser scope text in 5415/5416).
4. **NAICS is a prime-award proxy** for the sub's trade, not the sub's registered code.
5. **Match precision** is centroid-cosine + NAICS gate (v1). Per-chunk (vs centroid) matching and a PSC gate would sharpen it; treat `match_score` as a rank, not a probability.

## Production worker (standing surface)
- **Worker:** `pipelines/serving/materialize_captive_diversification.py` — Modal app `captive-diversification`, function `run_build` (snapshot-overwrite + BTREE/BITMAP indices). Logic is a 1:1 port of the validated build.
- **Ledger:** `ops.captive_diversification_serving_runs` (self-bootstrapping; one terminal row per run with the full stats payload).
- **Schedule:** `src/trigger/captive_diversification.ts` — `schedules.task`, **Mon 11:00 UTC**, dispatches `run_build` through the universal-dispatcher and waits on a durable token. Cron + `CAPTIVE_WINDOW_DAYS` (default 365) are tunable. **State as of 2026-06-19:** the schedule is **defined in `src/trigger/captive_diversification.ts` and merged to `main`**; it has **not** been registered with Trigger.dev via `npm run trigger:deploy` (deploy requires a `TRIGGER_ACCESS_TOKEN`). The Modal worker itself is deployed (app `captive-diversification`) and the dataset is rebuildable on demand now; the weekly cadence activates the moment `trigger:deploy` runs.
- **Deploy:** `modal deploy pipelines/serving/materialize_captive_diversification.py` (registers the worker by name; **done** — the Modal app is deployed and has executed, ledger row `status=success`, `completed_at=2026-06-19 15:35 UTC`) + `npm run trigger:deploy` (registers the schedule; **not yet run**). The dispatcher resolves `captive-diversification`/`run_build` by name — no dispatcher edit.
- **Manual run:** `modal run pipelines/serving/materialize_captive_diversification.py::main --cmd build` (or `verify` / `init_ops`).

> The earlier one-shot (`/tmp/build_div.py`) is superseded by the worker; the worker adds the rolling `award_action_date` window, the ops ledger, and the Trigger cadence.
