# captive_sub_diversification_90day — dataset spec

**Built:** 2026-06-19 · **SoR:** `s3://data-sink/active/captive_sub_diversification_90day/` (Lance v2.1, snapshot-overwrite) · **CSV mirror:** `~/Desktop/captive_sub_diversification.csv`

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
1. **Award side is bridge-bound.** Matches resolve only against the **2,298 distinct awards** that have harvested + award-linked solicitation scope text — a slice of the trailing-90-day prime universe, not all of it. Sub side is full (all 3,156 captives scored).
2. **Window mixes dates.** `award_action_date` ranges across the feed (mostly 2026, some older); filter by it for true freshness. The subaward feed is ~days stale (no daily Trigger cadence yet).
3. **Construction-skewed** — subaward + solicitation density concentrates in building/heavy construction; IT/professional-services subs get fewer aligned matches (sparser scope text in 5415/5416).
4. **NAICS is a prime-award proxy** for the sub's trade, not the sub's registered code.
5. **Match precision** is centroid-cosine + NAICS gate (v1). Per-chunk (vs centroid) matching and a PSC gate would sharpen it; treat `match_score` as a rank, not a probability.

## Production worker (standing surface)
- **Worker:** `pipelines/serving/materialize_captive_diversification.py` — Modal app `captive-diversification`, function `run_build` (snapshot-overwrite + BTREE/BITMAP indices). Logic is a 1:1 port of the validated build.
- **Ledger:** `ops.captive_diversification_serving_runs` (self-bootstrapping; one terminal row per run with the full stats payload).
- **Schedule:** `src/trigger/captive_diversification.ts` — `schedules.task`, **Mon 11:00 UTC**, dispatches `run_build` through the universal-dispatcher and waits on a durable token. Cron + `CAPTIVE_WINDOW_DAYS` (default 365) are tunable.
- **Deploy:** `modal deploy pipelines/serving/materialize_captive_diversification.py` (registers the worker by name) + `npm run trigger:deploy` (registers the schedule). The dispatcher resolves `captive-diversification`/`run_build` by name — no dispatcher edit.
- **Manual run:** `modal run pipelines/serving/materialize_captive_diversification.py::main --cmd build` (or `verify` / `init_ops`).

> The earlier one-shot (`/tmp/build_div.py`) is superseded by the worker; the worker adds the rolling `award_action_date` window, the ops ledger, and the Trigger cadence.
