# govcon_sub_diversification — dataset spec

**Built:** 2026-06-19 · **SoR:** `s3://data-sink/active/govcon_sub_diversification/` (Lance storage-format v2.1, snapshot-overwrite; dataset-version probe **v8**)

> Supersedes `captive_sub_diversification` as the canonical diversification surface. The single-prime
> "captive" segment is **no longer a baked substrate** — it is a query-time predicate (a derived
> **view**) over this full-universe table. See §"Captive as a view" below. All figures are
> live-measured against the R2 SoR on 2026-06-19 (`run_build` + read-back verify).

## What it is
The neutral, **full-universe** sub→NEW-prime diversification substrate. One row per (subawardee,
candidate NEW prime): a fresh prime award won by a prime the sub does **not** already work under,
whose scope the sub is a domain-aligned capability match for. Generalizes the retired captive build
(which baked single-prime ∧ recurring ∧ mid-market into the substrate and reached only 2,340 usable
subs) to score **every** sub against the same matchable-award ceiling.

## Build method (`pipelines/serving/materialize_sub_diversification.py`)
1. **Subs (full universe)** — over `usaspending_api_fresh/contract_subaward`: every sub with
   ≥ `SUBDIV_MIN_SUBAWARDS` (default 1) subawards. Per sub: `incumbent_prime_ueis` =
   `list(DISTINCT prime_awardee_uei)`, `n_incumbent_primes`, `sub_total_dollars`, `sub_n_subawards`,
   `sub_naics` = `mode(prime_award_naics_code)`. **25,450 subs**, 25,449 with a capability vector.
2. **Sub embedding** — mean-pooled, renormalized centroid of each sub's
   `govcon_sub_capability_vectors` chunks (BGE-large 1024-d).
3. **Semantic match** — per-sub ANN (k=15, nprobes=30, refine_factor=20) over `govcon_scope_vectors`
   (1.48M prime-solicitation scope chunks, IVF_PQ). Matched chunk → `contract_award_unique_key` → award.
4. **Award → prime** — `usaspending_api_fresh/contract_prime_txn` (latest txn per award) →
   `recipient_uei` (new prime), NAICS, `action_date`, value, agency, set-aside, PoP end.
5. **Filters / rank** — drop any `cand_prime` already in the sub's `incumbent_prime_ueis`
   (`NOT COALESCE(list_contains(...), FALSE)` — NULL-safe); rolling `award_action_date` window
   (`SUBDIV_WINDOW_DAYS`, default 365); cosine-distance ceiling 0.55; **hybrid NAICS-sector gate**
   (`naics2_aligned` / `naics4_aligned`) — raw cosine alone yields domain-wrong matches; the aligned
   tier is the usable list. Best chunk per (sub, new-prime); top `SUBDIV_TOP_PRIMES` (25) per sub.

## Coverage (measured 2026-06-19, 365-day window)
| | rows | distinct subs | distinct new primes |
|---|---:|---:|---:|
| All semantic matches | **235,088** | **25,421** | **2,412** |
| **`naics2_aligned` — the usable list** | **64,242** | **17,983** | — |
| `naics4_aligned` (high precision) | 26,245 | — | — |

avg match_score (cosine) **0.7089** · matchable awards resolved **3,027** (within the 4,988-award
scope-harvest ceiling) · single-prime subs **18,829** · multi-prime subs **6,592** ·
**incumbent-exclusion violations: 0** (no candidate prime is ever an existing incumbent).
**Always filter `naics2_aligned = true`** (or `naics4_aligned`) — the unaligned rows are semantic noise.

## Captive as a view (NOT substrate)
The "diversify off your one prime" play is now a query predicate, fully flexible:
```sql
-- broad single-prime ("captive") view
SELECT * FROM govcon_sub_diversification WHERE n_incumbent_primes = 1;   -- 173,933 rows

-- original banded captive (single-prime, recurring, mid-market) — reproduces the retired dataset
SELECT * FROM govcon_sub_diversification
WHERE n_incumbent_primes = 1 AND sub_n_subawards >= 3
  AND sub_total_dollars BETWEEN 500000 AND 50000000;                     -- 28,923 rows
```
**Parity proof (2026-06-19):** the banded view = **3,156 distinct subs** (exact match to the retired
`captive_sub_diversification`'s 3,156 scored), **2,343 NAICS2-aligned subs** (retired: 2,340),
**28,923 rows** (retired: 28,965). The ±3/±42 drift is award-feed freshness between the two builds;
per-sub matching is independent, so the view is a faithful, strictly-richer replacement. The retired
materialized `captive_sub_diversification` dataset was dropped (no longer independently built).

## Schema
`sub_uei, sub_name, sub_state, sub_total_dollars, sub_n_subawards, sub_naics, n_incumbent_primes,
incumbent_prime_ueis (list<string>), incumbent_prime_names (list<string>), cand_prime_uei,
cand_prime_name, award_key, match_score, award_naics, award_naics_desc, award_agency,
award_action_date, award_value, award_set_aside, award_pop_end, award_desc, naics2_aligned,
naics4_aligned, sub_evidence, built_at`.
BTREE: `sub_uei, cand_prime_uei, award_key, award_action_date, n_incumbent_primes` (the last makes
the captive view a pushdown filter). BITMAP: `naics2_aligned, naics4_aligned`. The incumbent arrays
are payload (not indexed); exclusion is done at build time.

## How to query (gtm-agent / execute_audience_query — auto-discovered on next registry refresh)
```sql
-- a sub's diversification bench, domain-aligned, freshest awards first
SELECT sub_name, n_incumbent_primes, cand_prime_name, award_naics_desc, award_agency,
       award_value, award_action_date, match_score, award_desc
FROM govcon_sub_diversification
WHERE naics4_aligned AND award_action_date >= '2026-03-21'
ORDER BY sub_total_dollars DESC, match_score DESC;
```

## Caveats (do not over-quote)
1. **Award side is bridge-bound.** Matchable-award ceiling = **4,988** distinct awards (only those with
   harvested + award-linked solicitation scope text in `govcon_scope_vectors`), i.e. 0.40% of the
   1.25M distinct prime awards. The sub side is full (all 25,449 vectored subs scored); this run
   resolved 3,027 awards within the 365-day window.
2. **Window mixes dates.** `award_action_date` ranges across the feed; filter by it for true freshness.
   The subaward feed is ~days stale (no daily Trigger cadence yet).
3. **Construction-skewed** — subaward + solicitation density concentrates in building/heavy
   construction; IT/professional-services subs (5415/5416) get fewer aligned matches.
4. **NAICS is a prime-award proxy** for the sub's trade (`mode(prime_award_naics_code)`), not the
   sub's registered code. For multi-prime subs it is the plurality sector.
5. **Match precision** is centroid-cosine + NAICS gate (v1); treat `match_score` as a rank, not a
   probability.

## Production worker (standing surface)
- **Worker:** `pipelines/serving/materialize_sub_diversification.py` — Modal app `sub-diversification`,
  function `run_build` (snapshot-overwrite + BTREE/BITMAP indices). Generalizes the retired
  `captive-diversification` app.
- **Ledger:** `ops.sub_diversification_serving_runs` (self-bootstrapping; one terminal row per run).
- **Schedule:** `src/trigger/sub_diversification.ts` — `schedules.task`, Mon 11:00 UTC, dispatches
  `run_build` through the universal dispatcher. `SUBDIV_WINDOW_DAYS` (default 365) tunable. **State as
  of 2026-06-19:** defined + merged; **not** yet registered with Trigger.dev (`npm run trigger:deploy`
  requires `TRIGGER_ACCESS_TOKEN`). The Modal worker is deployed and rebuildable on demand now.
- **Deploy:** `modal deploy pipelines/serving/materialize_sub_diversification.py`.
- **Manual run:** `modal run pipelines/serving/materialize_sub_diversification.py::main --cmd build` (or `verify` / `init_ops`).
