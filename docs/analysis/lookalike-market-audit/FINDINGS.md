# LOOKALIKE-MARKET AUDIT — FINDINGS (2026-07-21)

Adversarial audit of `apps/catalyst_api/src/routers/sub_dossier_v1.py::sql_market()` — the
"lookalike buyer market" for a subawardee. Every number regenerates from a named script in
this directory against pinned artifact `query-sidecar/query_sidecar_20260721T020734Z.duckdb`.
Coordinator independently re-ran `02` and `05` on 2026-07-21 — reproduced byte-for-byte.

## Confidence (data/method trustworthiness only — no pricing/quota framing)

| Claim | /100 |
|---|---:|
| Default `market.total` = true post-trim count (default dials, `total_capped=False`) | **95** |
| A dialed-down `market.total` (floor <~$1M or `min_lane_hits`=1) = truth | **25** (caps; flagged) |
| A returned top-ranked "lookalike" genuinely **resembles** the seeds | **15** |
| Base *set* read as "primes active in the seeds' code space at ≥$10M" (a reach universe) | **45** |
| A Tier-2 count ("subs out to target-shape recipients") | **85** |
| Tier 2 read as "high-confidence *lookalike* buyers" | **15** (inherits the base) |

## FRONT 1 — is the count correct? YES at defaults, hazard off-default. (`01`,`02`,`03`)

- **Exact.** Independent raw-SQL reconstruction == engine `market.total` for **8/8** firms
  (`02_reconstruct_total.py`): Carahsoft 9112 raw → **7260**, SAIC 7073 → **5581**, etc. Only
  reducers are target-family / JV / corporate-family collapse — no hidden cap, no double-drop.
- **25k ceiling is inert at defaults.** Only **14,482** firms clear `prime_obl_60mo ≥ $10M`
  universe-wide (`10_universe_ceiling.sql`) < 25,000. Both historical caps genuinely closed
  for the default path.
- **Residual cap under VALID dials (`03`).** At `market_prime_floor=0, min_lane_hits=1` the true
  set is **101,477**; the engine returns `total=22449, total_capped=True` — flagged, but wrong.
  The "25k is enough" proof holds only at defaults, not across the ranges the validator accepts.
- **Monotonicity is not a theorem.** At the loosest dials `total(ON)=1392 ≤ total(OFF)=22449`
  HOLDS while OFF is capped/wrong. #1271 tripped monotonicity only because its cap (100) was
  tight enough to flip the inequality; at 25k the bug class survives with monotonicity green.

## FRONT 2 — is it a defensible lookalike? NO. (`05_lookalike_validity.py`)

- **Identical top-10 across opposite firms.** CARAHSOFT (software reseller; seeds = IT
  integrators, NAICS 541512) and GLENAIR (aerospace connectors; seeds = aerospace primes,
  336411/414) return the **same 10 "lookalikes"** — all NAICS **541330**, all `lane_hits=2`,
  all tied at one `wt`. Disjoint seeds, disjoint work, identical introducible list.
- **The `wt` is degenerate.** The displayed top-50 is a single `wt` tie-class; ordering is just
  the `subout_5y/uei` tiebreak. `wt = Σ seed_ct·share_lifetime/LN(1+n_with_lane)` rewards
  mono-line concentration in a common code; the `/LN` damping is too weak to offset it.
- **Threshold fragility.** `sig_rank`/`sig_share` move the count ±10% but leave the top-50
  unchanged (jaccard 1.00). `min_lane_hits` 2→3 drops the count −30% and replaces the **entire
  top-50 (0/50 kept)**; 2→4 = −48%, 0/50. The list is an artifact of the loosest knob setting.
- **wt vs principled cosine = 0/50 overlap.** Cosine over the seed-centroid vector (the module's
  own competitor-ladder primitive) shares **nothing** with the wt top-50.
- **Scale is inverted.** All **299** candidates with `prime_obl_60mo ≥ $1B` (the seeds' true peer
  class) sit at wt-rank **3,890–7,293 of 9,112** (RTX $44B → rank 7293). No billion-dollar prime
  can reach any top-50.

**Verdict:** the base *count* is a reproducible **reach universe** ("primes with ≥2 seed codes
at ≥$10M"); the base *ranking* is **not** a lookalike ranking and must not be surfaced as "your
most similar buyers." An honest ranking needs (a) cosine over the seed centroid instead of the
degenerate `wt`, and (b) the scale / agency / prime-vs-sub axes that code-signature overlap ignores.

## TIERS (`04_tiers.py`)

- Tier 2 ("subs out to target-shape recipients") **is** cleanly computable as a segment of Tier 0
  (a membership query — no `prime_sub` archetype needed) — additive, as ruled.
- **Nesting trap:** Tier 2 (recipient-code cube) is **not** a subset of Tier 1 from
  `gtm_prime_sub_pairs` — the two sub-out marts disagree. Nesting holds only when **Tier 1 and
  Tier 2 come from the same mart** (`T2 ⊆ T1_cube ⊆ T0` = True everywhere).
- Shape-match is defensible but mildly loose (matches the recipient's *prime* history to the
  target's *sub* lanes). Tier0−Tier1 (~75% with no cube sub-out) is a neutral segment, not a defect.

## GUARDRAILS — my three proposals, judged

1. **Monotonicity test — theater.** Not a theorem under truncation; passes on a capped-wrong number.
2. **"Proven bound" — right idea, wrong scope.** True at defaults (14,482<25k), false at loosest
   legal dials (101,477). Must be proven against the *dial range*, or the ceiling made dynamic, or
   the dial ranges tightened.
3. **"Never `len()` of a LIMIT'd fetch" docstring — necessary, insufficient, unenforceable.** The
   design still IS `len(dedup(LIMIT 25000))`; prose is not a test.

**What they miss:** the family/JV dedup runs in **Python**, so a pure-SQL `COUNT(*)` over the gates
over-counts by the dedup magnitude — "just COUNT" is not currently valid.

**The one structural fix that kills the bug class:** push family/JV dedup into SQL and compute the
total as `COUNT(DISTINCT family_key)` over the gates with **no `LIMIT` in the total's lineage**
(keep the 25k fetch only for the 50-row display). Then no ceiling can shape the total and
`total_capped` ceases to exist. Blocker: porting `normalize_name_key`'s regex into SQL. **Honest
interim:** a build-failing `assert fetched_rows < CEILING` on every fixture — keys on observed
fetch size, so it WOULD have caught this bug, unlike a `LIMIT 25000` string assertion.

## Reproducibility

- Artifact-pinned; raw-SQL scripts raise `ArtifactRolled` on a roll (re-pin, expect drift).
- `current_date`: the default gate-OFF count, Tier 1, Tier 2 have **no** `current_date` term →
  date-stable per artifact. Only `require_subout=ON` totals drift (`sql_market` recency line).
  Scripts `02`/`03` call the live engine (read current `/healthz`).
