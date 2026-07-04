# FPDS L2 Origination Signals — Per-Agency Calibration (Cycle 2)

**Status:** ship-blocking config finding. **Global thresholds are DISQUALIFIED.** Live-probed 2026-07-04 off `usaspending_fpds_prime_award_state` + `usaspending_fpds_mod_delta` (⋈ on `contract_award_unique_key`). Companion to [`USASPENDING_FPDS_L2_SATELLITES_AND_JOIN_GRAPH.md`](USASPENDING_FPDS_L2_SATELLITES_AND_JOIN_GRAPH.md).

## Why this exists
The mod-footprint recon that seeded the `action_type_klass` mapping was computed on **one agency** (Air Force, a DoD sub-tier). This calibration answers the question that was open: *do mod behaviors and dollar scales differ enough by agency that a single global alert threshold misfires?* **Yes — catastrophically.** The differences are real, large, and captured here so the origination engine is agency-aware before it ships alerts.

---

## 1. Headline verdict

**Per-agency calibration is MANDATORY. The mechanism is PER-AGENCY PERCENTILE NORMALIZATION — one model keyed on `(CGAC × parent/child-pool)` — NOT a matrix of hand-curated per-agency threshold tables.**

Every signal (dollar deltas, ceilings, klass rates) is scored by its **percentile rank within its own agency's distribution**, not against an absolute cutoff. "Agency" enters only as a `GROUP BY` on a precompute step — like z-scoring. The per-agency dollar figures below are **not authored knobs**; they are the P99 column the empirical CDF emits, refreshed on the `mod_delta` build cadence.

Why a global absolute threshold is provably broken (spreads cross any single line in **opposite directions**):

| axis | spread | consequence of a global cut |
|---|---|---|
| "massive mod" Δceiling P99 | **226×** (GSA $0.36M → DoD $81.3M) | a global "$5M = massive" line sits deep in DoD's body (floods) and 14× *above* GSA's entire P99 (0% recall — GSA scope changes go invisible) |
| "big award" ceiling P99 | **47×** (DoD $0.80M → HHS $37.5M) | flags every HHS/DoT/NASA award as routine while burying an anomalous $2M DoD order |
| novation rate | **32×** (USDA 0.48/1k → GSA 15.57/1k) | a global "novation is rare" prior silences GSA vehicle-holder transfers — the highest-value succession signal — exactly where they're most common |
| termination rate | **117×** (NASA 0.1% → GSA 11.7%) | a shared "~1% = distress" line floods GSA (routine schedule pruning) and never fires NASA (where 1 termination is a 10× event) |

Three **structural corrections** ship *with* the normalization (they are inputs to it, not extra tables):
1. **Split the IDV-parent pool from the order-child pool BEFORE computing percentiles.** Highest-leverage fix — pooling is what manufactures DoD's $81.3M artifact. One partition key, not per-agency.
2. **DELETE `consumed_pct` from the starvation definition.** Saturated at P50=P90=1.0 for every agency — zero discrimination. Global fix.
3. **One order-share router flag** (`pct_order ≥ 55%` → IDV-mode vs DEF-mode). Changes *which* signal you score; a percentile rank can't express that. One parameter.

The strong-sense claim (curated per-agency tables) is **rejected as over-engineering** — near-zero marginal recall/precision over the normalized model, with maintenance-drift risk. (Adversarially stress-tested; the skeptic's refinement is adopted.)

---

## 2. Klass MAPPING is portable — Klass BASE RATES are not

Two opposite statements; do not conflate them.

- **MAPPING is PORTABLE — keep it global, do not touch it.** `action_type_code → klass` (scope_change / option_exercise / funding_only / termination / novation via `action_type_code='J'` / identity_boundary / admin / nonstandard) is a FAR-uniform enumeration. Same code, same meaning, every agency.
- **BASE RATES are NON-PORTABLE — baseline per CGAC.** Novation 32×, termination 117×, funding_only 15× spreads (below). These are rates (already sample-size-normalized), survive normalization, and align with known missions (GSA Schedules-vehicle churn; NASA cost-type R&D incremental funding; DOJ money-movement buying; VA recurring medical options). **The Air Force seed (scope 29.7% / fund 21.2% / novation 1.45/1k) is not representative of GSA, NASA, or DOJ on any klass axis** — every rate prior calibrated to it is calibrated to the wrong population.

Net: portable *labeling*, agency-specific *expectation*. Fire on deviation from the agency's own baseline, never a shared cutoff.

---

## 3. Agency-aware calibration table (13 top-tier CGAC agencies)

Materialized P99 snapshot the CDF emits. `massive_mod Δceiling` = agency P99 `|Δpotential_ceiling|` on scope-change mods; `big_award ceiling` = agency P99 award `potential_ceiling`; `mode` = IDV (`pct_order ≥ 55%`) vs DEF. Novation/term/fund are the **expected base rates** (the "normal," not the alarm).

| CGAC | Agency | mode | massive_mod Δceiling P99 | big_award ceiling P99 | novation /1k | term % | fund % | notes |
|---|---|---|---|---|---|---|---|---|
| 097 | DoD | **IDV** | **$81.3M** ⚠ artifact-capped | $0.80M | 1.45 | 3.0 | 21.2 | rank on parent/def pool only — the $81.3M is pool-mixing |
| 047 | GSA | **IDV** | **$0.36M** (floor) | $1.53M | **15.57** | **11.7** | 3.3 | schedules host; novation+termination churn is the real signal, not $ deltas |
| 036 | VA | DEF | $1.35M | $0.97M | 1.30 | 0.9 | 20.2 | opt 21.5% routine — don't read options as capacity expansion |
| 015 | DOJ | IDV | $2.15M | $1.74M | 0.72 | 0.4 | **49.2** | money-movement regime; under-weight scope |
| 019 | State | DEF | $1.84M | $1.37M | 0.68 | 0.9 | 24.0 | scope 41.6% (highest) — definitive-recompete surface |
| 012 | USDA | DEF | $1.06M | $3.56M | **0.48** (floor) | 1.5 | 17.8 | a 3/1k novation spike must still fire here |
| 075 | HHS | IDV | $7.04M | **$37.48M** (highest) | 0.79 | 0.8 | 18.2 | genuine mission scale (median $9.5K) — real fat tail, not mixing |
| 014 | Interior | DEF | $1.11M | $1.39M | 1.08 | 1.3 | 15.6 | |
| 070 | DHS | IDV | $3.52M | $9.27M | 0.69 | 0.9 | 17.2 | ident_p1k 323 — do NOT threshold ident |
| 020 | Treasury | IDV | $4.28M | $9.77M | 0.67 | 1.1 | 24.1 | |
| 013 | Commerce | DEF | $3.36M | $4.77M | 2.15 | 0.6 | 17.3 | ident_p1k 334 (highest) — measurement noise, demote |
| 069 | DoT | IDV | $3.62M | **$12.07M** | 0.65 | 0.4 | 36.1 | **highest expiry pressure** (pct_starving 0.37%), high median $16.4K |
| 080 | NASA | DEF | **$23.28M** (mission-real) | $7.91M | 0.88 | **0.1** (floor) | **43.1** | 1 termination = 10× event; funding-cadence IS the starvation tell |

**"Massive mod" alert:** fire on `|Δpotential_ceiling| ≥ agency P99` **within the correct parent/child pool**, gated by an absolute-floor backstop `min_Δ = max($100K, 0.25 × agency_ceil50)` so GSA's $0.36M P99 doesn't trip on noise. DoD's $81.3M is read against volume — P99 over ~15M mixed mods (a few giant weapons-IDV adjustments over an ocean of tiny task orders); the pool split dissolves it. Tell that it's an artifact: DoD has the **lowest** award ceiling ($0.80M) and a **14× ceiling-vs-obligation gap** ($81.3M ceiling Δ vs $5.76M obligation Δ = elastic IDV headroom, money barely moves).

**"Big award" gate:** fire on `potential_ceiling ≥ agency P99`. Largely mission-real (HHS/DoT have high *medians*, which a heavy tail can't produce) — the percentile transform neutralizes the 47× scale span for free.

**Starvation cohort (`consumed_pct` DELETED — it's saturated):**
```
starving = days_to_expiry ≤ agency-P-rank threshold
         AND is_termination_event = FALSE
         AND award_active = TRUE
         AND ( funding_only-mod cadence present
               OR remaining_ceiling_headroom < per-agency $ floor keyed to big_award_ceiling )
```
Rank `days_to_expiry` and `remaining_ceiling_headroom` on **per-agency percentile** — those two channels carry all the information now that the ratio channel (`consumed_pct`) is dead. Funding-only cadence is the true short-leash tell (NASA 43.1% / DoT 36.1% / DOJ 49.2% fund). A fixed $5M headroom floor is off DoD's chart yet ordinary for HHS.

---

## 4. What the L2 consumers must do

1. **[SHIP-BLOCKER] Add `awarding_agency_code` (CGAC) to `mod_delta`.** It does **not** currently carry it. Every agency-aware operation keys on CGAC; without it the calibration cannot bind. Trivial to add — `awarding_agency_code` is already in the L2 builder's `SPINE_SCAN_COLS`; project it into `delta_out` and index it (BITMAP). Until then, agency-aware kinetic scoring requires a `mod_delta ⋈ prime_award_state` join (as this calibration did).
2. **Add the parent/child pool flag to `mod_delta`** (IDV-parent vs order-child; derivable from `award_kind` + the referenced-IDV linkage). Percentiles are computed *within* `(CGAC × pool)` — this is what kills the DoD $81.3M artifact.
3. **Compute per-agency percentile ranks, NEVER a global $ cutoff.** Materialize the per-agency CDF breakpoints (P90/P95/P99) once per refresh; join at scoring time; the discriminator is the percentile hit, backstopped by the absolute floor only to suppress trivial noise.
4. **Carry the order-share router.** `mode = IDV if pct_order ≥ 55% else DEF`. IDV-mode scores parent-vehicle ceiling-headroom + vehicle recompete (+ GSA-only Schedule-holder novation watch); DEF-mode scores standalone-contract recompete + scope expansion.
5. **Refresh the calibration snapshot on the `mod_delta` build cadence.** Two materialized tables only — dollar-ceiling breakpoints and klass-rate baselines, both keyed by `(CGAC × pool)`. That is the whole agency-aware config.
6. **Fire klass-rate alerts on deviation from the agency's own P90 baseline,** not a shared cutoff (GSA novation bar 15.57/1k; a USDA spike at 3/1k still fires; NASA flags a single termination at its 0.1% floor).

---

## 5. Artifacts to NOT chase (adversarially confirmed)

Do not build agency logic to explain or alert on these — they are measurement/distributional artifacts, not mission behavior:

1. **DoD `dceil_p99_m = $81.3M` is heavy-tail + pool-mixing, not a norm.** Fixed by the parent/child pool split. Do not read it as "DoD mods are 226× bigger."
2. **`consumed_pct` P50=P90=1.0 everywhere is pure saturation** (FPDS `potential_ceiling == federal_action_obligation` on definitive/single-action lines). Delete globally.
3. **DoD `pct_starving = 0.04%` floor is denominator dilution** (51.7M awards, 82% orders at $0.6K median). Low ≠ "DoD starves nothing." Percentile-within-agency on the parent/definitive pool removes it.
4. **`identity_changed` (`ident_p1k`, tightest 2.8× spread) is measurement noise** — a per-mod field-rewrite flag, not a discrete event. Demote to corroborator; do not threshold. Novation (`J`) is the real succession signal.
5. **Low-n klass cells** (NASA term 0.1%, opt 2.1%) — don't freeze exact rates; percentile/z-score within agency handles it.
6. **`ceil50_k` 27× / `ceil99_m` 47× are REAL SCALE, not behavioral** — need normalization (free via percentile), not per-agency behavioral logic.

---

## 6. Method (reproducible)

Top-tier CGAC = `awarding_agency_code` (BITMAP on `prime_award_state`). All figures approx-quantile, `LANCE_BYPASS_SPILLING=true`, DuckDB out-of-core.

- **Capacity** (per agency, single streaming aggregate over `prime_award_state`): `GROUP BY awarding_agency_code` → `approx_quantile(consumed_pct / potential_ceiling / days_to_expiry, …)`, `pct` filters for starving/kind/terminated. No join.
- **Kinetic** (per agency × klass): `mod_delta ⋈ prime_award_state` on `contract_award_unique_key` to attach `awarding_agency_code`, then `GROUP BY (agency, action_type_klass)` → `approx_quantile(abs(delta_potential_ceiling), abs(delta_federal_action_obligation), 0.9/0.99)` + klass mix + `action_type_code='J'` / `is_termination_event` / `identity_changed` rates.

Refresh alongside each `mod_delta` rebuild; the P99 columns track live distribution drift. Once `awarding_agency_code` lands on `mod_delta` (§4.1), the kinetic aggregate needs no join.

---

**Bottom line:** one normalized model, keyed by `(CGAC × parent/child-pool)`, ranking on within-group percentile, with `consumed_pct` deleted, a single global floor formula, and one order-share router. Agency-awareness is delivered *by the normalization* — not by curated tables. The ship-blocker is putting `awarding_agency_code` and the parent/child pool flag on `mod_delta`; everything else binds off those two columns.
