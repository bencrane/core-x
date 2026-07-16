# SIDECAR GAP REPORT — 2026-07-16 — billing-latency

- **Date:** 2026-07-16
- **Sidecar artifact:** `query-sidecar/query_sidecar_20260716T030427Z.duckdb` (95 tables)
- **Session topic:** platform-app billing/financing queries unusable at ~49 s; PoP-on-events cannot compose with the job/need combo layer. Demand source: the phrase-agent's disclosed limitations (operator-forwarded) + operator directive ("lightning fast or what's the point — real use in actual sales demos").

---

## Entry 1 — Billing/financing shapes are seconds-class (join with the 83M side)

1. **Intent** — "Companies whose active book is FFP / unfinanced / small-determined" and every award-grain billing shape ("active FFP awards for UEI X", "expiring FFP unfinanced").
2. **Why not the sidecar** — `wrong grain / missing denormalization`: the pricing latest-state lives on `award_plan_state` (83M) and every consumer query joins it to `usaspending_fpds_prime_award_state` (83M) at query time. ANY query-time join with an 83M side saturates the 2-thread serving box — the position-orders lesson, re-learned. Reproduced on serving: bare join (active FFP, distinct recipients) = **32.0 s**; the platform-app's fuller predicate stack = ~49 s.
3. **What I ran instead** — the join, at 32–49 s. Unusable interactively.
4. **Cost** — 32–49 s per query; demo-blocking.
5. **Recurrence** — recurring: the platform-app billing lane runs these shapes live.

## Entry 2 — PoP-on-events cannot compose with the job/need combo layer

1. **Intent** — "entities with event X in state S doing <combo>-shaped work" — event verb × PoP × NAICS/PSC in one statement.
2. **Why not the sidecar** — `wrong grain`: `txn_recipient_month_pop` (2026-07-15 cycle) carries no naics/psc; the phrase compiler refuses the composition.
3. **What I ran instead** — nothing (refusal); the fact `txn_events_combo_by_geo` can serve it but the compiler's rollup lane targets the month rollup.
4. **Cost** — refusal in the phrase layer.
5. **Recurrence** — recurring: job/need × geo × event is a stated demo query family.

---

## Disposition (build cycle 2026-07-16, artifact `PENDING-VERIFY`)

### Build scope block (adjacency sweep, written before the build fired)

**Ships from demand:**
- E1 root-cause fix: `award_plan_state` moved BEFORE `usaspending_fpds_prime_award_state` in the manifest; `_PARENT_WINDOW_SQL` **denormalizes** `latest_plan`, `latest_pricing_code`, `latest_financing_code`, `latest_business_size` onto the award row (1:1, exact parity kept). Billing shapes become single-table pruned reads — no query-time 83M join exists anymore.
- E1 demo lens: `gtm_entity_pricing_mix` (767k rows probe-measured, uei-sorted) — per-entity active/total book split by pricing class (fixed A,B,J,K,L,M / cost R,S,T,U,V / tm_lh Y,Z / other — class map probe-verified against `fpds_code_vocab`), `active_obl_ffp_unfinanced` (+ct), `active_obl_small_determined`, `active_fixed_share`, `active_ffp_unfinanced_share`. "Predominantly-FFP primes" = one uei-sorted read.
- E2: `naics_code`, `psc_code` added to the `txn_recipient_month_pop` grain (27.5M → 37.0M rows, probe-measured +35%) — event × geo × combo composes; sort unchanged so existing state-anchored reads prune identically; coarser-grain aggregations remain correct (SUM over finer rows).

**Adjacency riders:**
- `active_ffp_unfinanced_ct` alongside the $ column (count phrasing rides the same FILTER pass).
- Both share columns precomputed (`active_fixed_share`, `active_ffp_unfinanced_share`) — threshold phrases need no division at query time.

**Considered and NOT taken:**
- Lifetime-window class splits on the mix — active book is the demo lens; lifetime totals ride as `award_ct`/`obl_total` context only. Structural-gated if demanded.
- A pricing-sorted copy of award_state — pricing predicates are residual filters after the `current_end_date` prune (actives cluster at the sort tail); denormalization removes the join, which was the actual cost.

### Per-entry verdicts

| Entry | Verdict | Shipped |
|---|---|---|
| E1 billing latency | **Promoted** (denormalization + one entity-grain mart) | pricing cols on award_state; `gtm_entity_pricing_mix` |
| E2 PoP × combo composition | **Promoted** (grain widening, no new table) | naics/psc on `txn_recipient_month_pop` |

### Measured deltas (before → after)

- PENDING-VERIFY
