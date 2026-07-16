# SIDECAR GAP REPORT — 2026-07-15 — subout-rate-by-recipient-shape

- **Date:** 2026-07-15 (same session as the pricing-terms cycle)
- **Sidecar artifact:** `query-sidecar/query_sidecar_20260716T014507Z.duckdb` (94 tables, ready)
- **Session topic:** farm-out rate characterized by the RECIPIENT's shape (what the sub does, evidenced by its own prime awards), not just the prime's combo.

---

## Entry 1 — Sub-out rate by recipient shape (prime × context × recipient-code)

1. **Intent** — "Primes that route ≥N% of their <context-code> work to subs whose own shape is Y" — the farm-out *rate* where the recipient side is characterized by the sub's own prime-award history (`recipient_code_source = 'awarded_prime_contracts_in_code'`) or its other identity lenses. Operator: "there is a demand for this," building on the shipped combo-grain `farmout_share_*`.
2. **Why not the sidecar** — `missing column(s)` on `gtm_prime_subout_by_recipient_code` (11.8M): carries the numerator (`subaward_amt_total`) but not the prime's obligations in the context code (denominator lives on `gtm_entity_code_lanes`, side='prime', same uei + code axis) and no precomputed rate or within-context share. Additionally `wrong sort` for the demanded anchor: the table is sorted `prime_awardee_uei` only — recipient-shape-anchored reads scan 11.8M unpruned (measured 2.6 s for a full aggregate).
3. **What I ran instead** — described the derivation join in-chat; the rate was not computed. Any consumer must hand-write an 11.8M × 1.7M join with a probe-side `side='prime'` gate (the exact NL-join trap class) each time.
4. **Cost** — none paid numerically this session — which is the gap: the number wasn't cheap enough to just produce. Unpruned recipient-anchored scans are seconds-class.
5. **Recurrence** — recurring: declared input to the revamped phrase/query-search ("routes X-shaped work to Y-shaped subs" is a stated query family).

---

## Ranking

Single entry, operator-directed. Demand only — no proposed solutions.

---

## Disposition (build cycle 2026-07-15/16, artifact `query_sidecar_20260716T030427Z.duckdb`)

Build: single run, success — 95 tables, 1,314,981,229 rows, 48.04 GiB, all parity gates OK
(base cube 11,844,606 rows exact-parity through the denominator join; sort copy identical).
Serving hot-swapped; measurements below are on the live endpoint.

### Build scope block (adjacency sweep, written before the build fired)

**Ships from demand:**
- `subout_rate_lifetime` on `gtm_prime_subout_by_recipient_code` = `subaward_amt_total / prime_obl_lifetime_in_context` — row-preserving LEFT JOIN to `gtm_entity_code_lanes` (side='prime' pre-materialized to a temp table so join keys stay pure equalities; uniqueness of (uei, code_type, code) on the prime side probe-verified → exact-parity gate kept).
- Sort copy `gtm_prime_subout_by_code` — same 11.8M rows re-clustered (`recipient_code_source`, `recipient_code_type`, `recipient_code`): every query filters ONE evidence lens first (the four lenses overlap; summing across them double-counts), then the recipient shape — the demanded anchor prunes instead of full-scanning. Structural, operator-directed.

**Adjacency riders (one line each):**
- `prime_obl_24mo_in_context`, `prime_obl_60mo_in_context`, `prime_obl_lifetime_in_context` — the denominator family rides the same join (windowed rates become query-time dials against `last_subaward_action_date`).
- `prime_action_ct_in_context`, `prime_last_action_in_context` — "is the prime still active in this context" is the next question after any rate read; same join, free.
- `share_of_context_subout` — within-lens window share: "of everything this prime subs out in context X, what fraction goes to shape Y" — the sibling ratio of the demanded one, same build pass.

**Considered and NOT taken, with rationale:**
- Entity-grain overall sub-out share on `gtm_entity_behavior_rollup` — the zoom-out is already answerable warm in ms (SUM over the uei-sorted 38k farm-out lanes); no build needed.
- Share normalization on `gtm_subbed_under_to_primed_in_cooccurrence` — carries both $ sides already; query-time trivial over 589k rows.
- Pair-grain (NAICS×PSC) context for the subout cube — upstream Lance dataset grain, ingest-scale, not a rebuild rider; parked structural.

### Per-entry verdicts

| Entry | Verdict | Shipped |
|---|---|---|
| E1 sub-out rate by recipient shape | **Promoted** (column-grain + one operator-directed sort copy) | rate + 5 denominator/activity riders + within-context share; recipient-anchored sort copy |

### Measured deltas (before → after)

| Shape | Before | After (measured on serving) |
|---|---|---|
| Demanded: "primes routing ≥30% of their 541712 work to subs who prime in 541330" | unanswerable (hand-written 11.8M × 1.7M join with probe-side gate, per session) | **25.5 ms** on `gtm_prime_subout_by_code` (77 primes) |
| Recipient-shape-anchored read (subs-who-prime-in-236220 across all primes) | 2.6 s unpruned scan | **8.0 ms** on the sort copy (325× improvement) |
| Prime-anchored rate/share portrait | numerator only; no rate | **18.4 ms** — rate + within-context share + denominators on the base cube |
