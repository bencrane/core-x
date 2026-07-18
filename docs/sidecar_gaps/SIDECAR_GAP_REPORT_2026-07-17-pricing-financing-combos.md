# SIDECAR GAP REPORT — 2026-07-17 — pricing×financing combos (capital-provider lens)

- **Date**: 2026-07-17 (late session, after the market-query router shipped: core-x #1186)
- **Serving artifact**: `query-sidecar/query_sidecar_20260717T202427Z.duckdb` (104 tables)
- **Session topic**: GC platform predicate-grammar build + operator query testing; operator
  DIRECTIVE landed mid-session (this report's entry 1 is directive-backed, not merely observed).

---

## Entry 1 — pricing × financing combo matrix at entity grain

1. **Intent**: "What payment combos does a firm's active book carry?" — e.g. "firms whose
   active book is ≥X% progress-payment-financed", "performance-based-payments primes",
   the full pricing-class × financing-class matrix per firm. Operator ruling (verbatim):
   "this collapse is hugely problematic... pricing x financing combo is a first-class
   predicate citizen... for our capital provider partners".
2. **Why not the sidecar**: `missing column(s)` — `gtm_entity_pricing_mix` carries pricing-class
   splits and exactly ONE combo (FFP × unfinanced). No financing-class columns
   (progress / performance-based / commercial), no pricing×financing matrix.
3. **What I ran instead**: award-grain GROUP BY over `usaspending_fpds_prime_award_state`
   (date-pruned active, `latest_pricing_code × latest_financing_code`), 83ms — answers the
   market-level matrix but NOT the per-entity predicate ("firms whose…"), which would need
   this aggregation per query per conjunction.
4. **Cost**: 83ms for the matrix; the entity-grain predicate shape = repeated per-query
   aggregation over the 85M award state inside every composed conjunction.
5. **Recurrence**: recurring by construction — a first-class predicate family for every
   capital-provider composition (operator directive).

## Entry 2 — momentum predicate slow (3.1s) — MIS-ROUTED, not a gap

1. **Intent**: "firms that won a new award (≥$250k) in the last 3 months" —
   `recent_award_actions` leg of the new market-query router.
2. **Why not the sidecar**: `didn't know it was there` — the leg full-scanned
   `gtm_txn_recipient_month_rollup` (uei-sorted). A `(action_type_code, month)`-sorted copy
   `txn_recipient_month_by_type` (34.1M rows) has existed since the 2026-07-15 audience-spec
   cycle, built for EXACTLY this shape.
3. **What I ran instead**: base-table scan with an `action_type_vocab` IN-subquery: 3,117ms.
4. **Cost**: 3.1s per momentum leg.
5. **Recurrence**: every momentum predicate → ROUTING FIX in `market_query_v1.py`
   (retarget + literal code families for zone-map pruning), no build required.

## Entry 3 — lifetime standalone-instrument split (definitive contract vs purchase order)

1. **Intent**: "firms that have (ever) held definitive contracts vs purchase-order-only
   firms" — surfaced during the ontology sitting (test-log entry 9: the standalone bucket
   is 94% purchase orders); operator asked what offering the split requires.
2. **Why not the sidecar**: `missing column(s)` + `missing sort (too slow unpruned)` — no
   entity-grain instrument counts; lifetime scan of the 85M award state can't prune on the
   `current_end_date` sort.
3. **What I ran instead**: nothing (answered structurally from topology probes); active-scope
   variant is a date-pruned computed leg (ms); lifetime variant unserved.
4. **Cost**: n/a today; forbidden-shape full scan if attempted.
5. **Recurrence**: plausible composer predicate awaiting operator customer-facing ruling;
   promotable free as an adjacency rider on any pricing-mix rebuild (same scan).

---

**Ranking (recurrence × cost)**: 1 (directive, recurring, per-conjunction cost) ≫
3 (free rider) > 2 (routing fix, no build).
