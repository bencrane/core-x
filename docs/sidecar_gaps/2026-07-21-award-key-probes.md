# GAP: per-award txn probes rely on uei-pruning, not award-key sort

Session 2026-07-21 (award profile build, catalyst market-slice /award).

The per-award ledger reads (`txn_events_combo` fy sums, `txn_rows` recent
actions, PoP county/country subqueries) have no award-key-sorted access path;
raw probes full-scan 108M rows (~11.6s each). Mitigated in #1299 by carrying
the recipient-uei predicate from the state row (11.6s → ~1.1s combo /
~75ms txn_rows), which is fine for the drawer but still ~1s-class on combo
and depends on the recipient being single-uei per award.

Promotion candidate: an award-key-sorted txn projection (award_key,
action_date, obligation, action_type, pop fields) — turns every award-grain
ledger read into a pruned ms-class probe. Demand evidence: the Explore award
drawer (every dot click), any future award tear-sheet surface.

## Addendum (same session, post-#1299)

Measured per-probe after uei-pruning: combo fy 0.65s · place 0.38s · txn_rows
75ms — but end-to-end /award still ~13s. The dominant residuals are
award-KEY probes on tables sorted by other columns:

- `usaspending_fpds_prime_award_state` key probe: **4.8s** (sorted by
  current_end_date for expiry pruning; a contract_award_unique_key probe
  scans zone maps across 83M)
- `usaspending_award_pop_centroids` key probe: **0.9s**

Promotion shape refined: an award-key-sorted point-read companion (state row
+ centroid + txn slice keyed by award_key) makes the whole profile ms-class.
Demand evidence: every award-dot click in the Explore award lens.
