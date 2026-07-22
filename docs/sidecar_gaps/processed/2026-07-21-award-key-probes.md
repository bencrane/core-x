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

---

## DISPOSITION (2026-07-22) — PROMOTED, shipped

**Verdict:** promoted (structural, operator-directed + demand-evidenced by every Explore award-dot
click). Shipped four award-key point-read companions in the build-cycle rebuild
(`query_sidecar_20260722T032457Z`, 113 tables):

| Mart | Source | Sort |
|---|---|---|
| `prime_award_state_by_key` | usaspending_fpds_prime_award_state (full 56-col) | award_key_pfx, contract_award_unique_key |
| `txn_events_combo_by_award` | txn_events_combo | award_key_pfx, award_key, action_date |
| `txn_rows_by_award` | txn_rows | award_key_pfx, contract_award_unique_key, action_date |
| `award_pop_centroids_by_key` | usaspending_award_pop_centroids | award_key_pfx, generated_unique_award_id |

**The pfx twist (PR #1305, caught at phase-5 verification):** the first build (#1304) sorted by the
full award key alone and STILL full-scanned — DuckDB string zone-maps truncate min/max at 8 bytes and
every key opens `CONT_AWD_`/`CONT_IDV_`, so pruning was zero. Fix: `award_key_pfx = substr(key,10,12)`
as the leading sort key; probes carry `award_key_pfx = substr('<key>',10,12) AND <fullkey> = '<key>'`.

**Measured before → after (same award):**
| Probe | Before (end-date/name-sorted) | After (pfx) |
|---|---|---|
| anchor row | 8,619 ms | **32 ms** |
| FY ledger | 11,516 ms | **29 ms** |
| recent actions | ~8,400 ms | **61 ms** |
| centroid | 3,309 ms | **24 ms** |
| /award end-to-end (BFF) | 13–27 s | **0.81 s** |

Consumed by catalyst `/market-slice/award` (core-x #1308) — the #1299 uei-pruning legs removed, the
BFF 90s timeout reverted to 30s (gc-hq-new #101). **`award_subout_rollup` was correctly NOT
companioned** — it is a 197k-row GROUP-BY aggregate, already cheap (adversarial review confirmed).
