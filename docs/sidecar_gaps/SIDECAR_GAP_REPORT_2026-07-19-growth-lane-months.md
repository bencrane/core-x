# SIDECAR GAP REPORT — 2026-07-19 — growth-window cuts for the surety Growth card

Artifact at session: `query-sidecar/query_sidecar_20260718T021418Z.duckdb` (105 tables).
Session topic: the gc Markets "Growth" card (surety lane) — firm growth measured as
recent-window vs baseline-window obligations, per construction work-lane, windows dialed
at query time. Operator directive on record this session: **"I am expressing a strong
desire here to build the sidecar. My demand for it is there."** Month grain ruled
acceptable (sub-month windows nixed).

## Entry 1 — lane-scoped growth-window sums

1. **Intent** — "Which firms in a construction work-lane (the 5 surety pair-sets, 544
   pairs) grew their in-lane obligations N× comparing the last X months against the Y
   months before, within a recent-window dollar band?" Windows are dials (30/60/90/…
   days originally; months after the grain ruling).
2. **Why not the sidecar** — wrong grain / missing sort (too slow unpruned). Lane-scoped
   windowed sums require the 108M-row `gtm_txn_events_slim` scanned per query (pairs
   VALUES join + two filtered sums + GROUP BY uei). The uei-month rollups
   (`gtm_txn_recipient_month_rollup` / `txn_recipient_month_by_type`) carry no naics/psc.
   **Probe correction (build-cycle step 1):** `txn_recipient_month_pop` DOES carry
   naics × psc × month and can express the shape — but it is sorted
   (action_type, pop_state, county, month), so a pair-scoped scan cannot prune: measured
   2.5s for the same lane cut (37M rows scanned). The verdict is therefore cost, not
   expressibility: the card's per-interaction recompute budget is ms-class.
3. **What I ran instead** — live sidecar SQL over `gtm_txn_events_slim` (columns: uei,
   naics_code, psc_code, action_date, obligation): pairs VALUES join, two
   `sum(obligation) FILTER (action_date …)` windows, GROUP BY uei, ratio + band gate.
4. **Cost** — single lane 12/24: 2.3s · 45d/90d: 1.4s · five lanes per-lane in one
   statement: 7.2s · 72-month lookback: 1.5s. Rows scanned: the pair-matched slice of
   108M per query; returned: 10–360.
5. **Recurrence** — recurring by construction: this is the Growth card's every-dial-turn
   query shape (the cut rail recomputes per interaction), plus the rshq Surety Growth
   viewer's rebake shape.

## Entry 2 — publication-watermark anchoring

1. **Intent** — "What is the last month the transaction record is actually complete
   through?" (FPDS publication lag: the freshest ~3.5 weeks before the bake are empty;
   weeks 4–12 run ~half of steady-state volume — DoD's ~90-day embargo.)
2. **Why not the sidecar** — not a gap in data, a gap in anchored usage: windows keyed to
   `current_date` silently include unpublished air. Served by `max(month)` on the month
   marts; needs no build — recorded so the pattern lands in the guide.
3. **What I ran instead** — weekly volume series over `gtm_txn_events_slim` (26 weeks) to
   locate the cliff; `max(month)` CTE as the window anchor in the growth probes.
4. **Cost** — 0.6s (diagnostic only).
5. **Recurrence** — every short-window growth query must anchor this way.

## Ranking

Entry 1 is the build: recurring × ~2s per interaction on a product surface (and 7s for
the five-lane pack shape). Entry 2 is a usage pattern for the guide, no build.
