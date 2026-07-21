# SIDECAR GAP REPORT — 2026-07-20 — capitalization triggers

- **Date:** 2026-07-20
- **Artifact at session start:** `query_sidecar_20260720T025249Z` (106 tables)
- **Session topic:** event-driven capitalization-trigger recon for institutional Capital
  Providers (private credit / ABL / invoice factoring / supply-chain finance) — discover
  the working-capital-friction "shapes" (unfinanced fixed-price carry, mobilization velocity
  spike, cost-intensive contract shift, SCA labor-whale, win-then-borrow) and size each TAM in
  the $5M–$100M band. Report: `docs/reference/CAPITALIZATION_TRIGGERS_RECON_2026-07-20.md`.

**Framing:** every answer this session was served BY the sidecar HTTP endpoint — no Lance
scan, no catalyst fallback. The entries below are **degraded answers**: recurring shapes that
returned seconds-class because the metric is computed at query time off a non-sort-key column
of the 108M-row fact (or an unbounded interval join), where an entity-grain rollup would make
them ms-class. `missing sort (too slow unpruned)` / `wrong grain` per §7.

## Entry 1 — FFP → Cost-Plus / T&M contract-type transition (pricing FLOW)

1. **Intent** — "How many middle-market firms are transitioning from firm-fixed-price into
   cash-intensive cost-reimbursement or T&M contract types — prior-24mo fixed-dominant →
   recent-24mo materially cost/T&M — in the $5M–$100M band?" The contract-type-shift capital
   trigger (operator hypothesis b).
2. **Why not the sidecar** — wrong grain. `gtm_entity_pricing_mix` carries the ACTIVE pricing
   STOCK per uei (`active_obl_fixed/cost/tm_lh` + shares) but no prior-vs-recent time-bucketed
   FLOW. Detecting the transition requires the per-action pricing series — only on
   `txn_events_combo` (108M; sorted `naics_code, psc_code, action_date`), scanned off-sort-key
   on `pricing_code` + `action_date`. No `uei × pricing-class × time-window` grain exists.
3. **What I ran instead** — `txn_events_combo`, columns `uei, pricing_code, obligation,
   action_date`; FILTERed sums into recent (≥2024-07) vs prior (2022-07…2024-07) × pricing
   class (fixed A/B/J/K/L/M · cost R/S/T/U/V · tm Y/Z), `GROUP BY uei`, then thresholded
   (prior cost ≤10% of prior obl, recent cost ≥30% of recent obl; band on recent obl).
4. **Cost** — 2,768 ms; ~108M rows scanned (full fact, `pricing_code` off-sort-key) vs 1
   aggregate row returned (56 firms pure cost-plus / 896 cost-or-T&M).
5. **Recurrence** — recurring. "Who is shifting into cash-intensive contract types" is a core
   capital-provider routing segment; re-runs every refresh and per band/threshold/window dial.

## Entry 2 — Win-then-borrow open-window (award ↔ UCC interval pairing)

1. **Intent** — "Firms that just took fresh federal money (≤90d, ≥$250k), have a documented
   history of borrowing within 90d of winning (≥2 prior paired UCC financing filings), and
   have filed no new lien since their last award — the open-loan-window proprietary list."
2. **Why not the sidecar** — wrong grain / too slow (unbounded interval join). Requires
   `gtm_txn_events_slim` (108M) ⋈ `sam_ucc_filings` (376k) on `uei` with a date-BETWEEN
   interval predicate (`first_filing_date BETWEEN action_date AND action_date + 90`) plus a
   NOT-EXISTS anti-join ("no filing since last award"). No materialized award↔borrow pairing
   or entity-grain win-then-borrow event exists. (Signal is also CA/CO-only — a UCC-ingest
   coverage gap, orthogonal to the mart gap.)
3. **What I ran instead** — CTE chain: prune the 108M event stream to debt-layer UEIs
   (`SELECT DISTINCT uei FROM sam_ucc_filings`), 90d fresh-money aggregate, borrow-history
   interval join (HAVING ≥2), NOT-EXISTS open-window filter; columns `uei, action_date,
   action_type_code, obligation` (events) + `first_filing_date, filing_class` (UCC).
4. **Cost** — 5,571 ms; pruned interval join (108M pre-filtered to the debtor-uei subset) vs 1
   aggregate row returned (198 firms / $1.67B fresh money).
5. **Recurrence** — recurring. The most surgical proprietary origination signal in the
   dataset; the 90-day window slides daily (re-runs every refresh) and re-runs per UCC-state
   coverage expansion.

## Entry 3 — SCA/DBA statutory-labor exposure per entity

1. **Intent** — "How many firms carry recent statutory-labor-covered (SCA/DBA) work in the
   $5M–$100M band — the payroll-mobilization / factoring cohort?" plus the
   `labor_standards_code` obligation distribution.
2. **Why not the sidecar** — missing sort / wrong grain. `labor_standards_code` exists only on
   `txn_events_combo` (108M; sorted `naics_code, psc_code, action_date`) — not a sort key and
   not rolled up to entity grain. Neither `gtm_entity_pricing_mix` nor
   `gtm_entity_behavior_rollup` carries the labor-standards dial; no `uei × labor_standard ×
   recent-window` obligation rollup exists.
3. **What I ran instead** — `txn_events_combo`, columns `uei, labor_standards_code, obligation,
   action_date`; recent filter (≥2024-07) `GROUP BY uei` with band HAVING (the labor cut), and
   a separate full `GROUP BY labor_standards_code` for the vocab distribution.
4. **Cost** — 784 ms (band cut) + 3,085 ms (full vocab group-by); recent-slice / full-fact
   rows scanned off-sort-key vs 1–4 rows returned (3,681 firms in band; Y=696k actions/$376B).
5. **Recurrence** — recurring. Labor-whale / payroll-funding is a core capital-provider
   segment and a standing filter for staffing + payroll-finance routing.

---

## Footer — ranked by recurrence × cost

1. **Entry 2 — win-then-borrow** (5.6s, recurring, highest-value proprietary signal; interval
   join re-runs on a daily-sliding window). Top demand.
2. **Entry 1 — FFP→cost/T&M transition** (2.8s, recurring core segment; 108M off-sort-key scan).
3. **Entry 3 — SCA/DBA labor exposure** (0.8–3.1s, recurring standing filter; off-sort-key).

**Served acceptably via proxy this session — recorded for honesty, NOT fallbacks (no build
demand asserted):**
- **Whole-universe MONTHLY velocity (all-NAICS).** The economy-wide YoY spike was answered
  ms-class via annual `gtm_entity_fy_won` (70 ms) — sufficient for the "3–5× spike" question.
  A monthly recent-vs-prior velocity grain exists only for construction
  (`gtm_construction_lane_months`); the all-NAICS monthly mirror is absent. Latent grain gap
  only — no degraded answer incurred.
- **Recent-window multi-state mobilization.** Used lifetime `gtm_prime_pop_lanes` (53 ms) as a
  qualifier for "≥3 PoP states"; a recent-window geo-velocity grain is absent. Lifetime
  footprint sufficed as an intersection filter — no degraded answer incurred.

*Demand capture only — no proposed solutions, per §7. Promotion gating, adjacency sweep, and
build disposition are Mode-2 (build-cycle) work.*
