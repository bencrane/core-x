# PLATE — query-sidecar demand, reconciled

**Generated:** 2026-07-23 by `/sidecar-priority` · sources: 5 top-level gap reports +
parked-structural sweep of all 25 `processed/` dispositions · serving artifact at generation:
`query_sidecar_20260722T032457Z` (113 tables · 1,714,347,196 rows · 68.37 GiB)

Regenerated wholesale each run — do not hand-edit; edit dossiers.

## Open candidates, ranked recurrence × cost

| # | Candidate | Recurrence | Cost of not having it | Proposed delta | Note |
|---|---|---|---|---|---|
| 1 | [award-grain-geo-spine](award-grain-geo-spine.md) | 3 entries / 2 reports (07-22, 07-23 ×2; report-ranks #1 and #2) | 4.3–25 s per ask; two serving OOMs; Arc-1 number still UNANSWERED; sector substrate operator-flagged unacceptable | ~1–2 GiB | Merges county-at-award-grain + distinct-places + PoP-vs-HQ into one ~30M-row mart |
| 2 | [ecec-labor-cost-components](ecec-labor-cost-components.md) | 2 dated touches on the same table (parked 07-14, demanded 07-23) | ~6 min + credentialed Lance-direct per ask, for demo-quoted numbers | ~0.05 GiB | Cheapest item on the plate |
| 3 | [novation-mod-reason](novation-mod-reason.md) | 1 report, recurring GTM trigger (named buyer segment) | ms-fast proxy but accuracy-wrong (misses asset deals, over-counts DBA edits) | ~0.5–1.5 GiB | Column projection + tiny events mart |
| 4 | [award-outlay-spine](award-outlay-spine.md) | recurring (video, both drawers, OBBBA trace; report-rank #1) | ~40 s directional-only Lance scan | <0.1 GiB | **BLOCKED upstream** — reconciled-spine plan must land first; excluded from the buildable slate |
| 5 | [bea-io-purchased-services](bea-io-purchased-services.md) | 1 dated entry (07-15) + announced demo draw; upstream landed 07-23 | credentialed Lance-direct per slice | ~0.1–0.5 GiB | Small tables only; QCEW-scale stays gated |

## Capacity check (top buildable slate: #1 + #2 + #3 + #5)

- Projected artifact: 68.4 → **~71–72 GiB** (+2.5–4 GiB).
- Disk headroom: swap peak = 2× artifact ≈ 144 GiB vs ~372 GiB usable (400 GB disk).
  **Wedge sits at ~183 GiB artifact — this slate consumes ~2% of the remaining runway.**
- Duration: ~0.5 min/GB ⇒ +~2 min on the median ~32 min build (60-min line at ~118 GB
  artifact — not approached). Re-derive both from `/healthz` + `ops.query_sidecar_runs` at
  gating time.

## Parked ledger (alive, not ranked — full detail in dossiers)

Ready-on-trigger: [sbir-phase-ladder](sbir-phase-ladder.md) (spec ready; execute whenever a
build touches `txn_events_combo`) · [win-then-borrow](win-then-borrow.md) (needs
equality-key design) · [compliance-friction-mart](compliance-friction-mart.md) +
[staffing-absorption-mart](staffing-absorption-mart.md) (formula/methodology freeze) ·
[entity-month-velocity](entity-month-velocity.md) (sparklines-as-page-section trigger).

Blocked upstream/external: [gwac-vehicle-crosswalk](gwac-vehicle-crosswalk.md) (reference
data) · [ucc-state-corpus-expansion](ucc-state-corpus-expansion.md) (ingest roadmap) ·
[audience-mart-rebuild-riders](audience-mart-rebuild-riders.md) ·
[military-installation-riders](military-installation-riders.md) (flags) ·
[subout-pair-grain-context](subout-pair-grain-context.md).

Dormant (no recurrence since park): [sam-attachment-substrate](sam-attachment-substrate.md)
· [ucc-derived-grain-residuals](ucc-derived-grain-residuals.md) ·
[pricing-family-residuals](pricing-family-residuals.md) ·
[setaside-award-book-split](setaside-award-book-split.md) ·
[txn-description-per-action](txn-description-per-action.md) (only parked item that
materially moves the disk math if ever promoted) ·
[agency-lens-residuals](agency-lens-residuals.md) ·
[pdl-domain-sorted-copy](pdl-domain-sorted-copy.md) ·
[equipment-geo-distance](equipment-geo-distance.md) (likely absorbed by the geo spine) ·
[labor-demand-poc-marts](labor-demand-poc-marts.md) ·
[bls-oews-staffing-patterns](bls-oews-staffing-patterns.md).

## Hygiene (not build items)

- Top-level `SIDECAR_GAP_REPORT_2026-07-17-lender-book-bridge.md` is a strict stale subset
  of the processed copy (verified by diff) — archive/delete via the next `/sidecar-gaps`
  disposition PR.
- Builder-infra parks from the pricing-flow handoff (add `retries=1–2`; incremental-publish
  design note) remain open infra ideas — not sidecar tables, not in this plate's scope.

## Recommendation (operator can overrule)

**BUILD NOW.** Four open capabilities ship for ~3 GiB and +~2 min; two of them
(geo spine, ECEC) unblock currently-unanswerable or operator-flagged demo/sector narratives,
and recurrence evidence is multi-report for the top item. Waiting buys nothing — the outlay
spine (the one genuinely blocked item) is excluded and loses nothing by this build.

Suggested scope block for `/sidecar-gaps` gating:

- **Promote (demand-evidenced):**
  - `award_geo_spine` (~30M rows: award_key, uei, zip5, pop_county_fips, pop_state,
    hq_state, obligated, current_value, window dates, active_flag; sorted
    `pop_county_fips`) + `zip_county_xwalk` reference.
  - `bls_ecec_costs` (627k) + `bls_ecec_burden` (321), series-key decode columns.
  - `reason_for_modification` projected onto `txn_rows` + `gtm_entity_novation_events`
    rollup.
  - BEA IO small-table set (IO use detail + concordances) — exact list fixed at gating
    after schema probes.
- **Adjacency sweep at gating:** county/state names, congressional district, FPDS
  mod-family siblings, NAICS↔IO crosswalks, `bls_oews_2025` only if a staffing shape
  exists by then.
- **Stays parked:** everything in the parked ledger above, each with its stated trigger.

Execution path: `/sidecar-gaps` Mode 2 gates this slate (probe schemas before believing
column names) → `/sidecar-build` fires. This plate does NOT fire anything.
