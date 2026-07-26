# PLATE — query-sidecar demand, reconciled

**Generated:** 2026-07-26 by `/sidecar-priority` · serving artifact at generation:
`query_sidecar_20260724T044059Z` (126 tables · 1,798,697,203 rows · 73.45 GiB,
/healthz-verified 2026-07-26).

Regenerated wholesale each run — do not hand-edit; edit dossiers.

## Reconciliation notes this run

- New dossier [demo-region-grain](demo-region-grain.md) (open) — born from
  `SIDECAR_GAP_REPORT_2026-07-26-demo-region-grain.md` + operator-articulated demand
  ("too much computing / lag on the actual demo").
- New dossier [award-key-point-reads](award-key-point-reads.md) (promoted,
  `query_sidecar_20260722T032457Z`) — back-filled: the 2026-07-22 award-key-pfx companions
  had shipped with no dossier; reconciles the award-key-probes gap + the 2026-07-21
  capital-video Entry 2 (13–27 s → 0.81 s per award).
- Top-level `SIDECAR_GAP_REPORT_2026-07-17-lender-book-bridge.md` verified as a stale
  pre-disposition duplicate of the processed copy — zero uncovered entries; recorded in
  [ucc-derived-grain-residuals](ucc-derived-grain-residuals.md) as do-not-double-count.
- 2026-07-21 capital-video Entry 1 (outlay spine) confirmed already carried by
  [award-outlay-spine](award-outlay-spine.md) (parked, blocked on the reconciled-spine plan).

## Open candidates, ranked recurrence × cost

| # | Candidate | Demand | Cost per recurrence | Shape | Delta |
|---|---|---|---|---|---|
| 1 | [demo-region-grain](demo-region-grain.md) | 5 entries (2026-07-26) + 2 precursor notes (2026-07-23) + operator directive; recurs on EVERY demo iteration, region add, and data refresh; 14 macro-region videos planned | One full bake = ~21 region passes × 1–3 min each; the firms-stats join form 408s outright on large macros (killed one bake run after ~25 min) | 3 small tables: `demo_region_firm_stats` (~21 rows), `demo_region_naics_fy` (≤130K rows), `demo_region_archetype_awards` (~126 rows) | **<100 MiB** |

The only open candidate — but the highest-leverage per GiB ever plated: the entire demo-bake
layer (all six one-button scripts' region math) collapses from ~30–60 min of full-mart scans
to warm keyed lookups, for a delta that rounds to zero against the wedge.

## Parked ledger (alive, not ranked — full detail in dossiers)

Ready-on-trigger: [sbir-phase-ladder](sbir-phase-ladder.md) (spec ready; **trigger fires if
demo-region-grain touches `txn_events_combo`** — the same build should take it) ·
[win-then-borrow](win-then-borrow.md) (needs equality-key design) ·
[compliance-friction-mart](compliance-friction-mart.md) +
[staffing-absorption-mart](staffing-absorption-mart.md) (formula/methodology freeze) ·
[entity-month-velocity](entity-month-velocity.md) (sparklines-as-page-section trigger).

Blocked upstream/external: [award-outlay-spine](award-outlay-spine.md) (reconciled-spine plan
first) · [gwac-vehicle-crosswalk](gwac-vehicle-crosswalk.md) (reference data) ·
[ucc-state-corpus-expansion](ucc-state-corpus-expansion.md) (ingest roadmap) ·
[audience-mart-rebuild-riders](audience-mart-rebuild-riders.md) ·
[military-installation-riders](military-installation-riders.md) (flags) ·
[subout-pair-grain-context](subout-pair-grain-context.md).

Prior-cycle parks (recorded in the geo/ecec/bea dossiers): vehicle/IDV county backfill ·
`zip_county_xwalk` (refuted at +0.2%) · `pop_congressional_code_current` (lobbying trigger) ·
BLS ECI escalation rates (needs ingest) · `bls_oews_2025` · `bea_io_use_detail` (9 yr stale) ·
QCEW-scale BEA/BLS members.

Dormant (no recurrence since park): [sam-attachment-substrate](sam-attachment-substrate.md) ·
[ucc-derived-grain-residuals](ucc-derived-grain-residuals.md) ·
[pricing-family-residuals](pricing-family-residuals.md) ·
[setaside-award-book-split](setaside-award-book-split.md) ·
[txn-description-per-action](txn-description-per-action.md) (only parked item that materially
moves the disk math) · [agency-lens-residuals](agency-lens-residuals.md) ·
[pdl-domain-sorted-copy](pdl-domain-sorted-copy.md) ·
[equipment-geo-distance](equipment-geo-distance.md) (likely absorbed by the geo spine) ·
[labor-demand-poc-marts](labor-demand-poc-marts.md) ·
[bls-oews-staffing-patterns](bls-oews-staffing-patterns.md).

## Capacity check

- Current artifact **73.45 GiB** → swap peak 2× = 146.9 GiB of ~372.5 GiB usable (39.4%).
- Wedge at ~183 GiB artifact — ~110 GiB runway. The open slate consumes **<0.1 GiB** of it.
- Duration: last full build 36.8 min; ~0.5 min/GB → this cycle lands in the same envelope.

## Recommendation (operator can overrule)

**BUILD NOW.** Operator directive on the record (2026-07-26), five same-day gap entries, one
already-failed bake run, and a near-zero artifact delta. Suggested scope block for
`/sidecar-gaps` Mode 2 gating:

- **Promote:** `demo_region_firm_stats` (region×metric, incl. active-book cols) ·
  `demo_region_naics_fy` (region×NAICS×FY obligations, national `US` rows included) ·
  `demo_region_archetype_awards` (region×archetype×tier top awards).
- **Adjacency riders:** state-grain rows for all 51 states (macros are state unions — free in
  the same pass) · KLEMS-sector collapse columns on the NAICS rollup ·
  `equipment_flowdown`-weighted columns (the ratio the bakes compose by hand) ·
  **[sbir-phase-ladder](sbir-phase-ladder.md) if the build touches `txn_events_combo`**
  (standing ready-on-trigger).
- **Stays gated:** everything in the parked ledger above.
