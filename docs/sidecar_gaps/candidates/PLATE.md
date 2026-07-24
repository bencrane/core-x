# PLATE — query-sidecar demand, reconciled

**Generated:** 2026-07-24 (post-build) by `/sidecar-priority` + `/sidecar-gaps` Mode 2 · serving
artifact at generation: `query_sidecar_20260724T044059Z` (126 tables · 1,798,697,203 rows ·
73.45 GiB, /healthz-verified) · ledger id 46.

Regenerated wholesale each run — do not hand-edit; edit dossiers.

## Just shipped (2026-07-24 geo + labor + cost-structure cycle, PR #1337)

The entire prior open slate promoted in one build. Probe-verification overturned two of the four
headline claims before any cost was paid.

| Candidate | Verdict | Shipped | Before → after |
|---|---|---|---|
| [award-grain-geo-spine](award-grain-geo-spine.md) | promoted (reduced) + routing-fix | `award_geo_state` (82.87M, EXACT), `pop_place_fy` (486k), 4-col combo rider, 4 county refs | Entry 1 OOM → 16 ms; Entry 2 40.6%-sample → 100% coverage @ 1.6 s; Gap 1 haversine (94% short) → 59 ms |
| [ecec-labor-cost-components](ecec-labor-cost-components.md) | promoted | `bls_ecec_costs` (627k), `bls_ecec_burden` (321) | ~6 min credentialed Lance-direct → 18.6 ms (7.3% health share) |
| [bea-io-purchased-services](bea-io-purchased-services.md) | promoted (reduced) | `bea_bls_klems`, `bea_contingent_labor_intake`, `bea_io_use_summary_annual`, `bea_naics_concordance` | credentialed Lance-direct → 10 ms (25.6% service share) |
| [novation-mod-reason](novation-mod-reason.md) | routing-fix + rider | `gtm_award_novation_events` (88k) + `action_type_code` guide correction | 10 s predecessor-linkage → 9 ms; proxy over-count ~2× → exact |

Artifact 68.37 → 73.45 GiB (+5.08, ~3% of the wedge runway consumed); build 36.8 min; 126 tables,
**zero parity mismatches**. Two correctness disclosures on the record (import ratios were on a 40%
sample; per-field arg_max is coverage-maximizing not strict latest-txn) — carried in the dossiers
and the agent guide.

## Open candidates, ranked recurrence × cost

**None.** The open slate cleared in this build. The next `/sidecar-priority` run promotes from
whatever the next gap reports carry.

## Parked ledger (alive, not ranked — full detail in dossiers)

Ready-on-trigger: [sbir-phase-ladder](sbir-phase-ladder.md) (spec ready; execute whenever a build
touches `txn_events_combo`) · [win-then-borrow](win-then-borrow.md) (needs equality-key design) ·
[compliance-friction-mart](compliance-friction-mart.md) + [staffing-absorption-mart](staffing-absorption-mart.md)
(formula/methodology freeze) · [entity-month-velocity](entity-month-velocity.md) (sparklines-as-page-section
trigger).

Blocked upstream/external: [award-outlay-spine](award-outlay-spine.md) (reconciled-spine plan must
land first — the one item deliberately excluded from the just-fired build) ·
[gwac-vehicle-crosswalk](gwac-vehicle-crosswalk.md) (reference data) ·
[ucc-state-corpus-expansion](ucc-state-corpus-expansion.md) (ingest roadmap) ·
[audience-mart-rebuild-riders](audience-mart-rebuild-riders.md) ·
[military-installation-riders](military-installation-riders.md) (flags) ·
[subout-pair-grain-context](subout-pair-grain-context.md).

New parks from this cycle (recorded in the geo/ecec/bea dossiers): vehicle/IDV county backfill
(closes the ~38% award-grain county hole; separate cycle) · `zip_county_xwalk` (refuted at +0.2%) ·
`pop_congressional_code_current` (present-day district; lobbying trigger) · BLS ECI escalation rates
(not in Lance — needs an ingest first) · `bls_oews_2025` (no staffing demand) · `bea_io_use_detail`
(9 yr stale) · QCEW-scale BEA/BLS members (own dated demand).

Dormant (no recurrence since park): [sam-attachment-substrate](sam-attachment-substrate.md) ·
[ucc-derived-grain-residuals](ucc-derived-grain-residuals.md) ·
[pricing-family-residuals](pricing-family-residuals.md) ·
[setaside-award-book-split](setaside-award-book-split.md) ·
[txn-description-per-action](txn-description-per-action.md) (only parked item that materially moves
the disk math if ever promoted) · [agency-lens-residuals](agency-lens-residuals.md) ·
[pdl-domain-sorted-copy](pdl-domain-sorted-copy.md) ·
[equipment-geo-distance](equipment-geo-distance.md) (likely absorbed by the geo spine now built) ·
[labor-demand-poc-marts](labor-demand-poc-marts.md) · [bls-oews-staffing-patterns](bls-oews-staffing-patterns.md).

## Capacity check (for the next cycle)

- Current artifact **73.45 GiB** → swap peak 2× = 146.9 GiB of ~372.5 GiB usable (39.4%).
- **Wedge sits at ~183 GiB artifact** — ~110 GiB of artifact runway remains. Re-derive from
  `/healthz` + `ops.query_sidecar_runs` at the next gating.
- Duration: build now 36.8 min (was 27.9 at 113 tables); ~0.5 min/GB, 60-min line at ~118 GiB.

## Recommendation (operator can overrule)

**WAIT.** The open slate is empty — everything demand-evidenced has shipped. The next build fires
when new gap reports land enough recurring × costly demand to clear the structural gate, or on an
operator directive. `/sidecar-priority` re-synthesizes when the next reports arrive.
