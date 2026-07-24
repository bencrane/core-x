# award-outlay-spine

**Status:** `open` — **blocked upstream** (not buildable by a sidecar rebuild alone)

## Capability

Obligated vs outlaid dollars (paid-%) at award grain over the in-force book — per-award
drawer clock, per-firm paid-% of book, and financing×pricing paid-% cuts; carries the
OBBBA/IIJA/COVID supplemental obligation/outlay trace in the same lane.

## Evidence trail

- 2026-07-21 — [SIDECAR_GAP_REPORT_2026-07-21-capital-video-surfaces.md](../SIDECAR_GAP_REPORT_2026-07-21-capital-video-surfaces.md)
  Entry 1 (ranked #1): outlays structurally absent from the sidecar (FPDS = obligations
  only; outlays are Treasury File-C); ~40 s Lance scan, 744k rows → 12 result rows,
  directional-only because the upstream pull is capped. Recurring: video narrative, award
  drawer, firm drawer, OBBBA trace.
- 2026-07-22 disposition (same report): PARKED — blocked upstream (api_fresh pull
  half-covered; bulk snapshot stale). Protocol path authored as
  [docs/plans/2026-07-22-AWARD_OUTLAY_STATE_RECONCILED_SPINE_PLAN.md](../../plans/2026-07-22-AWARD_OUTLAY_STATE_RECONCILED_SPINE_PLAN.md)
  — reconciled bulk∪fresh spine, parity to 255,901 active keys, then sidecar promotion (its
  P8 is one Tier-C exact-parity manifest entry).

## Proposed shape

- Upstream (the blocker): build `usaspending_award_outlay_state` per the reconciled-spine
  plan (data leg, not a sidecar rebuild).
- Sidecar: one Tier-C entry `{ds:'usaspending_award_outlay_state', tier:'C',
  sort:['contract_award_unique_key']}` — ~256k rows, SELECT * exact parity.
- Projected artifact delta: **<0.1 GiB**.

## Adjacency candidates

Per the plan's §6: linkage flag, financing-unknown bucket, paid-% denominators, supplemental
columns — all land upstream and ride the copy for free.

## Notes

Do not promote until the upstream spine exists and passes its Tier-1 gates. The sidecar leg
is trivial; the demand is already operator-committed via the plan.
