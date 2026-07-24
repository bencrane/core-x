# novation-mod-reason

**Status:** `open`

## Capability

Distinguish real portfolio-change events (novation agreement / transfer action / change
PIID) from admin mods — "which contractors just had a contract book change hands in the
last 18–24 months" — via FPDS `reason_for_modification`, which no sidecar mart carries.

## Evidence trail

- 2026-07-20 — [SIDECAR_GAP_REPORT_2026-07-20-novation-mod-reason.md](../SIDECAR_GAP_REPORT_2026-07-20-novation-mod-reason.md)
  Gap 1: `reason_for_modification` absent from `txn_rows`, `gtm_txn_events_slim`,
  `txn_events_combo`, `usaspending_fpds_prime_award_state`; A–Y action codes cannot
  distinguish novation. Proxy via `sam_master_profile_deltas` is ms-fast but
  accuracy-wrong (misses same-UEI asset deals, over-counts DBA edits; ~2,936 firms
  directional). Footer: recurring × high-accuracy-cost; named GTM segment
  (legal-AI / M&A / corp-dev buyers).

## Proposed shape

- Column-grain: project `reason_for_modification` onto `txn_rows` (108M rows, mostly
  NULL/short VARCHAR) — rides any rebuild's existing projection.
- Optional mart: `gtm_entity_novation_events` (uei, reason, action_date, piid, prior/
  successor linkage, obligation) — novations are rare; thousands-of-rows scale, sorted
  `uei` + a `mod_reason`-sorted copy per the report.
- Projected artifact delta: **~0.5–1.5 GiB** (dominated by the 108M-row column add).

## Adjacency candidates

Sibling FPDS mod-family columns in the same projection pass (e.g. `action_type` already
present; check `other_than_full_and_open_competition`-class siblings at source before the
build per the adjacency sweep).

## Notes

The cheap, correct fix is the projection; the events mart is a convenience rollup that can
ride the same build.
