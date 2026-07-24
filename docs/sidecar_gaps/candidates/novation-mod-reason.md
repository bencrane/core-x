# novation-mod-reason

**Status:** `routing-fix` + `promoted` (rider) — query_sidecar_20260724T044059Z (2026-07-24, ledger id 46, PR #1337). Probe overturned the premise: reason_for_modification DOES NOT EXIST - the dimension is action_type_code (J=novation, S=change PIID, T=transfer), already on 11 serving tables + glossed in action_type_vocab; the capability served at 0.94s the whole time -> ROUTING FIX in AGENT_GUIDE §4. The one genuinely-missing leg (predecessor->successor identity) built as gtm_award_novation_events (88,092, aggregate, local lag() window - 9ms; the SAM-delta proxy over-counted ~2x). The dossier's 108M-row column add was rejected (already projected + ~660MiB for zero new info).

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
