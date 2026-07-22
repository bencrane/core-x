# Sidecar Gap Report — 2026-07-20 — novation / modification-reason trigger

- **Artifact stamp:** `query-sidecar/query_sidecar_20260720T025249Z.duckdb` (built 2026-07-20T02:52:49Z)
- **Topic:** M&A / novation trigger for GovCon contract-portfolio due-diligence segmentation (Legora/Harvey market recon)

---

## Gap 1 — FPDS `reason_for_modification` (novation / transfer action) not projected

1. **Intent** — "Which contractors just had a contract portfolio change hands — i.e. a
   *novation agreement* or *transfer action* modification — in the last 18–24 months?" This is
   the crisp, day-precision M&A/novation trigger: FPDS stamps `reason_for_modification =
   'NOVATION AGREEMENT'` / `'TRANSFER ACTION'` / `'CHANGE PIID'` on the exact mod that re-papers
   an acquired contract.
2. **Why not the sidecar** — `missing column`. No sidecar mart carries the modification-reason
   dimension. `DESCRIBE txn_rows` (the canonical-name action row) returns 18 columns —
   `action_type_code`/`action_type_description` (A–Y stub codes) but **no
   `reason_for_modification`**. `gtm_txn_events_slim`, `txn_events_combo`, and
   `usaspending_fpds_prime_award_state` likewise omit it. The A–Y action-type codes do not
   distinguish a novation from an ordinary admin mod.
3. **What I ran instead** — proxied the novation pulse from `sam_master_profile_deltas`
   (`field IN ('legal_business_name','cage_code','entity_structure')`, `to_date >= 2024-07-01`)
   and corporate structure from `entity_hierarchy` (ultimate-parent fan-out ⋈
   `gtm_entity_behavior_rollup` for award-active children). The name-change + corroborating
   CAGE/structure-change fingerprint gives ~2,936 firms, but it is directional (SAM re-registration
   is a lagging, optional side-effect of novation — not the contractual event itself).
4. **Cost** — proxy queries were ms-to-sub-second (SAM-delta + hierarchy tables are small/sorted).
   The cost is *accuracy*, not wall time: the proxy misses novations where SAM identity is unchanged
   (asset deals under the same UEI) and over-counts benign DBA/punctuation name edits.
5. **Recurrence** — **recurring.** Novation/transfer detection is a first-class GTM trigger for any
   contract-review / due-diligence buyer (legal AI, M&A advisory, corp-dev). It recurs on every
   "who just got acquired / needs portfolio novation" pull.

**Proposed shape (for the promotion cycle):** a thin entity-grain mart
`gtm_entity_novation_events` — project from the FPDS canonical on Lance:
`uei`, `reason_for_modification`, `action_date`, `award_id_piid`, prior/successor PIID + parent
linkage, `obligation`. Keyed/sorted by `uei` (and a `mod_reason`-sorted copy for cross-entity
"who novated in window W"). Pure column projection off the 392-col canonical — no new
corporate-resolution engine required (`entity_hierarchy` already resolves parents). Rides any
rebuild; adjacency-sweep candidate: fold in `reason_for_modification` alongside the existing
action-type dial in `txn_events_combo` so the portrait layer can slice novation share by
agency/NAICS/PSC in the same pass.

---

_Footer — rank by recurrence × cost:_ Gap 1 is recurring × low-wall-cost-but-high-accuracy-cost.
The demand is real (M&A/novation is a named segment in the Legora/Harvey recon); the fix is a
cheap projection, not a structural build. Report is demand-only; promotion cycle gates disposition.
