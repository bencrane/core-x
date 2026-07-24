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

---

## Disposition (sidecar-gaps Mode 2, 2026-07-24 — artifact `query_sidecar_20260724T044059Z`, ledger id 46)

| # | Verdict | What shipped |
|---|---|---|
| Gap 1 | **Routing fix + Promote (rider)** | The report's load-bearing premise is **factually wrong on two counts**, verified against the 392-col Lance source (v19): (1) there is **no `reason_for_modification` column anywhere** — USAspending renames the FPDS `reasonForModification` element to **`action_type_code`**, which is already on 11 serving tables incl. all four the report named as omitting it; (2) "the A–Y codes do not distinguish a novation" is false — the sidecar's own `action_type_vocab` already ships `J`='NOVATION AGREEMENT', `S`='CHANGE PIID', `T`='TRANSFER ACTION' (`M`='OTHER ADMINISTRATIVE ACTION' is the admin mod they separate from). The capability **serves today at 0.94 s** — that's a routing-guide fix, not a build. The one genuinely unserved leg is predecessor→successor identity, which FPDS carries in no column; **promoted** as a rider `gtm_award_novation_events` (1/(J/S/T action) · 88,092, aggregate, local off `txn_rows_by_award` via a `lag()` window — NOT a self-join). After: change-of-hands since 2024-07-01 = **3,948 events / 1,508 firms / 1,439 with `is_uei_change` in 9.4 ms** (was ~10 s predecessor-linkage at query time; the SAM-delta proxy the session used over-counted at ~2,936 firms — exactly the accuracy failure the report predicted). |

The `reason_for_modification` → `action_type_code` correction and the J/S/T gloss ship to `QUERY_SIDECAR_AGENT_GUIDE.md` §4. Rider merged in PR #1337; the routing capability needed no build. Artifact 68.37 → 73.45 GiB; build 36.8 min.
