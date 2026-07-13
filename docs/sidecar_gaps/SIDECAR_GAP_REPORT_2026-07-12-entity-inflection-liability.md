# Sidecar Gap Report — 2026-07-12 — entity-inflection-liability + award-doc-coverage

- **/healthz artifact stamp:** `query-sidecar/query_sidecar_20260712T224718Z.duckdb` (85 tables)
- **Session topics:** (1) PDF/solicitation-document coverage for a 266-row active-award handoff
  set; (2) 120-day entity inflection & liability snapshot (JV formations, new-CAGE, NAICS profile
  expansion, sub→prime, obligation spike). The inflection pull was answered mostly ON the sidecar;
  three data questions fell to Lance or degraded — recorded below. **Demand only, no solutions.**

---

## Gap A — Award → solicitation → attachment/PDF/extracted-text coverage for an award set

1. **Intent** — For 266 active prime awards (handoff CSV), do we hold any SAM solicitation
   *document* content — attachment links, downloaded PDF bytes, extracted text — reachable by
   joining Award → FPDS solicitation number (own + parent-IDV) → SAM solicitation notice →
   attachment manifest → download ledger → extraction state? Report match-rate honestly.
2. **Why not the sidecar** — `missing table`. None of the attachment substrate is in the artifact:
   `sam_opps_attachment_manifest*` (Stage-2 links), `sam_attachment_files` (Stage-3 download
   ledger), `sam_attachment_extraction` (Stage-4 text state), and the `sam-gov-opps/{active,
   archived}` universe. Catalog probe (`/api/v1/tables`, grep attach|manifest|extract|opps) → **0
   matches**. `award_descriptions` carries the join keys (`solicitation_identifier`,
   `solicitation_date`) but not the attachment/bytes/text layer.
3. **What I ran instead** — pylance over R2 (doppler `core-x/prd` R2 creds), DuckDB for the joins.
   Filter-pushdown scans, only the needed columns: universe active+archived
   (`solicitation_number, notice_id, notice_type`), union of 9 manifest shards
   (`solicitation_number, resource_id, file_name, mime_type`), ledger (`resource_id, status,
   size_downloaded`), extraction (`resource_id, state, text_chars, n_chunks`). Predicates:
   `solicitation_number IN (68 sols)`, `resource_id IN (209)`.
4. **Cost** — three interactive scripts, tens of seconds wall. Heaviest scan: archived universe
   2.84M rows filtered to 68 sols; 9 manifest shards ≈ 1M citation rows. Returned: 373 universe
   rows / 5,122 citations / 44 ledger rows / 95 extraction rows. Highly reductive (billions→
   thousands) but each scan is a fresh remote Lance read.
5. **Recurrence** — recurring shape. "Do we have readable documents for this award/UEI set, and at
   what coverage rate" is a standing question against any handoff list; the award→sol coverage
   *rate* is analytical even though the bytes/text live on Lance.

## Gap B — "New CAGE code assigned to an existing UEI within trailing 120 days"

1. **Intent** — Flag existing UEIs assigned a *new/additional* CAGE code in the last 120 days
   (re-CAGE / structure change / new division).
2. **Why not the sidecar** — `wrong grain` + `missing column(s)`. `gtm_sam_entities` /
   `sam_master_entities` hold exactly ONE `cage_code` per UEI as of the snapshot, no
   CAGE-assignment date, no historical CAGE list. `last_update_date` / `activation_date` exist but
   are record-level, not CAGE-specific. Detecting a *new* CAGE needs a CAGE-assignment date or a
   prior SAM snapshot to diff — neither is in the artifact.
3. **What I ran instead** — nothing answered it honestly; reported unobservable. No Lance fallback
   (the SAM SoR would need two dated snapshots to diff — a pipeline question, not a query).
4. **Cost** — n/a (not runnable).
5. **Recurrence** — recurring. "Structural change in the last N days" is the core shape of any
   inflection/liability monitor.

## Gap C — "Existing UEI added a high-liability NAICS to its ACTIVE SAM PROFILE within 120 days"

1. **Intent** — Entities that *declared* a new high-liability NAICS (236220 / 541512 / 561612) on
   their SAM registration within the trailing 120 days (declared-capability expansion, not
   demonstrated award activity).
2. **Why not the sidecar** — `missing column(s)` + `wrong grain`. `gtm_sam_entities.naics_codes`
   is a flat `VARCHAR[]` of the current declaration with no per-code add date; single snapshot, no
   history. The declared-profile add is unobservable. Delivered a **demonstrated-activity proxy**
   (first FPDS action in the target NAICS within 120d) — a related but different question.
3. **What I ran instead** — `gtm_txn_events_slim` filtered `naics_code IN ('236220','541512',
   '561612')`, `GROUP BY uei, naics_code HAVING min(action_date) >= DATE '2026-02-26'`, joined
   `gtm_entity_behavior_rollup.first_action_date < window` to keep pre-existing entities.
4. **Cost** — 2.0s + 2.7s on serving; scanned 108M-row spine filtered to 3 NAICS, returned 161
   entities.
5. **Recurrence** — recurring. Declared-vs-demonstrated is a standing distinction; the declared
   side needs a dated SAM NAICS-declaration history (or snapshot diff) to be answerable.

---

**Rank (recurrence × cost):**

1. **Gap A** — recurring × highest cost (multi-dataset remote Lance scan incl. 2.84M-row universe
   + 9 manifest shards every time an award/UEI list needs a document-coverage read).
2. **Gaps B + C (shared root)** — recurring × low query cost but the demand is blocked by one
   structural absence: **the sidecar holds no change-dated / time-series view of SAM entity profile
   fields (CAGE, declared NAICS, business types, entity structure)**. Every "structural change in
   the last N days" question collapses to this one missing capability.
