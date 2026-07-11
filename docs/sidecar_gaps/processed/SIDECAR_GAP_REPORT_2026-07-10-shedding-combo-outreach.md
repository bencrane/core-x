# SIDECAR GAP REPORT — 2026-07-10 — shedding-combo outreach session

- **Date:** 2026-07-10
- **Sidecar artifact:** `query-sidecar/query_sidecar_20260710T200418Z.duckdb` (healthz ok, 61 tables)
- **Session topic:** award-emission trends (standalone vs orders, DoD-administered/funded); govt-wide screen of high-inflow/high-shed prime NAICS×PSC combos; sub populations per combo + their own prime PSC signatures; PoP-density feasibility funnel for capacity-question outreach; work_summary text assessment for the outreach letter slot.

---

## Entry 1 — NAICS×PSC natural-language layers (work_summary / what_was_done)

1. **Intent** — "What do these codes correspond to — title and work_summary value?" then again for the subject's NAICS 561621 pairings, then a 14-PSC × ~40-row sample to assess what LLM cleaning the `work_summary` text needs before it can fill the capacity-question slot ("Are you set up to take on additional ___ in {region}?").
2. **Why not the sidecar** — missing tables: `naics_psc_labor_profile` (16,291 rows: work_summary, is_labor_play, OEWS mapping, psc_full_description) and `naics_psc_deliverable` (20,998 rows: what_was_done, work_type, regime, confidence, review_status) are Lance-only. PSC/NAICS *titles* are served (`psc_reference`/`v_psc_names`, `v_naics_names`); the sentence/phrase layers are not.
3. **What I ran instead** — three pylance reads of `s3://data-sink/active/naics_psc_labor_profile/` (+ one of `naics_psc_deliverable/`) via doppler R2 creds, filtered on `psc_code IN (...)` (BTREE), columns `naics_code, psc_code, work_summary` (+ `what_was_done, work_type`).
4. **Cost** — ~20–40 s wall each (uv cold-start dominates; the filtered read itself is sub-second; rows returned 3–40 per call vs 16k/21k table sizes).
5. **Recurrence** — recurring and rising. Every outreach-rendering or on-page-language task joins these layers onto sidecar-resolved code sets; this session alone needed it three times, and the identified next build (a `capability_phrase` cleaned-column pass over all 16,291 rows) will make the layer a standing join target for the letter renderer. Same demand already recorded in `SIDecar_GAP_REPORT_2026-07-10-funding-tab-pdl-match.md` (what-was-done entry) — second independent session hitting the identical gap.

---

## Footer — ranking

Single gap. Rank: **high** — recurring across two sessions (this + funding-tab-pdl-match), cheap per hit but on the critical path of every natural-language rendering task, and the tables are small (16k/21k rows, reference-shaped, BTREE-keyed on the exact join key the sidecar already serves everywhere else).

Non-gap notes for the record (no entries): sub-PoP county (3-digit-in-state) vs prime-PoP (5-digit FIPS) join needed an in-query state-FIPS stitch — served, but the format mismatch cost one dead query; sidecar parse quirk confirmed: bare column alias after `)` fails, `AS` required.

---

## Disposition (gap-pass-5, 2026-07-10)

| # | Verdict | What shipped |
|---|---|---|
| 1 | **Promoted (as the combo-language layer)** | 4 tables, all full-width, (naics_code, psc_code)-sorted: `naics_psc_labor_profile` (16,291 — work_summary, labor-play, OEWS), `naics_psc_deliverable` (20,998 — what_was_done, work_type, regime), + sweep riders `naics_psc_labor_profile_categories` (54,235 — the ranked SOC/SCA "additional ___" candidates the capacity letter needs) and `naics_psc_vertical_map` (279 — equipment_intensity dial). `naics_psc_labor_dim` deliberately skipped: flattened duplicate of profile⋈categories rank-1 |
| — | **Routing fixes** | Guide §6: the `) AS alias` parse quirk and the 3-digit vs 5-digit county-FIPS stitch (this report's non-gap notes) are now documented caveats |

Demand corroboration: the identical gap was independently appended to the
funding-tab-pdl-match residual by a second session the same day (its Entry 2 is
dispositioned in that processed file's addendum). Second consumer named at compile
time: the `capability_phrase` cleaned-column pass will target the served layer.

Artifact: `query_sidecar_20260711T011505Z.duckdb` — 67 tables, 45.15 GiB, 67/67 parity (ops ledger run 19; run 18 aborted mid-build on a propagated client kill — parity/pointer held, nothing published). Smoke: the 4-layer join (profile + deliverable + rank-1 category + vertical map) on the session subject NAICS 561621 — 14ms, vs ~20-60s per Lance pull recorded across the two demanding sessions.
