# Sidecar Gap Report — 2026-07-09 — on-call market brief / diagnostic build session

**Date:** 2026-07-09 (session span 2026-07-08 → 07-09)
**Sidecar artifact at compile:** `query-sidecar/query_sidecar_20260709T193639Z.duckdb` (47 tables; the artifact grew twice DURING the session — 40 → 42 → 47 — so several entries below were partially served mid-session)
**Session topic:** testing-page v35–v36 (Market Opportunity), new-approach chains (on-call Market Memo/Brief v1–v7, diagnostic v0–v3), market-brief population methodology, Bowler Pons award/subaward diagnostics.

---

## Entry 1 — Ladder/population rebuild ran on Lance while the tables sat in the sidecar

1. **Intent** — Reproduce the audience-ladder Cut-1→Cut-5 population (the 200; later the 2,703) to read live state and market composition.
2. **Why not the sidecar** — `didn't know it was there`. `gtm_prime_combo_lanes`, `gtm_prime_pop_lanes`, `gtm_prime_farmout_combo_lanes`, `usaspending_subaward_canonical`(slim) were all in the artifact; the session rebuilt the ladder by pulling all four from Lance because the established probe pattern predated the sidecar and no routing check happened.
3. **What I ran instead** — `lance.dataset(...).to_table()` on all four datasets (combo lanes: uei/naics/psc/obl cols; subaward canonical: 9 cols, full table) + local DuckDB, per run of `testing-page/_build/market_opportunity.py`.
4. **Cost** — ~3–5 min per run; run ≥5 times before a local cache was built. Millions of rows pulled per run vs a 200-row survivor set. (Direct evidence of the delta: the identical population derivation later ran sidecar-side in **504 ms**, `market_brief_population.py`.)
5. **Recurrence** — recurring (every subject re-derivation) — resolved behaviorally mid-session after the operator's routing instruction; recorded here because the cost was real and the fix is guide/skill visibility, not schema.

## Entry 2 — Live-state read of a derived prime population (PoP-live, ordering windows, action classes, plans, county collapse)

1. **Intent** — For the Cut-5 200: awards in period of performance today; parent IDV ordering windows open; new orders / A / C / G actions in trailing windows; subcontracting-plan flags on live awards; all of it collapsed to the record's counties.
2. **Why not the sidecar** — `missing column(s)` / `wrong grain` at the time (40-table artifact): no single surface carried award-grain `period_of_performance_current_end_date` + `ordering_period_end_date` + `parent_award_id_piid` + `pop_county_fips` + `subcontracting_plan` + action classes for an arbitrary UEI population. (Mid-session the combo layer + `usaspending_fpds_prime_award_state` + `gtm_open_awards` landed most of this.) Residual gap now: an award-grain "carries a subcontracting plan (latest action)" flag for arbitrary populations — plan lives at txn grain (`gtm_txn_events_slim`/`txn_events_combo`) and on `gtm_open_awards` (open awards only); closed/full-population award-grain plan state still requires a latest-txn window computation.
3. **What I ran instead** — Lance pull of `usaspending_fpds_canonical_txn` filtered `recipient_uei IN (200)` → 1,925,706 txns × 18 cols → local award rollup with pinned latest-txn tie-break (`market_opportunity.py` parts 2–10; persisted to a local `mo_cache.duckdb` to stop paying the pull).
4. **Cost** — ~3–5 min per run, multiple runs (incl. one full re-run after a crash at the final step); 1.93M rows in vs a few hundred aggregate rows out. Operator escalation over the latency mid-session.
5. **Recurrence** — recurring — this is the Market Opportunity tab read, intended to re-run per target entity.

## Entry 3 — Award and subaward description text (the diagnostic surface)

1. **Intent** — (a) Every Bowler prime award with the contracting office's `transaction_description` (base action); (b) every subaward with the paying prime's FSRS `subaward_description` + the prime award's `prime_award_base_transaction_description`. Feeds the diagnostic history tabs and is the declared input to the per-engagement question-generation pipeline.
2. **Why not the sidecar** — `missing column(s)` by design: descriptions are explicitly excluded from the artifact (guide §2 routes "full transaction row detail (descriptions…)" to Lance).
3. **What I ran instead** — Lance pulls: `usaspending_fpds_canonical_txn` filtered one UEI (14 cols incl. `transaction_description`) and `usaspending_subaward_canonical` filtered one subawardee UEI (desc + agency cols) — `bowler_award_history.py`, `bowler_subaward_descriptions.py`.
4. **Cost** — ~2–3 min per pull; small row counts out (44 awards / 41 subawards); ran 3×.
5. **Recurrence** — recurring, hard: every diagnostic file (per target) needs both description sets; the on-call product plans one per prospect. This is the highest-frequency future shape in the session.

## Entry 4 — Solicitation identifiers/dates for the active-award PDF handoff

1. **Intent** — For 266 active in-footprint awards: `solicitation_identifier` (award and parent-IDV grain) + `solicitation_date`, as join keys for a solicitation-PDF matching agent.
2. **Why not the sidecar** — partly `didn't know it was there` (`usaspending_fpds_prime_award_state` carries `solicitation_identifier`; unverified at the time), partly `missing column(s)` (`solicitation_date` absent from the artifact; parent-grain sol id requires the parent row join). *[Verified 2026-07-09, demand-side check: `solicitation_identifier` IS on the serving `usaspending_fpds_prime_award_state`; `solicitation_date` confirmed present on the Lance canonical, absent from the artifact.]*
3. **What I ran instead** — Lance pull of `usaspending_fpds_canonical_txn` filtered to the 52 holder UEIs, 5 cols (`recipient_uei, award_id_piid, solicitation_identifier, solicitation_date, action_date`) — `active_awards_pdf_handoff.py`.
4. **Cost** — ~1–2 min; hundreds of thousands of rows in vs 266 rows out.
5. **Recurrence** — recurring if the PDF-lookup handoff becomes per-target-set practice.

## Entry 5 — County-scoped market story across ALL primes (footprint class trends + active mix)

1. **Intent** — For the record's 8 counties, every prime and agency: obligations by PSC class by FY (FY21→FY26), active-award mix today, construction-vs-sustainment winner overlap.
2. **Why not the sidecar** — at the time: `missing column(s)`/`missing sort` — no county-sorted txn surface with class + PoP-end. (`txn_events_combo_by_geo` shipped mid-session and appears to cover this shape now.)
3. **What I ran instead** — Lance pull of `usaspending_fpds_canonical_txn` filtered `pop_county_fips IN (8) AND action_date >= FY21` → 1,501,859 rows × 11 cols → local aggregates + `fp_footprint.duckdb` cache (`footprint_market_story.py`).
4. **Cost** — ~2–3 min pull; 1.5M rows in vs ~40 aggregate rows out.
5. **Recurrence** — recurring (per-target footprint story; geo collapse is a standing pattern in every surface built this session).

## Entry 6 — Subawardee designation pulse (business-types over the 5-yr subawardee universe)

1. **Intent** — Of firms winning subawards in the past 5 years (by min-$ bands): what share carry veteran/SDVOSB/women-owned/HUBZone/8(a)/minority designations on the FSRS record.
2. **Why not the sidecar** — `didn't know it was there` / unverified: went Lance-direct without checking whether `subaward_canonical_slim` carries `subawardee_business_types` (36-col slim; presence unconfirmed either way at compile time). *[Verified 2026-07-09, demand-side check: `DESCRIBE subaward_canonical_slim` → `subawardee_business_types` ABSENT from the artifact; column confirmed present on the `usaspending_subaward_canonical` Lance schema — reclassify as `missing column(s)`.]*
3. **What I ran instead** — Lance pull of `usaspending_subaward_canonical`, 4 cols (`subawardee_uei, subaward_amount, subaward_action_date, subawardee_business_types`), full table → firm-grain flags (`subawardee_designation_pulse.py`).
4. **Cost** — ~2–3 min; millions of rows in vs 5 band rows out.
5. **Recurrence** — plausible recurring (designation-based supply-side pulses tie to the routing/designation thesis), not yet repeated.

## Entry 7 — Awarding-vs-funding agency split (agency-lens market brief)

1. **Intent** — For the gated market (anchor families × Navy `1700` + DLA `97AS`, FY23→FY25): obligations split by funding agency vs awarding agency — who pays vs who signs — the share of $ where funding ≠ awarding, cut by instrument and FY. This is section 02 of the on-call "Market Analysis (Agency-Lens)" tab (spec complete, build queued); operator-directed same-day capture.
2. **Why not the sidecar** — `missing column(s)`: no funding-agency fields anywhere in the artifact — verified via `DESCRIBE usaspending_fpds_prime_award_state` (awarding codes only) and the `txn_events_combo` catalog (awarding_agency_code / awarding_sub_agency_code only). Source columns verified present on the Lance canonical (`usaspending_fpds_canonical_txn`): `funding_agency_code`, `funding_agency_name`, `funding_sub_agency_code`, `funding_sub_agency_name` (plus `funding_office_code/name`).
3. **What I ran instead** — nothing yet: demand captured at design time, before any fallback. The tab section is specified and blocked on this cut; the fallback shape would be a Lance canonical scan over the gated award set.
4. **Cost** — none paid yet; prospective = minutes-class canonical pull per target, recurring per prospect.
5. **Recurrence** — recurring, hard: every agency-lens brief (one per prospect in the on-call funnel); first consumer is the in-flight on-call v8 build cycle.

---

## Ranking (recurrence × cost)

1. **Entry 3 — descriptions** (every future target, excluded by design today; per-target Lance round-trip is the standing tax on the diagnostic + question pipeline)
2. **Entry 2 — population live-state** (largest single cost this session; mostly landed mid-session, residual = award-grain plan state for arbitrary/closed populations)
3. **Entry 5 — county-scoped market story** (heavy pull; likely already served by `txn_events_combo_by_geo` — needs a routing/verification note in the guide)
4. **Entry 1 — routing miss on existing tables** (zero schema work; pure guide/skill visibility; cost delta measured at ~400×)
5. **Entry 4 — solicitation keys** (small, real, recurs with the PDF workstream; `solicitation_date` is the only truly missing column)
6. **Entry 6 — designation pulse** (cheap-ish, unproven recurrence; may already be servable — verify slim's columns first)

**Ranking amendment (2026-07-09, demand-side):** Entry 7 enters at **#1** — recurring per-prospect AND blocking the in-flight agency-lens tab build. Entry 3's award-grain demand has since been served by `award_descriptions` (#1098), leaving per-action text as its residual. Entry 6 reclassified to `missing column(s)` (verified absent; see entry annotation).

---

## Disposition (gap-pass-1, 2026-07-09 — covers Entries 1–7 incl. the demand-side amendments)

| # | Verdict | What shipped |
|---|---|---|
| 1 | **Routing** — no build | Guide/skill/CLAUDE.md routing stack shipped mid-session; the 400× delta (minutes → 504 ms) is the measured evidence. No schema work |
| 2 | **Promoted (residual)** | `award_plan_state` — award-grain latest-action subcontracting-plan flag (`arg_max(subcontracting_plan, action_date)`, sorted award key). Closed/full-population plan reads = one pruned join |
| 3a | **Served** | Award-grain base descriptions already in the compile-stamp artifact (`award_descriptions`, #1098) — matches the demand-side amendment. Per-ACTION `transaction_description` stays gated (≈2× growth, no workload yet) |
| 3b | **Promoted** | `prime_award_base_transaction_description` added to `subaward_canonical_slim` (both sort copies) — both description sides on one row |
| 4 | **Promoted** | `solicitation_identifier` + `solicitation_date` added to `award_descriptions`; parent-IDV sol-id = join via `usaspending_fpds_prime_award_state.parent_award_id_piid` (pattern, not a table) |
| 5 | **Served** | `txn_events_combo_by_geo` covers the county market story (the guide's county zoom-in example is this exact shape) |
| 6 | **Promoted** | `subawardee_business_types` added to `subaward_canonical_slim` — demand-side reclassification to `missing column(s)` confirmed absent-in-artifact/present-on-Lance; rides an existing 1.3M-row projection for ~zero cost |
| 7 | **Promoted (entered at #1)** | `funding_agency_code` + `funding_sub_agency_code` added to `txn_events_combo` (+`_by_geo` inherits) — who-pays vs who-signs cuts at any grain; names resolve via the existing `agency_vocab`/`agency_sub_vocab` joins (shared code space). Unblocks the agency-lens tab section 02 |

Artifact: v7, **48 tables** (adds `award_plan_state`; widens `award_descriptions` +2,
`subaward_canonical_slim`/`_by_sub` +2, `txn_events_combo`/`_by_geo` +2). 48/48 parity
gates the publish; serving hot-swaps on the refresh hook.
