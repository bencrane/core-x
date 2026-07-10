# Sidecar Gap Report — 2026-07-09 — on-call Agency-Lens tab build (v8–v12)

**Date:** 2026-07-09 (evening session)
**Sidecar artifact at compile:** `query-sidecar/query_sidecar_20260709T214133Z.duckdb` (48 tables; the artifact rebuilt mid-session from `...T193639Z`/47 — most build queries ran against the earlier artifact)
**Session topic:** on-call tab 05 "Market Analysis · Agency Lens" build cycles v8→v12 (Navy 1700; ring = PSC groups J/N/42 any-NAICS; position/active/sub-out-history ladder; active-award value split; geographic footprint map).
**Predecessor file:** `SIDECAR_GAP_REPORT_2026-07-09-oncall-market-brief.md` (morning session; Entry 7 there covers the funding-agency columns — not re-filed here).

Session note: every query in this session ran through the sidecar (`POST /api/v1/sql`; zero Lance scans). The entries below are the two honest demand signals that remain: one degraded answer, one served-but-slow recurring shape.

---

## Entry 1 — Place-of-performance country code (overseas vs unstated split)

1. **Intent** — Of the 1,082 active Navy awards in PSC groups J/N/42, 102 awards ($359M) carry no U.S. state place of performance. Split them: performed overseas (country ≠ USA — expected bulk: ship repair / equipment maintenance at forward fleet locations) vs genuinely unstated. Needed for the geographic-footprint map legend to say what the bucket actually is, and for any "share of this market performed overseas" statement on a call.
2. **Why not the sidecar** — `missing column(s)`: no place-of-performance country field anywhere in the artifact. `txn_events_combo` carries `pop_state`/`pop_county_fips`/`pop_county_name` only; `usaspending_fpds_prime_award_state` carries no PoP fields; `usaspending_award_pop_centroids` is US-only (state/zip5). Source columns exist on the Lance canonical (`usaspending_fpds_canonical_txn`: `primary_place_of_performance_country_code`, `..._country_name`).
3. **What I ran instead** — nothing: answer degraded. The on-page legend was written as "no U.S. state place of performance — overseas or unstated; not shown," and the operator was told the split requires a Lance probe.
4. **Cost** — none paid yet; prospective = per-question canonical pull (minutes) vs ms if the country code rode the combo fact.
5. **Recurrence** — recurring: every geographic read of a Navy (or other overseas-heavy) market hits this bucket; the map section is now a standing feature of the agency-lens tab, one per prospect.

## Entry 2 — Award→parent ordering-window resolution (the position/active ladder shape)

1. **Intent** — The tab's core ladder, re-computed at every ring change: (a) firms holding an indefinite-delivery contract with ordering period open at snapshot that has carried Navy orders in given PSC groups ("position"); (b) of them, firms with ≥1 active award ("active"); (c) of them, firms with subawards reported against their prime awards, total and in-ring ("sub-out history"); plus seat counts and set overlaps.
2. **Why not the sidecar** — `missing table` / `missing sort (too slow unpruned)`: served by the sidecar, but each statement requires a double self-join on `usaspending_fpds_prime_award_state` (83M rows: order rows → `parent_award_key_resolved` → parent rows for ordering-window state) seeded by a distinct-award scan of `txn_events_combo`, then set algebra over recipient UEIs. No precomputed award→parent-window surface or recipient-grain position rollup exists; `gtm_prime_vehicle_lanes` (16k, uei-sorted) does not carry parent window state per PSC context.
3. **What I ran instead** — the sidecar itself, repeatedly: ~10 statements of this shape during v8–v12 at 18–23s each (`elapsed_ms` 18,320–22,761 measured), ~3–4 min cumulative wall inside interactive build loops.
4. **Cost** — served, but ~20s per statement × every ring/dial variation; the shape re-runs per prospect (every agency-lens tab build) and per ring adjustment within a session.
5. **Recurrence** — recurring, hard: this ladder is now the structural spine of the on-call tab; one per prospect, several variations per session.

---

## Ranking (recurrence × cost)

1. **Entry 2 — position/active ladder shape** (spine of a per-prospect deliverable; ~20s × many statements per session today; a recipient-grain "position state" surface or award→parent-window precompute turns the whole ladder ms-class)
2. **Entry 1 — PoP country code** (one column on the combo fact; unblocks the overseas/unstated split every geographic section needs; currently a standing degraded answer on-page)

---

## Disposition (gap-pass-2, 2026-07-09)

Schema probes preceding the verdicts (probe-never-guess): `primary_place_of_performance_country_code` + `pop_country_name` confirmed on the Lance canonical (v19; the report's guessed `..._country_name` does not exist — the name column is `pop_country_name`); `ordering_period_end_date` confirmed on the canonical at txn grain (4,521,876 rows carry a value), absent from `usaspending_fpds_prime_award_state`; `parent_award_key_resolved` semantics measured on serving — flag `resolved` = 64.4M true child orders, `self` = 17.1M standalones + 0.96M vehicles pointing at themselves, `dangling` = 396k (parent join must gate on `parent_match_flag='resolved'`).

| # | Verdict | What shipped |
|---|---|---|
| 1 | **Promoted** | `pop_country_code` (ISO3) added to `txn_events_combo` (+`_by_geo` inherits) — overseas vs unstated split at any grain. `country_vocab` (code→majority name, same dedup rule as the agency vocabs — collapses "UNITED STATES OF AMERICA" variants) rides along for map-legend/display naming |
| 2 | **Promoted** | Two-part: (a) `award_ordering_windows` — award-grain latest-action `ordering_period_end_date` (`arg_max` by action_date) off the canonical, 982,193 rows, sorted by award key; (b) `usaspending_fpds_prime_award_state` widened at build with own `ordering_period_end_date` + the RESOLVED parent's window state (`parent_ordering_period_end_date`/`parent_current_end_date`/`parent_potential_end_date`) **and attribution** (`parent_awarding_agency_code`/`parent_awarding_sub_agency_code`/`parent_idv_type_code`/`parent_award_type_code`/`parent_type_of_set_aside_code`) — 1:1 LEFT JOINs gated on `parent_match_flag='resolved'`; exact row-count parity preserved. The ladder's per-query double self-join on the 83M table is precomputed once at build — position/active reads and "whose vehicle / what instrument" are one pass. Measured: 18.3–22.8s baseline (report) → **2.3s warm / 6.0s cold** via `gtm_position_orders` (same 1,455-firm result, cross-validated against the award_state one-pass path) |
| + | **Adjacency rider** | `type_of_set_aside_code` added to `txn_events_combo` (+`_by_geo`) — the set-aside dial on the portrait fact |

**Policy note (operator-directed, standing):** the demand-evidence gate governs *new tables / grains / sort copies* — structural growth with recurring cost. Column-grain adds that ride a join or scan the build is already paying for are taken **opportunistically** whenever the adjacent question is foreseeable ("what will the consumer of this feature ask in the same session?"). Waiting for a gap entry to add a free column to a committed build is a wasted rebuild. The parent-attribution and set-aside columns above shipped under this rule.

Residuals: a third combo copy sorted agency-first stays gated — the agency-anchored seed scan measured **1.6s warm** post-build, acceptable. A recipient-grain position rollup stays gated (rings vary per session). The award-grain **open-window substrate** (`gtm_position_orders`) DID ship later in this same build cycle, forced by post-v8 measurement — see the gap-pass-3 disposition (`SIDECAR_GAP_REPORT_2026-07-09-allocation-serving-skew.md`).

Build defects hit and fixed in-cycle (5 aborted runs, nothing published, serving stayed on v7 throughout — the parity + pointer ordering held every time): (1) the `ordering_windows` manifest flag had no dispatch branch — generic copy produced 108M rows instead of the ~1M aggregate; `_preflight()` now asserts flag wiring at build start. (2,3,5) Three runs presented as zero-CPU stalls in the parent-window step; two intermediate theories (concurrent-reader deadlock, stream-fed pipeline) were wrong — `py-spy dump --native` on the wedged process showed `PhysicalBlockwiseNLJoin`: the ON clause mixed a probe-side predicate (`parent_match_flag='resolved'`) with the equality key, degrading the plan to an 83M×83M blockwise nested-loop join. Fix: probe-side gates fold into CASE-derived join keys (pure equality → hash join); the fixture now EXPLAIN-gates every join (a 4-row fixture executes a pathological plan instantly — only the plan tells the truth). (4) A local DNS blip killed the non-detached modal client mid-build, taking the healthy remote app with it — builds launch `--detach` (skill + doctrine updated).

Artifact: v8/v9 final — `query_sidecar_20260710T081000Z.duckdb`, **52 tables, 1.195B rows, 41.76 GiB** (one build shared with gap-pass-3, batched per the one-build-per-committed-cost rule). This pass adds `award_ordering_windows` (982,193 rows) + `country_vocab`, widens `txn_events_combo`/`_by_geo` +2 cols and `usaspending_fpds_prime_award_state` +9 cols. **52/52 parity** gated the publish (ops ledger run 15); serving hot-swaps on the refresh hook. Disk watch: artifact 41.53 GiB, blue-green peak ~83 GiB vs the 100 GB Render disk — grow the disk before the artifact passes ~48 GiB.
