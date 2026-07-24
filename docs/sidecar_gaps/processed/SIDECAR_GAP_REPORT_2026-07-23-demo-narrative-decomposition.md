# Sidecar Gap Report — 2026-07-23 — demo-narrative decomposition probes

**Serving artifact at session time:** `query-sidecar/query_sidecar_20260722T032457Z.duckdb`
(built 2026-07-22T03:24:57Z, 113 tables)
**Session topic:** building the gc operator sales-demo narrative (⌘B walk) — 5-yr historical
aggregates, HQ-vs-PoP geography deltas, the payroll/equipment dollar decomposition, and the
Clay gap-judging input build. Most probes served warm; the entries below are the questions
that could NOT be answered on the sidecar (or failed on it) and what was run instead. The
operator's stated goal for these shapes: on-screen instantaneous.

---

## Entry 1 — distinct places of work over the 5-yr window

1. **Intent:** "How many distinct places of work (PoP zip5s) received federal contract work
   in FY2021–FY2025?" — the Arc-1 'work happens everywhere' number.
2. **Why not the sidecar:** wrong grain / memory-infeasible on serving. The shape requires
   `DISTINCT award_key` over `gtm_txn_events_slim` (33.1M rows in-window) joined to
   `usaspending_award_pop_centroids` (30.7M). Two attempts OOMed the serving instance
   (1.1 GiB): the wide `DISTINCT award_key, uei` CTE, then the single-column
   `DISTINCT award_key` variant. No pre-aggregated award→zip5 (or window→distinct-places)
   mart exists.
3. **What I ran instead:** the all-time ceiling on the sidecar itself —
   `SELECT count(DISTINCT zip5) FROM usaspending_award_pop_centroids WHERE country_code='USA'`
   (23,801 zips / 29.8M awards, 331 ms) — and shipped the demo line as "20,000+" (a bound,
   not the asked number). The 5-yr-window figure remains uncomputed.
4. **Cost:** two failed attempts (~35 s wall incl. retries) + the proxy; the actual question
   is still open — answered by bounding, not by measurement.
5. **Recurrence:** recurring — every re-bake of the Arc-1 numbers wants this (and its FY23–25
   variant), and region-scoped versions of the same distinct-places shape are coming for
   Arc 3.

## Entry 2 — non-local share of the active universe (award ⋈ PoP ⋈ HQ)

1. **Intent:** "Of active-award dollars, what share is performed in a different state than
   the holder's HQ?" — plus the per-state work-done / HQ-alloc / imported-share table (the
   pink-dot 'import ratio' layer: MD 38%, PA 62%, DC 76%…).
2. **Why not the sidecar:** it DID serve, but as a 3-way row-level join recomputed from
   scratch (`usaspending_fpds_prime_award_state` × `usaspending_award_pop_centroids` ×
   `gtm_sam_entities`): 20.8 s for the aggregate pass, ~1–3 s per follow-up state cut.
   No materialized award-grain (pop_state, hq_state, obl) mart; the join legs are re-paid
   per question. Same family as Entry 1's OOM — the historical variant of this exact shape
   is what tipped over.
3. **What I ran instead:** the live 3-way join on serving (accepting the 20.8 s), then two
   follow-up GROUP BYs for the per-state table and top-12 concentration.
4. **Cost:** ~25 s total wall across three queries; ~170K award rows × two 30M-row-class
   join probes each pass. Returned: 1 aggregate row + 18 state rows + 1 concentration row.
5. **Recurrence:** recurring and growing — this is the demo's geographic thesis. The Arc-3
   region cut re-asks it at radius grain (150/300 mi) per prospect, and the pink-dot layer
   (if built) asks it per state on every bake.

## Entry 3 — ECEC compensation-component decomposition (healthcare share)

1. **Intent:** "What share of total compensation is employer health insurance (and the full
   wages/benefits component split), private industry, latest quarter?" — the healthcare
   sub-slice of the demo's payroll number (7.3% → ≈$75B).
2. **Why not the sidecar:** missing table. `bls_ecec_costs` is a Lance dataset
   (`s3://data-sink/active/bls_ecec_costs/`, 627,050 rows, full CM series universe) that
   was never promoted to serving; the sidecar carries only the composed `naics_labor_share`
   scalar. First attempt on the sidecar → Catalog Error.
3. **What I ran instead:** Lance-direct read via pylance + local DuckDB (doppler R2 creds),
   pulling the full 627K-row table client-side, then four iterative queries to isolate the
   flagship series (the series-family key structure — ownership × industry × occupation ×
   subcell × datatype × estimate — took schema archaeology; the winning filter was the
   `CMU2__0000000000P` series-id pattern).
4. **Cost:** ~6 min wall across five attempts (incl. one R2 region-config failure and one
   pytz dependency stumble); 627K rows scanned repeatedly for 27 result rows.
5. **Recurrence:** recurring — every demo bake and the walkthrough page cite the component
   split; per-industry burden components (construction vs manufacturing healthcare share)
   are the obvious next asks and hit the same wall.

---

## Ranking (recurrence × cost)

1. **Entry 2** — the geographic thesis of the whole sales narrative; re-asked per prospect,
   per region, per bake; every ask re-pays 30M-row-class joins (and its historical variant
   is Entry 1's OOM).
2. **Entry 1** — same join family at the heavier window; currently UNANSWERED (shipped as a
   bound); blocks exact Arc-1 copy.
3. **Entry 3** — cheap once known, but currently requires leaving the sidecar entirely,
   with credentialed Lance access and client-side pulls, for numbers the demo quotes.

**Context note (demand signal, not entries):** this session also landed 14 new Lance
datasets none of which serve warm yet — the industry-cost-structure batch (KLEMS, BEA fixed
assets, ACES cells, IRS SOI cells, QCEW 86.3M, TFP detailed/major, productivity tables/cells,
BDS pending) and the BEA IO Use family (detail, summary-annual, SUT concordance,
contingent-labor intake). The demo's remaining slices (M/E/S, equipment spend rate, IT,
contingent labor) will draw on these; where those questions get asked and answered off-sidecar,
they'll appear in the next report.

---

## Disposition (sidecar-gaps Mode 2, 2026-07-24 — artifact `query_sidecar_20260724T044059Z`, ledger id 46)

Probe-verified before build. Serving after-numbers measured on the new artifact.

| Entry | Verdict | What shipped · before → after |
|---|---|---|
| 1 — distinct places of work FY21-25 | **Promoted** | `pop_place_fy` (1/(fy,pop_state,county_fips,zip5) · 485,766 rows, aggregate, local off `txn_events_combo`). `count(DISTINCT pop_zip5) WHERE fy 2021-25` = **23,296 zips / 3,101 counties** — the exact number the report could only bound as "20,000+". **Before: two serving OOMs → after: 15.8 ms.** |
| 2 — non-local share (award ⋈ PoP ⋈ HQ) | **Promoted (correctness, not speed)** | `award_geo_state` (82.87M, EXACT parity, zero R2 read). The report's 20.8 s did not reproduce (1.7 s warm) — but the query-time centroid route silently sampled **40.6%** of the active universe (topology-biased: vehicles 11% covered), so the published import ratios (MD 38% / PA 62% / DC 76%) were computed on a non-random 40% sample. The mart derives PoP from `txn_events_combo` at **100% award-key coverage** (62.4% county fill, 1.5× the centroid route). After: **160,971 active awards, $2.76T work value, $561B imported (20.3% non-local) in 1.63 s, single-table on full coverage.** ⚠ **The mart moves the published ratios — the demo owner must treat the prior MD/PA/DC figures as provisional.** |
| 3 — ECEC compensation-component decomposition | **Promoted** | `bls_ecec_costs` (627,050, EXACT) + `bls_ecec_burden` (321, EXACT), plain copies. Series key already decoded into columns. **Before: ~6 min credentialed Lance-direct + client-side pull → after: Health insurance = 7.3% of total comp in 18.6 ms.** Mandatory consumer predicates (pin area / datatype / year+period / hierarchy level) documented in `QUERY_SIDECAR_AGENT_GUIDE.md`. |
| context note — BEA family | **Promoted (reduced)** | The 14-dataset industry-cost-structure batch's demanded slices ship as 4 small tables (`bea_bls_klems`, `bea_contingent_labor_intake`, `bea_io_use_summary_annual`, `bea_naics_concordance`) — see the `bea-io-purchased-services` dossier and PR #1337. KLEMS service-share of gross output (NAICS 5415) = 25.6% in 10 ms. QCEW-scale members stay gated. |

**Correctness disclosures owed (from the probe, on the record):** (1) the import ratios above; (2) per-field `arg_max` is deliberate — each geo field pins the latest txn CARRYING it (coverage-maximizing), NOT strict latest-txn-per-award. Both documented in the manifest comment and the agent guide. Merged in PR #1337; artifact 68.37 → 73.45 GiB; build 36.8 min; 126 tables, zero parity mismatches.
