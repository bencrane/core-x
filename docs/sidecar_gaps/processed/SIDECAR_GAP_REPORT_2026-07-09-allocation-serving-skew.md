# Sidecar Gap Report — 2026-07-09 — allocation workload + serving-snapshot skew (on-call v13–v14)

**Date:** 2026-07-09 (late session)
**Sidecar artifact at compile:** `query-sidecar/query_sidecar_20260709T214133Z.duckdb` (48 tables, per `/healthz`). NOTE: individual `/api/v1/sql` responses during this session carried BOTH `query_sidecar_20260709T193639Z` and `...T214133Z` stamps at different times, and late-session identical statements returned two different data states (Entry 2) — the header stamp does not describe all queries in this report.
**Session topic:** on-call tab 05 v13–v14 — estimated allocation of subbed-out dollars (top-5 prime-record signature method), SAM-declaration analysis of the unattributed pool, declared-NAICS × award-matrix attribution, comparison-pair build.
**Predecessor files:** `SIDECAR_GAP_REPORT_2026-07-09-oncall-market-brief.md` (morning) · `SIDECAR_GAP_REPORT_2026-07-09-agency-lens-v8-v12.md` (evening; its Entry 2 — the position/active ladder shape — is the substrate this report's Entry 1 extends). Per operator instruction this is a NEW file; predecessors untouched.

Session note: every query ran through the sidecar (zero Lance scans). Entries are served-but-slow shapes, a serving-consistency defect, and two documentation traps that produced (since-retracted) wrong numbers.

---

## Entry 1 — The allocation workload: receiver portfolios + signature allocation over a derived prime set (served, ~20–25 s per statement, run ~15×; final batch 10+ minutes wall)

1. **Intent** — The tab's allocation analyses, recomputed at every methodological turn: (a) the wallet — subawards against a derived prime set's ring awards, windowed, per receiver; (b) each receiver's own-prime PSC lanes, ranked, floored, top-N signature; (c) proportional allocation of received dollars across the signature; (d) aggregations by destination code; (e) subject-slice sums; (f) per-firm concentration bands (top-3/top-5 share of book); (g) SAM-declaration coverage of the unattributed; (h) declared-NAICS × matrix tier-2 attribution; (i) conservation diagnostics.
2. **Why not the sidecar** — `missing table` / `missing sort (too slow unpruned)`: served, but every statement must rebuild from scratch: ring award scan on `txn_events_combo` → double self-join on `usaspending_fpds_prime_award_state` (83M; order rows → `parent_award_key_resolved` → parent rows) → field/active set algebra → `subaward_canonical_slim` wallet aggregation (prime-side IN-list) → `gtm_prime_combo_lanes` window functions (rank, per-firm totals) → normalization → allocation. No precomputed surfaces exist for: (i) recipient-grain position/active state per PSC context (predecessor Entry 2); (ii) per-firm ranked PSC signature from the prime record (the rank/floor/top-N over `gtm_prime_combo_lanes` is recomputed in full every time); (iii) a subaward wallet rollup keyed by (prime set × ring × window) → (receiver, $).
3. **What I ran instead** — the sidecar itself, repeatedly: the same ~45-line CTE chain cloned into ~15+ statements during v13–v14 (distribution, subject slices, top-3/top-5 concentration, strict coverage ×3, matrix translation, two-tier allocation, conservation diagnostics, canonical recounts). Measured `elapsed_ms` 20,600–23,668 per statement; the closing two-tier + diagnostics sequence exceeded 10 minutes wall (operator-observed).
4. **Cost** — ~5–7 minutes of pure query serialization on a typical iteration loop, 10+ minutes on the final one; identical sub-plans (the f555 chain) re-executed in every statement.
5. **Recurrence** — recurring, hard: this allocation is now a standing page section, one per prospect, and every methodology adjustment inside a session re-runs the whole family several times.

## Entry 2 — Serving-snapshot consistency: identical SQL returned two different data states

1. **Intent** — Internally consistent multi-statement analyses: the page carries totals, sub-slices, and coverage counts that must reconcile to the dollar across statements.
2. **Why not the sidecar** — `freshness required` (consistency variant): single-snapshot consistency across statements is not guaranteed by the serving layer. Observed concretely with identical SQL, minutes apart, same day: wallet $3,729.3M / unattributed 534 firms / $771.9M in some responses vs $3,871.8M / 508 / $762.3M in others, oscillating across runs (not monotonic — an early recount got the new state, a later "canonical" rerun got the old state back). `/healthz` reported `...T214133Z` throughout the late session while the numeric signatures matched both the pre- and post-swap artifacts; per-response `artifact` stamps for the conflicting pair were not captured at the time. Signature consistent with multiple serving instances holding different artifacts after the mid-session hot-swap (`193639Z` → `214133Z`).
3. **What I ran instead** — collapsed entire multi-step analyses into single mega-statements (both allocation bases + subject slices + diagnostics in one ~50-line CTE query) to force intra-statement consistency, and re-ran conservation checks to detect which state a response came from. The shipped v14 comparison pair is built from one such statement.
4. **Cost** — several duplicate ~23 s runs purely to detect/diagnose the skew; one wrong-footed defect hunt (conservation appeared broken when it was snapshot skew); page numbers from earlier sections (v12/v13) are pinned to a state the endpoint no longer serves deterministically.
5. **Recurrence** — recurring by construction: any multi-statement session that spans a serving refresh, and any session running while rebuilds ship, is exposed. Every future per-prospect build is multi-statement.

## Entry 3 — `sam_master_entities` declaration columns: near-duplicate fields, semantics undocumented

1. **Intent** — Of the unattributed receivers, how many declare PSC codes in SAM (and what).
2. **Why not the sidecar** — `didn't know it was there` (documentation): the table carries near-duplicate declaration fields with undocumented semantics — `psc_code_counter` (VARCHAR), `psc_code_string` (VARCHAR), `psc_codes` (VARCHAR[]), and the NAICS analogues. Programmatic column pick-up selected `psc_code_counter` and produced "84% of unattributed firms declare PSC codes" — published to the operator, later measured strictly on `psc_codes` as **109 of 508 (~21%)**; retracted. Correct semantics measured this session: NAICS declarations near-universal among registrants (409/508 unattributed firms; their receipts $715M of $762M), PSC declarations sparse/optional (109/508). Guide catalog does not document these columns or the trap.
3. **What I ran instead** — the sidecar (correct column, strict distinct-firm counts) after the wrong-column result; the cost was the wrong intermediate answer, not a fallback.
4. **Cost** — one retracted published figure; two extra ~23 s runs; a `DESCRIBE` round-trip.
5. **Recurrence** — recurring: declaration coverage is now part of the standing allocation section, and any agent touching `sam_master_entities` faces the same column forest.

## Entry 4 — `psc_reference` active-vintage name gaps for display

1. **Intent** — Verbatim reference names for destination codes shown on-page (ruling: labels verbatim from the reference).
2. **Why not the sidecar** — `didn't know it was there` (documentation): joining `psc_reference ... WHERE is_active` returns NULL names for codes that exist only on retired vintages yet still carry historical award dollars (encountered: `7030`, `D399`, `D318`, `J058`, `AD27`). Name resolution requires a vintage-aware fallback (active row if present, else most recent inactive); nothing in the guide notes this.
3. **What I ran instead** — per-code follow-up queries without the `is_active` filter, picking vintages manually.
4. **Cost** — three extra round-trips; display rows initially rendered with missing names.
5. **Recurrence** — recurring at low intensity: every code-grain display section hits some retired-vintage codes.

---

## Ranking (recurrence × cost)

1. **Entry 2 — serving-snapshot consistency** (correctness, not speed: silently mixes states across statements; forced mega-statement authoring and duplicate diagnostic runs; every multi-statement session is exposed)
2. **Entry 1 — allocation workload precompute** (largest recurring wall-time; the f555 chain + per-firm signature ranking re-executed identically in every statement; 10+ minutes on the final batch; per-prospect standing shape)
3. **Entry 3 — declaration column semantics** (produced a retracted number; standing section now depends on the correct columns)
4. **Entry 4 — reference name vintages** (small, steady display tax)

---

## Disposition (gap-pass-3, 2026-07-09 — batched into the gap-pass-2 rebuild: one build, both passes)

Schema probes preceding the verdicts: `naics_codes`/`psc_codes` confirmed `VARCHAR[]` on the serving `sam_master_entities` (the `*_counter`/`*_string` fields are raw ingest artifacts); `psc_reference` carries `is_active` + `source_vintage`, `naics_reference` carries `source_vintage` only (no is_active — same trap, different shape); `gtm_entity_code_lanes` carries all four obligation windows + `action_ct` + `last_action_date` (richer signature source than `gtm_prime_combo_lanes`).

| # | Verdict | What shipped |
|---|---|---|
| 1 | **Promoted** | `gtm_prime_code_signature` (1,177,177 rows, sorted uei/code_type/rank_lifetime) — per-firm ranked prime record with `rank_24mo`/`rank_lifetime`/`share_24mo`/`share_lifetime` precomputed for BOTH code types (the declared-NAICS × matrix attribution in the same session is the sibling demand); floors/top-N stay query-time dials so the moving methodology never bakes in. Local `from_table` build off code lanes — zero R2 cost. Substrate (i) of the entry (position/active per PSC context) is served by gap-pass-2's parent-window columns on `usaspending_fpds_prime_award_state`; the (prime set × ring × window) wallet stays query-time by construction — the prime set is derived per methodology, but every leg of its derivation is now precomputed. **Post-v8 measurement forced a residual promotion in the same cycle:** with the self-join precomputed, the ladder still ran 17–22s — component isolation showed ring scan 1.6s, open-window filter 2.3s, and the join burning the rest: ANY join with an 83M-row side saturates the 2-thread/1.5GB serving box regardless of shape (inverted/semi-join variants measured 17–22s too). Promoted: `gtm_position_orders` — open-window orders only (as-of build date), ~17M × 4 narrow cols, sorted by award key; the ladder becomes ring-keys (320k) ⋈ narrow position table |
| 2 | **Serving fix** (not a manifest change) | Root cause read from `apps/query_sidecar_api/main.py`: `/api/v1/refresh` swaps only the instance the load balancer delivers the POST to; instances otherwise read LATEST only at boot — any multi-instance window (zero-downtime deploy overlap, recycle) serves two artifacts side by side, matching the observed oscillation. Shipped: (a) per-instance LATEST poll (`LATEST_POLL_S`=60s) — convergence guaranteed regardless of topology; (b) `require_artifact` pin on `/api/v1/sql` → 409 on mismatch (skew becomes loud, never silent mixed math); (c) `instance` identity in /healthz + sql responses; (d) fixed a latent race where `S.con`/`S.meta` were read separately mid-swap, so a response could stamp an artifact the query never ran against. Guide §1/§6 document the pin pattern |
| 3 | **Routing fix + view** | Guide catalog warning on `sam_master_entities` (LIST columns are the truth; counter/string are ingest artifacts; measured semantics recorded: NAICS near-universal, PSC ~21% sparse). `v_sam_declared_codes` ships so the correct read (unnest) is the easy one |
| 4 | **Routing fix + views** | `v_psc_names` / `v_naics_names` — active-else-latest-vintage name resolution, one row per code (kills the class: `naics_reference` had the same multi-vintage duplicate-join hazard). Guide reference-row warning + §4 pattern |

Adjacency riders on this pass: signature carries all four obligation windows + `action_ct` + `last_action_date` (recency dial) — same window-function pass, zero marginal cost.

Artifact: final — `query_sidecar_20260710T081000Z.duckdb`, 52 tables (+`gtm_prime_code_signature` 1,177,177 rows, +`gtm_position_orders` ~17M), 3 new views; 52/52 parity (ops ledger run 15; build shared with gap-pass-2). Allocation-chain re-timing on the new artifact: representative signature-allocation leg (live-derived receiver set ⋈ signature) **1.19s**, signature reads 23ms, position rung **2.3s warm** — vs 20.6–23.7s/statement baseline. Consistency fix deploys with the PR merge (Render autodeploy on `apps/query_sidecar_api/**`); poll convergence + pin verification is the first post-merge action (healthz `instance` + converge log line + a deliberate 409).
