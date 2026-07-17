# Sidecar gap report — 2026-07-17 · market-composition predicates (gc-hq platform)

- **Date:** 2026-07-17
- **Serving artifact at compile:** `query_sidecar_20260717T030649Z`
- **Session topic:** gc-hq market-composition substrate survey
  (`~/Desktop/hq/SIDECAR_MARKET_COMPOSITION_SUBSTRATE.md`) — measured probes of the
  predicate legs a platform customer composes markets from. **Operator directive
  (2026-07-17): the repeated-demand bar is relaxed for this product** — platform users
  slice markets in arbitrary ways; foreseeable composer predicates promote on direction.

## Entry 1 — per-entity FY-window won ("won FY23–25 > $X" as a composer leg)

1. **Intent:** the collections doctrine's headline measure — Σ prime obligations per firm
   in a fiscal-year window — as a predicate leg in composed markets.
2. **Why not the sidecar:** `missing table` — no uei × FY mart; the measure exists only
   as scope-embedded computation (collections engine) or ad-hoc group-by.
3. **What I ran instead:** whole-universe GROUP BY over `gtm_txn_events_slim` (108M)
   with FY date gates, joined to rollup + SAM (probe 1 of the substrate report).
4. **Cost:** 790 ms per composed-market count; every FY-measure composition pays it.
5. **Recurrence:** recurring by construction — the most common money predicate the
   platform will serve.

## Entry 2 — per-entity committed/vehicle book (ontology terms as named columns)

1. **Intent:** `active_committed_book`, `vehicle_capacity`, headroom, runway, and
   award-size texture (median/avg committed award) per firm — the ontology's derived
   measures as composable legs.
2. **Why not the sidecar:** `missing table` — `gtm_entity_behavior_rollup` carries the
   obligations flavor only (`active_obl`); the value flavor with the doctrine's
   committed-vs-vehicle split and zero floors is per-query SQL that re-encodes doctrine
   every time (drift risk).
3. **What I ran instead:** date-pruned GROUP BY over `usaspending_fpds_prime_award_state`
   (probe 7).
4. **Cost:** 21 ms (fast) — the gap is definitional pinning, not speed: every consumer
   hand-writes the floors/topology gates.
5. **Recurrence:** recurring — second most common money predicate + the quote/snapshot
   path's accountability measure.

## Entry 3 — employee-size / firmographic predicate at spine grain

1. **Intent:** "firms of 11–200 people" (and industry / founded / LinkedIn identity) as
   a composer leg.
2. **Why not the sidecar:** `wrong grain / missing sort` — servable only through
   `bridge_sam_pdl` (802k rows, NOT 1/uei: 464k distinct) ⋈ `pdl_normalized_companies`
   (35.4M, id-sorted); the query-time join is saturation-class.
3. **What I ran instead:** the bridge⋈PDL⋈rollup join on serving.
4. **Cost:** **10,000.5 ms** measured — unusable as an interactive leg.
5. **Recurrence:** recurring — a natural customer slice in nearly every vertical.

## Entry 4 — set-aside WIN history (vs mere certification)

1. **Intent:** "firms that actually win 8(a)/SDVOSB/WOSB/HUBZone set-aside work" —
   sharper than designation flags.
2. **Why not the sidecar:** `missing column(s)` at entity grain — `type_of_set_aside_code`
   exists on the txn fact only.
3. **What I ran instead:** (not run — identified in the survey as compute-only.)
4. **Cost:** would be a 108M-fact group-by per composition.
5. **Recurrence:** recurring for any designation-oriented partner (surety, mentors,
   capital with SBA programs).

## Footer — ranking

E3 (10s, unusable) > E1 (790ms × every FY composition) > E4 (blocks a predicate family)
> E2 (fast but definitionally unpinned). All four recurring under the platform framing.

---

## Disposition (build cycle 2026-07-17, operator-directed)

**Gate verdicts:**

| Entry | Verdict | Shipped as |
|---|---|---|
| 1 · FY won | **Promote** (structural; operator-directed platform framing) | `gtm_entity_fy_won` — uei × federal FY, sorted (uei, fy), local off `txn_events_combo` |
| 2 · award book | **Promote** (structural; pins doctrine as columns) | `gtm_entity_award_book` — 1/uei, sorted uei, local off `usaspending_fpds_prime_award_state` |
| 3 · firmographics | **Promote** (structural; 10s → ms) | `gtm_entity_firmographics` — 1/uei (best-row rule), sorted uei, local join bridge⋈PDL |
| 4 · set-aside wins | **Promote as columns** riding Entry 1's scan | `won_obl_set_aside` + `won_obl_8a/sdvosb/wosb/hubzone` on `gtm_entity_fy_won`; `committed_value_set_aside` on `gtm_entity_award_book` |

**Not promoted (parked, with reasons):**
- **Predicate-grammar router** — catalyst_api work, not a sidecar table; next core-x
  cycle after the gc-hq ontology rulings.
- **Per-program set-aside split at award-book grain** (8a committed value etc.) —
  grain-multiplying; FY-grain family columns cover the demand; structural-gated.
- **UCC beyond CA/CO** — ingestion work (new state corpora), not a sidecar promotion.
- **`won_fy23_25` as a single baked column** — deliberately NOT baked: the FY mart keeps
  the window a query-time dial (SUM over 3 rows, uei-pruned); baking one window would
  freeze the platform's most tunable dial.

**Build scope block (adjacency sweep, written before the build):**
- *From demand:* the three tables above.
- *Join-side sweep (firmographics, the one new join):* took every plausibly-asked PDL
  column in the pass — company_name, normalized_domain + is_generic_domain (match-quality
  disclosure), linkedin_slug, locality/region/country, industry, employee_size_range,
  year_founded, plus bridge's duns and pdl_company_id (re-join keys). Excluded: source
  bookkeeping (source_version, built_at).
- *Sibling-column sweep (fy_won):* action_ct, award_ct (distinct award_key) ride the
  scan; set-aside family columns per Entry 4 (codes probe-verified: NONE/None = none;
  families 8A%, SDVOSB%, %WOSB%, HZ%).
- *Sibling-column sweep (award_book):* committed ct/value/obligated/runway +
  median/avg (size texture) + vehicle ct/ceiling/headroom + next_committed_end_date
  (expiry zoom) + active_agency_ct (buyer breadth) + committed_value_set_aside.
- *Next-question simulation:* "won in FY24 only" → row filter (served); "trend across
  FYs" → 1-uei read (served); "expiring soon AND big book" → award_book ⋈ rollup (served);
  "8(a) winners in TX over $1M" → fy_won ⋈ SAM (served); "employee size + industry" →
  firmographics single read (served); "per-program committed book" → parked (above);
  "what share of the market has firmographic coverage" → count vs spine (served,
  disclosure: 464k/2.0M registrants bridged).

**Fixture verification (through the dispatch path, pre-build):** preflight OK; all three
specs built via `_build_one`; assertions on zero-floors, committed/vehicle separation,
terminated-exclusion, negative-obligation passthrough, NULL-uei filter, best-row rule
(employee_size beats non-generic domain); EXPLAIN gate on the firmographics join — hash
join, no NL/cross nodes.

**Measured before → after (artifact `query_sidecar_20260717T193347Z`, built this cycle:
fy_won 3,813,994 rows/17.6s · award_book 766,803/3.2s · firmographics 463,741/5.3s,
all parity=OK):**
- FY-won composite leg (won FY23–25 >$5M AND active AND CO): 790 ms → **347 ms**,
  identical count (289) — semantic parity of the mart vs the raw group-by.
- Employee-size leg: 10,000.5 ms → **40.6 ms** (246×). Count corrected 13,393 → 7,936:
  the raw bridge join double-counted multi-match UEIs; the mart's best-row-per-uei
  grain is the honest number.
- Award-book leg (committed_value > $20M): 21 ms hand-written doctrine → **8.9 ms**
  named columns. Count 4,572 → 4,610 (snapshot semantics: mart uses build-date
  days_to_expiry per house convention + fresher award_state pin — expected drift class).
- NEW predicate family live: set-aside WINNERS — 8(a) work won in FY25 = 3,198 firms /
  $14.1B in 33.6 ms.

**Guide updated in the same PR:** catalog rows for the three marts, header count, §4
patterns.
