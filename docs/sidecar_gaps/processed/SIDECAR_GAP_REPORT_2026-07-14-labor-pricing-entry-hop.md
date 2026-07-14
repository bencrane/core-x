# SIDECAR GAP REPORT — 2026-07-14 — labor pricing scalar + role-name entry hop

**Artifact at session:** `query-sidecar/query_sidecar_20260713T043612Z.duckdb` (87 tables)
**Topic:** labor-wiring deltas 1+2 build session (PRs #1147, #1148) + declared platform-app
live-card consumption. Both new composed dims landed Lance-only; every read of them this
session was a Lance fallback, and the operator has declared the recurring consumer: live
narrative cards in rare-structure-hq served via catalyst-api phrase queries, which execute
ONLY against the sidecar.

---

## Entry 1

1. **Intent** — "What fraction of an award dollar is labor, fully loaded, for this NAICS —
   so a combo/award/target list can be priced as expected labor $ by category?"
   (`expected labor $ = award_$ × loaded_labor_share × pct_of_industry/100`.)
2. **Why not the sidecar** — missing table: `naics_labor_share` (NEW this session, PR #1147 —
   1,133 rows, 1/6-digit NAICS, `loaded_labor_share = payroll_share × burden_multiplier` +
   BEA cross-check + provenance flags) is Lance-only.
3. **What I ran instead** — pylance probe of `s3://data-sink/active/naics_labor_share/` +
   duckdb join onto `naics_psc_labor_profile_categories` (warm) pulled down to local;
   columns: naics_code, payroll_share, burden_multiplier, loaded_labor_share,
   bea_comp_share_of_output.
4. **Cost** — ~20–30 s per probe session (uv + Lance handshake); 1,133 scanned / handfuls
   returned. The real cost: the warm combo layer cannot price an award dollar at all — the
   composition identity's scalar lives off-artifact.
5. **Recurrence** — recurring, declared: every pricing-lens card ("the margin math"),
   every expected-labor-$ statement on a live card, and the phrase production planned
   against it. This is the whole point of the dim.

## Entry 2

1. **Intent** — "Resolve a free-text role name ('travel nurses', 'network engineers') to
   SOC/SCA codes so the pre-call chain can enter the ranked combo layer."
2. **Why not the sidecar** — missing table: `occupation_alias_lookup` (NEW this session,
   PR #1148 — 66,878 rows, 1/(alias_norm, code_type, code); O*NET primary/reported/
   alternate + SCA titles, parenthetical variants split, bridged soc_code inline,
   in_combo_layer reachability flag) is Lance-only.
3. **What I ran instead** — pylance probe of `s3://data-sink/active/occupation_alias_lookup/`
   + duckdb chain verification (alias_norm probe → soc_code → categories join → labor-share
   join); columns: alias, alias_norm, code_type, code, occupation_title, title_source,
   in_combo_layer, bridged_soc_code.
4. **Cost** — ~20–30 s per probe session; 66,878 scanned / tens returned. Chain verification
   ("travel rn" → 29-1141 → 213 priced combos) required registering three datasets locally —
   the exact shape a live card needs in <100 ms.
5. **Recurrence** — recurring, declared: the entry hop for every pre-call / role-input card;
   the platform-app's role-text input resolves through this table on every query.

---

## Ranking (recurrence × cost)

1. **Entry 2** — `occupation_alias_lookup`: the entry hop; every role-text interaction
   crosses it first.
2. **Entry 1** — `naics_labor_share`: the pricing scalar; every expected-labor-$ number
   crosses it.

Adjacency note for the build cycle: the two tables plus the already-warm
`naics_psc_labor_profile_categories` form one connected chain (alias → SOC/SCA → ranked
combos → loaded labor $). The recurring composite shape is the three-table join — the
chain verification above assembled it manually on Lance. Total added volume ≈ 68k rows /
single-digit MB — negligible against the 1.23B-row artifact.

---

## Disposition (labor-pricing build cycle, 2026-07-14)

Probed every claimed schema before gating (both tables built this session; serving-side
`DESCRIBE naics_psc_labor_profile_categories` verified for the view join: `pct_of_industry`
DOUBLE, `a_median` VARCHAR → TRY_CAST in the view).

### Gate verdicts
| Entry | Verdict | Rationale |
|---|---|---|
| 1 `naics_labor_share` | **promote** | structural gate met: recurring demand (declared live-card consumption + this session's fallbacks), trivial recurring cost (1,133 rows) |
| 2 `occupation_alias_lookup` | **promote** | structural gate met: same evidence; 66,878 rows |

### Build scope block (written BEFORE the build)
| Item | Rows | Source | Rationale |
|---|---|---:|---|
| `naics_labor_share` (Tier D, sort `naics_code`) | 1,133 | demand (Entry 1) | the award-dollar pricing scalar; SELECT * (all provenance/cross-check columns ride — column adds are free during a committed build) |
| `occupation_alias_lookup` (Tier D, sort `alias_norm, code`) | 66,878 | demand (Entry 2) | the entry hop; sorted on the probe key so an alias lookup prunes; SELECT * |
| `v_role_priced_combos` (view) | — | **adjacency (next-question)** | the recurring composite: alias → (soc leg ∪ sca leg) → ranked combos ⋈ labor share, with `category_award_share = loaded_labor_share × pct_of_industry/100` PRECOMPUTED — bakes the percent-vs-fraction correction into the artifact so no consumer can inflate expected labor $ 100× |

**Sibling-column sweep:** both tables ship whole (small); no excluded columns.
**Join-side sweep:** the view carries the categories side's `soc_title`, `sca_title`,
`role_class`, `a_median` (cast), `pct_of_industry`, `ep_growth_2024_2034_pct` — the label,
wage and growth columns a card renders next to any match; and the labor-share side's
`payroll_share`, `burden_multiplier`, `payroll_share_level`, `burden_match_level` (the
provenance dials a card footnotes).
**Next-question simulation:** "price this role in this county" → view ⋈ `v_wd_county_rates`
/ `soc_state_wage` on soc/occupation code (both already warm) ✓; "who wins this work" →
view's (naics_code, psc_code) joins `txn_events_combo` / `combo_award_active_state` ✓;
"name the SCA code" → `dol_sca_occupations` warm ✓.
**Parked (structural-gated, no demand):** `bls_ecec_costs` (627k) / `bls_ecec_burden` (321)
— calibration detail behind the composed scalar; the dim carries the resolved multiplier.
`bls_oews_2025` (413k) — staffing-pattern detail, no card shape yet. `govcon_labor_demand` /
`sam_labor_poc_people` — unchanged from gap-pass-6 parking.

### Measured deltas (serving, before → after)
| Shape | Before (Lance fallback) | After (serving) |
|---|---|---|
| E1 loaded share for a NAICS | ~20–30 s pylance probe | **1.8 ms** |
| E2 alias probe (role text → codes) | ~20–30 s pylance probe | **2.9 ms** (4.1 cold) |
| Composite chain (role text → priced ranked combos, `v_role_priced_combos`) | ~60 s three-dataset local assembly | **5.4 ms** (11.0 cold) |

Composite verified end-to-end on serving: `alias_norm='travel rn'` → SOC 29-1141
(Registered Nurses) → 213 ranked combos, each carrying `loaded_labor_share` and the
precomputed `category_award_share` (top: 622110 hospitals combos @ 0.1616 of the award
dollar for the RN category).

Artifact: `query_sidecar_20260714T230548Z.duckdb`, **89 tables** (87 → 89),
48,651,579,392 B (45.31 GiB). Both new marts parity=OK against pinned Lance versions
(naics_labor_share 1,133 @ v5; occupation_alias_lookup 66,878 @ v12). Build
`ap-QocQhw8XPgaiA4kE3VhenF`.
