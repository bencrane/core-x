# Active Subcontracting Demand × DSBS Supply — Opportunity Density Audit

**Mode:** READ-ONLY analysis over the Gen-3 Lance SoR (`s3://data-sink/active/`). No materialization writes.
**Date:** 2026-07-03 (UTC). **Compute:** DuckDB out-of-core over Lance/R2 (108M-row spine scan, column-pushdown).
**Verification:** 4 independent adversarial verifiers re-derived every headline from R2 with their own SQL — **0.000% deviation** on all figures. Methodology critic returned **confirmed-with-caveat** (arithmetic exact; two framing corrections applied below).

---

## 0. What this measures (and three corrections to the source directive)

Intersect **active prime contracts legally obligated to subcontract** against the **historically-proven `(NAICS, PSC)` execution footprint** of the SBA/DSBS small-business cohort — proven both as outright **primes** (all-time FPDS canonical spine) and as **subawardees** (FSRS subaward universe). Three corrections were required for a defensible result:

| # | Directive as written | Correction applied | Why |
|---|---|---|---|
| C1 | `subcontract_plan IS NOT NULL` | `has_subcontracting_plan` = FPDS `subcontracting_plan_code ∈ {C,D,E,F,G,H}` | The raw field carries codes `A`–`H`; `B`="plan not required" (~840k txns), `A`="no plan", empty=none. `IS NOT NULL` over-counts ~10×. Only C–H are a real FAR 52.219-9 obligation to subcontract. |
| C2 | Sum `federal_action_obligation` over active **transactions** | Sum `total_dollars_obligated` at **award grain** | "Active" is an award-level concept (period-of-performance end > today). The 108M-row txn spine records mods/de-obligations; summing it double-counts. `total_dollars_obligated` is award-lifetime cumulative (verified: Σobl/Σfao = 36.98, the cumulative signature). |
| C3 | (implied) use pre-built capability surface | Use **executed** combos only; **reject `capability_lanes`** | `capability_lanes` is a forward-looking *recommender* (evidence tiers `primed-direct / subbed-hop / primed-hop / declared`, capped 10/firm, *excludes* already-subbed lanes, includes inferred hops + SAM-declared-never-executed). It is not "proof of performance." |

**Demand ledger** = `govcon_active_awards` where `has_subcontracting_plan ∧ (active_current ∨ active_potential)` — identical to the canonical `materialize_active_subcontracting_obligations` filter (reconciles exactly to that MV's 26,573 rows).

### Correction C4 — the dollar the headline reports is a *demand signal*, not *addressable dollars*

`govcon_active_awards` has **no small-business-subcontract dollar field** (full-schema verified). `total_dollars_obligated` is the **prime's** obligated pool. So the "$1,053B pool / 74.1% coverage" answers *"where does subcontracting-plan demand concentrate, and can DSBS firms prove competency there?"* — a **targeting signal**. It is **not** "dollars a DSBS firm can win." The dollars a DSBS firm can actually capture are the **small-business subcontract carve-out** — a FAR-goal fraction of a fraction. That correctly-scoped number is sized separately in §1B from realized FSRS `subaward_amount`.

---

## 1A. LAYER 1 — Macro opportunity density (demand-signal coverage)

**Active subcontractable demand pool** (26,573 active prime awards obligated to subcontract, 2,587 `(NAICS,PSC)` combos):

| Measure | Total demand pool | DSBS-addressable* | Coverage |
|---|---:|---:|---:|
| Prime obligated $ | **$1,053.2B** | $780.5B | **74.1%** |
| Prime potential ceiling $ | $1,791.0B | $1,340.5B | 74.8% |
| Prime unspent headroom $ | $738.8B | $561.0B | 75.9% |
| Active awards | 26,573 | 25,524 | 96.1%△ |
| `(NAICS,PSC)` combos | 2,587 | 2,009 | 77.7% |

*addressable = the combo has ≥1 DSBS firm with proven prime-or-sub execution in that exact `(NAICS,PSC)`.
△ award-coverage is inflated by one low-$ catch-all combo — see §3 caveat. **Dollar-weighted coverage (74.1%) is the robust headline.**

**Robustness of the 74.1% headline:**
- **Sub-proof only** (require proven *subcontractor* experience — the on-point GTM motion): **64.5%** of the pool ($679.6B, 859 combos) still addressable.
- **Prime-proof only:** 65.2% ($686.2B, 1,909 combos).
- **Symmetric 2021+ window** (restrict prime footprint to the same recency as FSRS subs): **67.2%** ($707.5B, 1,833 combos). The headline survives the recency-asymmetry stress test within ~7pp.

**The non-addressable $272.7B (578 combos / 1,049 awards) is structural, not a gap in our roster.** It resolves to national-lab **GOCO facility operation** (PSC `M181`), classified **space-flight R&D** (`AR33/AR62`), and **missile-launcher** manufacturing — combos no small business has ever primed or subbed. Top non-addressable: `541710|M181` GOCO R&D ops $83.4B, `561210|M181` $69.0B, `336414|AR62` space-station R&D $22.4B.

## 1B. LAYER 1 (corrected) — realized *subcontract* dollars, the true addressable measure

Sized from actual FSRS `subaward_amount` (the field that exists and was previously unused). **This is the correctly-scoped opportunity.**

| Scope | Sub $ | DSBS-captured | DSBS share | Displaceable to DSBS |
|---|---:|---:|---:|---:|
| All FSRS subawards (2021+) | $437.8B | — | — | — |
| **In addressable combos** (all-time) | **$329.1B** | $39.3B | **11.9%** | **~$289.8B** |
| Under the *active* demand primes | $94.2B | $14.3B | 15.1% | ~$79.9B |

**The GTM headline is not 74%. It is 12%.** DSBS firms have *proven competency* across combos carrying $329B of realized subcontract flow, yet **capture only 11.9% of it** — the other **~$290B currently routes to non-DSBS subcontractors**. The opportunity is displacement within proven wheelhouses, not greenfield.

---

## 2. LAYER 2 — Combo density (Pareto)

### Top 25 addressable `(NAICS, PSC)` by active prime obligated $

| # | NAICS | PSC | Sector | Awards | Obligated | Unspent | DSBS firms | primed/subbed | $/firm |
|---:|---|---|---|---:|---:|---:|---:|---|---:|
| 1 | 561210 | M1JZ | Facilities Support Svcs | 6 | $140,013M | $79,606M | 216 | 21/196 | $648.2M |
| 2 | 336611 | 1905 | Ship Building & Repairing | 10 | $83,006M | $16,295M | 80 | 10/74 | $1,037.6M |
| 3 | 336411 | 1510 | Aircraft Manufacturing | 24 | $44,778M | $12,288M | 116 | 6/112 | $386.0M |
| 4 | 541710 | AZ11 | R&D Physical Sciences | 1 | $41,069M | $52,249M | 16 | 16/0 | $2,566.8M |
| 5 | 561210 | M159 | Facilities Support Svcs | 1 | $27,140M | $11,643M | 1 | 1/0 | $27,139.8M |
| 6 | 541715 | M1HA | R&D Physical Sciences | 4 | $17,296M | $3,731M | 58 | 0/58 | $298.2M |
| 7 | 541512 | DA01 | Computer Systems Design | 170 | $16,166M | $16,233M | 1,160 | 810/469 | $13.9M |
| 8 | 336414 | 1410 | Guided Missile & Space Vehicle | 18 | $13,926M | $2,789M | 51 | 2/49 | $273.0M |
| 9 | 541715 | R425 | R&D Physical Sciences | 22 | $13,516M | $14,164M | 261 | 107/172 | $51.8M |
| 10 | 541715 | AR22 | R&D Physical Sciences | 92 | $13,403M | $2,371M | 2 | 1/1 | $6,701.4M |
| 11 | 336414 | AR11 | Guided Missile & Space Vehicle | 3 | $13,029M | $1,551M | 23 | 0/23 | $566.5M |
| 12 | 541330 | R499 | Engineering Services | 169 | $9,977M | $12,531M | 2,015 | 1,782/385 | $5.0M |
| 13 | 336411 | 1520 | Aircraft Manufacturing | 12 | $9,805M | $2,247M | 35 | 3/32 | $280.1M |
| 14 | 524114 | Q201 | Health/Medical Insurance | 2 | $9,275M | $61,594M | 4 | 0/4 | $2,318.7M |
| 15 | 621111 | Q403 | Offices of Physicians | 33 | $7,537M | $8M | 14 | 11/3 | $538.4M |
| 16 | 336414 | 1425 | Guided Missile & Space Vehicle | 1 | $7,202M | $2,455M | 25 | 1/24 | $288.1M |
| 17 | 541715 | AC24 | R&D Physical Sciences | 1 | $6,495M | $1,356M | 39 | 2/37 | $166.5M |
| 18 | 541330 | R425 | Engineering Services | 196 | $5,984M | $6,410M | 1,498 | 1,080/654 | $4.0M |
| 19 | 236220 | Y1PZ | Commercial Bldg Construction | 13 | $5,921M | $0M | 319 | 298/21 | $18.6M |
| 20 | 236220 | Y1AA | Commercial Bldg Construction | 21 | $5,422M | $175M | 555 | 525/31 | $9.8M |
| 21 | 524114 | G007 | Health/Medical Insurance | 22 | $5,414M | $18,397M | 3 | 1/3 | $1,804.8M |
| 22 | 336414 | 1420 | Guided Missile & Space Vehicle | 6 | $5,303M | $3,291M | 28 | 0/28 | $189.4M |
| 23 | 562211 | F108 | Hazardous Waste Treatment | 4 | $5,131M | $313M | 64 | 64/0 | $80.2M |
| 24 | 541715 | AR12 | R&D Physical Sciences | 120 | $4,949M | $3,123M | 13 | 4/9 | $380.7M |
| 25 | 541512 | R499 | Computer Systems Design | 45 | $4,673M | $8,854M | 577 | 347/268 | $8.1M |

### Supply-thickness × demand cross-tab (addressable combos)

| DSBS firms in combo | Combos | Awards | Obligated | Unspent | Read |
|---|---:|---:|---:|---:|---|
| **1 firm (monopoly)** | 235 | 722 | $42.9B | $18.6B | thin — single point of failure, high $/firm |
| 2–5 | 464 | 1,640 | $59.4B | $105.8B | thin — low competition |
| 6–25 | 622 | 14,288 | $149.7B | $134.4B | balanced |
| 26–100 | 465 | 4,185 | $204.5B | $84.0B | deep |
| **100+ (deep bench)** | 223 | 4,689 | $324.0B | $218.3B | commoditized — capacity is not the constraint |

### High-leverage beachheads — high $ + thin supply (≥$1B obligated AND ≤5 capable DSBS firms) — 15 combos

The GTM gold: large demand, almost no proven small-business competition.

| NAICS | PSC | Sector | Awards | Obligated | Unspent | DSBS firms |
|---|---|---|---:|---:|---:|---:|
| 561210 | M159 | Facilities Support Svcs | 1 | $27,140M | $11,643M | 1 |
| 541715 | AR22 | R&D Physical Sciences | 92 | $13,403M | $2,371M | 2 |
| 524114 | Q201 | Health/Medical Insurance | 2 | $9,275M | $61,594M | 4 |
| 524114 | G007 | Health/Medical Insurance | 22 | $5,414M | $18,397M | 3 |
| 336415 | AR11 | Guided Missile & Space Vehicle | 1 | $4,431M | $43M | 3 |
| 334511 | 1820 | Search/Detection/Navigation | 4 | $3,106M | $119M | 1 |
| 541715 | AR32 | R&D Physical Sciences | 1 | $3,044M | $1,480M | 2 |
| 333314 | AR21 | Optical Instrument Mfg | 1 | $2,511M | $68M | 1 |
| 922190 | R499 | Justice/Public Order | 3 | $1,929M | $268M | 1 |
| 237990 | Y1ED | Heavy/Civil Engineering Constr | 3 | $1,824M | $124M | 5 |
| *(+5 more, all ≥$1B / ≤5 firms)* | | | | | |

---

## 3. LAYER 3 — Entity-level target saturation

For each DSBS firm: count of **distinct active sub-obligated prime awards** operating in that firm's proven `(NAICS,PSC)` wheelhouse.

| Active targets in wheelhouse | DSBS firms | Share of firms-with-targets |
|---|---:|---:|
| **1** | 824 | 5.5% |
| **2–5** | 1,925 | 12.9% |
| **6+** | 12,146 | 81.5% |
| — of which 6–20 | 1,798 | |
| — of which 21–100 | 3,721 | |
| — of which 100+ | 6,627 | |
| **Firms with ≥1 target** | **14,895** | of 18,564 with proven history (67,234 rostered) |

Median = **76** targets/firm · mean = 196 · max = 12,763.

**Interpretation:** the binding constraint is **not target discovery** — the median proven DSBS firm can point at 76 live sub-obligated primes in its wheelhouse. It is **prioritization and warm introduction**. This is a ranking problem, not a coverage problem.

**Mandatory caveat (verified):** the *mean/max/21+ band* is inflated by two artifacts — (a) all-time prime footprint, and (b) catch-all 4-char PSCs. One combo, `336111|2310` (Automobile Mfg | Passenger Motor Vehicles), is **41.7% of all demand awards (11,082 of 26,573) at only $0.6B** — any firm proven there instantly "reaches" 11,082 low-dollar targets. The **median (76) is robust** (drops only to 71 when the top-10 mega-combos are excluded). Use the **median and dollar-weighted views**, not raw award counts or the tail. The `96.1%` award-coverage in §1A is dominated by this same combo and should never be quoted without the dollar-weighted `74.1%` beside it.

---

## 4. Beyond the directive — actionable extensions

### 4A. Top active primes to approach (prime-obligated $ in DSBS-addressable combos)

The concrete call-list: active primes carrying the most subcontracting obligation in combos DSBS firms can prove.

| Prime | UEI | Awards | Addr. combos | Obligated | Unspent |
|---|---|---:|---:|---:|---:|
| Electric Boat Corp | E7BEKJ4V9528 | 3 | 1 | $56,526M | $5,913M |
| NTESS (Sandia) | LUJEPCRRT377 | 1 | 1 | $42,370M | $12,564M |
| Lawrence Livermore Nat'l Security | PM52LCJH72T9 | 1 | 1 | $41,069M | $52,249M |
| Triad Nat'l Security (LANL) | X7WUS5LRBQU3 | 1 | 1 | $35,003M | $13,249M |
| Consolidated Nuclear Security | EWV8QKG1JUV7 | 1 | 1 | $34,070M | $19,442M |
| Lockheed Martin | G4KDGE4JFFK7 | 22 | 9 | $33,523M | $1,194M |
| General Dynamics IT | SMNWM6HN79X5 | 154 | 47 | $8,980M | $9,975M |

*(GDIT is the highest-*breadth* target: 154 active awards across 47 addressable combos — the widest surface for a multi-combo DSBS bench.)*

### 4B. DSBS certification-cohort lens (reachable pool — heavily overlapping, not additive)

| Cohort | Addressable firms | Distinct active targets | Reachable obligated pool† |
|---|---:|---:|---:|
| VOSB | 6,987 | 24,851 | $744.2B |
| SDVOSB | 6,223 | 24,607 | $741.8B |
| WOSB | 4,818 | 24,340 | $766.3B |
| 8(a) | 2,842 | 21,796 | $583.5B |
| HUBZone | 2,570 | 23,628 | $707.2B |
| EDWOSB | 1,552 | 21,353 | $600.6B |

†Reachable pool is prime-obligated $ (a signal, per C4) and **overlaps across cohorts** — a single award reachable by both a WOSB and a VOSB firm is counted in both rows. Not additive.

### 4C. Evidence provenance (grain-labeled — avoids the three-"both" conflation trap)

- **Combo grain (2,009 addressable):** prime-proof 1,909 · sub-proof 859 · both 759.
- **Pair grain (336,323 proven `(uei,combo)`):** prime-only 318,890 (94.8%) · sub-only 15,336 · both 2,097.
- **Firm grain (18,564 proven firms):** 14,895 map to ≥1 live target; ~3,669 (20%) have proven history but zero current demand overlap.

**94.8% of proven evidence is *prime*-proof, but the demand is a *sub* motion.** Weight proven-as-sub above proven-as-prime-once when ranking (the §1A sub-proof-restricted 64.5% is the honest floor).

---

## 5. Methodology, lineage, limitations

**Data lineage (all `s3://data-sink/active/`):**
- Demand: `govcon_active_awards` (189,272 → 26,573 filtered), award grain, unique on `contract_award_unique_key`.
- Supply/prime: `usaspending_fpds_canonical_txn` (108,181,354 rows, all-time), distinct `(recipient_uei, naics|psc)`.
- Supply/sub: `subaward_naics_psc_wide` (627,582 rows, 2021+ FSRS), distinct `(subawardee_uei, prime_naics|prime_psc)`; `subaward_amount` for §1B.
- Roster: `sba_dsbs_certified_firms` (67,234 UEIs).
- Key normalization: `upper(trim())` on UEI/PSC, `trim()` on NAICS — verified zero silent join misses (FPDS UEIs already normalized; PSC casing 0 divergent).

**Known limitations (ranked; each is a next-iteration lever):**
1. **Prime pool ≠ sub carve-out** (C4) — mitigated by §1B realized-sub sizing; still lacks per-active-award forward sub-target dollars (not in FPDS).
2. **Recency asymmetry** — prime all-time vs sub 2021+; §1A symmetric-window test bounds the effect (74.1% → 67.2%). No per-combo recency decay applied.
3. **No geographic feasibility** — `pop_state/city/zip` (demand) and DSBS firm address are both present but unused; addressability overstates breadth for place-bound services (PSC R/S/Y families).
4. **No primary-NAICS / min-transaction gate** — "proven" fires on any historical combo including one-off secondary-NAICS transactions.

**Verification record:** 4 adversarial verifiers (macro / grain / saturation / join-integrity) independently rebuilt every headline from R2 — **max deviation 0.000%**; full-population saturation reconciliation 14,895/14,895 exact. Methodology critic: **confirmed-with-caveat** — all P0/P1 framing findings (C4 dollar scope, catch-all inflation, prime-vs-sub evidence, recency) are addressed above in §1B, §3, §4C, and §5.

**Artifacts** (`reports/dsbs_overlap/`): `summary.json`, `refinements.json`, `overlap_combos.parquet`, `demand_combos.parquet`, `entity_saturation.parquet`, `dsbs_proven_footprint.parquet`, `top_combos.parquet`, `top_primes.parquet`.
**Reproduce:** `doppler run -p core-x -c prd -- .venv/bin/python scripts/dsbs_active_demand_overlap.py` (then `…_refinements.py`). Read-only; no Lance writes.
