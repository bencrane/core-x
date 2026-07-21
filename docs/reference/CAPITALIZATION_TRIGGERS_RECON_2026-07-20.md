# Event-Driven Capitalization Triggers — GovCon Working-Capital Friction Recon

**Mode:** READ-ONLY sidecar recon → strategy report. **For:** institutional Capital Providers
(private credit, ABL, invoice factoring, supply-chain finance) seeking proprietary GovCon deal flow.
**Snapshot:** query-sidecar artifact `query_sidecar_20260720T025249Z.duckdb` (2026-07-20, 106 tables).
**Band convention:** firm-level — active obligated book **OR** latest-FY won in **$5M–$100M** (re-cuttable at
per-award grain on request). **Window:** last ~18–24 months (FY2025 + partial FY2026; FY23–24 baseline).
**All counts are live sidecar queries** (elapsed_ms noted), not modeled estimates.

---

## 0. Verdict (ranked)

| # | Segment (the "shape") | Trigger metric | TAM ($5–100M) | Compute today |
|---|---|---|---:|---|
| **A** | **Unfinanced fixed-price carry** | active book ≥70% FFP with **zero** progress/PBP financing | **4,757** | ✅ ms-class, pre-built |
| **B** | **Mobilization velocity spike** | FY25 won ≥3–5× FY23–24 annualized pace | **1,005 @3× · 600 @5×** | ✅ ms-class, pre-built |
| **C** | **Cost-intensive contract shift** | active cost-reimb/T&M ≥30% of book (stock); FFP→cost/T&M (flow) | **1,020 stock · 896 flow** | ⚠️ stock ✅ / flow 2.8s scan |
| **D** | **SCA labor-whale (payroll)** | recent statutory-labor-covered work in band | **3,681** | ⚠️ 0.8s scan |
| **⊕** | **Win-then-borrow (proprietary)** | just won + prior post-win borrowing + **open loan window** | **198** (CA/CO only) | ⚠️ 5.5s interval join |

**Headline:** Lead with **A (unfinanced FFP carry, 4,757 firms / $105.7B active)** — it is the working-capital-gap
thesis expressed as a single pre-computed number: these firms front ~100% of contract cost and receive **no**
progress payments, so every active dollar is cash they carry until they invoice. Layer **B (velocity)** to find
*acute* burn (accelerating), and **⊕ win-then-borrow** as the surgical, revealed-preference proprietary list.

**Infra answer (Q4):** A v1 routing engine stands up **today with no new build** — Segments A, B, and C-stock are
ms-to-sub-second reads off three already-built entity-grain marts (`gtm_entity_pricing_mix`, `gtm_entity_fy_won`,
`gtm_construction_lane_months`). What needs a **new DuckDB sidecar build** is the *velocity/transition/anomaly
cache* that turns the seconds-class ad-hoc scans (C-flow, whole-universe monthly velocity, win-then-borrow) into
one hot `gtm_capitalization_triggers` row per firm. Spec in §5.

---

## 1. Denominator (the middle-market GovCon universe)

`gtm_entity_pricing_mix` (1/uei, 71 cols — the billing/capital-provider lens), active obligated book:

| Active book band | Firms |
|---|---:|
| $1M–$5M | 6,395 |
| **$5M–$100M (target)** | **7,237** |
| >$100M (enterprise) | 1,431 |
| any active | 35,027 |

**7,237 firms** is the addressable middle-market spine. Everything below is a subset with a specific friction shape.

---

## 2. Segment A — The Unfinanced Fixed-Price Carry  *(the headline)*

**Trigger:** `gtm_entity_pricing_mix.active_ffp_unfinanced_share ≥ 0.70` AND `active_obl ∈ [5M,100M]`.

| Cut | Firms | Active obl |
|---|---:|---:|
| ≥70% FFP-unfinanced | **4,757** | $105.7B |
| ≥90% FFP-unfinanced | 4,316 | — |
| …of which small-business-determined | 4,074 | — |

Avg unfinanced share across the ≥70% set = **0.978** — not "mostly," *essentially all* of the book is
firm-fixed-price with **no** progress payments and **no** performance-based financing.

**Why this is undeniable working-capital demand:** On a firm-fixed-price contract with no financing clause, the
contractor performs first and bills on delivery/acceptance. Payroll, materials, and subcontractor invoices all
come out of the contractor's own cash for the full performance period, then wait on government net-30/45 (often
longer with acceptance lag). There is **no contractual mechanism** to draw cash against work-in-progress. That is
the exact gap **invoice factoring / AR financing / a working-capital revolver** fills — the contract *is* the
collateral and the receivable. 66% of the entire middle-market band sits in this posture.

**Product mapping:** invoice factoring · AR-backed line · working-capital term loan.
**Compute:** one ms-class read (`10.3 ms`). Pre-built. No build required.

---

## 3. Segment B — The Mobilization Velocity Spike  *(operator hypotheses a + c)*

### B.1 Economy-wide YoY award spike — `gtm_entity_fy_won` (the high-coverage version)

FY2025 won vs FY2023–24 annualized, FY25 won ∈ [5M,100M]:

| Spike | Firms | FY25 won |
|---|---:|---:|
| ≥2× | 1,663 | — |
| ≥3× | **1,005** | $20.1B |
| ≥5× (operator's number) | **600** | — |
| new at scale (zero prior, ≥$5M FY25) | 270 | — |

### B.2 Construction mobilization — `gtm_construction_lane_months` (monthly grain, new-work isolated)

Trailing-12mo ≥4× prior-24mo annualized, recent ∈ [5M,100M], across the 5 surety work-lanes:

| Lane | ≥3× | ≥4× | ≥4× **new-work-driven** | new entrant @ scale |
|---|---:|---:|---:|---:|
| civil-infrastructure | 108 | 91 | 89 | 67 |
| building-repair/alteration | 107 | 86 | 83 | 35 |
| vertical-building | 82 | 63 | 62 | 55 |
| building-maintenance | 31 | 26 | 25 | 23 |
| industrial-defense-facilities | 16 | 16 | 12 | 27 |
| **distinct firms (≥1 lane, 4×)** | | **262** | | |

262 construction firms, $5.45B recent obligations, and **~98% of the 4× growth is genuine new-award work, not
funding mods** — this is real mobilization, not accounting motion.

### B.3 The operator's literal hypothesis (a), tested — sub-award volume spike

Prime-side farm-out (`subaward_canonical_slim`) trailing-12 vs prior-24, recent ∈ [5M,100M]:
**49 firms @3× · 32 @5×.** This is **coverage-thin** — FSRS sub-award reporting is 1.3M rows vs 108M prime
actions, so only farm-out-heavy primes above the reporting threshold surface. **Reframe:** read the same
"aggressive mobilization" thesis off *prime awards won* (B.1) — 19× the population (600 vs 32 at 5×) at full
coverage. Sub-award velocity is a *refiner*, not the primary lens.

**Why this is undeniable demand:** A firm that just went from $2M/yr to $10M+/yr of contract volume has to
mobilize *ahead* of revenue — hire, buy/lease equipment, stand up subs — 60–90 days before the first invoice
clears. The faster the ramp, the deeper and more sudden the working-capital hole. New-at-scale (270) and
new-entrant construction firms have **no trailing balance sheet** to absorb it.

**Product mapping:** ABL · mobilization line · equipment finance (construction) · PO/contract financing.
**Compute:** B.1 ms-class (`70 ms`), B.2 sub-second (`48 ms`). Pre-built. Whole-universe *monthly* velocity
(all-NAICS, not just construction) is the one gap — see §5.

---

## 4. Segment C — The Cost-Intensive Contract Shift  *(operator hypothesis b)*

### C.1 Stock — who holds cash-intensive contract types now (`gtm_entity_pricing_mix`)

Band [5M,100M]:

| Posture | Firms | Cost obl |
|---|---:|---:|
| any cost-reimbursement active | 1,247 | $27.3B |
| cost-reimb ≥30% of active book | **1,020** | — |
| any T&M / labor-hour active | 1,350 | — |

### C.2 Flow — the actual FFP→Cost-Plus *transition* (`txn_events_combo` pricing series)

Prior-24mo fixed-dominant (≤10% cost) → recent-24mo materially reimbursement-based (≥30%):

| Transition | Firms | Recent obl |
|---|---:|---:|
| FFP → **pure cost-plus** | **56** | $1.58B |
| FFP → **cost-plus OR T&M** | **896** | — |

**Honest finding:** the operator's literal FFP→Cost-Plus shift is **real but small (56 firms)** at the middle
market — true cost-reimbursement is a large-prime instrument; below $100M it is rare. The financeable adjacent
shape is **(i) the unfinanced-FFP carry of §2 (4,757)** and **(ii) the shift into T&M/labor-hour billing (896)**,
where the firm now bills hours and carries 30–60 day labor AR.

**Why cost-plus / T&M maps to capital need:** On cost-reimbursement the contractor floats every cost and is
reimbursed on a billing-cycle lag (provisional rates, DCAA-paced) — a revolving working-capital need that grows
with the contract. On T&M the firm fronts labor and bills in arrears. Both are AR-financing / private-credit
revolver shapes rather than factoring-a-single-invoice shapes.

**Product mapping:** private credit / working-capital revolver (cost-plus) · T&M AR factoring.
**Compute:** stock ✅ ms-class; **flow is a 2.8s off-sort-key scan today — the prime sidecar-build candidate (§5).**

---

## 5. Segment D — The SCA Labor-Whale (payroll mobilization)

**Trigger:** `txn_events_combo.labor_standards_code = 'Y'` (statutory SCA/DBA wage floor applies),
recent (≥2024-07) obligations ∈ [5M,100M].

**3,681 firms · $80.1B.** These are heavy-W2-payroll services/construction primes on wage-determination-covered
work. Statutory wage floors mean payroll is non-compressible; they must make payroll every two weeks while
billing the government monthly in arrears. This is the classic **payroll-funding / factoring** cohort and overlaps
heavily with §2 (labor-driven FFP services). Cleanest, largest single labor signal.

**Compute:** 0.8s scan (off-sort-key on `labor_standards_code`). Works ad-hoc; a labor-exposure rollup makes it
hot (§6).

---

## 6. ⊕ Proprietary overlay — Win-then-Borrow open window  *(the surgical list)*

The one signal a competitor cannot buy off a lead list. Firms that (1) took **fresh federal money in the last 90
days** (≥$250k, new-award/funding actions), (2) have a **documented history of borrowing within 90 days of
winning** (≥2 prior UCC financing filings paired to awards — revealed preference), and (3) have **no new UCC
filing since their last award** — i.e., they behave like borrowers-after-winning and the window is **open right now**:

**198 firms · $1.67B fresh money in 90 days.**

**Coverage caveat:** UCC lien data is **CA + CO only** today (the SoS∩SAM intersection). 198 is a floor for two
states; national coverage scales with UCC ingest. This is the highest-conviction, lowest-volume list — hand it to
a lender tomorrow.

**Compute:** 5.5s pruned interval join. Correct but not hot-path — materialize (§7).

---

## 7. The acute-burn intersection (the composite target)

Stacking the shapes isolates maximum friction — accelerating **and** self-funding **and** geographically mobilizing:

| Funnel | Firms |
|---|---:|
| FY25 spike ≥3× (band) | 1,005 |
| …**also** ≥70% FFP-unfinanced | **563** |
| …**also** multi-state (≥3 PoP states) | **329** |

**329 firms** are growing 3×+, fronting ~100% of contract cost with no financing, and deploying across 3+ states
simultaneously. This is the tightest, highest-intent routing list in the dataset. (Multi-state alone is *not* a
segment — 27,942 firms touch ≥3 states lifetime; it is a qualifier that only earns signal when intersected.)

---

## 8. Infrastructure & Sidecar Assessment (Q4 — the operational answer)

### 8.1 What computes on the fly TODAY (stand up v1 with zero new build)

| Segment | Source mart (already built) | Latency |
|---|---|---|
| A — unfinanced FFP carry | `gtm_entity_pricing_mix` | 10 ms |
| B.1 — economy-wide YoY spike | `gtm_entity_fy_won` | 70 ms |
| B.2 — construction velocity | `gtm_construction_lane_months` | 48 ms |
| C.1 — cost/T&M stock | `gtm_entity_pricing_mix` | 8 ms |
| Acute-burn intersection | the three above ⋈ `gtm_prime_pop_lanes` | 243 ms |

These four are entity-grain, uei-sorted, ms-class. A routing engine can query them live per firm **now**.

### 8.2 What needs a new DuckDB sidecar build (the velocity/anomaly cache)

Four metrics are currently **seconds-class ad-hoc scans** — correct for recon, too slow for a live per-firm
routing loop and incomplete in coverage:

1. **C.2 FFP→Cost/T&M transition flow** — 2.8s scan of `txn_events_combo` (off-sort-key on `pricing_code`+date).
2. **Whole-universe monthly velocity** — the construction mart is lane-scoped; all-NAICS monthly velocity has no
   pre-built mirror (would use `gtm_txn_recipient_month_rollup`, uei-sorted, but needs the recent/prior split baked).
3. **Win-then-borrow** — 5.5s interval join; also CA/CO-coverage-bound.
4. **SCA labor exposure** — 0.8s scan; wants a `uei × labor_standard × recent-window` rollup.

### 8.3 Proposed mart — `gtm_capitalization_triggers` (1/uei, the financial-anomaly cache)

One row per award-active firm; the routing engine reads a single row and filters/scores in ms.

**Cross-database joins (all pure equality on `uei` — EXPLAIN-gate no nested-loop per build doctrine):**

```
BASE      gtm_entity_behavior_rollup            -- the 262k award-active uei spine
 ⋈ velocity/transition/geo leg (ONE GROUP BY uei over txn_events_combo, 36-mo slice):
            recent_12mo_obl, prior_24mo_obl,
            recent_new_award_obl (action_type base/new),
            cost_share_recent, cost_share_prior, tm_share_recent   (pricing_code class map)
            distinct_recent_states, distinct_recent_agencies
 ⋈ gtm_entity_fy_won        (uei)  -> won_latest_fy, won_prior2_fy   (YoY ratio)
 ⋈ gtm_entity_pricing_mix   (uei)  -> active_obl, active_ffp_unfinanced_share, cost/tm shares
 ⋈ subaward_canonical_slim  (uei, prime side, agg) -> farmout_recent/prior   [LEFT]
 ⋈ sam_ucc_filings          (uei)  -> last_financing_filing_date, is_active [LEFT, CA/CO]
 ⋈ sam_ucc_debtor_overlap   (uei)  -> has_active_lien                        [LEFT, CA/CO]
 ⋈ gtm_sam_entities / gtm_entity_firmographics (uei) -> name,state,employees,domain (hydrate)
```

**Baked columns (floors applied at query time — the dial doctrine):**

| Column | Formula |
|---|---|
| `velocity_ratio_12v24` | `recent_12mo_obl / nullif(prior_24mo_obl/2, 0)` |
| `yoy_won_ratio` | `won_latest_fy / nullif(won_prior2_fy/2, 0)` |
| `ffp_unfinanced_share` | passthrough from pricing_mix |
| `cost_transition_flag` | `cost_share_prior ≤ 0.10 AND cost_share_recent ≥ 0.30` |
| `new_award_share_recent` | `recent_new_award_obl / nullif(recent_12mo_obl, 0)` |
| `open_loan_window_flag` | `last_award_date > coalesce(last_financing_filing_date,'1900-01-01') AND fresh_money_90d ≥ 250000` |
| `acute_burn_score` | weighted z-composite: velocity, ffp_unfinanced_share, new_award_share, multi-state, labor_share |

**Thresholds (query-time dials, not baked):** band `active_obl OR won_latest_fy ∈ [5e6, 100e6]`;
velocity/yoy `≥ 3.0` (`≥ 5.0` = hot); `ffp_unfinanced_share ≥ 0.70`; `distinct_recent_states ≥ 3`.

**Build mechanics:** the velocity/transition/geo leg is the single expensive step — one full `txn_events_combo`
GROUP BY uei — but it runs **at build time in Modal**, not at serving time. Everything else is an equality join to
an existing uei-sorted mart. Follows the blue-green LATEST pointer + parity gate
(`count_rows == gtm_entity_behavior_rollup uei count` at pinned Lance version) per
`build_query_sidecar.py` doctrine. Launch detached (`modal run --detach`), ledger `ops.query_sidecar_runs`.

**Coverage extension (data, not compute):** the win-then-borrow signal is gated by UCC ingest (CA/CO). Adding TX,
FL, NY UCC corpora multiplies the proprietary list — a data-factory ingest, tracked separately from this mart.

---

## 9. Honest caveats

- **Bands are firm-level** (active book / FY-won), not per-award. Re-cuttable at award grain on request.
- **Win-then-borrow = CA/CO only** — 198 is a two-state floor, not national.
- **Sub-award velocity is coverage-thin** (FSRS 1.3M) — use prime-won velocity as primary.
- **FFP→Cost-Plus transition is genuinely small** (56) at middle market — the value is in the unfinanced-carry and
  T&M-shift adjacencies, not the literal cost-plus flow.
- **FPDS publication lag** — the freshest ~1 month is empty, months 2–3 half-reported (DoD ~90-day embargo); short
  windows understate. Growth windows anchor to each mart's `max(month)` watermark, never `current_date`.
- Snapshot, not live: reconcile multi-statement totals against the `/healthz` artifact stamp.

---

## 10. Recommended next action

1. **Ship v1 routing on Segments A + B today** — no build; three ms-class marts already serve it.
2. **Authorize the `gtm_capitalization_triggers` build (§8.3)** to make velocity/transition/win-then-borrow hot and
   fold all shapes into one per-firm anomaly row + `acute_burn_score`.
3. **Lender-side match:** join the triggered firm set to `active/capital_provider_signals` (the native lender
   taxonomy — factoring / ABL / equipment-finance / private-credit) so each triggered contractor routes to the
   *right product* desk, not a generic list.
4. **Queue TX/FL/NY UCC ingest** to scale the proprietary win-then-borrow list beyond CA/CO.
