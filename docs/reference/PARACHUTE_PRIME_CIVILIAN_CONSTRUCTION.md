# Parachute Prime & Civilian Construction Density — findings

**As-of:** 2026-06-21 (UTC) · **Source:** read-only DuckDB probe of `s3://data-sink/active/govcon_active_awards/` (189,272 active prime awards) cross-referenced to `s3://data-sink/active/govcon_equipment_rental_construction_match/`.
**Harness:** [`scripts/parachute_prime_probe.py`](../../scripts/parachute_prime_probe.py) — single R2 connection, one filtered base materialization, self-validating diagnostics. Re-run: `doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb python3 scripts/parachute_prime_probe.py`.

## Definitions (locked)

| Term | Predicate | Notes |
|---|---|---|
| **active + future end** | `active_current OR active_potential` | GREATEST(current_end, potential_end) ≥ as-of. **Excludes `pop_unknown`** (both PoP ends NULL — no end date, out of scope by the "future end date" rule). |
| **Davis-Bacon** | `construction_wage_rate_requirements = 'YES'` (= code `Y`) | Confirmed raw values: `NOT APPLICABLE` 116,910 · `NO` 26,864 · `YES` 5,015. |
| **Civilian** | awarding toptier ≠ `Department of Defense` **AND** funding toptier ≠ `Department of Defense` | Leak-audit confirms zero USACE/Army/Navy/Air-Force/Marine strings on the civilian side — all service branches roll up to the single `Department of Defense` toptier and are excluded. |
| **Parachute Prime** | `recipient_state_code <> pop_state_code` (both non-null, trimmed/upper) | HQ state ≠ place-of-performance state. |
| **Golden** | Parachute **AND** `has_subcontracting_plan` | Pre-derived boolean (subcontracting_plan_code ∈ C–H). |

**Value columns:** `current_total_value_of_award` (canonical, = the MV's `award_value`) leads. `potential_total_value_of_award` is the ceiling (incl. options). `base_and_all_options_value` is **rejected** — ~30% zero in this cohort.

## Cohort funnel

| Population | Count |
|---|---|
| All active prime awards | 189,272 |
| Active + future end | 148,789 |
| Active + future end, Davis-Bacon | **5,015** |
| └ Davis-Bacon, `pop_unknown` (no end date — excluded) | 1,666 (1,379 civilian) |
| Civilian (Davis-Bacon, non-DoD, active+future) | **4,198** |

> Sensitivity: include `pop_unknown` Davis-Bacon and the civilian cohort rises 4,198 → 5,577; the all-DB cohort rises 5,015 → 6,681. Tighten to `active_current` only and civilian = 4,064, parachute = 2,688, golden = 394.

---

## 1. Civilian / infrastructure heavyweights

**Civilian Davis-Bacon active construction: 4,198 awards · $147.28B current value · $163.76B potential · $127.96B obligated-to-date.**

### Top 5 agencies by active construction award count

| # | Agency | Awards | Current value | Potential value |
|---|---|---:|---:|---:|
| 1 | Department of Veterans Affairs | **907** | $4.62B | $4.68B |
| 2 | General Services Administration | **718** | $4.79B | $5.69B |
| 3 | Department of the Interior | **489** | $4.35B | $4.40B |
| 4 | Department of Agriculture | **473** | $0.94B | $0.99B |
| 5 | Department of Transportation | **365** | $2.69B | $3.14B |
| | **Top-5 combined** | **2,952** (70.3% of civilian) | **$17.38B** | **$18.90B** |

### ⚠ Dollar-weighting is misleading — rank by count

Ranked by **value**, the table is dominated by capital-intensive M&O vehicles, not dirt-moving:

| Agency | Awards | Current value |
|---|---:|---:|
| Department of Energy | 64 | **$91.59B** (62% of civilian dollars) |
| Department of Homeland Security | 328 | $26.43B |
| GSA / VA / Interior | — | $4–5B each |

DOE's $91.6B is 6 contracts — Triad/Los Alamos ($35.6B), Bechtel/Hanford ($18.9B), Mission Support & Test Services/Nevada ($11.3B), Savannah River, Oak Ridge — facilities-support (NAICS 561210) and environmental-remediation (562910) M&O, **not** equipment-rental demand. **Civilian value ex-DOE = 4,134 awards / $55.7B.** Use **award count** as the rental-demand proxy.

The count cohort is genuinely construction: NAICS **236220** (commercial/institutional building) = 1,895 awards (45%), 237310 (highway/bridge), 237110 (water/sewer), 238xxx specialty trades. ~1,014 of 4,198 carry a non-NAICS-23 code (facilities/remediation/engineering with DB wage clauses).

---

## 2. Parachute Prime cohort (DoD + civilian combined)

| Metric | Count |
|---|---:|
| All active Davis-Bacon construction awards | 5,015 |
| └ with both HQ-state and PoP-state populated | 4,983 |
| **Out-of-state ("Parachute") — HQ state ≠ PoP state** | **2,781** (55.8% of geocoded) |
| └ civilian / DoD split | 2,326 / 455 |
| **🥇 Golden — Parachute + subcontracting plan** | **404** |
| └ civilian / DoD split | 282 / 122 |

The 404 golden awards carry $116.2B current / $131.5B potential — again DOE/DHS-skewed; **404 is the addressable target count.** The civilian slice of golden (282) is the cleanest secondary-campaign list: out-of-state prime + Davis-Bacon dirt + mandatory small-business subcontracting quota, non-DoD.

---

## 3. Civilian construction county hotspots (top 10 by award count)

| # | State | County | Awards | Current value | w/ plan |
|---|---|---|---:|---:|---:|
| 1 | DC | District of Columbia | 181 | $2.09B | 12 |
| 2 | MD | Montgomery | 148 | $1.81B | 7 |
| 3 | NY | Kings (Brooklyn) | 67 | $48.9M | 0 |
| 4 | CA | Los Angeles | 64 | $633.6M | 7 |
| 5 | NY | New York (Manhattan) | 63 | $304.2M | 5 |
| 6 | CA | San Diego | 51 | $2.37B | 6 |
| 7 | MD | Prince Georges | 41 | $642.8M | 5 |
| 8 | IL | Cook (Chicago) | 40 | $284.8M | 3 |
| 9 | HI | Honolulu | 35 | $209.6M | 4 |
| 10 | VA | Fairfax | 34 | $115.7M | 11 |

(3 civilian awards have null county/state.) **Read:** the National Capital Region dominates — DC + Montgomery + Prince Georges + Fairfax = 4 of the top 10 (GSA federal-building program + VA central). The rest are dense metros anchored by **VA medical hubs** (Brooklyn, Manhattan, LA, San Diego, Honolulu, Chicago). This maps to VA-hospital + GSA-building corridors, **not** national parks or rural highway segments (those disperse and don't concentrate by county).

### Bonus — local (≤50 mi) rental supply around the hotspots

Cross-referenced to `govcon_equipment_rental_construction_match` on `contract_award_unique_key` (the MV carries no county). **MV coverage caveat:** the MV demand side is gated to `naics_code LIKE '23%' AND active_potential`, so 3,168 of 4,198 civilian awards (75.5%) are present; the `awards_with_local_supply` column is read against that NAICS-23 ∩ active_potential subset, **not** the full cohort.

| State | County | Civilian awards | Awards w/ local supply* | Distinct local rental firms |
|---|---|---:|---:|---:|
| DC | District of Columbia | 181 | 152 | 429 |
| MD | Montgomery | 148 | 137 | 449 |
| NY | Kings (Brooklyn) | 67 | 8 | 222 |
| CA | Los Angeles | 64 | 52 | 300 |
| NY | New York (Manhattan) | 63 | 29 | 221 |
| CA | San Diego | 51 | 32 | 156 |
| MD | Prince Georges | 41 | 16 | 449 |
| IL | Cook (Chicago) | 40 | 26 | 184 |
| HI | Honolulu | 35 | 34 | 64 |
| VA | Fairfax | 34 | 10 | 422 |

\* Low ratios (Kings 8/67, Fairfax 10/34, Prince Georges 16/41) are **MV NAICS-23 coverage artifacts**, not supply deserts. **Supply is abundant in every hotspot** — 64–449 reachable rental firms within 50 mi. Supply is not the constraint; targeting is.

---

## Caveats shipped with the data

1. **`pop_unknown` excluded** by the "future end date" rule: 1,666 active-membership Davis-Bacon awards (1,379 civilian) have no PoP dates. Defensible per the brief; quantified, not silent.
2. **Dollar figures are M&O-skewed** (DOE 62% of civilian $). Count is the rental-demand proxy.
3. **MV bonus coverage = NAICS-23 ∩ active_potential** (75.5% of the civilian cohort); supply counts read against that subset.
4. **`supply_addr_is_hq_pin`**: national chains (United/Sunbelt/Herc) register one corporate HQ, not branch yards — their radius match is advisory (per the MV reference doc).

---

# PSC decomposition — the 2,781 Parachute cohort

**Harness:** [`scripts/parachute_psc_probe.py`](../../scripts/parachute_psc_probe.py). Reconciled: parachute = 2,781 (0 null PSC), golden-civilian = 282; macro categories sum to 2,781.

## Macro (Build vs. Fix)

| PSC cat | Meaning | Awards | % | Current value |
|---|---|--:|--:|--:|
| **Z** | Maintenance/Repair/Alteration of real property | **1,260** | 45.3% | $8.57B |
| **Y** | Construction of structures & facilities | **682** | 24.5% | $54.89B |
| F | Environmental / natural-resources (remediation) | 220 | 7.9% | $10.47B |
| N / J | Equipment install / equipment maint | 107 / 109 | 7.8% | $0.65B |
| C | Construction A&E | 77 | 2.8% | $0.73B |
| 6 | FSC supply (6350 security systems) | 69 | 2.5% | $0.44B |
| M / R | Facility M&O / mgmt support (dollar-skew, not rental) | 29 / 58 | 3.1% | $57.6B |

**Fix (Z) > Build (Y) 1.85 : 1 by count.** Y+Z = 69.8%; +F = 77.7% "physical work on real property."

## Top-20 exact PSC (62.2% of cohort)

Office repair **Z2AA (286)** leads; environmental remediation **F108 (174)** is #2; the hospital triplet (Y1DA 90 + Z2DA 115 + Z1DA 84 = **289**) is the largest single theme; heavy-civil roadwork **Y1LB (72)**. Y1PZ ($16.1B/75) and F999 ($9.1B/34) are mega-contract value outliers — size the fleet by count.

## Golden 282 — brief premise corrected

Top-5 PSC: **6350 security systems (58)**, Z2AA office repair (20), N059 electrical install (20), Y1JZ misc buildings (16), Y1PZ non-building facilities (12).

**Golden is DOT-led, not GSA/VA — and VA is absent.** By agency: **DOT 100** (6350 security 58 / N059 20 / Y1LB highways 11), DHS 41 (Y1PZ/Y1JZ border facilities), GSA 34 (Z2AA office repair 15 / Y1AA 4), Interior 22 (Y1NE water / Z2KA dams), EPA 19 (F108 remediation). VA leads the civilian cohort by count (907) but its SDVOSB/small-business set-asides are exempt from subcontracting plans, so VA drops out of the plan-gated Golden set. **GTM:** DOT's 6350/N059 (78 awards) are low rental-intensity — prioritize DHS Y1PZ/Y1JZ, Interior Y1NE/Z2KA, GSA Y1AA/Z2AA, DOT Y1LB instead.
