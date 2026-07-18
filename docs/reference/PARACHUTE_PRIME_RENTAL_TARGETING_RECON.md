# Parachute Prime — Equipment-Rental Targeting Recon (Target A / Target B)

**As-of:** 2026-06-22 (UTC) · **Source:** read-only DuckDB probe of `s3://data-sink/active/govcon_active_awards/` (189,272 prime awards; **148,789 active + future-end**) × `s3://data-sink/active/sba_dsbs_certified_firms/` (67,234 certified firms).
**Harness:** [`scripts/archive/parachute_prime_targeting_recon.py`](../../scripts/archive/parachute_prime_targeting_recon.py) — single R2 connection, one filtered base materialization, self-validating literal histograms. Re-run: `doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb python3 scripts/archive/parachute_prime_targeting_recon.py`.

> **Verdict: database is READY to wire into the Catalyst-API / LLM translation layer.** Every predicate the two profiles need exists, is indexed, and was confirmed against live literals. **Two schema hurdles** must be encoded in the translation layer (state-code↔name mapping; NAICS depth via `naics_all_codes`) and **one architecture rule** must hold on the map (supply join is **radius**, not state equality). All three are detailed in §5.

---

## 1. Data readiness & schema verification

### 1.1 The "Out-of-State" predicate — CONFIRMED

| Concept | Column | Format | Live coverage (active+future) |
|---|---|---|---|
| Vendor / Recipient HQ state | `recipient_state_code` | 2-char USPS code (`CA`,`TX`,…) | 145,558 non-null |
| Place-of-Performance state | `pop_state_code` | 2-char USPS code | 143,920 non-null |
| **Out-of-State prime** | `recipient_state_code <> pop_state_code` | both non-null, trimmed/upper | **43,350 awards** (30.2% of the 143,746 with both states present) |

Both are the **same 2-char namespace** → the HQ≠PoP comparison is apples-to-apples with no normalization inside GAA. (3,231 rows null HQ-state, 4,869 null PoP-state — excluded from the parachute denominator.)

### 1.2 Exclusion filters — CONFIRMED (the "Alarm Guys" + VA carve-outs work)

| Filter | Predicate | Active rows it touches |
|---|---|---|
| Security/alarm FSC | `psc_code = '6350'` | 330 |
| Equipment installation | `psc_code LIKE 'N0%'` | 544 |
| Veterans Affairs | `awarding_agency_name = 'Department of Veterans Affairs'` | 16,858 |

**Schema hazard caught:** agency must be matched by **exact equality**, not `LIKE '%…%'`. `LIKE '%TRANSPORTATION%'` leaks **"National Transportation Safety Board"** into the DOT set; `LIKE '%VETERANS%'` is safe but equality is the disciplined form. The five canonical literals are frozen below.

### 1.3 Frozen agency literals (exact strings, live counts)

| GTM agency | Exact `awarding_agency_name` | Active awards |
|---|---|---:|
| GSA | `General Services Administration` | 38,161 |
| VA *(excluded)* | `Department of Veterans Affairs` | 16,858 |
| DOI | `Department of the Interior` | 6,564 |
| DHS | `Department of Homeland Security` | 5,320 |
| DOT | `Department of Transportation` | 3,844 |

### 1.4 "Construction" is not one column — it is three signals

There is no single `is_construction` flag. The three independent, reliable signals (all present + indexed):

| Signal | Predicate | Active awards |
|---|---|---:|
| Davis-Bacon prevailing wage | `construction_wage_rate_requirements = 'YES'` (code `Y`) | 5,015 |
| Construction NAICS sector | `naics_code LIKE '23%'` | 5,987 |
| PSC real-property work | `psc_category IN ('Y','Z')` (build / repair-alter) | 6,843 |
| **Union (any of the three)** | — | **9,414** |

**Decision (Target A construction gate = the union).** The union is the defensible "physical work on real property" definition and is the one under which the 6350/N0* exclusions do real work. The per-signal sensitivity is in §2.2 so the map default can be tuned. **Target B needs none of this** — it is pinned to `psc_code = 'Z2AA'` per the brief.

---

## 2. The volume drop-off (Broad vs Strict)

`Broad = Out-of-State prime + Agency + Construction (+ 6350/N0*/VA exclusions). Strict = Broad + has_subcontracting_plan = true.`
Value column = `current_total_value_of_award` (canonical; `potential_total_value_of_award` shown as the option-ceiling).

### 2.1 Headline

| Profile | Query | Awards | Current value | Potential value |
|---|---|---:|---:|---:|
| **A — Heavy Earthmoving** (DHS/DOI/DOT + construction) | Broad | **923** | **$27.06B** | $27.58B |
| | Strict (+ plan) | **97** | $10.60B | $10.74B |
| **B — Aerial & Power** (GSA + PSC Z2AA) | Broad | **301** | **$1.14B** | $1.17B |
| | Strict (+ plan) | **18** | $708.7M | $722.2M |

### 2.2 Drop-off — the plan constraint is brutal

| Profile | Broad → Strict | Retained | **Lost** |
|---|---|---:|---:|
| **A** | 923 → 97 | **10.5%** | 826 awards (89.5%) |
| **B** | 301 → 18 | **6.0%** | 283 awards (94.0%) |

**`has_subcontracting_plan` deletes ~90% of the map.** A subcontracting plan is only mandated on large unrestricted awards above the FAR threshold; the parachute cohort is dominated by **small-business set-asides** (plan-exempt) and **sub-threshold** awards. By dollars the loss is milder (A keeps 39% of value; B keeps 62%) because plans cluster on the few mega-awards — confirming the plan flag is a *size* proxy, not a demand proxy.

> **Map-default recommendation:** default the map to **Broad**, expose `has_subcontracting_plan` as an optional "socioeconomic-compliance" toggle. Defaulting to Strict would hide 89–94% of live rental demand.

### 2.3 Target A construction-definition sensitivity (tune the map default)

Out-of-state + DHS/DOI/DOT + exclusions held constant; only the construction gate varies.

| Construction gate | Broad awards | Broad value | Strict awards |
|---|---:|---:|---:|
| Davis-Bacon only | 614 | $26.52B | 84 |
| NAICS `23%` only | 778 | $25.82B | 70 |
| PSC `Y`/`Z` only | 698 | $25.81B | 79 |
| **Union (any) — selected** | **923** | **$27.06B** | **97** |

The three signals are materially disjoint (union 923 ≫ largest single signal 778). NAICS-23 is the strongest standalone; the union recovers ~145 awards the NAICS sector code alone misses (Davis-Bacon and PSC-Y/Z jobs filed under non-23 NAICS).

---

## 3. Geographic & entity distribution (combined Broad: A ∪ B = 1,224 awards)

The two profiles are **agency-disjoint** (A = DHS/DOI/DOT, B = GSA) so the union is clean: 923 A + 301 B = 1,224.

### 3.1 Top PoP states by volume (rental-demand footprint)

| # | PoP state | Awards | of which A / B | Current value |
|---|---|---:|---:|---:|
| 1 | **DC** | **128** | 59 / 69 | $884.4M |
| 2 | **CA** | **84** | 76 / 8 | $4.98B |
| 3 | **TX** | **60** | 47 / 13 | $15.15B |
| 4 | **NY** | **50** | 23 / 27 | $239.7M |
| 5 | **AZ** | **48** | 42 / 6 | $1.13B |
| 6 | FL | 47 | 44 / 3 | $209.8M |
| 7 | VA | 47 | 42 / 5 | $557.5M |
| 8 | MD | 42 | 25 / 17 | $412.8M |

**Clean segmentation for the map:** Target B (GSA office repair) concentrates in the **urban federal-building corridors** — DC and NY are B-heavy. Target A (earthmoving) concentrates on **border + western public lands** — CA/TX/AZ/FL are A-dominant. The map's two layers have distinct geographies.

### 3.2 Top primes — these are MID-MARKET firms, not billion-dollar primes

Ranked by cohort award frequency; size proxied by the firm's **total active federal portfolio**.

| # | Prime | Size | Cohort awards | Total active portfolio | PoP states |
|---|---|---|---:|---:|---:|
| 1 | PROJECT SOLUTIONS, INC. | SMALL | 18 | $12.1M | 13 |
| 2 | PROCON CONSULTING LLC | SMALL | 16 | $48.2M | 13 |
| 3 | KIK TECHNOLOGIES LLC | SMALL | 13 | $5.8M | 3 |
| 4 | TK ELEVATOR CORPORATION | OTHER | 12 | $21.7M | 12 |
| 5 | SKOOKUM EDUCATIONAL PROGRAMS | OTHER | 12 | $162.2M | 2 |
| 6 | EMCOR GOVERNMENT SERVICES | OTHER | 11 | $206.0M | 1 |
| 7 | PARLIAMENT LLC | SMALL | 11 | $28.2M | 5 |
| 8 | KCORP SUPPORT SERVICES | SMALL | 11 | $0.37M | 1 |
| 9 | MC DODD CONSTRUCTION LLC | SMALL | 9 | $15.8M | 3 |
| 10 | BKM CONSTRUCTION LLC | SMALL | 9 | $25.7M | 2 |

**Answer to the brief's question: mid-market.** 7 of 10 most-frequent parachute primes are **SMALL BUSINESS**; the largest total active portfolio in the top-10 is $206M (EMCOR) — none are billion-dollar primes. The repeat players are **multi-state specialty contractors** (project/construction management, elevators, facility services) operating across up to **13 PoP states** from a single HQ — the literal parachute profile, and they need local rental in every state they touch. The mega-*dollars* live in a handful of large awards (§3.3), not with these frequent flyers — **rank rental demand by award count, not value.**

### 3.3 Anomalies (non-obvious concentration)

- **DC is 10.4% of the entire combined cohort in one county** (127 awards / $884M) — driven by Target B (GSA office repair, 69 awards) + DHS HQ. The single densest demand pin.
- **TX / Hidalgo County: 11 awards but $3.06B** — DHS border-construction mega-contracts. Earthmoving-relevant, value-dominant, count-light.
- **WY / Park County: 14 awards / $328.7M** — DOI / Yellowstone gateway infrastructure. A genuinely **rural** parachute hotspot (out-of-state primes mobilizing to a national-park county).
- **GA / Glynn County: 15 awards** — FLETC (Federal Law Enforcement Training Center, DHS, Brunswick GA). A non-obvious single-installation magnet.
- **CA / San Diego: 14 awards / $2.14B** — border + federal-facility corridor.

Read: outside DC, the demand **disperses** into border counties (Hidalgo, San Diego) and public-lands counties (Park) where a few mega-awards dominate dollars — the rural/border worksites where local certified rental supply is thinnest and the pitch is strongest.

---

## 4. DSBS supply-side match (certified small-business rental yards)

Filter: `naics_all_codes` contains **532412** (Construction Machinery Rental) or **532310** (General Rental Centers), in the Top-5 PoP states from §3.1 (DC, CA, TX, NY, AZ).

### 4.1 NAICS depth matters — primary code alone undercounts 12×

| Scope | By `naics_primary` only | By `naics_all_codes` |
|---|---:|---:|
| Nationwide certified rental yards | **70** | **842** |
| └ 532412 (construction machinery) | — | 737 |
| └ 532310 (general rental centers) | — | 191 |

Rental capability is almost always a **secondary** NAICS on a certified firm (they certify under a primary trade and carry 532xxx among their `naics_all_codes`). **The translation layer must query `naics_all_codes`, never `naics_primary`.**

### 4.2 In the Top-5 demand footprint: **238 certified rental yards**

| State | Certified rental firms | (primary-NAICS subset) | 532412 | 532310 |
|---|---:|---:|---:|---:|
| California | **108** | 16 | 95 | 20 |
| Texas | **93** | 4 | 79 | 23 |
| New York | **18** | 1 | 14 | 5 |
| Arizona | **17** | 2 | 14 | 4 |
| District of Columbia | **2** | 0 | 1 | 1 |
| **Top-5 total** | **238** | 23 | — | — |

**Double-value overlay (all 238 carry ≥1 active SBA certification — they ARE the certified registry):** VOSB 149 · SDVOSB 142 · WOSB 67 · HUBZone 32 · EDWOSB 21 · 8(a) 17. A certified rental yard local to an out-of-state prime is a **two-sided pitch**: equipment capability **and** small-business/socioeconomic subcontracting credit for the prime.

### 4.3 The DC supply-desert is a state-match artifact, not a real gap

DC is the **#1 demand state (128 awards)** but shows **2** in-state certified rental yards. DC is ~68 sq mi — its worksites are served by the MD/VA metro across the state line. The prior radius-matched MV ([`govcon_equipment_rental_construction_match`](EQUIPMENT_RENTAL_CONSTRUCTION_MATCH.md)) found **429 reachable rental firms within 50 mi of DC worksites**. **State-equality supply matching fails for DC, NY-metro, and every border worksite.** The live map must join supply by **drive-radius**, not `pop_state = firm_state` (§5, hurdle 3).

> **Thesis proven, with one correction:** there is an active, targetable, *certified* small-business rental supply sitting in the parachute footprint — **238 certified yards across the top-5**, **842 nationwide** — concentrated exactly where Target-A earthmoving demand is (CA 108, TX 93). The only place state-match shows a gap (DC) is precisely where radius-match shows abundance.

---

## 5. Schema hurdles for the LLM translation layer (the engineering output)

| # | Hurdle | Rule the translation layer must encode |
|---|---|---|
| **H1** | **State namespace mismatch.** GAA = 2-char USPS code (`pop_state_code`,`recipient_state_code`); DSBS `state` = **full name** ("California"). | Carry a code↔name map (the 56-entry dict is in the probe). Any DSBS supply filter from a GAA state needs `code → full-name` translation; match `upper(trim(state))` against {name, code} for safety. |
| **H2** | **NAICS depth.** `naics_primary` captures 70 rental yards; the real 842 live in `naics_all_codes` (pipe-delimited 6-digit tokens). | Rental/equipment capability filters run against `naics_all_codes LIKE '%<code>%'` (safe — fixed-width tokens). Never gate supply on `naics_primary` alone. |
| **H3** | **Supply join is radius, not state.** State-equality buries cross-border supply (DC←MD/VA). | The map's supply layer joins through `govcon_equipment_rental_construction_match` (haversine ×1.3 road factor, `tier='local'` ≤50mi), not `pop_state = firm_state`. State counts are a coarse pre-filter only. |
| **H4** | **Agency = exact equality.** `LIKE '%TRANSPORTATION%'` leaks NTSB into DOT. | Translate agency intents to the five frozen literals in §1.3 (`IN (...)`), never substring `LIKE`. |
| **H5** | **PSC null-safety.** Exclusions (`6350`,`N0*`) and the Z2AA pin must not silently drop null-PSC construction awards. | Apply exclusions as `coalesce(psc_code,'') <> '6350' AND coalesce(psc_code,'') NOT LIKE 'N0%'` (keeps null-PSC); Target B's `psc_code='Z2AA'` is an intentional inclusion pin. |
| **H6** | **"Construction" is a 3-signal union, not a column.** | Target A = `(naics_code LIKE '23%' OR construction_wage_rate_requirements='YES' OR psc_category IN ('Y','Z'))`. Expose the gate as a tunable map default (§2.3). |
| **H7** | **Value is mega-contract-skewed.** A few border/M&O awards dominate dollars. | Size rental demand by **award count**; reserve `current_total_value_of_award` for prioritization, not for demand density. |

---

## 6. Caveats shipped with the data

1. **`active + future-end`** = `active_current OR active_potential` (GREATEST(end) ≥ as-of). Excludes `pop_unknown` (no end date) per the "future end date" rule — same boundary as the locked Parachute Prime cohort.
2. **DSBS state nullity:** 1,981 of 67,234 certified firms have null `state` — excluded from the footprint match (cannot be geo-placed).
3. **DSBS = certified registry only.** §4 counts *certified* small-business yards. The broader (non-certified) rental supply is larger; see the SAM-sourced `govcon_equipment_rental_construction_match` for the full reachable supply universe.
4. **`naics_all_codes` ≠ active-line-of-business.** A carried NAICS is a claimed capability, not proof the firm actively rents today — qualify on outreach. National chains may register one HQ (the `supply_addr_is_hq_pin` caveat in the rental-match MV applies downstream).
5. **No subcontracting-plan inference.** `has_subcontracting_plan` is the FPDS award-stamped flag; absence ≠ no subbing, it means no *mandated* plan (set-aside/sub-threshold). Hence Broad is the demand-true default.
