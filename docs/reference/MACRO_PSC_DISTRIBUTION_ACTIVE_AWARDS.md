# Macro PSC distribution across active federal awards — GTM vertical-prioritization probe

**Mode:** READ-ONLY probe → report. **Snapshot:** 2026-06-21 (UTC). **Source:** `s3://data-sink/active/govcon_active_awards/` (Lance SoR).
**Probe:** `scripts/psc_distribution_probe.py` — single R2 connection, one filtered base temp table, all aggregations off it, self-validating categorical histograms + in-script consistency assertions. Raw JSON: `/tmp/psc_probe.json`.

**Active cohort (membership):** `active_current OR active_potential` — i.e. the directive's *"active_potential = true OR pop_current_end >= current_date."* `pop_unknown` awards (both PoP ends NULL) are out of scope by this definition; **0** such awards exist inside active membership, so nothing is silently dropped.

**Column note:** `govcon_active_awards` renames the raw FPDS fields — `product_or_service_code → psc_code`, `product_or_service_code_description → psc_description` — and pre-derives `psc_category` (first char), `psc_is_service` (first char `[A-Z]`), and `has_subcontracting_plan` (`subcontracting_plan_code ∈ {C,D,E,F,G,H}`). The probe re-derives the first-char class from raw `psc_code` and confirms it matches the stored `psc_is_service` (0 mismatches).

**Universe:** **148,789 active prime awards · $1,607.52B current value · $2,182.10B potential ceiling.** Every active award carries a non-null PSC (0 null/empty).

---

## 1. The Macro Split — Services vs Products

First character of `psc_code`: **letter (A–Z) = Services**, **digit (0–9) = Products**.

| Bucket | Award count | % of count | Current value | % of value | Potential ceiling |
|---|---:|---:|---:|---:|---:|
| **Services** (letter) | 69,907 | 46.98% | **$1,313.48B** | **81.71%** | $1,840.99B |
| **Products** (digit) | 78,882 | 53.02% | $294.04B | 18.29% | $341.10B |
| **Total** | **148,789** | 100% | **$1,607.52B** | 100% | $2,182.10B |

**The headline asymmetry:** Products are the majority by *volume* (53%) but Services hold **4.5×** the dollar value ($1.31T vs $294B) and capture **82% of all active obligated value.** Products skew high-frequency / low-ticket (vehicles, tools, office & medical supplies — see §2); Services skew lower-frequency / high-ticket (support, IT, facility operation). **The money — and the teaming/labor leverage — is in Services.**

### Top 5 Services top-level categories (by award count)

| Rank | Letter | Category | Award count | % of services | Current value |
|---:|:---:|---|---:|---:|---:|
| 1 | **R** | Professional, Administrative & Management Support | 16,003 | 22.9% | $182.94B |
| 2 | **D** | IT & Telecommunications Services | 13,071 | 18.7% | $109.17B |
| 3 | **S** | Utilities & Housekeeping Services | 6,563 | 9.4% | $35.79B |
| 4 | **J** | Maintenance, Repair & Rebuilding of Equipment | 6,460 | 9.2% | $20.15B |
| 5 | **Z** | Maintenance, Repair & Alteration of Real Property | 5,122 | 7.3% | $27.33B |

**R + D alone = 29,074 awards (41.6% of all services).** These two letters *are* the services market by count — Professional Support (R) and IT (D) are the two largest addressable service verticals in the active landscape.

**Value-concentration caveat (not in the count top-5, but where the dollars hide):** by *value*, the heaviest service letters are **M — Operation of Government-Owned Facilities** (659 awards / **$487.20B**) and **A — Research & Development** (2,607 awards / **$230.44B**). These are mega-vehicle cohorts (GOCO national labs, large R&D programs) — enormous dollar concentration on few awards, **not SMB/staffing-targetable.** Exclude them when sizing a teaming/staffing TAM; include them only for enterprise-prime targeting.

---

## 2. The Micro "Top 25" — overall heavyweight PSC codes

Grouped by exact 4-char `psc_code` (label = modal `psc_description`; `distinct_descriptions = 1` for all 25, so no code is fragmented across FPDS text variants). Ranked by active-award count.

| # | PSC | Description | Type | Award count | Current value | Potential ceiling |
|---:|:---|:---|:--:|---:|---:|---:|
| 1 | 2310 | Passenger Motor Vehicles | P | 16,478 | $1.03B | $1.03B |
| 2 | DG11 | IT & Telecom — Network: Satellite Comms & Telecom Access Svcs | S | 5,098 | $3.78B | $112.75B |
| 3 | R499 | Support — Professional: Other | S | 4,276 | $73.25B | $110.66B |
| 4 | 4220 | Marine Lifesaving & Diving Equipment | P | 3,234 | $1.32B | $1.32B |
| 5 | 6515 | Medical & Surgical Instruments, Equipment & Supplies | P | 3,202 | $1.10B | $2.78B |
| 6 | 7110 | Office Furniture | P | 2,562 | $0.11B | $0.12B |
| 7 | 5120 | Hand Tools, Nonedged, Nonpowered | P | 2,447 | $0.005B | $0.005B |
| 8 | 7A21 | IT & Telecom — Business Application SW (Perpetual License) | S | 2,294 | $4.28B | $8.25B |
| 9 | 7510 | Office Supplies | P | 2,281 | $0.03B | $0.03B |
| 10 | DA10 | IT & Telecom — Business App / App Dev Software-as-a-Service | S | 2,245 | $8.50B | $20.66B |
| 11 | J065 | Maint/Repair/Rebuild — Medical, Dental & Veterinary Equip | S | 2,078 | $0.93B | $1.50B |
| 12 | 7350 | Tableware | P | 2,061 | $0.002B | $0.002B |
| 13 | 5210 | Measuring Tools, Craftsmen's | P | 1,819 | $0.003B | $0.003B |
| 14 | DA01 | IT & Telecom — Business App / App Dev Support Svcs (Labor) | S | 1,796 | **$41.57B** | $70.07B |
| 15 | R408 | Support — Professional: Program Management/Support | S | 1,747 | $14.90B | $22.47B |
| 16 | Z1AA | Maintenance of Office Buildings | S | 1,607 | $1.96B | $3.17B |
| 17 | 1560 | Airframe Structural Components | P | 1,557 | $2.42B | $2.42B |
| 18 | R425 | Support — Professional: Engineering/Technical | S | 1,492 | **$47.98B** | $79.81B |
| 19 | 6640 | Laboratory Equipment & Supplies | P | 1,429 | $0.29B | $0.35B |
| 20 | S201 | Housekeeping — Custodial/Janitorial | S | 1,322 | $1.45B | $2.13B |
| 21 | 5340 | Hardware, Commercial | P | 1,273 | $0.06B | $0.09B |
| 22 | 6525 | Imaging Equipment & Supplies: Medical, Dental, Veterinary | P | 1,271 | $0.51B | $0.56B |
| 23 | 5110 | Hand Tools, Edged, Nonpowered | P | 1,143 | $0.0008B | $0.0008B |
| 24 | DG10 | IT & Telecom — Network as a Service | S | 1,024 | $2.08B | $11.35B |
| 25 | S206 | Housekeeping — Guard | S | 1,002 | $7.67B | $13.61B |

**Read:** The #1 code (2310 Passenger Motor Vehicles, 16,478 awards) is a **volume artifact** — federal fleet POs, ~$62K average — not a strategic vertical. The strategic density is **IT services (DG11, 7A21, DA10, DA01, DG10 — 5 of the top 25)** and **Professional Support (R499, R408, R425)**. Three rows carry the real money: **R499 $73.25B, R425 $47.98B, DA01 $41.57B** — high count *and* high value = the prime targeting sweet spot. Commodity products (tools, tableware, office supplies) appear by count but are sub-$50M total markets — ignore for vertical campaigns.

---

## 3. Subcontracting-quota hotspots — `has_subcontracting_plan = true`

Mandatory-plan cohort = `subcontracting_plan_code ∈ {C,D,E,F,G,H}` (FPDS legend below). **26,573 active awards carry a mandatory small-business subcontracting plan · $1,329.62B current value** — the prime obligations the government is actively forcing.

**FPDS plan-code legend (verified in-data, mandatory codes bolded):** A = no plan (no possibilities) · B = plan not required · **C = required, incentive not included** · **D = required, incentive included** · **E = required (pre-2004)** · **F = individual plan** · **G = commercial plan** · **H = DoD comprehensive plan**. Cohort composition: G 17,009 · F 8,311 · H 752 · C 451 · D 32 · E 18.

### Top 15 PSC codes by mandatory-plan frequency

| Rank | PSC | Description | Mandatory-plan awards | Current value |
|---:|:---|:---|---:|---:|
| 1 | 2310 | Passenger Motor Vehicles | 11,110 | $0.59B |
| 2 | 6525 | Imaging Equipment & Supplies: Medical/Dental/Vet | 890 | $0.31B |
| 3 | R499 | Support — Professional: Other | 837 | **$48.30B** |
| 4 | 6515 | Medical & Surgical Instruments, Equip & Supplies | 426 | $0.45B |
| 5 | R425 | Support — Professional: Engineering/Technical | 414 | **$33.18B** |
| 6 | DA01 | IT & Telecom — Business App / App Dev Support (Labor) | 395 | **$25.39B** |
| 7 | 6505 | Drugs & Biologicals | 344 | $0.67B |
| 8 | C219 | Architect & Engineering — General: Other | 314 | $1.09B |
| 9 | J065 | Maint/Repair/Rebuild — Medical/Dental/Vet Equip | 279 | $0.57B |
| 10 | R408 | Support — Professional: Program Management/Support | 275 | $5.78B |
| 11 | S112 | Utilities — Electric | 243 | $2.22B |
| 12 | DG11 | IT & Telecom — Network: SATCOM & Telecom Access | 227 | $3.52B |
| 13 | 1680 | Miscellaneous Aircraft Accessories & Components | 220 | $3.17B |
| 14 | 6530 | Hospital Furniture, Equipment, Utensils & Supplies | 205 | $0.003B |
| 15 | S222 | Housekeeping — Waste Treatment/Storage | 201 | $0.03B |

**Read:** #1 (2310, 11,110 plans) is again a **vehicle-fleet count artifact** — commercial subcontract plans (code G) attached to automaker fleet buys; high frequency, trivial teaming value ($53K avg). The **real teaming markets are the services rows: R499 ($48.30B), R425 ($33.18B), DA01 ($25.39B), R408 ($5.78B)** — these are where a mandated prime is *forced* to find small-business subs on large-dollar professional/IT/engineering work. **That trio (R499 / R425 / DA01) = the subcontracting-entry target list.**

---

## 4. Service / staffing hotspots — `labor_standards = 'YES'` (Service Contract Act)

SCA flag = guaranteed labor-dependent services contract (janitorial, guard, basic IT, admin, facility ops). Confirmed literal: `labor_standards` raw values are `NOT APPLICABLE` (95,719), `NO` (31,544), **`YES` (21,525)**, `''` (1). **SCA cohort: 21,525 active awards · $324.01B current value** — the labor-driven services TAM.

### Top 10 PSC codes within the SCA cohort

| Rank | PSC | Description | SCA awards | Current value |
|---:|:---|:---|---:|---:|
| 1 | Z1AA | Maintenance of Office Buildings | 1,348 | $1.58B |
| 2 | R499 | Support — Professional: Other | 1,283 | $21.27B |
| 3 | S201 | Housekeeping — Custodial/Janitorial | 875 | $1.11B |
| 4 | S206 | Housekeeping — Guard | 782 | $5.58B |
| 5 | S222 | Housekeeping — Waste Treatment/Storage | 646 | $0.03B |
| 6 | R408 | Support — Professional: Program Management/Support | 504 | $2.59B |
| 7 | F003 | Natural Resources/Conservation — Forest/Range Fire Suppression | 502 | $1.56B |
| 8 | R425 | Support — Professional: Engineering/Technical | 491 | $10.48B |
| 9 | S216 | Housekeeping — Facilities Operations Support | 472 | $1.55B |
| 10 | R699 | Support — Administrative: Other | 400 | $1.69B |

**Read — the staffing-agency outreach list:** SCA splits cleanly into two motions. **Building/facility labor** (Z1AA, S201, S206, S222, S216 — janitorial, guard, waste, facility ops) is the classic blue-collar staffing wedge — high headcount, low ticket. **Professional support labor** (R499 $21.27B, R425 $10.48B, R408, R699) is the higher-value white-collar staffing wedge. **R499 and R425 appear in all three lists (top-25, subcontracting, SCA)** — they are the single most universally addressable service codes in the active landscape and should anchor the first non-construction campaign.

---

## 5. Cross-vertical synthesis (campaign prioritization)

| Vertical | Anchor PSCs | Active awards | Active value | Why now |
|---|---|---:|---:|---|
| **Professional Support (R)** | R499, R425, R408, R699 | 16,003 (R total) | $182.94B | Largest service letter by count; dominates subcontracting *and* SCA; R499/R425 are the universal codes. |
| **IT Services (D + 7A21)** | DA01, DA10, DG11, DG10, 7A21 | 13,071 (D total) | $109.17B | 5 of top-25 codes; DA01 carries $41.57B and a mandatory-plan obligation — IT teaming entry. |
| **Facility / Janitorial / Guard (S + Z1AA)** | S201, S206, S216, S222, Z1AA | ~8,170 | ~$37B | The blue-collar SCA staffing wedge — high-headcount, recurring labor. |
| **Construction / RPMA (Y + Z)** | Y, Z1AA | 6,843 (Y+Z) | $103.01B | Existing vertical; Z (RPMA) overlaps facility-maintenance SCA. |

**Recommended first non-construction campaign: Professional Support (R) + IT Services (D), anchored on R499 / R425 / DA01.** These codes maximize the intersection of award volume, dollar value, mandatory-subcontracting obligation, and SCA labor dependence — the four signals that independently predict a teaming/staffing buyer.

---

## 6. Validation (self-checks that passed)

- **Partition closure:** services (69,907) + products (78,882) + other (0) = base (148,789), asserted in-script.
- **Derived-column agreement:** stored `psc_is_service` vs raw-first-char rule → **0 mismatches**.
- **Subcontracting flag integrity:** `has_subcontracting_plan` vs `code ∈ {C,D,E,F,G,H}` → **0 mismatches**; component codes sum to 26,573.
- **Value closure:** services value + products value = base value ($1,607.52B), exact.
- **PSC coverage:** 0 null/empty `psc_code` in the active cohort.
- **Literal confirmation:** `labor_standards` and `subcontracting_plan_code` raw histograms emitted (not assumed).

Reproduce: `doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb python3 scripts/psc_distribution_probe.py`.
