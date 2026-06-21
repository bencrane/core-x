# "Cash Crunch" indicators — Small-Business capital-lending TAM

**Mode:** READ-ONLY probe → report. **Snapshot:** 2026-06-21 (UTC). **Recency window:** last 90 days (≥ 2026-03-23).
**Sources:** `govcon_active_awards` (full-coverage award substrate) ⨝ `govcon_award_scope_requirements` (PDF extractions, partial coverage).
**Probe:** [`scripts/cash_crunch_probe.py`](scripts/cash_crunch_probe.py) — single R2 connection, SB base temp table, self-validating business-size histogram + explicit join-coverage denominators. Raw JSON: `/tmp/cash_crunch.json`.

**Small-Business definition (authoritative FPDS determination):** `business_size_code='S'` (raw `business_size='SMALL BUSINESS'`). Confirmed literal: S = **83,400**, O = 65,388, blank = 1 (active cohort = 148,789). Expanding to "any small/disadvantaged set-aside flag" adds only **+949 awards (+1.1%)** — the COD determination already captures the universe, so all sections run on it.

**Active cohort:** `active_current OR active_potential` (future end dates; `pop_unknown` = 0 inside membership).

---

## 0. The base & the load-bearing caveat

| | awards | current value |
|---|---:|---:|
| **All active awards** | 148,789 | $1,607.52B |
| **Small-Business primes (active)** | **83,400** (56.1%) | **$152.20B** |
| SB awards with any PDF extraction (`govcon_award_scope_requirements`) | **7,051** | — |

**⚠ Coverage floor:** PDF-derived scope requirements exist for only **8.5% of the SB cohort.** Sections **2 (equipment)** and **3 (bonding)** are computed *only* over awards whose solicitation attachments were harvested + extracted — they are **floors, not censuses.** Section **1 (Whale Win)** is the exception: it is computed entirely from full-coverage `govcon_active_awards` fields (PSC, `labor_standards`, current value) and does **not** depend on extraction coverage — it is the most reliable of the three markets.

---

## 1. The "Whale Win" — payroll-mobilization crunch

**Cohort:** SB AND (`psc_code ∈ {DA01, R499, R425}` OR `labor_standards='YES'`). **Full-coverage signal** (no PDF dependency).

| Metric | Awards | Current value |
|---|---:|---:|
| **Total Whale cohort** | **17,922** | **$78.00B** |
| current value **> $5M** | **1,948** | **$68.43B** |
| current value **> $10M** | **1,144** | **$62.73B** |

**Composition:** via `labor_standards='YES'` (SCA) = 14,508 · via Big-Three PSC = 4,459 · both = 1,045. The cohort is **labor-driven services**, dominated by the Service Contract Act flag — exactly the heavy-payroll profile a payroll funder / factoring desk underwrites.

**Read:** 1,948 small businesses are each carrying an active labor-services contract worth **>$5M** ($68.43B aggregate), and 1,144 of them **>$10M** ($62.73B). The >$5M cut alone holds **88% of the cohort's dollar value in 11% of its awards** — a tight, high-value target list for payroll mobilization financing. This is the largest and cleanest of the three lending markets.

---

## 2. The "Iron Crunch" — asset-based finance & leasing

**Cohort:** SB AND `construction_wage_rate_requirements='YES'` (Davis-Bacon construction), joined to PDF extractions.

| Metric | Awards | Current value |
|---|---:|---:|
| SB Davis-Bacon construction (full coverage) | **4,073** | **$17.25B** |
| …joined to scope-requirements (PDF coverage) | 687 (16.9%) | — |
| …**with populated `equipment_capability`** | **139** | **$794.24M** |

**Equipment Top 10** (139-award group; 238 occurrences, **218 distinct strings**):

| # | equipment string | award occurrences |
|---:|:---|---:|
| 1 | fire_extinguisher | 4 |
| 2 | spark_arrester | 4 |
| 3 | motorized_sweeper | 3 |
| 4 | scheduling_software:primavera_p6_or_ms_project | 3 |
| 5 | water_tank_truck | 3 |
| 6 | water_tank_truck_or_trailer | 3 |
| 7 | calibrated_test_equipment | 2 |
| 8 | copper_conductors_only_no_aluminum | 2 |
| 9 | negative_air_containment | 2 |
| 10 | negative_air_devices | 2 |

**⚠ Honest read — this field does not serve the equipment-finance thesis.** `equipment_capability` is a **free-form LLM field with 91.6% cardinality** (218 distinct in 238 occurrences) — the same high-noise field flagged in the labor-extraction audit. The surfaced items are **compliance/safety gear and one-off spec callouts** (fire extinguishers, spark arresters, "copper conductors only"), **not the financeable heavy iron** (excavators, dozers, cranes, loaders) an equipment lessor underwrites. Note the fragmentation: `water_tank_truck` and `water_tank_truck_or_trailer`, `negative_air_containment` and `negative_air_devices` are the same concept split across strings. **Do not size an equipment-leasing TAM from this array.** The credible asset-finance signal is the **SB Davis-Bacon construction cohort itself — 4,073 awards / $17.25B** (full coverage); these primes must mobilize equipment to perform regardless of whether a PDF happened to enumerate it.

---

## 3. The "Bonding & Insurance" crunch — surety / credit brokers

**Cohort:** entire active SB cohort (services + construction), joined to PDF extractions.

| Metric | Awards | Current value |
|---|---:|---:|
| SB awards joined to scope-requirements | 7,051 | — |
| …**with populated `insurance_bonding`** | **790** | **$5.30B** |

**Bonding/insurance Top 12** (790-award group; **84 distinct strings — semi-controlled, clean**):

| bonding/insurance | award occurrences | bonding/insurance | award occurrences |
|:---|---:|:---|---:|
| payment_bond | 556 | insurance:workers_compensation | 101 |
| bid_bond | 499 | insurance:professional_liability(:1) | 65 |
| performance_bond | 493 | payment_bond:100pct | 16 |
| insurance:general_liability | 148 | insurance:general_liability:5000000 | 15 |
| insurance:automobile_liability | 111 | performance_bond:100pct | 13 |

**Read:** This is the **cleanest PDF-derived signal of the three** (84 distinct values vs equipment's 218; controlled `value_norm_hints` forms). **790 small businesses carry explicit, citable bonding/insurance mandates** extracted from their solicitations, **$5.30B** in active value — the payment/performance/bid-bond trio dominates (the classic Miller Act surety stack). Every row is evidence-quote-backed, so a surety/credit broker gets a citable hook per lead. **Floor caveat applies:** this is 790 of the 8.5%-covered slice; the true SB bonding population is materially larger than what extraction has reached.

---

## 4. Recency — the "Need Cash NOW" pipeline

Across the three crunch cohorts, awards with a `latest_action_date` (latest modification/funding action) in the **last 90 days**:

| Crunch cohort | Awards | Recent (≤90d) | Recent value |
|---|---:|---:|---:|
| **A — High-Value Services** (Whale >$5M) | 1,948 | **1,451** (74.5%) | $43.11B |
| **B — Equipment-Mandated Construction** | 139 | **110** (79.1%) | $655.01M |
| **C — Bond-Mandated** | 790 | **650** (82.3%) | $4.67B |
| **Union (deduped — hand to a lender tomorrow)** | **2,690** | **2,060** | **$46.34B** |

**Read — the headline number: 2,060 distinct small-business awards ($46.34B current value) had a funding or modification action in the last 90 days** across the three crunch profiles. The recency rate is high (75–82%) because large active contracts receive frequent **funding-increment modifications** — so "recent action" here reads as *live capital movement on the contract*, which is precisely the buying signal a lender wants (not merely new signings). Every SB award has a parseable action date (`act_null=0`), so this count is exact within the cohort definitions.

---

## 5. Synthesis — which lending product the data actually supports

| Product | Cohort | Awards | Active value | Coverage | Verdict |
|---|---|---:|---:|---|---|
| **Payroll funding / factoring** | Whale services >$5M | 1,948 | $68.43B | **full** | **Strongest** — large, clean, full-coverage, SCA-labor-driven |
| **Surety / credit brokerage** | Bond-mandated SB | 790 | $5.30B | 8.5% floor | **Solid** — cleanest PDF signal, citable, undercounted |
| **Equipment finance / leasing** | SB Davis-Bacon construction | 4,073 | $17.25B | full | **Use the construction cohort, NOT the equipment array** |

**Recommendations:**
1. **Lead with the Whale payroll cohort (1,948 awards / $68.43B).** It needs no PDF coverage, is dominated by labor-heavy SCA services, and 1,451 are "hot" (≤90-day action). This is the immediate, defensible TAM.
2. **Run surety off `insurance_bonding` (790 / $5.30B)** as a high-precision, citable list — but communicate it as a *floor*; expanding attachment-extraction coverage (currently 8.5% of SB) is the single highest-leverage move to grow every PDF-derived count.
3. **Do not market equipment leasing off `equipment_capability`** — it is noisy compliance-gear text, not financeable iron. Target equipment finance via the **SB Davis-Bacon construction cohort (4,073 / $17.25B)** plus construction PSCs/NAICS; the obligation to mobilize equipment is implied by the work, not by the (sparse, noisy) extraction.
4. **"Hot leads" deliverable: 2,060 deduped SB awards with ≤90-day funding action ($46.34B).** This is the list to hand a lender tomorrow.

---

## 6. Validation (self-checks that passed)
- Business-size literal confirmed via histogram (S=83,400 / O=65,388 / blank=1; sum=148,789 = active base).
- §1 membership partition: big-three (4,459) + labor_standards (14,508) − both (1,045) = 17,922 ✓.
- SB-definition sensitivity: COD-small (83,400) vs COD-or-any-setaside (84,349) = +1.1% — COD is the right primary key.
- Coverage denominators reported for every PDF-derived count (8.5% SB overall; 16.9% SB-construction).
- Equipment/bonding cardinality emitted so field noise is visible, not hidden (218 vs 84 distinct).
- `act_null=0` — recency is exact across the cohort.

Reproduce: `doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb python3 scripts/cash_crunch_probe.py`.
