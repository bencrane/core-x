# Equipment-Finance Vertical Recon — Agriculture/Forestry & Aerospace/Defense vs. Yellow Iron

**Mode:** READ-ONLY probe → market sizing. **Date:** 2026-06-22 (UTC).
**SoR:** `s3://data-sink/active/govcon_active_awards/` (Lance v2.1) · `as_of_date` = 2026-06-20 · 189,272 rows / 127 cols.
**Probe:** [`scripts/equipment_finance_vertical_recon.py`](../../scripts/equipment_finance_vertical_recon.py) (read-only; no writes, no index changes).
**Active filter (all figures):** `pop_current_end >= CURRENT_DATE` — the directive's literal, on the BTREE-indexed date column.

---

## 0. Methodology & directive reconciliation

The directive assumed three column names that do **not** exist on this table. Each was mapped to the real, indexed column and the enum literal was confirmed by preflight (not guessed):

| Directive assumed | Real column | Confirmed literal |
|---|---|---|
| `contracting_officers_determination_of_business_size = 'S'` | `business_size_code` | `'S'` = SMALL BUSINESS (112,576) · `'O'` = OTHER THAN SMALL (76,695) · 1 null |
| `construction_wage_rate_requirements = 'YES'` | `construction_wage_rate_requirements` | `'YES'`/`'Y'` valid (6,681 all-rows) vs `NO`/`NOT APPLICABLE` |
| `pop_current_end >= current_date OR active awards` | `pop_current_end` (date32, BTREE) | used literally; sensitivity below |
| `has_subcontracting_plan = true` | exists as-is (bool, BITMAP) | direct |
| `recipient_state_code` (prime HQ) | exists as-is | direct |

**Liveness denominator (sensitivity — the chosen cut is strict):**

| definition | rows | note |
|---|---:|---|
| all rows (table membership) | 189,272 | includes option-tail + unknown-PoP |
| **`pop_current_end >= CURRENT_DATE`** | **141,014** | **← used here** (committed end still in future) |
| `active_current` flag (≥ as_of 06-20) | 142,294 | 2-day gap vs CURRENT_DATE |
| `active_potential` flag | 148,789 | adds unexercised option years |
| `pop_unknown` / `pop_current_end IS NULL` | 40,483 | excluded by the strict cut |

The strict cut excludes 40,483 NULL-PoP awards and option-tail-only awards. Adopting `active_potential` raises every count below by ~5%. **Value metric** = `current_total_value_of_award` (committed ceiling; the house-standard "contract value" that sums to the doc's $1,607.5B headline).

**Geography caveat:** `recipient_state_code` is **prime HQ**, not place of performance. VA/MD/CT leadership below reflects corporate/beltway HQ addresses, not where equipment operates. For operating geography, `pop_state_code` is the correct column (follow-up).

---

## 1. Agriculture & Forestry — NAICS sector 11 (primary)

**Filter:** `left(naics_code,2) = '11' AND pop_current_end >= CURRENT_DATE`

| metric | value |
|---|---:|
| active awards | **795** |
| Σ current contract value | **$0.54B** |
| Σ obligated | $0.54B |
| Σ potential (all options) | $0.67B |
| "iron" proxy — `has_subcontracting_plan = true` | **7** (0.9%) |

**Top 10 prime-HQ states:**

| state | awards | value | % of vertical |
|---|---:|---:|---:|
| OR | 222 | $0.14B | 27.9% |
| CA | 49 | $0.18B | 6.2% |
| WA | 43 | $0.01B | 5.4% |
| CO | 33 | $0.04B | 4.2% |
| AR | 33 | — | 4.2% |
| MT | 32 | $0.01B | 4.0% |
| FL | 28 | $0.01B | 3.5% |
| NM | 26 | — | 3.3% |
| WI | 23 | — | 2.9% |
| AZ | 23 | $0.03B | 2.9% |

**Composition:** the bucket is **forestry-support services, not heavy-ag-iron**. `115310 Support Activities for Forestry` = 589 of 795 awards ($0.456B), concentrated in Oregon. The remainder is animal-production support (39), soil prep (36), marine fishing (25), forest nurseries (16), hay farming (11). Negligible row-crop / large-equipment operators.

**ALT lens — PSC Category F (Natural Resources & Conservation services):** 2,505 awards / **$21.07B** — far larger than the NAICS cut, but it is **wildfire + environmental-remediation work**, not ag iron: `F108` environmental remediation (575), `F003` forest/range fire suppression (559), `F999` other environmental (381), `F014` tree thinning (91). These firms run dozers/masticators/aircraft, but they overlap the existing construction motion and are not an "agriculture" segment.

---

## 2. Aerospace & Defense Manufacturing — NAICS subsector 3364 (primary)

**Filter:** `left(naics_code,4) = '3364' AND pop_current_end >= CURRENT_DATE`

| metric | value |
|---|---:|
| active awards | **5,522** |
| Σ current contract value | **$211.44B** |
| Σ obligated | $175.11B |
| Σ potential (all options) | $227.46B |
| tooling/finance proxy — Small Business (`business_size_code='S'`) | **2,215** (40.1% of awards · $2.0B = 0.95% of value) |

**Top 10 prime-HQ states:**

| state | awards | value | % of vertical |
|---|---:|---:|---:|
| TX | 606 | $77.35B | 11.0% |
| CT | 552 | $12.90B | 10.0% |
| NY | 531 | $3.04B | 9.6% |
| CA | 420 | $5.58B | 7.6% |
| MO | 375 | $6.98B | 6.8% |
| PA | 287 | $3.29B | 5.2% |
| WA | 281 | $5.34B | 5.1% |
| FL | 279 | $35.34B | 5.1% |
| AZ | 255 | $11.91B | 4.6% |
| WI | 214 | $0.01B | 3.9% |

**Composition (NAICS 3364xx):** value concentrates in OEM/space; **volume concentrates in the parts-machining base** — the equipment-finance ICP.

| NAICS | description | awards | value |
|---|---|---:|---:|
| 336413 | Other Aircraft Parts & Auxiliary Equipment Mfg | **4,581** | $11.49B |
| 336411 | Aircraft Manufacturing | 541 | $85.14B |
| 336412 | Aircraft Engine & Engine Parts Mfg | 250 | $9.46B |
| 336414 | Guided Missile & Space Vehicle Mfg | 76 | $96.63B |
| 336419 | Other Guided Missile/Space Vehicle Parts | 47 | $2.70B |
| 336415 | Guided Missile Propulsion Units | 27 | $6.03B |

**ALT lens — PSC FSG 15/16/17:** FSG 15 airframe structural 1,699 awards / $82.47B / 1,031 small-biz · FSG 16 components 1,602 / $8.44B / 619 small-biz · FSG 17 ground/launch 165 / $0.03B / 139 small-biz. Corroborates ~1,789 small-business aircraft-component awards.

### 2b. Small-business aerospace target segment (the stated ICP)

2,215 live small-biz awards held by **416 distinct primes**. This is the campaign-actionable list.

**Top prime-HQ states (small-biz only):** NY 427/34 primes · CA 281/78 primes/$267M · WA 198/6 primes · NM 148/5 primes/$144M · FL 144/26 · TX 142/33 primes/$633M · WI 95/5 · GA 89/13 · PA 72/22 · VA 52/18.

**Value bands:** the segment is overwhelmingly small-dollar, high-volume parts work, with a thin large-dollar tail:

| band | awards | Σ value |
|---|---:|---:|
| ≥ $50M | 7 | $1,423M |
| $10–50M | 13 | $318M |
| $1–10M | 39 | $156M |
| < $1M | **2,156** | $112M |

**Targeting nuance:** the ≥$10M tail is **NewSpace integration/R&D**, not machine-shop tooling — Axiom Space (TX, $451M/$131M/$31M), Lunar Outpost (CO, $220M), Venturi Astrolab (CA, $219M), Mistral (MD, $191M), Cymstar (OK, $126M, simulators). Those carry a venture/growth-capital profile, not an equipment-lease one. **The CNC/autoclave/tooling equipment-finance fit is the broad <$1M `336413` parts-machining base (2,156 awards), not the headline tail.**

---

## 3. Yellow Iron baseline — `construction_wage_rate_requirements = 'YES'`

**Filter:** `construction_wage_rate_requirements = 'YES' AND pop_current_end >= CURRENT_DATE`

| metric | value |
|---|---:|
| active awards | **4,832** |
| Σ current contract value | **$229.23B** |
| Σ obligated | $191.60B |
| Σ potential (all options) | $246.83B |

**Top 10 prime-HQ states:** VA 433/$32.68B · CA 380/$6.92B · MD 361/$11.36B · AK 218/$1.12B · FL 207/$1.91B · WA 203/$2.39B · TX 182/$1.55B · NY 161/$0.96B · PA 129 · MO 128. (VA/MD lead = beltway GovCon HQ, not where dirt moves — see geography caveat.)

---

## 4. Side-by-side & verdict

| vertical | active awards | current value | obligated | % of all live awards |
|---|---:|---:|---:|---:|
| **Aerospace (NAICS 3364)** | **5,522** | **$211.44B** | $175.11B | 3.9% |
| **Yellow Iron (Davis-Bacon)** | 4,832 | $229.23B | $191.60B | 3.4% |
| **Ag/Forestry (NAICS 11)** | 795 | $0.54B | $0.54B | 0.6% |

**Aerospace is a real, dedicated-campaign-worthy vertical — bigger than yellow iron by award count (5,522 vs 4,832), ~92% by value ($211B vs $229B).** The equipment-finance thesis lands precisely: 2,215 small-business awards (40% of the vertical, <1% of its dollars) across 416 small primes manufacturing federal aircraft parts — the exact profile that finances CNCs, autoclaves, and custom tooling. Target the `336413` parts-machining base (concentrated in NY/CA/WA/NM/TX/FL), not the NewSpace dollar tail.

**Agriculture/Forestry is not worth a dedicated federal-contract campaign.** 795 awards / $0.54B, dominated by Oregon forestry-support services; the subcontracting-plan proxy is effectively dead (7 awards). The larger PSC-F population ($21B) is wildfire/remediation work that already overlaps the construction motion. **Structural reason:** ag equipment demand is overwhelmingly private-sector — row-crop and livestock operators do not sell to the federal government — so SAM/FPDS is the wrong lens for sizing an agriculture equipment-finance TAM. If Ag is pursued, source it from private firmographics (DSBS, PPP, dealer/lien data), not federal awards.

---

## 5. SQL appendix (exact filters)

```sql
-- shared liveness predicate (directive literal, BTREE-indexed date col)
--   LIVE := pop_current_end >= CURRENT_DATE

-- 1. Ag/Forestry (primary)           : left(naics_code,2) = '11'                      AND LIVE
--    Ag iron proxy                   : ... AND has_subcontracting_plan = true
--    Ag ALT lens (PSC services)      : psc_category = 'F'                             AND LIVE
-- 2. Aerospace (primary)             : left(naics_code,4) = '3364'                    AND LIVE
--    Aero tooling/finance proxy      : ... AND business_size_code = 'S'
--    Aero ALT lens (PSC products)    : psc_fsg IN ('15','16','17')                    AND LIVE
-- 3. Yellow Iron baseline            : construction_wage_rate_requirements = 'YES'    AND LIVE

-- volume + value template
SELECT count(*),
       sum(current_total_value_of_award)  AS active_contract_value,
       sum(total_dollars_obligated)       AS obligated
FROM govcon_active_awards
WHERE <vertical_filter> AND pop_current_end >= CURRENT_DATE;

-- top-10 prime-HQ states template
SELECT recipient_state_code, count(*) AS awards,
       sum(current_total_value_of_award) AS value
FROM govcon_active_awards
WHERE <vertical_filter> AND pop_current_end >= CURRENT_DATE
  AND nullif(trim(recipient_state_code),'') IS NOT NULL
GROUP BY 1 ORDER BY awards DESC LIMIT 10;
```

**Reproduce:** `doppler run --project core-x --config prd -- python3 scripts/equipment_finance_vertical_recon.py`
