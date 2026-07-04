# Labor × GovCon Crosswalk — What's Live and the GTM Unlocks

**Companion to** [`USASPENDING_FPDS_L2_SATELLITES_AND_JOIN_GRAPH.md`](USASPENDING_FPDS_L2_SATELLITES_AND_JOIN_GRAPH.md). Same Gen-3 plane (LanceDB on R2), same **filter-then-join** DuckDB-over-Lance discipline. All row counts/columns below are from a live `lance.dataset(...)` probe on **2026-07-04**; where an older doc disagrees, ground truth wins.

## Thesis
Sell to **staffing firms**: reach companies that just won federal contracts and have imminent labor needs. The substrate turns an award's coarse codes into a priced labor answer, and the L2 satellites add *when the work turns over*:

```
what was bought          who must be hired         at what wage                    is the incumbent unionized     when does it turn over
(NAICS × PSC × PoP)   →   (SOC / SCA occupations)   (OEWS market / SCA floor / NAF)  (CBA-covered UEI)              (L2 capacity + kinetic)
```

---

## 1. What's live right now

**Occupation ↔ wage (priced-labor core)**
| dataset | rows | grain / key |
|---|--:|---|
| `bls_oews_2025` | 413,527 | area × `occ_code`(SOC) × `naics` — OEWS staffing pattern + wages |
| `soc_state_wage` | 35,223 | `soc_code` × `state_fips`/`prim_state` — OEWS wage percentiles per **state** |
| `soc_priced_skilled` | 830 | `soc_code` — national skilled-SOC wage + O*NET |
| `bls_employment_projections_2024_2034` | 1,113 | `occupation_code` — growth + median wage + openings |
| `bls_ep_industry_occupation_matrix_2024_2034` | 113,473 | `industry_code`×`occupation_code`×`naics_code` |

**SCA taxonomy + the SCA↔SOC bridge**
| `dol_sca_occupations` | 502 | `occupation_code` — SCA labor categories |
| `sca_soc_crosswalk` | 424 | `occupation_code` ↔ `soc_code` (+ `tier`, `confidence`, `dominance_ratio`) |

**Statutory floors + union identity**
| `sam_wage_determinations` | 10,055 | `wd_id` / `cba_number` / `full_reference_number` |
| `sam_wd_cba_pointers` | 4,298 | `wd_id` ↔ `cba_number`, `contractor_union`, `effective_start/end_date` |
| `sam_wd_cba_coverage` | 4,270 | `wd_id` ↔ state/county |
| `olms_cba_crosswalk` | 4,844 | **`uei`** ↔ `union_name`, `exp_date`, `is_active` |
| `olms_cba_index` | 4,849 | `cba_pub_id` — employer/union/`naics`/`no_of_emp` |

**NAF prevailing wage + county geography**
| `naf_wage_rates` | 1,670,700 | `wage_area`×`schedule_number`×`grade`×`step` → `hourly_rate` |
| `naf_wage_area_county_fips` | 769 | `wage_area` ↔ **`county_fips`** |
| `view_county_wage_arbitrage_benchmark` | 502 | `county_fips` → NAF grade min/max benchmark |

**NAICS×PSC labor bridge (the pre-joined hub)**
| `naics_psc_labor_dim` | 16,291 | `naics_code`×`psc_code` → `is_labor_play`, `rank1_soc_code`, `rank1_sca_code` |
| `naics_psc_labor_profile_categories` | 54,235 | naics×psc×`rank` → `soc_code`, `sca_code`, `role_class`, `a_median`, `ep_growth_2024_2034_pct`, `pct_of_industry` |
| `naics_psc_labor_profile` | 16,291 | naics×psc → labor-play summary + `oews_industry_code` |
| `naics_reference` 2,125 · `psc_reference` 6,108 · `naics_vertical_taxonomy` 2,432 · `naics_psc_vertical_map` 279 | | reference / verticals |

**Award & solicitation-linked demand (keys straight to the spine/L2)**
| `active_award_labor_demand` | 1,080 | **`contract_award_unique_key`** + `recipient_uei` + `labor_role` |
| `govcon_labor_demand` | 20,598 | `contract_award_unique_key` + `notice_id` → `headcount`, `clearance_level`, `wage_floor` |
| `govcon_pricing` | 170,532 | `contract_award_unique_key` + `notice_id` — solicitation pricing text |
| `sam_labor_poc_people` | 29,464 | **`uei`** → staffing POC + `company_linkedin_url`, `in_our_staffing` |

**Not materialized (404 on probe):** `psctool`, `govcon_labor_demand_90day`, `govcon_pricing_90day`.

---

## 2. The crosswalk key graph

The connective keys — and the **name bridges** that must be written explicitly in each `ON`:

| bond | FPDS/L2 side | labor side | note |
|---|---|---|---|
| product/service | `product_or_service_code` | `psc_code` | **name differs, value identical** |
| industry | `naics_code` | `naics_code` / `naics` / `industry_code` | 6-digit NAICS |
| occupation (market) | — | `soc_code` ≡ OEWS `occ_code` ≡ EP `occupation_code` | |
| occupation (statutory) | — | SCA `occupation_code` | bridged to SOC via `sca_soc_crosswalk` |
| locality | spine `pop_county_fips` / `primary_place_of_performance_state_code` | `county_fips` / `state_fips`·`prim_state` | **`pop_county_fips` is on the L1 spine, NOT on L2 `prime_award_state`** |
| wage area | — | `wage_area` (`naf_wage_rates` ↔ `naf_wage_area_county_fips`) | |
| award | `contract_award_unique_key` | `contract_award_unique_key` (demand tables) | 1:1 / 1:N |
| entity | `recipient_uei` | `uei` (`olms_cba_crosswalk`, `sam_labor_poc_people`) | sub side: `subawardee_uei` |
| solicitation | — | `notice_id` / `solicitation_number` | `govcon_labor_demand` ↔ `govcon_pricing` ↔ SAM opps |

**The accelerator:** `naics_psc_labor_dim` is *pre-joined*. One join from any award's `(naics_code, product_or_service_code)` yields `is_labor_play` + top SOC + top SCA — no 4-way chain. Reach `naics_psc_labor_profile_categories` only when you need the full ranked role mix + per-role wage/growth.

---

## 3. Recipes (each an unlock)

### 3.0 Boilerplate
```python
import os, duckdb, lance
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")
so = {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
      "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
      "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}
A = "s3://data-sink/active"
con = duckdb.connect(); con.execute("SET memory_limit='8GB'")
def reg(name, uri, columns=None, filter=None):     # push projection+predicate into Lance scalar indices
    con.register(name, lance.dataset(uri, storage_options=so).scanner(columns=columns, filter=filter).to_table())
```
> Bridge the key name in the `ON`: `d.psc_code = s.product_or_service_code`. Awards with no labor profile → `LEFT JOIN` → `is_labor_play` NULL.

### 3.1 Priced-labor profile of any award
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","naics_code","product_or_service_code","recipient_uei","life_to_date_obligated"],
    filter="award_topology='standalone' AND life_to_date_obligated > 5000000")
reg("dim", f"{A}/naics_psc_labor_dim/",
    columns=["naics_code","psc_code","is_labor_play","rank1_soc_code","rank1_soc_title","rank1_sca_code","rank1_sca_title"])
con.sql("""
  SELECT s.contract_award_unique_key, s.naics_code, s.product_or_service_code,
         d.rank1_soc_code, d.rank1_soc_title, d.rank1_sca_code, d.rank1_sca_title
  FROM st s JOIN dim d ON d.naics_code=s.naics_code AND d.psc_code=s.product_or_service_code
  WHERE d.is_labor_play LIMIT 50
""").show()
```

### 3.2 Labor-intensity filter on the L2 starvation cohort
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","naics_code","product_or_service_code","consumed_pct","days_to_expiry"],
    filter="consumed_pct>0.85 AND consumed_pct<=5 AND days_to_expiry BETWEEN 0 AND 90 "
           "AND is_terminated=false AND award_topology='standalone'")
reg("dim", f"{A}/naics_psc_labor_dim/", columns=["naics_code","psc_code","is_labor_play"])
con.sql("""
  SELECT count(*) FILTER (WHERE d.is_labor_play) AS starving_labor_plays, count(*) AS starving_total
  FROM st s LEFT JOIN dim d ON d.naics_code=s.naics_code AND d.psc_code=s.product_or_service_code
""").show()
```

### 3.3 Full role mix + wage envelope (market via OEWS state + SCA floor bridge)
```python
# ranked occupation mix for one award's (naics, psc) — a_median + growth come straight from categories
reg("cat", f"{A}/naics_psc_labor_profile_categories/",
    columns=["naics_code","psc_code","rank","soc_code","soc_title","sca_code","role_class","a_median","ep_growth_2024_2034_pct"],
    filter="naics_code='541512' AND psc_code='D307'")
reg("wg", f"{A}/soc_state_wage/",                       # locality-specific market wage (state grain)
    columns=["soc_code","prim_state","a_median","a_pct25","a_pct75"], filter="prim_state='VA'")
reg("x", f"{A}/sca_soc_crosswalk/",                      # SCA statutory occupation ↔ SOC (now landed)
    columns=["occupation_code","soc_code","tier","confidence"])
con.sql("""
  SELECT c.rank, c.soc_code, c.soc_title, c.role_class,
         c.a_median national_med, w.a_median va_med, c.ep_growth_2024_2034_pct growth,
         c.sca_code, x.tier sca_soc_tier
  FROM cat c
  LEFT JOIN wg w USING (soc_code)
  LEFT JOIN x  ON x.soc_code = c.soc_code AND x.occupation_code = c.sca_code
  ORDER BY c.rank LIMIT 15
""").show()
```

### 3.4 Wage-arbitrage / displacement (needs PoP county → via the L1 spine)
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","naics_code","product_or_service_code"],
    filter="consumed_pct>0.85 AND days_to_expiry BETWEEN 0 AND 120 AND award_topology='standalone'")
keys = [r[0] for r in con.sql("SELECT contract_award_unique_key FROM st").fetchall()]
inlist = ",".join("'" + k.replace("'","''") + "'" for k in keys[:5000])          # batch large sets
reg("sp", f"{A}/usaspending_fpds_canonical_txn/",                                 # PoP county is on the spine
    columns=["contract_award_unique_key","pop_county_fips","action_date"],
    filter=f"contract_award_unique_key IN ({inlist})")
reg("arb", f"{A}/view_county_wage_arbitrage_benchmark/",
    columns=["county_fips","state","naf_na_min","naf_na_max"])
con.sql("""
  WITH pop AS (SELECT contract_award_unique_key, arg_max(pop_county_fips, action_date) county_fips
               FROM sp GROUP BY 1)
  SELECT s.contract_award_unique_key, p.county_fips, a.state, a.naf_na_min, a.naf_na_max
  FROM st s JOIN pop p USING (contract_award_unique_key)
            LEFT JOIN arb a ON a.county_fips = p.county_fips
  ORDER BY a.naf_na_max DESC LIMIT 50
""").show()
```
> The `IN (...)` pushdown keeps the 108M spine scan to the starving-award key set. For sets > a few thousand, register `sp` filtered and semi-join instead.

### 3.5 Union / §4(c) successorship exposure
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","recipient_uei","days_to_expiry","terminal_action_type_code"],
    filter="consumed_pct>0.8 AND days_to_expiry BETWEEN 0 AND 180 AND award_topology='standalone'")
reg("cba", f"{A}/olms_cba_crosswalk/",
    columns=["uei","union_name","exp_date","tier","score"], filter="is_active=true")
con.sql("""
  SELECT s.contract_award_unique_key, s.recipient_uei, c.union_name, c.exp_date, s.days_to_expiry
  FROM st s JOIN cba c ON c.uei = s.recipient_uei
  ORDER BY c.exp_date LIMIT 50          -- CBA expiring before the recompete = successorship + continuity risk
""").show()
```

### 3.6 Live labor-demand → bench match (the staffing product)
```python
reg("gd", f"{A}/govcon_labor_demand/",
    columns=["contract_award_unique_key","labor_category","headcount","clearance_level","wage_floor","pop_end"],
    filter="clearance_level IS NOT NULL")
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","recipient_uei"])
con.sql("""
  SELECT g.labor_category, g.clearance_level,
         sum(TRY_CAST(g.headcount AS BIGINT)) fte, count(DISTINCT g.contract_award_unique_key) awards
  FROM gd g LEFT JOIN st s USING (contract_award_unique_key)
  GROUP BY 1,2 ORDER BY fte DESC NULLS LAST LIMIT 30
""").show()
```

### 3.7 Contactable POC at labor-play winners
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","recipient_uei","naics_code","product_or_service_code"],
    filter="consumed_pct>0.85 AND days_to_expiry BETWEEN 0 AND 90 AND award_topology='standalone'")
reg("dim", f"{A}/naics_psc_labor_dim/", columns=["naics_code","psc_code","is_labor_play"])
reg("poc", f"{A}/sam_labor_poc_people/",
    columns=["uei","legal_business_name","company_linkedin_url","poc_type","in_our_staffing"])
con.sql("""
  SELECT DISTINCT s.recipient_uei, p.legal_business_name, p.company_linkedin_url, p.poc_type
  FROM st s JOIN dim d ON d.naics_code=s.naics_code AND d.psc_code=s.product_or_service_code AND d.is_labor_play
           JOIN poc p ON p.uei = s.recipient_uei
  LIMIT 50
""").show()
```

### 3.8 Prime → sub labor cascade
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["contract_award_unique_key","naics_code","product_or_service_code"],
    filter="consumed_pct>0.8 AND days_to_expiry BETWEEN 0 AND 180 AND award_topology='standalone'")
reg("dim", f"{A}/naics_psc_labor_dim/", columns=["naics_code","psc_code","is_labor_play"])
reg("sub", f"{A}/usaspending_subaward_canonical/",
    columns=["prime_award_unique_key","subawardee_uei","subawardee_name","subaward_amount"])
reg("poc", f"{A}/sam_labor_poc_people/", columns=["uei","company_linkedin_url"])
con.sql("""
  SELECT sub.subawardee_uei, any_value(sub.subawardee_name) name,
         round(sum(sub.subaward_amount)/1e6,2) sub_M, any_value(poc.company_linkedin_url) linkedin
  FROM st s JOIN dim d ON d.naics_code=s.naics_code AND d.psc_code=s.product_or_service_code AND d.is_labor_play
           JOIN sub ON sub.prime_award_unique_key = s.contract_award_unique_key
           LEFT JOIN poc ON poc.uei = sub.subawardee_uei
  GROUP BY 1 ORDER BY sub_M DESC LIMIT 50          -- subaward_amount is sub-grain-safe to SUM
""").show()
```

### 3.9 Vertical / portfolio labor scoring (one entity)
```python
reg("st", f"{A}/usaspending_fpds_prime_award_state/",
    columns=["recipient_uei","naics_code","product_or_service_code","life_to_date_obligated"],
    filter="recipient_uei='ABC123DEF456'")
reg("vt", f"{A}/naics_vertical_taxonomy/", columns=["naics_code","vertical_category"])
reg("vm", f"{A}/naics_psc_vertical_map/", columns=["naics_code","psc_code","equipment_intensity","work_type"])
con.sql("""
  SELECT v.vertical_category, m.work_type, m.equipment_intensity,
         round(sum(s.life_to_date_obligated)/1e6,1) obligated_M
  FROM st s LEFT JOIN vt v USING (naics_code)
            LEFT JOIN vm m ON m.naics_code=s.naics_code AND m.psc_code=s.product_or_service_code
  GROUP BY 1,2,3 ORDER BY obligated_M DESC
""").show()
```

### 3.10 High-growth occupation targeting (BLS EP × govcon dollars)
```python
reg("cat", f"{A}/naics_psc_labor_profile_categories/",
    columns=["naics_code","psc_code","soc_code","soc_title","a_median","ep_growth_2024_2034_pct"],
    filter="ep_growth_2024_2034_pct > 8 AND a_median > 90000")
reg("dim", f"{A}/naics_psc_labor_dim/",
    columns=["naics_code","psc_code","total_dollars_obligated","is_labor_play"], filter="is_labor_play=true")
con.sql("""
  SELECT c.soc_code, any_value(c.soc_title) title, round(avg(c.a_median),0) med_wage,
         round(avg(c.ep_growth_2024_2034_pct),1) growth_pct, round(sum(d.total_dollars_obligated)/1e9,2) govcon_B
  FROM cat c JOIN dim d ON d.naics_code=c.naics_code AND d.psc_code=c.psc_code
  GROUP BY 1 ORDER BY govcon_B DESC LIMIT 25
""").show()
```

---

## 4. Grain hazards & live-truth notes

- **`sca_soc_crosswalk` now EXISTS (424 rows).** `01_LABOR_PRICING_FOUNDATION.md` §1.4 (2026-07-02) states there is "no SCA↔SOC crosswalk landed" and that the wage axes "do not meet at occupation identity." **Stale** — the bridge landed since; OEWS market wage and SCA statutory floor now meet at occupation (recipe 3.3), and `naics_psc_labor_dim` carries both `rank1_soc_code` and `rank1_sca_code`.
- **`pop_county_fips` is on the L1 spine, not on L2 `prime_award_state`.** Any wage-*locality* join (3.4) routes through `usaspending_fpds_canonical_txn` (or `award_search`) for the county/state, then to the wage datasets. Adding `pop_county_fips` + `primary_place_of_performance_state_code` to a future `prime_award_state` rebuild would collapse that hop.
- **Wages are snapshots, never additive.** `a_median`/`h_mean`/OEWS percentiles/NAF `hourly_rate` describe a rate — never `SUM` them. Multiply a rate by `headcount` (from `govcon_labor_demand`) for a cost estimate.
- **`soc_state_wage` is state-grain; NAF/arbitrage is county-grain.** Market wage resolves to state; statutory/NAF floor resolves to county. Pick the granularity the question needs.
- **The labor docs reference the old 131-col / 108M spine.** The join keys (`naics_code`, `product_or_service_code`, `pop_county_fips`, `recipient_uei`) are all valid on the current 392-col spine (v19) and on `prime_award_state` (except `pop_county_fips`, above).
- **`govcon_labor_demand.headcount` / `wage_floor` are extracted** (LLM `confidence` column) — treat as signal, `TRY_CAST` before math, and gate on `confidence` for high-stakes use.
- **Only three amount columns SUM safely across the whole graph** (carried from the L2 doc): `prime_award_state.life_to_date_obligated`, `mod_delta.delta_federal_action_obligation`, `subaward_canonical.subaward_amount`. Everything else — award values, wages, ceilings — is a snapshot.

---

## 5. The one-line map
`award (naics_code, product_or_service_code, recipient_uei) → naics_psc_labor_dim → {SOC → OEWS/soc_state_wage, SCA → sca_soc_crosswalk → sam_wage_determinations} + {county → naf/arbitrage} + {recipient_uei → olms_cba_crosswalk, sam_labor_poc_people}`, all gated by **L2 `prime_award_state` (starvation) + `mod_delta` (kinetic)** for *when* to act and joined to **`subaward_canonical`** for *who performs beneath the prime*.
