# DLA DIBBS Ingestion, Opportunity Engine & Arbitrage Playbook — Authoritative Handover

> [!IMPORTANT]
> **Purpose of This Document:** This is the authoritative technical and strategic handover for the DLA DIBBS pipeline, Postgres/Lance data warehouse, Opportunity Engine CLI, and the **American Supply Partners (`ASP`)** / **Bazaar Global** operating model in `/Users/benjamincrane/core-x`. Any future agent or engineer can run the commands and SQL queries in this document verbatim.

---

## 1. Architecture & Infrastructure Overview

### 1.1 Codebase Components (`core-x`)

| File Path | Modal App Name | Role & Status |
| :--- | :--- | :--- |
| [`core/modal_dispatcher.py`](file:///Users/benjamincrane/core-x/core/modal_dispatcher.py) | `universal-dispatcher` | FastAPI gateway on Modal (`@modal.fastapi_endpoint`) that spawns background Modal worker functions for Trigger.dev. Fixed on `2026-11-09` to use custom `key` query-param authentication instead of Modal Starter-plan `requires_proxy_auth=True` (`401`). |
| [`pipelines/dibbs/capture_rfq_daily.py`](file:///Users/benjamincrane/core-x/pipelines/dibbs/capture_rfq_daily.py) | `dibbs-pipelines` | Daily scraper for DIBBS RFQ archives (`in{yymmdd}.txt`, `bq{yymmdd}.zip` containing `bq*.txt` + `as*.txt`, and `ca{yymmdd}.zip` containing all daily solicitation PDFs). Lands raw byte-identical archives in R2 (`s3://data-sink/raw/dibbs_rfq_daily/...`) and automatically spawns `dibbs-pdf-parser::backfill_all_days` whenever new `ca*.zip` files land. |
| [`pipelines/dibbs/capture_awards_daily.py`](file:///Users/benjamincrane/core-x/pipelines/dibbs/capture_awards_daily.py) | `dibbs-awards-pipelines` | Daily scraper for DIBBS Awards (`Awards/AwdRecs.aspx`). Patched on `2026-11-09` to walk all ASP.NET GridView `Page$2 .. Page$N` postback windows (`50` rows/page, `~80–100` pages/day) across 6 concurrent `.mil` sessions (`awd_{yymmdd}_p{PPP}.html`). |
| [`pipelines/dibbs/parse_rfq_pdfs.py`](file:///Users/benjamincrane/core-x/pipelines/dibbs/parse_rfq_pdfs.py) | `dibbs-pdf-parser` | High-speed `PyMuPDF` (`pymupdf`) parser (`parse_day_pdfs`, `backfill_all_days`, `materialize_lance_sor`). Streams every `ca{yymmdd}.zip`, `in{yymmdd}.txt`, and `bq{yymmdd}.zip` (`as{yymmdd}.txt`) from R2, parses solicitation metadata, Section B technical/quality/packaging/inspection rules, Approved Makers, and the embedded 20-buy `Procurement History` table into Postgres (`dibbs.*`) and Gen-3 Lance SoR tables on R2. |
| [`pipelines/dibbs/tests/test_parse_rfq_pdfs.py`](file:///Users/benjamincrane/core-x/pipelines/dibbs/tests/test_parse_rfq_pdfs.py) | *(Local pytest)* | Unit tests validating `in*.txt`, `as*.txt`, and `PyMuPDF` PDF extraction accuracy. |
| [`scripts/dibbs_opportunity_engine.py`](file:///Users/benjamincrane/core-x/scripts/dibbs_opportunity_engine.py) | *(Local CLI)* | Primary analytical & target-screening CLI querying Postgres (`dibbs.*` and `ops.dibbs_pdf_parse_runs`). |
| [`src/trigger/dibbs_rfq_daily.ts`](file:///Users/benjamincrane/core-x/src/trigger/dibbs_rfq_daily.ts) | *(Trigger.dev `prod`)* | Scheduled cron (`11:00 UTC` & `23:00 UTC` daily) invoking `dibbs-pipelines::run` via `universal-dispatcher`. |
| [`src/trigger/dibbs_awards_daily.ts`](file:///Users/benjamincrane/core-x/src/trigger/dibbs_awards_daily.ts) | *(Trigger.dev `prod`)* | Scheduled cron (`12:00 UTC` & `00:00 UTC` daily) invoking `dibbs-awards-pipelines::run` via `universal-dispatcher`. |

---

### 1.2 Environment, Credentials & Execution Notes

1. **Doppler Project & Config:**
   - Project: `core-x`, Config: `prd`
   - Key secrets in Doppler (`core-x/prd`):
     - `HQX_DB_URL_POOLED`: Postgres connection string (contains `dibbs` and `ops` schemas).
     - `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`: Cloudflare R2 (`s3://data-sink`).
     - `MODAL_DISPATCHER_URL`: `https://bencrane--universal-dispatcher-dispatch.modal.run`
     - `MODAL_KEY`: Custom query-param token used by `universal-dispatcher` (Note: `MODAL_KEY` is **not** a Modal CLI token; the real Modal CLI credentials live in `~/.modal.toml`).
2. **Running Modal Commands:**
   - Because `MODAL_KEY` in Doppler is for the HTTP dispatcher rather than the Modal CLI, always invoke `/opt/homebrew/bin/modal` directly (reading `~/.modal.toml`) rather than wrapping `modal` inside `doppler run`:
   ```bash
   # Deploy any of the DIBBS Modal apps:
   /opt/homebrew/bin/modal deploy /Users/benjamincrane/core-x/pipelines/dibbs/parse_rfq_pdfs.py
   /opt/homebrew/bin/modal deploy /Users/benjamincrane/core-x/pipelines/dibbs/capture_rfq_daily.py
   /opt/homebrew/bin/modal deploy /Users/benjamincrane/core-x/pipelines/dibbs/capture_awards_daily.py
   /opt/homebrew/bin/modal deploy /Users/benjamincrane/core-x/core/modal_dispatcher.py
   ```
3. **Running Local Python Scripts Against Postgres:**
   ```bash
   /opt/homebrew/bin/doppler run -p core-x -c prd -- /Users/benjamincrane/core-x/.venv/bin/python /Users/benjamincrane/core-x/scripts/dibbs_opportunity_engine.py --mode all
   ```

---

## 2. Database Schema & Corpus Inventory

### 2.1 Current Corpus Size (As of `2026-11-10`)
- **R2 Raw Archives (`s3://data-sink/raw/dibbs_rfq_daily/`):**
  - **80 active `ca{yymmdd}.zip` archives** (`58` pre-outage days from `2026-06-29` to `2026-08-31` + `22` post-recovery days from `2026-10-11` to `2026-11-08`; note that `2026-09-01` to `2026-10-10` expired off DIBBS's 30-day rolling window during the dispatcher 401 outage, though 20-buy historical awards for those NSNs are recovered inside the October/November PDFs).
  - **129,334 Solicitation PDFs** parsed (`0` failed days).
- **Postgres Tables (`HQX_DB_URL_POOLED`):**
  1. **`ops.dibbs_pdf_parse_runs`** (`80` completed weekday/weekend archive runs)
  2. **`dibbs.solicitation_items`** (`137,352+` RFQ line items across `86,303+` unique NSNs)
  3. **`dibbs.approved_makers`** (`104,313+` `(NSN, maker_cage, part_number)` rows across `12,099` unique Maker CAGEs, with `102,675` resolved to full legal company names directly from the solicitation PDFs)
  4. **`dibbs.procurement_history`** (`803,092+` deduplicated historical contract awards across `74,569` unique NSNs and `13,064` winning CAGEs)
- **R2 Gen-3 Lance SoR Datasets (`s3://data-sink/active/`):**
  - `s3://data-sink/active/dibbs_solicitation_items/` (BTREE indexed on `nsn`, `solicitation_number`)
  - `s3://data-sink/active/dibbs_approved_makers/` (BTREE indexed on `nsn`, `maker_cage`)
  - `s3://data-sink/active/dibbs_procurement_history/` (BTREE indexed on `nsn`, `winner_cage`)

### 2.2 Postgres Table Schemas (`dibbs.*` & `ops.*`)

```sql
-- 1. Parse run ledger
CREATE TABLE IF NOT EXISTS ops.dibbs_pdf_parse_runs (
    post_date DATE PRIMARY KEY,
    ca_r2_key TEXT NOT NULL,
    total_pdfs INT NOT NULL,
    parsed_pdfs INT NOT NULL,
    solicitations_ingested INT NOT NULL,
    history_rows_ingested INT NOT NULL,
    approved_makers_ingested INT NOT NULL,
    anomaly_count INT NOT NULL,
    duration_s NUMERIC(10,2) NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'error')),
    detail TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Solicitation line items (merged from in{yymmdd}.txt + PDF Section A & Section B)
CREATE TABLE IF NOT EXISTS dibbs.solicitation_items (
    post_date DATE NOT NULL,
    solicitation_number TEXT NOT NULL,
    pr_number TEXT NOT NULL,
    nsn TEXT NOT NULL,
    fsc TEXT,
    nomenclature TEXT,
    quantity NUMERIC,
    unit_of_issue TEXT,
    return_date DATE,
    buyer_code_index TEXT,
    amsc TEXT,                       -- 'G' = Make-to-Print (open drawings); 'C'/'D'/'Z'/'R'/'P' = Approved-Source locked
    amc TEXT,
    set_aside TEXT,                  -- 'N' = Unrestricted (86.9%), 'Y' = Small Biz (10.5%), 'L' = WOSB, 'R' = SDVOSB, 'H' = HUBZone
    pdf_name TEXT,
    naics_code TEXT,
    buyer_name TEXT,
    buyer_code TEXT,
    buyer_email TEXT,
    buyer_phone TEXT,
    delivery_days INT,
    fob_point TEXT,                  -- 'ORIGIN' (78.6%) vs 'DESTINATION' (21.4%)
    inspection_point TEXT,           -- 'DESTINATION' (82.7% - no DCMA visit) vs 'ORIGIN' (17.3%)
    acceptance_point TEXT,
    packaging_standard TEXT,         -- 'ASTM D3951' (commercial cardboard box) vs 'MIL-STD-2073'
    has_additive_mfg_ban BOOLEAN,    -- Procurement Note L31 (true on ~95% of RFQs)
    has_fat BOOLEAN,                 -- First Article Testing required
    has_csi BOOLEAN,                 -- Critical Safety Item
    has_higher_quality BOOLEAN,      -- ISO 9001 / AS9100 required
    has_cfolders BOOLEAN,            -- Technical drawings in DLA cFolders
    has_nmr_waived BOOLEAN,
    no_history_available BOOLEAN,
    history_row_count INT,
    item_description TEXT,
    PRIMARY KEY (post_date, solicitation_number, pr_number)
);

-- 3. Approved Manufacturers per NSN (merged from as{yymmdd}.txt + PDF Section B)
CREATE TABLE IF NOT EXISTS dibbs.approved_makers (
    nsn TEXT NOT NULL,
    maker_cage TEXT NOT NULL,
    part_number TEXT NOT NULL,
    maker_name TEXT,                 -- Extracted from PDF Section B ("COMPANY NAME 12345 P/N XYZ")
    first_seen_date DATE NOT NULL,
    last_seen_date DATE NOT NULL,
    PRIMARY KEY (nsn, maker_cage, part_number)
);

-- 4. Historical Contract Awards per NSN (extracted from PDF Procurement History tables)
CREATE TABLE IF NOT EXISTS dibbs.procurement_history (
    nsn TEXT NOT NULL,
    fsc TEXT,
    contract_number TEXT NOT NULL,
    winner_cage TEXT NOT NULL,       -- Compare against dibbs.approved_makers.maker_cage to distinguish OEM vs Broker wins
    award_date DATE NOT NULL,
    quantity NUMERIC NOT NULL,
    unit_cost NUMERIC NOT NULL,
    total_cost NUMERIC NOT NULL,     -- quantity * unit_cost
    surplus_material BOOLEAN NOT NULL,
    source_post_date DATE NOT NULL,
    source_pdf TEXT NOT NULL,
    PRIMARY KEY (nsn, contract_number)
);
```

---

## 3. Ready-to-Run CLI Commands & SQL Queries

### 3.1 Using the Opportunity Engine CLI ([`scripts/dibbs_opportunity_engine.py`](file:///Users/benjamincrane/core-x/scripts/dibbs_opportunity_engine.py))

Run any mode directly via Doppler:

```bash
# Run all reports:
/opt/homebrew/bin/doppler run -p core-x -c prd -- /Users/benjamincrane/core-x/.venv/bin/python /Users/benjamincrane/core-x/scripts/dibbs_opportunity_engine.py --mode all

# Run individual modes:
# 1. Pipeline health & row counts:
/opt/homebrew/bin/doppler run -p core-x -c prd -- /Users/benjamincrane/core-x/.venv/bin/python /Users/benjamincrane/core-x/scripts/dibbs_opportunity_engine.py --mode status

# 2. Exact Bucket B market size by Inspection Point & Packaging Standard:
/opt/homebrew/bin/doppler run -p core-x -c prd -- /Users/benjamincrane/core-x/.venv/bin/python /Users/benjamincrane/core-x/scripts/dibbs_opportunity_engine.py --mode dropship-sizing

# 3. Top partner manufacturers ranked by modeled annual gross profit (15% volume discount + 10% undercut):
/opt/homebrew/bin/doppler run -p core-x -c prd -- /Users/benjamincrane/core-x/.venv/bin/python /Users/benjamincrane/core-x/scripts/dibbs_opportunity_engine.py --mode undercut-partners --limit 35

# 4. High-fragmentation "under-the-radar" manufacturers (10+ brokers, <=$180K/yr per broker so brokers don't retaliate):
/opt/homebrew/bin/doppler run -p core-x -c prd -- /Users/benjamincrane/core-x/.venv/bin/python /Users/benjamincrane/core-x/scripts/dibbs_opportunity_engine.py --mode fragmented-partners --limit 35
```

---

### 3.2 Standalone SQL Queries (Copy-Paste Ready for Future Agents)

#### Query A: High-Fragmentation "Under-the-Radar" Partner Manufacturers (`Bucket B` + `DESTINATION` Inspection + `10+` Brokers)
Finds non-bidding manufacturers (`0` direct wins in `2023–2026`) whose military parts have `DESTINATION` inspection and are split across `10+` competing brokers so no single broker makes enough money off that factory to notice or fight back when undercut:

```sql
WITH nsn_maker_counts AS (
    SELECT nsn, count(DISTINCT maker_cage) AS maker_cnt
    FROM dibbs.approved_makers
    GROUP BY nsn
),
maker_names AS (
    SELECT maker_cage, max(maker_name) AS maker_name, count(DISTINCT nsn) AS total_nsns
    FROM dibbs.approved_makers
    GROUP BY maker_cage
),
dest_nsns AS (
    SELECT DISTINCT nsn
    FROM dibbs.solicitation_items
    WHERE inspection_point = 'DESTINATION'
),
recent_hist AS (
    SELECT
        am.maker_cage,
        ph.nsn,
        ph.fsc,
        ph.winner_cage,
        ph.unit_cost,
        LEAST(ph.total_cost, 250000) AS capped_cost
    FROM dibbs.procurement_history ph
    JOIN nsn_maker_counts nmc ON nmc.nsn = ph.nsn AND nmc.maker_cnt <= 2
    JOIN dibbs.approved_makers am ON am.nsn = ph.nsn
    JOIN dest_nsns dn ON dn.nsn = ph.nsn
    WHERE ph.award_date >= DATE '2023-01-01'
      AND LEFT(ph.fsc, 2) NOT IN ('65', '89', '91')
),
maker_self_check AS (
    SELECT
        maker_cage,
        count(*) FILTER (WHERE winner_cage = maker_cage) AS self_wins,
        count(DISTINCT winner_cage) FILTER (WHERE winner_cage <> maker_cage) AS distinct_brokers
    FROM recent_hist
    GROUP BY maker_cage
),
nsn_economics AS (
    SELECT
        rh.maker_cage,
        rh.nsn,
        count(*) AS orders_23_26,
        sum(rh.capped_cost) AS spend_23_26,
        min(rh.unit_cost) AS min_broker_uc,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY rh.unit_cost) AS med_broker_uc
    FROM recent_hist rh
    JOIN maker_self_check msc ON msc.maker_cage = rh.maker_cage
    WHERE msc.self_wins = 0 AND msc.distinct_brokers >= 10
      AND rh.winner_cage <> rh.maker_cage
    GROUP BY rh.maker_cage, rh.nsn
    HAVING count(*) >= 2 AND min(rh.unit_cost) > 0
),
nsn_modeled AS (
    SELECT
        maker_cage,
        nsn,
        orders_23_26,
        spend_23_26,
        (0.85 * 0.92 * min_broker_uc) AS our_discounted_uc,
        GREATEST(0.98 * min_broker_uc, 0.90 * med_broker_uc) AS our_undercut_bid_uc
    FROM nsn_economics
),
maker_rollup AS (
    SELECT
        nm.maker_cage,
        sum(nm.orders_23_26) AS total_orders_23_26,
        sum(nm.spend_23_26) / 3.8 AS annual_gov_spend,
        sum(nm.spend_23_26 * ((nm.our_undercut_bid_uc - nm.our_discounted_uc) / nm.our_undercut_bid_uc)) / sum(nm.spend_23_26) AS weighted_gross_margin_pct,
        sum((nm.spend_23_26 / 3.8) * ((nm.our_undercut_bid_uc - nm.our_discounted_uc) / nm.our_undercut_bid_uc)) AS est_annual_gross_profit
    FROM nsn_modeled nm
    GROUP BY nm.maker_cage
)
SELECT
    mr.maker_cage,
    coalesce(mn.maker_name, mr.maker_cage) AS name,
    mn.total_nsns,
    msc.distinct_brokers,
    round((mr.annual_gov_spend / msc.distinct_brokers)::numeric, 0) AS avg_rev_per_broker,
    round(mr.annual_gov_spend::numeric, 0) AS annual_spend,
    round((mr.weighted_gross_margin_pct * 100)::numeric, 1) AS gross_margin_pct,
    round(mr.est_annual_gross_profit::numeric, 0) AS annual_gross_profit
FROM maker_rollup mr
JOIN maker_names mn ON mn.maker_cage = mr.maker_cage
JOIN maker_self_check msc ON msc.maker_cage = mr.maker_cage
WHERE mr.annual_gov_spend BETWEEN 400000 AND 4000000
  AND (mr.annual_gov_spend / msc.distinct_brokers) <= 180000
ORDER BY mr.est_annual_gross_profit DESC
LIMIT 35;
```

---

#### Query B: Per-Manufacturer Catalog & Annual Unit Forecast (For Negotiating Distributor Tier / Blanket POs)
Given any target `maker_cage` (e.g., `'32142'` for Hydrasearch or `'82877'` for Rotron), outputs every approved `NSN` and `part_number`, the number of military orders and total units ordered, plus our internal price floor and undercut bid target:

```sql
SELECT
    am.maker_cage,
    max(am.maker_name) AS maker_name,
    am.nsn,
    string_agg(DISTINCT am.part_number, ', ') AS part_numbers,
    max(si.nomenclature) AS item_name,
    max(si.inspection_point) AS inspection,
    max(si.packaging_standard) AS packaging,
    count(DISTINCT ph.contract_number) AS orders_23_26,
    round(sum(ph.quantity)::numeric, 0) AS total_units_23_26,
    round((sum(ph.quantity) / 3.8)::numeric, 1) AS est_units_per_year,
    count(DISTINCT ph.winner_cage) AS competing_brokers,
    -- INTERNAL ONLY (Never show the columns below to the manufacturer):
    round(min(ph.unit_cost)::numeric, 2) AS min_broker_unit_price,
    round((percentile_cont(0.5) WITHIN GROUP (ORDER BY ph.unit_cost))::numeric, 2) AS med_broker_unit_price,
    round((0.85 * 0.92 * min(ph.unit_cost))::numeric, 2) AS target_discounted_buy_cost,
    round(GREATEST(0.98 * min(ph.unit_cost), 0.90 * (percentile_cont(0.5) WITHIN GROUP (ORDER BY ph.unit_cost)))::numeric, 2) AS smart_undercut_bid_price
FROM dibbs.approved_makers am
JOIN dibbs.procurement_history ph ON ph.nsn = am.nsn AND ph.award_date >= DATE '2023-01-01'
LEFT JOIN dibbs.solicitation_items si ON si.nsn = am.nsn
WHERE am.maker_cage = '32142' -- Replace with any target Maker CAGE
GROUP BY am.maker_cage, am.nsn
ORDER BY sum(LEAST(ph.total_cost, 250000)) DESC;
```

---

#### Query C: Daily Live Bidding Feed for Signed Partner Manufacturers (`DESTINATION` Inspection + Under `\$35K` Fast-Pay Filter)
Once a manufacturer partner is signed (or for initial spot-undercut bidding), this query finds all open/recent DIBBS solicitations matching those `maker_cage`s that have `DESTINATION` inspection and calculate the exact undercut bid price:

```sql
WITH hist_pricing AS (
    SELECT
        nsn,
        count(*) AS hist_orders,
        min(unit_cost) AS min_uc,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY unit_cost) AS med_uc
    FROM dibbs.procurement_history
    WHERE award_date >= DATE '2023-01-01' AND unit_cost > 0
    GROUP BY nsn
)
SELECT
    si.post_date,
    si.return_date,
    si.solicitation_number,
    si.pr_number,
    si.nsn,
    si.nomenclature,
    am.maker_cage,
    am.maker_name,
    am.part_number,
    si.quantity,
    si.unit_of_issue,
    si.delivery_days,
    si.fob_point,
    si.inspection_point,
    si.packaging_standard,
    round(hp.med_uc::numeric, 2) AS hist_median_unit_price,
    round(GREATEST(0.98 * hp.min_uc, 0.90 * hp.med_uc)::numeric, 2) AS recommended_bid_unit_price,
    round((si.quantity * GREATEST(0.98 * hp.min_uc, 0.90 * hp.med_uc))::numeric, 2) AS est_contract_value
FROM dibbs.solicitation_items si
JOIN dibbs.approved_makers am ON am.nsn = si.nsn
JOIN hist_pricing hp ON hp.nsn = si.nsn
WHERE si.inspection_point = 'DESTINATION'
  AND si.return_date >= CURRENT_DATE
  AND am.maker_cage IN ('83130', '82877', '91816', '96046', '32142', '08484', '34712', '00994', '75333', '32770')
  AND (si.quantity * hp.med_uc) BETWEEN 2000 AND 35000 -- Fast Pay friendly order size window
ORDER BY si.return_date ASC, est_contract_value DESC;
```

---

## 4. Empirical Market Findings & Strategic Playbook

### 4.1 Macro Market Sizing (`80` Active Archive Days Annualized to `250` Weekdays)
1. **Total DIBBS Daily RFQ Stream:** `~\$6.53 Billion / year` (`~2,100` RFQs/weekday).
2. **Approved-Source Locked Stream (`AMSC != 'G'/'T'`):** **`\$3.92 Billion / year`** (`90,336` RFQs across `80` days).
3. **Pure `Bucket B` (`0` Direct Manufacturer Wins in `2023–2026`; `100%` Broker-Won):** **`\$2.07 Billion / year`** (`51,643` RFQs across `28,484` unique NSNs).
4. **Breakdown of `Bucket B` by Inspection & Packaging:**
   - **`DESTINATION` Inspection + `ASTM D3951` (Standard Commercial Cardboard Box):** **`\$630.0 Million / year`** (`18,643` RFQs across `10,705` unique NSNs). Requires zero military preservation bags and zero DCMA inspector visits—ships straight from the factory's FedEx table in a normal cardboard box with a `4×6` `MIL-STD-129` barcode sticker + FedEx label.
   - **`DESTINATION` Inspection + `MIL-STD-2073` (Military Packaging):** **`\$1.22 Billion / year`** (`28,816` RFQs across `15,651` unique NSNs).
   - **`ORIGIN` Inspection:** `\$217.6 Million / year` (`4,085` RFQs — skip these to avoid DCMA origin inspector scheduling).

### 4.2 TopModeled Partner Targets (`15%` Volume Discount + `10%` Undercut Below Median Broker)
Across the **Top 35 non-exclusive `Bucket B` manufacturers** (`DESTINATION` inspection only), total government spend is **`\$95.56 Million / year`**, and modeled gross profit after undercutting the median winning broker by `10%` is **`\$29.14 Million / year` (`30.5%` weighted gross margin)**.

High-priority **High-Fragmentation ("Under-the-Radar") Manufacturers** where `13 to 32` brokers split the volume so the average broker only wins `1.5 to 3` orders/year (`\$16K–\$36K/yr` in profit) and will not notice or retaliate when undercut:

| Maker CAGE | Manufacturer Name | Industry / Product | NSNs | Competing Brokers | Avg Rev / Broker | Annual Gov Spend | **Our Margin %** | **Modeled Annual Profit** |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `83130` | **Air & Liquid Systems Corp** | Naval heat exchangers & pumps | 59 | 21 | `\$146K/yr` | `\$3,074,243/yr` | `44.8%` | **`\$1,378,488/yr`** |
| `82877` | **Rotron Incorporated** | Avionics/radar cooling fans | 41 | 16 | `\$168K/yr` | `\$2,697,158/yr` | `47.2%` | **`\$1,273,925/yr`** |
| `91816` | **Circor Aerospace, Inc.** | Aerospace fluid control valves | 88 | 17 | `\$173K/yr` | `\$2,948,093/yr` | `42.3%` | **`\$1,247,803/yr`** |
| `96046` | **Crane Pumps & Systems** | Heavy industrial/marine pumps | 45 | 11 | `\$332K/yr` | `\$3,655,078/yr` | `29.6%` | **`\$1,081,022/yr`** |
| `32142` | **Hydrasearch Company, LLC** | Naval refueling couplings & fittings | 79 | **32** | **`\$75K/yr`** | `\$2,389,478/yr` | `33.5%` | **`\$799,555/yr`** |
| `08719` | **Ducommun LaBarge Tech** | Ruggedized aerospace electronics | 74 | **23** | **`\$88K/yr`** | `\$2,026,890/yr` | `34.5%` | **`\$698,266/yr`** |
| `08484` | **Breeze-Eastern LLC** | Helicopter rescue hoists & winches | 66 | 15 | `\$148K/yr` | `\$2,213,980/yr` | `29.1%` | **`\$643,927/yr`** |
| `34712` | **Lake Shore Systems, Inc.** | Naval deck winches & equipment | 135 | **22** | **`\$98K/yr`** | `\$2,153,159/yr` | `28.6%` | **`\$615,445/yr`** |
| `00994` | **Columbus Jack Corporation** | Aircraft hydraulic jacks | 64 | **19** | **`\$99K/yr`** | `\$1,888,201/yr` | `30.2%` | **`\$570,367/yr`** |
| `75333` | **Kingsbury Inc.** | Marine propulsion thrust bearings | 22 | 13 | `\$134K/yr` | `\$1,743,395/yr` | `32.3%` | **`\$562,466/yr`** |
| `32770` | **Kato Engineering Inc.** | Heavy-duty power generators | 45 | **17** | **`\$66K/yr`** | `\$1,130,290/yr` | `40.1%` | **`\$453,416/yr`** |

---

### 4.3 Operating Model: Apartment Laptop + Shipping-Desk Kiosk + Negative Working Capital

1. **Zero-Warehouse Drop-Ship Fulfillment (`ASP Shipping Terminal`):**
   - **Order #1 with a factory:** Email/text a 1-click PDF containing the `4×6` `MIL-STD-129` military barcode sticker and the `4×6` FedEx shipping label to print on their existing shipping printer.
   - **Repeat Partner Factories (`\$350–\$400` Kiosk Kit):** Mail the factory's shipping clerk a pre-configured **Cellular Tablet + Zebra `4×6` Thermal Label Printer + `\$35` USB Shipping Scale**.
   - **How Box Weight Works:** Unit weights are pre-tabulated in software (`Quantity × Part Unit Weight + Box Tare Weight`) so standard orders pre-fill weight and box dimensions automatically; for first-time parts, the `\$35` USB scale populates the weight onto the tablet automatically. Small-parcel weight variances (`1–2 lbs`) never trigger military rejection, and FedEx laser-weighs boxes on its conveyor belt automatically.
   - When the clerk taps **Print Labels** on the tablet, our backend submits the shipment to `DLA VSM` / FedEx API, prints both `4×6` stickers on the Zebra printer in 3 seconds, and submits the electronic `WAWF` invoice.

2. **Cash-Poor / Negative Working Capital Cycle:**
   - **Start in the `Fast Pay` (`FAR 52.213-1`) + `FOB Origin` (`78.6%` of RFQs) Window (`\$2,500–\$10,000` orders, up to `\$35,000`):** Under `Fast Pay`, the Pentagon does **not** wait for the depot to receive the box—the payment clock starts on **Day 1** when FedEx scans the box at the factory dock, and `DFAS` wires cash in **`7 to 12 days`** under Small Business Accelerated Payments (`FAR 52.232-40`).
   - **Manufacturer Payment Terms (`Net 30` or Business Credit Card Float):**
     - On Day 1 (before `\$25K+` bank balance is built), use personal-FICO-underwritten business cards that require `\$0` bank balance (**Amex Blue Business Cash / Chase Ink Unlimited** with `12-month 0% intro APR` + `2%` cashback, or **Amex Business Plum** with `60-day interest-free terms`).
     - Charge the card on shipment (`Day 1`), receive the U.S. Treasury wire (`Day 10`), and pay off the card balance with the government's cash (`Day 11`).
     - Once `\$25K–\$50K` of retained profits sit in the business checking account, add **Ramp** for per-manufacturer auto-capped virtual cards.
   - **Volume Discount Without Upfront Prepayment:** Sign **12-Month Authorized Defense Distributor Tier Agreements (`15%–20%` off spot broker quotes)** billed per order as it ships (`\$0` upfront commitment, zero liability if annual volume projections fall short). Only use Blanket POs (`50%` of historical annual unit demand) on high-velocity repeat parts after a relationship is proven.

3. **Brand & Outreach Architecture (`Bazaar Global` vs. `American Supply Partners`):**
   - **Bazaar Global (Merchant Bank):** Hosts the executive podcast (***Mission Critical*** or ***Engineered Excellence***) and owns the **Bazaar Global Commercial Industrial Intelligence Tool** (a purely commercial, non-GovCon B2B tool where industrial CEOs enter their website and see commercial EPC projects, power plants/data centers/shipyards, OEM customer lookalikes, and M&A comps in their niche).
   - **American Supply Partners (`ASP`):** Positioned strictly as a **portfolio operating company** of Bazaar Global. During or right after the podcast, the host casually asks how the CEO handles military aftermarket orders, notes that Bazaar Global owns **American Supply Partners**, and makes a top-down warm intro from the CEO to their VP of Sales.
   - **Information Discipline:** Never show the manufacturer the historical dollar prices the Pentagon paid or the `30%–50%` broker markup—only discuss **order counts, unit volumes, part numbers, and broker quote fragmentation** (`"32 brokers pestering your sales desk for quotes on lowest-bid auctions"`).
