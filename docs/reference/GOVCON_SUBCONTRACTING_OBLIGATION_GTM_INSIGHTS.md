# GTM Insights — Active Subcontracting Obligations & Designation Demand

**Date:** 2026-06-27 (UTC) · **Source view:** `s3://data-sink/active/govcon_active_subcontracting_obligations/` (Lance v2.1, 1 row / `contract_award_unique_key`) · **Builder:** `pipelines/serving/materialize_active_subcontracting_obligations.py`

Distilled from the structured analysis of `govcon_active_awards` (FPDS) ⨝ `contract_subaward` (FSRS) ⨝ `govcon_subawardee_designations` (SAM Reps & Certs). All figures verified live against the R2 system of record.

---

## TL;DR — the five that matter

1. **26,573 active prime awards are legally obligated to subcontract right now** — a complete, authoritative target list, free, no document mining.
2. **99.3% of those obligated primes are large businesses** — the buyer side of small-/disadvantaged-business subcontracting is almost entirely big primes.
3. **93.5% have no reported subcontracting yet** (24,842 of 26,573) — the single largest GTM whitespace on the board.
4. **Realized demand skews to SDB / woman-owned / WOSB / minority** (800–900 primes each) over SDVOSB (507) and HUBZone (381); 8(a)-JV is negligible (39).
5. **The answer was structured all along.** Mining solicitation PDFs for this is strictly worse than the FPDS field — a methodological lesson that redirects future govcon questions.

---

## The base numbers

| Layer | Count |
|---|--:|
| Active awards (current ∪ potential) | 148,789 |
| **…obligated to subcontract** (`has_subcontracting_plan`) | **26,573** |
|   — large business (`O`) | 26,394 |
|   — small business (`S`) | 179 |
| …with realized subawards reported (FSRS) | 1,731 (6.5%) |
| Reported subaward volume | **$86.6B** |

Plan-type mix (active awards): `COMMERCIAL` 17,009 · `INDIVIDUAL` 8,311 · `DOD COMPREHENSIVE` 752 · `PLAN REQUIRED` variants 501 · vs `PLAN NOT REQUIRED` 98,598.

Structured designation flags already on active awards (no extraction): VOSB 17,589 · WOSB 15,504 · SDVOSB 14,888 · 8(a) 8,822 · HUBZone 4,640 · SDB 500.

---

## Core insights

### 1. The obligation is a complete, structured target list — not a research project
`has_subcontracting_plan` is **100% populated** in FPDS. Every active prime with a subcontracting obligation is identifiable today: **26,573 awards**, with recipient identity, parent, agency, NAICS, award value, and plan type all attached. There is no acquisition cost to this list — it is a query, not a harvest.

**GTM action:** this is the demand-side universe for anything that sells *into* prime subcontracting programs (compliance, matchmaking, sub sourcing) or positions *subs* for placement.

### 2. The buyers are large primes (99.3%)
26,394 of 26,573 obligated awards are large-business primes. Subcontracting plans are a large-business obligation; small-business set-aside winners (the other 122k active awards) generally carry none. The subcontracting market is structurally **big-prime demand → small/disadvantaged-sub supply**.

**GTM action:** segment outreach to large primes by agency and NAICS (both carried in the view); they are the accounts with a standing obligation to place small-business work.

### 3. The whitespace: 24,842 obligated primes with zero reported subcontracting
93.5% of obligated primes show **no** subaward activity in FSRS (`has_reported_subs = false`). Read two ways, both actionable:
- **Reporting gap / lag** — the obligation exists; fulfillment isn't visible. A prime that must subcontract but shows nothing is a live target the moment it begins placing work.
- **Genuine under-fulfillment** — primes behind on small-business goals are exposed and motivated.

**GTM action:** the highest-value cut is `has_subcontracting_plan AND NOT has_reported_subs AND business_size_code='O'` — obligated, large, and not visibly subcontracting. This is the cleanest outreach list on the board.

### 4. Realized demand reveals which designation lanes primes actually buy
Among the 1,731 primes with observable behavior, distinct primes subcontracting to each designation:

| Designation | Primes | Designation | Primes |
|---|--:|---|--:|
| Self-certified SDB | 904 | SDVOSB | 507 |
| Woman-owned | 850 | HUBZone | 381 |
| WOSB | 806 | 8(a) | 309 |
| Minority-owned | 801 | EDWOSB | 176 |
| Veteran-owned | 616 | WOSB joint venture | 39 |

**GTM action:** for a sub deciding which certification/positioning to lead with, prime demand is deepest for SDB / woman-owned / WOSB / minority and thinnest for 8(a)-JV. This is realized behavior, not self-reported intent.

### 5. Two-sided matchmaking surface, already built
This view (demand: obligated primes, by realized designation appetite) pairs directly with `govcon_subawardee_designations` (supply: 25,450 designated subawardees, verbatim-identical flag vocabulary). The shared flag names make prime-demand ↔ sub-supply a zero-drift join. The $86.6B realized volume is the visible size of that market.

**GTM action:** match a designated sub to primes whose realized behavior shows appetite for that designation in that NAICS/agency — both sides are now structured and joinable.

---

## The methodological insight (the most transferable one)

**Don't mine documents for what the structured layer already holds.** The original plan was to extract designations and subcontracting-plan requirements from solicitation PDF text. A controlled validation (400-file stratified sample over the already-downloaded 213 GB corpus) showed attachment text is a **strictly worse** signal:

- Designation recall vs known set-asides: **0.16 SDVOSB · 0.21 small-business · 0.0 8(a)** (the status lives in award metadata and the SF1449/clause matrix, not the SOW/spec bodies).
- False-positive rate on unrestricted awards: **0.22** (boilerplate clause-matrix mentions).
- Zero-text (scanned) blind spot: **11%**.

Meanwhile FPDS carries `has_subcontracting_plan` (100% populated) and the full designation flag set directly. The text pass would have produced a noisier copy of structured truth at the cost of a 42,637-file Modal extraction.

**Rule of thumb for govcon GTM questions:** ask the structured stack first — **FPDS** (award attributes, set-aside, subcontracting-plan, recipient self-certs) + **FSRS** (realized subawards) + **SAM Reps & Certs** (entity designations). Reserve text extraction for genuinely unstructured signals only — scope/labor demand, or the one residual here: *negotiated* subcontracting goal percentages, which live in the plan document and not in FPDS.

---

## Coverage & honesty

- **Obligation list — complete.** 26,573 is authoritative (100%-populated FPDS field).
- **Realized-sub layer — partial by construction.** Observable for 1,731 primes (6.5%), bounded by the upstream `contract_subaward` FSRS ingest (6,347 distinct primes total; 1,858 intersect the obligated set). This is upstream data coverage, **not** a join defect — verified. The unreported majority is correctly flagged, not silently dropped.
- **SDB / emerging-small-business** designation flags are NULL in the subawardee source (SDB folded into 8(a) in SAM; ESB is an FPDS-only construct), so realized-sub rollups exclude them. Prime-side SDB is present from FPDS.

---

## Query surface (zero-join, index-backed)

4 BTREE (`contract_award_unique_key`, `recipient_uei`, `recipient_parent_uei`, `naics_code`) + 21 BITMAP (business size, plan code, reported-subs, and every designation flag). Representative cuts:

```sql
-- Whitespace: obligated large primes not visibly subcontracting
SELECT recipient_name, awarding_agency_name, naics_description, current_total_value_of_award
FROM govcon_active_subcontracting_obligations
WHERE has_subcontracting_plan AND NOT has_reported_subs AND business_size_code='O'
ORDER BY current_total_value_of_award DESC;

-- Demand for a designation: primes that actually place SDVOSB subs, by $ placed
SELECT recipient_name, naics_description,
       sub_n_service_disabled_veteran_owned_business  AS n_sdvosb_subs,
       sub_amt_service_disabled_veteran_owned_business AS amt_sdvosb
FROM govcon_active_subcontracting_obligations
WHERE sub_n_service_disabled_veteran_owned_business > 0
ORDER BY amt_sdvosb DESC;
```

---

## Provenance

- **Datasets:** `govcon_active_awards` (FPDS active), `usaspending_api_fresh/contract_subaward` (FSRS), `govcon_subawardee_designations` (SAM Reps & Certs decode).
- **Build:** `pipelines/serving/materialize_active_subcontracting_obligations.py` (idempotent snapshot-overwrite, pre/post gates).
- **Validation that redirected the approach:** stratified 400-file sample extraction over the downloaded attachment corpus; designation lexicon + matcher (`pipelines/sam_gov/designation_lexicon.json`, `designation_match.py`) parked for the negotiated-goal-percentage residual only.
