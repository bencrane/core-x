# Federal Market Recon — Facilities Services (S2 Lane): Analysis & Disposition

**Date:** 2026-07-31
**Data source:** query-sidecar artifact `query_sidecar_20260726T231318Z.duckdb` (USASpending/FPDS-derived marts; 107.9M transaction rows; FSRS subaward canonicals). All dollar figures are **federal obligations**, FY2023–FY2025 unless stated. Obligations ≠ contractor revenue recognized ≠ margin.
**Method note:** "Entry band" throughout = awards whose 3-yr summed obligations fall in **$25K–$5M**. Award grain = `award_key` (obligations summed across transactions). Small-business attribution = FPDS contracting-officer size determination (`S`/`SMALL BUSINESS`).
**Provenance:** every table was produced by SQL against the sidecar in-session. Interpretive sections are explicitly labeled **[Interpretation]**. Regulatory statements cite their source and were not independently verified by counsel.
**Outcome up front:** the lane's demand data validated; every capture model available to this operator was examined and **declined** (§8). This document records the analysis and the disposition.

---

## 1. Scope convergence

Full-universe entry-band scan surfaced services as the largest small-firm-winnable pools (FY23–25, 3-yr):

| Family | Description | Entry-band $ | SB % of band |
|---|---|--:|--:|
| R4 | Professional/admin support services | $45.0B | 58% |
| Z2 | Repair/alteration of buildings | $18.3B | 84% |
| Y1 | Construction of structures | $12.2B | 78% |
| S2 | Housekeeping/facility services | $12.3B | 52% |
| J0 | Maintenance/repair of equipment | $13.9B | 40% |
| Z1 | Maintenance of real property | $8.6B | 74% |
| F0 | Natural-resource services | $5.7B | 95% |

Operator exclusions: no construction (Y1/Z2 — Miller Act bonding, 15–25% prime self-performance), no repair/maintenance (Z1, J0), no natural resources (F0), no guard (S206 — licensing/armed-liability regime), no food (S203), no fire protection (S202). **Remaining core lane:**

| PSC | Service | 3-yr $ | Awards | Median award | SB% | Set-aside% |
|---|---|--:|--:|--:|--:|--:|
| S201 | Custodial/janitorial | $6.23B | 12,486 | $129K | 45% | 28% |
| S208 | Landscaping/grounds | $3.06B | 10,094 | $98K | 65% | 21% |
| S216 | Facilities operations support | $2.86B | 6,812 | $98K | 61% | 17% |
| S205 | Trash/refuse | $1.77B | 5,724 | $104K | 64% | 25% |
| S222 | Waste treatment/storage | $0.88B | 8,468 | $46K | 69% | 22% |
| S215 | Warehousing services | $0.75B | 1,082 | $300K | 66% | 13% |
| S209 | Laundry/drycleaning | $0.72B | 2,456 | $90K | 60% | 22% |
| S207/S218/S214 | Pest / snow / carpet | $0.45B | 2,792 | $55–89K | 77–85% | 37–45% |
| **Core lane** | | **~$16.7B (~$5.6B/yr)** | ~50K | ~$100K | ~55% | ~25% |

Contracts are recurring by structure (typically 1 base year + option years); ~16–17K awards/yr churn through solicitation.

---

## 2. Compliance path (regulatory; sources cited, not legal advice)

Set-aside flavor split of the core lane (entry band):

| Flavor | 3-yr $ | Share |
|---|--:|--:|
| Unrestricted / no set-aside | $6.4B | ~75% |
| Status set-asides (8(a), SDVOSB, WOSB, HUBZone…) | $1.3B | ~15% |
| Plain total-small-business set-aside | $0.7B | ~8% |

- **Unrestricted (75%):** limitations on subcontracting (FAR 52.219-14 / 13 CFR 125.6) attach to set-aside awards only; no self-performance floor on unrestricted awards. A subcontract-out prime model is unconstrained here; awards are decided on evaluation (past performance, price).
- **Plain-SB set-asides (8%):** ≤50% of the amount paid may go to **non-similarly-situated** subs; payments to similarly-situated subs (small under the contract's NAICS) are exempt. Size standards: janitorial NAICS 561720 ~$22M; landscaping 561730 ~$9.5M; facilities support 561210 ~$47M — the local sub population predominantly qualifies.
- **Status set-asides (15%):** require similarly-**statused** subs; excluded from scope by operator decision.
- **Ostensible-subcontractor rule (13 CFR 121.103(h)):** under current SBA rules a similarly-situated sub performing primary/vital work is analyzed as a JV-type arrangement rather than automatic disqualifying affiliation; protest surface remains.
- **Bonding:** Miller Act applies to construction >$150K; the retained lane is services — no bonding regime.
- **Subcontracts on federal work are never informal:** paying a firm to perform a federal prime contract makes it a subcontractor with mandatory flow-downs (SCA wages/fringes, E-Verify, etc.) regardless of how the arrangement is papered.
- **Service Contract Act applies** to these contracts: wage-determination floors govern sub labor cost and compress achievable prime-sub spread.
- **Net bid-eligible with sub-out model intact: ~$7.1B/3yr (~$2.4B/yr).**

---

## 3. Buyer and access structure (data)

| Buyer | 3-yr $ | Share |
|---|--:|--:|
| DoD | $4.85B | 58% |
| VA | $1.04B | 12% |
| GSA | $0.81B | 10% |
| DHS, Interior, DOT, HHS, State, DOJ, USDA, other | $1.6B | 20% |

Access: on-base S2 work requires installation access credentialing (background screening, DBIDS-class badging, E-Verify) — **not security clearances**; facility-clearance work is a thin, explicitly-flagged niche. Civilian sites carry lighter vetting. Credential legwork belongs to the sub; performance accountability (cure notices, CPARS ratings) attaches solely to the prime's CAGE code — the government has privity only with the prime.

---

## 4. Site fragmentation and winner locality (data)

Site grain = place-of-performance zip (caveat: large installations span multiple zips; co-located buildings share one; per-buyer grouping splits co-located civilian sites).

| Side | Site profile | Sites | 3-yr $ | Avg vendors/site |
|---|---|--:|--:|--:|
| DoD | 3+ S2 service types in zip | 406 | $3.24B (81% of DoD) | 7.1 |
| DoD | 1–2 service types | 1,276 | $0.80B | ~1.4 |
| Civilian | 3+ service types | 423 | $1.52B (54% of civilian) | 6.2 |
| Civilian | 1–2 service types | 2,201 | $1.31B | ~1.5 |

Winner locality (recipient state vs. place-of-performance state, entry band):

| Side | In-state winners | Out-of-state winners |
|---|--:|--:|
| DoD | $2.01B (50%) | $2.02B (50%) |
| Civilian | $1.30B (46%) | $1.54B (54%) |

**[Interpretation]** "Distant prime, local fulfillment" is already the market norm (~52% of dollars). DoD is a hub market (81% of its dollars at ~7-vendor installations); civilian is a scatter market with lower access friction.

---

## 5. Participant population (data)

**5,372 distinct firms** won core-lane entry-band awards FY23–25:

| Metric | Value |
|---|--:|
| Median award won by a firm | $90K |
| Median firm 3-yr federal S2 book | $198K (≈$66K/yr) |
| p75 / p90 / p99 book | $835K / $3.7M / $21M |
| Median awards per firm (3 yrs) | **1** |
| p90 awards per firm | 8 |

Firm size (PDL/LinkedIn firmographic match ≈38%; unmatched skew smaller):

| Employees | Firms | Median 3-yr book |
|---|--:|--:|
| 1–10 | 601 | $241K |
| 11–50 | 471 | $281K |
| 51–200 | 329 | $668K |
| 201–500 | 160 | $758K |
| 500+ | ~460 | $155K–$1.3M |

**[Interpretation]** The median participant holds exactly one federal contract; federal work is incidental to a private commercial book (offices, HOAs, private facilities — revenue invisible to federal data). The 52% out-of-state win rate is consistent with local non-participation rather than local losses.

**Subaward economy (FSRS, FY23+):** reported subawards concentrate under S216 bundled contracts — **44 primes** paid **910 subs** $1.31B (median subaward $68K). Small-award PSCs are nearly unreported (S201: 48 subawards vs. 12,486 prime awards — reporting thresholds plus informality). Only **93 of 8,976** lane primes ever appear as S2 subawardees: primes and subs are largely distinct populations.

---

## 6. The adjacency heuristic (validated, retained for reference)

> Target contracts at sites where ≥1 small local firm already performs an adjacent S2 service (credentialed, revealed by award history) and does not hold the target contract — prioritizing where the current winner is out-of-state. Bid as prime; subcontract to the firm already inside the gate.

Support: 72% of lane dollars sit at multi-contract multi-vendor sites (§4); the sub population is abundant and under size caps (§5); legality per §2. The matching layers exist in the current data stack: award flows by county/zip (`pop_place_fy`, `pop_entity_fy`), 300K federal site locations (`federal_sites_lance`), registered entities + POCs (`gtm_sam_entities`, `sam_pocs`), prime→sub history (`gtm_prime_subout_by_recipient_code`, `gtm_prime_sub_pairs`).

---

## 7. Business-model economics (the decision layer)

**Prime-aggregator model** (win as prime, sub out execution):

- Revenue = contract value; earnings = prime-sub spread, **~10–20% gross [assumption]**, compressed from below by SCA wage floors and from above by admin burden.
- Mature facilities primes net mid-single digits; exit multiples on facilities books run ~4–8× EBITDA.
- Modeled trajectory: 10–30 contracts / $1–3M revenue (yr 1–2) → 100–250 contracts / $10–25M revenue / $1.5–5M gross (yr 3–5) → $50–100M revenue tier requires a decade-scale ops organization (where nationals already compete).
- Structural character: pass-through prime earns a broker's margin while carrying an operator's full liability (CPARS on every contract; SCA compliance for sub payrolls; one sub failure = a permanent rating on the prime's record). No compounding economic asset beyond the past-performance credential.
- Unresolved gate regardless of model: past-performance cold start on evaluated awards.

**Connector/marketplace model** (no prime role; sell matching):

- Demand side: the professionalized prime tier is ~44 firms (§5) — a call list, not a market; in-house BD is the entrenched alternative.
- Supply side: thousands of one-contract local firms — technically unsophisticated, minimal software budget, anchored by free government matchmaking (SBA SubNet, APEX Accelerators).
- Ceiling: niche two-sided data product, low-single-digit-$M ARR shape; intermediation is capped by the lane's total spread pool (~10–20% of ~$2.4B/yr).

---

## 8. Disposition

All capture models for this lane were examined and **declined** by the operator:

| Model | Reason declined |
|---|---|
| Self-performing operator | Labor/superintendent/bonding business; zero fit to operator's data/AI edge |
| Prime aggregator (sub-out fulfillment) | Broker margin with operator liability; ~$1–2M/yr net after ~5 years of sub-wrangling; low exit multiple; no compounding asset |
| Connector / data product | 44-buyer demand side; supply side unwilling/unable to pay; free government alternatives anchor price at zero |

**Recon result: negative with validated data.** The demand pool ($5.6B/yr core lane; $2.4B/yr bid-eligible), fragmentation structure, and participant population are real and documented above; no examined mechanism converts them into a business that clears this operator's bar. The lane analysis is retained as reference — the fragmentation/adjacency findings and the participant dataset remain reusable for any future federal-services market question.

---

## 9. Data caveats

1. Entry-band definition ($25K–$5M summed obligations) proxies winnability; not a legal or contractual category.
2. Zip-grain site analysis under- and over-merges physical sites (§4 caveat).
3. FPDS CO size determinations are per-action and can be inconsistent across a firm's awards.
4. Firmographic coverage ≈38%; the size table describes the matched subset.
5. FSRS subaward data materially underreports the true sub economy (thresholds + noncompliance); §5 subaward figures are floors.
6. Set-aside share computed from `type_of_set_aside_code` presence; FPDS coding practices vary by agency/era.
7. Sidecar artifact is a 2026-07-26 snapshot; FY2025 figures near-complete but not final.
8. An early in-session small-business share table used a faulty `ILIKE '%SMALL%'` match (caught and corrected); all SB figures here use corrected `IN ('S','SMALL BUSINESS')` logic.

## 10. Artifacts

| Artifact | Path |
|---|---|
| This document | `~/Desktop/hq/2026-07-31-federal-market-recon-facilities-services.md` |
