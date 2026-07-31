# Federal Market Recon — Make-to-Print Parts (In-House Production Cell)

**Date:** 2026-07-31
**Data source:** query-sidecar artifact `query_sidecar_20260726T231318Z.duckdb` (USASpending/FPDS-derived marts; 107.9M transaction rows). All dollar figures are **federal obligations**, FY2023–FY2025 unless stated. Obligations ≠ contractor revenue recognized ≠ margin.
**Method note:** "Entry band" throughout = awards whose 3-yr summed obligations fall in **$25K–$5M** — a proxy for contracts winnable by a new small firm. Award grain = `award_key` (obligations summed across transactions). Small-business attribution = FPDS contracting-officer size determination (`S`/`SMALL BUSINESS`).
**Provenance:** every table was produced by SQL against the sidecar in-session. Interpretive sections are explicitly labeled **[Interpretation]**. Regulatory statements cite their source and were not independently verified by counsel.

---

## 1. Origin of the thesis

1. **Starting hypothesis (operator):** US-manufacturing entry via federal demand, textiles/apparel lane (hospital gowns, scrubs) — low-expertise sewn goods vs. high-expertise aerospace. Motivated by SBA 7(a)/504 financing tailwinds.
2. **Textiles finding:** the sewn-goods universe (PSC 83xx/84xx/6532) obligates ~$3.1B/yr but is structurally gated: Berry Amendment domesticity, AbilityOne set-asides (National Industries for the Blind and prison industries among top-25 incumbents), DLA dominance ($5.4B of the $9.4B 3-yr universe), incumbent concentration (top 10 primes = 29% of dollars; top 50 = 69%).
3. **Pivot:** operator open to non-textile production given AM/CNC feasibility, excluding safety-critical items. Recon moved to commodity hardware PSCs, then widened to the full hard-goods universe.
4. **Standing operator constraints:** in-house-only fulfillment (no broker/partner production — "we do everything in-house or we don't do it"); no equipment-repair (J0) services angle; no safety-critical/flight-critical part families.

---

## 2. Market size (data)

Entry-band dollars, hard-goods FSC families (FY23–25, 3-yr):

| Segment | 3-yr entry-band $ | Awards |
|---|--:|--:|
| Mechanical/fabricated-parts FSCs (16, 20, 23, 25, 29, 30, 31, 39, 40, 43, 47, 48, 49, 51, 53, 63, 81, 93, 95) | ~$25B (~$8B/yr) | ~199K |
| All numeric-PSC (product) families, entry band | ~$150B (~$50B/yr) | — |

Award-size distribution, selected 4-digit PSCs (per-award 3-yr totals):

| PSC | Description | Awards | Median | p90 | # ≥$1M | % of $ in $1M+ awards |
|---|---|--:|--:|--:|--:|--:|
| 5330 | Gaskets/packing | 78,177 | $1K | $12K | 9 | 4% |
| 5365 | Bushings/rings/shims | 33,293 | $1K | $9K | 2 | 2% |
| 4730 | Fittings/hose/tube | 53,043 | $1K | $21K | 20 | 13% |
| 4820 | Valves (non-powered) | 38,832 | $7K | $64K | 83 | 17% |
| 3040 | Power transmission parts | 21,476 | $5K | $51K | 52 | 25% |
| 2510 | Vehicle cab/body/frame | 11,271 | $2K | $37K | 21 | 25% |
| 8145 | Shipping/storage containers | 5,487 | $15K | $172K | 110 | 47% |
| 2590 | Misc vehicular components | 12,618 | $4K | $95K | 118 | 74% |
| 9330 | Fabricated plastics | 5,720 | $1K | $23K | 31 | 77% |
| 9390 | Fabricated nonmetallics | 1,855 | $4K | $48K | 13 | 77% |

**[Interpretation]** Two distinct sub-markets: a fastener/gasket ocean (median order ~$1K; a quoting-automation game with incumbent bid-bots) and pooled lanes (8145/9330/9390/4820) where 47–77% of dollars sit in $1M+ awards — the band where an owned production facility is economically plausible. The top of 2590 (max award $580M) is OEM-adjacent platform support, not entry ground.

Small-business and set-aside shares, target hardware PSCs (FY23–25): SB dollar share 55–83%; set-aside dollar share 8–39% (e.g., 5340 hardware 69% SB / 14% set-aside; 9390 83% SB / 39% set-aside). **[Interpretation]** Small firms win most of these dollars in open competition; no special status is required to compete, though statuses add a fenced lane.

---

## 3. Demand-side data infrastructure (executed this session)

- **DIBBS (DLA Internet Bid Board System)** is the pre-award faucet: ~1,800 RFQ lines/day observed. Daily artifacts: fixed-width index `in{yymmdd}.txt` (~260KB; solicitation #, NSN, quantity, due date, PDF filename, code block incl. set-aside/acquisition-method flags), solicitation-PDF bundle `ca{yymmdd}.zip` (≥245MB), batch-quote `bq{yymmdd}.zip`, plus Awards-area daily files (per-NSN winning unit prices, absent at FPDS grain).
- **Files are destroyed after ~10 business days** (verified: older dates 404). Bulk history is unrecoverable once missed; capture must be continuous.
- The DoD-consent gate is a scriptable ASP.NET postback (verified end-to-end in-session; a real index file was downloaded).
- **FLIS/FedLog** (per-NSN criticality/AMC-AMSC master data) has **no verified public bulk endpoint**; criticality and competition-code signals come from the solicitation artifacts themselves.
- **Capture directive written and hardened** (adversarially reviewed; 8 findings remediated): `~/Desktop/hq/directives/2026-07-29-dla-dibbs-daily-rfq-capture-phase-1.md`. Scope: daily Trigger.dev-scheduled, Modal-executed capture of 4 artifact families to R2 raw storage; LLM/parse work deferred to a Phase-2 directive. **Not yet executed.**
- Pipeline stages: (1) index/manifest layer and (2) raw PDF content are captured together (DIBBS pre-bundles them); (3) parsing is Phase 2 — non-time-critical and infinitely re-runnable once raw bytes are landed.

---

## 4. Validation design (agreed, not yet run)

**Shadow-bid test (Tranche 0, capital-light).** The government publishes both demand (DIBBS solicitations) and outcomes (DIBBS Awards + FPDS: winner and price). Therefore win-rate and margin can be estimated **without bidding**:

1. Write a production-cell envelope spec (machines, materials, size envelope, tolerances, finishes) before the test starts.
2. Capture DIBBS daily for 90 days; auto-filter to in-envelope, non-safety-critical, open-competition solicitations.
3. Shadow-price each one; compare against actual published award prices.
4. Output: would-have-won rate and would-have-margin. Kill criteria fire before any machine is bought.

Staging: Tranche 0 (shadow quarter, ~one engineer-quarter of cost) → Tranche 1 (buy the exact cell the shadow data specifies; SBA 504-financed; first real bids only after in-house first-article capability) → Tranche 2 (widen the envelope by machine class/material/finish only where adjacent-envelope shadow data shows margin). Growth = envelope expansion, never fulfillment outsourcing (operator constraint).

**[Interpretation] Moat claim.** The difficulty in this market is informational, not mechanical: (1) seeing all ~1,800 daily solicitations vs. the ~30 a shop owner reads; (2) pricing against full public award history; (3) collapsing print-to-quote cost with vision-assisted estimating; (4) automating compliance mechanics (WAWF, DFARS flow-downs, MIL-STD-2073 packaging, first-article paperwork). The median competitor is a small shop with an aging owner; machines are commodities, the funnel is not. Every quote cycle accumulates proprietary pricing data.

---

## 5. Known risks

- Obligations ≠ margin; DLA applies price-reasonableness against procurement history. Assume make-to-print gross margins in the 15–30% range, not software margins.
- Quoting bots and parts brokers already operate in the low-value DIBBS ocean; automation there is table stakes.
- Supply-side costs (cost to produce compliant parts, first-article testing, packaging) are unmeasured; the shadow quarter measures the demand side only.
- Utilization risk transfers to owned capex at Tranche 1; the cell must be sized to the floor of demonstrated in-envelope flow, not the average.
- In-envelope TAM at any moment is a fraction of total flow — a first cell plausibly sees $0.5–1.5B/yr of biddable demand (estimate, not measured).

---

## 6. Data caveats

1. Entry-band definition ($25K–$5M summed obligations) proxies winnability; it is not a legal or contractual category.
2. FPDS CO size determinations are per-action and can be inconsistent across a firm's awards.
3. Set-aside share computed from `type_of_set_aside_code` presence; FPDS coding practices vary by agency/era.
4. Sidecar artifact is a 2026-07-26 snapshot; FY2025 figures near-complete but not final.
5. An early in-session small-business share table used a faulty `ILIKE '%SMALL%'` match (caught and corrected); all SB figures here use corrected `IN ('S','SMALL BUSINESS')` logic. An early hardware-PSC dollar table double-counted via a duplicate-name `psc_reference` join; all figures here are from corrected, join-free aggregations.

## 7. Artifacts

| Artifact | Path |
|---|---|
| DIBBS daily capture directive (Phase 1, paste-ready) | `~/Desktop/hq/directives/2026-07-29-dla-dibbs-daily-rfq-capture-phase-1.md` |
| Adversarial review of the directive (8 findings, all remediated) | `~/Desktop/hq/directives/2026-07-29-dibbs-directive-adversarial-review.md` |
| This document | `~/Desktop/hq/2026-07-31-federal-market-recon-make-to-print-parts.md` |
