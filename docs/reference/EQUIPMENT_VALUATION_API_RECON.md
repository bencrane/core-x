# Heavy Equipment Pricing & Valuation APIs — Provider Recon

**Purpose:** evaluate the four industry-standard equipment-data providers as a candidate **pricing/valuation oracle**
for the rental & equipment-finance GTM motion — real-time FMV/FLV, standard rental rates, market data — and
identify which (if any) is integratable by a startup as a programmatic dependency versus an enterprise sales motion.

**Scope of "oracle":** machine-consumable valuation (FMV/FLV/OLV, retail/auction), standard rental rates,
operating/ownership cost, and a **licensable taxonomy** to align scraped inventory to canonical model IDs.

**Date:** 2026-06-22 · web-research recon (4 parallel agents, primary-source-verified) · no SoR writes

---

## TL;DR — The Verdict

| Provider | Real REST API? | FMV/FLV/OLV | Rental Rates | Op/Own Cost | Licensable Taxonomy | Query by Serial | Self-Serve | Integratability |
|---|---|---|---|---|---|---|---|---|
| **EquipmentWatch** | **Yes — 7 products** | ✅ (adj + unadj) | ✅ daily/wk/mo | ✅ (rolled-up) | ✅ **dedicated Taxonomy API + aliasing** | ✅ Verification API | ❌ key-request | **#1 — only true dev API** |
| **Rouse Services** | Partner/private only | ✅ retail/whsl/auction/midpt + OLV/FLV/NOLV | ✅ **best-in-class benchmark** | client-provided, benchmarked | ❌ proprietary, internal | ✅ (ingested) | ❌ white-glove | #2 — co-op SaaS, not API |
| **FleetEvaluator (Sandhills/VIP+)** | **No API** (SaaS only) | ✅ FMV/OLV/FLV + asking/auction | ❌ none | ❌ none | ❌ marketplace trees only | ✅ VIN/serial decode | partial (5 free/day) | #3 — best asking-price data, no pipe |
| **Ritchie Bros. (RB/IronPlanet)** | **No** (web lookup + PDFs) | sold-price comps only | ❌ none | ❌ none | ❌ none | ❌ not a search key | free UI only | #4 — funnel data, use Rouse instead |

**One-line recommendation:** **EquipmentWatch is the only provider exposing a productized, documented REST API
surface** spanning valuation, rental rates, cost, specs, serial-decode, and a licensable taxonomy — it is the
default integration target. Everything else is a sales-gated SaaS (Rouse, Sandhills) or a free manual lookup
(Ritchie Bros.). See [§5 Feasibility](#5-feasibility-recommendation).

---

## EquipmentWatch  — *(Fusable / Randall-Reilly; sibling = Price Digests)*

A **real, productized REST+JSON API suite** — 7 named products, each with documented endpoint and field names.
Public developer portal at `docs.equipmentwatchapi.com` (client-side JS app, key-gated). Every product is gated
behind a "Request an API Key" form: no self-serve signup, no public Swagger, no published pricing.

**Universal join key:** `modelRdbId` ("Rdb" = reference database) ties every product together. Common identity
envelope on every response: `modelRdbId`, `modelName`, `modelAliases`, `manufacturerRdbId/Name`,
`categoryRdbId/Name`, `equipmentSubtype*`, `equipmentSubtypeSize*`, `year`, `revisionDate`.

### 1. Data Availability (The Payload)
- **Values API** → **FMV, FLV, OLV** (adjusted + unadjusted): `adjustedFmv`, `adjustedFlv`, `unadjustedFmv`,
  `unadjustedFlv`, `original`; adjustment inputs `meterreads`, `condition`, `country`, `subdivision`.
  Endpoints: `GET Values`, `GET Options & Extras`, `GET Value Trending` (current + trended).
  *Gap:* no discrete retail/wholesale/trade-in/residual fields — those live in Market Data, not Values.
- **Retail Rental API** (the "Rental Rates" product) → `nationalAverageRetailRentalRates` /
  `averageRetailRentalRates[]` each with **`daily_rate`, `weekly_rate`, `monthly_rate`, `date`**, at national /
  regional / specific-rental-house granularity. Endpoints: `GET Rental Rates by Model | by Size | Rental Houses |
  Rental House Rates`. Coverage: Earthmoving + Lift/Access (not Ag).
- **Costs API** (Cost Recovery, sourced from the **Rental Rate Blue Book**) → `ownershipCost` (annual),
  `hourlyOwnershipCost`, `hourlyOperatingCost`, `fhwaRate`. Endpoints: `GET Cost Recovery Rate`, `GET Internal
  Charge Rate, Ownership | Operating`. *Gap:* fuel/tire/maintenance/depreciation are **rolled up** into
  `hourlyOperatingCost`/`ownershipCost`, not exposed as discrete line items via API.
- **Market Data API** → auction/resale transactions: `saleType`, `condition`, `date`, `price`,
  `marketplaceName/Url`, `country/state/city`. Endpoints incl. `GET Market Data, Auction | Resale |
  by Serial Number | Volume Trending`, `Popularity`, `Utilization`.
- **Specs API** → 33k+ models, parsed spec families. **Integration API** → round-trip saved assets/groups
  (its sample body carries an `"apiKey"` field).

### 2. Taxonomy / Search Keys
- **Query by:** serial number (Verification + Market Data), make/model/year (`modelRdbId` or mfr/model/year),
  category/subtype/size (Taxonomy).
- **Taxonomy API** — the licensable classifier: 3-level **Category → Subtype → Size Class**, 100k+ models across
  Earthmoving, Lift/Access, Ag. Fields: `categoryRdbId/Name/Slug`, `subtypeRdbId/Name/Slug`, `sizeId`,
  `sizeClass` (sibling Price Digests exposes numeric `sizeClassMin/Max/Uom`). **Critically, ships extensive
  aliasing to normalize naming + compensate for input errors** — this is the intended mechanism for mapping
  messy/scraped inventory strings → canonical model IDs. *Open question:* whether the taxonomy/alias crosswalk
  is licensable as a **bulk dataset** vs. per-call lookups only — not stated publicly; a sales conversation.
- **Verification API** ("Serial Number Correction") → verifies year-of-manufacture from serial for ~30k models:
  `serialNumber`, `year`, `highSerialNumber`, `lowSerialNumber`. `GET Year Verification | Serial Number Location |
  Manufacturer Notes`. Powers a standalone iOS/Android app.

### 3. API Access & Pricing
- **Docs:** partially public (product-level endpoint lists + sample JSON at `equipmentwatch.com/api/<product>/`);
  interactive reference is key-gated.
- **Auth:** **API key** (confirmed via Integration API sample). No OAuth observed. Exact header transport
  (`x-api-key` vs `Bearer` vs query) not publicly documented.
- **Model:** enterprise sales + data licensing, partner-gated; pitched as org-level/seat-displacing licensing on
  a ~15k-user subscription base. Listed on **AWS Marketplace** (possible procurement path). **No public pricing.**

### 4. Developer-Friendliness
REST+JSON; API-key auth. **No public OpenAPI spec, no published base URL, no documented rate limits, no sandbox,
no SDKs.** Positioned for enterprise integration (JD Edwards, SAP, HCSS, Sage/Timberline) — integration-led,
not indie-developer-led. Still, **materially the most developer-friendly of the four.**

### Confidence & Gaps
- **Unverifiable (key/sales-walled):** exact auth header format, live base URL/host, pricing, rate limits,
  sandbox, SDKs, OpenAPI. Assume "none public" until a key is issued.
- **The single most important open question:** is the taxonomy/alias crosswalk licensable as a bulk file? This
  is the make-or-break for a scraped-inventory mapping use case — raise directly with EW sales.

### Sources
- https://equipmentwatch.com/api/ · https://equipmentwatch.com/api/values/ · https://equipmentwatch.com/api/costs/
- https://equipmentwatch.com/api/market-data/ · https://equipmentwatch.com/api/specs/ · https://equipmentwatch.com/api/taxonomy/
- https://equipmentwatch.com/api/verification/ · https://equipmentwatch.com/api/rental/ · https://equipmentwatch.com/api/integration/
- https://equipmentwatch.com/resource/api/ · https://docs.equipmentwatchapi.com/ · https://apis.io/providers/equipmentwatch/
- https://pricedigests.com/api/taxonomy/

---

## Rouse Services (Rouse Analytics)  — *(RB Global subsidiary, acq. 2020 ~$275M)*

Equipment data/intelligence arm of RB Global. Three products: **Appraisals**, **Fleet Manager** (incl.
**Equipment Insights** valuations), **Rental Insights** (rate/utilization benchmark). Model = **enterprise SaaS
analytics + managed service + a data-contribution co-op**, *not* a developer API.

### 1. Data Availability (The Payload)
- **Transaction-based valuation** (the differentiator vs EW's appraisal-book model): values derived from actual
  observed sales (retail, wholesale/trade-in, auction), updated monthly, back-tested monthly. ~100k makes/models,
  ~$59B/yr transactions evaluated.
- **Equipment Insights** → **Retail, Wholesale, Auction, Midpoint**, adjusted for meter hours, config, region.
  Rouse is "the only company authorized to use Ritchie Bros. transactional data" — a structural data moat.
- **Appraisals** → **OLV, FLV, NOLV, NFLV**, **Residual Value** (lease-finance), **Fair Value (ASC 805)**.
  (Emphasizes liquidation values; FMV implied by retail/midpoint, not branded "FMV book.")
- **Rental Insights** (richest payload) → Daily/Weekly/Monthly rates (monthly = 28-day) with client-vs-benchmark
  + distributional **Bench Max/Top-Q/Bottom-Q/Min** (share-weighted); "Monthly New" fresh-rate signal; MoM/YoY
  rate trends; **physical (time) utilization** + **dollar (financial) utilization** (ROC story); fleet age,
  investment (OEC), on-rent growth; book-rate discount/premium.
- **Op-cost/depreciation:** surfaced in the Rental Assets Grid but **client-provided** (LTD/YTD maintenance,
  labor, OEC, **NBV**) — Rouse benchmarks the client's own cost data; it does not publish a modeled cost curve.
- **Delivery:** SaaS portal (`*.rouseanalytics.com`), mobile app, Excel-exportable grids, managed monthly/
  quarterly reporting. Ingest via **ERP-tethered nightly feeds** (unit snapshot + invoice-line detail).

### 2. Taxonomy / Search Keys
- **Dual-key crosswalk** (most useful finding): client ERP supplies raw `Serial/VIN, Make, Model, Model Year,
  Category, Cat Class, Equipment Type, Equipment ID`; Rouse maps to standardized **Rouse Make / Model / Category /
  Product Type** (Product Type = the cross-company benchmarking join key).
- **Taxonomy:** 4-tier **Category → Subcategory → Make → Model**, expert-curated (not purely algorithmic).
  **Not published or licensable standalone** (unlike EW's Taxonomy API). Public bridge = industry-standard
  **Cat Class** (ASA/AED RentalMan) — Rouse keys on it; scraped inventory can be Cat-Class-mapped independently.
- **Fleet-list ingestion:** yes — core model is "hand us your fleet + invoices, we match + benchmark." Fleet
  Manager supports AI Excel load with OEM validation. Strong fit if you want to hand over a list and get values.
- **RB relationship:** a *data* advantage (exclusive RB/IronPlanet auction data), not an *access* advantage — no
  free tap; everything gated through Rouse commercial products.

### 3. API Access & Pricing
- **API:** enterprise/partner-gated. No public docs/portal/OpenAPI. Secondary sources note private "API endpoints"
  for customers + BI connectors (TARGIT) — partner APIs exist but are **not self-serve or documented**. Rouse
  pushes data *into* host ERPs (Point of Rental, MCS, Fame/integraRental, TARGIT) rather than exposing a pull-API.
- **Model:** subscription + managed analytics on a **data-contribution membership**. Rental benchmarks require
  **≥5 participant companies** per cell, reported on a **≥90-day lag** (trailing-12-mo trendline for recent
  periods), antitrust-governed. **For Rental Insights you generally must contribute your own data** (true co-op).
  Equipment Insights *values* come from Rouse's own corpus and are not subject to that contribution gate.

### 4. Developer-Friendliness
Low. No self-serve keys, no public OpenAPI/SDK/sandbox. White-glove onboarding (ERP tethering, analyst-supported
mapping, managed reporting). "Integration" = pre-built connectors into a few rental ERPs. For a startup,
this is an enterprise sales + data-contribution motion, not a plug-in API.

### Confidence & Gaps
- **High:** rental metric set, dual-key crosswalk, 4-tier taxonomy, 5-participant/90-day pooling rules,
  value types, exclusive RB data license, ERP-feed delivery (all primary-sourced incl. live dashboard guide PDF).
- **Unverifiable:** pricing (none public); scope/spec of the private customer "API endpoints"; whether Equipment
  Insights is licensable à la carte without the rental co-op; current participant count.

### Sources
- https://www.rouseservices.com/ · /solutions/rental-insights/ · /solutions/appraisals/ · /solutions/fleet-manager/
- https://rdo.rouseanalytics.com/assets/images/RDO_User_Guide.pdf (dashboard user guide — primary metric/key ground truth)
- https://rbglobal.com/insights/rouse-equipment-insights/ · https://rbglobal.com/services/rouse-appraisals/
- https://www.point-of-rental.com/introducing-rouse-rental-and-equipment-insights-... · https://www.targit.com/.../data-sources/rouse
- https://www.rermag.com/.../rouse-analytics-rental-benchmark-service-tops-50-participants · https://www.integrarental.com/integrations/rouse-analytics/

---

## FleetEvaluator (Sandhills Global / MachineryTrader)  — *(now "Value Insight Portal" / VIP+)*

**Naming (load-bearing):** "FleetEvaluator" is legacy. The engine is rebranded **Value Insight Portal (VIP)**
(free/light) and **VIP+** ("formerly FleetEvaluator," enterprise) inside **Sandhills Cloud**. Same payload.

### 1. Data Availability (The Payload)
- **Values:** **FMV, OLV, FLV** (the three lender values, verbatim) + **retail and auction**, location-adjusted;
  VIP+ widens to **auction, wholesale, market, asking**.
- **Asking-vs-sold spread is the differentiator** — engine sits on live marketplace **asking-price** inventory
  (MachineryTrader/TractorHouse/TruckPaper/ForestryTrader) + **AuctionTime** results. ~25k listings/wk editor-
  reviewed into 4.7M+ assets; fueled by **~$182B/yr** of data.
- **Trade-In tool** (field-rep pricing) is a wrapper on the same engine. **FutureCast** = forecasted auction +
  retail values **up to 5 years** forward.
- **Equipment Value Index (EVI):** monthly published reports (asking/auction values, inventory levels, **EVI
  spread** = asking-vs-auction %), construction/ag/trucking — **reports/charts + email only, no feed.**
- **Rental rates:** **none** (valuation product). **Op-cost / cost-to-own:** **none** (that's EW's turf;
  depreciation only implicit in FutureCast curves). **REST API: NONE** — analyst-confirmed "do not offer any
  advanced delivery mechanisms, like API." Delivery = web/portal/mobile + Trade-In tool + partner integrations.

### 2. Taxonomy / Search Keys
- **Query:** enter **serial/VIN** → auto-populates make/model/year (strong decode); or make/model/category/year +
  condition. Adjusters: **Model Year, Region, Configuration, Usage (hours)**.
- **Coverage:** Construction, Lift/Access, Ag, Transportation (Class 1–8), trailers, forestry, RV.
- **Licensable taxonomy:** **none** — category trees exist only as marketplace navigation; no standalone
  crosswalk feed. Real gap for scraped-inventory mapping (you'd reverse-engineer category pages).

### 3. API Access & Pricing
- **No public API docs, no portal, no OpenAPI, no sandbox** — fully sales-gated via lead forms.
- **Model:** data-monetization on the marketplaces. **VIP (light):** freemium, **5 free valuations/day**; free
  white-label VIP page for dealers. **VIP+ (enterprise):** color-coded pricing, Customer Assets tab, exportable
  **"Detailed Pricing Analysis" XLSX** (closest thing to bulk export — manual, not a feed). Underlying
  **cost-per-asset/per-valuation** model, quote-based. Sold to lenders, dealers, fleets.

### 4. Developer-Friendliness
**Effectively zero self-serve surface.** No keys/OpenAPI/SDK/sandbox. Integration = packaged ISV partnerships
(e.g., **InTempo ↔ Sandhills Cloud**), not an open API. Only "data out" for a consumer = **manual XLSX** or
**white-label web embed** (no JSON). All Sandhills marketplace domains are **Cloudflare/PerimeterX bot-walled** —
hostile to scraping the underlying inventory.

### Confidence & Gaps
- **High:** value types, query inputs/adjusters, taxonomy coverage, **no-API status**, freemium + cost-per-asset
  model, EVI reports-only, rental/cost-to-own gaps (cross-verified vs archived primary pages + The Heavy analyst).
- **Unverifiable:** VIP+ enterprise pricing; field-level response schema (no spec); whether Sandhills will do a
  custom bulk/batch agreement (plausible given InTempo precedent, not advertised). Live pages bot-walled —
  facts recovered via Wayback + search extracts.

### Sources
- https://www.sandhills.com/news/article/25321 · https://www.prnewswire.com/news-releases/sandhills-global-launches-value-insight-portal-...-301336649.html
- https://web.archive.org/web/20230526143148id_/https://www.fleetevaluator.com/financial-institutions/details/ (archived primary: FMV/OLV/FLV, FutureCast, Trade-In)
- https://web.archive.org/web/20250614174922id_/https://www.totheheavy.com/glossary/fleetevaluator-by-sandhills/ (analyst glossary: inputs, "no API", cost-per-asset)
- https://www.sandhills.com/news/article/250045249 (EVI methodology) · https://www.intemposoftware.com/product/features/sandhills-cloud (ISV integration, not API)

---

## Ritchie Bros. (Rouse / IronPlanet ecosystem)

**Blunt verdict:** **no public, self-serve, developer-facing valuation/pricing API anywhere in the RB ecosystem.**
RB-branded data = web-UI lookup ("Price Results", free), PDF market reports, and an enterprise SaaS (RB Asset
Solutions / IMS) whose only "API" is a customer *inventory-upload ingress*, not a pricing egress. The sole entity
with a genuine valuation feed is **Rouse** (covered above).

### 1. Data Availability (The Payload)
- **Sold-price results — yes, UI-only.** **Price Results** (`app.rbassetsolutions.com/rbpriceresults`) — free,
  registration-gated, aggregates **RB auctions + Marketplace-E + Mascus** sold prices; filter by brand/model/
  year/meter/region; **depth only ~3 months–2 years**, no export. Plus public **Recent Results** /
  IronPlanet/GovPlanet/TruckPlanet **Auction Results** browsing.
- **Not programmatically queryable** — HTML lookup tools, no endpoint/feed. Only programmatic access in the wild
  is **third-party scraping** (Apify), unofficial + ToS-risky, with make/model/year/serial buried in free-text.
- **Market trends — free PDFs.** Rouse-powered quarterly **"Construction & Transportation Market Trends Report"**
  with ML-derived **price indexes** — human-readable, **no machine-readable feed**.
- **Rental rates: none** (RB-side). **Op-cost/depreciation:** only as in-app IMS analytics, not a data product.

### 2. Taxonomy / Search Keys
- **Public query keys:** make/brand, model, year, meter, region, category. **No serial/VIN search** on valuation
  tools (serial is a display field only). **No published/licensable taxonomy.**
- *(Note: third-party "IronGuides"/Iron Solutions sells a serial/spec/valuation API — a **separate company**,
  not RB Global. Do not conflate.)*

### 3. API Access & Pricing
- **No public API docs / developer portal.** `rbauction.com/api/auth/*` = internal Next.js auth; `github.com/
  rbauction` = internal Salesforce tooling only. The one real RB "API" is **IMS inventory upload (ingress)** —
  pushes *your* fleet in, does not pull RB market data out.
- **Model:** data is an **auction-commission byproduct / funnel** — Price Results is free lead-gen; monetized
  analytics is **bundled into Rouse**. Bulk/partner data = bespoke contract via `dataproducts@ritchiebros.com`.

### 4. Developer-Friendliness / does an oracle even exist
**No self-serve API/keys/OpenAPI/SDK** for valuation data. Realistic access, ranked: (1) **Rouse** — the only
machine-consumable feed (separate, enterprise); (2) **free Price Results web lookup** — manual, ~2-yr depth, no
serial search, not automatable without scraping; (3) **partner data agreement** — undocumented. **Treat RB as a
free manual comp source + free market-index PDF, not an integratable oracle.**

### Confidence & Gaps
- **High:** no RB/IronPlanet valuation REST API exists; Price Results free/UI-only/shallow; Trends Report = free
  Rouse-powered PDF; IMS "API" = inventory upload.
- **Unverifiable:** terms/pricing behind `dataproducts@ritchiebros.com`; formal exclusivity clause wording
  (Rouse marketing says "only company authorized"; the 2020 press release doesn't use "exclusive").

### Sources
- https://app.rbassetsolutions.com/rbpriceresults · https://blog.ritchiebros.com/rbas-price-results-tool/
- https://www.ironplanet.com/main/auction_results.jsp · https://rbglobal.com/insights/rouse-equipment-insights/
- https://investor.rbglobal.com/news/news-details/2020/Ritchie-Bros.-and-Rouse-team-... · https://github.com/rbauction
- https://apify.com/lulzasaur/ironplanet-scraper/api/openapi (unofficial 3rd-party scraper)

---

## 5. Feasibility Recommendation

**Integrate EquipmentWatch as the primary valuation/rental/cost oracle.** It is the *only* provider with a
productized, documented REST+JSON API spanning the full payload the GTM motion needs:

- **Valuation** (Values API: FMV/FLV/OLV, adjusted + unadjusted, trended)
- **Standard rental rates** (Retail Rental API: daily/weekly/monthly, national→rental-house granularity)
- **Operating/ownership cost** (Costs API: hourly ownership/operating, FHWA)
- **Serial-number decode** (Verification API) — the bridge from a scraped serial to a canonical model + year
- **A licensable, alias-equipped taxonomy** (Taxonomy API: `modelRdbId` + aliasing) — the single feature that
  directly solves "align our scraped inventory lists to a standard database," which **no other provider offers
  as a product**.

The two friction points are uniform across the entire category and not disqualifying for EW: (a) **no self-serve
signup** — all four are key-request/sales-gated; (b) **no public pricing**. EW's edge is that once a key is
issued you get real endpoints with documented fields, plus an **AWS Marketplace** procurement path.

**Pair, don't replace:** EW's valuation is **appraisal-book-derived**. If the finance use case demands
**transaction-realized** values (what iron *actually* sold for), layer in **Rouse Equipment Insights** (retail/
wholesale/auction/midpoint off the exclusive RB auction corpus) — accepting it is an enterprise SaaS/co-op, not
an API. For pure **asking-price + auction-spread** signal, **Sandhills VIP+** is strongest but delivers only via
XLSX/white-label, no pipe. **Ritchie Bros. direct is not an integration target** — use its free Price Results for
manual comps only; its real data is Rouse.

### Recommended sequencing
1. **EW sales call — lead with one question:** *is the Taxonomy + alias crosswalk licensable as a bulk dataset
   (for offline scraped-inventory matching), or per-call lookup only?* This determines whether EW can be the
   inventory-normalization layer or just a per-asset valuation lookup. Confirm auth header format, base URL,
   rate limits, and whether pricing is per-call vs flat license (pursue the AWS Marketplace contract path).
2. **Prototype against EW** Values + Retail Rental + Verification + Taxonomy once keyed.
3. **Evaluate Rouse Equipment Insights** as a transaction-realized valuation overlay for the finance side — but
   budget for enterprise SaaS + (for rental benchmarks) a data-contribution co-op, not a self-serve API.
4. **Sandhills VIP+ / RB Price Results** = supplementary/manual comp sources, not pipeline dependencies.

### Independent fallback regardless of vendor
**Cat Class (ASA/AED RentalMan)** is the one public, industry-standard classification both Rouse and rental ERPs
key on. Mapping scraped inventory → Cat Class independently is a vendor-agnostic normalization hedge that keeps
the pipeline from being locked to a single provider's proprietary taxonomy.
