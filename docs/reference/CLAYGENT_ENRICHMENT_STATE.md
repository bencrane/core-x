# Claygent GTM Enrichment — Canonical State of Affairs

**Status snapshot date: 2026-06-24.** Every quantitative claim below is tagged with one of three
labels and (for facts) the query that produced it. Re-run the appendix queries to refresh.

- **[VERIFIED FACT]** — backed by a query actually run against the live stores this session.
- **[OPERATOR INTENT]** — a stated goal or plan, NOT yet built / not yet true on disk.
- **[RECOMMENDATION]** — analysis/opinion derived from the facts; a proposed action, not a fact.

Two stores are in play:
1. **Postgres control-plane** (`HQX_DB_URL_POOLED`, schema `gtm`) — holds the **raw heterogeneous
   payload sink** `gtm.existing_claygent_payloads`. This is the system of record for the *raw*
   Claygent JSON.
2. **LanceDB on Cloudflare R2** — `s3://data-sink/active/companies/` is the Gen-3 **system of
   record for the companies spine** (firmographic identity graph). Addressed by R2 URI, no catalog.

---

## 1. Purpose & how to use this doc

This document brings a fresh agent (or the operator) fully up to speed on the GTM enrichment data
that exists today, so a decision can be made: **RE-LLM-ENRICH** the data, or **COALESCE/EXCAVATE**
what already exists. It is exhaustive on purpose. Read §2 for the TL;DR, §4–§6 for what the data
actually is and how much to trust it, §7–§8 for where it lives and how the two stores reconcile,
and §9–§11 for the open decisions and the recommended path.

### How to re-verify (access recipes)

**Postgres (raw sink):**
```sh
doppler run -p core-x -c prd -- sh -c 'psql "$HQX_DB_URL_POOLED" -v ON_ERROR_STOP=1 -P pager=off -c "<SQL>"'
```
For multi-line SQL, write a `.sql` file and run `psql ... -f /path/to.sql` (avoids shell-quote
hell; this is how the §12 reliability queries were run).

**Lance (companies SoR):** write a python script and run via
`doppler run -p core-x -c prd -- python3 /tmp/x.py`. `lance`, `duckdb`, `psycopg` are installed.
```python
import os, lance
def so():
    ep = os.environ.get("R2_ENDPOINT"); aid = os.environ.get("R2_ACCOUNT_ID")
    if not ep and aid: ep = f"https://{aid}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}
ds = lance.dataset("s3://data-sink/active/companies/", storage_options=so())
```

**READ-ONLY DISCIPLINE.** Everything in this doc was produced with `SELECT` / Lance reads only. Do
not `INSERT/UPDATE/DELETE/TRUNCATE`, do not `lance.write_dataset` / `create_index` / `restore`.
The 4,556 capital-provider rows already on the spine are DONE — leave them.

---

## 2. Executive summary / TL;DR

1. **[VERIFIED FACT]** One discriminated raw sink, `gtm.existing_claygent_payloads`, holds
   **23,901 rows** across **15** `enrichment_payload_type` values spanning **10,888** distinct
   normalized domains. It is verbatim JSONB, append-only, idempotent. The entire table landed in a
   single ~13-minute window on **2026-06-24 22:36–22:49 UTC**.
2. **[VERIFIED FACT]** The "is this a capital provider?" signal lives in
   `capital-provider-json-1` (10,710 rows). Its `providesCapital` boolean splits
   **true = 7,344 distinct domains** / **false = 3,220** / null = 24.
3. **[VERIFIED FACT]** The lender taxonomy was attempted **three different ways with three
   different vocabularies** — `capitalType` (12 values, ~10,686 records), `classification` (8
   values, 535 records), `financingMode` (~7 values, ~3,015 records). They are NOT one consistent
   pass, and `financingMode` is even spelled inconsistently across types
   (`direct-lender` vs `directLender`, `broker` vs `broker/marketplace`).
4. **[VERIFIED FACT]** `providesCapital=false` is **partially unreliable** (false negatives):
   of 3,220 false domains, **1,455 are `confidence='low'`**, **457 are fetch-failures**
   ("could not / unable to access / returned no content"), **918 (28.5%) carry a lender token in
   the domain name** (capital|financ|fund|lend|loan|credit|leasing|bank|mortgag|factor), and **11
   self-contradict** (`capitalType ∈ {equipmentFinancing,factoring,nonBankLender}` yet
   providesCapital=false). The inverse contradiction also exists: 140 rows are
   `capitalType=notCapitalProvider` yet `providesCapital=true`.
5. **[VERIFIED FACT]** `equipment-provider-status-1` **over-fires on lenders** — e.g.
   `activebusinessloans.com` and `allcapfund.com` are flagged `equipmentProvider=true`. Treat
   `mode`/`equipmentProvider` as noisy.
6. **[VERIFIED FACT]** The **`providesCapital=true` set is clean**: 7,344 distinct domains, **0**
   fail a basic domain regex. Across the *whole* table only **6** distinct `domain_norm` values are
   malformed, all in `equipment-finance-classification-one` (100 of them are literally the string
   `"skipped"`).
7. **[VERIFIED FACT]** The Lance companies SoR `s3://data-sink/active/companies/` is at
   **version 88, 25,226 rows, 21 columns**, keyed by `normalized_domain` (0 nulls, 25,226 distinct
   — fully domain-unique today). BTree indexes on `normalized_domain` and `company_id`.
8. **[VERIFIED FACT — already shipped this session]** The **4,556** `providesCapital=true` domains
   that were missing from the spine were appended, tagged
   `source_platform='enrichment:capital-provider-json-1'`, all firmographic columns null. They are
   a clean subset of the true set (0 outside it). After the add, **all 7,344 true domains are in
   the spine** (2,788 were already there + 4,556 added). **Reversible** via `restore(87)` —
   version 87 = 20,670 rows, version 88 = 25,226 (delta exactly 4,556).
9. **[OPERATOR INTENT]** The Lance "is capital provider" representation should accept **only**
   `providesCapital=true`; the **false** set is to be **re-assessed by a fresh LLM run** given the
   unreliability in bullet 4 (the actionable false population not yet on the spine is **2,765**
   domains). The operator also treats the curated `elfa`/`sfnet`/`exa`/`exa-all` cohorts already on
   the spine as capital providers.
10. **[OPEN DECISION]** Whether/how to materialize the three taxonomies into Lance — one table per
    taxonomy, one wide signals table, or keep them in the Postgres jsonb sink and project on
    demand. See §10–§11. **[RECOMMENDATION]:** Option C (project-on-demand from Postgres) for the
    three messy taxonomies; promote `provides_capital` only via the boolean already actioned.

---

## 3. The raw landing table `gtm.existing_claygent_payloads` (Postgres)

**[VERIFIED FACT]** A single discriminated verbatim-JSONB sink. Source DDL:
`apps/edge_api/sql/existing_claygent_payloads.sql`. Landing endpoint:
`apps/edge_api/src/routers/existing_claygent_payloads_v1.py`.

### Schema

| column | type | semantics |
|---|---|---|
| `record_id` | `text` PRIMARY KEY | `sha256(enrichment_payload_type \| domain_norm \| sha256(canonical_json(raw_payload)))` |
| `enrichment_payload_type` | `text NOT NULL` | the discriminator — filter on this at read time |
| `domain` | `text NOT NULL` | verbatim, exactly as sent |
| `domain_norm` | `text NOT NULL` | normalized dedup key: lower/trim → strip `https?://` → strip leading `www.` → strip `/.*$` path → strip trailing dots |
| `raw_payload` | `jsonb NOT NULL` | stored **EXACTLY** as sent — any shape, no projection, no typed columns, no coercion |
| `landed_at` | `timestamptz NOT NULL DEFAULT now()` | landing time |

PK btree only. **No secondary indexes by design** — this is a deferred-processing sink, not a
resolution SoR.

### Endpoint & semantics

- **`POST /api/v1/existing-claygent-payloads/land`** — service-token gated (`require_service_token`).
- Wire body: `{ "domain": "...", "enrichment_payload_type": "...", "raw_payload": { ... } }`.
  `company_domain` is accepted as an alias for `domain`.
- `raw_payload` must be a JSON object (422 otherwise). `enrichment_payload_type` and a
  normalizable `domain` are required (422 otherwise).
- **Append-only, idempotent.** Insert is `ON CONFLICT (record_id) DO NOTHING`. A **byte-identical**
  resend is a no-op (`landed:false, already_present:true`). A different payload — or a different
  type — for the same domain lands as a **DISTINCT row** (append-only history). Grain = one row per
  `(enrichment_payload_type × domain_norm × canonical-raw_payload)`.
- Why one table not 12: Claygent emits divergent shapes per enrichment type; jsonb enforces no
  schema, so every shape lands losslessly (including the sources-as-string-vs-array divergence) and
  is sorted out at read time. Adding enrichment type #16 needs zero DDL.

**[VERIFIED FACT]** Totals (query A1): **23,901 rows · 15 distinct types · 10,888 distinct
`domain_norm`**. Landed window (query A2): `2026-06-24 22:36:27Z` → `2026-06-24 22:49:49Z`.

---

## 4. Enrichment-type inventory

**[VERIFIED FACT]** Master table (query B1 = rows/domains; query B2 = key-sets). "Primary verdict
field(s)" is the load-bearing classification field(s) for that type.

| # | enrichment_payload_type | rows | distinct domains | primary verdict field(s) | dominant key-set (shape) |
|---|---|---:|---:|---|---|
| 1 | `capital-provider-json-1` | 10,710 | 10,580 | `providesCapital` (bool), `capitalType` (enum) | `capitalType, confidence, evidencePhrases, forcedToFinishEarlyBecauseOfCost, providesCapital, reasoning, sourceUrls, stepsTaken, timeTakenInSeconds, totalCostToAIProvider, totalInputTokens, totalOutputTokens` |
| 2 | `equipment-financing-status-2` | 1,938 | 1,923 | `providesEquipmentFinancing` (bool), `financingMode` (enum) | `confidence, evidenceSummary, financingMode, forcedToFinishEarlyBecauseOfCost, providesEquipmentFinancing, reasoning, sources, stepsTaken, …tokens` |
| 3 | `equipment-manufacturer-status-1` | 1,938 | 1,923 | `isEquipmentManufacturer` (bool) | `confidence, evidence, forcedToFinishEarlyBecauseOfCost, isEquipmentManufacturer, rationale, reasoning, stepsTaken, …tokens` |
| 4 | `equipment-seller-status-1` | 1,938 | 1,923 | `isEquipmentSeller` (true/false/unknown) | `confidence, evidence, forcedToFinishEarlyBecauseOfCost, isEquipmentSeller, reason, reasoning, stepsTaken, …tokens` |
| 5 | `equipment-provider-status-1` | 1,938 | 1,923 | `equipmentProvider` (bool) + `mode` (sell/rent/both/none) | `confidence, equipmentProvider, evidenceSnippet, evidenceUrl, forcedToFinishEarlyBecauseOfCost, mode, reasoning, stepsTaken, …tokens` |
| 6 | `equipment-financing-evidence-2` | 1,130 | 1,125 | `providesEquipmentFinancing` (bool), `financingMode` (enum) | `confidence, coreToBusiness, evidence, evidenceSummary, financingMode, providesEquipmentFinancing, reasoning, stepsTaken` |
| 7 | `company-classification-1` | 971 | 966 | `bucket` (A/B/C/unknown) + `bucketLabel` | `bucket, bucketLabel, confidence, evidence, observedBrands, reason, reasoning, stepsTaken` |
| 8 | `equipment-finance-classification-one` | 535 | 390 | `classification` (8-value enum) | `classification, confidence, evidence, [notes], reasoning, stepsTaken` |
| 9 | `phone-hours-one` | 521 | 446 | `phoneNumber`, `hoursOfOperation` (both optional) | `confidence, [phoneNumber], [hoursOfOperation], reasoning, stepsTaken` |
| 10 | `equipment-finance-one` | 521 | 446 | `providesEquipmentFinancing` (bool), `financingMode` (enum) | `confidence, evidenceSummary, financingMode, providesEquipmentFinancing, reasoning, sources, stepsTaken` |
| 11 | `industries-served-one` | 520 | 446 | `industriesServed` (array) | `confidence, industriesServed, reasoning, sources, stepsTaken` |
| 12 | `equipment-finance-status-two` | 399 | 362 | `isIndependentEquipmentFinancingProvider` (bool) + `oemOrSellerFlags` | `confidence, evidence, isIndependentEquipmentFinancingProvider, oemOrSellerFlags, reason, reasoning, sourceUrls, stepsTaken` |
| 13 | `equipment-financing-status-1` | 398 | 395 | `isIndependentEquipmentFinancingProvider` (bool) + `oemOrSellerFlags` | `confidence, evidence, isIndependentEquipmentFinancingProvider, oemOrSellerFlags, reason, reasoning, sourceUrls, stepsTaken` |
| 14 | `equipment-financing-evidence-3` | 222 | 220 | `providesEquipmentFinancing` (bool), `financingMode` (enum) | `confidence, coreToBusiness, evidence, evidenceSummary, financingMode, providesEquipmentFinancing, reasoning, stepsTaken` |
| 15 | `construction-equipment-financing-1` | 222 | 220 | `conclusion` (yes/no/unclear) | `conclusion, confidence, evidence, rationale, reasoning, stepsTaken` |

### Per-type verdict distributions (the un-inspected & noteworthy ones)

**[VERIFIED FACT]** `company-classification-1.bucket` (query B3): `unknown` 434 / `C` 390 /
`B` 83 / `A` 64. (Bucket A/B/C is a fit-grade ladder; ~45% land in `unknown`.)

**[VERIFIED FACT]** `construction-equipment-financing-1.conclusion` (query B3):
`unclear` 121 / `yes` 85 / `no` 16. (Mostly unclear — low signal.)

**[VERIFIED FACT]** `industries-served-one.industriesServed` (query B5): present as a JSON **array**
in all 520 rows. `phone-hours-one.phoneNumber` (query B5): present in 398 rows, absent in 123.

**[VERIFIED FACT]** Equipment-status boolean verdicts (query B4):

| type | verdict value | rows |
|---|---|---:|
| `equipment-manufacturer-status-1` | isEquipmentManufacturer=false | 1,768 |
| | isEquipmentManufacturer=true | 170 |
| `equipment-seller-status-1` | isEquipmentSeller=unknown | 924 |
| | isEquipmentSeller=true | 702 |
| | isEquipmentSeller=false | 302 |
| | (no-answer envelope, no field) | 10 |
| `equipment-provider-status-1` | provider=false, mode=none | 1,017 |
| | provider=true, mode=both | 371 |
| | provider=true, mode=sell | 238 |
| | provider=true, mode=rent | 188 |
| | provider=false, mode=both | 57 |
| | provider=false, mode=rent | 51 |
| | provider=true, mode=none | 8 |
| | provider=false, mode=sell | 8 |
| `equipment-finance-status-two` | isIndependent=true | 297 |
| | isIndependent=false | 102 |
| `equipment-financing-status-1` | isIndependent=true | 254 |
| | isIndependent=false | 144 |

Note `equipment-provider-status-1` flags `mode=rent` even when `equipmentProvider=false` (51+8
rows) — incoherent, reinforcing that this type is noisy (see §6).

---

## 5. The lender taxonomies — THREE vocabularies, not one

**[VERIFIED FACT]** The lender/financier classification was attempted three separate ways with
three separate vocabularies. They do not share a value set and were not reconciled. Treat them as
three independent, partially-overlapping opinions.

### 5.1 `capitalType` — in `capital-provider-json-1` (10,686 records carry it; 24 lack it)

Query C1:

| capitalType | rows | distinct domains |
|---|---:|---:|
| `nonBankLender` | 3,653 | 3,612 |
| `notCapitalProvider` | 2,489 | 2,480 |
| `equipmentFinancing` | 1,273 | 1,249 |
| `hardMoney/bridge` | 782 | 775 |
| `advisoryOnly` | 563 | 561 |
| `brokerOrMarketplace` | 504 | 504 |
| `factoring` | 395 | 387 |
| `assetBasedLender` | 375 | 366 |
| `bank` | 297 | 297 |
| `privateCredit` | 289 | 286 |
| `mezzanine` | 34 | 34 |
| `ventureDebt` | 32 | 31 |
| *(null — no-answer envelope)* | 24 | 24 |

### 5.2 `classification` — in `equipment-finance-classification-one` (535 records)

Query C2. **Note: this type contains the malformed `domain_norm` junk** (100 `"skipped"` rows etc.,
see §6/§8), so its distinct-domain counts are deflated.

| classification | rows | distinct domains |
|---|---:|---:|
| `independentFinancer` | 231 | 176 |
| `bankOrCreditUnion` | 125 | 95 |
| `captiveOemFinancingArm` | 62 | 30 |
| `generalLenderWithEquipmentProduct` | 36 | 33 |
| `brokerMarketplace` | 36 | 31 |
| `noEquipmentFinancing` | 21 | 21 |
| `other` | 20 | 13 |
| `equipmentSellerWithThirdPartyFinancing` | 4 | 4 |

### 5.3 `financingMode` — in 4 types, SPELLED INCONSISTENTLY

**[VERIFIED FACT]** Query C3. The same conceptual values are spelled differently depending on the
type: `equipment-finance-one` and `equipment-financing-status-2` use **hyphenated**
(`direct-lender`, `through-partner`); `equipment-financing-evidence-2/3` use **camelCase**
(`directLender`) and **slashed** (`broker/marketplace`, `partner/referral`). Any consumer MUST
normalize these before grouping.

| type | financingMode | rows | distinct domains |
|---|---|---:|---:|
| `equipment-finance-one` | `direct-lender` | 319 | 296 |
| | `unclear` | 132 | 83 |
| | `broker` | 57 | 57 |
| | `through-partner` | 13 | 13 |
| `equipment-financing-evidence-2` | `directLender` | 398 | 398 |
| | `unclear` | 335 | 334 |
| | `partner/referral` | 210 | 210 |
| | `broker/marketplace` | 187 | 184 |
| `equipment-financing-evidence-3` | `directLender` | 139 | 139 |
| | `unclear` | 49 | 49 |
| | `broker/marketplace` | 26 | 25 |
| | `partner/referral` | 8 | 8 |
| `equipment-financing-status-2` | `direct-lender` | 797 | 790 |
| | `unclear` | 724 | 721 |
| | `through-partner` | 385 | 384 |
| | `broker` | 31 | 31 |
| | `multi-lender` | 1 | 1 |

**Distinct spellings of the same concept observed:** direct → {`direct-lender`, `directLender`};
broker → {`broker`, `broker/marketplace`, `brokerOrMarketplace` (capitalType), `brokerMarketplace`
(classification)}; partner → {`through-partner`, `partner/referral`}; plus `multi-lender`,
`unclear`.

### 5.4 `providesCapital` (boolean) — in `capital-provider-json-1`

**[VERIFIED FACT]** Query C4:

| providesCapital | json type | rows | distinct domains |
|---|---|---:|---:|
| `true` | boolean | 7,456 | **7,344** |
| `false` | boolean | 3,230 | **3,220** |
| *(null — no-answer envelope)* | — | 24 | 24 |

This boolean is the cleanest single capital-provider signal and is what was promoted to Lance (§7).

---

## 6. Reliability assessment (per-signal trust rating)

**[VERIFIED FACT]** unless tagged otherwise. The trust rating is **[RECOMMENDATION]**.

### 6.1 `sources` field — string-vs-array divergence across runs

Query D1 (`jsonb_typeof(raw_payload->'sources')` over rows carrying the key):

| type | sources type | rows |
|---|---|---:|
| `equipment-finance-one` | **string** (stringified JSON) | 521 |
| `equipment-financing-status-2` | **array** (real array) | 1,938 |
| `industries-served-one` | **array** (real array) | 520 |

→ **The same logical field is a stringified-JSON-array in one type and a real array in others.**
Any cross-type consumer must branch on `jsonb_typeof`. This is exactly the divergence the jsonb
sink was designed to absorb losslessly.

### 6.2 `providesCapital=false` is PARTIALLY UNRELIABLE (false negatives) — **trust: LOW**

Query D3, over the 3,220 distinct false domains:

| signal | distinct domains | share of 3,220 |
|---|---:|---:|
| fetch-failure reasoning ("could not/unable to access/no content/returned no/did not return") | **457** | 14.2% |
| `confidence='low'` | **1,455** | 45.2% |
| lender token in domain name (capital\|financ\|fund\|lend\|loan\|credit\|leasing\|bank\|mortgag\|factor) | **918** | **28.5%** |
| self-contradiction (`capitalType ∈ {equipmentFinancing,factoring,nonBankLender}` yet false) | **11** | 0.3% |

Confidence distribution within the false set (rows, query D3b): high 1,586 / **low 1,459** /
medium 96 / very high 89. Nearly half the false verdicts are low-confidence, and **>1 in 4 false
domains literally has a lender word in the URL** — strong evidence of false negatives.
**[RECOMMENDATION]:** do not trust `providesCapital=false` as ground truth; re-enrich it.

> Drift note: the prior estimate of ~486 fetch-failures is now **457**, and ~1,454 low-confidence
> is now **1,455** — within noise; the table is static (single load) so the deltas come only from
> the slightly different DISTINCT-domain framing of the refreshed query.

### 6.3 `equipment-provider-status-1` OVER-FIRES on lenders — **trust: LOW**

Query D4: among `equipmentProvider=true` rows whose domain contains a lender token, obvious lenders
are flagged as equipment providers, e.g. **`activebusinessloans.com` (mode=both)**,
**`allcapfund.com` (mode=rent)**, `2goodcapital.com`, `5starcapitalfunding.com`,
`acquirecommercialfinancing.com`, `agequipmentfinance.com`, `alphacapfunding.com`. The brief's two
named examples reproduce exactly. Treat `equipmentProvider`/`mode` as noisy.

### 6.4 "No-answer" fallback envelopes — quantified

**[VERIFIED FACT]** A generic shape `{result, confidence, stepsTaken, inputTokens, outputTokens,
totalTokensUsed, totalCostToAIProvider}` (no verdict field; carries a `result` key) is mixed into
two types. Query D2 (rows carrying a `result` key):

| type | no-answer envelope rows |
|---|---:|
| `capital-provider-json-1` | **24** |
| `equipment-seller-status-1` | **10** |

These are exactly the 24 null-`capitalType`/null-`providesCapital` rows in §5, and the 10
field-less `equipment-seller-status-1` rows in §4. No other type carries a `result` key. They are a
small, isolatable "model gave up" cohort — filter on the absence of the primary verdict field.

### 6.5 Within-type shape variance is benign optional-key jitter

**[VERIFIED FACT]** Query D5: `equipment-finance-classification-one` splits **283 / 252** solely by
presence of the optional `notes` key — both halves carry the full `classification` verdict. The
other multi-shape types differ only by optional `reasoning`/`reason`/`evidenceSnippet`/
`hoursOfOperation` keys or the no-answer envelope. **No cross-enrichment MISLABELING remains** in
this batch (an earlier batch reportedly had it; that batch was wiped and re-sent). The key-set
table (query B2, 31 rows) shows every shape per type — all variance is optional-key or the
no-answer envelope.

### 6.6 Trust summary

| signal | type(s) | trust | why |
|---|---|---|---|
| `providesCapital=true` | capital-provider-json-1 | **HIGH** | clean domains (0 malformed), corroborated by capitalType; already promoted |
| `providesCapital=false` | capital-provider-json-1 | **LOW** | 45% low-confidence, 28.5% lender-token domains, fetch-failures, contradictions |
| `capitalType` (true side) | capital-provider-json-1 | **MED-HIGH** | granular, mostly consistent with providesCapital |
| `classification` | equipment-finance-classification-one | **MED** (junk-contaminated) | 100 `"skipped"` + malformed domains inflate noise; real verdicts fine |
| `financingMode` | 4 types | **MED** | useful, but spelling must be normalized; lots of `unclear` |
| `isEquipmentManufacturer` / `isEquipmentSeller` | mfr/seller-status | **MED** | seller has 924 `unknown` (47%); manufacturer skews false |
| `equipmentProvider` / `mode` | equipment-provider-status-1 | **LOW** | over-fires on lenders; provider=false with mode=rent incoherence |
| `bucket` | company-classification-1 | **LOW-MED** | 45% `unknown` |
| `conclusion` | construction-equipment-financing-1 | **LOW** | 55% `unclear` |

---

## 7. The Lance system of record `s3://data-sink/active/companies/`

**[VERIFIED FACT]** Query E1/E2. This is the Gen-3 standalone SoR for the companies spine. The
dexarchive→Lance overwrite that originally built it is **RETIRED** — `ingest_gtm_company_people`
refuses every call (`pipelines/gtm/companies_people_bulk.py`); rows now arrive only via **direct
append** (manual seeds + exa.ai websets via `pipelines/exa_websets/ingest.py` + Waterfall ICP +,
now, the capital-provider promotion). Net-new datasets are Lance `data_storage_version=2.1`.

- **Version: 88. Rows: 25,226. Columns: 21.** Identity anchor `normalized_domain` (string,
  BTree-indexed) — **0 nulls, 25,226 distinct** (the spine is fully domain-unique today; the bulk
  worker's old "743→639 distinct" non-uniqueness note no longer holds for the current contents).
  PK `company_id` (uuid-as-string, BTree-indexed). Provenance `source_platform`.

### Full schema (all 21 fields — every field nullable=True)

| # | field | type |
|---:|---|---|
| 1 | `company_id` | string (PK, uuid text) |
| 2 | `company_name` | string |
| 3 | `normalized_domain` | string (anchor) |
| 4 | `company_linkedin_url` | string |
| 5 | `source_platform` | string (lineage) |
| 6 | `industry` | string |
| 7 | `employee_size_band` | string |
| 8 | `employees_on_linkedin` | int64 |
| 9 | `company_type` | string |
| 10 | `founded_year` | int32 |
| 11 | `followers` | int64 |
| 12 | `specialties` | list<string> |
| 13 | `hq_city` | string |
| 14 | `hq_state` | string |
| 15 | `hq_region` | string |
| 16 | `hq_continent` | string |
| 17 | `uei` | string |
| 18 | `firmo_linkedin_url` | string |
| 19 | `company_linkedin_source` | string |
| 20 | `firmo_match_key` | string |
| 21 | `firmo_materialized_at` | timestamp[us, tz=UTC] |

> Correction to the brief's "all 21 are string": fields 8/10/11 are integer, 12 is a string list,
> 21 is a timestamp. **All 21 are nullable.** The firmographic columns (6–21) are populated only
> for firmo-enriched cohorts; the capital-provider add left them null.

### Committed indexes (query E2)

| name | type | field(s) |
|---|---|---|
| `normalized_domain_idx` | BTree | normalized_domain |
| `company_id_idx` | BTree | company_id |
| `employee_size_band_idx` | Bitmap | employee_size_band |
| `industry_idx` | Bitmap | industry |
| `company_type_idx` | Bitmap | company_type |
| `hq_region_idx` | Bitmap | hq_region |

### source_platform distribution (all 25,226 rows — query E3)

| source_platform | rows |
|---|---:|
| `active_people_equiv` | 11,502 |
| **`enrichment:capital-provider-json-1`** | **4,556** |
| `enrichment:equipment_rental_candidates` | 4,420 |
| `epd_lec_status_candidates` | 3,636 |
| `equipment-finance-construction-candidates` | 289 |
| `exa-all` | 259 |
| `sfnet` | 165 |
| `find-all-equip-finance-parallel-ai` | 135 |
| `prospeo-parallel.ai` | 128 |
| `elfa` | 43 |
| `exa` | 40 |
| `session-tgrp` | 25 |
| `manual-surety-bonds` | 10 |
| `equipment-oem-candidates` | 10 |
| `sfnet-manual-resolve` | 4 |
| `csv_2026_05_23` | 3 |
| `manual-seed` | 1 |

### What was added this session (VERIFIED FACT)

- **4,556 rows** tagged `source_platform='enrichment:capital-provider-json-1'`, `company_id` =
  uuid5(domain), all firmographic columns null — the `providesCapital=true` domains that were
  **missing** from the spine.
- Query E3 confirms **exactly 4,556** rows carry that tag, and reconciliation (query F1) confirms
  those 4,556 are a **clean subset of the 7,344 true-set domains** (0 outside it).
- **Reversible:** version 87 = **20,670** rows, version 88 = **25,226** (query E4). `restore(87)`
  cleanly reverts the add (delta = 4,556). *(Do not run it — the add is intended to stay.)*

---

## 8. Cross-store reconciliation

**[VERIFIED FACT]** Query F1 (Lance spine domains ∩ Postgres capital-provider verdicts):

| population | distinct domains | in spine | not in spine |
|---|---:|---:|---:|
| `providesCapital=true` | 7,344 | **7,344** | **0** |
| `providesCapital=false` | 3,220 | 455 | **2,765** |

- **The true set is fully landed.** 7,344 true domains = 2,788 that pre-existed on the spine +
  4,556 appended this session. After the add, every true domain resolves on `normalized_domain`.
- **The false set is the re-enrich target.** 2,765 false domains are absent from the spine (the
  455 present arrived via other curated cohorts, not via the false verdict). This 2,765 is the
  actionable population for §11's "re-enrich" path. *(The brief's "~3,212 false" figure was the row
  count framing; the distinct-domain figure is 3,220, of which 2,765 are spine-absent.)*

### Curated / association cohorts the operator treats as capital providers

**[VERIFIED FACT]** Query E3/F1 — already on the spine, curated/association origin:

| cohort | rows | what it is |
|---|---:|---|
| `exa-all` | 259 | Exa.ai company discovery (broad webset harvest), curated into the spine |
| `sfnet` | 165 | Secured Finance Network member directory (asset-based lenders / factors) |
| `elfa` | 43 | Equipment Leasing & Finance Association member directory |
| `exa` | 40 | earlier Exa discovery cohort (predecessor label to `exa-all`) |

**[OPERATOR INTENT]** These 507 rows are considered capital providers by curation/association
membership, independent of any Claygent verdict.

### Junk / malformed domains

**[VERIFIED FACT]** Query F2/F3. Across the whole raw table, only **6** distinct `domain_norm`
values fail the regex `^(?=.{1,253}$)([a-z0-9](-?[a-z0-9])*\.)+[a-z]{2,}$`, **all in
`equipment-finance-classification-one`**:

| domain_norm | rows |
|---|---:|
| `skipped` | 100 |
| `company website unavailable` | 14 |
| `1) us business funding: https:` | 1 |
| `capital one equipment leasing & finance: https:` | 1 |
| `commercial capital company, llc: https:` | 1 |
| `cumberland capital: https:` | 1 |

**[VERIFIED FACT]** The `providesCapital=true` set is **clean: 7,344 distinct domains, 0 malformed**
(query F4). The capital-provider promotion therefore carried no junk onto the spine.

---

## 9. Operator intent & open decisions

**[OPERATOR INTENT]** (clearly separated from verified state):

1. **ONE discriminated landing table, not 12** — **DONE** (`gtm.existing_claygent_payloads`, §3).
2. **Lance "is capital provider" accepts ONLY `providesCapital=true`** (the 7,344) — **NOT** the
   false set. The 4,556 missing-true domains are already appended (§7). The false set is to be
   re-assessed separately.
3. The curated `elfa` / `sfnet` / `exa` / `exa-all` cohorts already on the spine **are** capital
   providers (curated/association origin) — §8.
4. The **2,765** spine-absent `providesCapital=false` domains: **RE-RUN an LLM enrichment** to
   re-assess, given the §6.2 unreliability (45% low-confidence, 28.5% lender-token domains).
5. **OPEN:** materialize the three taxonomies (`capitalType` / `classification` / `financingMode`)
   into Lance — as three tables, or one wide table, or project-on-demand? Operator is unsure how
   Lance handles this — see §10.

---

## 10. Lance materialization options (how Lance works here + the choices)

**[RECOMMENDATION/EXPLANATION]** How Lance works for this use case, plainly:

- **Schema-on-write columnar dataset, addressed by R2 URI.** A Lance dataset has a **fixed column
  set** declared at write time. Unlike the Postgres jsonb sink, it does **not** absorb arbitrary
  per-row shapes — heterogeneous JSON must be flattened into named columns first.
- **Append vs merge_insert.** `mode="append"` adds rows. `merge_insert(on="normalized_domain")`
  performs an **upsert** keyed by a column — match → update, no-match → insert. This is how you
  attach a new signal column's values to existing spine rows by domain.
- **Scalar index.** A `BTREE` index on `normalized_domain` makes point lookups index-pushdown, not
  a scan. (The spine already has `normalized_domain_idx`.)
- **Versioned = reversible.** Every write creates a new version (the spine is at v88); any version
  is restorable. Writes are cheap to undo, which de-risks materialization experiments.
- **Heterogeneous arbitrary JSON belongs in the Postgres jsonb sink.** Lance wants a fixed column
  set, so a wide table **must enumerate every signal column up front**; sparse signals are simply
  nullable columns.

### Option A — one Lance table per taxonomy

`active/capital_provider_type`, `active/equipment_finance_classification`,
`active/equipment_financing_mode` — each keyed by `normalized_domain`, each a thin
`(normalized_domain, <verdict>, confidence, source_payload_type)` grain.

- **Pros:** clean per-taxonomy schema; each table's value set is internally consistent; easy to
  drop/rebuild one taxonomy without touching others; mirrors the existing per-grain dataset
  convention (`companies`, `people`, `company_target_industries`, `discovered_websets`).
- **Cons:** three datasets to join at read time; the three vocabularies still don't reconcile to
  each other (a domain can be `nonBankLender` here, `independentFinancer` there, `directLender`
  elsewhere); `financingMode` spelling still needs normalization on write.

### Option B — one wide `enrichment_signals` table keyed by `normalized_domain`

Fixed columns: `(normalized_domain, provides_capital, capital_type, ef_classification,
financing_mode, is_equipment_manufacturer, is_equipment_seller, equipment_provider, …,
confidence_*, source_*)`, all sparse/nullable, upserted via `merge_insert`.

- **Pros:** single join for any consumer; one row per domain; natural home for *all* Claygent
  signals, not just the three taxonomies.
- **Cons:** must enumerate (and version-migrate) a column per signal up front; very sparse (most
  domains have only a subset of signals); bakes the messy/inconsistent vocabularies into the SoR
  schema; every new enrichment type is a schema change (the exact thing the jsonb sink avoided).

### Option C — keep raw in Postgres jsonb; materialize ONE flattened Lance projection on demand

The Postgres sink stays the SoR for raw. A single read-time projection (DuckDB over the jsonb)
flattens whatever subset of signals a consumer needs into an ephemeral or periodically-rebuilt
Lance table.

- **Pros:** zero schema lock-in; the messy three-vocabulary reconciliation happens in one
  versioned transform you can iterate freely; raw stays lossless and re-projectable; no premature
  commitment while the false set is still being re-enriched.
- **Cons:** the flattened view is derived, not a first-class SoR; consumers need the projection
  job; point-in-time only as fresh as the last projection.

---

## 11. The re-enrich vs coalesce decision

**[RECOMMENDATION]** Frame each path by what it gets you, given §6:

### Path 1 — COALESCE / EXCAVATE what exists

Use the data already landed; do not pay for new LLM calls.

- **What you get cleanly:** the **7,344 `providesCapital=true` capital providers** (already on the
  spine, clean, §7/§8) + the **507 curated** elfa/sfnet/exa/exa-all rows. That is a solid, trusted
  capital-provider universe **today** with no further work.
- **What you get with caveats:** `capitalType` (true side), `financingMode` (after spelling
  normalization), `classification`, manufacturer/seller flags — usable as **secondary attributes**
  on the already-true domains, not as primary truth.
- **What you must NOT trust:** `providesCapital=false` as a negative, `equipmentProvider`/`mode`,
  `bucket`/`conclusion` (high `unknown`/`unclear`). Coalescing these in would inject false
  negatives and over-fires.
- **Best for:** standing up the capital-provider spine **now** (done) and bolting on secondary
  signals for the true set via a projection.

### Path 2 — RE-LLM-ENRICH

Re-run enrichment, scoped tightly.

- **Highest-value scope:** the **2,765 spine-absent `providesCapital=false` domains** (§8) — these
  are where the false-negative risk concentrates (45% low-confidence, 28.5% lender-token domains).
  Re-enriching just these is the cheapest way to recover missed capital providers. Optionally also
  re-score the **918** lender-token false domains regardless of spine membership.
- **What it gets you:** a trustworthy negative set + recovered true positives, closing the one real
  gap in Path 1.
- **What it does NOT need to touch:** the 7,344 true domains (already trusted) — re-enriching them
  is wasted spend.
- **Vocabulary fix:** if re-enriching, emit **one** consistent taxonomy and **one** consistent
  `financingMode` spelling so the three-way split in §5 never recurs.

### Recommended synthesis **[RECOMMENDATION]**

1. **Keep the done work:** 7,344 true + 507 curated = the capital-provider spine. No action.
2. **Re-enrich only the 2,765 spine-absent false domains** (Path 2, tight scope) — that is the only
   place the existing data is demonstrably unreliable. Emit a single canonical taxonomy.
3. **For the three taxonomies, choose Option C** (project-on-demand from the Postgres jsonb).
   Reasons grounded in the data: (a) the three vocabularies are inconsistent and unreconciled (§5),
   so baking them into a wide SoR schema (Option B) freezes the mess; (b) `financingMode` needs
   normalization that is better expressed once in a transform than re-applied by every consumer;
   (c) the raw sink already is the lossless SoR and is cheap to re-project as the re-enrich in step
   2 lands new data. If a first-class materialized signal is later required, promote **only**
   `provides_capital` (already actioned as a spine membership) and, if needed, a single normalized
   `capital_type` column via `merge_insert` on `normalized_domain` (a thin slice of Option A) —
   leave `classification`/`financingMode` in the projection until they've been unified by a clean
   re-enrich.

---

## 12. Appendix — every verification query (copy-pasteable, verified 2026-06-24)

All Postgres queries run via:
```sh
doppler run -p core-x -c prd -- sh -c 'psql "$HQX_DB_URL_POOLED" -v ON_ERROR_STOP=1 -P pager=off -c "<SQL>"'
```
Lance queries run via `doppler run -p core-x -c prd -- python3 /tmp/x.py` with the `so()` helper from §1.

### A — landing table totals

**A1** (totals): result 23,901 / 15 / 10,888.
```sql
SELECT count(*) AS total_rows,
       count(DISTINCT enrichment_payload_type) AS distinct_types,
       count(DISTINCT domain_norm) AS distinct_domains
FROM gtm.existing_claygent_payloads;
```
**A2** (landed window): 2026-06-24 22:36:27Z → 22:49:49Z.
```sql
SELECT min(landed_at) AS first_landed, max(landed_at) AS last_landed
FROM gtm.existing_claygent_payloads;
```

### B — type inventory + shapes + verdict distributions

**B1** (per-type rows + domains):
```sql
SELECT enrichment_payload_type, count(*) AS rows, count(DISTINCT domain_norm) AS distinct_domains
FROM gtm.existing_claygent_payloads GROUP BY 1 ORDER BY 2 DESC;
```
**B2** (key-set / shape per type — 31 rows):
```sql
WITH ks AS (
  SELECT enrichment_payload_type,
         (SELECT string_agg(k, ',' ORDER BY k) FROM jsonb_object_keys(raw_payload) k) AS keyset
  FROM gtm.existing_claygent_payloads
)
SELECT enrichment_payload_type, keyset, count(*) AS rows
FROM ks GROUP BY 1,2 ORDER BY 1, 3 DESC;
```
**B3** (bucket + conclusion for the un-inspected types):
```sql
SELECT 'company-classification-1.bucket' AS field, raw_payload->>'bucket' AS value,
       count(*) AS rows, count(DISTINCT domain_norm) AS doms
FROM gtm.existing_claygent_payloads WHERE enrichment_payload_type='company-classification-1' GROUP BY 2
UNION ALL
SELECT 'construction-equipment-financing-1.conclusion', raw_payload->>'conclusion',
       count(*), count(DISTINCT domain_norm)
FROM gtm.existing_claygent_payloads WHERE enrichment_payload_type='construction-equipment-financing-1' GROUP BY 2
ORDER BY 1, 3 DESC;
```
**B4** (equipment-status boolean verdicts):
```sql
SELECT enrichment_payload_type AS t,
  raw_payload->>'isEquipmentManufacturer' AS is_mfr,
  raw_payload->>'isEquipmentSeller' AS is_seller,
  raw_payload->>'equipmentProvider' AS is_provider,
  raw_payload->>'mode' AS mode,
  raw_payload->>'isIndependentEquipmentFinancingProvider' AS is_indep,
  count(*) AS rows
FROM gtm.existing_claygent_payloads
WHERE enrichment_payload_type IN ('equipment-manufacturer-status-1','equipment-seller-status-1',
  'equipment-provider-status-1','equipment-finance-status-two','equipment-financing-status-1')
GROUP BY 1,2,3,4,5,6 ORDER BY 1, 7 DESC;
```
**B5** (industries-served + phone-hours primary fields):
```sql
SELECT 'industries-served-one' AS t, jsonb_typeof(raw_payload->'industriesServed') AS typ, count(*)
FROM gtm.existing_claygent_payloads WHERE enrichment_payload_type='industries-served-one' GROUP BY 2
UNION ALL
SELECT 'phone-hours-one', (raw_payload ? 'phoneNumber')::text, count(*)
FROM gtm.existing_claygent_payloads WHERE enrichment_payload_type='phone-hours-one' GROUP BY 2
ORDER BY 1;
```

### C — the three taxonomies

**C1** (`capitalType`):
```sql
SELECT raw_payload->>'capitalType' AS capital_type, count(*) AS rows,
       count(DISTINCT domain_norm) AS distinct_domains
FROM gtm.existing_claygent_payloads WHERE enrichment_payload_type='capital-provider-json-1'
GROUP BY 1 ORDER BY 2 DESC;
```
**C2** (`classification`):
```sql
SELECT raw_payload->>'classification' AS classification, count(*) AS rows,
       count(DISTINCT domain_norm) AS distinct_domains
FROM gtm.existing_claygent_payloads WHERE enrichment_payload_type='equipment-finance-classification-one'
GROUP BY 1 ORDER BY 2 DESC;
```
**C3** (`financingMode` across 4 types — note inconsistent spelling):
```sql
SELECT enrichment_payload_type, raw_payload->>'financingMode' AS financing_mode,
       count(*) AS rows, count(DISTINCT domain_norm) AS distinct_domains
FROM gtm.existing_claygent_payloads
WHERE enrichment_payload_type IN ('equipment-finance-one','equipment-financing-evidence-2',
  'equipment-financing-evidence-3','equipment-financing-status-2')
GROUP BY 1,2 ORDER BY 1, 3 DESC;
```
**C4** (`providesCapital` + json type):
```sql
SELECT raw_payload->>'providesCapital' AS provides_capital,
       jsonb_typeof(raw_payload->'providesCapital') AS json_type,
       count(*) AS rows, count(DISTINCT domain_norm) AS distinct_domains
FROM gtm.existing_claygent_payloads WHERE enrichment_payload_type='capital-provider-json-1'
GROUP BY 1,2 ORDER BY 3 DESC;
```
**C5** (`capitalType` × `providesCapital` crosstab — consistency):
```sql
SELECT raw_payload->>'capitalType' AS capital_type, raw_payload->>'providesCapital' AS provides_capital,
       count(*) AS rows
FROM gtm.existing_claygent_payloads WHERE enrichment_payload_type='capital-provider-json-1'
GROUP BY 1,2 ORDER BY 1,2;
```

### D — reliability

**D1** (`sources` string-vs-array):
```sql
SELECT enrichment_payload_type, jsonb_typeof(raw_payload->'sources') AS sources_type, count(*) AS rows
FROM gtm.existing_claygent_payloads WHERE raw_payload ? 'sources'
GROUP BY 1,2 ORDER BY 1,3 DESC;
```
**D2** (no-answer fallback envelopes — `result` key present):
```sql
SELECT enrichment_payload_type, count(*) AS result_envelope_rows
FROM gtm.existing_claygent_payloads WHERE raw_payload ? 'result'
GROUP BY 1 ORDER BY 2 DESC;
```
**D3** (`providesCapital=false` reliability — run via `psql -f`):
```sql
WITH f AS (
  SELECT domain_norm, raw_payload
  FROM gtm.existing_claygent_payloads
  WHERE enrichment_payload_type='capital-provider-json-1' AND raw_payload->>'providesCapital'='false'
)
SELECT
  count(DISTINCT domain_norm) AS false_domains,
  count(DISTINCT domain_norm) FILTER (
    WHERE lower(coalesce(raw_payload->>'reasoning','')) ~ '(could not|unable to access|no content|returned no|did not return)'
  ) AS fetch_failures,
  count(DISTINCT domain_norm) FILTER (
    WHERE lower(coalesce(raw_payload->>'confidence','')) = 'low'
  ) AS low_confidence,
  count(DISTINCT domain_norm) FILTER (
    WHERE domain_norm ~ '(capital|financ|fund|lend|loan|credit|leasing|bank|mortgag|factor)'
  ) AS lender_token_domain,
  count(DISTINCT domain_norm) FILTER (
    WHERE raw_payload->>'capitalType' IN ('equipmentFinancing','factoring','nonBankLender')
  ) AS self_contradiction
FROM f;
-- result: 3220 | 457 | 1455 | 918 | 11
```
**D3b** (confidence distribution within the false set):
```sql
SELECT raw_payload->>'confidence' AS confidence, count(*) AS rows
FROM gtm.existing_claygent_payloads
WHERE enrichment_payload_type='capital-provider-json-1' AND raw_payload->>'providesCapital'='false'
GROUP BY 1 ORDER BY 2 DESC;
-- result: high 1586 / low 1459 / medium 96 / very high 89
```
**D4** (`equipment-provider-status-1` over-fires on lenders):
```sql
SELECT domain_norm, raw_payload->>'equipmentProvider' AS provider, raw_payload->>'mode' AS mode
FROM gtm.existing_claygent_payloads
WHERE enrichment_payload_type='equipment-provider-status-1'
  AND raw_payload->>'equipmentProvider'='true'
  AND domain_norm ~ '(loan|fund|capital|lend|financ|leasing)'
ORDER BY 1 LIMIT 20;
-- includes activebusinessloans.com (both), allcapfund.com (rent)
```
**D5** (within-type variance = optional `notes` key only):
```sql
SELECT (raw_payload ? 'notes') AS has_notes_key, count(*) AS rows
FROM gtm.existing_claygent_payloads WHERE enrichment_payload_type='equipment-finance-classification-one'
GROUP BY 1 ORDER BY 2 DESC;
-- result: true 283 / false 252
```

### E — Lance companies SoR (python; `so()` from §1)

**E1** (version + rows + schema):
```python
ds = lance.dataset("s3://data-sink/active/companies/", storage_options=so())
print(ds.version, ds.count_rows())                 # 88 25226
for f in ds.schema: print(f.name, f.type, f.nullable)   # 21 fields, all nullable
```
**E2** (indices):
```python
for ix in ds.list_indices(): print(ix)
# normalized_domain_idx (BTree), company_id_idx (BTree),
# employee_size_band_idx / industry_idx / company_type_idx / hq_region_idx (Bitmap)
```
**E3** (source_platform distribution + the added tag count):
```python
from collections import Counter
sp = ds.to_table(columns=["source_platform"]).column("source_platform").to_pylist()
for k,v in sorted(Counter(x or "<NULL>" for x in sp).items(), key=lambda kv:-kv[1]): print(v,k)
# enrichment:capital-provider-json-1 -> 4556
```
**E4** (reversibility — version 87 vs 88 row counts; READ-ONLY checkout):
```python
for v in (87, 88):
    print(v, lance.dataset("s3://data-sink/active/companies/", storage_options=so(), version=v).count_rows())
# 87 -> 20670 ; 88 -> 25226 ; delta 4556
```

### F — cross-store reconciliation (python; reads both stores)

**F1** (true/false vs spine — the core reconciliation):
```python
import psycopg
ds = lance.dataset("s3://data-sink/active/companies/", storage_options=so())
spine = {d for d in ds.to_table(columns=["normalized_domain"]).column("normalized_domain").to_pylist() if d}
with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as c, c.cursor() as cur:
    cur.execute("SELECT DISTINCT domain_norm FROM gtm.existing_claygent_payloads "
                "WHERE enrichment_payload_type='capital-provider-json-1' AND raw_payload->>'providesCapital'='true'")
    true_doms = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT DISTINCT domain_norm FROM gtm.existing_claygent_payloads "
                "WHERE enrichment_payload_type='capital-provider-json-1' AND raw_payload->>'providesCapital'='false'")
    false_doms = {r[0] for r in cur.fetchall()}
print(len(true_doms), len(true_doms & spine), len(true_doms - spine))    # 7344 7344 0
print(len(false_doms), len(false_doms & spine), len(false_doms - spine)) # 3220 455 2765
```
**F2** (count of malformed `domain_norm` across the whole table — run via `psql -f`):
```sql
WITH d AS (SELECT DISTINCT domain_norm FROM gtm.existing_claygent_payloads)
SELECT (SELECT count(*) FROM d) AS distinct_domain_norms,
       count(*) FILTER (WHERE domain_norm !~ '^(?=.{1,253}$)([a-z0-9](-?[a-z0-9])*\.)+[a-z]{2,}$') AS malformed
FROM d;
-- result: 10888 distinct / 6 malformed
```
**F3** (which types the malformed domains came from):
```sql
SELECT domain_norm, string_agg(DISTINCT enrichment_payload_type, ', ') AS types, count(*) AS rows
FROM gtm.existing_claygent_payloads
WHERE domain_norm !~ '^(?=.{1,253}$)([a-z0-9](-?[a-z0-9])*\.)+[a-z]{2,}$'
GROUP BY 1 ORDER BY 3 DESC;
-- all 6 are equipment-finance-classification-one ("skipped"=100, ...)
```
**F4** (the `providesCapital=true` set is clean — run via `psql -f`):
```sql
WITH t AS (
  SELECT DISTINCT domain_norm FROM gtm.existing_claygent_payloads
  WHERE enrichment_payload_type='capital-provider-json-1' AND raw_payload->>'providesCapital'='true'
)
SELECT count(*) AS true_distinct_domains,
       count(*) FILTER (WHERE domain_norm !~ '^(?=.{1,253}$)([a-z0-9](-?[a-z0-9])*\.)+[a-z]{2,}$') AS malformed
FROM t;
-- result: 7344 / 0
```

---

### Source files (for endpoint/schema/write-pattern)

- Landing endpoint: `apps/edge_api/src/routers/existing_claygent_payloads_v1.py`
- Landing DDL: `apps/edge_api/sql/existing_claygent_payloads.sql`
- Lance companies spine writer (RETIRED overwrite; direct-append now): `pipelines/gtm/companies_people_bulk.py`
- Exa webset → spine direct-append path: `pipelines/exa_websets/ingest.py`
