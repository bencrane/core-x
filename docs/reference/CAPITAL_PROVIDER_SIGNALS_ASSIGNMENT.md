# ASSIGNMENT — Materialize `active/capital_provider_signals` (native lender taxonomy → Lance)

**Status:** ready to execute · **Verified:** 2026-06-24 · **Owner of record:** operator (bjc)
**Type:** build spec / work order. A fresh agent should be able to execute this end-to-end with no further questions.

---

## 0. TL;DR (the mandate)

Build a single, lean Lance dataset at `s3://data-sink/active/capital_provider_signals/`, keyed by
`normalized_domain`, that coalesces the **useful** claygent enrichment payloads out of the Postgres
jsonb sink (`gtm.existing_claygent_payloads`) into a few clean, query-ready columns — preserving the
**native lender taxonomy** (`capitalType`) at full granularity so GTM outbound can filter on real
lending categories (`factoring`, `equipmentFinancing`, `assetBasedLender`, `privateCredit`, …).

- **Do NOT** collapse into coarse buckets. Factoring / A-R, equipment finance, and private credit are
  distinct businesses; the payload already encodes the distinction — keep it.
- **Do NOT** project from Postgres on the fly. Materialize natively into Lance (R2 is the SoR).
- The table makes it trivial to find which `active/companies` are **missing** a given signal, so the
  operator can target re-enrichment per claygent run.

Required reading before starting: [`docs/reference/CLAYGENT_ENRICHMENT_STATE.md`](./CLAYGENT_ENRICHMENT_STATE.md)
(the verified state of the payload sink and the `active/companies` SoR).

---

## 1. Stores & access (read-only except the one new Lance dataset)

| store | what | access |
|---|---|---|
| Postgres (control plane) | `gtm.existing_claygent_payloads` — raw jsonb sink, **source of truth for the signal** | `doppler run -p core-x -c prd -- sh -c 'psql "$HQX_DB_URL_POOLED" -c "…"'` |
| Lance on R2 (SoR) | `s3://data-sink/active/companies/` — join target · `s3://data-sink/active/capital_provider_signals/` — **the new dataset to create** | `lance.dataset(uri, storage_options=so)` under `doppler run -p core-x -c prd -- python3 …` |

R2 storage options (sourced from the `core-x/prd` Doppler config):
```python
def storage_options():
    ep = os.environ.get("R2_ENDPOINT"); aid = os.environ.get("R2_ACCOUNT_ID")
    if not ep and aid: ep = f"https://{aid}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}
```
`lance`, `duckdb`, `psycopg` are installed in the `core-x/prd` runtime. Network egress required.

**Deliverable code:** `pipelines/gtm/materialize_capital_provider_signals.py` (clean-room pattern —
DuckDB/psycopg does the transform locally, Lance is written straight to R2; mirror
`pipelines/gtm/companies_people_bulk.py` and `pipelines/exa_websets/ingest.py` for the write idiom).

---

## 2. Verified input state (2026-06-24 — re-verify before writing; numbers may drift)

`gtm.existing_claygent_payloads`: ~23,901 rows · 15 `enrichment_payload_type` values · ~10,888 distinct `domain_norm`.

`capital-provider-json-1`: 10,710 rows · 10,580 domains. `providesCapital` → **true 7,344** · false 3,220 · null/no-answer 24.

**`capitalType` over the `providesCapital='true'` set (the verified-lender taxonomy — THE asset):**

| capital_type | lenders | notes |
|---|---|---|
| nonBankLender | 3,611 | generic catch-all — coarse, refine later |
| equipmentFinancing | 1,243 | clean segment |
| hardMoney/bridge | 775 | clean segment (slash in the value — keep verbatim) |
| factoring | 383 | clean segment — **≠ equipment finance** |
| assetBasedLender | 366 | clean segment |
| bank | 297 | depository |
| privateCredit | 286 | clean segment |
| notCapitalProvider | 140 | ⚠ contradicts providesCapital=true → `is_intermediary`/exclude |
| brokerOrMarketplace | 101 | ⚠ intermediary, not a direct lender |
| advisoryOnly | 96 | ⚠ not a lender |
| mezzanine | 34 | clean segment |
| ventureDebt | 31 | clean segment |

**`financingMode` (raw values across the financing enrichments — note the spelling split):**
`direct-lender` 1,016 · `unclear` 925 · `directLender` 536 · `through-partner` 397 · `partner/referral` 218 · `broker/marketplace` 209 · `broker` 87 · `multi-lender` 1.

**`ef_classification`** (`equipment-finance-classification-one`, domain-framed; **100 rows carry junk `domain_norm="skipped"` — drop them**):
independentFinancer 176 · bankOrCreditUnion 95 · generalLenderWithEquipmentProduct 33 · brokerMarketplace 31 · captiveOemFinancingArm 30 · noEquipmentFinancing 21 · other 13 · equipmentSellerWithThirdPartyFinancing 4.

`active/companies`: 25,226 rows · Lance version 88 · `normalized_domain` unique + BTREE-indexed · 21 cols all nullable. Curated lender origins present: `elfa` 43 · `sfnet` 165 · `exa` 40 · `exa-all` 259. (The 4,556 capital providers already appended this cycle carry `source_platform='enrichment:capital-provider-json-1'`.)

---

## 3. Scope — EXTRACT vs IGNORE

### EXTRACT (these `enrichment_payload_type` values feed the table)
1. **`capital-provider-json-1`** → `capitalType` (native, verbatim), `providesCapital`, `confidence`, `reasoning`(prov only). **The spine.**
2. **Financing cluster** → coalesced into one normalized `financing_mode` + `provides_equipment_financing`:
   `equipment-financing-status-2`, `equipment-financing-evidence-2`, `equipment-financing-evidence-3`,
   `equipment-finance-one`, `equipment-finance-status-two`, `equipment-financing-status-1`.
3. **`equipment-finance-classification-one`** → `ef_classification` (refiner; drop `domain_norm='skipped'`).
4. **`construction-equipment-financing-1`** → `construction_equip_financing` — **its own vertical signal.**
   Verdict field is `conclusion` ∈ {yes, no, unclear}; it has NO `providesEquipmentFinancing`/`financingMode`
   and is NOT part of the financing cluster. Identifies construction-equipment financiers specifically
   (e.g. `coastalkapital.com`). 220 domains: yes 83 · unclear 121 · no 16 (verified 2026-06-24).
5. **`independent-equipment-financing-1`** → `independent_equip_financing` — **high-precision independent-
   financier signal.** Verdict field is `isIndependentEquipmentFinancingProvider` (bool); the payload also
   carries `oemOrSellerFlags {isOem, sellsOrRentsEquipment}` so OEMs/sellers are explicitly cleared (unlike
   the noisy `equipment-provider-status-1`). 45 domains: true 41 · false 4 (verified 2026-06-25).

### IGNORE entirely (not lender-type, or unreliable)
- `equipment-provider-status-1` — noisy; `equipmentProvider`/`mode` over-fires on real lenders.
- `equipment-seller-status-1`, `equipment-manufacturer-status-1` — equipment shops, not lenders.
- `phone-hours-one` — contact metadata.
- `industries-served-one` — targeting metadata, not lender type.
- `company-classification-1` — generic; carries no lender taxonomy.

---

## 4. Target schema — `active/capital_provider_signals`

One row per `normalized_domain`. All columns nullable except `normalized_domain` and `materialized_at`.

| column | type | source / derivation |
|---|---|---|
| `normalized_domain` | string (NOT NULL, BTREE) | join key to `active/companies.normalized_domain` |
| `capital_type` | string | `capital-provider-json-1.capitalType` — **native, verbatim** (the primary outbound filter) |
| `provides_capital` | bool | `capital-provider-json-1.providesCapital` |
| `is_intermediary` | bool | `true` when `capital_type ∈ {brokerOrMarketplace, advisoryOnly, notCapitalProvider}` (exclude-from-outbound flag) |
| `financing_mode` | string | normalized enum: `direct` \| `broker` \| `partner` \| `multi` \| `unclear` (see §5.3) |
| `provides_equipment_financing` | bool | `true` if ANY financing-cluster payload has `providesEquipmentFinancing ∈ {yes,true}` |
| `construction_equip_financing` | string (null) | `construction-equipment-financing-1.conclusion` → `yes` \| `no` \| `unclear`; null if not enriched |
| `independent_equip_financing` | string (null) | `independent-equipment-financing-1.isIndependentEquipmentFinancingProvider` → `yes` \| `no`; null if not enriched |
| `ef_classification` | string (null) | `equipment-finance-classification-one.classification` where present |
| `confidence` | string | `capital-provider-json-1.confidence` (low/medium/high/very high) |
| `signals` | list<string> | the `enrichment_payload_type` values that contributed to this row (provenance + gap analysis) |
| `materialized_at` | timestamp[us, UTC] (NOT NULL) | build timestamp (pass in via arg; do not call wallclock inside a workflow) |

**No `gtm_bucket` column.** Coarse 3-bucket coalescing is explicitly OUT OF SCOPE (see §9). Outbound
filters on the native columns directly (see §8).

---

## 5. Transformation rules (exact)

### 5.1 Row universe (which domains get a row)
**Default:** every domain with `capital-provider-json-1.providesCapital='true'` (≈7,344) — the verified
lenders. PLUS curated-origin lender domains from `active/companies` where `source_platform ∈ {elfa, sfnet}`
that lack a capital payload (these are association-verified lenders: ELFA = Equipment Leasing & Finance
Assoc, SFNet = Secured Finance Network) — emit them with `capital_type=NULL`, `signals=['source:elfa']` etc.
PLUS every domain enriched by `construction-equipment-financing-1` (any `conclusion`) — **ledger
completeness for the construction vertical**; any such domain not already in the universe lands with
`capital_type=NULL`, its `construction_equip_financing` value, and `signals=['construction-equipment-financing-1']`.
PLUS every domain enriched by `independent-equipment-financing-1` (any verdict) — **ledger completeness
for the independent-financier vertical**; same NULL-capital handling for net-new domains.
**Exclude** `providesCapital='false'` and the no-answer envelopes — those are a separate re-enrichment task (§9).
⚠ **Operator decision flag (§10-A):** confirm whether curated-origin-without-payload rows are in or out.

### 5.2 `capital_type` — native passthrough + dedup
Pass the `capitalType` value through **verbatim** (including `hardMoney/bridge` with its slash). A domain
may have >1 `capital-provider-json-1` payload — **dedup to one row**: among the domain's
**`providesCapital='true'` payloads only** (per the operator directive "we don't accept the false values,
just the true ones in there"), pick the highest `confidence` (`very high` > `high` > `medium` > `low`),
tie-break on latest `landed_at`. Apply that winning payload's `confidence`; `provides_capital=true`.
Non-`true` payloads (false / no-answer) do **NOT** set `capital_type` — even when a false payload has
higher confidence, it never overrides the lender verdict — but they DO get recorded in `signals` (§5.6).

### 5.3 `financing_mode` — coalesce + normalize spelling (the messy part)
Across ALL financing-cluster types, pick the winning payload per domain (highest confidence, then latest),
then map its raw `financingMode` through this table:

| raw value(s) | → normalized |
|---|---|
| `direct-lender`, `directLender` | `direct` |
| `broker`, `broker/marketplace` | `broker` |
| `through-partner`, `partner/referral` | `partner` |
| `multi-lender` | `multi` |
| `unclear`, null/empty | `unclear` |

Any unrecognized raw value → `unclear` and **log it** (so new spellings are caught, not silently dropped).

### 5.4 `provides_equipment_financing`
`true` if any financing-cluster payload for the domain has `lower(providesEquipmentFinancing) ∈ {yes,true}`;
`false` if it appears with only `{no,false}`; `NULL` if the domain has no financing-cluster payload.

### 5.5 `ef_classification`
From `equipment-finance-classification-one.classification`, where present (dedup by confidence/latest).
**Drop rows where `domain_norm='skipped'`** (100 junk rows). Native values verbatim.

### 5.6 `signals`
Sorted distinct list of the `enrichment_payload_type` values that **ran on** the domain (had any payload
in the sink for that `domain_norm`), plus `source:<platform>` tokens for curated origins. **Ledger rule:**
record an enrichment type if it ran AT ALL — including `capital-provider-json-1` payloads that returned
`providesCapital=false` or a no-answer envelope. Otherwise the gap query (§8) re-flags an already-enriched
domain and triggers wasted re-enrichment. This column is the gap-analysis lever.

### 5.7 `construction_equip_financing`
From `construction-equipment-financing-1.conclusion` (dedup by confidence/latest). Pass the value through
as `yes` / `no` / `unclear`; `NULL` when the domain was never run through this enrichment. Keep `unclear`
distinct from `NULL` (enriched-but-uncertain ≠ not-enriched — the ledger distinction). This is a vertical
specialization flag, orthogonal to `capital_type`; a `factoring` lender can still be `yes` here.

### 5.8 `independent_equip_financing`
From `independent-equipment-financing-1.isIndependentEquipmentFinancingProvider` (bool, dedup by
confidence/latest): `true → yes`, `false → no`, `NULL` when never run. The payload also clears
`oemOrSellerFlags` internally, so a `yes` here is a high-precision "true independent equipment financier"
verdict — the cleanest equipment-finance refiner available, and trustworthy unlike `equipment-provider-status-1`.

---

## 6. Data hygiene (apply during the transform)
- **Junk domains:** drop any `domain_norm` failing `^(?=.{1,253}$)([a-z0-9](-?[a-z0-9])*\.)+[a-z]{2,}$`
  (catches `skipped`, `1) us business funding: https:`, etc.). Log the count dropped.
- **No-answer envelopes:** payloads with a top-level `result` key and no verdict field (the
  "I could not find an answer" shape) carry no signal — they contribute nothing; ensure the coalescer
  ignores them rather than emitting null verdicts that look real.
- **Self-contradictions** (`providesCapital=true` but `capital_type ∈ intermediary set`): keep the row,
  set `is_intermediary=true` — let the operator filter, don't silently drop.
- **Domain normalization** must be byte-identical to the `active/companies.normalized_domain` rule
  (lower/trim → strip scheme → strip leading `www.` → strip path/query → strip trailing dots → null if
  emptied). `domain_norm` in the sink already follows this; re-assert it, don't re-derive differently.

---

## 7. Write mechanics (Lance → R2)
- **URI:** `s3://data-sink/active/capital_provider_signals/`
- **Mode:** `overwrite` — this is a fully derived, rebuild-from-source table. Re-running reproduces it
  deterministically. (No append; the source jsonb is the SoR.) `data_storage_version="2.1"`.
- **Index:** after write, `ds.create_scalar_index("normalized_domain", index_type="BTREE")`.
- **Fragment sizing:** fleet defaults — `max_rows_per_file=1048576`, `max_bytes_per_file=90*1024**3`.
- Build the Arrow table with an explicit pyarrow schema matching §4 exactly (string/bool/list<string>/
  timestamp[us,tz=UTC]); cast before writing; fail loud on a NOT-NULL null.
- **Provenance:** record a run row in `ops.*` if a sibling pattern exists (optional, mirror
  `companies_people_bulk`); not required for v1.

---

## 8. How the operator uses it (acceptance of "useful")
```sql
-- equipment-finance lenders (native + cross-signal)
WHERE (capital_type IN ('equipmentFinancing','assetBasedLender') OR provides_equipment_financing)
  AND NOT is_intermediary
-- factoring / A-R only (a DISTINCT segment)
WHERE capital_type = 'factoring' AND NOT is_intermediary
-- private credit / middle market
WHERE capital_type IN ('privateCredit','mezzanine','ventureDebt') AND NOT is_intermediary
-- construction-equipment financiers (the vertical)
WHERE construction_equip_financing = 'yes'
-- verified INDEPENDENT equipment financiers (high precision, OEM/seller cleared)
WHERE independent_equip_financing = 'yes'
-- direct lenders only (drop brokers/partners)
WHERE financing_mode = 'direct'
-- GAP ANALYSIS: verified lenders missing the equipment-finance classifier → re-enrich queue
WHERE 'equipment-finance-classification-one' != ALL(signals)
```
Join to `active/companies` on `normalized_domain` for firmographics/outreach fields.

---

## 9. Out of scope / non-goals
- **The 3 coarse GTM buckets** (Factoring/AR · Equipment Finance/ABL · Private Credit/MM) — rejected;
  they destroy the native distinction. Filter on `capital_type` instead.
- **Re-enriching `providesCapital=false`** (~2,765 not yet on the spine) — separate assignment; this
  table only materializes what exists today.
- **On-the-fly Postgres projection** — rejected; this is a native Lance materialization.
- **Firmographic enrichment** of the lender rows — lives in the `active/companies` backfill, not here.

---

## 10. Operator decision flags (resolve or accept the default)
- **A. Curated-origin rows:** include `elfa`/`sfnet` (and `exa`/`exa-all`?) domains that have no capital
  payload, as rows with `capital_type=NULL`? **Default: include elfa+sfnet, exclude exa/exa-all** (generic
  discovery, no lender prior).
- **B. `nonBankLender` (3,611):** kept as a native coarse value. It is your largest single segment and the
  prime candidate for a refinement re-enrich pass. No action here beyond keeping it.
- **C. Intermediaries (`is_intermediary`, ~337):** kept-and-flagged (default) vs dropped. Default keeps them.
- **D. `bank` (297):** native value retained; banks do all three product lines — treat as its own segment.

---

## 11. Acceptance criteria (the build is done when…)
1. `lance.dataset("s3://data-sink/active/capital_provider_signals/")` opens; `count_rows()` ==
   `count_distinct(normalized_domain)` (key is unique).
2. Row count ≈ 7,344 (true-only) or ≈ 7,344 + curated additions per §10-A; reconcile and report any delta.
3. `capital_type` distribution matches §2 within drift; values are verbatim natives (no re-spelling).
4. `financing_mode` contains ONLY `{direct, broker, partner, multi, unclear}` — zero raw spellings leak.
5. `provides_equipment_financing` is bool-or-null; `ef_classification` present only for the ≤290 clean
   classifier domains; `domain_norm='skipped'` appears nowhere.
6. BTREE index on `normalized_domain` is committed (`ds.list_indices()`).
7. Join coverage reported: of N rows, how many match `active/companies.normalized_domain` (expect ~100%
   of the providesCapital=true set, since all 7,344 are now on the spine).
8. Spot-check 5 domains end-to-end (raw payload → row) across `factoring`, `equipmentFinancing`,
   `nonBankLender`, an intermediary, and a curated-origin row.
9. Idempotent: a second run reproduces identical row/column counts.

---

## 12. Appendix — source queries (copy-paste, read-only)

```sql
-- capitalType over verified lenders
SELECT raw_payload->>'capitalType', count(DISTINCT domain_norm)
FROM gtm.existing_claygent_payloads
WHERE enrichment_payload_type='capital-provider-json-1'
  AND raw_payload->>'providesCapital'='true' AND domain_norm IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;

-- financingMode raw values (to validate the normalization map covers everything)
SELECT raw_payload->>'financingMode', count(DISTINCT domain_norm)
FROM gtm.existing_claygent_payloads
WHERE raw_payload ? 'financingMode' AND domain_norm IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;

-- providesEquipmentFinancing across the financing cluster
SELECT enrichment_payload_type, lower(raw_payload->>'providesEquipmentFinancing'), count(DISTINCT domain_norm)
FROM gtm.existing_claygent_payloads
WHERE raw_payload ? 'providesEquipmentFinancing' AND domain_norm IS NOT NULL GROUP BY 1,2 ORDER BY 1,3 DESC;

-- ef_classification (exclude junk)
SELECT raw_payload->>'classification', count(DISTINCT domain_norm)
FROM gtm.existing_claygent_payloads
WHERE enrichment_payload_type='equipment-finance-classification-one'
  AND domain_norm <> 'skipped' AND domain_norm IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;

-- multiple-payload domains (confirms the dedup rule is needed)
SELECT domain_norm, count(*) FROM gtm.existing_claygent_payloads
WHERE enrichment_payload_type='capital-provider-json-1' GROUP BY 1 HAVING count(*) > 1 ORDER BY 2 DESC LIMIT 20;
```

Lance read/write skeleton:
```python
import os, pyarrow as pa, lance, duckdb, psycopg
# … build rows per §5/§6 into `records` (list of dicts) …
schema = pa.schema([
    pa.field("normalized_domain", pa.string(), nullable=False),
    pa.field("capital_type", pa.string()),
    pa.field("provides_capital", pa.bool_()),
    pa.field("is_intermediary", pa.bool_()),
    pa.field("financing_mode", pa.string()),
    pa.field("provides_equipment_financing", pa.bool_()),
    pa.field("ef_classification", pa.string()),
    pa.field("confidence", pa.string()),
    pa.field("signals", pa.list_(pa.string())),
    pa.field("materialized_at", pa.timestamp("us", tz="UTC"), nullable=False),
])
tbl = pa.Table.from_pylist(records, schema=schema)
lance.write_dataset(tbl, "s3://data-sink/active/capital_provider_signals/",
                    mode="overwrite", data_storage_version="2.1",
                    max_rows_per_file=1048576, max_bytes_per_file=90*1024**3,
                    storage_options=storage_options())
lance.dataset("s3://data-sink/active/capital_provider_signals/",
              storage_options=storage_options()).create_scalar_index("normalized_domain", index_type="BTREE")
```

---

**End of assignment.** Execute §5–§7, verify against §11, report the final row count + `capital_type`
distribution + join coverage. Flag §10 decisions if not yet resolved.
