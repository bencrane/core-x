# TiC Payer Reverse-Mapping — Architecture & Runbook (Aetna & UHC)

**Role of this document.** The standing architecture and operating runbook for the
TiC reverse-mapper: how payer Transparency-in-Coverage (TiC) machine-readable
files (MRFs) become the `tic_negotiated_rates` Lance fact table and the
`tic_employer_file_bridge` dataset, bridging the local `practice_group_360` SoR
(commit `e2b479c`) to commercial negotiated rates. All hard numbers below are
**measured on live payer infrastructure on 2026-06-07** unless explicitly labeled
*(extrapolated)*; treat every dated URL, count, and token property as a snapshot
of that date (see Preflight before any production run).

**Implementation.** `pipelines/tic_mrf/` (Python — the core-x compute plane;
the canonical streaming stack is `ijson` (yajl2_c) + DuckDB → Lance):
- `reverse_map.py` — out-of-core two-source streaming reverse-mapper (the engine).
- `orchestrate.py` — Modal production fan-out (worklist → bridge → reverse-map → index), blast-radius isolated, deploy+spawn launched.
- `part1_filter_spine.py` — deterministic cohort extractor from the local SoR.
- `ops_tic_reverse_map_runs.sql` — idempotency / terminal-state ledger.
- `test_reverse_map.py` — offline unit tests for the extraction logic (synthetic fixture, no network).

---

## 0. Three directive premises corrected against reality (measured 2026-06-07)

| # | Directive premise | Reality (proven) | Where |
|---|---|---|---|
| 1 | "Type 2 NPI (the group billing identifier)" | **No Type-2/org NPI, EIN, or TIN exists anywhere in the SoR.** The canonical group billing anchor is the PECOS `group_enrlmt_id`. The Type-1 array ships natively as `practice_group_360.member_npis`. | live schema |
| 2 | Aetna Module A: "Scan the [ToC] index for string matches against your NPI array" | **TiC ToC files contain zero provider NPIs** (TiC v2.0.0: ToC maps `reporting_plans → in_network_files[].location` only). NPI matching is structurally impossible at the ToC layer; it requires descending into an in-network file and resolving `provider_references`. | Aetna + UHC ToC, verified |
| 3 | UHC Module B: "reconstruct the filename deterministically … bypass index crawling … fire direct GETs" | **0/15 blind-GETs succeed (all HTTP 409)** — *even when the reconstructed filename is byte-for-byte correct* — because `mrfstore.uhc.com` is SAS-token gated. Master-index string-match: **7/15**. The filename is also non-reconstructable (mixed-case, `&`→`--`, divergent legal names). | UHC, empirically tested |

The pipeline implements the correct inversion for each payer (below).

---

## Part 1 — The Filter Spine (local context extraction)

Pulled from the local SoR **before** any external request, so the (expensive)
MRF streams only ever search for a tiny fixed cohort. The snapshot partition is
a CLI parameter (`--group-snapshot`); pin it to the current month at run time.

**Geography = NY · Specialty = Orthopedic surgery (NUCC `207X00000X`).**

Selection against `practice_group_360` (253,740 groups, 1 fragment): bitmap
pushdown `group_state='NY'`, residual `top_specialty='207X00000X'`,
`member_count BETWEEN 2 AND 9` (**multi-provider + fan-in ≤ 9** — "fan-in" is the
count of distinct providers reassigning Medicare billing into the group,
`materialize.py:PRACTICE_AGG`), `independent_member_count = member_count`
(**dual-pole independent** — every member's *smallest* pole is this group, i.e. a
real independent practice, not a consolidator's billing arm), `distinct_specialties ≤ 2`.

### A) 5 target independent groups — 15 Type-1 NPIs (extracted 2026-06-07)

| Org (string-match handle) | PECOS `group_enrlmt_id` (billing anchor) | Members | Medicare $ | Type-1 NPIs |
|---|---|---|---|---|
| MANHATTAN ORTHOPEDIC & SPORTS MEDICINE GROUP | `O20060215000749` | 5 | $11,254,134 | 1013024801, 1518075662, 1386684314, 1972610798, 1457587941 |
| SOUTHERN WESTCHESTER ORTHOPEDICS & SPORTS | `O20141028001847` | 3 | $7,078,487 | 1245219856, 1891924064, 1942289541 |
| ROSS ORTHOPEDIC GROUP PC | `O20070822001001` | 2 | $4,797,645 | 1295708600, 1720020845 |
| MICHAEL L. PARKS, M.D, PLLC | `O20110930000152` | 2 | $2,971,208 | 1710732920, 1992762587 |
| STATEN ISLAND ORTHOPEDICS & SPORTS MEDICINE | `O20040312000535` | 3 | $2,917,978 | 1760445001, 1407819741, 1316900657 |

> `type2_npi` = **(NOT IN SUBSTRATE — PECOS `group_enrlmt_id` is the billing anchor)**.

### B) UHC seed — top NY self-funded health employers (head of 50, extracted 2026-06-07)

`form5500_main` (19,114 large/Schedule-H plans — **not** the 5500-SF short form,
which is small plans < 100 participants), `SPONS state = NY`, `TYPE_WELFARE_BNFT_CODE`
contains `4A` (health), `FUNDING_GEN_ASSET_IND='1' OR BENEFIT_GEN_ASSET_IND='1'`
(self-funded ⇒ benefits paid from sponsor general assets), ranked by
`TOT_ACTIVE_PARTCP_CNT`.

| Sponsor | EIN | Active participants |
|---|---|---|
| GALWAY HOLDINGS LP | 851574210 | 4,811 |
| RAYMOURS FURNITURE COMPANY, INC. | 150556500 | 4,345 |
| SENECA FOODS CORPORATION | 160733425 | 2,750 |
| BALDOR SPECIALTY FOODS, INC. | 113059167 | 2,595 |
| OSCAR MANAGEMENT CORPORATION | 473979452 | 2,213 |
| OUTFRONT MEDIA LLC | 464042148 | 2,212 |
| … | … | (50 total) |

---

## Part 2 — Payer-specific architecture playbooks

### Module A — The Aetna Inversion (master-index parsing)

**Endpoint topology (discovered 2026-06-07 by intercepting the SPA's XHR; the `#/`
route is a HealthSparq/Sapphire Ember SPA shell on AWS CloudFront+S3):**
- **Portal/API:** `health1.aetna.com` — HealthSparq backend is **session-gated**
  (HTTP **440** until the `GET /healthsparq/service/public/login?...` handshake
  sets a trace-session cookie). Not needed for the data plane.
- **Data plane (public, no auth):** `https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/<brandCode>/` — BunnyCDN edge → Google Cloud Storage origin.
- **Master manifest:** `…/AETNACVS_I/ALICSI/latest_metadata.json` = **6,971,311 B**, enumerates **12,077 files** (2,019 `TABLE_OF_CONTENTS`, **9,410 `IN_NETWORK_RATES`**, 648 `ALLOWED_AMOUNTS`) with `{reportingPlans, fileSchema, fileName, filePath}`. Plus `deep_link_map.json` (3,115,578 B). *(counts as of 2026-06-07 — re-verify at run time)*
- **⚠ Trap:** the JS bundle still contains a hardcoded `s3.us-west-2.amazonaws.com/prd2-directory-generation-service-machine-readable-json/<brand>/index.json` template — a **stale/dead code path that returns 403 AccessDenied on every key**. Do not build on it.

**Flow:** `latest_metadata.json` → filter `fileSchema=IN_NETWORK_RATES` → stream
each in-network file → resolve `provider_references` → match cohort. (The
per-plan ToC files exist but are redundant given the master manifest.) The
worklist tags each file with `reportingPlans[0]` as `plan_id` — a **sample**, not
an attribution (a shared network file serves many plans); exact employer/plan
attribution is the bridge's job (below).

### Module B — The UHC Inversion (employer-first) + the refuted shortcut

**Endpoint topology:** Azure App Service behind Azure Front Door
(`*.azurewebsites.net`, `x-azure-ref`, `x-cache`). Blobs on `mrfstore.uhc.com`,
**SAS-token gated**.
- **Master index:** `GET https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/`
  = **31,299,180 B** (identity-encoded, *not* gzipped), **86,514 entries**:
  66,912 `_index.json` (employer ToCs), **7,170 `in-network-rates`**, 12,428 `allowed-amounts`.
  Each entry = `{name, downloadUrl, size}` where `downloadUrl` carries a
  **container-scoped SAS token** (identical `sig` across all entries).
  *(counts and token scope as of 2026-06-07 — re-verify at run time)*

**Empirical test of the directive's "deterministic filename → blind GET" premise (2026-06-07):**

| Method | Result |
|---|---|
| **Blind-GET** reconstructed filenames (HEAD `mrfstore.uhc.com`) | **0 / 15 hits — all HTTP 409 `PublicAccessNotPermitted`** |
| Proof it is auth, not slug: the directive's own example `2026-06-01_1-800-RADIATOR-OF-DALLAS-FORT-WORTH-LLC_index.json` | **409 without SAS · 200 (2,013 B) *with* SAS** |
| **Master-index string-match** (same 15 employers) | **7 / 15 hits** (MSK, Marsh & McLennan, Estée Lauder, Corning, Columbia, Pfizer, Verizon) with resolvable SAS URLs |

**Verdict: `deterministic-fails-use-master-index`.** Two independent failure modes:
(1) anonymous blob access is disabled — the SAS HMAC cannot be reconstructed
client-side, so you must fetch the index to obtain it; (2) UHC's real slugs are
**mixed-case** with `&`→`--` (`2026-06-01_Marsh--McLennan-Companies-Inc_index.json`)
and the filed entity strings diverge from sponsor legal names (only 7/15 matched
even under case-insensitive normalization). **Flow:** fetch master index once →
use the exact `downloadUrl` (filename reconstruction is both unnecessary and
unreliable). 66,912 employer ToCs dereference to only **7,170 distinct in-network
files** — the worklist dedups by URL (≈ 9 employers per shared network file).

**SAS token discipline (load-bearing).** The token is a credential, not identity:
`sig` is re-minted on every master-index fetch and the token expires on the
payer's clock. The pipeline therefore splits the two everywhere:
- The worklist row carries `in_network_url` **token-stripped** (the stable blob
  path) plus `sas_query` separately; workers recombine at fetch time.
- The ledger, the fact-table `source_file_url`, and the bridge all key on the
  **stripped** URL — the same blob always resolves to the same identity across
  worklist rebuilds and monthly drops.
- A worker hitting 403/409 mid-fan-out (token expired under a worklist built
  hours earlier) re-resolves a fresh `downloadUrl` from the master index and
  retries the file once (`_uhc_fresh_sas`).

---

## Part 3.1 — Unified schema blueprints (verbatim, from live bytes, 2026-06-07)

### ToC reference block — plan → file path

**Aetna** (`…/2026-06-05/tableOfContents/2026-06-05_91108011_index.json.gz`, TiC v2.0.0):
```json
{ "reporting_plans": [ {
    "plan_name": "CONNECTWISE, LLCAetna Choice POS II",
    "issuer_name": "Aetna Life Insurance Company",
    "plan_id_type": "ein", "plan_id": "821582035",
    "plan_sponsor_name": "CONNECTWISE, LLC", "plan_market_type": "group" } ],
  "in_network_files": [ {
    "description": "in-network file",
    "location": "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICSI/2026-06-05/inNetworkRates/2026-06-05_pl-4el-tr25_Aetna-Life-Insurance-Company.json.gz" } ] }
```

**UHC** (`2026-06-01_NYU-Langone-Hospitals_index.json`, 4,939 B):
```json
{ "reporting_plans": [ {
    "plan_name": "CHOICE-EPO-50-E", "plan_id": "133971298", "plan_id_type": "EIN",
    "plan_market_type": "group", "plan_sponsor_name": "NYU-Langone-Hospitals",
    "issuer_name": "NYU-Langone-Hospitals" } ],
  "in_network_files": [ {
    "description": "in-network files",
    "location": "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/2026-06-01/2026-06-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_EP1-50_C1_in-network-rates.json.gz" } ] }
```

### Rate-table node — NPI array → CPT → allowed amount

The link is **indirect**: `in_network[].negotiated_rates[].provider_references` is a
list of integer `provider_group_id`s, dereferenced against the top-level
`provider_references[]`.

**Aetna** — rate node + the provider-reference it resolves to:
```json
{ "negotiation_arrangement": "ffs", "name": "AMINO ACIDS, MULT QUAL",
  "billing_code_type": "CPT", "billing_code": "82128",
  "negotiated_rates": [
    { "provider_references": [ 20275 ],
      "negotiated_prices": [ { "negotiated_type": "fee schedule", "negotiated_rate": 10.0,
        "service_code": ["CSTM-00"], "billing_class": "professional", "setting": "both" } ] },
    { "provider_references": [ 107476, 161617, 209567, 557552, 917375, 990514 ],
      "negotiated_prices": [ { "negotiated_rate": 11.0, "billing_class": "professional" } ] } ] }
```
```json
{ "provider_group_id": 991625,
  "provider_groups": [ { "npi": [ 1447416615, 1053845859, 1093780280 ],
      "tin": { "type": "ein", "value": "760622208", "business_name": "Acosta Crystal" } } ],
  "network_name": [ "Aetna Choice POS II", "Aetna Select", "Open Access Aetna Select" ] }
```

**UHC** — rate node + the provider-reference it resolves to (real NYC practices):
```json
{ "negotiation_arrangement": "ffs", "name": "RBC DNA HEA 35 AG 11 BLD GRP WHL BLD CMN ALLEL",
  "billing_code_type": "CPT", "billing_code": "0001U",
  "negotiated_rates": [ { "provider_references": [ 15 ],
    "negotiated_prices": [ { "setting": "inpatient", "negotiated_rate": "432.0",
      "service_code": ["CSTM-00"], "negotiated_type": "negotiated",
      "expiration_date": "9999-12-31", "billing_class": "professional" } ] } ] }
```
```json
{ "provider_group_id": 15, "network_name": [ "NYU Langone Hospitals" ],
  "provider_groups": [
    { "npi": [ 1235592239 ], "tin": { "type": "ein", "value": "263069729", "business_name": "SPINE AND PAIN CONSULTANT" } },
    { "npi": [ 1770540403 ], "tin": { "type": "ein", "value": "132852297", "business_name": "ALLERGY ASTHMA ASSOCIATES OF MURRAY HILL" } } ] }
```

The `tin` object inside every `provider_group` is the **only org-level identifier
in the whole chain** (premise-correction #1: the SoR carries no Type-2 NPI/EIN).
The engine carries it onto every emitted row — it is what joins a rate row to the
practice entity, to Form 5500 sponsors, and to the wider entity graph.

---

## The engine — two-source streaming reverse-map (`reverse_map.py`)

A TiC drop is two tiers: the ToC/index (plans → file URLs, **no NPIs**) and the
in-network rate files (rates keyed by billing_code, providers attached inline or
— for national payers — by reference through `provider_references[]`). The only
memory-bounded way to extract "rates for these N NPIs" is a two-source streaming
join, not a string scan:

- **Pass A (provider spine).** Stream `provider_references` (inline or the
  external reference file) → keep, per matched `provider_group_id`, one entry per
  provider_group: `{npis ∩ cohort, tin_type, tin_value, tin_business_name}`.
  RAM = O(matched groups) ≈ O(cohort). The TIN rides the spine from here on.
- **Pass B (rate spine).** Stream `in_network[]` → resolve each rate node's
  `provider_references` ids against the spine (or inline `provider_groups`) →
  emit one flat row per (npi × billing_code × price), each row carrying its
  group's TIN. RAM = O(1) streaming; rows stage locally, then commit to Lance.

I/O is O(file bytes); RAM is bounded by the cohort, never the payload. The engine
handles **multi-member gzip** (payer streaming-gzip writers concatenate
independent members; a fresh decompressor rolls across each `dec.eof` boundary or
the JSON silently truncates) and retries 429/503 with `Retry-After`/exponential
backoff before the stream starts.

**ijson backend is pinned.** `require_fast_ijson()` runs at every worker start
and fails fast unless the `yajl2_c` C backend is active (the pure-Python fallback
is ~an order of magnitude slower — see the cost model's dependency note in
Preflight). `TIC_ALLOW_SLOW_IJSON=1` downgrades to a loud warning for local
debugging only.

### Fact-table schema & the EIN join rule

`s3://data-sink/active/tic_negotiated_rates/` (append-only Lance), one row per
(npi × billing_code × price):

| Column | Notes |
|---|---|
| `payer`, `plan_id` | `plan_id` is provenance-grade (worklist sample), not attribution — use the bridge for employer/plan attribution |
| `npi` | Type-1 practitioner NPI (cohort member) |
| `tin_type`, `tin_value`, `tin_business_name` | the provider_group's TIN, verbatim |
| `billing_code`, `billing_code_type`, `negotiated_rate`, `negotiated_type`, `billing_class`, `service_codes`, `expiration_date` | the rate facts |
| `source_file_url` | **token-stripped** blob path (stable identity) |
| `file_version` | the ingested version (ETag-derived surrogate, never NULL) — distinguishes monthly re-drops at row level |
| `captured_at` | capture date (env `TIC_CAPTURE_TS` or the run's UTC date) |

Indexes: BTREE `npi`, `billing_code`, `tin_value`; BITMAP `payer`,
`billing_class`, `billing_code_type`, `tin_type`.

**The `tin_type == 'npi'` exclusion rule (first-class, load-bearing).** CMS's TiC
schema allows TIN=NPI for sole proprietors. Those rows are **preserved** (the rate
facts are real) but their `tin_value` is the proprietor's own NPI, **not an EIN**.
Every org-level join — Form 5500 (`SPONS_DFE_EIN`), the employer bridge, the
entity graph — must gate on `tin_type = 'ein'` (and digits-normalize both sides),
or sole-proprietor NPIs silently collide with the 9-digit EIN space. Payer-mix
rollups must additionally group by **practice entity, not raw TIN** — a practice
billing under several TINs appears under several provider_groups (NPPES
corroborates multi-EIN practices at serving time; see Joins below).

### Atomic per-file commit

A worker streams matched rows into a **local** Lance stage (bounded 50k-row
flushes) and, only after the source stream completes without error, commits the
whole file to the SoR as **one** Lance append (single manifest commit —
`publish_stage_to_sor`). A mid-stream failure publishes nothing; a retry of a
failed file can never double-append. This is the same local-stage-then-publish
transport the NPPES analytical layer uses (D8). Scalar-index (re)build stays a
separate, isolated, heavy job (`rebuild_indexes`) — a flaky multi-GB parse can
never corrupt the index.

### Idempotency ledger

`ops.tic_reverse_map_runs` (HQX_DB_URL_POOLED), unique on
`(payer, source_file_url, file_version)`:
- `source_file_url` is **token-stripped** — the blob path is the identity; the
  SAS query re-mints per index fetch.
- `file_version` is **NOT NULL**, always: `derive_file_version` prefers ETag,
  then Last-Modified, then the dated slug every UHC/Aetna filename carries
  (`date:2026-06-01`), then a `bytes:<content_length>` surrogate. (A NULL would
  be distinct to the unique index — skip and upsert would both silently
  disable.)
A file whose version already has a `success` row is skipped — safe to retry,
safe to reshard a nationwide run; a monthly drop publishes a new version and
re-ingests exactly once.

---

## The employer→file bridge (`tic_employer_file_bridge`)

Rates alone give price position; book-of-business sizing needs the **volume
proxy**: which self-funded employers (with Form 5500 participant counts) route
through the network file where a practice's rates appear. The employer ToCs are
already being read to build the worklist — the bridge materializes them instead
of discarding them.

`build_employer_bridge` writes
`s3://data-sink/active/tic_employer_file_bridge/payer=<payer>/` (overwrite per
payer — rebuildable, idempotent; local Lance stage → boto3 publish), one row per
(reporting_plan × in-network file):

| Column | Notes |
|---|---|
| `payer` | uhc \| aetna \| cigna \| … |
| `ein` | sponsor EIN, **digits-only normalized** (mixed hyphenated/plain formats occur within one file); NULL for non-EIN plan ids |
| `plan_id_raw`, `plan_id_type`, `plan_name`, `plan_sponsor_name`, `plan_market_type`, `issuer_name` | plan metadata verbatim |
| `in_network_url` | **token-stripped** — joins to `tic_negotiated_rates.source_file_url` |
| `toc_url`, `captured_at` | provenance |

Indexes: BTREE `ein`, `in_network_url`; BITMAP `payer`, `plan_market_type`.

**Join path (the sizing chain):**
```
form5500_main.SPONS_DFE_EIN  (digits-normalized)
  = tic_employer_file_bridge.ein            → in_network_url
  = tic_negotiated_rates.source_file_url    → npi, tin_value (tin_type='ein'), rates
```
**Fan-in caveat (schema contract):** file-level attribution is many-to-many —
≈ 9 employers per shared network file (UHC) up to ~1,075:1 (Cigna). The bridge
yields **candidate-employer sets** per file, not exact panel membership.
Issuer-EIN structures (`issuer_name` = an insurer; e.g. the fully-insured book)
carry the issuer's EIN in `plan_id` — filter on `plan_market_type` and sponsor
fields when seeding from Form 5500's self-funded book.

UHC: the bridge fan-fetches the 66,912 small `_index.json` ToCs from the master
index. Cigna and other single-national-index payers: pass the index URL
(`--index-url`) and it streams directly.

---

## Part 3.2 — Engineering & scale metrics (measured 2026-06-07)

### File footprints (compressed vs uncompressed, real)

| Payer | Sampled in-network file | Compressed | Uncompressed | Ratio | gzip members |
|---|---|---|---|---|---|
| Aetna | `…_pl-4nh-tr25_Aetna-Life-Insurance-Company.json.gz` | 39,748,278 B (37.9 MB) | 955,555,341 B (911 MB) | **24.04×** | 1 (single-member) |
| UHC | `…_NYU-Langone-Hospitals_CSP-939-T321_in-network-rates.json.gz` | 9,903,225 B (9.4 MB) | 177,543,948 B (169 MB) | **17.93×** | **3 (multi-member)** |
| Aetna ToC | `…_91108011_index.json.gz` | 384 B | 679 B | — | 1 |

> The UHC file is **concatenated multi-member gzip**; `zlib.decompressobj` decodes
> only member 1 and parks the rest in `unused_data` → silent JSON truncation. The
> engine rolls a fresh decompressor across each `dec.eof` boundary. This bug was
> caught and fixed during the POC; the recon agent's claimed `uncompressed_bytes
> = 65536` for this file was flagged as fabricated by the verifier and discarded —
> the 177,543,948 B above is re-measured.

### Compute performance (real, `time.perf_counter` + `resource.getrusage`)

Engine = `reverse_map.py`, full two-pass reverse-map over the network, cohort =
15 NPIs, `ijson` yajl2_c backend:

| Run | Bytes moved (2 passes) | Uncompressed processed | Wall-ms | Peak RSS | Throughput | Matches |
|---|---|---|---|---|---|---|
| Aetna in-network (full) | 79,496,556 B | 1,911,110,682 B | 24,492 ms | **353.8 MB** | 74.4 MB/s | 0 (cohort not in this plan slice) |
| UHC in-network (full) | 19,806,450 B | 355,087,896 B | 4,574 ms | **344.2 MB** | 74.0 MB/s | 0 (cohort not in this plan slice) |
| **UHC positive control** (3 NPIs known present) | 19,806,450 B | 355,087,896 B | 3,945 ms | 453.8 MB | 85.9 MB/s | **165,729 rate rows** |

Wall-clock and worker telemetry (`parse_ms`, `throughput_mb_s`) span **both**
passes — the fan-out ledger times Pass A + Pass B under one clock, matching this
table's methodology.

**Architecture validation — bounded RAM is real.** During recon, the *naive*
approach (`ijson.kvitems` materializing the whole `in_network` array) peaked at
**6,934 MB RSS** on the Aetna file; true `ijson.items` streaming held **68.8 MB**.
The engine's RAM is O(cohort + matched groups), **not** O(payload) — confirmed by
holding ~350 MB peak while streaming ~1.9 GB of decompressed JSON.

**Positive control (end-to-end proof on production bytes).** Reverse-mapping the
real UHC NYU file against 3 NPIs known to be present emitted **165,729 flat rate
rows with real negotiated dollars**, e.g. NPI `1881741544` → CPT `0001U` →
**$302.40**; `1144207275` → **$345.60**; `1235592239` → **$432.00**. The full chain
(multi-member gzip stream → `provider_references` resolution → `in_network`
extraction → row emission) is validated. The cohort's **0 matches in the two
sampled files is honest and expected**: each file is one network slice of a
9,410- / 7,170-file universe; finding the 5 NY-ortho groups requires the full scan
that Part 3.3 sizes.

### Anti-bot reconnaissance (measured 2026-06-07)

| | Aetna (data plane) | UHC (data plane) |
|---|---|---|
| Edge / origin | BunnyCDN → GCS | Azure Front Door → `mrfstore.uhc.com` (Azure Blob) |
| WAF / bot challenge | **None** on MRF blobs | **None** (no Cloudflare/Akamai, no CAPTCHA/JS challenge) |
| Auth on rate files | **None** (fully public) | **SAS token required** (409 `PublicAccessNotPermitted` without it; token is in the master index) |
| UA sensitivity | **None** (identical 200 for curl-UA and Chrome-UA) | **None** (200 with browser-UA, curl-UA, and empty UA) |
| Rate limiting | none observed | none observed (5 rapid HEADs → 200, no 429/Retry-After; not stress-tested) |
| Session gate | HealthSparq **API** returns 440 pre-login (irrelevant to blobs) | portal/API on `*.azurewebsites.net` (irrelevant to blobs) |

**Programmatic posture (implemented in `reverse_map.py`):** (1) browser-shaped
`User-Agent` (`TIC_USER_AGENT` override) — measured unnecessary on both payers,
kept purely defensive for future CDN edge rules; (2) **UHC: fetch the master
index to obtain the SAS-bearing `downloadUrl`** — there is no auth-free path;
(3) discover the Aetna data-plane URL from `latest_metadata.json`, never the dead
`prd2` S3 template; (4) HTTP HEAD → `Content-Length`/`ETag` before download for
the projection and the version key; (5) streaming with a compressed-byte cap for
sampling; (6) 429/503 retry with `Retry-After`/exponential backoff — the fan-out
concurrency regime is not the probed 5-HEAD regime, so the backoff ships
regardless of what any probe shows.

---

## Part 3.3 — Production resource projection (403,179 candidate NPIs)

**Key property:** I/O is proportional to the **file universe streamed, not the
cohort size.** Searching 15 NPIs or 403,179 streams the *same* files; the cohort
only sizes the in-RAM match index. 403,179 NPIs as a Python `set` ≈ **~40 MB**;
the matched-`provider_group` map is bounded by groups containing ≥1 candidate
(≈ hundreds of MB). **The bounded-RAM out-of-core model holds unchanged at
nationwide scale** — a 4 GB worker suffices.

### Universe size (2026-06-07; Aetna sizes HEAD-sampled n=45; UHC summed exactly from the index)

| Payer | In-network files | Compressed total | Ratio | Uncompressed total |
|---|---|---|---|---|
| Aetna | 9,410 | **19.33 TB** *(mean 1.91 GiB/file, extrapolated from n=45)* | 24.04× | 464.7 TB |
| UHC | 7,170 (distinct) | **88.94 TB** *(exact; median 13.7 GiB/file, max 21 GiB)* | 17.93× | 1,594.7 TB |
| **Combined** | **16,580** | **≈ 108.3 TB compressed** | — | **≈ 2.06 PB uncompressed** |

### Compute (Modal), at the measured **75 MB/s uncompressed per worker** (yajl2_c)

| Mode | Worker-hours | Wall-clock @ 500 workers | @ 1,000 workers |
|---|---|---|---|
| **Single-pass** (refs-first optimization — UHC confirmed refs precede `in_network`) | **≈ 7,630 hr** | 15.3 h | 7.6 h |
| **Two-pass** (current validated engine; inline refs) | ≈ 15,250 hr | 30.5 h | 15.3 h |

> Compute is **CPU-parse-bound**, not bandwidth-bound: downloading 108 TB
> compressed at ~62 MB/s/conn ≈ 480 worker-hr, vs ~7,630 worker-hr to
> decompress+parse 2.06 PB. Payer CDN egress is **free** (public files); R2
> ingress is **free**.
>
> **Cost band** (assumption: blended Modal ~$0.10/core-hr for a 1-core/4 GB worker):
> single-pass ≈ **$760/full monthly run**, two-pass ≈ **$1,525**. At $0.05–0.15/core-hr: **$380–$2,290**.
> Shipping the single-pass-refs-first optimization halves this — recommended next step.
> The whole band assumes the **yajl2_c** backend (worker-start assertion); the
> pure-Python fallback is ~10× slower and would put a full run near $7,600/week-scale.

### Storage (Cloudflare R2, $0.015/GB-mo, zero egress) — **filter, don't mirror**

| Strategy | Monthly footprint | R2 cost/mo | Verdict |
|---|---|---|---|
| **Raw mirror** of all in-network files | 108.3 TB/snapshot (grows unbounded; 12-mo retention ≈ 1.3 PB) | ≈ **$1,624/snapshot** (~$19.5K/mo at 12-mo) | inert, wasteful |
| **Reverse-mapped fact table** (this pipeline) — only matched rate rows for the candidate universe, Lance + scalar indexes | **≈ 0.2–1.5 TB** *(resolves on first full run; ~1–15 B rows × ~50 B, dictionary/RLE-compressed)* | ≈ **$3–$23/mo** | queryable, indexed, **50–500× smaller** |

---

## Joins — where enrichment lives

- **NPPES (monthly, in-factory): at serving time, not ingest time.** The fact
  table keys on NPI; enrich with NPPES taxonomy/practice-address at query/mart
  time to (a) validate matched NPIs are still active and (b) supply the payer-mix
  output's geography/specialty dimensions. NPPES "Other Provider Identifier" and
  organization records also corroborate `tin_value` for multi-EIN practices —
  the input to the group-by-practice-entity rollup rule. No change to the
  streaming engine.
- **Form 5500: through the bridge** (join path above). Coverage scope: the
  employer seed is the self-funded/large-plan book; the fully-insured
  (issuer-stamped, ~25.5% per the Cigna analysis) book is a known blind spot, and
  participant counts are plan-year filings lagging ~1 year — acceptable for
  deal-sourcing granularity.
- **CMS Open Payments: no.** Industry-payment signal, not payer-mix or volume;
  it belongs to the entity-360 layer, not this pipeline.

---

## Architecture decisions (defense)

1. **Reverse-map, don't mirror.** Streaming-filter the 2.06 PB universe to the
   candidate-NPI fact table (~sub-TB). 50–500× storage reduction; the output is
   indexed and queryable, the raw mirror is not.
2. **Bounded RAM, out-of-core.** `ijson` event streaming + gzip-member streaming;
   RAM = O(cohort + matched groups), proven ~350 MB peak over 1.9 GB payloads vs
   6.9 GB for the naive materialize.
3. **Append-only SoR, atomic per file.** Local stage while streaming; ONE Lance
   append (single manifest commit) on complete success; the SoR is never
   rewritten in place and a retry can never double-append.
4. **Blast-radius isolation.** worklist (network-only) → bridge (network-only) →
   reverse-map (per-file, fanned) → index rebuild (heavy external sort,
   isolated). A flaky multi-GB parse touches one file's stage, never the SoR,
   never the index, never another file.
5. **Identity ≠ credential.** Ledger, fact rows, and bridge key on the
   token-stripped blob path; the SAS token rides separately and refreshes from
   the master index on 403/409.
6. **Idempotency ledger with non-null versions.** `(payer, source_file_url,
   file_version)` unique; `file_version` always derived, never NULL; rows also
   carry `file_version` so monthly re-drops are distinguishable in the fact table.
7. **Carry the TIN or lose the outcome.** The provider_group TIN is the only
   org-level identifier in the chain; it rides Pass A → Pass B → every row, with
   `tin_type` making the sole-proprietor (TIN=NPI) case structurally excludable
   from EIN joins.
8. **Materialize the employer bridge.** The employer ToCs are read anyway; the
   bridge (tiny: 66,912 UHC / 29,216 Cigna rows) is the volume-proxy half of the
   sizing outcome.
9. **Deploy + spawn, never sync-drive.** The fan-out driver runs server-side on
   the deployed app and is spawned (async input); a laptop-tethered sync driver
   dies with the client.

---

## Preflight / re-verification (run before any production fan-out)

**Fast-decaying — re-verify in one cheap pass (monthly drops rotate everything dated):**
- Re-fetch `latest_metadata.json` and the UHC master index; confirm the file
  counts (9,410 / 7,170 / 86,514 as of 2026-06-07) and re-sample sizes — the
  cost band scales linearly with them.
- Measure the SAS token TTL and confirm the token is still container-scoped
  (one `sig` for all blobs). The TTL bounds how stale a worklist can be before
  the mid-run refresh path starts doing real work.
- Probe anti-bot posture **at fan-out concurrency** — the 2026-06-07 probe was 5
  sequential HEADs; 64 containers pulling ~88 TB through Azure Front Door is a
  different regime. The 429/503 backoff is already in the engine regardless of
  what the probe shows.
- Bump `--group-snapshot` on `part1_filter_spine.py` to the current
  `practice_group_360` partition and re-extract the cohort.
- Confirm the Modal image resolves `ijson` with the **yajl2_c** backend (the
  worker asserts it at start; the entire cost model depends on it).

**Architecture-stable — do NOT re-verify:** TiC v2.0.0 schema shapes,
`provider_references` indirection, the ToC-has-no-NPIs finding, multi-member gzip
handling, the dead Aetna `prd2` S3 path, master-index-first inversion,
bounded-RAM engine behavior.

---

## Runbook

```bash
# Part 1 — filter spine (reads R2 creds via doppler; pin the current snapshot)
doppler run -- uv run pipelines/tic_mrf/part1_filter_spine.py \
    --state NY --specialty 207X00000X --group-snapshot 2026-07

# Engine, one file (POC/local; UHC needs the SAS downloadUrl from the master index)
uv run pipelines/tic_mrf/reverse_map.py --innetwork "<in_network_url>" --npis <csv> --payer <aetna|uhc> --cap-mb 0

# Offline unit tests (no network)
uv run --with "ijson>=3.3" --with pytest python -m pytest pipelines/tic_mrf/test_reverse_map.py -q

# Production — deploy FIRST, then spawn-fire (the fan-out driver runs server-side)
modal deploy pipelines/tic_mrf/orchestrate.py
modal run pipelines/tic_mrf/orchestrate.py::build_worklist --payer uhc
modal run pipelines/tic_mrf/orchestrate.py::bridge --payer uhc
modal run pipelines/tic_mrf/orchestrate.py::run --payer uhc --cohort-key active/tic_cohort/ny_ortho.json
#   ^ prints FUNCTION_CALL_ID; follow with `modal app logs tic-mrf-pipelines`
modal run pipelines/tic_mrf/orchestrate.py::rebuild_indexes
```
