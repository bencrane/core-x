# TiC Payer Integration & Reverse-Mapping POC — Aetna & UHC

**Scope.** Reverse-engineer Aetna and UnitedHealthcare (UHC) Transparency-in-Coverage
(TiC) machine-readable files (MRFs); bridge the local `practice_group_360` SoR
(commit `e2b479c`) to commercial negotiated rates; establish the out-of-core
production pipeline. All numbers below are **measured on live payer infrastructure
on 2026-06-07**, not estimated, unless explicitly labeled *(extrapolated)*. Every
hard claim was adversarially re-verified by an independent agent.

**Implementation.** `pipelines/tic_mrf/` (Python — this is the core-x compute
plane; the directive's "TypeScript / JSONStream / oboe.js" stack does not apply,
the canonical streaming equivalent here is `ijson` + DuckDB → Lance):
- `reverse_map.py` — out-of-core two-source streaming reverse-mapper (the engine).
- `orchestrate.py` — Modal production fan-out (worklist → reverse-map → index), blast-radius isolated.
- `part1_filter_spine.py` — deterministic cohort extractor from the local SoR.
- `ops_tic_reverse_map_runs.sql` — idempotency / terminal-state ledger.

---

## 0. Three directive premises corrected against reality

| # | Directive premise | Reality (proven) | Where |
|---|---|---|---|
| 1 | "Type 2 NPI (the group billing identifier)" | **No Type-2/org NPI, EIN, or TIN exists anywhere in the SoR.** The canonical group billing anchor is the PECOS `group_enrlmt_id`. The Type-1 array ships natively as `practice_group_360.member_npis`. | live schema |
| 2 | Aetna Module A: "Scan the [ToC] index for string matches against your NPI array" | **TiC ToC files contain zero provider NPIs** (TiC v2.0.0: ToC maps `reporting_plans → in_network_files[].location` only). NPI matching is structurally impossible at the ToC layer; it requires descending into an in-network file and resolving `provider_references`. | Aetna + UHC ToC, verified |
| 3 | UHC Module B: "reconstruct the filename deterministically … bypass index crawling … fire direct GETs" | **0/15 blind-GETs succeed (all HTTP 409)** — *even when the reconstructed filename is byte-for-byte correct* — because `mrfstore.uhc.com` is SAS-token gated. Master-index string-match: **7/15**. The filename is also non-reconstructable (mixed-case, `&`→`--`, divergent legal names). | UHC, empirically tested |

The pipeline implements the **correct** inversion for each payer (below).

---

## Part 1 — The Filter Spine (local context extraction)

Pulled from the local SoR **before** any external request, so the (expensive)
MRF streams only ever search for a tiny fixed cohort.

**Geography = NY · Specialty = Orthopedic surgery (NUCC `207X00000X`).**

Selection against `practice_group_360` (253,740 groups, 1 fragment): bitmap
pushdown `group_state='NY'`, residual `top_specialty='207X00000X'`,
`member_count BETWEEN 2 AND 9` (**multi-provider + fan-in ≤ 9** — "fan-in" is the
count of distinct providers reassigning Medicare billing into the group,
`materialize.py:PRACTICE_AGG`), `independent_member_count = member_count`
(**dual-pole independent** — every member's *smallest* pole is this group, i.e. a
real independent practice, not a consolidator's billing arm), `distinct_specialties ≤ 2`.

### A) 5 target independent groups — 15 Type-1 NPIs

| Org (string-match handle) | PECOS `group_enrlmt_id` (billing anchor) | Members | Medicare $ | Type-1 NPIs |
|---|---|---|---|---|
| MANHATTAN ORTHOPEDIC & SPORTS MEDICINE GROUP | `O20060215000749` | 5 | $11,254,134 | 1013024801, 1518075662, 1386684314, 1972610798, 1457587941 |
| SOUTHERN WESTCHESTER ORTHOPEDICS & SPORTS | `O20141028001847` | 3 | $7,078,487 | 1245219856, 1891924064, 1942289541 |
| ROSS ORTHOPEDIC GROUP PC | `O20070822001001` | 2 | $4,797,645 | 1295708600, 1720020845 |
| MICHAEL L. PARKS, M.D, PLLC | `O20110930000152` | 2 | $2,971,208 | 1710732920, 1992762587 |
| STATEN ISLAND ORTHOPEDICS & SPORTS MEDICINE | `O20040312000535` | 3 | $2,917,978 | 1760445001, 1407819741, 1316900657 |

> `type2_npi` = **(NOT IN SUBSTRATE — PECOS `group_enrlmt_id` is the billing anchor)**.

### B) UHC seed — top NY self-funded health employers (head of 50)

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

**Endpoint topology (discovered by intercepting the SPA's XHR; the `#/` route is a
HealthSparq/Sapphire Ember SPA shell on AWS CloudFront+S3):**
- **Portal/API:** `health1.aetna.com` — HealthSparq backend is **session-gated**
  (HTTP **440** until the `GET /healthsparq/service/public/login?...` handshake
  sets a trace-session cookie). Not needed for the data plane.
- **Data plane (public, no auth):** `https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/<brandCode>/` — BunnyCDN edge → Google Cloud Storage origin.
- **Master manifest:** `…/AETNACVS_I/ALICSI/latest_metadata.json` = **6,971,311 B**, enumerates **12,077 files** (2,019 `TABLE_OF_CONTENTS`, **9,410 `IN_NETWORK_RATES`**, 648 `ALLOWED_AMOUNTS`) with `{reportingPlans, fileSchema, fileName, filePath}`. Plus `deep_link_map.json` (3,115,578 B).
- **⚠ Trap avoided:** the JS bundle still contains a hardcoded `s3.us-west-2.amazonaws.com/prd2-directory-generation-service-machine-readable-json/<brand>/index.json` template — a **stale/dead code path that returns 403 AccessDenied on every key**. Do not build on it.

**Correct flow:** `latest_metadata.json` → filter `fileSchema=IN_NETWORK_RATES`
→ stream each in-network file → resolve `provider_references` → match cohort.
(The per-plan ToC files exist but are redundant given the master manifest.)

### Module B — The UHC Inversion (employer-first) + the refuted shortcut

**Endpoint topology:** Azure App Service behind Azure Front Door
(`*.azurewebsites.net`, `x-azure-ref`, `x-cache`). Blobs on `mrfstore.uhc.com`,
**SAS-token gated**.
- **Master index:** `GET https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/`
  = **31,299,180 B** (identity-encoded, *not* gzipped), **86,514 entries**:
  66,912 `_index.json` (employer ToCs), **7,170 `in-network-rates`**, 12,428 `allowed-amounts`.
  Each entry = `{name, downloadUrl, size}` where `downloadUrl` carries a
  **container-scoped SAS token** (identical `sig` across all entries).

**Empirical test of the directive's "deterministic filename → blind GET" premise:**

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
even under case-insensitive normalization). **Correct flow:** fetch master index
once → string-match employer → use the exact `downloadUrl` (filename
reconstruction is both unnecessary and unreliable). Note: 66,912 employer ToCs
dereference to only **7,170 distinct in-network files** — the production worklist
must dedup by URL (≈ 9 employers per shared network file).

---

## Part 3.1 — Unified schema blueprints (verbatim, from live files)

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

---

## Part 3.2 — Engineering & scale metrics (measured)

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

Engine = `reverse_map.py`, full two-pass reverse-map over the network, cohort = 15 NPIs:

| Run | Bytes moved (2 passes) | Uncompressed processed | Wall-ms | Peak RSS | Throughput | Matches |
|---|---|---|---|---|---|---|
| Aetna in-network (full) | 79,496,556 B | 1,911,110,682 B | 24,492 ms | **353.8 MB** | 74.4 MB/s | 0 (cohort not in this plan slice) |
| UHC in-network (full) | 19,806,450 B | 355,087,896 B | 4,574 ms | **344.2 MB** | 74.0 MB/s | 0 (cohort not in this plan slice) |
| **UHC positive control** (3 NPIs known present) | 19,806,450 B | 355,087,896 B | 3,945 ms | 453.8 MB | 85.9 MB/s | **165,729 rate rows** |

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

### Anti-bot reconnaissance

| | Aetna (data plane) | UHC (data plane) |
|---|---|---|
| Edge / origin | BunnyCDN → GCS | Azure Front Door → `mrfstore.uhc.com` (Azure Blob) |
| WAF / bot challenge | **None** on MRF blobs | **None** (no Cloudflare/Akamai, no CAPTCHA/JS challenge) |
| Auth on rate files | **None** (fully public) | **SAS token required** (409 `PublicAccessNotPermitted` without it; token is in the master index) |
| UA sensitivity | **None** (identical 200 for curl-UA and Chrome-UA) | **None** (200 with browser-UA, curl-UA, and empty UA) |
| Rate limiting | none observed | none observed (5 rapid HEADs → 200, no 429/Retry-After; not stress-tested) |
| Session gate | HealthSparq **API** returns 440 pre-login (irrelevant to blobs) | portal/API on `*.azurewebsites.net` (irrelevant to blobs) |

**Programmatic remediation (both implemented in `reverse_map.py`):** (1) browser-shaped
`User-Agent` (`TIC_USER_AGENT` override) — harmless here, defensive for CDN edge
rules; (2) **UHC: must fetch the master index to obtain the SAS-bearing
`downloadUrl`** — there is no auth-free path; (3) discover the Aetna data-plane URL
from `latest_metadata.json`, never the dead `prd2` S3 template; (4) HTTP HEAD →
`Content-Length`/`ETag` before download for the projection and the idempotency key;
(5) streaming with a compressed-byte cap for sampling.

---

## Part 3.3 — Production resource projection (403,179 candidate NPIs)

**Key property:** I/O is proportional to the **file universe streamed, not the
cohort size.** Searching 15 NPIs or 403,179 streams the *same* files; the cohort
only sizes the in-RAM match index. 403,179 NPIs as a Python `set` ≈ **~40 MB**;
the matched-`provider_group` map is bounded by groups containing ≥1 candidate
(≈ hundreds of MB). **The bounded-RAM out-of-core model holds unchanged at
nationwide scale** — a 4 GB worker suffices.

### Universe size (Aetna sizes HEAD-sampled n=45; UHC summed exactly from the index)

| Payer | In-network files | Compressed total | Ratio | Uncompressed total |
|---|---|---|---|---|
| Aetna | 9,410 | **19.33 TB** *(mean 1.91 GiB/file, extrapolated from n=45)* | 24.04× | 464.7 TB |
| UHC | 7,170 (distinct) | **88.94 TB** *(exact; median 13.7 GiB/file, max 21 GiB)* | 17.93× | 1,594.7 TB |
| **Combined** | **16,580** | **≈ 108.3 TB compressed** | — | **≈ 2.06 PB uncompressed** |

### Compute (Modal), at the measured **75 MB/s uncompressed per worker**

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

### Storage (Cloudflare R2, $0.015/GB-mo, zero egress) — **filter, don't mirror**

| Strategy | Monthly footprint | R2 cost/mo | Verdict |
|---|---|---|---|
| **Raw mirror** of all in-network files | 108.3 TB/snapshot (grows unbounded; 12-mo retention ≈ 1.3 PB) | ≈ **$1,624/snapshot** (~$19.5K/mo at 12-mo) | inert, wasteful |
| **Reverse-mapped fact table** (this pipeline) — only matched rate rows for the candidate universe, Lance + scalar indexes | **≈ 0.2–1.5 TB** *(resolves on first full run; ~1–15 B rows × ~50 B, dictionary/RLE-compressed)* | ≈ **$3–$23/mo** | queryable, indexed, **50–500× smaller** |

The reverse-mapped table is the SoR: `s3://data-sink/active/tic_negotiated_rates/`
(append-only Lance), BTREE `npi`/`billing_code`, BITMAP `payer`/`billing_class`/`billing_code_type`.

---

## Architecture decisions (defense)

1. **Reverse-map, don't mirror.** Streaming-filter the 2.06 PB universe to the
   candidate-NPI fact table (~sub-TB). 50–500× storage reduction; the output is
   indexed and queryable, the raw mirror is not.
2. **Bounded RAM, out-of-core.** `ijson` event streaming + gzip-member streaming;
   RAM = O(cohort + matched groups), proven ~350 MB peak over 1.9 GB payloads vs
   6.9 GB for the naive materialize. DuckDB cast→Lance uses `memory_limit` +
   local-NVMe `temp_directory` for spill on the append.
3. **Append-only SoR.** Matched rows append as new Lance fragments
   (`mode="append"`); the SoR is never rewritten in place.
4. **Blast-radius isolation (3 phases).** worklist (network-only) → reverse-map
   (per-file, fanned) → index rebuild (heavy external sort, isolated). A flaky
   multi-GB parse touches one fragment, never the index, never another file.
5. **Idempotency ledger.** `ops.tic_reverse_map_runs` keyed on
   `(payer, source_file_url, file_version=ETag)` — a file already ingested at the
   same version is skipped; safe to retry and to reshard a nationwide run.
6. **Dedup the worklist.** 66,912 UHC employer ToCs → 7,170 distinct in-network
   URLs; process distinct files, not per-employer.

## Reproduce

```bash
# Part 1 — filter spine (reads R2 creds via doppler)
doppler run -- uv run pipelines/tic_mrf/part1_filter_spine.py --state NY --specialty 207X00000X

# Part 2/3 — reverse-map one real file (public Aetna; UHC needs the SAS downloadUrl from the master index)
uv run pipelines/tic_mrf/reverse_map.py --innetwork "<in_network_url>" --npis <csv> --payer <aetna|uhc> --cap-mb 0

# Production — Modal fan-out (build_worklist is payer-aware: reads each master manifest, dedups URLs)
modal run pipelines/tic_mrf/orchestrate.py::build_worklist --payer uhc
modal run pipelines/tic_mrf/orchestrate.py::run --payer uhc --cohort-key tic_cohort/ny_ortho.json
modal run pipelines/tic_mrf/orchestrate.py::rebuild_indexes
```
