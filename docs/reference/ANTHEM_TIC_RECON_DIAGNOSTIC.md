# Anthem TiC — Reconnaissance & Scale Diagnostic

**Mode:** read-only · public data only · 127 S3 GETs · 6.7s wall
**Run (UTC):** 2026-06-07T15:24:07+00:00
**Anthem data path:** `https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/{EIN9}.json` (public S3 · no auth · no bot wall)
**Form 5500 plane:** `/Users/benjamincrane/core-x-lake/active` (local LanceDB)

## Part 1 — API Profile & Payload Blueprint

### Base contract

| Field | Value |
|---|---|
| Host (primary) | `antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com` |
| Host (failover) | `antm-pt-prod-dataz-nogbd-nophi-us-east2.s3.us-east-2.amazonaws.com` |
| Method | `GET` (plain HTTP; no POST, no body, no token) |
| Per-EIN key | `anthem/{EIN9}.json`  ·  `EIN9` = the 9-digit EIN, dash stripped |
| Name→EIN dir | `namesearch/{a-z}.json` + `namesearch/others.json` (27 files) |
| Master ToC | `anthem/YYYY-MM-01_anthem_index.json.gz` |
| Health probe | `status.json` → `{"status":"true"}` |
| Auth | **none** — anonymous public-read S3 object |
| Miss code | **403** (bucket has no public `ListBucket` → AccessDenied for absent key) or 404 = EIN not in Anthem's book; Anthem's own JS treats 403≡404≡"0 results" |

**Per-EIN payload** = 4 arrays of `{url, displayname}` over a multi-host fan-out: In-Network & Out-of-Network links resolve back to the **same S3 bucket** (`anthem/*.json.gz`); BCBS-Out-of-Area links point to **`*.mrf.bcbs.com`** with **CloudFront-signed** URLs (`Expires`/`Signature`/`Key-Pair-Id` — time-limited, fetch promptly); Carelon Behavioral Health is a 4th array (often empty).

### Anti-automation audit (empirical)

- `www.anthem.com` shell is behind **Akamai Bot Manager** (`_abck`/`bm_sz` cookies, obfuscated sensor script) + Akamai mPulse RUM. **Irrelevant to the data path.**
- The data path is **raw S3** (`Server: AmazonS3`). Live test: identical object served `200` to a browser UA and `206` (range honored) to a `python-requests/2.31.0` UA — **no UA filtering, no cookies, no JS challenge, no token.**
- **Verdict: plain `httpx`/`aiohttp`/`requests` with any UA. A headless browser (Playwright) is contraindicated** — it only re-introduces the Akamai surface the data path avoids, at 50–100× the cost per lookup.

### Sample response (real hit, truncated to 2 links/category)

```json
{
  "lastupdated": "2026-06-01",
  "In-Network Negotiated Rates Files": [
    {
      "url": "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/CA_ELHOMEDELHO.json.gz",
      "displayname": "CA_ELHO_ELHS_ELHOMEDELHO.json.gz"
    },
    {
      "url": "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/CA_ELRKMEDELRK.json.gz",
      "displayname": "CA_ENTPRS_DIABETS_ELRKMEDELRK.json.gz"
    },
    "\u2026 +102 more"
  ],
  "Out-of-Network Allowed Amounts Files": [
    {
      "url": "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2026-06-01_anthem-232259884_VERIZON-WIRELINE_allowed-amounts.json.gz",
      "displayname": "2026-06-01_anthem-232259884_VERIZON-WIRELINE_allowed-amounts.json.gz"
    },
    {
      "url": "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2026-06-01_anthem-232259884_VERIZON-ASSOCIATES-NY-NE_allowed-amounts.json.gz",
      "displayname": "2026-06-01_anthem-232259884_VERIZON-ASSOCIATES-NY-NE_allowed-amounts.json.gz"
    },
    "\u2026 +1 more"
  ],
  "Blue Cross Blue Shield Association Out-of-Area Rates Files": [
    {
      "url": "https://anthembcbsin.mrf.bcbs.com/2026-06_020_02I0_in-network-rates_1_of_2.json.gz?&Expires=1784642480&Signature=pRQrilP4LpiLZZ4y-rNK9IuvODU28BSWU4CW1CWI85jVUXR2G1z-27zw0Tw7PoIg6-fBAijOpQfSWzmeLYFeTkr2t-o1uKPknG0hKMmgmDutlwt4-iegyjhE2xoxfivkFSuLUnsH8tm2hwtrl6dDcs2MMeTTLIYY6uYhDydGEZtR3wmEAp6zxdFHWW-S3VAeqIUYKa2ENVLyzv55S6LANbGftIma-jzX8YwwBqpQ7kiKTx5AjYbVv7p2t09P-2oQQXtDiA5~uYmFoa11xexkSxNryP5wdhnNOhykWGUqncnmeFa75AD4D~azn~CSdrLsqX1ILSdaRBwgT8Q5dTVJsA__&Key-Pair-Id=K27TQMT39R1C8A",
      "displayname": "2026-06_AR_02I0_in-network-rates_1_of_2.json.gz"
    },
    {
      "url": "https://anthembcbsin.mrf.bcbs.com/2026-06_020_02I0_in-network-rates_2_of_2.json.gz?&Expires=1784642480&Signature=a7dWVEaqedaCqUnAFklTG5Ok4NJZ3obWIUGxkBvJcv~oYRupawGJvHQTQjh35GKlq7EHdmPGRkYkNIWL7Wty6IFsb9iDZFll5RueIsMVa11arq2SajLOrnmGILUeSPSBRwtRb8gPQ7vCR-pJoCSKzvQoC9LYpsSS8eABXZpod2IjfVqQIroKfUNGjhyQADTBKLSsanoizmy7a6y6-eql0NY2BUXgCW0GGqgidDLfxJpWVdjKDPOa1TmwdsnxUFQco5lKru9tSxPOBa5DFyLd0mBwKrE327gBJ-CXmyC-9MtHNw9hzoM~Pcnq~HdGaJU3koRzVToasmokhME3WiCNPA__&Key-Pair-Id=K27TQMT39R1C8A",
      "displayname": "2026-06_AR_02I0_in-network-rates_2_of_2.json.gz"
    },
    "\u2026 +228 more"
  ],
  "Carelon Behavioral Health Rates Files": []
}
```

## Part 2 — Controlled Sample Test (live)

Cohort: **Tier A** = top 50 national employers by participant count · **Tier B** = 50 mid-market in **CA** (100–2500 participants).

| Cohort | Probed | HTTP 200 (hit) | HTTP 404 (miss) | Error | Hit rate |
|---|--:|--:|--:|--:|--:|
| Tier A (national) | 50 | 8 | 42 | 0 | 16.00% |
| Tier B (CA mid) | 50 | 9 | 41 | 0 | 18.00% |
| **Combined** | **100** | **17** | **83** | **0** | **17.00%** |

## Part 3 — Scale, Filtration & Compute Forecast

### 3.1 Hit-rate deficit — EXACT (whole local corpus, not sampled)

Set-intersection of the entire local Form 5500 EIN universe against Anthem's complete `namesearch` book of business.

| Universe | Distinct EINs | ∩ Anthem | Coverage |
|---|--:|--:|--:|
| Anthem book of business (namesearch) | 149,338 | — | — |
| Form 5500 — `main` | 16,482 | 968 | 5.87% |
| Form 5500 — `sf` | 193,093 | 13,670 | 7.08% |
| **Form 5500 — union** | **209,210** | **14,605** | **6.98%** |

> Live-probe hit rate (17.00% on 100 EINs) vs. exact corpus coverage (6.98%) — the cohort is participant/geo-skewed, so the exact figure is the planning number.

### 3.2 File-volume profile (per covered employer)

| Metric | Value |
|---|--:|
| In-Network files / hit — p50 | 66 |
| In-Network files / hit — p95 | 114 |
| In-Network files / hit — max | 115 |
| All-category files / hit — p50 | 259 |
| All-category files / hit — p95 | 463 |
| All-category files / hit — max | 464 |
| Pointer-JSON bytes / hit — p50 | 140,215 |
| Mean all-category files / hit | 291.1 |

> ⚠️ De-dup to ROOT files: Anthem emits many URL-parameter variants pointing at the same object (vendor-reported ~1k root vs 10k+ raw). Dedupe on the object path before counting/downloading. The **per-EIN file is a tiny pointer doc**; the referenced in-network rate files are the multi-GB payloads.

### 3.3 Compute footprint (measured per-EIN latency = lookup cost)

| Metric | Value |
|---|--:|
| Per-lookup latency p50 | 143 ms |
| Per-lookup latency p95 | 398 ms |
| Per-lookup latency max | 4246 ms |
| Mean | 209 ms |

Throttling: the data path is S3. S3 does **not** CAPTCHA; it returns `503 SlowDown` only above ~3,500 GET/s **per prefix**. Sharding the `anthem/` keyspace across many prefixes is not available to us (single prefix), but a single prefix sustains thousands of GET/s — the EIN-resolution phase is **not** the bottleneck.

#### Pointer-fetch cost — GET `anthem/{ein}.json` for HITS only (misses pruned free via namesearch · measured p50 143 ms/GET)

| EIN universe | Total EINs | Hits @ exact rate | Pointer GETs | Wall @16 | @64 | @256 |
|---|--:|--:|--:|--:|--:|--:|
| Local Form 5500 union | 209,210 | 14,605 | 14,605 | 2.2 min | 0.5 min | 0.1 min |
| National Form 5500 (~800k assumed) | 800,000 | 55,848 | 55,848 | 8.3 min | 2.1 min | 0.5 min |

> Serverless cost of the resolution phase is **negligible** (it is a few CPU-minutes of concurrent HTTP + JSON parse; egress is pointer-JSON only, ~hundreds of bytes/hit). The real spend is **download + parse of the referenced multi-GB in-network rate files** for covered employers — that is a separate, storage/egress-bound phase sized off the actual `url` targets, not the EIN scan.

### 3.4 Triage strategy — prune BEFORE scraping

**The `namesearch` set IS the triage.** It is the exact, authoritative roster of Anthem-covered EINs (149,338). Production resolution = intersect the target EIN list against it (a set op over 7.5 MiB) → scrape only true hits, issue **zero** miss-bound requests. You do **not** process every Form 5500 EIN.

**Carrier-field cross-check is not yet computable locally.** The landed Schedule A table `form5500_sch_a_broker` is EFAST2 **`F_SCH_A_PART1` (broker / commission detail)** — it carries `INS_BROKER_NAME` but **not** insurer identity. The carrier name/EIN (`INS_CARRIER_NAME`, `INS_CARRIER_EIN`) lives in the Schedule A **header** `F_SCH_A`, absent from the current ingest STEMS. One-line fix: add `F_SCH_A` to land carrier identity and enable a carrier-share prune as a second signal alongside namesearch.

Top brokers in `form5500_sch_a_broker.lance` (34,358 rows) — intermediaries, not the payer network, so broker name is a **weak** triage key (shown for transparency):

| Broker (normalized) | Rows |
|---|--:|
| MARSH & MCLENNAN AGENCY LLC | 1,387 |
| USI INSURANCE SERVICES LLC | 1,273 |
| GALLAGHER BENEFIT SERVICES, INC. | 1,195 |
| LOCKTON COMPANIES, LLC | 392 |
| MERCER HEALTH AND BENEFITS, LLC | 386 |
| HUB INTERNATIONAL MIDWEST LIMITED | 337 |
| ALLIANT INSURANCE SERVICES, INC. | 328 |
| EDGEWOOD PARTNERS INSURANCE CENTER | 233 |
| IMA, INC. | 212 |
| MARSH & MCLENNAN AGENCY, LLC | 211 |

### 3.5 Enumeration economics

Anthem's complete book of business is **149,338 EINs** across 27 `namesearch` files totaling **7.5 MiB** — pulled here in the run. **This obviates per-EIN probing of misses entirely**: download the 27 files once, build the covered-EIN set, and intersect against any target list. You never issue a miss-bound request. The production resolution step is a set-intersection over ~7.5 MiB, not millions of HTTP calls.

## Method

- Anthem universe: all 27 `namesearch/*.json` files, EINs normalized to 9-digit.
- Hit rate: exact set-intersection (no sampling) for the corpus figure; 100-EIN live GET cohort for latency/file-count/payload telemetry.
- EIN join is byte-clean: Form 5500 keys are zero-padded VARCHAR, identical to Anthem's 9-digit `ein`.
- Read-only: no SoR mutation; sole write is this report.

