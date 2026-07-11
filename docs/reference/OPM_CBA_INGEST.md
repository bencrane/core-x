# OPM CBA Database — Ingest Record

**Status:** landed 2026-07-11 · **Repo:** `core-x` · **Module:** `pipelines/opm/opm_cba.py`
Sibling of the private-sector `olms_cba_*` corpus; complements `naf_wage_rates` via the NAF slice.
Companion to `LABOR_MARKET_SUBSTRATE.md` and `LABOR_x_GOVCON_CROSSWALK_GTM.md`.

## What this is

The EO-13836 **Collective Bargaining Agreements** collection OPM is statutorily required to
publish — every **federal-sector** agency⇄union CBA — at
`opm.gov/policy-data-oversight/labor-relations/collective-bargaining-agreements/`.

**1,248 documents · 45 agencies with published CBAs · 98 distinct unions · all `documentType=1`.**
(The 83 entries in the agency filter dropdown are the full universe; only 45 have documents.)
Department of Defense dominates: **889 of 1,248** (Army/Air Force/Navy + National Guard Bureau),
then Interior 81, Agriculture 59, Commerce 32, Treasury 27, Transportation 27.

**Value:** the **NAF slice** (nonappropriated-fund instrumentalities — exchanges/MWR/commissary/
billeting) carries negotiated wage appendices that complement `naf_wage_rates`. This corpus does
**NOT** serve the SAM §4(c) service-contractor CBA join — those are private contractor CBAs and
live on OLMS (`olms_cba_*`); see `OLMS_CBA_POINTER_JOIN_MEASUREMENT.md`.

## API recon (verified live, residential IP, 2026-07-11)

- **Endpoint:** `POST https://www.opm.gov/cba/api/documents/published`,
  `Content-Type: application/json; charset=utf-8`, key-less, cookie-less, CSRF-less.
- **Payload (EXACT):**
  ```json
  {"sortBy":"agencynameAsc","agencyIds":[],"subAgencyNames":[],"activityOfficeRegions":[],
   "laborUnionNames":[],"locals":[],"busCodes":[],"currentPage":1,"recsPerPage":20,"searchString":""}
  ```
- **⚠ THE TRAP:** `sortBy` MUST be the exact UI casing `agencynameAsc`. An invalid value silently
  poisons ASP.NET model binding — the server returns **200 but ignores `currentPage`/filters** and
  serves page 1 of everything. Verified-correct payload yields `p1≠p2`, `rowCount=1248`,
  `pageCount=63` at `recsPerPage=20` (server caps `recsPerPage` at ~20).
- **Envelope:** `{results:[...], currentPage, pageCount, pageSize, rowCount, firstRowOnPage, lastRowOnPage}`.
- **Per-record:** `id`(UUID), `documentType`, `agencyName`, `subAgencyOrComponent`,
  `activityOfficeRegion`, `laborUnionName`, `local`, `busCodes[]`, `expirationDate`(ISO),
  `fileUrl`(direct public PDF), `fileName`, `fileSize`("1.76 MB"). `highlights` is a
  search-relevance artifact — dropped.
- **`GET /cba/api/agencies`** — open (83 agencies, id/name/emailDomains).
- **`POST /cba/api/documents/export`** — issues a token but the `export/{token}` GET **503s
  permanently** (server-side broken). Not used.
- **`fileUrl`** is `https://www.opm.gov/cba/api/documents/{id}/attachments/{fileName}` — the **same
  `www.opm.gov` WAF host**, not a separate CDN. Directly fetchable, no auth. Space chars in the
  filename must be percent-encoded (`requote_uri`).
- **WAF:** challenges sustained CLI bursts with non-JSON interstitials. Mitigated by browser-shaped
  headers (UA/Origin/Referer), polite pacing (0.25s inter-page; 6 workers / 0.1s on blobs), and
  exponential backoff on 403/429/503 + non-JSON. **Never tripped** on this run: 63 catalog POSTs
  and 1,248 blob GETs completed with zero retries.

## Pipeline (`pipelines/opm/opm_cba.py`, modeled on `pipelines/dol/olms_opdr_cba.py`)

**Stage 1 `--index`** — paginate all 63 pages, dedupe on `id`, **fail closed** if distinct
`id` count ≠ live `rowCount`, overwrite-write `opm_cba_index`. Idempotent, re-runnable.

**Stage 2 `--documents --resume`** — GET each `fileUrl` → raw bytes verbatim to
`active/opm_cba_blobs/{id}.{ext}` + `opm_cba_documents` manifest. Resume-safe append (skips
non-retry statuses). A `200 text/html` body is treated as a WAF interstitial (retried, never
stored). A per-doc 4xx is recorded as terminal `http_NNN`, not fatal.

Run:
```
doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with requests \
  --with boto3 --with truststore --with 'psycopg[binary]' \
  python -m pipelines.opm.opm_cba --index
... python -m pipelines.opm.opm_cba --documents --resume
```

## Datasets landed (Gen-3 Lance SoR + R2 blobs)

| dataset | rows | indexes |
|---|--:|---|
| `s3://data-sink/active/opm_cba_index/` | 1,248 | BTREE `id`,`agency_name`; BITMAP `labor_union_name`,`document_type` |
| `s3://data-sink/active/opm_cba_documents/` | 1,248 | BTREE `id`,`sha256`; BITMAP `content_type`,`fetch_status`,`document_type` |
| `s3://data-sink/active/opm_cba_blobs/{id}.pdf` | 1,243 objects (~1.78 GB) | — |

`opm_cba_index` also carries `file_size` (verbatim human string) + `file_size_bytes` (derived) and
`bus_codes` (list<string>, present on all 1,248). `id` joins index↔documents↔blobs.
Ledger: `ops.opm_cba_runs` (auto-surfaced by `scripts/data-factory-catalog.py`; no manual registration).

## Coverage & known gaps

- **1,243 / 1,248 blobs fetched.** The **5 misses are all DoD / National Guard Bureau** state-guard
  agreements whose `fileUrl` **404s on OPM's server** (the index lists them; the attachment endpoint
  returns 404). Recorded as terminal `fetch_status='http_404'` — a `--resume` will not retry them.
  This is an upstream OPM gap, not a crawl failure.
- Server returns attachments as `application/octet-stream`; `file_ext` resolves to `pdf` from the
  filename (all 1,243 blobs are verified `%PDF-`). The manifest records the honest upstream
  `content_type`.

## NAF slice (build-plan step 4)

The NAF-instrumentality subset **is present and non-trivial** — concentrated in **DoD** (Army/Air
Force NAF exchange & MWR CBAs, filenames literally carrying `NAF`). The earlier session's "0 NAF
hits" was an artifact of the poisoned 20-row sample, not the corpus. Wage-appendix extraction on
this DoD NAF slice (to complement `naf_wage_rates`) is a **scoped downstream parse job**, separate
from landing this SoR. Identify the slice by filtering `opm_cba_index` on
`agency_name='Department of Defense'` + filename/subagency NAF/exchange/MWR/commissary tokens
(broad keyword regex over-matches — e.g. "exchange" hits *Securities and Exchange Commission* — so
gate on the DoD agency).
