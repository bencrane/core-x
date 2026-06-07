# Prime Award → SAM.gov Attachment Bridge — Feasibility Diagnostic

**Read-only.** No pipeline built, no substrate harvested, no PDFs downloaded.
Scopes the drop-off when translating the 90-day API-fresh prime-award feed into
actionable SAM.gov Notice IDs and downloadable PWS/SOW substrate. Executed
2026-06-07 with `pylance 7 / duckdb 1.5` over `core-x/prd` R2 creds for the Lance
reads, and live read-only GETs against SAM.gov for the API probes.

---

## 0. Verdict (read first)

The bridge is **structurally gated at the front, not the back**. Of recent prime
awards, only **17.4 %** even carry a solicitation identifier; of distinct
solicitation numbers, only **~34 %** resolve to a SAM.gov notice; but once
resolved, **~90 %** carry downloadable attachments. End-to-end, **~5.3 % of recent
prime awards (~66 K of 1.23 M)** terminate in a downloadable SAM.gov attachment
set.

The translation step needs **no live API at all** — an offline Lance join against
the SAM opportunities bulk we already hold (`sam-gov-opps`, 2.89 M rows)
reproduces the live hit rate to within 2 solnums (34.0 % vs 34.4 %). The only
unavoidable live-API stage is the per-notice attachment manifest.

---

## 1. The funnel

Source table: `s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/`
(verbatim `Contracts_PrimeTransactions`, 297 cols, txn grain, `last_modified_date`
90-day window). `solicitation_identifier` **is present** on this grain.

| Stage | Population | Measure | Result |
|---|---|---|---:|
| **0 — Universe** | API-fresh prime feed | txn rows | 1,406,045 |
| | | **distinct prime awards** (`contract_award_unique_key`) | **1,229,191** |
| **1 — Sol# available** | distinct awards | awards w/ non-null `solicitation_identifier` | 213,373 |
| | | **award-grain Sol# fill rate** | **17.36 %** |
| | | distinct solicitation_identifiers | 148,426 |
| | | (row-grain fill) | 21.49 % |
| **2 — Translation** | random 500 of the 148,426 distinct solnums (seed 42) | resolve to ≥1 `notice_id` (live frontend) | 172 |
| | | **live hit rate** | **34.40 %** |
| | | offline Lance-join hit rate (corroboration) | 34.00 % |
| | | distinct notice_ids produced | 279 (1.62 / solnum) |
| **3 — Substrate** | 279 resolved notices | notices w/ ≥1 downloadable attachment | 221 |
| | | **per-notice attachment yield** | **79.21 %** |
| | resolved solnums | **solnums w/ substrate (any notice)** | **154 / 172 = 89.53 %** |
| | | access-gated (non-public) attachments | 0 |

### Compounded end-to-end

`0.1736 × 0.344 × 0.8953 = 0.0535`

| Grain | Reachable to downloadable substrate |
|---|---:|
| **Prime awards** | ~5.35 % → **≈ 65,700** of 1,229,191 (approx; assumes resolving solnums carry a representative award share) |
| Distinct solnums | 34.4 % resolve → ≈ 51,000; × 89.5 % substrate → **≈ 45,700** solnums |
| Distinct notices | ≈ 82,800 notices; × 79.2 % → **≈ 65,500** notices carrying attachments |

The dominant loss is **Stage 1** (82.6 % of awards have no Sol#) followed by
**Stage 2** (65.6 % of solnums never reach a SAM notice). Stage 3 is nearly free.

---

## 2. Why the front of the funnel is so lossy

The miss set is **genuine absence, not a method artifact**. The feed is dominated
by Defense Logistics Agency simplified-acquisition / FSS call orders (`SPE*`), GSA
schedule buys (`47P*`), and other sole-source / SAT actions that never had a
competed SAM.gov Contract Opportunity notice. Independent confirmation:

- **Recall guard:** 5 frontend-0-hit solnums re-queried against the api.sam.gov
  *exact* `solnum` filter (window derived from each award's own `action_date`,
  ±1 yr) — **0/5 found** (the 6th–8th were quota-blocked). Frontend keyword recall
  is validated within the window: misses are real.
- **Precision:** only 3/500 returned a keyword hit with no exact solnum match
  (`solicitationNumber` normalized) — search noise is negligible.

---

## 3. Multiplicity — one solnum, many notices

**52.9 % of resolved solnums (91/172) return multiple notice_ids.** They are the
lifecycle stages of one procurement, exactly as hypothesized:

| Notices per solnum | solnums |
|---:|---:|
| 1 | 81 |
| 2 | 78 |
| 3 | 10 |
| 4 | 3 |

| Notice type (across 279 resolved notices) | count |
|---|---:|
| Award Notice | 91 |
| Combined Synopsis/Solicitation | 77 |
| Solicitation | 46 |
| Presolicitation | 35 |
| Special Notice | 17 |
| Sources Sought | 9 |
| Justification | 4 |

**Pattern:** a solnum typically pairs an **Award Notice** with its originating
**Combined Synopsis/Solicitation** or **Solicitation** (and often a
**Presolicitation** / **Sources Sought**). The PWS/SOW substrate lives on the
**solicitation-bearing** notice — the Award Notice and Sources Sought notices are
the bulk of the **58/279 zero-attachment** notices. A bridge must therefore pick
the solicitation-type notice per solnum, not the award notice, for substrate.

---

## 4. Structural anomalies encountered

1. **api.sam.gov quota wall (disqualifying for sweeps).** The developer gateway
   `GET https://api.sam.gov/opportunities/v2/search` returns after ~5 calls:
   `429 {"code":"900804","message":"Message throttled out","description":"You have
   exceeded your quota. You can access API after 2026-Jun-08 00:00 UTC"}`. Role is
   `SI-NONFED` (~5–10 req/day, resets at UTC midnight). A 500-solnum sweep through
   this gate is infeasible (~50–100 days). **This is why the unauthenticated
   frontend index is the correct path** — consistent with the existing
   `sam_attachment_manifest.py` architecture decision.

2. **api.sam.gov ≤1-year date window.** The gateway *requires* `postedFrom` /
   `postedTo` and rejects ranges >1 yr (`400 "Date range must be null year(s)
   apart"`). Even with quota, you cannot look a solnum up without already knowing
   its posting year ±1 yr — a non-starter for a blind bridge.

3. **Frontend endpoint idiom.** `GET https://sam.gov/api/prod/sgs/v1/search/?index=opp&q=<solnum>&size=100&is_active=false`
   — no api_key, no quota. Serves `application/hal+json` (a strict
   `Accept: application/json` 406s; send `application/json, text/plain, */*`).
   **`notice_id` is the `_id` field.** Returns `_embedded.results[]`;
   `solicitationNumber` and `type.value` per record. `_links` is empty (no
   self-href).

4. **Solnum formatting drift.** Match requires alphanumeric normalization
   (`upper(); strip [^A-Z0-9]`) — FPDS `solicitation_identifier` and SAM
   `solicitationNumber` differ on dashes/spacing. Without it the join under-counts.

5. **`opps/v2/opportunities/search` frontend endpoint is auth-gated** (401) — only
   the `sgs/v1/search` index is open.

6. **`size_bytes` is a lower bound, not true size** (pre-existing defect, per
   `sam_attachment_manifest.py`): SAM returns `((true-1) mod 10 MB)+1` for files
   ≥10 MB. Not measured here (no byte reads), but any substrate-budget projection
   must enforce real size at fetch via `Content-Length`, never trust the manifest
   `size`.

---

## 5. Architecture implication

**Translation = offline Lance join, not live API.** The live per-solnum probe
(34.4 %) is reproduced by a normalized DuckDB join of `contract_prime_txn.
solicitation_identifier` against `sam-gov-opps` (active ∪ archived) at 34.0 %
(170/500), with *more* multiplicity recovered (111 vs 91 multi-notice solnums) —
the archived corpus is more complete on lifecycle notices. Recommended bridge:

1. **Materialize the bridge offline** — DuckDB join → Lance:
   `(contract_award_unique_key, solicitation_identifier, notice_id, notice_type,
   posted_date)`, BTREE on `solicitation_identifier` + `notice_id`. No live calls.
2. **Pick the solicitation-bearing notice** per solnum
   (`Combined Synopsis/Solicitation` > `Solicitation` > `Presolicitation`), not the
   Award Notice — that is where SOW/PWS attachments are.
3. **Probe the live resources endpoint only for the resolved-notice subset**
   (`sam.gov/api/prod/opps/v3/opportunities/{notice_id}/resources`, ~82 K notices),
   harvesting the attachment manifest — this is the one stage that must be live.
   Run single-threaded from a residential IP (datacenter egress is 429'd).

This keeps the unavoidable live-API surface off the quota'd developer gateway and
confines it to a single, idempotent, resumable stage.

---

## 6. Reproducibility

Read-only throughout. Sample: 500 distinct non-null `solicitation_identifier`,
DuckDB `USING SAMPLE 500 ROWS (reservoir, 42)` over 148,426 distinct solnums.
Translation: `sam.gov/api/prod/sgs/v1/search` (frontend, no key), normalized
exact-match on `_id`. Substrate: `opps/v3/opportunities/{nid}/resources`,
`resourceId` presence only (no byte download). Recall guard:
`api.sam.gov/opportunities/v2/search?solnum=…` (5 calls before quota). Cross-check:
join vs `s3://data-sink/sam-gov-opps/{active,archived}/`. R2 creds from `core-x/prd`.
