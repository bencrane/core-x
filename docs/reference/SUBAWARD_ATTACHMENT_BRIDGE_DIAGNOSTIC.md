# Subaward → SAM.gov Attachment Bridge — Feasibility Diagnostic

**Read-only.** No pipeline built, no substrate harvested, no PDFs downloaded.
Scopes the drop-off when translating the 90-day API-fresh **subaward** feed into
actionable SAM.gov Notice IDs and downloadable PWS/SOW substrate. Executed
2026-06-07 with `pylance 7 / duckdb 1.5` over `core-x/prd` R2 creds for the Lance
reads, and live read-only GETs against the SAM.gov frontend for the substrate probe.
Companion to `PRIME_AWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md` (prime grain).

---

## 0. Verdict (read first)

**The subaward grain carries no solicitation field at all.** The verbatim
`Contracts_Subawards` schema (118 cols) has zero `solicitation_identifier` — its
only link to the procurement is `prime_award_unique_key` / `prime_award_piid`. The
bridge is therefore necessarily **three hops, all but the last offline**:

```
subaward.prime_award_unique_key
   ⋈  transaction_search_fpds.generated_unique_award_id   → solicitation_identifier   (hop 1, offline)
   ⋈  sam-gov-opps.solicitation_number  (alnum-normalized) → notice_id                 (hop 2, offline)
   →  GET opps/v3/opportunities/{notice_id}/resources      → download_url              (hop 3, live)
```

Despite the extra hop, the subaward path is **structurally healthier at the front
than the prime path**: subawards live on large *competed* primes (subcontracting
plans attach to those, not to the DLA/FSS simplified buys that dominate the raw
prime feed). End-to-end, **~12.7 % of recent distinct subawards (~16.5 K of 130 K)**
terminate in a downloadable SAM.gov attachment set — **2.4× the prime path's
5.35 %**.

Translation needs **no live search API** — the offline Lance join against
`sam-gov-opps` reproduces the prime probe's live hit rate (the prime diagnostic
corroborated this within 2 solnums). The only unavoidable live stage is the
per-notice resources manifest: **836/836 probed notices returned HTTP 200, zero
throttling.**

---

## 1. The funnel (subaward grain)

Source: `s3://data-sink/active/usaspending_api_fresh/contract_subaward/` (verbatim
`Contracts_Subawards`, procurement only, `subaward_sam_report_last_modified_date`
90-day window). Note the window is on the **reporting frontier**, not action date:
`subaward_action_date` spans **2001 → 2026**, so the underlying primes are mostly
old — solnum recovery **must** use the full FPDS corpus
(`usaspending/transaction_search_fpds`, 107 M txn rows), not the 90-day fresh
prime feed (which would barely intersect them).

| Stage | Population | Measure | Result |
|---|---|---|---:|
| **0 — Universe** | API-fresh subaward feed | rows | 199,901 |
| | | **distinct subawards** (`prime_award_unique_key`+`subaward_number`) | **130,011** |
| | | distinct underlying prime awards | 6,347 |
| **1 — Sol# available** | distinct subawards | **direct** `solicitation_identifier` on grain | **0 — field absent** |
| | distinct primes | primes resolved in FPDS | 6,327 / 6,347 (99.7 %) |
| | | primes carrying a solnum | 2,862 / 6,347 = **45.09 %** |
| | distinct subawards | subawards w/ a **recoverable** solnum (prime hop) | 86,912 |
| | | **subaward-grain Sol# fill** | **66.85 %** |
| | | distinct solnums recovered | 2,419 |
| **2 — Translation** | 500-sample of clean solnums (seed 42) | resolve to ≥1 `notice_id` (offline) | 160 = **32.00 %** |
| | full pop (2,387 clean) | resolve | 696 = 29.16 % |
| | full pop (2,417 raw) | resolve | 725 = 30.00 % |
| | | **subaward-weighted** resolution (clean) | 22.16 % |
| | | notices / resolved solnum (clean) | 5.26 |
| **3 — Substrate** | 836 resolved notices (sample, live) | per-notice w/ `download_url` | 666 = **79.67 %** |
| | 160 resolved solnums | **per-solnum substrate available** | 137 = **85.62 %** |
| | 160 winners (type-ranked) | per-winner yield | 114 = 71.25 % |
| | 8,228 attachments | access-gated (non-public) | 507 = 6.2 % |

### Compounded end-to-end (subaward-weighted)

`130,011 → ×66.85 % (86,912) → ×22.16 % (19,262) → ×85.62 % (16,492)`

| Grain | Reachable to downloadable substrate |
|---|---:|
| **Distinct subawards** | **12.69 % → ≈ 16,492** of 130,011 |
| (for contrast) prime awards | 5.35 % |

The dominant loss is **Stage 2** (≈78 % of solnum-bearing subawards sit on a solnum
with no discrete competed SAM notice), not Stage 1 — the inverse of the prime path,
where Stage 1 was the killer. Stage 3 is nearly free.

---

## 2. Why the grains differ

- **Stage 1 is 3.8× richer than prime** (66.85 % vs 17.36 %). Subcontract reporting
  (FFATA/FSRS) is triggered on large prime contracts with subcontracting plans —
  precisely the competed actions that *do* carry a SAM.gov solicitation. The raw
  prime feed is diluted by sole-source / SAT / FSS call orders that subawards rarely
  attach to.
- **Stage 2 is weaker on a subaward-weighted basis** (22.16 % vs ~34 %). The
  high-subcontract-count primes are disproportionately **DoD/IDV task-order
  vehicles** whose "solicitation_identifier" is the parent IDV or a GWAC, not a
  discrete competed notice. So weighting by subaward volume drags resolution *below*
  the flat solnum rate (29–32 %).

---

## 3. Multiplicity — one solnum, many notices

**75.6 % of resolved solnums (526/696 clean) return multiple notice_ids** (mean 5.26,
vs 1.62 for the prime sample). Across resolved notices, `notice_type` is dominated
by **Award Notice (9,811)** but `base_type` — the document-host identity, which does
*not* flip on award — is dominated by **Combined Synopsis/Solicitation (9,544)**.
**Rank winners on `base_type`, never `notice_type`.**

Substrate by `base_type` (live, 836 notices) pinpoints where the PWS/SOW lives:

| base_type | per-notice yield | attachments |
|---|---:|---:|
| Combined Synopsis/Solicitation | **91.3 %** | 949 |
| Solicitation | **90.8 %** | 4,409 |
| Presolicitation | 74.3 % | 2,570 |
| Sources Sought | 67.2 % | 158 |
| Special Notice | 65.9 % | 123 |
| **Award Notice** | **30.4 %** | 15 |

---

## 4. Structural anomalies encountered

1. **No solicitation field on the subaward grain (the headline).** All 118 verbatim
   columns checked: zero `solicitation*`. Bridge is mandatory two-hop via
   `prime_award_unique_key`. Award→solnum is clean **1:1** (0 awards with >1 distinct
   solnum across their FPDS transactions).

2. **Umbrella GSA Schedule / GWAC solnums with unusable fanout.** A handful of real
   solnums resolve to *thousands* of unrelated notices — `47QSMD20R0001` → **8,365
   notices**, `FCIS-JB-980001-B` (GSA IT Schedule 70) → 440, `FCO00CORP0000C` → 203.
   These are parent-schedule/GWAC solicitation numbers inherited by every task-order
   notice; you cannot pick "the one." **Quarantined** (26 solnums with >20 notices;
   only 389 subawards of weight — small). Genuine placeholders (`N/A`→`NA`, `NONE`)
   are caught by an ≥8-char normalized-length floor (30 solnums).

3. **`notice_type` flips to "Award Notice" post-award.** 9,811 of resolved notices
   read Award Notice by `notice_type` but Combined Synopsis/Solicitation by
   `base_type`. Winner-ranking on `notice_type` would route every awarded
   procurement to its (near-empty, 30 % yield) award notice.

4. **Per-winner < per-solnum substrate gap (71.25 % vs 85.62 %).** Strict
   `base_type` ranking sometimes elects a notice (e.g. a Presolicitation, 74 %
   yield) that is empty while a sibling notice holds the files. **Select the
   highest-tier notice _that has attachments_, or harvest all lifecycle notices per
   solnum** — do not hard-commit to the single type-winner.

5. **Reporting-frontier window, not action window.** `subaward_action_date` 2001→
   2026 in a "90-day" feed. Solnum recovery against the 90-day fresh prime feed would
   be near-empty; the full historical FPDS mirror is required (and is indexed on
   recipient/UEI/NAICS/CAGE but **not** on `generated_unique_award_id` — the hop-1
   join is a filter-pushdown scan over 107 M rows, 110,489 matched txn rows, runs in
   the low minutes).

6. **6.2 % access-gated attachments** (507/8,228) in the subaward set vs **0** in the
   prime sample — the subaward population reaches more DoD/controlled notices.
   Budget for `accessLevel != public` at fetch time.

7. **No api.sam.gov contact, no rate limits hit.** Translation was the offline Lance
   join; the live stage used only the unauthenticated frontend resources endpoint.
   836/836 HTTP 200 at 5.3 req/s single-threaded. The api.sam.gov developer-gateway
   quota wall (~10/day, ≤1-yr window) documented in the prime diagnostic is avoided
   entirely.

8. **`size_bytes` is a lower bound** (pre-existing SAM defect, per
   `sam_attachment_manifest.py`) — not measured here; enforce real `Content-Length`
   at fetch.

---

## 5. Architecture implication

The subaward→attachment bridge is **additive, not redundant**: of the resolved
notices, only **536 (3.9 %)** are already covered by the prime-feed manifest
(`sam_opps_attachment_manifest_90day_winners`) — the subaward solnums are a largely
disjoint award population. Recommended build, if pursued:

1. **Materialize the bridge offline** — three-table DuckDB join → Lance:
   `(prime_award_unique_key, subaward_number, solicitation_identifier, notice_id,
   base_type, posted_date)`, BTREE on `prime_award_unique_key` + `solicitation_identifier`
   + `notice_id`. **No live calls.** Apply the ≥8-char / ≤20-notice solnum filter to
   strip umbrella vehicles.
2. **Per solnum, prefer the highest `base_type` tier _with attachments_** (Combined
   Synopsis/Solicitation > Solicitation > Presolicitation), not the award notice.
3. **Probe the live resources endpoint only for the resolved-notice subset**
   (single-threaded, residential IP, resumable JSONL checkpoint) — the one
   unavoidable live stage. Reuse `sam_opps_attachment_manifest_90day_winners.py`'s
   harvester verbatim; ~96 % of the notices will be new.

---

## 6. Reproducibility

Read-only throughout. Source `usaspending_api_fresh/contract_subaward` (199,901 rows,
130,011 distinct subawards, 6,347 distinct primes). Hop 1: filter-pushdown scan of
`usaspending/transaction_search_fpds` on the 6,347 `generated_unique_award_id` keys
(110,489 matched txns; arg_max solnum by action_date). Hop 2: normalized join
(`upper`; strip `[^A-Z0-9]`) of 2,419 recovered solnums against
`sam-gov-opps/{active,archived}`. Sample: `USING SAMPLE 500 ROWS (reservoir, 42)`
over clean-eligible (≥8-char) solnums. Hop 3 (live): `opps/v3/opportunities/{nid}/
resources`, `resourceId` presence only, 836 resolved notices, 0.12 s pace. R2 creds
from `core-x/prd`.
