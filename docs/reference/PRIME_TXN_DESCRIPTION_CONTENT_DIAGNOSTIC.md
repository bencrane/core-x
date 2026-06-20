# Prime-Txn Description Content Diagnostic — richness & sub-mining viability

**Mode:** READ-ONLY recon. No DDL, no write, no index op.
**Date:** 2026-06-19 · **Plane:** core-x Gen-3 (LanceDB SoR on R2).
**Table:** `s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/` — Lance **v22**, **1,518,807 raw rows × 297 cols**, 7 fragments, all-VARCHAR, verbatim `Contracts_PrimeTransactions` download schema, transaction grain.
**Columns under study:** `prime_award_base_transaction_description` (col 96), `transaction_description` (col 95).
**Question:** is the narrative rich or sparse; does it convey work-to-be-done; can it be mined for what a prime must source/subcontract to deliver.
**Method:** deduped on `contract_transaction_unique_key` (latest `last_modified_date`) — the table re-pulls overlapping publish-lag windows. Probe: `/tmp/desc_content_investigation.py`.

---

## 0. Verdict (read first)

**~100% populated but predominantly SPARSE** (median 37 chars / 4 words). Not nulls/junk — **terseness + commodity nomenclature**. Genuine work-to-be-done narrative exists only in a **single-digit-to-~12% tail** (services/R&D/construction). For subcontract-need mining: **extractively useless** (text never states sub needs — `SUBCONTRACT` 0.01%), **inferentially viable on the substantive subset only**. Dollar size does NOT predict richness — the largest awards are title-only.

## 1. Census (deduped)

| | value |
|---|---|
| raw rows | 1,518,807 |
| distinct transactions | 1,426,826 (dup 1.064×) |
| distinct awards | 1,247,391 (1.144 txn/award) |
| base actions (mod 0/'') | 1,141,646 (80.0%) |
| modification actions | 285,180 (20.0%) |

## 2. Length / word distributions

| metric | `transaction_description` | `prime_award_base_transaction_description` | base_desc @ award grain |
|---|---|---|---|
| median chars / words | 37 / 4 | 37 / 4 | 37 / 4 |
| p90 chars / words | 165 / 23 | 161 / 23 | 156 / 22 |
| p99 chars | 250 | 250 | 249 |
| avg chars / words | 61 / 8 | 62 / 8 | 59 / 7.7 |
| max chars | 3,837 | 3,907 | 3,907 |
| **≤60 chars** | **76.6%** | **76.7%** | — |
| 61–200 chars | 15.7% | 15.7% | — |
| 201–500 chars | 7.7% | 7.4% | — |
| >500 chars (true paragraphs) | 0.1% | 0.3% | — |

Mod rows are longer (p50 46c) but admin-dominated; base rows are the scope-bearing set.

## 3. Boilerplate / junk census

- Top value (both cols): `FEDERAL SUPPLY SCHEDULE CONTRACT` — 2.65% of all txns, **13.15% of mods**, 2.14% of base.
- Head is DLA commodity nomenclature + IDIQ program names (`OASIS+`, `SEAPORT-NXG`, `CVN 78 SHIP CONSTRUCTION`) + recurring `SEE ATTACHED DOCUMENT FOR DETAIL`.
- Cardinality: txn 61.8% distinct, base 58.1% distinct.
- **Clean on obvious junk:** NULL/empty 0.0%; ≤3 chars 0.1%; `IGF::` tag 1.2–1.6%; literal junk phrase 0.0%; no-alpha 0.3/0.1%. The sparseness is terseness, not nullity.

## 4. Scope & subcontract lexicon

| signal | base_desc | txn_desc (base) |
|---|---|---|
| ≥1 scope/deliverable term | **24.4%** | 13.9% |
| ≥1 subcontract/teaming/clearance term | **2.3%** | 2.0% |

Per-term (base_desc, non-null): `SUBCONTRACT` 0.01% · `TEAMING` 0.00% · `JOINT VENTURE` 0.00% · `LABOR CATEGOR` 0.00% · `CLEARANCE` 0.14% · `SECRET` 0.05%. Non-trivial hits (`SME` 0.53%, `LICENSE` 0.43%, `SPARE` 0.39%, `OEM` 0.29%) are substring false positives (`SME` ⊂ `ASSESSMENT`) or commercial-product terms — **not teaming intent**.

## 5. Obligation-weighted substance (award grain)

- Awards with **substantive** base desc (≥120 chars & ≥15 words & non-junk): **148,648 = 11.9%** of 1,247,391.
- They carry **$62.8B of $336.8B = 18.6%** of obligated $ (Σ txn obligation).
- Richness is sector-split: commodity NAICS terse (311991 perishables median 2 words, 324110 petroleum 3, 311812 bakery 2); services/equipment richer (423850 = 19 words, 332216 = 10).
- **Top-$ awards are title-only:** `BORDER BARRIER DESIGN BUILD FOR BBT-5` $2.59B · `CVN 78 SHIP CONSTRUCTION` $1.86B · `PHARMACEUTICAL PRIME VENDOR (PPV) FY2026 APRIL` $1.19B. Megadeal scope is off-table.

## 6. Conclusion

Treat the description as a **high-precision / low-recall scope feature, not a corpus**. Strong on the ~12%-of-awards / ~19%-of-$ services-construction-R&D subset; worthless on the commodity bulk and the title-only top. For sub-targeting:

1. **Gate** on substantive base text (≥15 words + scope-lexicon hit) to isolate the mineable subset.
2. **Infer** scope → required-capability → sub mapping there (LLM, not regex).
3. **Fall back** to NAICS/PSC + the solicitation-attachment scope substrate elsewhere (that substrate is bridge-bound at **0.40% award coverage** — `SUBAWARDEE_GTM_AUDIENCE_READINESS_DIAGNOSTIC.md`).

Do **not** architect sub-need extraction as global text-mining over this field: 88% of awards lack the narrative and the field never names subs.

**Column choice:** use `prime_award_base_transaction_description`, base actions only, collapsed to one row/award on latest `last_modified_date` (award-grain scope, constant per `contract_award_unique_key`, dodges the 20% mod admin text).

**Correction to prior recon:** `usaspending_90day_diagnostic.md` §3.6 lists `description` among omitted columns — that is the bulk `award_search.description` *name*; the narrative IS carried in the download projection under these two columns (verified here).
