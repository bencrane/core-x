# Canonical Financial-Institution Registries — Lender-Designation Recon

Read-only sweep of the Gen-3 Lance SoR (`s3://data-sink/active/`, R2) for datasets that
carry a **canonical government designation** of a financial institution — FDIC cert, NCUA
charter, Fed RSSD, SEC CIK/CRD, GLEIF LEI, federal-regulator agency code — so UCC secured
parties (lenders) can be classified by **deterministic join instead of LLM inference**.

- **Harnesses (non-mutating):** [`scripts/fininst_registry_probe.py`](../../scripts/fininst_registry_probe.py)
  (schema/identifier/classification audit) · [`scripts/ucc_lender_designation_overlap_probe.py`](../../scripts/ucc_lender_designation_overlap_probe.py)
  (UCC↔registry coverage).
- **Attestation:** `lance.dataset(...).scanner()/count_rows()/list_indices()` + DuckDB `SELECT`. Zero writes. Probe run 2026-06-21.
- **Pairs with:** [UCC_CA_CO_LENDER_GTM_RECON.md](UCC_CA_CO_LENDER_GTM_RECON.md) (the UCC side — established there is **no** stored lender taxonomy).

---

## 0. Headline

1. **The canonical bank/credit-union designation already exists in the SoR — embedded in
   `sba_7a`.** Every SBA 7(a) loan carries its originating lender's **`bank_fdic_number`**
   (FDIC cert, ~90% populated) and **`bank_ncua_number`** (NCUA charter, the credit-union
   rows), keyed to `bank_name` (BTREE). That is a ready-made *name → FDIC-cert / NCUA-charter*
   crosswalk — a hard government designation, no LLM.
2. **`hmda_panels`** adds 52,009 mortgage-lender records with `agency_code` (federal
   supervisor), **`respondent_rssd`** (Fed RSSD), **`lei`**, and **`tax_id`** (EIN) — and
   **`crosswalk_hmda_gleif`** is a pre-built LEI↔RSSD↔name bridge (BTREE on `normalized_legal_name`,
   directly UCC-joinable).
3. **SEC side is present:** `sec_adv_part1` (RIA / private-fund-manager registry: CRD, SEC#,
   AUM) and `edgar_form_d` (`industry_group_type`, `investment_fund_type`, `is_40_act` — the
   **BDC / private-credit-fund** surface). `gleif_l1_entities` (3.35M LEI↔name) + `_l2_relationships`
   give an LEI spine and corporate-parent rollup.
4. **Coverage (measured, exact-name FLOOR):** **20.1% of CA / 22.4% of CO** secured-party
   *appearances* match a canonical registry on exact normalized name alone — only **3.2% / 4.4%
   of distinct names**. Deterministic classification covers the **high-frequency head** (the
   real banks/CUs/fintech lenders); the long tail of one-off lessors stays for the LLM.
5. **Gaps:** **no standalone FDIC** (BankFind) **or NCUA** registry, **no per-entity NMLS
   roster**, **no dedicated BDC list**. FDIC/NCUA certs exist today only *as embedded columns
   in `sba_7a`* — a subset, not the full universe.

---

## 1. Canonical registries present in the SoR

| Dataset | Rows | Hard government ID(s) | Designation field | Classification use |
|---|--:|---|---|---|
| **`sba_7a`** | 1,947,098 | **`bank_fdic_number`**, **`bank_ncua_number`** (→ `bank_name`, BTREE) | FDIC cert ⇒ bank · NCUA charter ⇒ credit union | **cleanest hard bank/CU designation** |
| **`hmda_panels`** | 52,009 | `respondent_rssd`, `lei`, `tax_id` (EIN), `respondent_id` | `agency_code` (1 OCC·2 FRS·3 FDIC·5 NCUA·7 non-dep·9 CFPB) | regulator + RSSD/LEI spine |
| **`crosswalk_hmda_gleif`** | 6,470 | `lei`, `hmda_respondent_rssd`, `hmda_tax_id` | `hmda_agency_code`, `gleif_entity_status` | pre-joined LEI↔RSSD↔name; **BTREE `normalized_legal_name`** |
| **`sec_adv_part1`** | 36,846 | `crd_number`, `sec_number`, `lei` | `filer_type` (IA 73% / ERA 27%), `large_fund_adviser_flag`, `regulatory_aum` | RIA / fund-manager registry |
| `sec_adv_w` | 21,076 | `crd_number`, `sec_number` | `filing_type` (FULL/PARTIAL) | deregistered advisers (negative signal) |
| **`edgar_form_d`** | 57,496 | `primary_issuer_cik`, `file_num` | `industry_group_type` (Pooled Fund 62%, Commercial Banking, Other Banking & Fin Svcs, REITS & Finance), `investment_fund_type` (Hedge/VC/PE), `is_40_act` | **BDC / private-credit / fund** surface |
| `edgar_cik_map` | 10,412 | `cik_str`, `cik10`, `ticker` | `exchange` | public-company identity spine |
| `gleif_l1_entities` | 3,348,282 | `lei` | `entity_status` (ACTIVE 92.5%) | LEI↔name spine (no FI typing alone) |
| `gleif_l2_relationships` | 476,870 | `lei`, `parent_lei` | `relationship_type` | subsidiary → holding-co rollup |
| `sba_504` | 227,404 | — | `cdc_name`, `third_party_lender_name` (BTREE) | CDC + bank lender names (no cert) |
| `form5500_sch_a_carrier` | 23,648 | `INS_CARRIER_NAIC_CODE` | — | insurer registry (tangential to lending) |

`edgar_form_4` (insider) and the `nmls_mcr_*` tables are present but not institution
registries (insider transactions / Mortgage-Call-Report aggregates respectively).

---

## 2. Coverage — what fraction of UCC lenders classifies deterministically

Reference set = 54,135 distinct canonical lender names (`sba_7a` bank names 5,610 + HMDA
panel 14,783 + SEC ADV 36,247), exact-`_name_norm`-joined to the UCC organization secured
parties. **This is a FLOOR** — no alias canonicalization, no `NA↔NATIONAL ASSOCIATION` fold,
no filing-agent unmasking.

| | CA `secured_parties` | CO `ucc_co_secured_parties` |
|---|--:|--:|
| Org secured-party **appearances** | 4,684,136 | 2,009,600 |
| Distinct normalized lender names | 92,698 | 66,474 |
| **Matched — appearance-weighted** | **20.06%** (939,460) | **22.38%** (449,806) |
| Matched — distinct names | 3.20% (2,966) | 4.35% (2,892) |

The head/tail gap is the story: **~1 in 5 filings** names a lender we can stamp with a
government designation **for free**, but those concentrate in a few thousand high-frequency
institutions. The 90k+ distinct-name tail (one-off equipment lessors, private parties,
out-of-state niche financiers) is where the LLM earns its keep.

### Government-designated class mix (appearance-weighted, consolidated)

| Class | CA appearances | CO appearances |
|---|--:|--:|
| **Bank** — FDIC cert (hard) + HMDA 1/2/3 + SBA | 416,100 (8.9%) | 305,340 (15.2%) |
| **CFPB-supervised** (agency 9 — mostly mega-banks; ambiguous) | 217,332 (4.6%) | 58,903 (2.9%) |
| **Credit union** — NCUA charter (hard) + HMDA agency 5 | 145,086 (3.1%) | 42,644 (2.1%) |
| **Non-bank mortgage** (HMDA agency 7 — e.g. GoodLeap, Loanpal) | 157,849 (3.4%) | 41,952 (2.1%) |
| **Investment adviser** (SEC ADV) | 3,093 (0.1%) | 967 (0.0%) |

Top matched lenders confirm the signal is real and useful: JPMorgan Chase, U.S. Bank,
Wells Fargo, Bank of the West, Comerica, Bank of Colorado, KeyBank (banks); Technology /
Elevations / Wheelhouse / LA Federal **Credit Union** (NCUA); GoodLeap, Loanpal (fintech).

---

## 3. Methodology caveats (read before trusting the class split)

1. **HMDA `agency_code` is a *supervisory* assignment, not a charter type.** Code **9 = CFPB**,
   which supervises the **largest depositories (>$10B)** *and* large non-banks — so Chase/Wells/
   US Bank's `…NA` records carry agency 9, not "bank." **Prefer the hard FDIC-cert / NCUA-charter
   from `sba_7a`**; use HMDA agency only for 1/2/3 (bank), 5 (CU), 7 (non-dep mortgage); treat 9
   as needs-disambiguation.
2. **Name-variant fragmentation directly costs coverage and accuracy** — proven in-data: `WELLS
   FARGO BANK NATIONAL ASSOCIATION` and `US BANK NATIONAL ASSOCIATION` resolve to **BANK (FDIC
   cert)** via SBA, while `WELLS FARGO BANK NA` / `US BANK NA` fall to the **CFPB-9** bucket — the
   *same bank* in two classes. The `NA↔NATIONAL ASSOCIATION` fold (already flagged required in
   [UCC_CA_CO_LENDER_GTM_RECON.md](UCC_CA_CO_LENDER_GTM_RECON.md) §5 / the SoS blueprint) both
   lifts the 20% floor and collapses the ambiguous agency-9 megabanks onto their FDIC cert.
3. **Filing-agent masking persists** — `… AS REPRESENTATIVE` / CSC / CT Corp name a representative,
   not the lender; those appearances can't be classified from the secured-party name at all.
4. **Coverage is bounded by which institutions appear in SBA/HMDA.** A community bank or CU that
   neither makes SBA loans nor reports HMDA has no record here — see §4.

---

## 4. Gaps to close (to push coverage past the embedded subset)

| Missing canonical source | What it would add | Size / effort |
|---|---|---|
| **FDIC Institution Directory / BankFind** | Full ~4,500 insured banks: cert #, charter class, active/failed, RSSD, established date — the *complete* bank universe, not just SBA lenders | small, free, authoritative |
| **NCUA call-report / charter list** | Full ~4,600 federally-insured credit unions: charter #, name, state, asset band | small, free, authoritative |
| **Per-entity NMLS roster** | NMLS ID → company name/type for non-depository lenders/MLOs (landed NMLS is only `nmls_mcr_*` aggregates + `nmls_state_entity_counts` (59 rows)) | medium |
| **BDC registry** (SEC N-2 / closed-end fund list) | Explicit Business Development Companies (today only EDGAR-indirect via Form D `is_40_act` + `industry_group_type`) | medium |
| GLEIF L1 `entity.category` + ELF legal-form | Would let GLEIF self-type FUND/BRANCH/SOLE-PROP (current L1 carries only name/address/status) | re-ingest field add |

Highest leverage is **FDIC + NCUA direct ingest** (both are single small authoritative files):
they complete the depository universe and convert the "BANK/CU" classes from a SBA/HMDA subset
to ground truth. But note alias canonicalization (§3.2) is the bigger coverage lever than more
registries.

---

## 5. Recommended deterministic-classification build

Mirror the established crosswalk pattern (UCC↔SoS bridge; `crosswalk_sam_usaspending`):

1. **Build `ref_lender_designation`** = UNION of canonical *name → designation* pairs, normalized
   with the SoR's `_name_norm` macro, BTREE on `normalized_legal_name`:
   - `sba_7a` → `nn(bank_name)` → {FDIC cert ⇒ BANK, NCUA charter ⇒ CREDIT_UNION} + carry the cert/charter #.
   - `hmda_panels` → `nn(respondent_name)` → agency 1/2/3 ⇒ BANK, 5 ⇒ CREDIT_UNION, 7 ⇒ NONBANK_MORTGAGE, 9 ⇒ flag-ambiguous; carry `respondent_rssd` + `lei`.
   - `sec_adv_part1` → `nn(legal_name / primary_business_name)` → INVESTMENT_ADVISER; carry `crd_number` + AUM.
   - `edgar_form_d` issuers → PRIVATE_FUND / BDC-candidate (`is_40_act`, `industry_group_type`).
   - **(after FDIC/NCUA ingest)** add the full directories as the authoritative BANK/CU spine.
2. **Apply the `NA↔NATIONAL ASSOCIATION` + brand alias fold** (the §3.2 lever) on *both* sides
   before the join.
3. **Equi-join** `ca_ucc/secured_parties.org_name` / `ucc_co_secured_parties.organization_name`
   (same `_name_norm`) against `ref_lender_designation` — all BTREE-indexed → sub-second, single
   bulk hash-join. Stamp the matched secured parties with `lender_class` + the source cert/charter/RSSD/LEI/CRD as **new provenance-stamped columns**.
4. **LLM only on the unmatched residual** (the distinct-name long tail) — and even there, seed it
   with the canonical names as few-shot anchors. The government-sourced head stays hard truth,
   cleanly separable from any inferred label.

---

## 6. The unmatched residual — frequency-band distribution (CA+CO combined)

Sizing the LLM/manual workload after the canonical join. Appearances summed across both
states (same normalized name → one row, per operator framing). Harness:
[`scripts/ucc_unmatched_lender_bands_probe.py`](../../scripts/ucc_unmatched_lender_bands_probe.py).

Combined distinct org-lender names **145,826** · matched **3,966** (2.7%) · **unmatched 141,860**
(97.3%) carrying **5,304,470** appearances.

| Appearances (combined) | Unmatched names | % names | Appearances | % appx |
|---|--:|--:|--:|--:|
| = 1 | 86,248 | 60.8% | 86,248 | 1.6% |
| 2–4 | 32,756 | 23.1% | 82,647 | 1.6% |
| 5–9 | 9,036 | 6.4% | 58,733 | 1.1% |
| 10–14 | 3,398 | 2.4% | 39,696 | 0.7% |
| 15–24 | 3,051 | 2.2% | 57,271 | 1.1% |
| 25–49 | 2,824 | 2.0% | 97,899 | 1.8% |
| 50–99 | 1,797 | 1.3% | 125,340 | 2.4% |
| 100–249 | 1,375 | 1.0% | 213,257 | 4.0% |
| 250–999 | 951 | 0.7% | 447,864 | 8.4% |
| **1000+** | **424** | **0.3%** | **4,095,515** | **77.2%** |

**Cumulative — cut the tail at ≥N appearances:**

| ≥ N | Names to classify | % of unmatched names | Appearances covered | % of unmatched appx |
|---|--:|--:|--:|--:|
| ≥2 | 55,612 | 39.2% | 5,218,222 | 98.4% |
| **≥5** | **22,856** | 16.1% | 5,135,575 | **96.8%** |
| **≥10** | **13,820** | 9.7% | 5,076,842 | **95.7%** |
| **≥15** | **10,422** | 7.3% | 5,037,146 | **95.0%** |
| ≥25 | 7,371 | 5.2% | 4,979,875 | 93.9% |
| ≥50 | 4,547 | 3.2% | 4,881,976 | 92.0% |
| ≥100 | 2,750 | 1.9% | 4,756,636 | 89.7% |
| ≥250 | 1,375 | 1.0% | 4,543,379 | 85.7% |

**Read:** the tail is extreme — **84% of unmatched names (119,004) appear ≤4 times** and are
worth only 3.2% of unmatched volume. Cutting at **≥10 collapses the workload from 141,860 to
13,820 names (−90%) while retaining 95.7%** of unclassified filings; the **424 names at ≥1000
alone cover 77.2%**.

**But the unmatched head is not all lenders** — the highest-volume names are three buckets, and
only the third is the GTM target:

1. **Government statutory-lien filers** (strip via `filing_type='UCC'`): EMPLOYMENT DEVELOPMENT
   DEPARTMENT (887k), US SMALL BUSINESS ADMINISTRATION (430k, EIDL direct), CA DEPT OF TAX & FEE
   ADMIN (212k), IRS / IRSOHIO (166k+93k), BOARD OF EQUALIZATION (78k), FRANCHISE TAX BOARD.
2. **Filing agents** (flag `lender_unknown`): CORPORATION SERVICE COMPANY (110k), C T CORPORATION
   SYSTEM (86k), FIRST CORPORATE SOLUTIONS (28k), CHTD COMPANY (22k).
3. **The prize — captive equipment / specialty / clean-energy finance** (unmatched because captive
   arms carry no FDIC cert / don't report HMDA / aren't SBA banks): SNAP-ON CREDIT (190k+27k+16k,
   3 variants), KUBOTA CREDIT (60k), JOHN DEERE (56k+20k), CATERPILLAR FINANCIAL (44k), DE LAGE
   LANDEN (20k), CNH INDUSTRIAL CAPITAL (15k), WELLS FARGO EQUIPMENT/VENDOR FINANCE (17k+14k),
   NEXTGEAR CAPITAL (14k), TOYOTA COMMERCIAL FINANCE (12k), AGCO FINANCE (12k), US BANK EQUIPMENT
   FINANCE (14k); solar — SOLAR MOSAIC (58k+54k), EVERBRIGHT (35k), LOANPAL/PARAMOUNT EQUITY (27k),
   ENFIN (13k); FARM CREDIT SERVICES OF AMERICA (16k — FCA-regulated, a 4th regulator beyond
   OCC/FDIC/NCUA).

**Consequence:** after a `filing_type='UCC'` pre-filter and an `AS REPRESENTATIVE` agent flag, the
genuine equipment/alt-lender classification target is a **few thousand high-frequency names**,
heavily variant-fragmented (SNAP-ON ×3, SOLAR MOSAIC ×2, JOHN DEERE ×2) — canonicalization collapses
it further. A hand-curatable / single-LLM-pass set, not a 142k-name problem.

---

## 7. Do the unmatched "look like banks"? — FDIC / NCUA-ingest ROI

Decision: ingest the full FDIC (BankFind) + NCUA charter directories, or have SBA+HMDA
already captured the depository universe deductively? Harnesses:
[`scripts/ucc_unmatched_bank_signal_probe.py`](../../scripts/ucc_unmatched_bank_signal_probe.py)
(lexical depository signal × match status) ·
[`scripts/ucc_bank_canon_recovery_probe.py`](../../scripts/ucc_bank_canon_recovery_probe.py)
(variant-vs-novel split).

**Answer: by and large, no — the unmatched residual is NOT banks.** Of 6,693,736 org
secured-party appearances (CA+CO), only **24.5% (1,642,889) name a depository-looking entity**
at all (bank/CU name tokens); the other **75.5% are non-depository** — captive equipment finance,
fintech/solar, government, agents, private parties. And SBA+HMDA already captured most of the
depository slice:

| Lexical class | Total appx | **% already matched (SBA+HMDA)** | Unmatched appx |
|---|--:|--:|--:|
| BANK-looking | 1,423,051 | **66.7%** | 474,001 |
| CREDIT_UNION-looking | 219,838 | **85.2%** | 32,524 |
| NEITHER (non-depository) | 5,050,847 | 5.0% | 4,797,585 |

The unmatched depository residual (506,525 appx = **7.6% of all org-lender appearances**) is the
*entire* prize an FDIC/NCUA ingest could chase — and most of it isn't novel:

| Unmatched depository | Recovered by canonicalization alone | **Still novel (registry headroom)** |
|---|--:|--:|
| BANK (474,001 appx) | ≥138,751 (29.3%) — *floor* | ≤335,250 (70.7%) |
| CREDIT_UNION (32,524 appx) | 319 (1.0%) | 32,205 (99.0%) |

The 29.3% bank recovery is a **conservative floor** (a literal `NA↔NATIONAL ASSOCIATION` +
agent/division tail-strip). Inspection of the "still-novel" head shows most of it is *also*
variants of institutions already in the data, just under messier strings — `US BANK EQUIPMENT
FINANCE A DIVISION OF US BANK` (US Bank), `HSBC BANK NEVADA NA` (HSBC), `US BANCORP`, `NORWEST
BANK COLORADO`, `CALIFORNIA BANK & TRUST` (Zions), `SHEFFIELD FINANCIAL A DIVISION OF TRUIST BANK`,
`TCF EQUIPMENT FINANCE`, `BANC OF AMERICA LEASING`. A fuzzy/core-token match absorbs most of these.
The genuinely-absent-from-all-registries tail is a thin set of community/regional banks (`COLORADO
BUSINESS BANK`, `VECTRA BANK`, `MIDFIRST BANK`, `STEARNS BANK`, `MARLIN BUSINESS BANK`, `FIRST
NATIONAL BANK OF FT COLLINS`) — and **those are HMDA reporters anyway**, so better canonicalization
against the existing HMDA panel catches them without a new ingest.

### Verdict

- **FDIC bulk ingest — low coverage ROI.** You've already captured 2/3 of bank appearances; the
  unmatched bank residual is overwhelmingly *variants of banks you already have* (a canonicalization
  problem), not banks missing from your data. FDIC's real value is as an **authoritative canonical
  name + cert-number spine that strengthens the alias fold** — a nice-to-have reference, not a
  match-coverage unlock. **Build canonicalization first; ingest FDIC only to stamp hard cert IDs.**
- **NCUA ingest — marginally higher ROI, but tiny volume.** Credit unions are the one depository
  class with a genuinely-novel tail (99% of the unmatched CU residual is net-new — CUs underreport
  HMDA: San Diego Metropolitan, Mountain America, Ent, Warren, Clean Energy FCU). But that's only
  **32,205 appx ≈ 0.5%** of all org-lender appearances. Cheap to ingest, won't move the needle.
- **The 75.5% non-depository majority is the actual GTM target** (the equipment financiers) and **no
  government bank registry touches it** — it needs name canonicalization + LLM/heuristic
  classification regardless of any FDIC/NCUA decision.

**Net:** the bank/CU universe is essentially already captured deductively via SBA+HMDA; spend the
effort on the alias-canonicalization layer (recovers the variant residual *and* sharpens the
non-depository classification), and treat FDIC/NCUA ingest as an optional authoritative-ID reference,
not a coverage play.

---

## 8. Recency — the bands above are ALL-TIME; the active universe is ~5× smaller

The secured-party tables are **dateless**, so §2/§6 appearance counts are **all-time** — they span
the full filing history (**CA 1965, CO 1966 → May 2026**). A high all-time count can be a long-dead
lender (GE Money Bank, Norwest, First Republic). Attaching `filing_date` by join (CA
`secured_parties→filings` on `ucc1_num/ucc3_num`; CO `→co_ucc_transactions` on `file_id`, min date
per key; **100% join coverage, no fan-out — 6,693,736 appearances preserved exactly**) recasts the
heads on trailing windows. Harness:
[`scripts/ucc_lender_recency_bands_probe.py`](../../scripts/ucc_lender_recency_bands_probe.py).

**Most of the volume is old:** only **5.3%** of appearances fall in the last 12 months, **10.9%** in
the last 24, **17.2%** in the last 36 (ref date 2026-06-01).

**Distinct lender names with ≥N appearances — all-time vs trailing windows:**

| ≥ N | All-time | Last 36mo | Last 24mo | Last 12mo | Last-24mo matched / unmatched |
|---|--:|--:|--:|--:|--:|
| ≥5 | 25,101 | 6,478 | 5,021 | 3,210 | 812 / 4,209 |
| **≥10** | **15,559** | 4,140 | **3,191** | 2,002 | **608 / 2,583** |
| ≥15 | 11,891 | 3,173 | 2,426 | 1,530 | 501 / 1,925 |
| ≥25 | 8,581 | 2,319 | 1,751 | 1,058 | 405 / 1,346 |
| ≥50 | 5,459 | 1,465 | 1,083 | 619 | 278 / 805 |

**Recency shrinks the target ~5×.** "≥10 appearances all-time" = 15,559 names; require those 10 to
fall in the **last 24 months** and it's **3,191** (608 already-classifiable + 2,583 unmatched);
last 12 months, **2,002**. The *active* lender universe worth targeting is low single-digit thousands.

**Recency also reshuffles the head** — it is a different list, not a subset:

- **Surging (recent ≫ historical share):** ENFIN CORP (11,099 of 12,673 all-time — a near-brand-new
  solar lender), EVERBRIGHT LLC (13,824 of 35,017), WEBBANK ITS SUCCESSORS AND ASSIGNEES (3,215 of
  4,748 — fintech BaaS charter; FDIC-insured, match breaks on the suffix), MIDDESK INC AS
  REPRESENTATIVE (a new fintech filing agent), DLL FINANCE.
- **Still dominant & active:** SNAP-ON CREDIT (28,879 in 24mo), KUBOTA CREDIT (8,545), CATERPILLAR
  FINANCIAL (6,482), DEERE (4,744+2,875), DE LAGE LANDEN (2,780), CNH INDUSTRIAL (2,511), MATCO
  TOOLS, FARM CREDIT SERVICES.
- **New contaminant visible at the top:** CO **medical-provider liens** (Movement Dynamics PT,
  Injury Care Network, Synergy Chiropractic, Mountain View Pain Center — ~2,000 each in 24mo) ride
  up the recent list; strip with `filing_type` (CO `lien_hosp`) alongside the gov/agent filters.

**Consequence for the GTM:** band on a **trailing 12–24-month `filing_date`** (via the join), not
all-time, to get the *active* lender set — a few thousand names — then apply the `filing_type='UCC'`
+ agent/medical-lien filters and the canonical join. The deliverable target list is small, current,
and dominated by exactly the equipment/solar/specialty financiers the campaign is after.
