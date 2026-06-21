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
