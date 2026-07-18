# DOL Form 5500 (2025) Universe — Schema Diagnostic, Relational Map & Commercial Treasure Map

**Source authority:** EBSA/DOL EFAST2 public dissemination — `https://askebsa.dol.gov/FOIA Files/2025/Latest/`
**Manifest:** `data-sink/landing/form-5500/form5500_2025_files.csv` (41 archive rows)
**Mode:** Read-only, pre-ingest. **Zero data archives downloaded** — every fact below is derived from the remote `*_layout.txt` record-layout files only, per the no-zip constraint. No DDL, no Lance writes, no R2 mutation.
**Date:** 2026-06-06 · **Vintage:** Plan year 2025 "Latest" weekly dissemination.
**Method:** Parsed the manifest, issued HTTP GET against all 40 `secondary_url` layout files (all `200 OK`), and parsed the EFAST2 fixed layout grammar (`FIELD_POSITION,FIELD_NAME,TYPE,SIZE` after a `===` rule). **1,289 columns across 40 datasets** classified by key-presence and grain. Layout files carry field **name + type + size only** — no free-text column descriptions exist in the dissemination; semantics below are resolved from the EFAST2 field-naming convention and form line-item mapping. One manifest row (Schedule DCG) exposes no `_layout.txt` and was not fetched (see §1).

---

## 1. Manifest reconciliation

| Bucket | Count | Note |
|---|--:|---|
| Manifest archive rows | 41 | one row per `data_file` zip |
| Rows with a `*_layout.txt` schema (fetched + parsed) | 40 | all returned `200 OK` |
| Rows **without** a layout (skipped) | 1 | **Schedule DCG** — its `secondary_url` is `F_SCH_DCG_PLAN_TRANSFER_2025_latest.zip` (a companion *data* archive, "Plan Transfer"), not a layout. No DCG schema is recoverable without opening a zip; excluded by constraint. |

**Coverage:** 40/41 schemas mapped (97.6%). The DCG head layout is the single blind spot; it is not on the high-value path (DCG = Defined Contribution Group reporting arrangement — a 2024+ consolidated-filing vehicle, low commercial-signal density relative to the H/C/A core).

---

## 2. Relational topology

### 2.1 The two keys

| Key | Field(s) | Type | Role | Scope |
|---|---|---|---|---|
| **Hub / join key** | `ACK_ID` | `TEXT/30` | EFAST2 filing acknowledgment ID. **Present in all 40 tables.** The one column that joins every schedule and detail row back to its filing. | Unique per *filing* within a vintage. The canonical intra-vintage join key. |
| **Business identity** | `SPONS_DFE_EIN` (`TEXT/9`) + `SPONS_DFE_PN` (`TEXT/3`) | composite | Sponsor EIN + 3-digit Plan Number. Identifies the *plan as an entity* independent of any single filing. | The **cross-year / cross-filing resolution key** (EIN+PN persists; `ACK_ID` does not). On Form 5500-SF the pair is `SF_SPONS_EIN` + `SF_PLAN_NUM`. |

`ACK_ID` is how you assemble one filing; `EIN+PN` is how you track one plan through time and dedupe amended filings.

### 2.2 Three structural tiers (measured, not assumed)

Every dataset falls into exactly one grain class, determined by which keys it carries:

```
TIER 0 — FILING HEAD (root entity)                         key: ACK_ID  (PK)
  ├─ F_5500            Form 5500   (large/standard)  140 cols  · SPONS_DFE_EIN + SPONS_DFE_PN
  └─ F_5500_SF         Form 5500-SF (small plans)    191 cols  · SF_SPONS_EIN + SF_PLAN_NUM
        │
        │  one filing carries ≤1 of each schedule head (1:0..1), keyed on ACK_ID
        ▼
TIER 1 — SCHEDULE HEAD (denormalized sponsor EIN+PN+plan-year)   key: ACK_ID  (FK→head)
   A · C(stub) · D · G · H · I · R · MB · SB · MEP
        │
        │  one schedule head fans out to N detail rows (1:N), keyed ACK_ID (+FORM_ID for A) + ROW_ORDER
        ▼
TIER 2 — DETAIL (repeating rows)                           key: ACK_ID [+FORM_ID] + ROW_ORDER
   A_PART1 · C_PART1_ITEM1/2/3 · C_PART2 · C_PART3 · D_PART1/2 · G_PART1/2/3 ·
   H_PART1 · I_PART1 · R_PART1 · MB_* · SB_* · MEP_PART2 · 5500_SF_PART7
        │
        │  service-provider rows fan out to N service codes (1:N)
        ▼
TIER 3 — GRANDCHILD (service-code long tables)             key: ACK_ID + ROW_ORDER + CODE_ORDER
   C_PART1_ITEM2_CODES · C_PART1_ITEM3_CODES · C_PART2_CODES
```

### 2.3 Join recipes (exact field names)

| Join | Left | Right | ON | Cardinality |
|---|---|---|---|---|
| Filing → any schedule head | `F_5500.ACK_ID` | `F_SCH_*.ACK_ID` | `ACK_ID` | 1 : 0..1 |
| Schedule head → its detail | `F_SCH_*.ACK_ID` | `F_SCH_*_PARTn.ACK_ID` | `ACK_ID` (+ `ROW_ORDER` orders rows) | 1 : N |
| **Schedule A → broker rows** | `F_SCH_A.ACK_ID,FORM_ID` | `F_SCH_A_PART1.ACK_ID,FORM_ID` | `ACK_ID + FORM_ID` | 1 : N |
| Provider row → service codes | `F_SCH_C_PART1_ITEM2.ACK_ID,ROW_ORDER` | `…_ITEM2_CODES.ACK_ID,ROW_ORDER` | `ACK_ID + ROW_ORDER` (`CODE_ORDER` orders codes) | 1 : N |
| Cross-year plan continuity | `EIN+PN` (year T) | `EIN+PN` (year T-1) | `SPONS_DFE_EIN + SPONS_DFE_PN` | N : N over vintages |

**Critical disambiguation — counterparty EIN ≠ sponsor EIN.** Detail tables (Schedule C provider rows, Schedule R contributing-employer rows, Schedule D DFE rows, Schedule H/I/SF transfer rows, MEP participating-employer rows) carry an `*_EIN` field, but it identifies the **counterparty** (the service provider, contributing employer, investment entity, or transferee plan) — **not** the filing's sponsor. The sponsor key lives **only** on the filing head and (denormalized) on each schedule head. Detail rows bind to the sponsor exclusively through `ACK_ID`. Treat every detail-table EIN as payload to be resolved against an external entity graph (e.g. the SAM/NPPES/Open-Payments hubs), never as a back-pointer to the plan.

**Schedule A is the one exception to pure-`ACK_ID` joining:** a single filing can attach many Schedule A's (one per insurance contract), so Schedule A introduces `FORM_ID` as a per-contract discriminator. Its Part 1 broker rows therefore require the **composite `ACK_ID + FORM_ID`** join, not `ACK_ID` alone.

---

## 3. The Treasure Map — commercial signals → exact file + column

> Convention: `BOY`=beginning-of-year, `EOY`=end-of-year, `CNT`=count, `AMT`=dollar amount (`NUMERIC`). Large plans (≥100 participants) file Schedule **H**; small plans file Schedule **I**; the smallest file the all-in-one **5500-SF**. To get assets/contributions for the whole universe you must `COALESCE` across **H ∪ I ∪ SF**.

### 3.1 Entity metadata (the "who")

| Signal | Dataset | Column | Type |
|---|---|---|---|
| Plan Sponsor Name | `F_5500` / `F_5500_SF` | `SPONSOR_DFE_NAME` / `SF_SPONSOR_NAME` | TEXT/70 |
| Sponsor DBA | `F_5500` / `F_5500_SF` | `SPONS_DFE_DBA_NAME` / `SF_SPONSOR_DFE_DBA_NAME` | TEXT/70 |
| **Sponsor EIN** | `F_5500` / `F_5500_SF` | `SPONS_DFE_EIN` / `SF_SPONS_EIN` | TEXT/9 |
| Plan Name | `F_5500` / `F_5500_SF` | `PLAN_NAME` / `SF_PLAN_NAME` | TEXT/140 |
| **Plan Number** | `F_5500` / `F_5500_SF` | `SPONS_DFE_PN` / `SF_PLAN_NUM` | TEXT/3 |
| Administrator Name | `F_5500` / `F_5500_SF` | `ADMIN_NAME` / `SF_ADMIN_NAME` | TEXT/70 |
| Administrator EIN | `F_5500` / `F_5500_SF` | `ADMIN_EIN` / `SF_ADMIN_EIN` | TEXT/9 |
| Sponsor mailing address | `F_5500` | `SPONS_DFE_MAIL_US_ADDRESS1/2`, `_CITY`, `_STATE`, `_ZIP` | TEXT |
| Sponsor physical (location) address | `F_5500` | `SPONS_DFE_LOC_US_ADDRESS1/2`, `_CITY`, `_STATE`, `_ZIP` | TEXT |
| Sponsor phone | `F_5500` | `SPONS_DFE_PHONE_NUM` | TEXT/10 |
| Admin address | `F_5500` | `ADMIN_US_ADDRESS1/2`, `_CITY`, `_STATE`, `_ZIP` | TEXT |
| Business code (industry) | `F_5500` | `BUSINESS_CODE` | TEXT |

### 3.2 Size & scale (the "how big")

| Signal | Dataset | Column | Type |
|---|---|---|---|
| **Total active participants** | `F_5500` | `TOT_ACTIVE_PARTCP_CNT` (line 6a) · `TOT_ACT_PARTCP_BOY_CNT` (BOY) | NUMERIC |
| Total active participants (SF) | `F_5500_SF` | `SF_TOT_ACT_PARTCP_BOY_CNT` · `SF_TOT_ACT_PARTCP_EOY_CNT` | NUMERIC |
| Total participants (all classes) | `F_5500` | `TOT_PARTCP_BOY_CNT` · `TOT_ACT_RTD_SEP_BENEF_CNT` | NUMERIC |
| Retired/separated & beneficiaries | `F_5500` | `RTD_SEP_PARTCP_RCVG_CNT`, `RTD_SEP_PARTCP_FUT_CNT`, `BENEF_RCVG_BNFT_CNT` | NUMERIC |
| Participants w/ account balances | `F_5500` | `PARTCP_ACCOUNT_BAL_CNT` | NUMERIC |
| **Total plan assets — large** | `F_SCH_H` | `TOT_ASSETS_EOY_AMT` (BOY: `TOT_ASSETS_BOY_AMT`) | NUMERIC |
| Net plan assets — large | `F_SCH_H` | `NET_ASSETS_EOY_AMT` | NUMERIC |
| **Total plan assets — small** | `F_SCH_I` | `SMALL_TOT_ASSETS_EOY_AMT` | NUMERIC |
| **Total plan assets — SF** | `F_5500_SF` | `SF_TOT_ASSETS_EOY_AMT` (net: `SF_NET_ASSETS_EOY_AMT`) | NUMERIC |
| **Total contributions — large** | `F_SCH_H` | `TOT_CONTRIB_AMT` (split: `EMPLR_CONTRIB_INCOME_AMT`, `PARTICIPANT_CONTRIB_AMT`, `OTH_CONTRIB_RCVD_AMT`) | NUMERIC |
| Total contributions — small | `F_SCH_I` | `SMALL_EMPLR_CONTRIB_INCOME_AMT`, `SMALL_PARTICIPANT_CONTRIB_AMT`, `SMALL_OTH_CONTRIB_RCVD_AMT` | NUMERIC |
| Total contributions — SF | `F_5500_SF` | `SF_EMPLR_CONTRIB_PAID_AMT`, `SF_PARTICIP_CONTRIB_INCOME_AMT` | NUMERIC |
| Total income / expenses / distributions (large) | `F_SCH_H` | `TOT_INCOME_AMT`, `TOT_EXPENSES_AMT`, `TOT_DISTRIB_BNFT_AMT`, `NET_INCOME_AMT` | NUMERIC |

### 3.3 Financial flow — service-provider compensation (Schedule C, the "who gets paid")

| Signal | Dataset | Column | Type |
|---|---|---|---|
| Service provider name (key/direct) | `F_SCH_C_PART1_ITEM2` | `PROVIDER_OTHER_NAME` | TEXT/35 |
| **Service provider EIN** | `F_SCH_C_PART1_ITEM2` | `PROVIDER_OTHER_EIN` | TEXT/9 |
| Provider name (eligible-indirect-only) | `F_SCH_C_PART1_ITEM1` | `PROVIDER_ELIGIBLE_NAME` / `PROVIDER_ELIGIBLE_EIN` | TEXT |
| **Direct fees paid** | `F_SCH_C_PART1_ITEM2` | `PROVIDER_OTHER_DIRECT_COMP_AMT` | NUMERIC |
| **Indirect fees (provider total)** | `F_SCH_C_PART1_ITEM2` | `PROV_OTHER_TOT_IND_COMP_AMT` | NUMERIC |
| Indirect fees — by-payor detail | `F_SCH_C_PART1_ITEM3` | `PROVIDER_INDIRECT_NAME`, `PROVIDER_INDIRECT_COMP_AMT`, `PROVIDER_PAYOR_NAME` | TEXT / NUMERIC |
| Provider relationship to plan | `F_SCH_C_PART1_ITEM2` | `PROVIDER_OTHER_RELATION` | TEXT/25 |
| Service codes (what they did) | `F_SCH_C_PART1_ITEM2_CODES` | `SERVICE_CODE` (grandchild, ON `ACK_ID+ROW_ORDER`) | TEXT/2 |
| Provider who failed to disclose | `F_SCH_C_PART2` | `PROVIDER_FAIL_NAME`, `PROVIDER_FAIL_EIN` | TEXT |
| Terminated accountant/actuary | `F_SCH_C_PART3` | `PROVIDER_TERM_NAME`, `PROVIDER_TERM_EIN`, `PROVIDER_TERM_POSITION` | TEXT |

### 3.4 Financial flow — insurance & brokers (Schedule A, the "commissions")

| Signal | Dataset | Column | Type |
|---|---|---|---|
| Insurance broker/agent name | `F_SCH_A_PART1` | `INS_BROKER_NAME` | TEXT/35 |
| Broker address | `F_SCH_A_PART1` | `INS_BROKER_US_ADDRESS1/2`, `_CITY`, `_STATE`, `_ZIP` | TEXT |
| **Broker commission paid (per broker)** | `F_SCH_A_PART1` | `INS_BROKER_COMM_PD_AMT` | NUMERIC |
| Broker fees paid (per broker) | `F_SCH_A_PART1` | `INS_BROKER_FEES_PD_AMT` (+ `_TEXT` purpose) | NUMERIC |
| Broker commission total (contract) | `F_SCH_A` | `INS_BROKER_COMM_TOT_AMT` | NUMERIC |
| Broker fees total (contract) | `F_SCH_A` | `INS_BROKER_FEES_TOT_AMT` | NUMERIC |
| Welfare retained commissions | `F_SCH_A` | `WLFR_RET_COMMISSIONS_AMT` | NUMERIC |
| Welfare premiums received | `F_SCH_A` | `WLFR_PREMIUM_RCVD_AMT` | NUMERIC |
| Persons covered (EOY) | `F_SCH_A` | `INS_PRSN_COVERED_EOY_CNT` | TEXT/7 |
| Policy term | `F_SCH_A` | `INS_POLICY_FROM_DATE` / `INS_POLICY_TO_DATE` | TEXT/10 |

### 3.5 Secondary commercial signals (relational reach beyond the brief)

| Signal | Dataset | Column |
|---|---|---|
| Contributing employers (multiemployer) | `F_SCH_R_PART1` | `PEN_CONTRIB_EMPLR_NAME`, `PEN_CONTRIB_EMPLR_EIN`, `PEN_CONTRIB_EMPLR_AMT` |
| Participating employers (MEP/PEP) | `F_SCH_MEP_PART2` | `MEP_PARTICIPATING_EMPLR_NAME`, `MEP_PARTICIPATING_EMPLR_EIN`, `MEP_AGGREGATE_ACCOUNT_BALANCE_AMT` |
| Investment-entity (DFE) linkage | `F_SCH_D_PART1/2` | `DFE_P1_ENTITY_NAME`, `DFE_P1_SPONS_NAME`, `DFE_P1_PLAN_EIN`, `DFE_P1_PLAN_PN` |
| Pension benefit-payment vehicle | `F_SCH_R` | `PEN_EMPLR_CONTRIB_PAID_AMT`, `PEN_EMPLR_CONTRIB_RQR_AMT` |
| Plan asset transfers (acquisition signal) | `F_SCH_H_PART1` / `F_5500_SF_PART7` | `PLAN_TRANSFER_NAME/EIN/PN` · `SF_PLAN_TRANSFER_NAME/EIN/PN` |

---

## 4. Dataset inventory (40 mapped + 1 skipped)

| Dataset (zip stem) | Cols | Grain | Purpose / highest signal |
|---|--:|---|---|
| `F_5500` | 140 | Filing head (`ACK_ID`) | **Root entity.** Sponsor + admin identity, addresses, participant counts. The spine of §3.1/§3.2-participants. |
| `F_5500_SF` | 191 | Filing head | Small-plan all-in-one: identity + assets + contributions + participants in one row. |
| `F_5500_SF_PART7` | 5 | Detail | SF plan-to-plan transfers (`SF_PLAN_TRANSFER_*`). |
| `F_SCH_A` | 90 | Sched head (`+FORM_ID`) | **Insurance contracts** — premiums, persons covered, broker comm/fee totals. |
| `F_SCH_A_PART1` | 19 | Detail (`+FORM_ID`) | **Per-broker commission/fee rows.** §3.4. |
| `F_SCH_C` | 2 | Sched stub | `PROVIDER_EXCLUDE_IND` flag only — presence marker for the C subtables. |
| `F_SCH_C_PART1_ITEM1` | 15 | Detail | Providers receiving only eligible indirect comp (name+EIN+addr). |
| `F_SCH_C_PART1_ITEM2` | 22 | Detail | **Key service providers — direct & total-indirect comp.** §3.3 core. |
| `F_SCH_C_PART1_ITEM2_CODES` | 4 | Grandchild | Service codes per Item-2 provider. |
| `F_SCH_C_PART1_ITEM3` | 19 | Detail | **Indirect-comp sources** (payor name + amount). |
| `F_SCH_C_PART1_ITEM3_CODES` | 4 | Grandchild | Service codes per Item-3 source. |
| `F_SCH_C_PART2` | 17 | Detail | Providers who failed to furnish disclosure. |
| `F_SCH_C_PART2_CODES` | 4 | Grandchild | Service codes per Part-2 provider. |
| `F_SCH_C_PART3` | 19 | Detail | Terminated accountants/actuaries. |
| `F_SCH_D` | 5 | Sched head | DFE/participating-plan registry head. |
| `F_SCH_D_PART1` | 8 | Detail | Plan's interests **in** DFEs (`DFE_P1_*`). |
| `F_SCH_D_PART2` | 6 | Detail | Plans participating **in this** DFE (`DFE_P2_*`). |
| `F_SCH_G` | 5 | Sched head | Financial-schedule head (loans/leases/non-exempt txns). |
| `F_SCH_G_PART1` | 22 | Detail | Loans/fixed-income in default (`LNS_DEFAULT_*`). |
| `F_SCH_G_PART2` | 12 | Detail | Leases in default (`LEASES_DEFAULT_*`). |
| `F_SCH_G_PART3` | 12 | Detail | Non-exempt (prohibited) transactions. |
| `F_SCH_H` | 166 | Sched head | **Large-plan financials** — assets, liabilities, income, expenses, contributions, distributions. §3.2 core. |
| `F_SCH_H_PART1` | 5 | Detail | Large-plan transfers in/out (acquisition signal). |
| `F_SCH_I` | 77 | Sched head | **Small-plan financials** (`SMALL_*` mirror of H). |
| `F_SCH_I_PART1` | 5 | Detail | Small-plan transfers. |
| `F_SCH_R` | 69 | Sched head | Retirement-plan info — distributions, funding, employer contributions. |
| `F_SCH_R_PART1` | 11 | Detail | **Contributing employers** (multiemployer) name+EIN+amount. |
| `F_SCH_MB` | 138 | Sched head | Multiemployer DB actuarial (funding, liabilities). |
| `F_SCH_MB_PART1` | 5 | Detail | MB Q3 contributions receivable. |
| `F_SCH_MB_PART2` | 5 | Detail | MB Q7 amortization bases. |
| `F_SCH_MB_ACTV_PART_DATA` | 7 | Detail | Active-participant age/service/comp distribution. |
| `F_SCH_MB_EMP_CONT_WITH_LIAB` | 6 | Detail | Projected employer contributions & withdrawal-liability payments. |
| `F_SCH_MB_PROJ_EXP_BENEF_PAY` | 7 | Detail | Projected expected benefit payments. |
| `F_SCH_MB_WITHD_LIAB_AMT` | 6 | Detail | Withdrawal-liability amounts. |
| `F_SCH_SB` | 124 | Sched head | Single-employer DB actuarial (funding target, MRC). |
| `F_SCH_SB_PART1` | 5 | Detail | SB Q18 contributions. |
| `F_SCH_SB_ACTV_PART_DATA` | 7 | Detail | Active-participant age/service/comp distribution. |
| `F_SCH_SB_EXPECT_BEN_PAYMTS` | 7 | Detail | Projected expected benefit payments. |
| `F_SCH_MEP` | 12 | Sched head | Multiple-Employer-Plan head (type, participating-employer count). |
| `F_SCH_MEP_PART2` | 6 | Detail | **Participating employers** name+EIN+account balance. |
| `F_SCH_DCG` *(skipped)* | — | — | No layout in manifest (`secondary_url` is the Plan-Transfer zip). Not fetched. |

**Universe totals:** 40 datasets · 1,289 columns · 1 head form pair (Tier 0) · 10 schedule heads (Tier 1) · 26 detail tables (Tier 2) · 3 service-code grandchildren (Tier 3).

---

## 5. Ingest blueprint (structural recommendations, in dependency order)

1. **Land the four spine tables first** — `F_5500`, `F_5500_SF`, `F_SCH_H`, `F_SCH_I`. These resolve identity + scale for the entire universe and unblock every downstream join.
2. **Hard `BTREE` scalar index on `ACK_ID`** for every one of the 40 datasets — it is the universal join key; no analysis joins without it.
3. **`BTREE` on the composite `(SPONS_DFE_EIN, SPONS_DFE_PN)`** (and the SF equivalent) on the head forms — the cross-year resolution and amended-filing dedup key.
4. **`BTREE` on `FORM_ID`** for `F_SCH_A` + `F_SCH_A_PART1` — the only schedule needing a composite (`ACK_ID+FORM_ID`) join; index both sides.
5. **Cast at the DuckDB projection boundary, not after.** Every `*_AMT`/`*_CNT` is emitted `NUMERIC` with no declared size — cast to `DECIMAL/BIGINT` on the way into Lance. `EIN`/`PN`/`ACK_ID` are `TEXT` with leading-zero significance — **never** coerce to integer (drops the leading zero, corrupts the key).
6. **Resolve the H ∪ I ∪ SF asset/contribution union into one derived `plan_financials` view** keyed on `ACK_ID`, so consumers read assets once instead of three-way-coalescing per query.
7. **Treat all detail-table `*_EIN` as counterparty entities** — route Schedule C provider EINs, Schedule R/MEP employer EINs, and Schedule D DFE EINs to the external entity-resolution hub; do not join them to the plan sponsor.
8. **Defer or drop the actuarial long tables** (`MB_*`, `SB_*` participant-data/projection sets) for a commercial-signal build — high row count, near-zero entity/fee signal.
9. **DCG gap:** if Defined-Contribution-Group coverage is later required, fetch `F_SCH_DCG_2025_latest_layout.txt` directly (derive the URL from the manifest pattern) — it is absent from the manifest's `secondary_url` column.

---

## Appendix — Provenance

- **Inputs:** `data-sink/landing/form-5500/form5500_2025_files.csv` (41 rows) → 40 remote `*_layout.txt` fetched from `https://askebsa.dol.gov/FOIA Files/2025/Latest/`, all `HTTP 200`.
- **Layout grammar:** `FIELD_POSITION,FIELD_NAME,TYPE,SIZE(text only)` after a `===` rule; `TYPE ∈ {TEXT, NUMERIC}`; `NUMERIC` carries no declared size.
- **No descriptions in source:** the dissemination layouts expose name+type+size only. All semantic labels in §3 are resolved from the EFAST2 naming convention and form line-item mapping, not from in-file text.
- **Constraint honored:** zero `data_file` archives downloaded; schema-level diagnostic only. Row counts, fill rates, and join cardinalities are **structural (schema-derived)**, not measured — measurement requires landing the data.
- **Local landing note:** `data-sink/landing/form-5500/` does **not** exist in this worktree; the manifest was read from the operator-attached copy at `~/Downloads/form5500_2025_files.csv` (byte-identical to the cited landing path).
