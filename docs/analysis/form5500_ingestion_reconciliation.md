# Form 5500 (2025) — Ingestion Reconciliation & Delta Diagnostic

**Result:** ✅ AUDIT COMPLETE — 41 staged archives · **7 integrated · 34 orphaned · 0 truncated** · patch applied + dry-run verified (4 new datasets, 27,996 rows, PASS)

**Mode:** Read-only audit (Parts 1–2) + verified pipeline patch (Part 3). No SoR mutation: the orphan zips were read from R2 landing (read-only) and the 4 new datasets were materialized to a **local** `--target` for verification only — nothing was written to `s3://data-sink/active/`.
**S3 landing zone:** `s3://data-sink/landing/form-5500/` (R2, operator-staged 2026-06-06 18:43 UTC) — 41 zips + manifest.
**Lake (SoR):** `s3://data-sink/active/form5500_*` (R2) — mirrored locally at `~/core-x-lake/active/*.lance`.
**Manifest:** `s3://data-sink/landing/form-5500/form5500_2025_files.csv` (41 rows).
**Date:** 2026-06-07 · **Vintage:** PY2025 "Latest" weekly dissemination.
**Lineage authority:** [`ingest_form5500.py`](../pipelines/form5500/ingest_form5500.py) `STEMS` registry · universe map from [`form5500_relational_diagnostic.md`](form5500_relational_diagnostic.md).

---

## Part 1 — State Capture

### 1.1 S3 landing manifest (41 archives, all physically staged)

Every `data_file` in the manifest is present in the landing prefix (`aws s3 ls` against R2, sizes verified). The non-ingested archives are therefore **genuinely orphaned** — staged, parseable, addressed by the manifest — not merely "not yet downloaded."

### 1.2 Local/SoR lake (7 materialized `form5500_*` datasets)

Confirmed in **both** planes — `s3://data-sink/active/` (7 prefixes) and `~/core-x-lake/active/` (7 `.lance` dirs, written 2026-06-06 20:15–20:27). Row/column census from the post-ingest diagnostic.

### 1.3 Lineage map — S3 zip → DuckDB transform → Lance dataset

| S3 archive (landing) | `STEMS` stem | Lance dataset | Rows | Src cols | Landed cols¹ |
|---|---|---|--:|--:|--:|
| `F_5500_2025_Latest.zip` | `F_5500` | `form5500_main` | 19,114 | 140 | 143 |
| `F_5500_SF_2025_Latest.zip` | `F_5500_SF` | `form5500_sf` | 199,363 | 191 | 194 |
| `F_SCH_H_2025_Latest.zip` | `F_SCH_H` | `form5500_sch_h` | 1,358 | 166 | 169 |
| `F_SCH_I_2025_Latest.zip` | `F_SCH_I` | `form5500_sch_i` | 10,059 | 77 | 80 |
| `F_SCH_C_PART1_ITEM2_2025_Latest.zip` | `F_SCH_C_PART1_ITEM2` | `form5500_sch_c_provider` | 3,774 | 22 | 25 |
| `F_SCH_C_PART1_ITEM2_CODES_2025_Latest.zip` | `F_SCH_C_PART1_ITEM2_CODES` | `form5500_sch_c_provider_code` | 8,117 | 4 | 7 |
| `F_SCH_A_PART1_2025_Latest.zip` | `F_SCH_A_PART1` | `form5500_sch_a_broker` | 34,358 | 19 | 22 |

¹ Landed = source cols **+ 3** provenance (`source_file`, `source_uri`, `ingested_at`). See §2.3.

---

## Part 2 — The Delta Audit

### 2.1 Fully Integrated (7 / 41)

The seven above. Each carries `BTREE(ACK_ID)`; the two head forms add per-column BTREE on the business identity (`SPONS_DFE_EIN`+`SPONS_DFE_PN` / `SF_SPONS_EIN`+`SF_PLAN_NUM`). 11 BTREE total, 0 tombstones, read-amp 1.00×.

> **Lineage correction (load-bearing):** the integrated `form5500_sch_a_broker` is `F_SCH_A_**PART1**` — the **per-broker commission detail** (`INS_BROKER_NAME`, `INS_BROKER_COMM_PD_AMT`). It does **not** carry carrier identity. `INS_CARRIER_NAME` / `INS_CARRIER_EIN` live on the **`F_SCH_A` head**, which is orphaned (§2.2). The broker detail also shipped **without a `FORM_ID` index**, so the composite `ACK_ID+FORM_ID` carrier↔broker join was unindexed on the detail side — backfilled by the patch.

### 2.2 Orphaned / Ignored Assets (34 / 41) — the critical path

The v1 ingest deliberately landed only the high-signal asset/participant/fee **spine** and deferred the long tail (`ingest_form5500.py` docstring). The deferral stranded the carrier-identity head and the bulk of the Schedule C counterparty graph. Tiered by commercial signal:

#### Tier A — Carrier identity + Schedule C counterparty completion (8) — **PATCHED**

| Orphan archive | Rows | Trapped high-value data |
|---|--:|---|
| **`F_SCH_A`** | **23,648** | **Insurance-contract head — `INS_CARRIER_NAME`, `INS_CARRIER_EIN`, `INS_CARRIER_NAIC_CODE`, `INS_CONTRACT_NUM`; welfare premiums/claims/retention (`WLFR_*`); broker comm/fee totals. The carrier-identity key for TiC triage.** |
| `F_SCH_C_PART1_ITEM3` | 2,656 | Indirect-comp **by payor** — `PROVIDER_INDIRECT_COMP_AMT`, `PROVIDER_PAYOR_NAME`/`_EIN`. PBM/consultant indirect-revenue flows. |
| `F_SCH_C_PART1_ITEM1` | 1,611 | Eligible-indirect-only providers — `PROVIDER_ELIGIBLE_NAME`/`_EIN` (+ address). |
| `F_SCH_C_PART3` | 81 | Terminated accountants/actuaries — `PROVIDER_TERM_NAME`/`_EIN`/`_POSITION`. |
| `F_SCH_C` | — | Stub: `PROVIDER_EXCLUDE_IND` presence flag (deferred — marker only). |
| `F_SCH_C_PART1_ITEM3_CODES` | — | Service codes per ITEM3 source (deferred — lookup grandchild). |
| `F_SCH_C_PART2` | — | Providers who failed to furnish disclosure (deferred — sparse). |
| `F_SCH_C_PART2_CODES` | — | Service codes per Part-2 (deferred — lookup grandchild). |

> The Schedule C **fee core** (`F_SCH_C_PART1_ITEM2`, direct + total-indirect comp) was **already integrated** as `form5500_sch_c_provider`. The TPA/PBM gap is therefore the **indirect-by-payor** detail (ITEM3) + the eligible/terminated provider identities — not the whole schedule.

#### Tier B — Financial & relational reach (10) — backlog, recommend next

| Orphan archive | Trapped data |
|---|---|
| `F_SCH_R` / `F_SCH_R_PART1` | Retirement-plan funding; **contributing employers** (multiemployer) `PEN_CONTRIB_EMPLR_NAME`/`_EIN`/`_AMT`. |
| `F_SCH_MEP` / `F_SCH_MEP_PART2` | **Participating employers** (MEP/PEP) `MEP_PARTICIPATING_EMPLR_NAME`/`_EIN` + account balance. |
| `F_SCH_D` / `F_SCH_D_PART1` / `F_SCH_D_PART2` | DFE / investment-entity linkage (`DFE_P1_*` / `DFE_P2_*`). |
| `F_SCH_H_PART1` / `F_SCH_I_PART1` / `F_5500_SF_PART7` | Plan asset transfers in/out — **M&A / plan-takeover signal**. |

#### Tier C — Defer (16) — low commercial-signal density

`F_SCH_G`(+P1/P2/P3) loans/leases/prohibited-txn defaults · `F_SCH_MB`(+6 actuarial long tables) · `F_SCH_SB`(+3 actuarial long tables) · `F_SCH_DCG` (**no `*_layout.txt` in the manifest** — `secondary_url` points at the Plan-Transfer companion zip; fetch the DCG layout directly if coverage is later required). High row volume, near-zero entity/fee signal — consistent with the v1 "defer the actuarial long tail" decision.

**Tally:** 8 (Tier A) + 10 (Tier B) + 16 (Tier C) = **34 orphaned**; + 7 integrated = **41 manifest rows.** ✓

### 2.3 Schema Drift / Truncation — **none found**

The hypothesis (the ingest drops source columns) is **disproven** for every integrated table. The transform reads the CSV **all-string from the header** (`zip_csv_to_arrow`) and the projection emits **every** source column (`build_projection` iterates the full Arrow column list) plus exactly 3 provenance columns. The arithmetic closes on all 7: landed = source **+ 3** (143=140+3, 194=191+3, 169=166+3, 80=77+3, 25=22+3, 7=4+3, 22=19+3). The design **structurally cannot** drop a column.

The only deviations are **intentional type bindings**, not column loss:
- `FORM_ID` is `NUMERIC` in the DOL layout but pinned `VARCHAR` (`FORCE_STRING`) — a composite-join key with structural significance; a numeric cast would be the corruption.
- `*_EIN` / `*_PN` keys are `TEXT` in the layout and pinned `VARCHAR` — leading-zero retention (proven: `SPONS_DFE_EIN LIKE '0%'` → 706; `SF_SPONS_EIN` → 7,330).
- `INS_PRSN_COVERED_EOY_CNT` is `TEXT/7` in the layout but lands `int64` (the `_CNT` rule) — a count, correctly typed.

No truncation debt exists on the integrated set. The reconciliation gap is **breadth (missing tables), not depth (dropped columns).**

---

## Part 3 — Ingestion STEMS Patch

**Architecture note (defended):** this pipeline is **layout-driven**, not schema-hardcoded. The type contract is derived at runtime from the EFAST2 `*_layout.txt` + four precedence rules (`FORCE_STRING` → `NUMERIC`/`_AMT`→`DOUBLE` → `_CNT`→`BIGINT` → `VARCHAR`). Hardcoding Arrow/Polars dtypes would fork a second source of truth that silently drifts from the DOL layout on every weekly dissemination. The correct patch is therefore **registry entries + key declarations** — *not* literal dtype maps. The resolved schemas (§3.3) are shown for reference; the pipeline computes them.

### 3.1 Extraction target list (new archives)

```python
# Added to the STEMS registry (the manifest already lists all four; the pipeline
# resolves each by stem from form5500_2025_files.csv — no separate download array exists).
NEW_ARCHIVES = [
    "F_SCH_A_2025_Latest.zip",              # carrier-identity head
    "F_SCH_C_PART1_ITEM3_2025_Latest.zip",  # indirect comp by payor (PBM/consultant)
    "F_SCH_C_PART1_ITEM1_2025_Latest.zip",  # eligible-indirect-only providers
    "F_SCH_C_PART3_2025_Latest.zip",        # terminated accountants/actuaries
]
```

### 3.2 Applied patch — `ingest_form5500.py`

**`STEMS` registry** (+4 datasets; `sch_a_broker` `biz_keys` backfilled with `FORM_ID`):

```python
{"stem": "F_SCH_A_PART1",       "name": "sch_a_broker",     "biz_keys": ["FORM_ID"], "lz_col": None},  # was []
# ── Reconciliation patch ──
{"stem": "F_SCH_A",             "name": "sch_a_carrier",    "biz_keys": ["FORM_ID", "SCH_A_EIN", "SCH_A_PLAN_NUM"], "lz_col": "INS_CARRIER_EIN"},
{"stem": "F_SCH_C_PART1_ITEM1", "name": "sch_c_eligible",   "biz_keys": [], "lz_col": "PROVIDER_ELIGIBLE_EIN"},
{"stem": "F_SCH_C_PART1_ITEM3", "name": "sch_c_indirect",   "biz_keys": [], "lz_col": "PROVIDER_PAYOR_EIN"},
{"stem": "F_SCH_C_PART3",       "name": "sch_c_terminated", "biz_keys": [], "lz_col": None},
```

**`FORCE_STRING`** (+6 keys — pin to VARCHAR regardless of layout type):

```python
"SCH_A_EIN", "SCH_A_PLAN_NUM",                 # Schedule A denormalized sponsor identity
"INS_CARRIER_EIN", "INS_CARRIER_NAIC_CODE",    # carrier identity (NAIC: TEXT/5, leading zeros)
"PROVIDER_PAYOR_EIN", "PROVIDER_TERM_EIN",     # Schedule C counterparty EINs
```

### 3.3 Target schemas (resolved — what the layout-driven rules land)

**`form5500_sch_a_carrier`** (`F_SCH_A`, 90 src cols → 93 landed; **45 numeric / 45 string** + 3 provenance). Carrier-identity head:

| Column | DOL layout | Landed (Arrow) | Rule |
|---|---|---|---|
| `ACK_ID` | TEXT/30 | `string` | FORCE_STRING (hub key) |
| `FORM_ID` | **NUMERIC** | `string` | FORCE_STRING (contract discriminator) |
| `SCH_A_EIN` | TEXT/9 | `string` | FORCE_STRING (sponsor EIN) |
| `SCH_A_PLAN_NUM` | TEXT/3 | `string` | FORCE_STRING (plan number) |
| `INS_CARRIER_NAME` | TEXT/70 | `string` | TEXT |
| `INS_CARRIER_EIN` | TEXT/9 | `string` | FORCE_STRING (carrier EIN) |
| `INS_CARRIER_NAIC_CODE` | TEXT/5 | `string` | FORCE_STRING (NAIC registry code) |
| `INS_CONTRACT_NUM` | TEXT/15 | `string` | TEXT |
| `INS_PRSN_COVERED_EOY_CNT` | TEXT/7 | `int64` | `_CNT` |
| `INS_BROKER_COMM_TOT_AMT`, `WLFR_PREMIUM_RCVD_AMT`, `WLFR_RET_COMMISSIONS_AMT`, … | NUMERIC | `double` | `_AMT`/NUMERIC |

**`form5500_sch_c_indirect`** (`F_SCH_C_PART1_ITEM3`, 19 src → 22 landed; **2 numeric / 17 string** + 3 provenance):

| Column | Landed (Arrow) | Rule |
|---|---|---|
| `ACK_ID` | `string` | FORCE_STRING |
| `ROW_ORDER` | `double` | NUMERIC |
| `PROVIDER_INDIRECT_NAME` | `string` | TEXT |
| `PROVIDER_INDIRECT_SRVC_CODES` | `string` | TEXT |
| `PROVIDER_INDIRECT_COMP_AMT` | `double` | `_AMT` |
| `PROVIDER_PAYOR_NAME` | `string` | TEXT |
| `PROVIDER_PAYOR_EIN` | `string` | FORCE_STRING |
| `PROVIDER_PAYOR_US_*` / foreign addr / `PROVIDER_COMP_EXPLAIN_TEXT` | `string` | TEXT |

### 3.4 Foreign-key / join map

```
F_5500 / F_5500_SF  ──ACK_ID──▶  F_SCH_A (carrier)        1 : 0..N   (one filing → N insurance contracts)
F_SCH_A             ──ACK_ID + FORM_ID──▶  F_SCH_A_PART1   1 : N      (carrier contract → broker rows)   ← composite, now BTREE both sides
F_5500 / F_5500_SF  ──ACK_ID──▶  F_SCH_C_PART1_ITEM1/3, PART3   1 : N  (filing → Sch C detail rows)
F_SCH_A cross-year  ──SCH_A_EIN + SCH_A_PLAN_NUM──▶  prior vintage    (= SPONS_DFE_EIN + SPONS_DFE_PN on the head)
```

| Join | Left key | Right key | Cardinality |
|---|---|---|---|
| Filing → carrier | `F_5500.ACK_ID` | `F_SCH_A.ACK_ID` | 1 : 0..N |
| **Carrier → broker** | `F_SCH_A.(ACK_ID, FORM_ID)` | `F_SCH_A_PART1.(ACK_ID, FORM_ID)` | 1 : N |
| Filing → Sch C detail | `F_5500.ACK_ID` | `F_SCH_C_PART1_ITEM{1,3}.ACK_ID` / `F_SCH_C_PART3.ACK_ID` | 1 : N |
| Cross-year plan continuity | `(SCH_A_EIN, SCH_A_PLAN_NUM)` | prior `(EIN, PN)` | N : N |

> **Counterparty discipline:** `INS_CARRIER_EIN`, `PROVIDER_PAYOR_EIN`, `PROVIDER_ELIGIBLE_EIN`, `PROVIDER_TERM_EIN` identify the **carrier/provider counterparty**, never the plan sponsor. The sponsor key binds through `ACK_ID` only. Resolve counterparty EINs against the external entity graph (SAM / NPPES / Open-Payments); for carriers, `INS_CARRIER_NAIC_CODE` is the NAIC-registry join for TiC triage.

### 3.5 Verification (local dry-run, no SoR mutation)

```
doppler run --project core-x --config prd -- uv run pipelines/form5500/ingest_form5500.py \
  --target /tmp/f5500_verify --only sch_a_carrier,sch_c_eligible,sch_c_indirect,sch_c_terminated
```

| Dataset | Rows | Landed cols | BTREE | ACK_ID null | Leading-zero proof | Status |
|---|--:|--:|---|--:|---|:--:|
| `form5500_sch_a_carrier` | 23,648 | 93 | `ACK_ID, FORM_ID, SCH_A_EIN, SCH_A_PLAN_NUM` | 0 | `INS_CARRIER_EIN LIKE '0%'` → **2,823** | ✅ PASS |
| `form5500_sch_c_eligible` | 1,611 | 18 | `ACK_ID` | 0 | `PROVIDER_ELIGIBLE_EIN` → 167 | ✅ PASS |
| `form5500_sch_c_indirect` | 2,656 | 22 | `ACK_ID` | 0 | `PROVIDER_PAYOR_EIN` → 128 | ✅ PASS |
| `form5500_sch_c_terminated` | 81 | 22 | `ACK_ID` | 0 | — (n/a) | ✅ PASS |

27,996 rows · 7 BTREE · all checks green. The carrier EIN landed as `string` with 2,823 leading-zero values intact — the TiC triage key is uncorrupted.

### 3.6 Execute against the SoR

The patch ships the pipeline change; it does **not** materialize to R2 (blast-radius separation — code vs. SoR mutation). To publish the four new datasets to the Gen-3 SoR:

```bash
doppler run --project core-x --config prd -- uv run pipelines/form5500/ingest_form5500.py \
  --only sch_a_carrier,sch_c_eligible,sch_c_indirect,sch_c_terminated
# (default --target s3://data-sink/active → clean delete-prefix → upload-ordered → read-back verify)
```

**Backlog (next):** Tier B — `F_SCH_R_PART1` (contributing employers) + `F_SCH_MEP_PART2` (participating employers) + the transfer tables (M&A signal). Tier C remains deferred.

---

## Appendix — Provenance

- **Landing inventory:** `aws s3 ls s3://data-sink/landing/form-5500/` via R2 (Doppler `core-x/prd`), 41 zips + manifest, sizes verified.
- **Active inventory:** `aws s3 ls s3://data-sink/active/` (7 `form5500_*` prefixes) + `~/core-x-lake/active/` (7 `.lance`).
- **Orphan column truth:** CSV headers extracted from the staged zips (`F_SCH_A`, `F_SCH_C_PART1_ITEM1/ITEM3`, `F_SCH_C_PART3`) + EFAST2 `F_SCH_A_2025_Latest_layout.txt` (HTTP 200, 90 cols, 45 TEXT / 45 NUMERIC).
- **Row counts:** integrated set from `form5500_post_ingest_diagnostic.md`; orphan set measured by decompressing the staged CSVs (`F_SCH_A` 23,648 · ITEM1 1,611 · ITEM3 2,656 · PART3 81).
- **Patch verification:** local `--target` dry-run, 2026-06-07T15:53Z, PASS. Zero writes to `s3://data-sink/active/`.
</content>
</invoke>
