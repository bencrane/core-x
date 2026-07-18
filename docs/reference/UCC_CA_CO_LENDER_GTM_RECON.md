# CA + CO UCC — Lender-GTM Reconnaissance

Read-only DuckDB/Lance probe of the California and Colorado UCC datasets in the Gen-3
system of record (`s3://data-sink/active/`, Cloudflare R2). Scopes the **secured-party
(lender)** surface for a GTM targeting alternative lenders / equipment financiers.

- **Harness (reproducible, non-mutating):** [`scripts/archive/ucc_ca_co_recon_probe.py`](../../scripts/archive/ucc_ca_co_recon_probe.py)
  — `doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb python3 scripts/archive/ucc_ca_co_recon_probe.py`
- **Attestation:** every figure is from `lance.dataset(...).count_rows()/scanner()/list_indices()` + DuckDB `SELECT`. Zero writes.
- **Snapshot:** CA `as_of 2026-05-31`; CO ledger `2026-05-31`; CO companion party tables `2026-05-08`. Probe run 2026-06-21.

---

## 0. Headline

1. **No lender taxonomy column exists — in either state.** There is no `lender_type`,
   `is_bank`, `lender_category`, `naics_code`, `sic`, or `institution_type` on any of the
   9 datasets. The only "type" column on a secured-party table is CA `secured_party_type`,
   and it is a **syntactic Organization/Individual flag** (98.7% / 1.3%), not a lender class.
2. **No LLM / inference artifacts anywhere.** Zero `_llm` / `_confidence` / `_extracted` /
   `_score` / `_model` / probability / embedding columns across all 9 datasets. Lender
   categorization is therefore **neither state ground-truth nor a prior LLM inference — it
   does not exist yet.** This is green-field: no prior subjective labels to trust-check.
3. **Lender NATURE is latent, not stored.** Bank vs non-bank vs equipment-lessor is only
   recoverable from (a) `filing_type` (consensual UCC vs involuntary statutory lien),
   (b) CA `alt_designation_type = Lessee/Lessor` (true equipment leases), and (c) the
   secured-party **name string** — which fragments and is masked by filing agents.
4. **Volume: 35.79M rows total** (CA 27.51M / CO 8.28M) — but that is event/party grain.
   Distinct UCC-1 originations ≈ **4.44M (CA)** + **1.50M (CO)**; secured-party rows
   **4.74M (CA)** + **2.06M (CO)**.

---

## 1. Table discovery & schema

Nine Lance datasets across two namespaces. CA bundles parties under `ca_ucc/`; CO splits
into a Gen-3-native bulk ledger (`co_ucc_transactions`) plus three companion tables
(`ucc_co_*`) **migrated out of the now-purged Gen-2 Polaris/Iceberg lake** — different
provenance lineage and a different (older) snapshot.

| # | Dataset (`s3://data-sink/active/…`) | State | Rows | Grain / role | Indices |
|--:|---|:--:|--:|---|--:|
| 1 | `ca_ucc/filings/` | CA | 7,751,890 | 1 / filing **event** (master) | 5 |
| 2 | `ca_ucc/debtors/` | CA | 5,855,416 | N / filing — debtor | 9 |
| 3 | **`ca_ucc/secured_parties/`** | CA | **4,743,627** | **N / filing — SECURED PARTY (lender)** | 9 |
| 4 | `ca_ucc/filing_amendments/` | CA | 3,305,823 | UCC3→UCC1 amendment bridge | 3 |
| 5 | `ca_ucc/debtor_index/` | CA | 5,855,416 | 1 / debtor appearance (derived, nested SP rollup) | 8 |
| 6 | `co_ucc_transactions/` | CO | 2,555,824 | 1 / filing **transaction** (ledger; **no party strings**) | 9 |
| 7 | `ucc_co_debtors/` | CO | 1,985,901 | N / filing — debtor companion | 8 |
| 8 | **`ucc_co_secured_parties/`** | CO | **2,055,777** | **N / filing — SECURED PARTY (lender) companion** | 8 |
| 9 | `ucc_co_collateral/` | CO | 1,682,948 | N / filing — collateral companion | 5 |

**Join keys:** CA on `ucc1_num` / `ucc3_num`; CO companions on `file_id` back to
`co_ucc_transactions`. Both secured-party tables are BTREE-indexed on name + the join key
→ sub-second resolution.

### 1.1 Primary lender table — CA `ca_ucc/secured_parties` (4,743,627 rows, 18 cols)

| Column | Type | | Column | Type |
|---|---|---|---|---|
| `ucc1_num` | string (BTREE) | | `addr1` `addr2` `addr3` | string |
| `ucc3_num` | string (BTREE) | | `city` | string (BTREE) |
| `secured_party_type` | string (**BITMAP**) | | `state` | string (BITMAP) |
| `org_name` | string (BTREE) | | `postal_code` | string (BTREE) |
| `last_name` | string (BTREE) | | `country` | string (BITMAP) |
| `first_name` `middle_name` `suffix` | string | | `source_file` `as_of` `ingested_at` | provenance |

### 1.2 Primary lender table — CO `ucc_co_secured_parties` (2,055,777 rows, 24 cols)

| Column | Type | | Column | Type |
|---|---|---|---|---|
| `file_id` | string (BTREE) | | `address1` `address2` `city` | string |
| `secured_party_id` | string | | `state` | string (BITMAP) |
| `action_type_code` / `action_type` | string (BITMAP) | | `zipcode` `country` | string |
| `record_status_code` / `record_status` | string (BITMAP) | | `party_name_normalized` | string (BTREE) — *derived* |
| `organization_name` | string (BTREE) | | `party_zip5` | string (BTREE) — *derived* |
| `last_name` | string (BTREE) | | `party_state_normalized` | string — *derived* |
| `first_name` `middle_name` | string | | `party_role` | string (const `secured_party`) |
| `assignor` | string | | `snapshot_date` `source_file` `ingested_at` | provenance |

> **CO has no `secured_party_type` analog at all.** `party_role` is a constant
> (`secured_party`, 100%); `action_type` / `record_status` are CRUD/lifecycle flags. The
> only individual-vs-org signal is indirect (population of `organization_name` vs `last_name`).
> The `party_name_normalized` / `party_zip5` / `party_state_normalized` columns are
> **deterministic normalization** carried over from the Gen-2 migration — not inference.

---

## 2. Lender classification pulse check

### 2.1 The only secured-party "type" column — CA `secured_party_type` (syntactic, not a lender class)

| `secured_party_type` | Rows | Share |
|---|--:|--:|
| Organization | 4,684,136 | 98.7% |
| Individual | 59,491 | 1.3% |

This separates an org from a human. **It does not separate banks from non-banks or
equipment lessors.** No such column exists.

### 2.2 Where lender NATURE actually lives — `filing_type` (the consensual-vs-statutory split)

The highest-frequency "secured parties" are **government statutory-lien filers**, not
lenders. The single most important GTM filter is `filing_type`:

| CA `filings.filing_type` | Rows | Share | | CO `transactions.filing_type` | Rows | Share |
|---|--:|--:|---|---|--:|--:|
| **UCC** (consensual) | 5,542,772 | **71.5%** | | **ucc** (consensual) | 2,080,081 | **81.4%** |
| Notice of State Tax Lien | 1,961,550 | 25.3% | | lien_irs (IRS) | 262,185 | 10.3% |
| Notice of Federal Tax Lien | 170,877 | 2.2% | | lien_hosp (hospital) | 97,415 | 3.8% |
| Judgment Lien | 75,370 | 1.0% | | efs (ag/effective financing stmt) | 79,699 | 3.1% |
| Pension/Attachment/Estate | 1,321 | <0.1% | | lien_othr_stat + 5 fed/state | 36,444 | 1.4% |

> **For "business pledged assets as collateral for commercial debt," filter
> `filing_type = 'UCC'` (CA) / `'ucc'` (CO).** ~28.5% (CA) / ~18.6% (CO) is involuntary
> statutory-lien noise filed by tax authorities, hospitals, courts — not addressable lenders.

### 2.3 The closest native "equipment financier" flag — CA `alt_designation_type`

| CA `filings.alt_designation_type` | Rows | Read |
|---|--:|---|
| No Value / Not Applicable | 7,479,255 | standard security interest |
| **Lessee/Lessor** | **222,202** | **true equipment lease** |
| Seller/Buyer | 39,747 | PMSI / conditional sale |
| Bailee/Bailor · Consignee/Consignor · Licensee/Licensor | 10,686 | specialty |

CO carries no `alt_designation_type`; its nearest signal is `document_type` (43 values,
e.g. *UCC financing statement* 47.1%) and `financial_statement_type` (99.7% NULL — unusable).

### 2.4 CO companion lifecycle/role columns (not a lender taxonomy)

| `ucc_co_secured_parties.action_type` | Rows | | `record_status` | Rows |
|---|--:|---|---|--:|
| add | 1,995,352 (97.1%) | | active | 1,987,472 (96.7%) |
| change only | 52,083 (2.5%) | | inactive | 68,305 (3.3%) |
| delete only / change and delete | 8,342 (0.4%) | | | |

`party_role` is a constant `secured_party` (100%). None of these classify the lender.

---

## 3. Provenance & LLM artifacts

**Column-name audit across all 9 datasets** (regex buckets: `_llm`/`_confidence`/
`_extracted`/`_score`/`_model`/`_inferred`/probability/embedding vs lender-class vs hard lineage):

| Signal class | Hits |
|---|---|
| `llm_or_inferred` | **0 — none, on any dataset** |
| `lender_classification` (`lender`/`is_bank`/`naics`/`category`/…) | **0 — none, on any dataset** |
| `hard_provenance` | `source_file`, `as_of` (CA) / `snapshot_date` (CO), `ingested_at` — **every** dataset |
| `nested` | only CA `debtor_index.secured_parties` — a `LIST<STRUCT>` deterministic rollup |

**What the provenance columns mean:** they are deterministic ingest breadcrumbs written by
the DuckDB→Lance worker — *which* source CSV/parquet (`source_file`), *which* snapshot
(`as_of` / `snapshot_date`), and *when* the fragment was written (`ingested_at`). They
record lineage, not judgment.

**Verdict on the operator's core question:** the lender categorization is **neither a hard
ground-truth fact from the state nor a subjective inference from a prior LLM run — it has
never been generated.** Every column in these tables is either (a) source-faithful state
data or (b) byte-deterministic transform output (trim/nullif/cast, name-normalization,
struct rollup). There are **no prior LLM labels to trust or untangle.** A bank /
non-bank / equipment-lessor taxonomy is a clean downstream build.

---

## 4. Row volume

| Grain | CA | CO |
|---|--:|--:|
| **Total rows (all datasets)** | **27,512,172** | **8,280,450** |
| Filing **events / transactions** | 7,751,890 | 2,555,824 |
| Distinct UCC-1 **originations** (`Lien Financing Stmt` / `Initial Filing`) | 4,437,111 | 1,501,275 |
| **Secured-party (lender) rows** | 4,743,627 | 2,055,777 |
| Debtor rows | 5,855,416 | 1,985,901 |
| Collateral rows | — (no CA collateral table) | 1,682,948 |

**Grand total: 35,792,622 rows across 9 datasets.**

**Coverage / vintage:** CA filings span `1965-01-04 → 2026-05-01` (snapshot `as_of`
`2026-05-31`); CO transactions span `1966-07-01 → 2026-05-29` (snapshot `2026-05-31`).
⚠️ **CO companion party tables are snapshot `2026-05-08`** — one snapshot (~3 weeks)
**behind** the CO ledger, and sourced from the retired Gen-2 lake. CO lender/debtor strings
therefore lag the CO transaction ledger; a refresh of the `ucc_co_*` companions is the
freshness gap to close before time-sensitive CO outreach.

---

## 5. Implications for the next pipeline step (lender classification build)

1. **It's a derivation, not a lookup.** No column to read — a `lender_class` must be
   manufactured. Inputs available: secured-party name string, `filing_type`,
   CA `alt_designation_type`, address/geo.
2. **Pre-filter to consensual debt first:** `filing_type ∈ {UCC, ucc}` strips ~28.5% (CA) /
   ~18.6% (CO) of statutory-lien noise before any classification spend.
3. **Name canonicalization is prerequisite** (per [CA_UCC_SOS_MAPPING_BLUEPRINT.md](CA_UCC_SOS_MAPPING_BLUEPRINT.md) §4.1):
   `NA`↔`NATIONAL ASSOCIATION`, brand `Inc`↔`LLC`, DBA folds, and **filing-agent masking**
   (`… AS REPRESENTATIVE` / CSC / CT Corp ≈ 200k+ filings hide the true lender) must be
   resolved before any bank/non-bank label is trustworthy.
4. **Time-windowing needs a join.** Neither secured-party table carries a filing date —
   join CA `secured_parties → filings` on `ucc1_num`/`ucc3_num`, CO `ucc_co_secured_parties
   → co_ucc_transactions` on `file_id`, for `filing_date` (e.g. 90-day hot leads).
5. **If LLM classification is chosen, label the secured-party name** (distinct, post-canonical
   — far fewer than 6.8M raw rows) and write the verdict as **new provenance-stamped columns**
   (`lender_class`, `lender_class_confidence`, `lender_class_model`) so the inference is
   explicitly separable from the source-faithful state data — the separation that is cleanly
   intact today.
