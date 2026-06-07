# Gen-2 (`dex-raw-landing-zone`) vs Gen-3 (`data-sink`) — Differential Diagnostic

> **🟥 SUPERSEDED — bucket PURGED 2026-06-07.** This diagnostic's verdict ("not safe to
> bulk-delete") reflected the state *before* the operator confirmed DEX / polaris-server
> MCP / `data-engine-x` were fully decommissioned. With those readers/writers gone, the
> bucket was deleted in full (1.682 TiB). The live-dependency findings below are retained
> for history. Full catalog + re-acquire gap list:
> [`DEX_R2_CATALOG_AND_GEN3_COVERAGE_GAP.md`](./DEX_R2_CATALOG_AND_GEN3_COVERAGE_GAP.md).

**Date:** 2026-06-07
**Buckets:** both in the same R2 account (one credential set reads/writes both).
**Method:** full recursive `rclone lsf` object listing of each bucket (size + server
modtime), aggregated by top-level prefix; Gen-2 prefix → Gen-3 dataset mapping is a
curated name crosswalk validated against the live `data-sink/active/` dataset set.
**Verdict:** **`dex-raw-landing-zone` is NOT safe to bulk-delete.** It is neither
frozen nor fully migrated — 21 prefixes are still receiving fresh writes, ~152 GiB of
data has no Gen-3 home, and the "dead" Polaris warehouse (77% of the bucket) shows
writes 5 days old.

---

## 1. Topline

| Bucket | Objects | Size | Top-level groups |
|---|---:|---:|---:|
| **Gen-2** `dex-raw-landing-zone` | 52,869 | **1.682 TiB** | 91 prefixes |
| **Gen-3** `data-sink` | 20,721 | **873 GiB** | 7 tiers / 142 active datasets |

Gen-2 is **~2× the byte volume** of the live Gen-3 lake — almost entirely because of
one dead artifact (Polaris, §3). Strip Polaris and Gen-2 is ~390 GiB of actual source
landings.

---

## 2. Gen-2 classification rollup

| Class | Prefixes | Objects | Size | Meaning |
|---|---:|---:|---:|---|
| **MIGRATED** | 29 | 8,458 | 239.5 GiB | Gen-3 Lance equivalent exists → raw is redundant |
| **MAPPED-but-MISSING** | 1 | 67 | 580 MiB | Raw landed; intended Gen-3 dataset never built |
| **ORPHAN-ACTIVE** | 21 | 893 | 12.0 GiB | No Gen-3 home **and still being written** (≥2026-05-25) |
| **ORPHAN (frozen)** | 34 | 19,676 | 140.1 GiB | No Gen-3 home; only copy lives here |
| **DEAD-INFRA** | 3 | 23,769 | **1.3 TiB** | Polaris / Iceberg catalog artifacts |
| **TRANSPORT/EPHEMERAL** | 3 | 6 | 234 MiB | Un-promoted transport (blitz_contacts, ae-jobs, cohort manifests) |

Deletable-without-loss today = **MIGRATED + DEAD-INFRA** (assuming Gen-3 trusted).
Everything else (`ORPHAN-*` + `MAPPED-but-MISSING` = **~152.7 GiB**) is **live or
sole-copy** and would be lost.

---

## 3. Critical findings

### 3.1 Polaris warehouse is 77% of the bucket — and NOT actually frozen
`polaris-warehouse/` = **23,665 objects / 1.3 TiB**, modtimes spanning 2026-05-12 →
**2026-06-02**. Protocol declares Polaris dead, yet objects landed 5 days before this
audit. Either a lingering Iceberg writer is still active, or it is metadata/compaction
churn. **Resolve the writer before reclaiming this 1.3 TiB** — it is the single biggest
cost line in the bucket and the highest-leverage cleanup, but a live writer means a
naive purge races an active process.

### 3.2 The "no longer writing" premise is false — 21 prefixes still landing to Gen-2
These have **no Gen-3 dataset** and a newest object **≥ 2026-05-25** (active pipelines
writing raw straight into the retired bucket):

| Prefix | Objs | Size | Newest |
|---|---:|---:|---|
| fmcsa-derived | 52 | 4.6 GiB | 2026-05-30 |
| sam-gov-opps | 65 | 3.7 GiB | 2026-05-30 |
| sec-dera | 712 | 3.4 GiB | 2026-05-30 |
| clinicaltrials-gov | 2 | 116.6 MiB | 2026-05-25 |
| chicago-permit-contractors | 1 | 96.0 MiB | 2026-05-29 |
| fl-cilb | 7 | 61.7 MiB | 2026-05-30 |
| warn | 10 | 44.2 MiB | 2026-05-30 |
| tx-tdlr · wa-lni-contractors · az-roc · or-ccb · il-idfpr-roofing · chicago-home-repair · nyc-dcwp-home-improvement · caltrans · azstate · fdot-area · ny-{data-construction,mta,local-authority}-procurements · nyc-contract-awards · opsc | — | small | 2026-05-25 → 05-30 |

These are mostly **new state/county licensing + procurement feeds** whose ingest paths
were never cut over to `data-sink/landing/`. `sam-gov-opps` is notable: it writes to
**both** Gen-2 `sam-gov-opps/` (3.7 GiB) **and** the Gen-3 `data-sink/sam-gov-opps/`
tier (9.5 GiB) — a split-brain landing path.

> **Action implied:** repoint each of these writers to `data-sink/landing/<source>/`
> before the Gen-2 bucket can be retired. Until then, deletion drops live data.

### 3.3 Large frozen orphans — sole copy lives only in Gen-2
No Gen-3 equivalent; deletion = permanent loss of source:

| Prefix | Objs | Size | Note |
|---|---:|---:|---|
| **noaa_ais** | 366 | **88.6 GiB** | Largest non-dead orphan. Never materialized. |
| **uspto-patents** | 62 | **27.2 GiB** | Only trademarks (`uspto_tm_*`) reached Gen-3; patents did not. |
| **sec-iapd** | 17,671 | **9.0 GiB** | 17.7k tiny objects; investment-adviser-rep data, no Gen-3 table. |
| usaspending-derived | 2 | 8.2 GiB | Derived products outside the Gen-3 `usaspending` dataset. |
| federal | 207 | 1.7 GiB | Ambiguous bucket; unmapped. |
| irs-990 | 56 | 1.7 GiB | No Gen-3 990 dataset. |
| sba-derived · fmcsa-carrier-essentials · ncua · google-maps · insurance-producers · nyc-property · nyc-dob-now · fdic · irs-bmf · bls-oews · … (24 more) | — | ~3 GiB total | Long tail of unmaterialized sources |

### 3.4 `fl-dor-nal` — raw landed, dataset never built
67 objects / 580 MiB landed 2026-05-29 targeting a `fl_dor_nal` dataset that **does not
exist** in Gen-3. Either an in-flight ingest that stalled at the materialize step, or an
abandoned one. Treat as a frozen orphan until the materialization is finished or killed.

### 3.5 Live code still bound to the dead bucket (2 defaults)
| File | Binding | Risk |
|---|---|---|
| [`pipelines/gtm/blitz_hydration_waterfall.py:82`](../../pipelines/gtm/blitz_hydration_waterfall.py) | `LANDING_BUCKET` **defaults** to `dex-raw-landing-zone` | Still **writes** raw Blitz contacts here (1 un-promoted payload, 2026-06-05). Paid enrichment with no Gen-3 promotion path yet. |
| [`pipelines/co_ucc/companions_bulk.py:70`](../../pipelines/co_ucc/companions_bulk.py) | `SOURCE_BUCKET` **defaults** to `dex-raw-landing-zone` | One-shot migration; CO UCC companions **already** materialized to Gen-3 (`ucc_co_{debtors,secured_parties,collateral}`). Reader is spent but default still points at the dead bucket. |

---

## 4. Gen-3 `data-sink` side

### 4.1 Tiers
| Tier | Objects | Size | Role |
|---|---:|---:|---|
| `active/` | 18,872 | 615.4 GiB | **System of record** — 142 native Lance datasets |
| `landing/` | 1,162 | 241.1 GiB | **Gen-3 transport** (the live replacement for the Gen-2 landing zone) |
| `sam-gov-opps/` | 78 | 9.5 GiB | Live SAM Opportunities reference feed |
| `scratch/` | 245 | 5.3 GiB | Scratch |
| `archive/` | 322 | 985 MiB | Archived/backup datasets |
| `usaspending_api_landings/` | 5 | 612 MiB | USASpending API delta landings |
| `tmp/` | 37 | 1.1 MiB | Temp |

### 4.2 Gen-3-native datasets (no Gen-2 source) — 68
Clean-room outputs built directly in Gen-3, never present in the Gen-2 lake. Headline
families: **CMS Medicare** (`cms_dme_*`, `cms_partd_*`, `cms_physician_*`,
`cms_qpp_experience` — ~49 GiB), **MSHA** (`msha_*`), **EPA derived**
(`epa_air_facilities`, `epa_rcra_handlers`, `epa_*_compliance`, `epa_to_sos_bridge`),
**NMLS MCR** (`nmls_mcr_*`), **SAM/SOS normalized masters** (`sam_master_entities`,
`sam_normalized_entities`, `sam_pocs`, `sos_normalized_master`), **crosswalks**
(`crosswalk_sos_sam`, `crosswalk_hmda_gleif`), `provider_360`, `firmographics_blitz`,
`ffata_exec_comp`, `osha_daily_triggers`, `usaspending` (239 GiB),
`uspto_tm_*`, `sec_adv_*`. Two backup snapshots present: `overture_places__bak_*`
(2.5 + 2.2 GiB).

---

## 5. Decommission-readiness matrix

| Tranche | Size | Precondition to delete |
|---|---:|---|
| **DEAD-INFRA** (polaris/iceberg) | 1.3 TiB | Stop the lingering Polaris writer (§3.1), then purge. **Biggest win.** |
| **MIGRATED** (29 prefixes) | 239.5 GiB | Row-count parity check Gen-2 raw vs Gen-3 dataset, then purge. |
| **TRANSPORT** (blitz/ae-jobs/cohorts) | 234 MiB | Repoint blitz writer → `data-sink/landing/`; preserve the 1 paid payload. |
| **ORPHAN-ACTIVE** (21 prefixes) | 12.0 GiB | **Repoint each writer to Gen-3 first.** Do not delete while live. |
| **ORPHAN-frozen** (34 prefixes) | 140.1 GiB | **Migrate to Gen-3 (or formally abandon) first.** Sole copy. |
| **MAPPED-but-MISSING** (fl-dor-nal) | 580 MiB | Finish or kill the materialization. |

**Sequence to actually empty the bucket:** (1) cut the Polaris writer + reclaim 1.3 TiB;
(2) repoint the 21 active orphan writers + blitz to `data-sink/landing/`; (3) decide
keep-vs-kill on the 35 frozen/missing orphans (noaa_ais and uspto-patents are the
material ones — 116 GiB combined); (4) parity-verify the 29 migrated prefixes; (5) purge.
Only after (1)–(4) is a bucket-level delete non-lossy.

---

## 6. Out of scope: `dex-db` (Gen-2 Postgres)
`dex-db` is **not** an R2 object — it is the Gen-2 Postgres (`DEX_DB_URL_POOLED`),
read by the `data-engine-x` repo. It is a separate decommission (a DB drop, different
blast radius) and is untouched by anything in this diagnostic. The only Gen-2 artifacts
in R2 are `dex-raw-landing-zone` and the dead Polaris warehouse inside it.

---

*Generated from live R2 listings 2026-06-07. Crosswalk for the long-tail orphans is
name-based; operator should confirm the ~34 frozen orphans are genuinely unmaterialized
(not renamed) before any keep/kill decision.*
