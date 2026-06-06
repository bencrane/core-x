# PDL Resolution Substrate — Build Report & Handoff

Factual handoff for the work completed on the People Data Labs (PDL) resolution surface. Every
figure below is traceable to a git commit, an R2 Lance manifest, or an ops-ledger row — verified
live, not estimated. As-of **2026-06-06**.

---

## 0. TL;DR (what exists now)

Two units of work shipped to `main`:

| PR | Merge commit | What |
|---|---|---|
| [#173](https://github.com/bencrane/core-x/pull/173) | `0e30cc6` | Read-only **diagnostic** of `pdl_companies` index topology + predicate pushdown (1 doc). |
| [#198](https://github.com/bencrane/core-x/pull/198) | `277a644` | **Built + verified** the `pdl_normalized_companies` blocking-key **sidecar** (6 files). |

Net new live artifact: **`s3://data-sink/active/pdl_normalized_companies/`** — a 35.4M-row,
6-index Lance dataset that lets cross-spine entity-resolution joins (SAM/GLEIF/etc. → PDL) run as
indexed point-lookups instead of full scans. The firmographic SoR `pdl_companies` was **not
modified** (still Lance v11).

---

## 1. Live datasets (verified manifests, R2 `data-sink/active/`)

### 1.1 `pdl_companies` — firmographic SoR (read-only source; UNCHANGED)
- Lance **v11**, **35,446,771 rows**, 34 fragments, **12 cols**, **10 indices**.
- Cols: `pdl_company_id, company_name, domain, linkedin_url, industry, employee_size_range, year_founded, locality, region, country, snapshot_date, ingested_at`.
- Indices: 6 BTREE (`pdl_company_id, company_name, linkedin_url, domain, locality, year_founded`) + 4 BITMAP (`industry, country, region, employee_size_range`).
- `pdl_company_id` is a perfect PK (35,446,771 distinct = rows). Single manual-overwrite snapshot (`snapshot_date=2026-05-31`).
- Ingest worker: `pipelines/pdl_companies/free_company_dataset.py` (Modal app `pdl-companies`).

### 1.2 `pdl_normalized_companies` — blocking-key sidecar (NEW, this work)
- Lance **v7**, **35,446,771 rows**, 34 fragments, **15 cols**, **6 indices** (all 100% trained: `num_indexed_rows=35,446,771`, `num_unindexed_rows=0`).
- Cols (actual build order): `pdl_company_id, company_name_norm, company_legal_base, normalized_domain, is_generic_domain, linkedin_slug, company_name, locality, region, country, industry, employee_size_range, year_founded, source_version, built_at`.
- Indices: 5 BTREE (`pdl_company_id, company_name_norm, company_legal_base, normalized_domain, linkedin_slug`) + 1 BITMAP (`is_generic_domain`).
- Grain: 1 row / `pdl_company_id` (1:1 passthrough). `source_version=11` (the `pdl_companies` version it was projected from).
- Build worker: `pipelines/pdl_companies/pdl_normalized_companies.py` (Modal app `pdl-normalized-companies`, deployed).

### 1.3 `bridge_sam_pdl` — existing SAM↔PDL crosswalk (pre-existing; context only, not modified)
- Lance **v5**, **801,831 rows**, 1 fragment, 7 cols, 4 BTREE indices (`uei, pdl_company_id, duns, normalized_domain`).
- Resolves SAM↔PDL on a materialized `normalized_domain`. The new sidecar enables a *name*- and *linkedin*-based edge that this domain-only bridge can't reach (PDL `domain` is ~66% filled).

---

## 2. Why this work exists — the diagnostic finding (PR #173)

`docs/reference/PDL_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md`. Read-only probe (pylance 7.0.0
`list_indices`/`index_stats`/`explain_plan`/`analyze_plan`; zero mutation). Findings, proven live:

- `pdl_companies` is **healthy**: all 10 committed indices trained (`indexed==total, unindexed==0`); all 12 columns flat scalar (no List/Struct/Map); no FEC-style dead-BTREE, no MSHA-style missing index. The directive's named `pdl_person` dataset **does not exist** — the PDL SoR is company-level `pdl_companies`.
- **The one cliff:** wrapping an indexed column in `name_norm()` at read time is non-indexable. Measured on the live `company_name` BTREE:
  - Raw `company_name = 'x'` → `ScalarIndexQuery@company_name_idx(BTree)`, **rows_scanned=524**, 3.27 MB, ~9 ms.
  - `name_norm(company_name) = 'X'` → no index, **rows_scanned=35,446,771** (full table), 623 MB, ~15,901 ms.
  - Differential: **67,646× rows scanned, 190× bytes, ~1,767× wall.** DuckDB `EXPLAIN` corroborated (post-scan `FILTER`, never pushdown).

Implication: cross-spine resolution keys never match raw across sources (case/suffix/`&`/`www`/path), and the macro that would fold them is non-indexable. The fix is to **materialize** the normalized form once and index it. That is what PR #198 builds.

---

## 3. Architecture decision — sidecar, NOT columns on the SoR

Recorded so a future agent does not "helpfully" inline these keys. Full rationale:
`docs/plans/PDL_NORMALIZED_COMPANIES_SIDECAR_PLAN.md` §2; adversarial re-litigation in
`..._REVIEW.md` §A (verdict: sidecar survives).

1. **SoR immutability is physical here.** `pdl_companies`'s only write paths (`overwrite`, `reindex`) publish via `_replace_r2_prefix`, which wipes the R2 prefix and re-uploads ~7 GB, destroying version history. `lance.add_columns` collapses into the same full rewrite under R2's multipart constraint. Inlining = rewriting a 35.4M-row SoR other consumers read.
2. **The norm rule is mutable policy; firmographics are not.** `core.name_norm`/`core.web_norm` change over time; coupling them to the SoR forces a base republish per rule revision. A sidecar rebuilds independently.
3. **Derived-from-immutable** ⇒ the sidecar is freely rebuildable, so `overwrite` of *it* is safe and idempotent; the base is the un-reconstructable artifact.
4. **Zero blast radius / no defensive snapshot** — nothing load-bearing is mutated; the heavy index external-sort runs against the sidecar in isolation.

Cost (a 1:1 sync obligation to the base snapshot) is closed by a `source_version` stamp + a refresh trigger + a fail-closed consumer assert (§5, §6).

---

## 4. Files shipped (PR #198 = `277a644`, 6 files, +1528 lines)

| File | Status | Purpose |
|---|---|---|
| `core/web_norm.py` | **new** | Canonical web-identity SQL builders (sibling of `core/name_norm.py`): `_bare_host`, `normalized_domain`, `linkedin_slug`, `is_generic_domain` + the `_GENERIC_DOMAINS` set. THE single source of truth — never re-inline. |
| `pipelines/pdl_companies/pdl_normalized_companies.py` | **new** | Modal worker (`pdl-normalized-companies`). Reads `pdl_companies` read-only (Lance scanner, projection pushdown, version-pinned) → DuckDB `name_norm`/`web_norm` projection → local Lance overwrite + 5 BTREE + 1 BITMAP → boto3 publish to the new prefix. Entrypoints: `initdb`, `run`, `reindex`, `show_ledger`. |
| `pipelines/pdl_companies/ops_pdl_normalized_runs.sql` | **new** | Ledger DDL `ops.pdl_normalized_runs` (mirrors `ops.pdl_company_runs`; adds `source_version`, `source_rows`). |
| `pipelines/pdl_companies/free_company_dataset.py` | **modified (+14)** | Additive **N6 fan-out** only: on `pdl_companies` ingest success, `modal.Function.from_name("pdl-normalized-companies","ingest_normalized_companies").spawn()`. Writes nothing new to the SoR. |
| `docs/plans/PDL_NORMALIZED_COMPANIES_SIDECAR_PLAN.md` | **new** | Canonical build plan (the spec the worker implements). |
| `docs/plans/PDL_NORMALIZED_COMPANIES_SIDECAR_PLAN_REVIEW.md` | **new** | Adversarial review (verdict SHIP-WITH-AMENDMENTS) that the plan/worker incorporate. |

### 4.1 The canonical keys (in `core/web_norm.py` + `core/name_norm.py`)
| Sidecar column | Builder | Example |
|---|---|---|
| `company_name_norm` | `name_norm(company_name)` | `AT&T Inc.` → `AT AND T INC` |
| `company_legal_base` | `legal_name_base(name_norm(company_name))` | `ACME LLC` → `ACME` |
| `normalized_domain` | `web_norm.normalized_domain(_bare_host(domain))` | `https://www.Acme.com/x` → `acme.com` |
| `linkedin_slug` | `web_norm.linkedin_slug(linkedin_url)` | `…/company/Foo-Bar/` → `foo-bar` |
| `is_generic_domain` | `web_norm.is_generic_domain(normalized_domain)` | `instagram.com` → `true`; `acme.com` → `false` |

Adversarial-review amendments folded into the above: **B1** (`linkedin_slug` lowercases the input, not output — uppercase hosts no longer NULL), **B2** (parity gate is committed==distinct==src_valid, not a hardcoded count), **B3** (`is_generic_domain` flag so shared hosts don't cause N×M false joins), **N1** (`_bare_host` strips `user@` userinfo), **N2** (`industry`/`employee_size_range`/`year_founded` inlined as tiebreaks), **N6** (refresh fan-out).

---

## 5. How to USE the sidecar (consumer contract — plan §10)

A bridge resolving `<spine> → PDL`:

```sql
-- 1. block on a normalized key (indexed point-lookup, not a 35.4M scan)
SELECT s.<spine_key>, p.pdl_company_id
FROM   <spine> s
JOIN   pdl_normalized_companies p
  ON   s.normalized_legal_name = p.company_name_norm   -- or p.normalized_domain / p.linkedin_slug
 AND   NOT p.is_generic_domain                          -- MANDATORY for normalized_domain joins (B3)
-- 2. tiebreak on inline geo/sector/size (no hydration join needed): p.locality / p.region /
--    p.country / p.industry / p.employee_size_range / p.year_founded
-- 3. hydrate firmographics ONLY when needed, by PK:  JOIN pdl_companies USING (pdl_company_id)
```

Hard rules:
1. **Domain joins MUST add `AND NOT is_generic_domain`** — ~2.25% of non-null domains are shared webmail/social/marketplace hosts (`instagram.com`, `indiamart.com`, …); joining them 1:1 is an N×M cartesian.
2. **Assert `pdl_normalized_companies.source_version == lance.dataset(pdl_companies).version` and fail closed on mismatch** — the staleness backstop.
3. **Import `core.name_norm` / `core.web_norm`** for the left side of any join — never re-inline the regex (byte-parity with the stored keys depends on it).

Note: `company_name_norm` is NULL for ~3% of rows whose names are entirely non-ASCII (correct `name_norm` behaviour — they normalize to empty); those rows are still resolvable by `normalized_domain` / `linkedin_slug`.

---

## 6. How to OPERATE / rebuild

```bash
# all commands run from the repo root via Doppler (R2 + HQX creds in core-x/prd)
doppler run -p core-x -c prd -- modal deploy pipelines/pdl_companies/pdl_normalized_companies.py
doppler run -p core-x -c prd -- modal run    pipelines/pdl_companies/pdl_normalized_companies.py::initdb       # ops table
doppler run -p core-x -c prd -- modal run --detach pipelines/pdl_companies/pdl_normalized_companies.py::run     # rebuild sidecar (~min, 32 GiB/512 GB-NVMe)
doppler run -p core-x -c prd -- modal run    pipelines/pdl_companies/pdl_normalized_companies.py::reindex       # indexes only
doppler run -p core-x -c prd -- modal run    pipelines/pdl_companies/pdl_normalized_companies.py::show_ledger   # ops audit
```

- **Refresh is automatic:** the N6 fan-out in `free_company_dataset.py` spawns `::run` whenever a new `pdl_companies` snapshot lands, so the sidecar tracks the base. `source_version` records lineage.
- The build is idempotent (deterministic `overwrite`-from-immutable-source); safe to retry.
- **Audit (verified):** `ops.pdl_normalized_runs` id=1 → `source_version=11, source_rows=35,446,771, rows_processed=35,446,771, distinct_ids=35,446,771, status=success`.

---

## 7. Verification evidence (8 gates, all PASS — actual numbers)

| Gate | Result |
|---|---|
| 1 — builder fixtures | PASS (uppercase-LinkedIn, `user@`, IP-literal, `is_generic_domain` cases) |
| 2 — live-sample transform (500k) | PASS — PK 1:1; fills: `company_name_norm` 96.9%, `normalized_domain` 66.2%, `linkedin_slug` 100%, `industry` 82.9%; `is_generic_domain` 2.25% of non-null domains |
| 3 — parity vs source | PASS — `count_rows()=35,446,771 == distinct(pdl_company_id)=35,446,771 == src_valid=35,446,771` |
| 4 — manifest | PASS — 6 indices (5 BTREE + 1 BITMAP) |
| 5 — trained truth | PASS — every index `indexed=35,446,771, unindexed=0` (zero FEC trap) |
| 6 — pushdown proof | PASS — `company_name_norm`/`normalized_domain`/`linkedin_slug` → `ScalarIndexQuery@*_idx(BTree)`; `is_generic_domain` → `ScalarIndexQuery@is_generic_domain_idx(Bitmap)` |
| 7 — round-trip | PASS — `company_name_norm` → `pdl_company_id` → `pdl_companies` row resolves (1 hit, fields match) |
| 8 — SoR untouched | PASS — `pdl_companies` Lance **v11**, 35,446,771 rows (unchanged) |

---

## 8. NOT done (explicit scope boundaries — follow-on work)

None of these were executed; they are named in the plan (§10/§13):
- **Name-sentinel suppression** — junk names (`x`, `test`, `closed`) still produce `company_name_norm` values; no consumer-side filtering added.
- **The SAM/GLEIF/company-spine ↔ PDL bridges** that consume these keys (the actual coverage product, e.g. recovering the ~34% domain-null PDL tail via name/linkedin edges).
- **Codifying the `bridge_sam_pdl` builder** — that dataset (v5, 801,831 rows) has no committed builder; it was built ad-hoc.
- **`sam_fmcsa_domain_spine.py` reconciliation** — its private `_norm_host_sql` + `CONSUMER_BLOCK` should migrate to `core.web_norm` so its `normalized_domain` is byte-identical to PDL's; `bridge_sam_fmcsa_domain` rebuild follows.
- **IDN/punycode** — hosts are stored raw (not punycoded); documented contract in `core/web_norm.py`.

---

## 9. Reference index

| Artifact | Path |
|---|---|
| Diagnostic (why) | `docs/reference/PDL_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md` |
| Build plan (spec) | `docs/plans/PDL_NORMALIZED_COMPANIES_SIDECAR_PLAN.md` |
| Adversarial review | `docs/plans/PDL_NORMALIZED_COMPANIES_SIDECAR_PLAN_REVIEW.md` |
| Canonical builders | `core/name_norm.py`, `core/web_norm.py` |
| Build worker | `pipelines/pdl_companies/pdl_normalized_companies.py` |
| Ledger DDL | `pipelines/pdl_companies/ops_pdl_normalized_runs.sql` |
| Refresh trigger | `pipelines/pdl_companies/free_company_dataset.py` (success-path fan-out) |
| This report | `docs/reference/PDL_NORMALIZED_COMPANIES_BUILD_REPORT.md` |
