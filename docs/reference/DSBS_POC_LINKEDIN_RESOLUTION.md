# DSBS POC → LinkedIn Resolution

**Canonical handoff / onboarding reference.** Everything a fresh engineer or agent needs to run the
pipeline and continue the work is in this one file. Ground truth is the code
(`pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py`, `core/serper_gateway.py`, the two `ops_*.sql`
DDLs) and the live ledgers/datasets — this document was verified against both on 2026-07-02 and the
observed numbers below are authoritative.

---

## 1. Overview

**What.** Resolve unresolved DSBS (SBA Dynamic Small Business Search) point-of-contact *people*
(owners, principals, primary contacts) to **validated LinkedIn `/in/` profile URLs** via
[serper.dev](https://serper.dev) (a real Google Search API). Serper credits are scarce
(2,500 per account), so the spend is ranked strictly top-down over a won-federal-dollars ·
reachability priority and hard-capped.

**Why.** `dsbs_poc_people` already resolves the DSBS decision-makers we can match to a person record
we already own. This pipeline chases the **complement** — DSBS-declared humans we have *no*
person/LinkedIn record for — and mints a durable LinkedIn handle for them. That handle is the
enrichment key for downstream GTM / outreach: it is the join point into people-enrichment providers
and the identity anchor for a firm's reachable decision-maker.

**Status (live, 2026-07-02).**
- **Ledger** (`ops.dsbs_poc_linkedin_spend`): 2,851 subjects queried · 2,846 credits spent ·
  **845 resolved** (29.6% resolve rate) · **788 distinct firms covered**.
- **System of record** (Lance `s3://data-sink/active/dsbs_poc_linkedin/`): **821 validated `/in/`
  rows** (821 < 845 because materialize only writes ledger rows that *still* pass the current
  worklist gate — hygiene tightening retroactively drops stale/false-positive rows).
- All spend to date landed in **tier 0 (award-active)** and on **name-consistent PDL-brand**
  subjects (`company_source='pdl'` for 100% of the ledger).
- Merged to `main` across **PR #915** (initial pipeline), **#920** (contact_person elevation +
  entity-name gate), **#921** (firm-coverage flags `--one-per-firm` / `--contacts-only`).

---

## 2. Terminology (READ THIS — "prime" is deprecated)

In federal contracting **"prime" means prime contractor / prime award.** Do **not** use "prime" or
"non-prime" to describe our priority cohorts or dollar bands anywhere — it is overloaded and
confusing. The following handles are canonical; use them consistently and do not reintroduce "prime."

| Term | Definition |
|---|---|
| **award-active** | The firm has active *obligated* federal dollars: `firmographics_company_map_serving.entity_active_obligated_usd > 0`. This is `priority_tier = 0`, the dominant priority key. (Formerly, in code comments/plans, loosely called "won-$"; "award-active" is the durable name.) |
| **sweet-spot band** | The `$1M–$50M` obligated-dollar VALUE band. It is `_dollar_band(obl) == 0` and ranks first among the dollar bands (see §10). |
| **Cohort A** (a.k.a. **lead owner-contacts**) | The highest-value cohort: **award-active · name-consistent · sweet-spot band ($1M–$50M) · `poc_type = contact_person`.** These are the firms' SBA-declared primary contacts — overwhelmingly the owner/CEO/president. The second serper key was spent here first. Use "Cohort A" or "lead owner-contacts"; never "prime cohort." |

> **Code residue to fix (non-blocking):** the `--contacts-only` argparse help string in
> `resolve_dsbs_poc_linkedin.py` still reads *"the prime owner-contacts."* That is the deprecated
> word. Read it as "the lead owner-contacts (Cohort A)." A future edit should replace it.

Other defined terms live in the **Glossary (§16)**.

---

## 3. Architecture / data plane

Clean-room, decoupled compute/storage — the core-x Gen-3 pattern. **DuckDB + pylance read the R2
Lance sources; matching/gating/ranking happen in Python; serper supplies real Google; every credit
and every validated result is committed to Postgres BEFORE anything touches Lance; a separate
`materialize` step rebuilds the Lance SoR from the ledger.**

```
  R2 Lance sources (read-only)                         build_worklist()  [FREE, pure read]
  ┌───────────────────────────────┐                    ┌─────────────────────────────────┐
  │ dsbs_pocs            (people)  │  DuckDB scan   →   │ anti-join already-resolved       │
  │ dsbs_poc_people   (anti-join)  │  + join +      →   │ hygiene gates (§9)               │
  │ crosswalk_dsbs_sam  (match id, │  chunked PDL   →   │ Lever 1: company term (§7)       │
  │   name-consistent, domains)    │  brand probe   →   │ priority sort tuple (§10)        │
  │ pdl_normalized_companies(brand)│                    │ → ranked worklist[]              │
  │ firmographics_..._serving ($)  │                    └───────────────┬─────────────────┘
  └───────────────────────────────┘                                    │ top-K (budget-bounded)
                                                                        ▼
                    serper.dev  ← core/serper_gateway.search(query)  per subject (§8)
                        │  primary: "First Last" site:linkedin.com/in
                        │  fallback (opt): "First Last" {company|domain}
                        ▼
                    _validate(): name-AND-company accept rule (§8) → resolved | unresolved
                        │
                        ▼  commit EVERY attempt (hit + miss) — the double-spend guard
              ┌──────────────────────────────────────────────┐
              │ ops.dsbs_poc_linkedin_spend   (Postgres)      │  PK subject_id, stores raw_json
              │ ops.dsbs_poc_linkedin_resolve_runs (Postgres) │  one row per pass
              └───────────────────────┬──────────────────────┘
                                      │ materialize  (rebuild from resolved rows still passing gate)
                                      ▼
              s3://data-sink/active/dsbs_poc_linkedin/  (Lance v2.1, overwrite)  = SoR (§4)
                  BTREE[subject_id,uei,linkedin_url] + BITMAP[company_source,name_consistent,poc_type,priority_tier]
```

Why Postgres and Lance are deliberately separated: the **heavy/risky credit spend** and the **clean
append** are different concerns. Postgres is the *credit double-spend guard* and *resume key* — a
subject present there is never re-queried, so the account budget is enforced globally as
`sum(credits_spent)`. Lance is the downstream *system of record* — rebuilt (`mode="overwrite"`) from
the ledger each `materialize`, so precision/hygiene improvements retroactively clean the SoR.

Run **locally via doppler, operator-supervised** — this is not a Modal automated pipeline (the credit
burn is scarce and human-gated).

---

## 4. Datasets

### 4.1 Source Lance datasets (R2, `s3://data-sink/active/`, read-only)

| Dataset (URI) | Grain | Columns consumed | Role |
|---|---|---|---|
| `dsbs_pocs/` | 1 row per (uei × POC entry) | `uei, name_key, first_name, last_name, full_name, title, poc_type` | The **people**. Exploded from `sba_dsbs_certified_firms`: `contact_person` (one primary contact) + `current_principals` (verbatim `"Name - Title; …"` string). `poc_type ∈ {contact_person, current_principal}`. `name_key` = order-independent normalized name (lowercased alpha tokens ≥2 chars, suffixes dropped, sorted). Built by `materialize_dsbs_pocs.py`. |
| `dsbs_poc_people/` | 1 row per (uei × person_id) | `uei, name_key` | DSBS POCs **already resolved** to a real person record. The worklist **anti-joins these out** on `(uei, name_key)` — we only chase the unresolved. Built by `materialize_dsbs_poc_people.py`. |
| `crosswalk_dsbs_sam/` | 1 row per uei | `uei, dsbs_legal_business_name, sam_legal_business_name, best_domain, matched_domain, matched_pdl_company_id, matched_name_consistent` | The DSBS↔SAM UEI bridge. Supplies the PDL match id, the two domains, both legal names, and `matched_name_consistent` (bool: DSBS firm-name tokens overlap the PDL brand tokens ≥1). `company_linkedin_url` (company slug) also lives here. Built by `crosswalk_dsbs_sam.py`. |
| `pdl_normalized_companies/` | 1 row per pdl_company_id (~30M+ rows) | `pdl_company_id, company_name, linkedin_slug` | PDL (People Data Labs) company sidecar. **`company_name` is the OPERATING/BRAND name** shown on LinkedIn, NOT the registered legal entity — this is the whole point of Lever 1 (§7). **NEVER full-scanned**: probed by a chunked `pdl_company_id IN (…)` filter, `PDL_CHUNK = 8000` ids per indexed IN-list. Built by `pdl_normalized_companies.py`. |
| `firmographics_company_map_serving/` | 1 row per uei | `uei, entity_active_obligated_usd, employee_size_band, has_federal_awards, award_count` | The won-$ + size signals. `entity_active_obligated_usd` is the **award-active** measure (obligated federal dollars). Drives the priority tier + dollar band + employee band. |

### 4.2 Target Lance dataset (SoR, written by `materialize`)

**`s3://data-sink/active/dsbs_poc_linkedin/`** — validated LinkedIn `/in/` resolutions. Lance
**v2.1**, written `mode="overwrite"` (rebuilt from the ledger each run).

- **Grain:** 1 row per subject, where `subject_id = uei|name_key`.
- **Rows (live):** 821. **Columns (live):** 19.
- **Full column list** (verified from `verify`):
  `subject_id, uei, name_key, first_name, last_name, full_name, poc_type, company_term,
  company_source, domain, name_consistent, obligated_usd, priority_tier, priority_rank,
  query_variant, linkedin_url, match_title, match_snippet, resolved_at`.
  - `resolved_at` is the ledger's `queried_at` renamed at materialize time.
  - `company_source ∈ {pdl, legal}`; `company_term` is the exact query company string used.
- **Indexes:**
  - **BTREE:** `subject_id`, `uei`, `linkedin_url`
  - **BITMAP:** `company_source`, `name_consistent`, `poc_type`, `priority_tier`
  - (Live `verify` lists `subject_id_idx, uei_idx, linkedin_url_idx, company_source_idx,
    name_consistent_idx, poc_type_idx, priority_tier_idx`.)
- **Retroactive hygiene:** `materialize` only writes ledger rows where `resolved=true AND
  linkedin_url IS NOT NULL` **and** whose `subject_id` still appears in the current
  `build_worklist()`. So a row spent under an older, looser gate (e.g. an org-in-person name like
  "UIC Government") is dropped from the SoR on the next `materialize`. This is why the SoR (821) can
  be smaller than the ledger's resolved count (845).

---

## 5. Files

| File | Role | Key functions |
|---|---|---|
| `core/serper_gateway.py` | The fleet's single credit-metered egress to serper. **Transport-only** — returns a normalized envelope, never interprets results, never raises for HTTP/business errors. | `search(query, num=10, gl="us", hl="en", session=None)` → `{ok, credits, http_status, organic[], error, raw}`. Bills **`credits=1` only on HTTP 200**; 402 (out of credits), 401/403 (bad key), and every network/5xx failure report `credits=0` so a retry or dead key never decrements the budget. Retries 429/5xx/network with exponential backoff (`MAX_RETRIES=4`, `RETRY_BACKOFF=1.5`). `key_present()` checks `SERPER_API_KEY`. |
| `pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py` | The main worker (all commands). | See below. |
| `pipelines/sba_dsbs/ops_dsbs_poc_linkedin_spend.sql` | DDL for the per-subject spend ledger (the double-spend guard). Idempotent. | — |
| `pipelines/sba_dsbs/ops_dsbs_poc_linkedin_resolve_runs.sql` | DDL for the per-run terminal ledger. Idempotent. | — |
| `pipelines/sba_dsbs/materialize_dsbs_pocs.py` | Builds the `dsbs_pocs` source (person-grain explosion). | `build_dsbs_pocs()`, `_name_parts`, `_name_key`. |
| `pipelines/sba_dsbs/materialize_dsbs_poc_people.py` | Builds the `dsbs_poc_people` source (already-resolved POCs, anti-joined out). | `build_dsbs_poc_people()`. |
| `pipelines/sba_dsbs/crosswalk_dsbs_sam.py` | Builds the `crosswalk_dsbs_sam` bridge (match id, name-consistency, domains). | `build_crosswalk()`. |
| `pipelines/pdl_companies/pdl_normalized_companies.py` | Builds the PDL brand sidecar (chunked-probe source). | `build_normalized_companies_sql()`. |
| `core/name_norm.py` | Canonical name blocking-key SQL builders (single source of truth). | `name_norm`, `legal_name_base`. |
| `core/web_norm.py` | Canonical web-identity blocking-key SQL builders. | `normalized_domain`, `linkedin_slug`, `is_generic_domain`. |

### Key functions in `resolve_dsbs_poc_linkedin.py`

- **`build_worklist() -> list[dict]`** — the heart. Pure read, no credits, no writes. DuckDB registers
  the five source scanners, aggregates `dsbs_pocs` to 1 row per `(uei, name_key)` with a `poc_type`
  rollup (`both` when a person is both a principal and the contact), anti-joins `dsbs_poc_people`,
  left-joins the crosswalk + firmographics, chunk-probes PDL for brand names, applies the hygiene
  gates and Lever 1, computes the `_sort` tuple, sorts ascending (best first), and stamps
  `priority_rank`. Deterministic (`subject_id` final tiebreak) so top-K is stable and resume-safe.
- **`_derive_names(first, last, full_name)`** — hygiene gate + honorific recovery → clean display
  `(first, last)` or `None`.
- **`_validate(first, last, company_term, domain, organic)`** — the name-AND-company accept rule.
- **`_build_query` / `_fallback_query`** — the two query shapes (§8).
- **`_spend_primary` / `_spend_fallback`** — threaded serper calls (`ThreadPoolExecutor`), each result
  committed under a write lock with `conn.commit()` per row.
- **Command dispatchers:** `cmd_init_ops, cmd_worklist, cmd_smoke, cmd_resolve, cmd_materialize,
  cmd_status, cmd_revalidate, cmd_verify` (§11).
- **Helpers:** `_ledger_state()` (spent + seen subject_ids), `_covered_firms()` (UEIs with a resolved
  person, for `--one-per-firm`), `_fallback_candidates()` (unresolved rows with a domain,
  name-consistent, fallback not yet done), `_dollar_band()`.

---

## 6. The Postgres ledgers

DSN env: **`HQX_DB_URL_POOLED`** (falls back to `HQX_DB_URL`). Schema: **`ops`**.

### 6.1 `ops.dsbs_poc_linkedin_spend` — the credit double-spend guard + resume key

**PK `subject_id` (`uei|name_key`).** One row per subject. A subject present here is **never
re-queried** — this is what makes the resolver safe to retry/resume and what makes the global credit
ceiling exact (`sum(credits_spent)`). This ledger retains **every attempt (hit and miss)** for credit
accounting and never-retry semantics; it is **not** the SoR for the resolutions (that is Lance).

Columns (full DDL in `ops_dsbs_poc_linkedin_spend.sql`):
`subject_id` (PK), `uei`, `name_key`, `first_name`, `last_name`, `full_name`, `poc_type`
(`current_principal|contact_person|both`), `company_term`, `company_source` (`pdl|legal`), `domain`,
`name_consistent` (bool), `obligated_usd` (double), `priority_tier` (smallint 0/1/2),
`priority_rank` (int, global rank at spend time), `query_variant` (`primary|primary+fallback`),
`serper_query`, `fallback_query`, `fallback_done` (bool), `credits_spent` (smallint; 1 primary,
+1 if a fallback also ran), `http_status`, `n_organic`, `resolved` (bool), `linkedin_url`,
`match_title`, `match_snippet`, `validation_reason`
(`validated|no_confident_match|no_in_link|error`), **`raw_json`** (jsonb — the serper organic
payload, verbatim), `queried_at`.

Secondary indexes: `(uei)`, `(resolved)`, `(priority_rank)`, `(resolved, fallback_done)`.

**Why `raw_json` is stored:** validation can *tighten* (precision fixes) and be **re-scored for
free** against the stored payloads via `revalidate` — zero new credits.

### 6.2 `ops.dsbs_poc_linkedin_resolve_runs` — per-run terminal ledger

One row per resolve pass. Run-state + credit accounting only (not a SoR). Columns:
`id` (identity PK), `feed`, `pass_name` (`primary|fallback`), `budget`, `reserve`, `ceiling`
(= budget − reserve), `prior_spent`, `attempted`, `credits_spent`, `resolved`, `unresolved`,
`status` (`success|error`), `error`, `started_at`, `completed_at`, `recorded_at`.

### 6.3 Semantics summary

- **Double-spend guard:** `ON CONFLICT (subject_id) DO NOTHING` on insert; the primary pass filters
  `subject_id NOT IN seen`.
- **Resume:** re-running `resolve` picks up where it left off — `seen` and `sum(credits_spent)` are
  read at the top of each pass.
- **Revalidate:** `cmd_revalidate` re-applies the *current* `_validate` to every row's stored
  `raw_json`, updating `resolved/linkedin_url/…` where the verdict changed — 0 credits.
- **Materialize gate:** `cmd_materialize` rebuilds Lance only from `resolved=true` rows that still
  pass `build_worklist()`.

---

## 7. The two precision levers

These are why this beats blind scraping / DuckDuckGo.

### Lever 1 — Company term = PDL brand, gated

When `crosswalk_dsbs_sam.matched_name_consistent` is true **AND** a PDL `linkedin_slug` + `company_name`
exist for the matched company, the query's company term is the **suffix-stripped PDL `company_name`**
— the operating brand the person's LinkedIn profile actually shows (`company_source='pdl'`). Otherwise
it falls back to the **suffix-stripped SAM/DSBS legal name** (`company_source='legal'`).

Concrete example: legal name **"ANOINTED PROFESSIONAL ENTERPRISE"** → brand **"Glorious Cleaning
Services"**. Searching the legal name on LinkedIn returns zero/wrong; the brand is what appears on the
profile. Roughly **1 in 4 firms** operate under a different brand than their legal name.

Gating matters: the PDL brand is only trusted when `matched_name_consistent` (the DSBS name tokens
overlap the PDL brand tokens), so a mis-matched PDL id can't inject a wrong brand.

### Lever 2 — Name-AND-company validation

Serper's first `/in/` result is accepted **only when the person's name tokens AND a company token
distinct from the name (or the domain root) both confirm** in the result title/snippet/slug.

- **Company confirmation is an EXACT word-token match** against the title/snippet — so `"tech"` does
  **not** confirm via `"technology"`. Only the **domain root** may confirm as a *substring* (it is
  distinctive by construction, gated to length ≥5).
- Company tokens must be **distinct from the person's name** (`comp_toks - {ftok, ltok}`) — an
  eponymous firm's surname appears on the person's own profile and must not self-confirm.
- Namesakes / orgs that don't confirm **abstain** → recorded `resolved=false`, never a banked wrong
  URL. Precision over recall.

---

## 8. Query shape + validation logic

### Primary query (empirically calibrated)

```
"First Last" site:linkedin.com/in
```

Bare exact-quoted name + `site:` → **maximizes `/in/` candidate retrieval**. The company/domain are
**deliberately NOT AND-ed into the Google query.** Calibration showed
`"name" company site:linkedin.com/in` **over-constrains Google to ~0 results** because it forces the
company string onto the profile page (which rarely contains it verbatim). Company/domain gate
**acceptance** in `_validate` instead — preserving precision without starving retrieval.

### Fallback query (optional, `--pass fallback`)

```
"First Last" {company or domain}          (no site: filter)
```

A genuinely *different* retrieval to rescue common-name misses the bare-name query failed to rank:
co-occur the exact name with the firm and drop `site:` so a common-name profile can surface through
company context (serper still returns the `/in/` link, which `_validate` confirms). Costs a **2nd
credit** (`credits_spent` → 2, `query_variant` → `primary+fallback`). **Not yet used** — 0 fallbacks
in the live ledger.

### The exact accept rule (`_validate`)

For the **first** organic result carrying a `linkedin.com/in/<slug>`:

1. **`name_ok`** — both `first` and `last` (punctuation stripped) appear as substrings of a
   spaces-removed haystack built from `slug + title`. (Slugs concatenate tokens, e.g.
   `/in/mardinorman`, so substring matching on a de-spaced haystack handles that; the surname anchors
   even when Google relaxes the given name to a nickname.)
2. **`company_ok`** — **any** distinctive company core-token (`_company_tokens`: alpha tokens ≥4 chars,
   stopword-filtered, minus the person's name tokens) appears as an **exact word-token** in the
   title+snippet, **OR** the domain root (≥5 chars) appears as a substring in `slug+title+snippet`.
3. Accept iff `name_ok AND company_ok` → `resolved=true`, `linkedin_url =
   https://www.linkedin.com/in/<slug>`, `validation_reason='validated'`.
4. Otherwise → `resolved=false`, reason `no_confident_match` (saw an `/in/` link but no confident
   match) or `no_in_link` (no `/in/` link at all).

---

## 9. Hygiene gates (never spend a credit on a non-person)

Applied in `build_worklist` / `_derive_names`. Order matters.

1. **Entity-drop (checked FIRST, on the RAW `full_name`).** Drop the subject when the raw `full_name`
   carries a corp/entity token: `_ENTITY_DROP = {corporation, incorporated, corp, inc, llc, holdings,
   holding, ventures, nation, tribe, tribes, subsidiary, enterprises}`. This is checked **before**
   honorific recovery, because recovery would otherwise strip "Corporation" off **"Chugach Alaska
   Corporation"** and mint a fake person **"Chugach Alaska."** This class is tribal / ANC holding-cos
   and JV partners listed in `current_principals` (e.g. "Choctaw Nation", "HunaTek Holding"). *(Added
   in #920.)*
2. **Honorific recovery.** When the explicit `first`/`last` don't form a clean human pair (a first-name
   slot holding a title — e.g. `"Dr Might - CEO"` → first=`"Dr"`), re-derive the real name from
   `full_name` by dropping honorifics + suffixes. `_HONOR = {dr, mr, mrs, ms, miss, mx, prof,
   professor, sir, madam, rev, hon, capt, col, sgt, lt, maj, gen, cmdr, messrs, mstr}`. If fewer than
   2 real name tokens remain, **drop the row** rather than spend a credit on a non-name. A valid pair
   also requires `first != last` and neither token being an honorific or an org token.
   *Titles get the same treatment upstream:* `materialize_dsbs_pocs._scrub_title` NULLs an
   `_HONOR`-only parsed title at emit ("JANE SMITH - Mrs." → title NULL, `raw_entry` verbatim), and
   `gtm_sam_people.usable_title_sql` bars honorific-only values from winning `best_title` from any
   source.
3. **Org-in-person guard.** Drop when **both** name tokens are already tokens of the firm name — the
   "person" is the company name repeated (e.g. **"UIC Government"** sitting in the person field at
   **"UIC Government Services"**). Checked against the union of company-term / SAM / DSBS legal-name
   tokens.

A subject also needs at least a `company_term` **or** a `domain` to be validatable — otherwise it is
un-disambiguatable and skipped.

---

## 10. Priority model

The `_sort` tuple in `build_worklist`, **ascending = best first**:

```python
(tier,
 0 if name_consistent else 1,
 _dollar_band(obl),
 band_ord,            # employee_size_band ordinal
 obl,                 # obligated_usd ASCENDING
 poc_rank,
 -award_count,
 subject_id)
```

| Key | Meaning |
|---|---|
| **`tier`** (dominant) | `0` = **award-active** (`entity_active_obligated_usd > 0`); `1` = `has_federal_awards` only; `2` = neither. In practice the credit spend only ever reaches **tier 0** (all 2,851 ledger rows are tier 0). |
| **`name_consistent`** | Name-consistent firms first (`0`). Higher match confidence, and this flag is *also* what selects the PDL-brand query term (Lever 1). |
| **`_dollar_band(obl)`** | A **VALUE** band, not raw magnitude: `0` = **$1M–$50M (sweet-spot band)**, `1` = $100K–$1M (emerging), `2` = >$50M (mega-contractor), `3` = <$100K (micro/marginal). The sweet-spot band leads because the SBA-declared owner IS the reachable LinkedIn identity there. Very large firms (>$50M) are **deprioritized below emerging firms** — figurehead principals, ultra-common names, lowest resolve rates. Micro sorts last. |
| **`band_ord`** | `employee_size_band` lower-bound ordinal — **smaller / SMB first** (unknown sorts last at 99999). |
| **`obl` ASCENDING** | Raw obligated dollars, ascending, so the *smaller / more-reachable* firm within a band leads. **Raw $ is demoted to a deep tiebreak** so magnitude never dominates the head of the list. |
| **`poc_rank`** | `current_principal`/`both` = `0`, `contact_person` = `1` — a **MINOR** tiebreak only. **57% of DSBS `contact_person` entries are their firm's ONLY named human (the owner)**, so contacts must NOT be starved behind other firms' principals. *(An earlier, pre-#920 version wrongly ranked `poc_type` **above** the value band and resolved 0 pure contacts across ~2,417 credits; #920 demoted it to this minor tiebreak.)* |
| **`-award_count`** | More awards first. |
| **`subject_id`** | Stable final tiebreak → deterministic, resume-safe top-K. |

**Cohort A / lead owner-contacts** (§2) sits at the intersection of the strongest keys: `tier=0`,
`name_consistent`, `_dollar_band=0`, `poc_type=contact_person`.

**Live worklist gate (observed):** 66,076 subjects gated; **tier 0 (award-active) = 4,543**;
tier 1 = 4,919; tier 2 = 56,614; name-consistent = 22,141; company-term source pdl = 22,141 /
legal = 43,935.

---

## 11. Runbook

Run harness (either form works):

```bash
# canonical (project venv)
doppler run -p core-x -c prd -- /Users/benjamincrane/core-x/.venv/bin/python \
  pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py <cmd> [flags]

# self-contained (uv, no project venv)
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
  --with 'psycopg[binary]>=3.2' --with 'requests>=2.32' \
  python3 pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py <cmd> [flags]
```

### Commands (verified against `main()`)

| Command | Credits | What it does |
|---|---|---|
| `init_ops` | 0 | Create the two PG ledgers (idempotent DDL). Run once. |
| `worklist [--out f.parquet]` | 0 | Build + summarize the ranked worklist (tier/name-consistent/source counts + top-12 sample). `--out` writes a Parquet snapshot. |
| `smoke` | 1 | One-credit end-to-end key + validation check against the top worklist subject. |
| `resolve [flags]` | ≤ cap | The spend pass. See flags below. |
| `materialize` | 0 | Rebuild the Lance SoR from `resolved=true` ledger rows that still pass the current gate. |
| `revalidate` | 0 | Re-score every stored `raw_json` under the current `_validate` (free precision re-flag). |
| `verify` | 0 | Print Lance SoR stats (uri, rows, cols, schema, indices). |
| `status` | 0 | Credit accounting + per-tier resolved breakdown. |

### `resolve` flags

- `--pass primary|fallback` (default `primary`) — primary = bare-name+`site:`; fallback = name+firm,
  no `site:`, over unresolved name-consistent rows that have a domain and haven't had a fallback yet.
- `--cap N` — **max credits THIS run** (0 = up to the global ceiling). The practical run-bounding
  lever.
- `--budget N` (default 2500) / `--reserve N` (default 500) — the global ceiling = `budget − reserve`,
  enforced as `sum(credits_spent)`.
- `--workers N` (default 8) — serper concurrency.
- `--dry-run` — print the first 15 queries it *would* run, spend nothing.
- `--one-per-firm` — skip UEIs that already have a resolved person (maximize **distinct-firm**
  coverage — don't spend a 2nd credit at a covered firm). *(Added #921.)*
- `--contacts-only` — restrict to `poc_type=contact_person` (Cohort A / lead owner-contacts).
  *(Added #921.)*

### Copy-pasteable examples

```bash
# one-time ledger creation
doppler run -p core-x -c prd -- .venv/bin/python \
  pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py init_ops

# free: inspect the ranked worklist
doppler run -p core-x -c prd -- .venv/bin/python \
  pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py worklist

# 1-credit smoke test (verifies the fresh key works end-to-end)
doppler run -p core-x -c prd -- .venv/bin/python \
  pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py smoke

# dry-run the next 300 lead owner-contacts at uncovered firms (spends nothing)
doppler run -p core-x -c prd -- .venv/bin/python \
  pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py resolve \
  --pass primary --contacts-only --one-per-firm --cap 300 --budget 100000 --reserve 0 --dry-run

# REAL spend, fresh key: bound the run with --cap (see the multi-key gotcha below)
doppler run -p core-x -c prd -- .venv/bin/python \
  pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py resolve \
  --pass primary --one-per-firm --cap 300 --budget 100000 --reserve 0

# push the validated resolutions to the Lance SoR
doppler run -p core-x -c prd -- .venv/bin/python \
  pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py materialize

# free re-score after a validation tightening
doppler run -p core-x -c prd -- .venv/bin/python \
  pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py revalidate

# credit accounting + Lance stats
doppler run -p core-x -c prd -- .venv/bin/python \
  pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py status
doppler run -p core-x -c prd -- .venv/bin/python \
  pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py verify
```

### Credit / budget mechanics + the multi-key gotcha (IMPORTANT)

- The global ceiling = `--budget − --reserve`, enforced as `sum(credits_spent)` in the spend ledger.
  A run's actual spend is `min(--cap, ceiling − prior_spent)`.
- **The ledger does NOT distinguish serper API keys.** `credits_spent` is a *total across every key
  ever used*. So when you move to a **fresh key** (a new 2,500-credit account), the ceiling math based
  on the old accumulated ledger is **misleading** — with the defaults (`budget=2500, reserve=500`)
  the ceiling is already exhausted (`status` shows `remaining_to_ceiling` negative, currently −846).
- **Operating practice on a fresh key:** set `--budget` high, `--reserve 0`, and **rely on `--cap N`
  to bound the run.** The real per-key account balance is tracked out-of-band by the operator (check
  the serper dashboard). Example: `--budget 100000 --reserve 0 --cap 300`.
- **Future improvement (see §15):** add per-serper-key credit accounting (a `serper_key_id` / key
  fingerprint column) so the ceiling can be enforced per account instead of globally.

---

## 12. Spend history & current state

**Observed live (2026-07-02) — authoritative:**

| Metric | Value |
|---|---|
| Subjects queried (ledger rows) | **2,851** |
| Credits spent (all keys) | **2,846** |
| Resolved (validated `/in/`) | **845** (29.6% resolve rate) |
| Distinct firms covered | **788** |
| Lance SoR rows | **821** |
| Ledger `company_source` | 100% `pdl` |
| Resolved by `poc_type` | `both` 370 · `current_principal` 360 · `contact_person` 115 |
| Ledger tier | 100% tier 0 (award-active) |

**Timeline:**
- **PR #915** — initial pipeline. First serper key: ~2,417 credits (incl. ~21 calibration) → ~730
  validated (~30%). Under the pre-#920 ranking these were all `current_principal`/`both` — pure
  `contact_person` subjects were starved (0 resolved).
- **PR #920** — elevated `contact_person` (demoted `poc_rank` to a minor tiebreak) **and** added the
  entity-drop gate (§9.1). Re-materialize dropped entity false-positives (SoR 725→706 at that time).
- **PR #921** — added `--one-per-firm` + `--contacts-only`.
- **Second (fresh) key** — 429 credits on Cohort A (lead owner-contacts) at uncovered firms → 115
  resolved (~26.8%), overwhelmingly owners/CEOs/presidents. (These are the 429 `contact_person`
  ledger rows.)

**Resolve rate is a flat ~30% across value bands** — driven by name commonness + LinkedIn presence,
not firm size. So the ranking changes **which firms get covered**, not the hit rate. Validation is
deliberately precise (abstains on namesakes), so the misses are mostly common-name ambiguity and
genuine LinkedIn absence.

---

## 13. Environment / secrets

Doppler project **`core-x`**, config **`prd`** (injected at run time by `doppler run -p core-x -c
prd -- …`):

| Secret | Used for |
|---|---|
| `SERPER_API_KEY` | The serper egress (`core/serper_gateway.py`). Never logged/persisted. |
| `R2_ENDPOINT` **or** `R2_ACCOUNT_ID` | R2 endpoint (`https://<account>.r2.cloudflarestorage.com` if only the account id is set). |
| `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | R2 (Lance source read + SoR write). |
| `HQX_DB_URL_POOLED` (fallback `HQX_DB_URL`) | The Postgres ledgers (`ops` schema). |

Optional tuning env (defaults shown): `SERPER_BUDGET=2500`, `SERPER_RESERVE=500`,
`SERPER_WORKERS=8`, `SERPER_TIMEOUT=20`, `SERPER_MAX_RETRIES=4`, `SERPER_RETRY_BACKOFF=1.5`,
`RESOLVE_MEMORY_LIMIT=12GB`, `RESOLVE_THREADS=4`, `RESOLVE_SCRATCH=/tmp/dsbs_poc_linkedin`,
`PDL_CHUNK=8000`, `DSBS_POC_LINKEDIN_URI` (override the SoR URI).

---

## 14. Git & ops caveats (the worktree/WIP trap)

> **Do NOT `git add -A` in `/Users/benjamincrane/core-x`.**

The operator's working checkout `/Users/benjamincrane/core-x` rides a feature branch
(`feat/naf-census` / `feat/naf-materialize`) and carries a large amount of **untracked naf-census
work-in-progress** (e.g. `pipelines/naf/*`, `reports/*`, `exports/`). The `main` branch is checked
out in a **separate worktree** (`.claude/worktrees/…`).

Therefore, when shipping a change to this pipeline: commit **only the intended files**. The safe
pattern is to capture a one-file diff as a patch, `git checkout -b` a fresh branch off `origin/main`,
apply the patch there, and PR that. **Squash-merge** PRs to `main`. PRs are CI/visibility artifacts,
**not** approval gates — merge them yourself, then pull into every operator-facing checkout so disk
truth matches the merged commit. Stacked PRs + squash is a trap (squash drops anything added after the
original diff) — open against `main` directly.

---

## 15. Open decisions & recommended next steps

The fresh serper key has budget remaining (~2,000 credits at handoff — **track the true balance on the
serper dashboard, not the ledger**, per §11's gotcha). Continuation options, in priority order:

1. **`resolve --pass primary --one-per-firm`** (no `--contacts-only`) across all uncovered
   award-active firms, value-ranked. **Maximal distinct high-value firm coverage** — one best
   decision-maker per firm (principal *or* contact, whichever ranks first).
2. **Remaining award-active `contact_person` subjects** at uncovered firms not already in Cohort A
   (e.g. lower dollar bands / non-name-consistent).
3. **The unused `--pass fallback`** to rescue primary-misses that have a domain
   (`_fallback_candidates`: unresolved, name-consistent, has domain, `fallback_done=false`). Costs a
   2nd credit each; a genuinely different retrieval.

**After any spend, run `materialize`** to refresh the Lance SoR.

**Future engineering improvements:**
- **Per-serper-key credit accounting** — add a key fingerprint column to the spend ledger so the
  ceiling is enforced per account, eliminating the multi-key gotcha (§11).
- **Fuzzy dedupe (last-name + first-initial)** to merge nickname/middle/maiden-name variants that
  currently split one human across a `contact_person` row and a `current_principal` row. Exact-name
  dedupe (which produces `poc_type='both'`) misses these — e.g. `"TOM MORTON"` vs `"Thomas Morton"`
  are two subjects today.
- **Replace the deprecated "prime" wording** in the `--contacts-only` argparse help string (§2).

---

## 16. Glossary

| Term | Meaning |
|---|---|
| **DSBS** | SBA **Dynamic Small Business Search** — the SBA's public directory of small businesses, source of the certified-firm universe and the POC people. |
| **UEI** | **Unique Entity Identifier** — the SAM.gov 12-char federal entity id. Both DSBS and SAM are UEI-native, 1 row per UEI. |
| **POC** | **Point of Contact** — a DSBS-declared person: either the firm's single `contact_person` or one of its `current_principals`. |
| **PDL** | **People Data Labs** — the firmographic provider whose company sidecar supplies the operating/brand `company_name` and `linkedin_slug` (Lever 1). |
| **serper / serper.dev** | The Google Search API used for retrieval (real Google organic results as structured JSON, no throttling). Bills 1 credit per 200 response; 2,500 credits per account. |
| **award-active** | `entity_active_obligated_usd > 0` — the firm has active obligated federal dollars. `priority_tier=0`. |
| **obligated_usd** | `firmographics_company_map_serving.entity_active_obligated_usd` — active obligated federal dollars for the firm. Drives tier + dollar band. |
| **sweet-spot band** | The `$1M–$50M` obligated-dollar value band (`_dollar_band==0`); ranks first. |
| **Cohort A / lead owner-contacts** | award-active · name-consistent · sweet-spot band · `contact_person`. |
| **name_key** | Order-independent normalized name: lowercased alpha tokens ≥2 chars, suffixes dropped, sorted, space-joined. Used to dedup people and to anti-join `dsbs_poc_people`. |
| **subject_id** | `uei \| name_key` — the grain of a resolution and the spend-ledger PK. |
| **name_consistent** | `crosswalk_dsbs_sam.matched_name_consistent` — DSBS firm-name tokens overlap the PDL brand tokens (≥1). Gates Lever 1 and ranks second in the sort. |
| **company_source** | `pdl` (brand term, Lever 1 fired) or `legal` (SAM/DSBS legal name fallback). |
| **company_term** | The suffix-stripped company string actually placed in the query / used for validation. |
| **poc_type** | `current_principal`, `contact_person`, or `both` (a person who is both). |
| **SoR** | System of record — here the Lance dataset `s3://data-sink/active/dsbs_poc_linkedin/`. |
| **validation_reason** | `validated` / `no_confident_match` (saw `/in/` but no confident match) / `no_in_link` (no `/in/` result) / `error`. |
