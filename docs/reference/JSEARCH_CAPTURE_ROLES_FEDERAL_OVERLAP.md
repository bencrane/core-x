# JSearch Capture-Roles Feed — Federal Prime/Subaward Overlap

> **🟩 LIVE FEED + AD-HOC OVERLAP ANALYSIS.** The `jsearch_capture_roles` harvest feed is
> built and landed (Gen-3 Lance SoR, indexed). The prime/subaward overlap numbers in §4 are
> **ad-hoc** — computed live from scratch scripts (§5), **not yet materialized as a durable
> dataset**. The durable form is specified in §7 and is the primary open work item. Any agent
> picking this up: read §3 (methodology) before trusting §4, and §6 (caveats) before quoting a
> number outward.

**Data as-of:** feed materialized `2026-07-03 03:22 UTC`; overlap analysis run `2026-07-04`.
**Repo state:** feed code on `main` (PRs #918 → #929 → #931, all 2026-07-02).
**Owning plane:** `core-x` data/compute plane. Gen-3 SoR = LanceDB under `s3://data-sink/active/`.

---

## 0. TL;DR

A company posting a **capture-manager** role is actively investing in winning US federal
contracts — a high-intent govcon account signal. The feed harvests those postings from JSearch
(OpenWeb Ninja's Google-for-Jobs aggregate) and lands them. This doc adds the downstream
question the feed was built to answer: **which of these employers are actually in the federal
market, as primes and/or subawardees, and how recently.**

- **9,422** postings harvested → **4,019** distinct companies.
- **1,730 (43%)** have a federal footprint (prime *or* subaward, all-time).
- **1,492 (37%)** win **prime** awards; **1,414 (35%)** win **subawards**; **1,176** do **both**.
- **1,405 (35%)** are **actively winning** (prime or sub action within the last 24 months).
- The subawardees skew established: **83% of subawardees also hold prime awards** — they are
  two-sided contractors, not pure subs.

---

## 1. The feed — what was built

Built 2026-07-02. Two-stage, land-then-hydrate, LeadMagic-shaped (mirrors `src/trigger/sam_opps_bulk.ts`).

### 1.1 Components

| File | Role |
|---|---|
| [`core/openwebninja_gateway.py`](../../core/openwebninja_gateway.py) | Transport-only JSearch `/search-v2` gateway. Credit-metered (**1 credit / page**, ≤10 jobs). Never-raises envelope (`ok`/`credits`/`cursor`/`jobs`/`raw`); retry on 429/5xx, short-circuit `credits=0` on 401/402/403. |
| [`pipelines/jsearch/harvest_capture_roles.py`](../../pipelines/jsearch/harvest_capture_roles.py) | The worker (41 KB). Dual-mode: Modal `@app.function` (prod, via Universal Dispatcher) + local `argparse` CLI. Stage 1 **harvest** → PG; stage 2 **materialize** → Lance. |
| [`src/trigger/jsearch_capture_roles.ts`](../../src/trigger/jsearch_capture_roles.ts) | Control plane. `jsearch-capture-roles-daily` (cron `0 13 * * *` UTC, `date_posted=3days`) + `jsearch-capture-roles-backfill` (manual, `date_posted=all`). Dispatch → durable waitpoint → Modal RAW callback. Auto-registered via `trigger.config.ts` `dirs:["./src/trigger"]`. |
| [`pipelines/jsearch/ops_jsearch_capture.sql`](../../pipelines/jsearch/ops_jsearch_capture.sql) | Repo-parity mirror of the inline `OPS_DDL`. **Inline Python copy is authoritative.** |

### 1.2 SoR chain

```
JSearch /search-v2  (OpenWeb Ninja; LinkedIn/Indeed/Glassdoor/ZipRecruiter/… aggregate)
   │  title-variant + geo×title fan-out, cursor pagination, 1 credit/page
   ▼
ops.jsearch_capture_postings      ← HQX Postgres landing SoR, grain = job_id
   │  ON CONFLICT (job_id) DO UPDATE (preserve first_seen_at + first query_variant)
   │  psycopg SELECT → Apache Arrow (NEVER pandas)
   ▼
s3://data-sink/active/jsearch_capture_roles/   ← Lance v2.1 SoR (full overwrite from PG truth)
   BTREE[job_id, employer_domain, employer_name]
   BITMAP[publisher, job_state, query_variant, employment_type,
          employer_is_confidential, employer_is_staffing]
```

**Land everything.** Recruiter/staffing fronts and "Confidential" employers are **flagged, not
dropped** (`employer_is_staffing` / `employer_is_confidential`). Filtering at harvest destroys
recoverable signal; the SoR carries raw truth and a stage-2 reader refines.

### 1.3 Query matrix

- **Daily incremental:** 7 core title variants (`QUERY_VARIANTS`), `date_posted=3days`. Lean —
  geo-sharding daily would just re-fetch the same live postings.
- **Backfill:** 25 national title queries (`EXTENDED_TITLES`) + **10 geo-titles × 133 govcon
  hub cities** (`GEO_TITLES` × `HUB_CITIES`) ≈ **1,355 queries**, deduped by `job_id` at upsert.
  Geo-sharding pierces Google-for-Jobs' per-query result cap.

### 1.4 Operational state (as-of `2026-07-03 03:22 UTC`)

- `9,422` postings in `ops.jsearch_capture_postings`; `33` confidential, `160` staffing-flagged.
- Last `materialize` run: **success**. Backfill runs landed (e.g. `jobs_seen=20,629 / jobs_new=4,114 / credits=3,665`).
- Secrets (Modal): `r2-credentials`, `hqx-postgres`, `openwebninja-api` (`OPENWEBNINJA_API_KEY`).
- **Deploy/cron liveness not verified in this pass.** PR #918 shipped "wiring only, undeployed";
  #929/#931 expanded the backfill. Confirm with `status` / `verify` (§5.1) and the Trigger dashboard.

---

## 2. Data assets in the overlap join

| Dataset | URI / location | Rows | Grain | Columns used here |
|---|---|---|---|---|
| jsearch capture-roles | `s3://data-sink/active/jsearch_capture_roles/` | 9,422 | `job_id` | `employer_domain`, `employer_name`, `employer_is_staffing`, `employer_is_confidential` |
| SAM master domains **(bridge)** | `s3://data-sink/active/sam_master_domains/` | 709,546 | (`uei`,`domain`) | `normalized_domain` → `uei` |
| DSBS↔SAM crosswalk (alt bridge) | `s3://data-sink/active/crosswalk_dsbs_sam/` | 67,234 | `uei` | `best_domain`, `normalized_domain` (DSBS-scoped; **not used** — `sam_master_domains` is broader) |
| subaward canonical | `s3://data-sink/active/usaspending_subaward_canonical/` | 1,315,680 | (prime, subaward_number) | `subawardee_uei`, `subawardee_name`, `subaward_action_date` |
| prime award canonical | `s3://data-sink/active/usaspending_award_canonical/` | 30,683,126 | award (393-col OBT) | `recipient_uei`, `recipient_name`, `action_date` |

Universe sizes: subawardee side = **105,189** distinct `subawardee_uei` / 94,339 normalized names.

---

## 3. Methodology — the bridge problem

**jsearch has no UEI.** Postings carry only `employer_name` + `employer_website` → `employer_domain`.
The federal datasets key on **UEI**. So every match must cross a bridge. Two independent paths;
a company counts as prime/sub if **either** hits (union). Grain throughout = one company per
`coalesce(employer_domain, lower(employer_name))` (the same key the feed's `status` uses).

### 3.1 Match path A — domain → SAM UEI → federal key (hard, precise)

```
jsearch.employer_domain  ──normalize──▶  sam_master_domains.normalized_domain ──▶ uei
   uei ∈ {subaward.subawardee_uei}   ⇒ subawardee
   uei ∈ {award.recipient_uei}       ⇒ prime
```

### 3.2 Match path B — normalized name (recall, softer)

```
norm(jsearch.employer_name) == norm(subaward.subawardee_name)   ⇒ subawardee
norm(jsearch.employer_name) == norm(award.recipient_name)       ⇒ prime
```
Name path restricted to normalized length ≥ 4 and excludes staffing/confidential-flagged rows to
suppress collisions.

### 3.3 Normalization (identical on both sides of each join)

```python
# domain
DNORM = "regexp_replace(lower(trim({c})), '^www\\.', '')"

# name: lower → non-alnum→space → strip legal suffixes → strip spaces
NNORM = (
  "regexp_replace(regexp_replace(regexp_replace(lower(trim({c})),'[^a-z0-9]+',' ','g'),"
  "'\\b(inc|incorporated|llc|l l c|llp|lp|plc|corp|corporation|co|company|ltd|limited|"
  "the|group|holdings|hldgs|intl|international)\\b','','g'),' ','','g')"
)
```

### 3.4 Recency

`subaward_action_date` and `action_date` are cast to `DATE` (`try_cast`), a per-company
`max(date)` is taken across all matched rows, then bucketed against `TODAY = 2026-07-04`
(24 mo cutoff = `2024-07-04`). Prime `action_date` reflects the **latest action** (mods keep
multi-year awards fresh), so prime recency runs hotter than subaward recency by construction.

### 3.5 Out-of-core note

Prime canonical is 30.68M rows. It is **streamed** through DuckDB via a projected Lance
scanner (`recipient_uei`, `recipient_name`, `action_date` only) — never materialized whole.
Two passes (UEI join, name join) against a ~4k build side; `memory_limit=8GB`, spill to scratch.
The small sides (jsearch 9.4k, sam 710k, subaward 1.3M) load fully to Arrow.

---

## 4. Findings

### 4.1 Company resolution funnel

```
4,019  distinct companies harvested
2,524  have a resolvable domain            (1,495 are name-only, no website on posting)
1,452  domain resolves to a SAM-registered UEI      (58% of domained)
  855  that UEI is an actual subawardee             (path A only)
```

### 4.2 Subawardee overlap (all-time)

| Path | Method | Matches |
|---|---|---|
| A — domain→UEI | `employer_domain`→SAM `uei`→`subawardee_uei` | 855 |
| B — name | norm `employer_name` == `subawardee_name` | 1,153 |
| A ∩ B | corroborated | 594 |
| **A ∪ B** | **any path = subawardee** | **1,414 (35.2%)** |

### 4.3 Prime × Subaward 2×2 (all-time, of 4,019)

| Segment | Count | % |
|---|---|---|
| Win **prime** (any) | **1,492** | 37% |
| Win **subaward** (any) | 1,414 | 35% |
| **Both** | **1,176** | 29% |
| Prime **only** | 316 | 8% |
| Sub **only** | 238 | 6% |
| **Either** (federal footprint) | **1,730** | 43% |
| Neither | 2,289 | 57% |

→ Of the 1,414 subawardees, **1,176 (83%) also hold prime awards.**

### 4.4 Recency ladders

| Window (cutoff) | Subawardees (`subaward_action_date`) | Primes (`action_date`) |
|---|---|---|
| last 12 mo (2025-07-04) | 640 | 1,106 |
| **last 24 mo (2024-07-04)** | **937** (66% of subs) | **1,241** (83% of primes) |
| last 36 mo (2023-07-04) | 1,076 | — |
| last 5 yr (2021-07-04) | 1,197 | 1,365 |
| stale (>24 mo) | 477 | — |

### 4.5 The targeting number

**1,405 of 4,019 (35%)** are **actively winning** federal work — a prime *or* subaward action
within the last 24 months. That is the live-govcon core of the capture-role harvest.

---

## 5. Reproduce

All commands run from repo root with `doppler run -p core-x -c prd -- uv run --no-project …`.
Secrets injected: `R2_*`, `HQX_DB_URL*`, `OPENWEBNINJA_API_KEY`.

### 5.1 Feed read-backs (built-in)

```bash
# ⚠ the file does `import modal` unconditionally at module top — add --with 'modal'
#   even for the local, Modal-less read-backs (the in-repo docstring omits it; bug).
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'modal' --with 'pylance>=7' --with 'pyarrow>=17' \
  --with 'psycopg[binary]>=3.2' --with 'requests>=2.32' \
  python3 pipelines/jsearch/harvest_capture_roles.py status   # PG counts, distinct_employers, run ledger
#   … verify   → Lance dataset existence / rowcount / indices
#   … smoke    → single live JSearch page (1 credit)
```

### 5.2 Overlap analysis (ad-hoc scripts)

Written to the session scratchpad (ephemeral — **the canonical logic is embedded in §3 and §8**;
re-create from there if the scratch files are gone):

- `jsearch_company_breakdown.py` — §4.1 funnel (PG-side distinct counts).
- `jsearch_subawardee_overlap.py` — §4.2 subawardee union.
- `jsearch_prime_sub_crosstab.py` — §4.3–4.5 full 2×2 + recency (streams the 30.68M prime rows).

```bash
mkdir -p /tmp/jx_crosstab
SCRATCH=/tmp/jx_crosstab doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.1' \
  python3 <path>/jsearch_prime_sub_crosstab.py
```

The consolidated, self-contained crosstab script is reproduced in **§8 (Appendix)**.

---

## 6. Known gaps & caveats — read before quoting a number

1. **All counts skew low (floor).** Resolution misses: domains absent from `sam_master_domains`,
   subawardees recorded under DUNS-era rows without a UEI, and name-spelling variants that don't
   normalize-equal. 2,289 "neither" companies include genuine non-federal firms **and**
   resolution failures — the two are not separated here.
2. **No parent rollup.** Prime match is to the direct `recipient_uei` / name. A firm that wins
   only through a differently-keyed subsidiary or parent is undercounted. Rolling up via
   `entity_hierarchy` (`s3://data-sink/active/entity_hierarchy/`, `uei → ultimate_parent_uei`)
   would raise recall — deliberately not done here.
3. **Name-path false positives.** Normalized exact-name match can collide (common/short names).
   Mitigated by length ≥ 4 + staffing/confidential exclusion, not eliminated. Path A (domain→UEI)
   has no such risk — prefer it when precision matters. 594 of 1,153 name matches are
   domain-corroborated.
4. **Time-unboundedness of "any".** All-time footprint counts a single historical award. Use the
   §4.4 recency ladder for "is this account live," not the all-time columns.
5. **Feed liveness unconfirmed.** See §1.4 — verify the daily cron is actually firing before
   assuming the 4,019 is current; it reflects the 2026-07-02 backfill + whatever incrementals ran.
6. **`sam_master_domains` domain→UEI is many-to-many.** A domain can map to several UEIs (holding
   companies, shared registrations); path A treats a company as prime/sub if **any** mapped UEI
   qualifies. This is intentional (recall) but can over-attribute for shared/reseller domains.

---

## 7. Open work — the durable bridge (primary next item)

The overlap is currently ad-hoc scratch. Productionize it as an append-/overwrite Lance dataset
so downstream serving (GTM audiences, scoring, MCP recall) reads a stable SoR instead of
re-deriving. This is the `employer_website → federal entity` resolution the feed was designed to feed.

**Proposed dataset:** `s3://data-sink/active/jsearch_capture_roles_federal_bridge/` (Lance v2.1, OVERWRITE snapshot).

**Grain:** one row per jsearch `company_key` (= `coalesce(employer_domain, lower(employer_name))`).

**Proposed schema:**

| Column | Type | Notes |
|---|---|---|
| `company_key` | text | PK / BTREE. `coalesce(employer_domain, lower(employer_name))` |
| `employer_name` | text | representative (most-recent posting) |
| `employer_domain` | text | normalized; nullable |
| `resolved_uei` | text[] | all SAM UEIs the domain maps to (BITMAP on cardinality bucket) |
| `is_prime` | bool | any `recipient_uei`/name match — BITMAP |
| `is_subawardee` | bool | any `subawardee_uei`/name match — BITMAP |
| `match_path_prime` | text | `domain` \| `name` \| `both` \| `none` — BITMAP |
| `match_path_sub` | text | idem |
| `last_prime_action_date` | date | max `action_date` |
| `last_subaward_action_date` | date | max `subaward_action_date` |
| `prime_active_24mo` | bool | derived |
| `sub_active_24mo` | bool | derived |
| `is_staffing` / `is_confidential` | bool | carried from feed |
| `built_at` | timestamptz | run stamp |

**Build approach:** Lance(jsearch, sam_master_domains, subaward_canonical, award_canonical) →
DuckDB (stream the 30.68M prime, §3.5) → Arrow → `lance.write_dataset(..., mode="overwrite",
data_storage_version="2.1")` → BTREE[`company_key`] + BITMAP[the bool/path columns]. Ops ledger
row → `ops.jsearch_federal_bridge_runs` (mirror the feed's `_record_run` pattern). Place the
builder at `pipelines/jsearch/build_federal_bridge.py`; wire a Trigger task only if a refresh
cadence is wanted (it recomputes cheaply; a manual/monthly cadence is sufficient since the prime
canonical refreshes on its own schedule).

**Recall upgrades to fold in at build time (each raises the match rate off the §6 floor):**
- Parent rollup via `entity_hierarchy` (caveat #2).
- Add `crosswalk_dsbs_sam.best_domain` as a second domain→UEI bridge for DSBS-only firms.
- Fuzzy name match (token-set / trigram) for the 1,495 name-only companies, gated by a score
  threshold and kept in a separate `name_fuzzy` match-path so precision stays auditable.

**Downstream consumers to notify** once built: the GTM audience marts (`gtm_*`), subawardee
designation/serving pipelines (`pipelines/serving/materialize_subawardee_*`), and any MCP recall
surface — a "hiring a capture manager AND actively winning subawards" segment is a strong,
novel GTM signal none of them currently have.

---

## 8. Appendix — consolidated crosstab script

Self-contained; reproduces §4.3–4.5. Requires `pylance`, `pyarrow`, `duckdb`; env from doppler.

```python
import os, lance, duckdb

def so():
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}

S = so(); ACTIVE = "s3://data-sink/active"
C24, C12, C36, C60 = "2024-07-04", "2025-07-04", "2023-07-04", "2021-07-04"  # cutoffs vs TODAY=2026-07-04

con = duckdb.connect()
con.execute("PRAGMA memory_limit='8GB'; PRAGMA threads=4;")
con.execute(f"PRAGMA temp_directory='{os.environ.get('SCRATCH','/tmp/jx_crosstab')}';")

DNORM = "regexp_replace(lower(trim({c})), '^www\\.', '')"
NNORM = ("regexp_replace(regexp_replace(regexp_replace(lower(trim({c})),'[^a-z0-9]+',' ','g'),"
         "'\\b(inc|incorporated|llc|l l c|llp|lp|plc|corp|corporation|co|company|ltd|limited|the|"
         "group|holdings|hldgs|intl|international)\\b','','g'),' ','','g')")
dn = lambda c: DNORM.format(c=c); nn = lambda c: NNORM.format(c=c)

js  = lance.dataset(f"{ACTIVE}/jsearch_capture_roles/", storage_options=S).to_table(
        columns=["employer_domain","employer_name","employer_is_staffing","employer_is_confidential"])
sam = lance.dataset(f"{ACTIVE}/sam_master_domains/", storage_options=S).to_table(columns=["normalized_domain","uei"])
sub = lance.dataset(f"{ACTIVE}/usaspending_subaward_canonical/", storage_options=S).to_table(
        columns=["subawardee_uei","subawardee_name","subaward_action_date"])
con.register("js_raw", js); con.register("sam_raw", sam); con.register("sub_raw", sub)

con.execute(f"""CREATE TABLE js AS
WITH b AS (SELECT NULLIF({dn('employer_domain')},'') dom, NULLIF(lower(trim(employer_name)),'') nm,
                  NULLIF({nn('employer_name')},'') nmn, employer_is_staffing st, employer_is_confidential cf FROM js_raw)
SELECT coalesce(dom,nm) company_key, max(dom) dom, max(nmn) nmn, bool_or(st) is_staffing, bool_or(cf) is_conf
FROM b WHERE coalesce(dom,nm) IS NOT NULL GROUP BY 1""")
con.execute(f"CREATE TABLE sam AS SELECT DISTINCT {dn('normalized_domain')} dom, uei FROM sam_raw WHERE normalized_domain IS NOT NULL AND uei IS NOT NULL")
con.execute("CREATE TABLE js_uei  AS SELECT DISTINCT j.company_key, s.uei FROM js j JOIN sam s ON j.dom=s.dom WHERE j.dom IS NOT NULL")
con.execute("CREATE TABLE js_name AS SELECT DISTINCT company_key, nmn FROM js WHERE nmn IS NOT NULL AND length(nmn)>=4 AND NOT is_staffing AND NOT is_conf")

con.execute(f"""CREATE TABLE sub_hits AS
SELECT company_key, max(d) last_sub FROM (
  SELECT u.company_key, try_cast(s.subaward_action_date AS DATE) d FROM sub_raw s JOIN js_uei u ON s.subawardee_uei=u.uei
  UNION ALL
  SELECT n.company_key, try_cast(s.subaward_action_date AS DATE) d FROM sub_raw s JOIN js_name n ON {nn('s.subawardee_name')}=n.nmn
) GROUP BY 1""")

prd = lambda: lance.dataset(f"{ACTIVE}/usaspending_award_canonical/", storage_options=S)\
        .scanner(columns=["recipient_uei","recipient_name","action_date"], batch_size=131072).to_reader()
con.register("p1", prd())
con.execute("CREATE TABLE prime_uei AS SELECT u.company_key, max(try_cast(p.action_date AS DATE)) last_prime FROM p1 p JOIN js_uei u ON p.recipient_uei=u.uei GROUP BY 1")
con.unregister("p1"); con.register("p2", prd())
con.execute(f"CREATE TABLE prime_name AS SELECT n.company_key, max(try_cast(p.action_date AS DATE)) last_prime FROM p2 p JOIN js_name n ON {nn('p.recipient_name')}=n.nmn GROUP BY 1")
con.unregister("p2")
con.execute("CREATE TABLE prime_hits AS SELECT company_key, max(last_prime) last_prime FROM (SELECT * FROM prime_uei UNION ALL SELECT * FROM prime_name) GROUP BY 1")

con.execute("""CREATE TABLE m AS
SELECT j.company_key, sh.last_sub, ph.last_prime,
       (sh.company_key IS NOT NULL) is_sub, (ph.company_key IS NOT NULL) is_prime
FROM js j LEFT JOIN sub_hits sh USING(company_key) LEFT JOIN prime_hits ph USING(company_key)""")

q = lambda s: con.execute(s).fetchone()[0]
print("companies      :", q("SELECT count(*) FROM m"))
print("prime any      :", q("SELECT count(*) FROM m WHERE is_prime"))
print("sub any        :", q("SELECT count(*) FROM m WHERE is_sub"))
print("both           :", q("SELECT count(*) FROM m WHERE is_prime AND is_sub"))
print("prime only     :", q("SELECT count(*) FROM m WHERE is_prime AND NOT is_sub"))
print("sub only       :", q("SELECT count(*) FROM m WHERE is_sub AND NOT is_prime"))
print("either         :", q("SELECT count(*) FROM m WHERE is_prime OR is_sub"))
for lbl, c in [("sub 24mo", C24), ("sub 12mo", C12), ("sub 5y", C60)]:
    print(f"{lbl:<14}:", q(f"SELECT count(*) FROM m WHERE last_sub >= DATE '{c}'"))
for lbl, c in [("prime 24mo", C24), ("prime 12mo", C12)]:
    print(f"{lbl:<14}:", q(f"SELECT count(*) FROM m WHERE last_prime >= DATE '{c}'"))
print("active 24mo    :", q(f"SELECT count(*) FROM m WHERE last_sub >= DATE '{C24}' OR last_prime >= DATE '{C24}'"))
```

---

## 9. Change log

| Date | Change |
|---|---|
| 2026-07-02 | Feed built — PRs #918 (wiring), #929 (expanded backfill), #931 (NUL-sanitize + 48→133 hubs, 6→10 geo titles). |
| 2026-07-04 | This doc — ad-hoc prime/subaward overlap + recency analysis; durable bridge spec (§7). |
