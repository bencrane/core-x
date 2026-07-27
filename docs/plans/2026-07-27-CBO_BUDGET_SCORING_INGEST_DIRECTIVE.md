# Directive: CBO Budget Scoring — cost estimates & baseline projections, the legislation grain (R2/Lance, core-x)

**Status:** ready for executor — **but read §2.1 first: the primary host hard-blocks automated clients.**
**Created:** 2026-07-27 UTC
**Type:** Ingest — the **legislation layer**, which nothing else in this program reaches. The two sibling directives measure money once it exists in an account (appropriated → apportioned → obligated). CBO measures the *decision that created it*: what a bill was scored to cost, by title, over a ten-year window. **This is where the `$785B` OBBA figure is expected to live** (CBO is the direct source; the sibling establishes USAspending's DEFC cannot tag OBBA) — but per §2.1 cbo.gov is blocked and the GovInfo route is unverified, so "expected," not "confirmed."
**Initiated by:** human (operator: "I prefer for the agent to basically 'get everything' … like the apportionment as well as the CBO data as well.")
**Predecessor:** `/Users/benjamincrane/core-x/docs/plans/2026-07-27-FEDERAL_APPROPRIATIONS_INGEST_DIRECTIVE.md`

---

## 🚀 Executor kickoff (read this first if picking up cold)

1. **Repo = `core-x`.** Gen-3 Lance ingest: ephemeral fetch → parse → `lance.write_dataset` to `s3://data-sink/active/<name>/`. Raw is transport-only; Lance is the SoR; no catalog layer.
2. **⚠ THIS DIRECTIVE HAS A LIVE BLOCKER.** `cbo.gov` returns **HTTP 403 to every automated client tested**, including one sending a full browser UA + Accept + Accept-Language header set (§2.1, Evidence). **Do NOT burn the cycle on user-agent tricks, header permutations, or proxies** — that is the exact failure the predecessor program recorded against `bls.gov` ("bls.gov web AND flat files hard-403 (Akamai) — do NOT attempt UA tricks"). Work §2.2 → §2.3 → §2.4 in order and stop at the first route that yields data.
3. **Worktree discipline (L0):** fresh branch off `main` (`claude/cbo-budget-scoring-ingest`); run from the checkout root.
4. **Secrets (L1):** `doppler run -p core-x -c prd --` injects `R2_*` + `HQX_DB_URL_POOLED`. **Check Doppler for `GOVINFO_API_KEY` / `DATA_GOV_API_KEY` before assuming a key must be minted** (§2.2).
5. **Fleet plumbing — reuse verbatim:** model on `pipelines/reference/industry_cost_structure_ingest.py`. Reuse `_build_indexes` / `_storage_options` from `pipelines/bls/ingest.py`. **⚠ The rate governor (token bucket, warm-up, circuit breaker, path-checkpoint) does NOT exist in either predecessor** (grep-confirmed) — it MUST be written new as a shared helper `pipelines/_lib/rate_governor.py`, landed with a unit test (sustained ≤2 req/s over any 10 s window; a synthetic `403` trips the breaker and a second trip returns `disposition='throttled'`; the path-checkpoint round-trips across a process restart). All three sibling directives import it; every network call routes through it. "Reuse verbatim" covers the skeleton ONLY.
6. **Zero LLM.** Deterministic parses only. **This is enforced hard here:** CBO cost estimates are prose PDFs with tables. Extracting numbers from prose with a model is out of scope — see §2.5 for what is and is not in scope.
7. **Git lifecycle end-to-end:** commit by explicit path → push → PR → self-merge after gates pass → `git -C /Users/benjamincrane/core-x pull` → `git log -1 --oneline`. Merged ≠ done.

## ⚠ Parallel-execution note

Safe to run concurrently with both siblings — disjoint hosts, disjoint datasets, separate ledgers:

| Directive | Host(s) |
|---|---|
| `2026-07-27-FEDERAL_APPROPRIATIONS_INGEST_DIRECTIVE.md` | `whitehouse.gov`, `api.usaspending.gov` |
| `2026-07-27-OMB_APPORTIONMENT_INGEST_DIRECTIVE.md` | `apportionment-public.max.gov` |
| **this one** | **`api.govinfo.gov` / operator landing drop** |

## ⚠ RATE DISCIPLINE (binding — read before writing the fetch loop)

**Assume the host will cut you off without warning.** These publishers return no
rate-limit headers — no `X-RateLimit-*`, no `Retry-After`. There is no warning shot: a
host either tolerates you or goes straight to a block page. This program has already been
hard-`403`'d at the edge by `bls.gov` and `cbo.gov`; those blocks are IP-scoped and can
persist for hours. A block does not just fail the run — it can cost access to the source
for the rest of the day, and it is not undoable by retrying.

**Binding limits. Do not raise them without an operator ruling. Do not "test" them.**

1. **Concurrency ≤ 3 workers. Sustained rate ≤ 2 req/s aggregate**, enforced by a token
   bucket, not `sleep()` between calls.
2. **Warm-up ramp.** First 100 requests at **1 req/s, single worker**. Ramp to the ceiling
   only after 100 consecutive clean `200`s. Any non-200 resets the counter.
3. **Circuit breaker — halt, never grind.** 3 consecutive non-200s, *or* any single `403`
   or `429`: stop all workers immediately, sleep **300 s**, resume at warm-up settings. A
   **second** trip in the same run: **halt the run**, write the ledger row (`status='failed'`, `disposition='throttled'`), flush the checkpoint, surface to the operator. Retrying into a
   wall is what converts a soft throttle into a persistent IP block.
4. **Honor `Retry-After`** if it appears, over every other setting here.
5. **Checkpoint every 200 completed files.** A block must cost only the in-flight batch,
   never the crawl. Re-runs resume from the checkpoint and re-fetch nothing cached.
6. **Descriptive User-Agent** — identify the client honestly, e.g.
   `core-x-data-factory/1.0 (federal reference-data ingest; contact: <operator email>)`.
   Never spoof a browser UA to evade a block (see the `cbo.gov` rule).
7. **One agent per host, ever.** These directives are parallel-safe *because* they touch
   disjoint hosts. Never run two agents, two shells, or two `--stream` invocations against
   the same host concurrently — that silently doubles the rate the host sees.
8. **Never probe a host to discover its limit.** Do not burst, benchmark, or ramp "just to see." Observed headroom is not permission.
9. **api.data.gov-keyed hosts (govinfo, congress) have a documented 1,000 req/hr quota — that is the binding limit, BELOW the 2 req/s ceiling.** 2 req/s exhausts the hour in ~8 minutes then 429s into the breaker. Throttle to **≤0.25 req/s** for these hosts, track cumulative key spend, stop cleanly at 900.

## [GLOBAL: THE DATA FACTORY PROTOCOL]

- **Lifecycle stages 2–3.** Stage-1 verification is **partial by design** — the blocker in §2.1 is verified; the working route is selected in-run per §2.2–§2.4.
- **Pattern A (direct hydration).**
- **Raw stays lossless:** land the structured tables verbatim; land the source PDF/XML bytes to R2 for anything not machine-parseable, so a later cycle can revisit without re-fetching.
- **Source ingest invariant:** bulk-statistical/reference → Lance SoR only.
- **F3 hook:** predecessor path verified at write time.

## [MISSION: CBO BUDGET SCORING R2 INGEST]

### 0. Why this matters (operator's words)

Every dollar figure in the demo that describes the *future* — the OBBA uplift above all — is currently an authored constant. The sibling directives close the "what was made available / released / spent" questions with real feeds, but none of them can answer "what did Congress decide this bill would cost." That answer exists, publicly, in CBO's scoring. Landing it converts the program's single largest assumption into a citation.

### 1. Objective

Land CBO's two machine-readable product families under `s3://data-sink/active/`: (a) the **budget & economic baseline projections** (spreadsheets — revenues, outlays, deficits, by category, ten-year windows, published ~2×/year), and (b) the **cost-estimate corpus** (per-bill scoring). Volume: baselines ~50–300K rows across workbooks; cost-estimate index ~1–3K rows/Congress. **Scope honesty:** (a) is fully structured and is the primary deliverable; (b) is a document corpus whose *metadata* is structured but whose *numbers* are in PDF tables — see §2.5.

### 2. Source-specific facts the executor MUST internalize

1. **VERIFIED BLOCKER — `cbo.gov` 403s automated clients.** Tested 2026-07-27, three URLs, two header profiles:
   - `https://www.cbo.gov/data/budget-economic-data` → **403** (bare UA), **403** (full browser UA + `Accept` + `Accept-Language`)
   - `https://www.cbo.gov/cost-estimates` → **403**
   - `https://www.cbo.gov/system/files/2025-01/51118-2025-01-Budget-Projections.xlsx` → **403**
   Response bodies are ~770 B edge-block pages, not CBO content. **Do not retry with header variations.** Treat `cbo.gov` as unreachable and proceed to §2.2.
2. **ROUTE 1 (try first) — GovInfo API.** `api.govinfo.gov` is GPO's official API and carries CBO cost estimates as a collection. It requires an `api.data.gov` key (free, instant self-service). **First check Doppler** (`doppler secrets -p core-x -c prd | grep -iE 'govinfo|data_gov'`) — if a key exists, use it. If not, mint one and store it in Doppler as `GOVINFO_API_KEY` (do not hardcode, do not commit). **`api.data.gov` keys carry a documented default limit of 1,000 requests/hour** — budget the sweep against it, track consumption, and stop cleanly before exhausting it rather than discovering the ceiling by hitting it. Discovery steps, in order: `GET /collections` (confirm a CBO-bearing collection and its exact name), then `GET /collections/{name}/{startDate}` to page package IDs, then `GET /packages/{packageId}/summary` and `/granules` for structure. **Record the observed collection name, package count, and one full summary payload in the run record** — this directive does not pre-verify them because the key was not available at authoring time.
3. **ROUTE 2 (fallback) — operator landing drop.** This program has an established precedent for hard-blocked hosts: the operator hand-drops the publisher's files into `s3://data-sink/landing/<source>/` and the pipeline reads from there instead of the upstream (see the predecessor directive's `s3://data-sink/landing/bls/productivity/` case, where 27 files / ~168 MB were dropped after bls.gov 403'd). **Check `s3://data-sink/landing/cbo/` FIRST, before any network call** — if the operator has dropped files there, that prefix is the source of record for this run and §2.1/§2.2 are moot for whatever it covers. If Route 1 fails and the prefix is empty, **stop and surface to the operator with a precise list of the files needed** (name the exact CBO products and their page locations); do not improvise a third route.
4. **ROUTE 3 (metadata only, if useful) — Congress.gov API.** `api.congress.gov` (same `api.data.gov` key) exposes bill records that reference CBO cost estimates. Useful for the bill↔estimate crosswalk (bill number, title, sponsor, enacted date, public law number — **including P.L. 119-21**), but the estimate documents themselves link back to `cbo.gov` and will 403. Land the crosswalk if Route 1 or 2 produced estimate documents to join it to; skip it otherwise.
5. **SCOPE LINE — structured vs prose.** Two different things wear the name "CBO data":
   - **In scope:** the baseline/projections **spreadsheets** (`.xlsx`) — revenues, outlays, deficits, debt, by function/category, actual + ten-year projection. Machine-readable, melt to long form exactly like the OMB workbooks in the sibling directive.
   - **In scope:** cost-estimate **metadata** — bill id, title, congress, committee, publication date, document URL, public-law number where enacted.
   - **OUT of scope this cycle:** extracting dollar figures out of cost-estimate **PDF prose tables**. Land the PDF bytes to `s3://data-sink/active/cbo_cost_estimate_docs/` (or the landing prefix) and stop. Table extraction is its own cycle with its own accuracy gates. **Do not use an LLM to read numbers out of PDFs and land them as facts** — a hallucinated budget figure is worse than a missing one.
6. **The OBBA question, stated precisely.** P.L. 119-21 is the target. The chain that would produce a sourced figure is: Congress.gov → the bill record for P.L. 119-21 → its CBO cost estimate document → the scored ten-year total by title. **Steps 1–2 are reachable; step 3 depends on Route 1 or 2 succeeding.** If the chain completes, record the scored figure, its exact source URL, its window (e.g. FY2025–2034), and whether it is net-of-revenue or outlay-only — **the `$785B` constant in the demo is unattributed, and the point of this directive is to attribute or correct it.** If the chain does not complete, say so plainly in the run record and change nothing about the constant.

### 3. Data Extraction

One module: `pipelines/reference/cbo_budget_scoring_ingest.py`, `--stream baselines|estimates_meta|estimates_docs|bill_crosswalk|all` + `--smoke`. Route selection (§2.2–§2.4) happens at the top of the run and is **logged to the ledger** (`route_used` column) — a future reader must be able to tell which door the data came through. xlsx via openpyxl; API via requests with backoff; PDFs streamed to R2 without parsing.

### 4. Required output streams

| # | Lance dataset | Grain (1 row =) | est. rows | BTREE keys |
|---|---|---|---:|---|
| 1 | `active/cbo_baseline_projections/` | measure × category × year × vintage | ~50–300K | `vintage`, `category`, `year` |
| 2 | `active/cbo_cost_estimates_meta/` | cost-estimate document | ~1–3K per Congress | `congress`, `bill_id`, `published_date` |
| 3 | `active/cbo_bill_crosswalk/` | bill (incl. public-law number) | ~10–20K per Congress | `congress`, `bill_id`, `public_law` |
| 4 | R2 raw prefix `active/cbo_cost_estimate_docs/` | source PDF/XML bytes | — | (object store, not Lance) |

**Column specs:**

- **1:** `vintage STR` (publication, e.g. `2026-01`), `product STR`, `sheet STR`, `category STR`, `subcategory STR NULLABLE`, `measure STR`, `year I32`, `is_projection BOOL`, `value_busd F64`, `units STR` (verbatim from the workbook — **do not assume $B; CBO mixes $B, % of GDP, and counts**), `source STR`, `ingested_at TS`.
- **2:** `estimate_id STR`, `congress I32`, `bill_id STR`, `bill_title STR`, `committee STR NULLABLE`, `published_date DATE`, `document_url STR`, `r2_key STR NULLABLE`, `public_law STR NULLABLE`, `source`, `ingested_at`.
- **3:** `congress I32`, `bill_id STR`, `bill_type STR`, `bill_number I32`, `title STR`, `sponsor STR NULLABLE`, `introduced_date DATE`, `enacted_date DATE NULLABLE`, `public_law STR NULLABLE`, `cbo_estimate_urls STR` (**pipe-joined** — Lance 1.5.x rejects `LIST<VARCHAR>`, canonical L54; downstream splits on `|`), `source`, `ingested_at`.
- **`is_projection` is the same discipline as `is_estimate` in the sibling directive:** CBO baselines put actuals and ten-year projections in adjacent columns. Flag every row. A projection landed as an actual is the single most damaging error this directive can make.

### 5. R2 Layout

`s3://data-sink/active/<dataset>/` for Lance; `s3://data-sink/active/cbo_cost_estimate_docs/{congress}/{bill_id}/{filename}` for raw documents. Full deterministic rebuild for Lance; raw docs are content-addressed and skipped if present.

### 6. Migration / audit ledger

`ops.cbo_budget_scoring_ingest_runs` in HQX: `run_id`, `stream`, **`route_used`** (`govinfo` | `landing` | `congress_api`), `resolved_urls` (jsonb), `rows_written`, `docs_landed`, `datasets` (jsonb), `started_at`, `finished_at`, `status`, `disposition`, `notes`. **`status` obeys canonical L4 — CHECK `IN ('running','completed','failed')`.** Throttle/block/partial ride a free-text `disposition` column, never `status`. Applied `IF NOT EXISTS` (L3).

### 7. Downstream wiring — DEFERRED

Nothing downstream. Sidecar promotion and demo-bake wiring are separate cycles.

### 8. Validation Gate

Fail-closed, **with one deliberate soft exit**:

- **Route gate — two terminal non-ok states, both open no PR:**
  - **`disposition='blocked'`** (`status='failed'`): landing prefix empty AND GovInfo yields no CBO collection. Write the ledger row + a run record naming the exact files needed; open no PR. A success condition, not an executor failure — the honest outcome when a publisher blocks automation. Do not fabricate a workaround.
  - **`disposition='partial'`** (`status='failed'`): a route opened and ≥1 dataset landed, but the sweep did not finish (rate/key budget, timeout). Write what landed, record the exact stopping point + remaining work, open **no** auto-merge PR, surface to the operator. A partial run is never marked complete or PR'd as whole.
- Baselines: `units` must be non-null on 100% of rows → fail (CBO mixes units; an unlabeled number is unusable). `is_projection=true` count must be `> 0` and contiguous at the tail of each series. Year range must span ≥ 10 years.
- Cost-estimate metadata: every row must carry a resolvable `document_url` → fail on nulls.
- Bill crosswalk: **P.L. 119-21 must be present only when the sweep ran to completion** (`disposition='ok'`); under `disposition='partial'` its absence is recorded, not fatal (a truncated sweep that stopped before that record is not a data error). Completed sweep + still absent → fail.
- No dollar figure may be written to any dataset from a PDF (§2.5) → the module must contain no PDF text-extraction path at all.
- `--smoke` passes end-to-end before the full run.

### Evidence

Captured 2026-07-27 UTC.

```
=== cbo.gov — VERIFIED BLOCKED ===
UA = bare curl default:
  GET https://www.cbo.gov/data/budget-economic-data   -> HTTP 403  ct=text/html  770 B
  GET https://www.cbo.gov/cost-estimates              -> HTTP 403  ct=text/html  767 B
  GET https://www.cbo.gov/system/files/2025-01/51118-2025-01-Budget-Projections.xlsx
                                                      -> HTTP 403  ct=text/html  770 B
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)
      Chrome/126.0.0.0 Safari/537.36"
  + Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
  + Accept-Language: en-US,en;q=0.9
  GET https://www.cbo.gov/data/budget-economic-data   -> HTTP 403
CONCLUSION: edge-level bot block, not a UA check. Same class as the bls.gov Akamai block
recorded in the predecessor program. Do not spend cycle time here.

=== US-CBO GitHub org — checked, NOT the route ===
GET https://api.github.com/orgs/US-CBO/repos?per_page=100 -> 200, 32 repos
 means_tested_transfer_imputations | captax | debtwelfare | eval-projections
 electric_vehicle_model | premium-growth-model | financial_regulation_model | EmpElastR
 conditional_forecasting_with_bvar | conventional-tariff-analysis-model
 ma_encounter_data_cleaning | ma_public_data_cleaning | …
These are analysis MODELS (research code), not the budget projections or cost-estimate
corpus. Useful reading; not a data source for this directive.

=== NOT pre-verified (no api.data.gov key at authoring time) ===
api.govinfo.gov  — collection name, CBO package counts, payload shape: §2.2 in-run discovery
api.congress.gov — bill/public-law crosswalk shape: §2.4 in-run discovery
s3://data-sink/landing/cbo/ — existence unchecked; §2.3 says check it FIRST
```

### Execution Command

```bash
cd /Users/benjamincrane/core-x
git checkout -b claude/cbo-budget-scoring-ingest

# 0. route selection happens inside the module; check the landing prefix first
doppler run -p core-x -c prd -- aws s3 ls s3://data-sink/landing/cbo/ --endpoint-url "$R2_ENDPOINT" || true

doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \
  --with openpyxl --with requests --with boto3 --with 'psycopg[binary]' \
  python -m pipelines.reference.cbo_budget_scoring_ingest --stream all --smoke

doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \
  --with openpyxl --with requests --with boto3 --with 'psycopg[binary]' \
  python -m pipelines.reference.cbo_budget_scoring_ingest --stream all
```

## Surfaces

| Atom | Path |
|---|---|
| Migration | `migrations/…_ops_cbo_budget_scoring_ingest_runs.sql` (IF NOT EXISTS) |
| Code | `pipelines/reference/cbo_budget_scoring_ingest.py` (new) |
| Lance datasets | `s3://data-sink/active/{cbo_baseline_projections,cbo_cost_estimates_meta,cbo_bill_crosswalk}/` |
| R2 raw | `s3://data-sink/active/cbo_cost_estimate_docs/` |
| Secret (if minted) | `GOVINFO_API_KEY` in Doppler `core-x/prd` — never committed |
| Ledger | `ops.cbo_budget_scoring_ingest_runs` (HQX) |
| Run record | `docs/reference/` note incl. route used + the OBBA chain outcome (§2.6) |

## Lessons learned (cite, don't re-explain)

- **L0** worktree path discipline · **L1** Doppler shell expansion · **L2** DDL `IF NOT EXISTS` · **L43/L44** verify structure before parsing (applied to the in-run GovInfo discovery) · **L45** R2 key hygiene.
- **Program precedent — hard-blocked publisher:** the bls.gov Akamai case in `2026-07-23-industry-cost-structure-batch-ingest.md` §2.8. The resolution there was an operator landing drop, not a bypass. Same resolution applies here (§2.3).

## Out of scope (don't do these)

- **Any attempt to bypass the cbo.gov block** — UA spoofing, proxies, headless browsers, scraping mirrors.
- **LLM extraction of numbers from cost-estimate PDFs** (§2.5). Land the bytes; stop.
- **Asserting or "correcting" the `$785B` demo constant in code.** Report the sourced figure and its window in the run record; the operator decides what the demo says.
- Sidecar promotion; demo-bake wiring; gc-hq-new TS artifacts.
- The siblings' modules and datasets.

## Iteration budget

Small if a route opens (a handful of workbooks + a paged metadata sweep); **zero-output-by-design if both routes are shut**, in which case the deliverable is the `status='blocked'` ledger row plus a precise file-request list for the operator. Single PR when data lands; no PR when blocked.

## Definition of done

**If a route opened:**
- [ ] Source(s) registered in `ops.data_source_catalog` (L60, `ON CONFLICT DO NOTHING`).
- [ ] Migration applied (`ops.cbo_budget_scoring_ingest_runs`, `IF NOT EXISTS`).
- [ ] `route_used` recorded in the ledger.
- [ ] §2.2 GovInfo discovery recorded (collection name, package counts, one full summary payload) — or §2.3 landing manifest recorded.
- [ ] `--smoke` passed end-to-end.
- [ ] Datasets landed; every §8 gate passed; `ds.count_rows()` recorded.
- [ ] BTREE indexes built.
- [ ] R2 listing verified for every prefix.
- [ ] **§2.6 OBBA chain outcome written to the run record**: the sourced figure + exact source URL + window + measure basis, OR an explicit statement that the chain did not complete.
- [ ] PR opened and self-merged per L39.
- [ ] `git -C /Users/benjamincrane/core-x pull` && `git log -1 --oneline`.
- [ ] Cycle report written.

**If both routes were shut:**
- [ ] Ledger row written with `status='blocked'` and `route_used='none'`.
- [ ] Run record names the exact CBO products needed and where they live on cbo.gov, formatted as a copy-paste request for the operator's landing drop.
- [ ] No PR opened. Surfaced to the operator.

## Execution log (executor fills in)

- [ ] Branch created
- [ ] `s3://data-sink/landing/cbo/` checked (contents recorded)
- [ ] GovInfo route attempted (key source, collection found?)
- [ ] Route selected
- [ ] Module written
- [ ] Migration applied
- [ ] Smoke passed
- [ ] baselines landed
- [ ] estimates_meta landed
- [ ] estimates_docs landed to R2
- [ ] bill_crosswalk landed
- [ ] Gates passed
- [ ] PR merged (or blocked-surface written)
- [ ] Operator checkout pulled + verified

## Final result (executor fills in)

- Route used:
- Landing prefix contents (if any):
- GovInfo collection + package counts:
- Per-dataset row counts:
- Documents landed to R2 (count, bytes):
- **P.L. 119-21 / OBBA: figure, source URL, window, measure basis — or "chain did not complete":**
- Wall-clock:
- PR:
- Cycle report path:
