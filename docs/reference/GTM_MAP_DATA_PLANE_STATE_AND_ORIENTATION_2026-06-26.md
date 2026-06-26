# GTM Map Data-Plane — State & Orientation (2026-06-26)

**Type:** State + understanding (canonical handoff). **Not** a roadmap. This documents what EXISTS, how it works, what changed this session, and what is known-open — so a new agent gains full footing to judge any later "next steps" proposal. Where this distinguishes *verified* (read in-repo / probed live) from *inferred*, it says so.

**Anchor checkouts:**
- core-x data plane (this session's worktree): `/Users/benjamincrane/core-x/.claude/worktrees/nice-jackson-ac676d`
- core-x operator `main` checkout: locate with `git worktree list | grep '\[main\]'` (currently `/Users/benjamincrane/core-x/.claude/worktrees/objective-keller-e0e255`)
- app consumer: `/Users/benjamincrane/rare-structure-hq`

---

## 1. Platform overview

A GTM prospecting platform over US federal contracting. Two repos, one upstream→downstream seam:

- **core-x** — the decoupled data/compute plane. Architecture: Modal extraction (or ad-hoc in-session scripts) → DuckDB compute over ephemeral Parquet → **LanceDB written directly to R2** as the system of record, addressed by R2 URI under `s3://data-sink/active/`. No catalog layer; datasets are addressed by URI. Every load-bearing resolution key carries a hard `BTREE` scalar index.
- **rare-structure-hq** — the consuming app. The "Query the Market" `/ask` map. It treats core-x as upstream via the catalyst/edge gateways.

**The `/ask` feature (natural language → map):** a sentence is routed to one of four map serving datasets, TRANSLATE-d (by the LLM) into a compiled filter object, EXECUTE-d deterministically over Lance, and returned as GeoJSON dots + headline metrics + aggregate tables. The translation contract is rendered from the **edge_api** decoder's prompt-facing subset; the execution contract is enforced by the **catalyst_api** decoder (authoritative). See §3.

**The two halves of the intelligence.** Financial/temporal axes live on the awards/active datasets; capability/scope axes live on the winners dataset. These are separate grains/datasets by design — see §6. This is architectural fact, not a gap to close.

---

## 2. The four map serving datasets + their grains

All four are Lance datasets under `s3://data-sink/active/`, registered in `apps/catalyst_api/src/config.py` `MAP_DATASET_URIS` (keys `winners`/`company`/`awards`/`active`). Each has exactly one decoder (`apps/catalyst_api/src/map_decoders.py`) and one materializer (`pipelines/serving/materialize_{...}_map.py`).

| Key | Dataset URI (under `active/`) | Grain | Direction | Measure column | Decoder version |
|---|---|---|---|---|---|
| `awards` | `usaspending_awards_map_serving/` | 1 row / positive-$ **PRIME award ACTION** | backward ("won $X in last N days") | `action_obligated_usd` | `awards.v10` |
| `winners` | `usaspending_winners_map_serving/` | 1 row / **winner_uei × winner_type** (entity rollup) | backward (window SUM snapshot) | `entity_obligated_usd` | `winners.v10` |
| `company` | `firmographics_company_map_serving/` | 1 row / **UEI** (firmographic rollup) | n/a (EPG-sourced) | `entity_active_obligated_usd` | `company.v5` |
| `active` | `govcon_active_awards_map_serving/` | 1 row / **active prime award (PIID/award)** | **forward** (recompete radar) | `contract_current_value_usd` (+ `_potential_value_usd`, `_obligated_usd`) | `active.v3` |

**awards** (`materialize_awards_map.py`) — the award-EVENT read model. **PRIME-ONLY** as of this session (subaward fan-out dropped at build; teaming intelligence lives on winners instead). De-obligations (<0) and $0 admin mods are excluded at build, so `>= X` is honest "won" semantics; each action stands alone (no multi-action aggregation). Carries recipient geo (`state`/`city`/`county`) vs place-of-performance geo (`pop_state`/`pop_city`) as distinct axes, plus NAICS, PSC, agency, set-aside, business_size, action_type/is_option_exercise, fiscal_year, and the GTM label axes (vertical/work_type/equipment_intensity + `what_was_done` display gloss).

**winners** (`materialize_winners_map.py`) — entity rollup over the build window. `entity_obligated_usd` = per-entity Σ positive obligations within the window (a snapshot — **not** re-windowable; see §5). Carries the PHASE-3 capability axes (clearance/CMMC/solicitation scope tags/labor categories), the SUB-only teaming axes (teaming_dollars_5y, n_teaming_primes, teaming_prime_names), and the SUB-only self-reported axes (subaward_description_tags, req_cert_tags).

**company** (`materialize_company_map.py`) — the prospecting-map firmographic rollup, 1 row/UEI reachable from a `firmographics_blitz` domain via `sam_master_domains`. Money `entity_active_obligated_usd` is **sourced from `entity_profile_gold` (EPG active obligations) — NOT a windowed sum**. The fresh feeds supply only the recency date (`latest_award_action_date`), not the money.

**active** (`materialize_active_awards_map.py`) — the FORWARD-looking recompete radar. Projected from `govcon_active_awards` (already collapsed to latest txn/award + active membership). The forward axis is `days_until_expiry` (a `days_ahead` decoder type over `pop_current_end`). Money is the contract value family (`contract_current_value_usd` / `contract_potential_value_usd` = ceilings) vs `contract_obligated_usd` (funded-to-date) — distinct concepts, never collapsed. `award_count` does not exist on this table (contract grain).

---

## 3. The decoder / EXECUTE / wire contract (the load-bearing invariants)

### Two decoders in parity
- `apps/catalyst_api/src/map_decoders.py` — **authoritative** (drives EXECUTE).
- `apps/edge_api/src/map_decoders.py` — **mirror** (drives TRANSLATE prompt rendering). A parity test asserts the two enum value-sets match.

Versions are currently aligned across both: `winners.v10`, `company.v5`, `awards.v10`, `active.v3` (edge `ROUTER_VERSION = router.v8`). The `version` string is the cache-busting key for edge_api's translation memo — **bump it on any field/enum/synonym/prompt-copy change**.

### The dataclasses (`map_decoders.py`)
- `FieldSpec(column, type, ops, enum, index, gated)` — query-name (dict key) → physical `column`. `type ∈ {string,int,float,bool,days_ago,days_ahead,list}`. `gated=True` marks a PHASE-3 capability axis.
- `AggregateSpec(measure, dims, metrics, winner_key, size_band_edges, …)` — the GROUP-BY allowlist; `measure` is the numeric column the metrics aggregate; `dims` maps group-by query-name → physical column.
- `Decoder(dataset_key, version, geometry, properties, fields, synonyms, aggregate)`.

### The MOVE-TOGETHER invariant (or boot 503)
A serving column and its dependents must move **atomically**:
1. **Serving column** — the materializer `SELECT … AS <col>` alias **and** its `BTREE_INDEXES`/`BITMAP_INDEXES` entry. Lance derives the scalar-index name as `{column}_idx` (every `create_scalar_index` is called with no explicit `name=`), so rebuild-with-overwrite renames the index for free — **there is no separate index-rename step**.
2. **Decoder** — `FieldSpec.column` (1st arg) **and** `AggregateSpec.measure` in **both** catalyst + edge, plus catalyst `decoder.properties`.
3. **Contract fixtures** — `apps/catalyst_api/tests/test_contract_check.py` `*_COLS` + `*_IDX` lists.

If any one lags, the boot-time contract check raises a HARD violation and flips `/healthz` to 503. (Details below.)

### The boot contract check (`lance_store.py`)
- `verify_decoder_contract(schema_field_names, indices, decoder)` — PURE (no R2). HARD violations: a geometry/property/`FieldSpec.column` missing from the live schema, OR a FieldSpec declaring an index whose physical column is not covered by any live `list_indices()` entry (declared-⊆-actual; live legitimately carries MORE indexes than declared — never asserts the reverse). SOFT notes (non-fatal): a column indexed with a different-but-valid scalar kind than declared.
- `_live_stat_findings(...)` — a 0-row dataset is a HARD violation (a schema-perfect-but-empty serving table is the #431 failure mode); unindexed-since-build rows are SOFT notes.
- `check_decoder_contracts()` — LIVE caller: opens each dataset against R2, runs the pure checker, adds row-count + per-index unindexed-row checks. Runs in the catalyst_api lifespan at boot. Returns `{dataset_key: {violations, notes}}`; empty `violations` = contract holds.
- Fixtures: `apps/catalyst_api/tests/test_contract_check.py` exercises the pure checkers with synthetic inputs in the real `list_indices()` shape (mixed-case `BTree`/`Bitmap` types; extra undeclared resolution-key indexes asserted not to violate).

### EXECUTE is decoder-driven (`lance_store.py`)
- `compile_map_filter(decoder, filters, today)` — builds an AND-combined Lance scanner predicate. **Column names come ONLY from `FieldSpec.column`** (a dict-key lookup, never interpolated from the caller); values are type-validated and `_sql_str`-escaped (quotes doubled). Off-allowlist field/op or mistyped value → `MapCompileError` → 422. `today` resolves `days_ago`/`days_ahead` at REQUEST time (memo-safe). **Scope-coverage safety gate:** if any `gated` clause is present, `has_extracted_scope = true` is ANDed in deterministically here (never left to the TRANSLATE prompt) — one LLM omission cannot leak ~1.25M winners through the ~0.96%-extracted-scope coverage ceiling to an empty map.
- `to_geojson(decoder, rows)` — emits `decoder.properties` verbatim as feature properties; a row missing a coordinate is emitted with `geometry: null` (RFC 7946 §3.2) so the qualifying-but-ungeocoded row still reaches the TABLE view (dot layer skips it).
- `map_query` streams batches and cuts at `limit` (NOT `scanner(limit=)` — pylance 7's limit-before-filter planner under-returns matches on a selective predicate). `map_count` uses `count_rows` pushdown. `map_aggregate` runs the SAME compiled predicate then a pyarrow hash-aggregate (no SQL engine in EXECUTE); group/measure columns come only from `AggregateSpec`.

### The WIRE is verbatim pass-through — and SILENTLY breaks (caused a live bug this session)
The chain: catalyst GeoJSON → edge `/ask` → BFF `apps/platform-api/src/lib/edge.ts:137` spreads `...(f.properties ?? {})` (adds only lat/lon) → app `AskMarketRow = Record<string, unknown>` (`apps/platform-app/src/demo/federalApi.ts:152`). **The wire JSON key == the serving column name.** The app reads columns by string literal off an untyped `Record`, and `askRowNum(missing) → 0` with **no throw and no TS error**. Therefore a serving-column rename that does not move the app's reads in lockstep surfaces as silent `$0` / `hasFed=false` — the failure mode behind the `active` `$0` bug this session. The mandated mitigation is a **visual money check** after every rename (table "FEDERAL $" column + map "FEDERAL AWARDS" headline must show real dollars). TypeScript cannot catch this.

---

## 4. The money SEMANTIC MODEL (obligation vs value; FPDS)

Federal contracting has two distinct money concepts the old schema conflated under bespoke names:

- **Obligation** = money legally committed/funded (FPDS `federal_action_obligation` per-action; cumulative `total_dollars_obligated`). Funded-to-date.
- **Value / ceiling** = the contract's negotiated total: `current_total_value_of_award` (base + exercised options) vs `potential_total_value_of_award` (base + all options). **Value ≠ obligation.** Exercising an option **mints an action** (an obligation event) but does not fully obligate the ceiling.

Grains stack: **action** (transaction) → **award / contract (PIID)**. A multi-action award's obligation is the sum across its actions; the awards map deliberately does NOT aggregate them (each action row stands alone).

**The grain-prefixed naming scheme adopted this session** (concept-first, grain-second, `_usd` unit):
- obligation → `{grain}_obligated_usd` — `action_obligated_usd` / `entity_obligated_usd` / `entity_active_obligated_usd` / `contract_obligated_usd`.
- value/ceiling → `contract_{current|potential}_value_usd`.
- **Never** suffix an obligation column with `_value` (reserved for ceilings). Counts stay `award_count` (no grain prefix).

**Decoder query-names (LLM-facing dict keys) were intentionally left UNCHANGED** — the decoder already supports query-name ≠ physical-column (e.g. company's `active_obligations` → `entity_active_obligated_usd`). Only the physical column, `AggregateSpec.measure`, `decoder.properties`, the serving alias/index, the contract fixtures, and the app reads moved. So the LLM prompt + synonyms are untouched. (`award_amount`/`total_obligation`/`current_value` etc. survive as query-name keys mapping to the new physical columns.)

Source raw columns (`federal_action_obligation`, `total_dollars_obligated`, `current_total_value_of_award`, `potential_total_value_of_award`, `subaward_amount`) are upstream and were **never** renamed.

---

## 5. WINDOW-AS-DATA

The time window is **QUERY-driven**: the decoder `days_ago` (backward, `awards`/`winners`/`company`) and `days_ahead` (forward, `active`) axes resolve against `date.today()` at EXECUTE time, compiling to a `DATE 'YYYY-MM-DD'` literal over the underlying date column. Resolving at EXECUTE (not TRANSLATE) keeps the memo safe: a cached "this week" sentence re-resolves to the current week on every execution. The window is carried as a DATA filter, never baked into a table/column name (the operator's 2026-06-14 WINDOW-AS-DATA decision).

**Build windows per dataset (verified in the materializers):**

| Map | Build window | Source / note |
|---|---|---|
| `awards` | **730d** (`AWARDS_WINDOW_DAYS`, by action_date) | `usaspending_api_fresh` (rolling freshness feed). 730d captures ~97% of currently-active contracts. Already 730 before this session. |
| `winners` | **730d** (`WINNERS_WINDOW_DAYS`) | same fresh feed. **Widened 90→730 this session** (PR #741) for parity with awards. |
| `company` | **none / EPG-sourced** | money comes from `entity_profile_gold`, not windowed; only the recency date is feed-sourced. No money build-window. |
| `active` | **full membership** (no date cutoff) | `govcon_active_awards` membership; genuinely query-driven. |

**Windowed per-entity dollar totals are computed at query time via the awards aggregate** (`group_by: winner` + a `days_since_action` filter), NOT re-derived from a pre-summed rollup. The winners `entity_obligated_usd` is a SUM over the *build* window — a snapshot, not re-windowable to a narrower query window. Widening winners 90→730 therefore changed its dollar numbers (larger per-entity totals, more entities), which is the intended effect; awards is action-grain so widening only adds rows.

**OPEN (do NOT resolve):** the precise temporal behavior of the upstream SAM/usaspending "fresh feed" is not fully nailed down — whether `usaspending_api_fresh` is a fixed rolling pull (~45-day modified window per the reconciliation directive) or an accumulating sink. The "90-day" prose still present in some SAM/usaspending docstrings was left in place pending that determination. The map serving docstrings/comments themselves were rewritten to map reality (PR #742); the residual "90-day" prose is on the upstream feed/extraction docs, not the serving layer.

---

## 6. The capability / scope intelligence half

A separate SAM/govcon attachment pipeline produces the capability intelligence: **manifest → byte download → text/structured extraction → scope vectors / labor demand / award-capability profiles**, feeding the deployed **gtm-mcp** semantic-search gateway (Render web service; runtime `DATASET = "govcon_scope_vectors"`, already de-suffixed in #542, resolved by a self-refreshing name→URI registry on a ~30-min TTL).

**Where the axes physically live (architectural FACT, not a to-do):**
- **Capability / scope axes** (clearance, CMMC, solicitation_scope_tags, labor_categories) + the SUB teaming axes (teaming_dollars_5y, n_teaming_primes, teaming_prime_names) + the SUB self-reported axes (subaward_description_tags, req_cert_tags) → live on the **winners** serving table.
- **Financial / temporal axes** → live on **awards** / **active**.
- They are separate grains/datasets. A query that needs both must hit both datasets.

**Coverage reality (the "honest, not silently filtered" posture):**
- **Vertical / GTM labels** (vertical/work_type/equipment_intensity, materialized onto awards + active from the top-279 (naics_code, psc_code) head): head-coverage only — ~80% of both-codes $ but ~35% of rows on awards; ~78% of recompete $ but ~38% of rows on active. Unlabeled rows surface in "not applied," **never silently filtered**. 23 of 24 verticals present in the head ("Staffing & Human Capital" is in-taxonomy at 0 labeled rows).
- **Capability/scope** (gated): ~0.96% of awards have extracted solicitation text (~4,220 scope-extracted winners); the gate (`has_extracted_scope = true`) is deterministically ANDed for any gated clause (§3).
- **Self-reported subs:** ~13,792 subs self-report capability (the ungated long-tail `subaward_description_tags` axis), vs the ~4,220 scope-extracted slice — gating the self-reported axis would defeat its long-tail purpose, so it is deliberately UNGATED.

---

## 7. What changed this session (executed cycles)

From `git log` + the PRs. Each is verified against the merged commit message + the live files.

### 7.1 active GTM axes (`active.v2`) — PRs #722, #726
Extended the vertical/work_type/equipment_intensity GTM label axes (+ `what_was_done` display gloss) from awards onto the forward recompete decoder, byte-identical to the awards lexicon. #726 corrected the coverage copy to dollar-weighted (~78% of recompete $), not row-weighted. (awards got these axes in `awards.v9` via PRs #715/#720; #713/#711 seeded the NAICS/PSC reference catalogs.)

### 7.2 Federal money-column rename (the big one) — core-x #731/#732/#733/#735 (+#736 cleanup); app #199–#203
Directive: `~/Desktop/hq/directives/2026-06-26-federal-money-column-rename.md`. Applied the obligation-vs-value model → grain-prefixed scheme across all four tables, one coordinated PR per table (expand/contract for the three working tables, single-shot for the already-`$0` active):
- `awards.v10`: `award_amount → action_obligated_usd` + **made awards prime-only** (dropped the subaward UNION branch). (#731)
- `winners.v9` (later v10): `total_obligation → entity_obligated_usd`. (#732)
- `company.v5`: `total_active_obligations → entity_active_obligated_usd` (output-alias only; EPG source untouched). (#733)
- `active.v3`: `current_value/potential_value/obligated → contract_{current_value,potential_value,obligated}_usd`. (#735)
- The live app `$0` bug (active had no money key in the guess-chain) was found and fixed; the app `data.ts` guess-chain now reads the four new keys (`apps/platform-app/src/demo/data.ts:1339-1343`), and `collapseAwardActions` write-back stamps `action_obligated_usd` (read-key == write-key). #736 untracked `query_outputs/` scratch + gitignored it.
- Mechanism that makes it safe: query-names unchanged (LLM prompt untouched); move-together invariant enforced; verified via the boot contract check + a visual money check. NO global find-replace (homonym collision: the same obligation/count names live on `entity_profile_gold`/`award_search`/`contractor_award_summary`/`entity_award_lines_gold`, consumed by the dossier/overview/active-contracts/past-performance surfaces + gtm_mcp — explicitly out of scope; see the directive's §6 do-not-touch list).

### 7.3 Window reconciliation — PR #741 (`winners.v10`)
Directive: `~/Desktop/hq/directives/2026-06-26-map-serving-window-reconciliation.md`. Widened winners build window **90→730** for parity with awards; rewrote winners coverage copy honestly. Findings recorded: company has no money build-window (EPG-sourced); awards was already 730. The edge guard `test_no_stale_90day_window_claim_in_any_prompt` forbids the literal "90 days" in any rendered prompt — coverage is phrased "~730 days"/"~24 months".

### 7.4 Map string accuracy — PR #742
Rewrote the serving-layer window/coverage docstrings/comments to map reality, affirmatively, no archaeology (state what IS, not the history of what was).

### 7.5 SAM `_90day` code-suffix drop — PR #747
Directive: `~/Desktop/hq/directives/2026-06-26-sam-90day-code-rename.md`; audit: `docs/plans/SAM_90DAY_CODE_SUFFIX_DROP_AUDIT.md`. Dropped `_90day` from the CODE/identifier layer (datasets + ledgers were already done in #542). **9 Python modules + 3 ops SQL files + identifiers renamed.** The non-obvious collision resolution: for `download`/`reconcile`, the `_90day` file was the CANONICAL/current worker and the suffix-free twin was SUPERSEDED legacy (the inverse of the intuitive read) — the legacy twins (zero importers) were archived, then the canonical `_90day` pair renamed into the freed names. Verified present post-rename: `sam_attachment_extract.py`, `sam_attachment_download.py`, `sam_attachment_reconcile.py`, `govcon_teaming_edges.py`. Residual `_90day` in `.py` is intentional: `govcon_gtm_schemas.py:24` + `build_award_capability_profiles.py:73` point at a dropped LEGACY shell URI (`govcon_award_capability_profiles_90day`), not active naming.

(Also this session, adjacent but outside the map data plane: Close.com webhook/crosswalk + SFNet contact GTM work — #740/#743/#744/#737/#734; edge_api agreement-doc/documenso work — #710/#717/#723–#729/#738. Listed for completeness; not part of the map serving plane.)

---

## 8. Conventions & operating guardrails a new agent must know

### Git lifecycle (operator owns the full loop)
Per unit of work (one table / one map = its own squash PR; **no stacked PRs** — squash drops later-added commits): branch off `main` → commit → push → open PR vs `main` → **self-merge** (`gh pr merge <n> --squash --delete-branch`) after self-verification → **pull into the operator's `main` checkout** (locate via `git worktree list | grep '\[main\]'`) → verify `git log -1 --oneline`. "Merged" ≠ done until the operator's checkout reflects it on disk. The app repo change is its own PR/merge in `rare-structure-hq`.

### Doppler + Lance rebuild recipe (run from a clean cwd — never `/tmp`; a stray `inspect.py` shadows stdlib)
```
cd /Users/benjamincrane/core-x/.claude/worktrees/nice-jackson-ac676d
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
  python3 pipelines/serving/<materializer>.py <init_ops|build|verify|demo> [window_days]
```

### Live boot contract check (must be 0 violations on all 4 decoders, or `/healthz` 503s)
```
doppler run -p core-x -c prd -- uv run python -c \
  "import json; from apps.catalyst_api.src.lance_store import check_decoder_contracts; \
   print(json.dumps({k:v['violations'] for k,v in check_decoder_contracts().items()}, indent=2))"
```

### Other guardrails
- **Query-name vs physical-column decoupling** — the decoder dict key (LLM-facing) is independent of `FieldSpec.column` (physical). Rename the column without touching the prompt by keeping the key.
- **Directive-vault predecessor gate** — directives in `~/Desktop/hq/directives/` carry a `Predecessor:` line; the predecessor-gate hook enforces ordering.
- **Probe scripts from a clean cwd** — a `/tmp/inspect.py` shadows the stdlib `inspect` module and breaks pylance/duckdb imports.
- **Warm-cache semantics** (`lance_store.py`) — dataset handles are warm-cached with stale-while-revalidate (TTL 300s, `CATALYST_DATASET_TTL_SECONDS`); a serving-table rebuild (atomic `mode="overwrite"` manifest commit) becomes visible within ~5 min. Map filter indices warm in the background at boot (never block boot).

---

## 9. Known issues / loose threads (FACTUAL, not prescriptive)

- **2 pre-existing test failures in `pipelines/sam_gov/tests/test_govcon_llm_lane.py`** — `test_validator_rejects_out_of_vocab_labor_category_and_bad_type_and_dup` and `test_artifacts_load_and_prompt_hash_is_stable_and_content_bound`. Both are prompt-template/vocabulary content-hash assertions (a reference-artifact `sha256` mismatch), independent of this session's renames; they fail on clean `main`. Verified by direct run this session (2 failed, 16 passed).
- **Deferred-and-flagged (per the audit + directives):**
  - **Live ops-index `ALTER … RENAME`** — the `ops.*_runs` index names still hard-code `_90day_runs_*` in DDL + prod (table names already de-suffixed in #542). Index names are not resolution keys; renaming live indexes is supervised DDL with near-zero payoff — deliberately left.
  - **FEED-ledger `feed`-column values** — 4 `FEED` strings still carry `_90day` (written to live `ops.*_runs` rows). Renaming the code FEED string would split `feed` history; the recommendation kept the files renamed but the FEED strings frozen (a FEED value is a historical key, not a name). Old run rows keep the old feed strings.
  - **`ops.sam_attachment_download_runs` missing both secondary indexes in prod** (pkey only) — a latent two-DDLs-one-table footgun flagged in the audit; the legacy schema survives as `ops.sam_attachment_download_runs_legacy_pre90day`.
- **Unresolved temporal-truth question (§5)** — the fresh-feed's rolling-vs-accumulating behavior; the residual "90-day" prose on upstream SAM/usaspending feed/extraction docstrings was left pending it.
- **Directives written this session** live in `~/Desktop/hq/directives/`: `2026-06-26-federal-money-column-rename.md`, `2026-06-26-map-serving-window-reconciliation.md`, `2026-06-26-sam-90day-code-rename.md`. Audit: `docs/plans/SAM_90DAY_CODE_SUFFIX_DROP_AUDIT.md`; data-layer precedent: `docs/plans/SAM_GOVCON_90DAY_RENAME_MIGRATION.md` (executed via #542).

---

## Appendix — fast file index

| Concern | Path |
|---|---|
| Authoritative decoders (EXECUTE) | `apps/catalyst_api/src/map_decoders.py` |
| Mirror decoders (TRANSLATE prompt) | `apps/edge_api/src/map_decoders.py` |
| Compiler + contract checker + GeoJSON | `apps/catalyst_api/src/lance_store.py` (`compile_map_filter`, `check_decoder_contracts`, `verify_decoder_contract`, `to_geojson`, `map_aggregate`) |
| Contract fixtures | `apps/catalyst_api/tests/test_contract_check.py` |
| Dataset URIs | `apps/catalyst_api/src/config.py` (`MAP_DATASET_URIS`) |
| Serving materializers | `pipelines/serving/materialize_{awards,winners,company,active_awards}_map.py` |
| App `/ask`→Company mapper, `collapseAwardActions` | `rare-structure-hq/apps/platform-app/src/demo/data.ts` |
| App row type (`AskMarketRow`) | `rare-structure-hq/apps/platform-app/src/demo/federalApi.ts:152` |
| BFF properties pass-through | `rare-structure-hq/apps/platform-api/src/lib/edge.ts:137` |
