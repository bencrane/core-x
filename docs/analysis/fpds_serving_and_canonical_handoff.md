# FPDS Serving + Canonical Reconciliation — Canonical Handoff

**Last updated:** 2026-06-28 (rev: canonical schema LOCKED to typed v2 SoR; existing consumers decoupled —
operator decision 2026-06-28) · **Source of truth for this doc:** the full session transcript
`f0785f80-e21e-4746-b707-cf55488c7990.jsonl` (~4 MB). All numbers, URIs, column names, PR numbers,
and commit hashes below are pulled from that transcript and cross-checked against the on-disk code.

---

## (a) TL;DR + Current State

This body of work fixes a **grain bug** in the platform-app `/ask` map ("which companies won contracts
> $X") and lays the groundwork to fix the **completeness bug** underneath it.

**What shipped and is live:**
1. A new **`contracts` serving table** at PIID grain (1 row/contract, money pre-summed) — fixes the
   grain bug where a `$X` threshold was applied per-action instead of per-contract. **Live in R2:
   1,075,214 rows.** (core-x #772, #773; rare-structure-hq #207, #208)
2. A **dataset toggle** in the consumer app (Auto / Companies / Contracts / Actions / Recompete),
   surfaced both in the ⌘K palette (while composing a query) and in the result banner.
3. The FPDS **data lineage was fully mapped**, revealing that the serving tables roll off a
   **recency-skewed** source (the `contract_prime_txn` API feed), undercounting the universe.
4. The **USAspending monthly Award Data Archive** (Full + Delta) was ingested into **two new Lance
   tables** — landing the third FPDS source and, critically, the **656 FPDS deletion records** that
   nothing else captures. (core-x #781)

**What is designed and LOCKED but NOT built (the next big thing):**
- **`usaspending_fpds_canonical_txn`** — a canonical reconciliation Lance that unions all three FPDS
  sources, deduped, with deletions tombstoned. Fully specified (see §e). **Schema is LOCKED to the
  fully-typed v2 SoR** (`Date32`/`double`/`bool`, one clean canonical vocabulary, sentinels nulled,
  PK-deduped, BULK's ~80 enrichment columns carried as native typed columns) — operator decision
  2026-06-28; the v1 all-VARCHAR conform is rejected (see §d.4). **Not started.**
- **`won_Nd` windowed-cumulative per-company table** (`won_90d / won_365d / won_730d / won_1825d`) —
  answers "a company whose *summed* awards over the last N days ≥ $X." Designed, **not built**; should
  be built off the canonical, not today's recency-skewed feed. It is greenfield (no legacy vocabulary)
  → it reads the typed canonical natively and is the canonical's first consumer.

**One-paragraph state:** The contracts grain-fix is shipped and live; the map can now correctly answer
"a single contract ≥ $X." All three raw FPDS sources are now landed as Lance SoRs. The next build is the
typed-v2 canonical (locked design) plus `won_Nd` off it. **Existing serving consumers are decoupled from
this build** — they keep reading their current sources and continue working unchanged; the typed canonical
does NOT trigger an immediate consumer re-point. Migrating the existing consumers onto the canonical is a
separate, deliberate FUTURE effort (and, because the canonical is typed, requires per-consumer code
changes — not a URI swap). The reconciliation is specified down to the proven join key and merge rule but
has not been built. **Resume at §e.**

---

## Domain Primer (read this if you know nothing)

**FPDS** = Federal Procurement Data System — the US government's record of every federal contract
*action*. Surfaced to the public via **USAspending.gov**.

**The grain ladder** (the same contract is measurable at four rungs; the dollars differ by orders of
magnitude):

| rung | key | what it is | serving table |
|---|---|---|---|
| **action** (transaction) | `transaction_unique_id` | one modification / option exercise | `awards` (`action_obligated_usd`) |
| **award** | **PIID** / `contract_award_unique_key` | one task/delivery/definitive contract = Σ its actions | **`contracts`** (NEW this session) |
| parent IDV | parent PIID | umbrella vehicle (GWAC/IDIQ) = Σ child awards | none |
| **entity** | UEI (`recipient_uei`) | everything a company won = Σ its awards | `winners` (windowed), `company` (active) |

- **PIID** = Procurement Instrument Identifier — the contract number. The award-grain key. In the data
  it appears as `contract_award_unique_key` (a `CONT_AWD_…`/`CONT_IDV_…` composite of
  subagency+PIID+parentPIID).
- **action vs award vs entity grain:** an action is one mod; an award (PIID) is the sum of its actions;
  an entity (UEI) is the sum of all its awards. **The original bug: a `$X` threshold bound to an action
  column, so a $20M contract paid out as 10× $2M actions was invisible** (no single action cleared the
  bar). Fix = pre-roll the sum to award grain, then filter.
- **UEI** = Unique Entity Identifier — the company key (replaced DUNS).
- **`federal_action_obligation`** = the dollar amount obligated by one action (can be negative —
  de-obligations). NET sum over a contract = its obligated value.
- **`base_and_all_options_value`** = the contract ceiling (max potential value incl. options).
- **`correction_delete_ind`** = a column in the monthly **Delta** archive marking each row as
  **C** (corrected/modified), **D** (deleted), or blank (added). **D rows are the only signal that
  FPDS removed a record** — no snapshot source can express a deletion (a deleted record is simply absent).

### The three FPDS sources

| | **pg-dump bulk** | **API fresh feed** | **monthly archive (Full + Delta)** |
|---|---|---|---|
| Lance table | `usaspending/transaction_search_fpds` | `usaspending_api_fresh/contract_prime_txn` | `usaspending_archive_full_fpds` / `usaspending_archive_delta_fpds` |
| rows | **107,250,527** | **1,986,682** (after the 06-26 advance) | Full **2,975,677** / Delta **3,060,070** |
| columns | 378, **typed** (`Date32`/`double`/`bool`, `rpt.*` schema) | 297, **all-VARCHAR** (`bulk_download/awards` verbatim names) | 300 / 302, all-VARCHAR (Award-Data-Archive names) |
| `action_date` span | 1962-05-01 → **2026-04-23** | 1993-11-15 → **2026-06-26** | Full FY2026 (2025-10-01→2026-06-04) / Delta 1978→2026-06-04 |
| source artifact | `usaspending-db_20260506.zip` (161 GiB pg_dump dir archive, dump_id 5968, snapshot 2026-05-06) | bulk_download/awards **API** → async CSV (zipped), `all_varchar=true` | operator-dropped monthly zips in R2 landing |
| ingest script | `pipelines/usaspending/usaspending_bulk.py` (Modal) | `pipelines/usaspending/usaspending_api_fresh.py` (Modal) | `pipelines/usaspending/usaspending_archive.py` (LOCAL CLI, NEW) |
| nature | **deep history**, frozen snapshot, ~2 mo behind | **recent tail + revisions**, recency-skewed | FY completeness bridge + **deletion ledger** |
| typing mechanism | pg_dump ships schema (`toc.dat`) → `TRY_CAST` per pg type | API CSV has no schema → lands as-is VARCHAR | verbatim VARCHAR |

**Why "recency-skewed" matters:** the API feed pulls by `last_modified_date`, so it only contains
recently-touched actions. Its year histogram (probed live): 2021 ≈ 24K rows, 2022 ≈ 39K, 2023 ≈ 68K,
2025 ≈ 246K, **2026 ≈ 1.16M** — older years are *under-captured*, not smaller. **The `contracts` and
`awards` serving tables roll off this feed**, so they under-represent the true universe. The pg-dump
holds the real history (107M rows) but stops at 2026-04-23.

**Neither source is complete alone:** bulk = deep history but ~2 mo stale; fresh = current but
historically thin. The correct base is their **union** (+ archive), deduped — that is the canonical.

---

## (b) Architecture — the `/ask` map pipeline + the 3 sources

### The /ask map pipeline (two repos)

```
core-x (data + compute plane)                       rare-structure-hq (consumer app)
─────────────────────────────                       ───────────────────────────────
raw FPDS sources                                     platform-app (UI, ⌘K palette, map)
   │  (DuckDB rollup → Lance)                            │  user picks Dataset toggle + types NL
   ▼                                                     ▼
serving Lance tables in R2                           platform-api  /ask?dataset=<table>
  usaspending_contracts_map_serving  ◄────┐              │
  …awards / winners / company / active    │              ▼
   │                                       └──────  catalyst_api + edge_api
   ▼                                                  - LLM compiles NL → filters (forced-tool)
catalyst_api / edge_api decoders                      - decoder binds $X → the table's money column
  contracts.v1 binds obligated/ceiling                - returns map pins (1 pin / row)
  to the SUMMED columns
```

- **catalyst_api boot contract:** every column a decoder declares **must physically exist** on the
  serving table, or catalyst hard-fails at boot under `CATALYST_CONTRACT_STRICT`. (This is why the
  table must be materialized *before* the services redeploy.)
- **The toggle bypasses the auto-router:** when the user pins a dataset, the compiler skips
  sentence→grain inference and binds the `$X` predicate directly onto the chosen table's money column
  (`contracts → contract_obligated_usd`, entity → `entity_obligated_usd`, actions →
  `action_obligated_usd`). This removed the least-reliable part of the original plan.
- **Suppression mechanism (confirmed):** `awards` binds `award_amount → action_obligated_usd` at
  `catalyst map_decoders.py:406` — action grain. That is the bug the contracts table fixes.

---

## (c) What EXISTS now

### Live serving table — `contracts`

| field | value |
|---|---|
| URI | `s3://data-sink/active/usaspending_contracts_map_serving/` |
| grain | **1 row per `contract_award_unique_key`** (PIID+agency composite) |
| rows | **1,075,214** |
| transactions rolled | 1,141,903 |
| geocoded (plottable) | 890,626 (82.8%) |
| columns / indexes | **36 / 25** (9 BTREE + 16 BITMAP) |
| money: obligated | `contract_obligated_usd = NET Σ federal_action_obligation`, `HAVING > 0` |
| money: ceiling | `contract_ceiling_usd = arg_max(base_and_all_options_value)` |
| window | **trailing 730 days** (`CONTRACTS_WINDOW_DAYS`, default 730) — see Decision §d.5 |
| rolls off | `s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/` (the recency-skewed feed) |
| acceptance probe | **63** TX construction contracts clear $1M on *summed* obligation (a per-action filter would suppress these) |
| FY2026 / FY2025 | 914,211 / 133,835 |

**Build rollup logic** (from `materialize_contracts_map.py`): scan prime feed → keep rows with non-empty
`recipient_uei` AND `contract_award_unique_key` → **DEDUP transactions on
`contract_transaction_unique_key`** (the feed carries ~12% dupes) via
`QUALIFY row_number() OVER (PARTITION BY contract_transaction_unique_key …)` → GROUP BY
`contract_award_unique_key` → sum/argmax. Capability-profile `LEFT JOIN` has a defensive 1:1 dedup guard
(`prof1`) so it cannot fan out and break the grain.

### Two new landed archive Lance tables (the third source)

| table | URI | rows | cols | `action_date` span | role |
|---|---|---|---|---|---|
| `usaspending_archive_full_fpds` | `s3://data-sink/active/usaspending_archive_full_fpds/` | **2,975,677** | 300 | 2025-10-01 → 2026-06-04 | FY2026 current-state snapshot (additive) |
| `usaspending_archive_delta_fpds` | `s3://data-sink/active/usaspending_archive_delta_fpds/` | **3,060,070** | 302 | 1978-09-30 → 2026-06-04 | all-FY change log (deletion ledger) |

- Both stamped `archive_snapshot_stamp='20260606'` + `archive_kind` + `archive_source_file`.
- **Delta `correction_delete_ind` breakdown:** `<added/blank>` = 3,059,414 · **`D` = 656** (the unique
  deletions). C-rows present too (modifications). The 656 D-keys are what the canonical tombstones.
- BTREE on `contract_transaction_unique_key`, `contract_award_unique_key`, `recipient_uei`,
  `action_date`, `last_modified_date`; BITMAP on `correction_delete_ind`, `archive_snapshot_stamp`.
- Both read **verbatim** (`read_csv(all_varchar=true)`, `SELECT *`, no filter — nothing left behind).

### API fresh feed advanced

- One-time `usaspending_api_fresh::daily --days 15` Modal run (window 2026-06-13 → 2026-06-28, append).
- **+249,024 rows** → table now **1,986,682**. **Frontier advanced 2026-06-19 → 2026-06-26.**
- Modal run: https://modal.com/apps/bencrane/main/ap-h5PD4phU8S2cqG6n3I50wF
- **No schedule wired** — the `daily` runs are manual; `usaspending_api_fresh.py` has no Modal cron.

### Code (file paths)

**core-x:**
- `pipelines/serving/materialize_contracts_map.py` — the contracts materialization. Key constants:
  `PRIME_URI = …/usaspending_api_fresh/contract_prime_txn/`,
  serving URI `…/usaspending_contracts_map_serving/`, `WINDOW_DAYS` (env `CONTRACTS_WINDOW_DAYS`, 730),
  `max_rows_per_file=64_000`, capability lists capped at 50 elements.
- `pipelines/serving/ops_contracts_map_runs.sql` (ops ledger for contracts) — created in #772.
- `pipelines/usaspending/usaspending_archive.py` — NEW local-CLI archive ingest (Full + Delta).
- `pipelines/usaspending/ops_usaspending_archive_runs.sql` — NEW ops ledger
  (`ops.usaspending_archive_runs`).
- catalyst_api / edge_api: `contracts.v1` decoder added; `ROUTER_VERSION` bumped v10 → v11; 36 parity
  tests pass. (decoders live under `apps/catalyst_api/src/` + `apps/edge_api/src/`; the awards binding
  is at `catalyst map_decoders.py:406`.)

**rare-structure-hq (consumer):**
- `apps/platform-api/src/lib/edge.ts:48` — `AskMarketDataset` enum (4 → 5, `+ "contracts"`).
- `apps/platform-app/src/demo/types.ts:159` — `MapQuery.dataset` mirror.
- `apps/platform-app/src/demo/federalApi.ts` (~205-207) — `askMap(q, dataset)`.
- `apps/platform-api/src/routes/federal.ts:135` — `/ask?dataset=` route guard.
- `apps/platform-app/src/demo/data.ts` — `askRowToCompany` (~1324) money-resolution chain now reads
  `contract_obligated_usd`; `collapseAwardActions` (~1440) dropped on the contracts path (rollup is
  server-side now).
- `CompactDatasetToggle` component (Auto / Companies / Contracts / Actions / Recompete).
- ⌘K palette: a "Dataset" row added under the search box so the table is chosen *while composing*
  (the old toggle only rendered in the result banner *after* a query, which is why it appeared missing).

### PRs and merge commits

| PR | repo | what | merge commit |
|---|---|---|---|
| [#772](https://github.com/bencrane/core-x/pull/772) | core-x | contracts materialization + catalyst/edge decoder wiring | `47a3443` |
| [#773](https://github.com/bencrane/core-x/pull/773) | core-x | write-encoder fix (cap lists @50 + 64k fragments) | `c16a5c3` |
| [#781](https://github.com/bencrane/core-x/pull/781) | core-x | archive ingest (Full + Delta → two Lance tables) | `d8e10cf` |
| [#207](https://github.com/bencrane/rare-structure-hq/pull/207) | rare-structure-hq | consumer enum/decoder + dataset toggle | `b0a42ec` |
| [#208](https://github.com/bencrane/rare-structure-hq/pull/208) | rare-structure-hq | ⌘K palette dataset selector | `0a91a77` |

> **NOTE on disk state:** this handoff worktree (`nice-jackson-ac676d`, branch
> `claude/contracts-write-fix`) shows `usaspending_archive.py` + its ledger as **untracked** and is *not*
> on the merged-archive commit — its `git log` tip is `005969b` (a re-commit of the archive ingest on
> this branch), and `c16a5c3` (the contracts write-fix) is in history. The merged commits above live on
> each repo's `main`. Verify `main` is current before resuming: `git -C /Users/benjamincrane/core-x log -1`.

### Deploys

- Railway auto-deploys both `main` branches on push. No Railway MCP was available in-session; the merges
  *are* the deploy trigger. core-x `main` at `c16a5c3` (then `d8e10cf`), rare-structure-hq `main` at
  `0a91a77`. To serve `/ask?dataset=contracts`, catalyst_api + edge_api must redeploy off core-x `main`
  (the table already exists, so the boot contract-check passes) and the platform app must redeploy.

---

## (d) Decisions + Rationale

### d.1 — The grain fix: build `contracts`, route `winners` for entity totals
- "single contract > $X" lives at **award/PIID** grain → build the `contracts` table (1 row/PIID, money
  pre-summed). The threshold then filters the *rolled-up* column — suppression gone.
- "aggregate/lifetime won > $X" lives at **entity/UEI** grain → route to `winners`/`company`
  (`entity_obligated_usd`), a config/router fix, not a new table.
- **Rejected** Option B (query-time GROUP BY PIID + `HAVING` over the largest table on every map
  request): breaks the warm sub-few-second map posture, and catalyst's aggregate path emits bars, not
  pins, and has no HAVING. Pre-rolling each grain into its own indexed table is the chosen shape ("master
  + join, minus the join").

### d.2 — transactions (atomic SoR) vs contracts (derived serving rollup)
- They are different *layers*, not competing tables. **Transactions = append-only-native SoR**
  (a new mod is a new row, idempotent on `transaction_unique_id`); **contracts = mutable-by-construction
  rollup** (a new mod changes an existing contract's sum → it must be a *re-derived view*, never a primary
  SoR). Decision: transactions is the base; contracts is downstream of it. The contracts table unblocks
  the map *now*; the canonical (the true transaction base) is built *under* it.

### d.3 — Ship the dataset toggle
- The `dataset` param was already plumbed end-to-end; only the front-end control was missing. Exposing
  it deletes the auto-router risk (user declares the grain), turns silent suppression into an explicit
  choice, and makes `contracts` useful the day it lands. Labels by grain/money-semantics, not table name.

### d.4 — schema verdict: **typed v2 SoR; consumers decoupled** (LOCKED 2026-06-28)
> **Decision log (operator, 2026-06-28):** build the canonical as the fully-typed v2 SoR. Existing
> consumers are decoupled from this build. The v1 all-VARCHAR conform is rejected.

- **Build TYPED (v2).** Cast/normalize once at the canonical materialization boundary: typed columns
  (`Date32`/`double`/`bool`), one clean canonical vocabulary, correctness (`'9' > '1000'` lexical
  string-compare bugs eliminated), index-served range pushdown, sentinel normalization
  (`-NONE-`/`''` → NULL), and PK-uniqueness enforced once. **The BULK dump's ~80 enrichment columns are
  carried as native typed columns** (`business_categories`, `recipient_levels`, `federal_accounts`,
  `recipient_hash`, …) — real GTM signal the API CSV lacks, NOT an additive VARCHAR bolt-on. API/archive
  rows get NULL there. VARCHAR was *only* the cheaper migration, never the better design — so it is not
  built.
- **The v1 all-VARCHAR conform is REJECTED.** Its sole merit was a zero-code URI swap for the 9 existing
  consumers (they project the FRESH `bulk_download/awards` verbatim names and string-filter `action_date`).
  Building a v1 all-VARCHAR canonical *solely* to serve those consumers — only to tear it down and rebuild
  it typed shortly after — is wasteful and architecturally wrong. Build it correctly once.
- **Existing consumers are decoupled from this build.** They keep reading their current sources and keep
  working unchanged; the typed canonical does NOT trigger an immediate re-point. Migration onto the
  canonical is a separate, deliberate FUTURE effort (see §e "Future migration targets").
- **Why that migration is non-trivial (and therefore deferred):** because the canonical is typed (new
  vocabulary + real types), each consumer needs genuine per-consumer CODE changes — column renames to the
  canonical vocabulary, cast-aware filters replacing string comparisons. Worked example: today a
  materializer does `scanner(filter="action_date >= '2024-06-01'")` against FRESH's VARCHAR `action_date`
  `"2024-06-01"`; against the typed canonical (`Date32`) that string-compare errors and must be rewritten
  to a date predicate. (Same per-consumer shape for renames like FRESH `type_of_set_aside_code` vs the
  canonical's set-aside column.) This is precisely why migration is a separate code effort, not the
  one-line URI swap a VARCHAR conform would have allowed.

### d.5 — Window default 730d, surfaced as a tunable
- `contract_obligated_usd` is the trailing-730d NET sum, so multi-year contracts undercount lifetime
  value (biases **low, never high**; consistent with the awards/winners family). Shipped at 730d for
  behavioral consistency; rebuild at any window via one arg (`… materialize_contracts_map.py build 1825`).

### d.6 — full-union-dedup with D-tombstones (the canonical merge rule)
- **NOT a hard date split.** FRESH re-modifies rows that have *old* `action_date` (confirmed: a 2022-action
  row carrying a 2026 `last_modified_date`). A boundary cut would keep BULK's stale copy and drop FRESH's
  newer revision. Instead: **full union, deduped on the transaction key, greatest `last_modified_date`
  wins**, then **anti-join out the Delta's `correction_delete_ind='D'` keys**.

### d.7 — Two monthly Lances (Full + Delta), not one
- Full = *data* (current-state rows, additive, latest-supersedes per FY); Delta = *change log*
  (append-only ledger, **keep every Delta forever** — a March deletion is permanent knowledge June won't
  repeat). Mixing supersede + accumulate semantics in one append-only table is messy. Both kept separate
  from the dump and API feed (per-source-landed pattern). Both ingested verbatim, fully.

### d.8 — The hard-won ingest lessons (the archive ingest near-disaster)
The first archive ingest attempt used **Modal** and made a mess. The recovery distilled four durable rules,
now encoded in `usaspending_archive.py`:
1. **Index-build OOM → set `LANCE_BYPASS_SPILLING=true` BEFORE any lance call.** The DataFusion
   external-merge sort pool OOMs (`LanceError: Resources exhausted: ExternalSorterMerge`) on a
   multi-million-row BTREE build. This was *also* the exact failure the contracts build hit. The flag sorts
   the scalar-index build in-RAM. (Same fleet rule per ARCHITECTURE.md.)
2. **Retries cause double-append.** `modal.Retries(max_retries=3)` re-ran the function; because the Lance
   *write* commits *before* the index builds, each retry **re-appended the file**. Both tables polluted to
   ~18M rows (should be ~3M each). **Fix: no auto-retries** (`Retries` removed / local run has none).
3. **`--detach` made the job un-killable** — killing the local launcher did not stop the retrying Modal
   job. **Fix: run LOCAL (doppler + uv), in-session, watched** — the contracts build and every probe ran
   this way correctly. There was no good reason to reach for Modal for this.
4. **Network-resilient: local-write-then-publish.** Direct-to-R2 streaming write **aborts wholesale on any
   wifi blip** (failed mid-upload at part 38; failed again on a phone-hotspot timeout). **Fix: write the
   Lance dataset to local disk + build indexes locally (offline), THEN boto3-publish file-by-file** —
   s3transfer auto-retries each part, so a blip retries that part instead of killing the 6 GB write. The
   proven correct run wrote 2,975,677 rows + all indexes locally in ~2s, then published 38 files in ~12 min.
5. **Stamp idempotency.** A re-run can never double-append: an append is skipped if its
   `archive_snapshot_stamp` is already present in the table (data-driven, robust to a prior partial run).
- **Cruft note:** the abandoned `usaspending-archive` Modal app may still be deployed (idle, 0 tasks) —
  harmless; `modal app stop` it if desired.

---

## (e) THE NEXT BIG THING — the canonical reconciliation (NOT built)

Full design, lifted from the locked plan (transcript L604). Build this next.

### Target table
- **`usaspending_fpds_canonical_txn`** at `s3://data-sink/active/usaspending_fpds_canonical_txn/`
  (Lance v2.1, `mode="overwrite"`, `max_rows_per_file=250_000`).
- A **derived** table that reconciles all three FPDS sources into one complete, deduped transaction table —
  the new base layer every serving table builds off. **The `/ask` decoders do NOT change** (they read the
  serving tables, which simply gain complete data).

### The proven keys (the whole reconciliation hinges on these — 100% sampled match)
- **Transaction PK:** `BULK.transaction_unique_id` ≡ `FRESH.contract_transaction_unique_key` —
  **byte-identical** (same 6-token `subagency_agency_PIID_mod_parentPIID_txn` grammar, same `-NONE-`
  sentinel). Direct equi-join, no derivation. (Archive uses the same
  `contract_transaction_unique_key` name.)
- **Award FK:** `BULK.generated_unique_award_id` ≡ `FRESH.contract_award_unique_key` ≡ archive
  `contract_award_unique_key` (identical `CONT_AWD_…`/`CONT_IDV_…` strings).
- **Precedence column:** `last_modified_date` populated in both (BULK `timestamp[us]`, 607 NULLs/107M;
  FRESH `'YYYY-MM-DD HH:MM:SS+00'` string, zero NULL).

### Merge rule (definitive)
```
live      = union(BULK renamed→FRESH-contract, FRESH as-is, archive_full)
            then per contract_transaction_unique_key keep MAX(last_modified_date)
            tie-break: prefer FRESH
canonical = live  ANTI-JOIN  archive_delta WHERE correction_delete_ind='D'   -- drop 656 tombstoned keys
```
**Edge handling:**
- FRESH `last_modified_date` → parse `'…+00'` → UTC `TIMESTAMP` for the comparison only.
- BULK 607 NULL mtimes → treat as `-inf` (any FRESH revision wins; BULK-only NULL row still survives).
- 523,537 FRESH-only rows past 2026-04-23 survive automatically (no BULK competitor); all BULK history
  < 1993 survives automatically.
- **Efficient mechanics — avoid a 109M-row window sort:** FRESH is a frozen-snapshot-plus-all-revisions,
  so for any key it holds, its mtime ≥ BULK's. Therefore:
  ```sql
  fresh_latest = FRESH deduped to latest row per key        -- sort only ~2M rows
  canonical    = fresh_latest
               ⊎ (BULK rows whose key ∉ fresh_latest)        -- hash anti-join, no BULK sort
               ⊎ (archive_full rows whose key ∉ above)
  ```
  Add an `OR BULK.lm_ts > fresh_latest.lm_ts` guard for the negligible "BULK newer" case. Turns a 109M
  external sort into a 2M sort + hash anti-join.

### Schema — typed v2 SoR (LOCKED, per §d.4)
> **Decision log (operator, 2026-06-28):** typed v2 SoR; consumers decoupled. The v1 all-VARCHAR conform
> is rejected — see §d.4.

- **Typed SoR (`Date32`/`double`/`bool`)** with one clean canonical vocabulary. Sentinels (`-NONE-`,
  empty string) normalized to NULL. **PK-deduped** on `contract_transaction_unique_key`. Casting and
  sentinel-normalization are applied **once, at the canonical materialization boundary** (consistent with
  the "cast once" principle) — the merge LOGIC below is unchanged; only the OUTPUT is typed.
- **Carry the BULK dump's ~80 enrichment columns as native typed columns** (`business_categories`,
  `recipient_levels`, `federal_accounts`, `recipient_hash`, …) — real GTM signal the API CSV lacks, folded
  into the typed schema (NOT an additive VARCHAR bolt-on). API/archive rows get NULL there.
- ZIP caveat: BULK ships `recipient_location_zip5`; FRESH ships `recipient_zip_4_code` (9-digit) — emit
  `recipient_zip_4_code` and derive zip5 downstream as `left(...,5)` (materializers + `geocode_xwalk`
  already do this).

### Pipeline
- **`pipelines/usaspending/usaspending_fpds_canonical.py`** — a heavy 107M-row scan + anti-join. Plan
  said "a Modal app, mirror `usaspending_bulk.py`" — **but given §d.8, strongly consider the proven
  local-CLI pattern** (doppler+uv, `LANCE_BYPASS_SPILLING=true`, no retries, local-write→boto3-publish).
- Stream BULK via `lance.dataset(...).scanner(columns=[crosswalk subset]).to_batches()` → DuckDB
  (`memory_limit` high, `temp_directory` spill, `LANCE_BYPASS_SPILLING=true`); register FRESH + archive
  (small) as tables; run the merge; write.
- **Indexes (BTREE):** `contract_transaction_unique_key`, `contract_award_unique_key`, `recipient_uei`,
  `action_date`, `last_modified_date`, `naics_code`, `product_or_service_code`,
  `federal_action_obligation`.
- **Ops ledger:** `ops.usaspending_fpds_canonical_runs` (rows_in_bulk, rows_in_fresh, rows_out,
  dedup_collapsed, fresh_only_tail, max_action_date, status).
- **`verify()`:** `rows_out ≈ |BULK ∪ FRESH keys|`; `max(action_date) == 2026-06-26`;
  `count(distinct key) == rows_out` (PK uniqueness — dedup actually worked); spot-join 20 known keys.
- **Idempotency:** fully deterministic from sources → `overwrite` is safe to re-run.

### Future migration targets (SEPARATE effort — NOT part of this build)
**Existing consumers are NOT re-pointed by this build.** Each continues to read its current source and
keeps running unchanged. Migrating them onto the typed canonical is a separate, deliberate future effort:
because the canonical is typed (new vocabulary + real types), this is **per-consumer CODE work** — column
renames to the canonical vocabulary and cast-aware filters replacing string comparisons (e.g. the
`action_date` VARCHAR-string-compare → `Date32` predicate rewrite in §d.4) — **NOT a one-line URI swap.**
The 9 files below are the migration targets, each needing code changes (the listed constant is the source
ref to repoint, not a swap-only change):

| # | file | current source ref → canonical |
|---|---|---|
| 1 | `serving/materialize_contracts_map.py` | `PRIME_URI` |
| 2 | `serving/materialize_awards_map.py` | `PRIME_URI` |
| 3 | `serving/materialize_winners_map.py` | `PRIME_URI` |
| 4 | `serving/materialize_company_map.py` | `FRESH_PRIME_URI` |
| 5 | `serving/materialize_active_awards.py` | `PRIMETXN_URI` |
| 6 | `serving/materialize_sub_diversification.py` | `PRIMETXN_URI` |
| 7 | `usaspending/geocode_xwalk.py` (bridge) | `PRIME_URI` |
| 8 | `sam_gov/build_award_capability_profiles.py` (bridge) | `TXN_URI` |
| 9 | `usaspending/govcon_prime_trajectories.py` | confirm source ref before migrating |

**Out of scope (separate, larger migration):** the resolution/spine layer (`award_lines_gold.py`,
`crosswalk_sam_usaspending.py`, `reconcile_entity_profiles.py`, …) reads the BULK `rpt.*` schema directly
— leave it on BULK; not a serving consumer.

### Build order — THIS build vs DEFERRED effort
**THIS build (land + verify the typed canonical; build won_Nd off it):**
```
1. Build & verify  usaspending_fpds_canonical_txn (typed v2)    (the new base)
2. Build the NEW won_Nd windowed-cumulative table off the canonical
       — greenfield (no legacy vocabulary): it reads the typed canonical NATIVELY and is
         the canonical's FIRST consumer / proving ground.
3. Wire won_Nd's catalyst/edge decoder bindings; verify on the map
```
Existing serving consumers (contracts/awards/winners/company/active/sub_diversification) and the bridges
are **untouched** by this build — they keep running on their current sources.

**DEFERRED separate effort (migrate existing consumers onto the canonical):**
```
A. Migrate (code changes) bridges: geocode_xwalk → award capability profiles
       (naics_psc_vertical_map is static — NO rebuild)
B. Migrate (code changes)  active_awards → active_awards_map     (second-order)
C. Migrate (code changes) the maps: contracts, awards, winners, company
D. Rebuild + redeploy catalyst/edge (decoders unchanged; tables now complete) — verify on the map
```
Each migration is per-consumer CODE work (renames + cast-aware filters), not a URI swap — see §d.4 /
"Future migration targets". Bridges before maps (the maps join them). Gate each rebuild on its `verify()`
green before the next.

### The `won_Nd` windowed-cumulative per-company table (designed, not built)
- **Purpose:** answer "a company whose **summed** awards over the last N days ≥ $X" (entity grain,
  windowed). On the contracts grain, "365 days" is only *recency of the last action* and "$X" is a *single
  contract's* value — a company with five $4M contracts is invisible. `won_365d` is the true cumulative.
- **Shape:** upgrade the per-company serving table with one BTREE-indexed scalar column per window:
  `won_90d`, `won_365d`, `won_730d`, `won_1825d` — each `= SUM(federal_action_obligation)` over that
  company's actions in the window, per `recipient_uei`. Compiler binds "won ≥ $X in last N days" →
  `won_Nd >= X` → company pins. Nesting is monotonic: `won_90d ≤ won_365d ≤ won_730d ≤ won_1825d`.
- **Why NO `won_lifetime` column (on today's feed):** on the recency-skewed fresh feed, "lifetime" sums
  ~5 yrs dense + a sparse partial pre-2021 tail → near-redundant with `won_1825d` AND misleading
  (implies a completeness the feed lacks). A true all-time figure is a **prerequisite (the canonical),
  not a column.** Build `won_Nd` **off the canonical** so every window is exact — NOT off the fresh feed.
- **Why pre-roll, not query-time:** the scalar columns are a *pushdown-speed* materialization. From the
  records you *can* derive any window (filter `action_date`, re-sum), but you can't recover `won_365d`
  from the bare scalar `won_lifetime` — and the group-by + HAVING path isn't supported by catalyst's
  aggregate path (no HAVING, returns bars not pins). Pre-rolling fixed windows is map-native and fast.
- Already represented in the toggle as **Companies**; only the materialization + decoder bindings remain.

---

## (f) OPEN TODO checklist

- [ ] **Build `usaspending_fpds_canonical_txn`** per §e (merge rule, proven keys, efficient
      small-side-dedup + anti-join, D-tombstones). **Schema LOCKED: typed v2 SoR** (operator, 2026-06-28)
      — typed columns, one clean vocabulary, sentinels nulled, PK-deduped, BULK's ~80 enrichment columns
      carried as native typed columns. The v1 all-VARCHAR conform is rejected.
- [ ] **Build the `won_Nd` windowed-cumulative company table** off the canonical (`won_90d/365d/730d/1825d`),
      wire its catalyst/edge decoder bindings — greenfield, the canonical's first consumer / proving ground.
- [ ] **(DEFERRED — separate effort, NOT gating this build)** Migrate the existing 6 serving tables + 2
      bridges onto the canonical. This is per-consumer CODE work (column renames + cast-aware filters),
      NOT a URI swap, because the canonical is typed (§d.4 / §e "Future migration targets"). Until then,
      existing consumers stay on their current sources, **untouched** and working.
- [ ] **Redeploy** catalyst_api + edge_api (off core-x `main`) and the platform app — required for
      `contracts` (and future tables) to be servable; confirm Railway built `c16a5c3`/`d8e10cf`/`0a91a77`.
- [ ] **Test the map** (the only step that touches the operator's ~$5 Anthropic key): ⌘K → Contracts →
      e.g. "construction contracts over $1M". (No `/ask` call was made in-session — operator drives this.)
- [ ] **Decide ongoing freshness:** the fresh feed is **manual** (no Modal cron). If genuine daily is
      wanted, wire a Modal Cron on `usaspending_api_fresh.py` (15-day trailing append). Rebuild the
      canonical after each fresh advance (full `overwrite`; deterministic from sources, safe to re-run).
- [ ] **(optional) `modal app stop` the abandoned `usaspending-archive` Modal app** (idle cruft).
- [ ] **(optional, deferred)** expand the typed canonical from the serving-relevant ~45 cols to the full
      297/378 (mechanical crosswalk extension).
- [ ] **Verify operator checkouts are current** before resuming (this worktree is NOT on the merged-archive
      `main`): `git -C /Users/benjamincrane/core-x fetch && git -C /Users/benjamincrane/core-x log -1 --oneline`.

---

## (g) Reference Index

### Lance tables (R2 SoR, `s3://data-sink/active/`)
| table | URI suffix | rows | grain / role |
|---|---|---|---|
| pg-dump bulk | `usaspending/transaction_search_fpds/` | 107,250,527 | deep history, typed, →2026-04-23 |
| API fresh feed | `usaspending_api_fresh/contract_prime_txn/` | 1,986,682 | recent tail, all-VARCHAR, →2026-06-26 |
| archive Full | `usaspending_archive_full_fpds/` | 2,975,677 | FY2026 current-state snapshot |
| archive Delta | `usaspending_archive_delta_fpds/` | 3,060,070 | all-FY change log (656 `D` deletions) |
| **contracts serving (NEW)** | `usaspending_contracts_map_serving/` | 1,075,214 | 1 row/PIID, money pre-summed |
| canonical (PLANNED) | `usaspending_fpds_canonical_txn/` | — | union of all 3, deduped, D-tombstoned |

### Key file paths
**core-x**
- `pipelines/serving/materialize_contracts_map.py` · `pipelines/serving/ops_contracts_map_runs.sql`
- `pipelines/usaspending/usaspending_archive.py` · `pipelines/usaspending/ops_usaspending_archive_runs.sql`
- `pipelines/usaspending/usaspending_api_fresh.py` (Modal, the fresh feed) ·
  `pipelines/usaspending/ops_usaspending_api_fresh_runs.sql`
- `pipelines/usaspending/usaspending_bulk.py` (Modal, the pg-dump ingest)
- catalyst_api `map_decoders.py:406` (the suppression binding) · `apps/catalyst_api/src/` +
  `apps/edge_api/src/` (decoders, `contracts.v1`, `ROUTER_VERSION` v11)
- PLANNED: `pipelines/usaspending/usaspending_fpds_canonical.py`

**rare-structure-hq**
- `apps/platform-api/src/lib/edge.ts:48` (`AskMarketDataset`) ·
  `apps/platform-app/src/demo/types.ts:159` (`MapQuery.dataset`)
- `apps/platform-app/src/demo/federalApi.ts:205` (`askMap`) ·
  `apps/platform-api/src/routes/federal.ts:135` (route guard)
- `apps/platform-app/src/demo/data.ts:1324` (`askRowToCompany`), `:1440` (`collapseAwardActions`)
- `CompactDatasetToggle` + ⌘K palette "Dataset" row

### PR numbers
- core-x: **#772** (build), **#773** (write fix), **#781** (archive ingest)
- rare-structure-hq: **#207** (wiring + toggle), **#208** (palette selector)

### Proven dedup / join keys
- **Transaction PK (dedup + cross-source join):** `contract_transaction_unique_key`
  ≡ `BULK.transaction_unique_id` — byte-identical. (Contracts materialization dedups the ~12% fresh-feed
  dupes on this key before summing.)
- **Award FK (rollup grain):** `contract_award_unique_key` ≡ `BULK.generated_unique_award_id`.
- **Precedence (merge winner):** `MAX(last_modified_date)`, tie-break prefer FRESH.
- **Deletion tombstone:** `archive_delta` rows WHERE `correction_delete_ind = 'D'` (656 keys).

### Run commands
```bash
# Rebuild contracts at a wider window (overwrite-safe):
doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' --with 'pyarrow>=17' \
  --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
  python3 pipelines/serving/materialize_contracts_map.py build 1825

# Archive ingest (local CLI; init_ops once, then ingest full / delta):
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
  --with 'boto3>=1.34' --with 'psycopg[binary]>=3.2' \
  python3 pipelines/usaspending/usaspending_archive.py ingest full \
  'landing/usaspending/2026-06-06/FY2026_All_Contracts_Full_20260606.zip' 20260606

# Advance the fresh feed (Modal, manual, one-time 15-day trailing append):
modal run pipelines/usaspending/usaspending_api_fresh.py::daily --days 15
```

### Ambiguities / things to re-verify (don't take on faith)
- Consumer file **line numbers** (e.g. `data.ts:1324/1440`, `edge.ts:48`) are from the transcript at
  ship time and may have drifted — grep the symbol, not the line.
- The contracts table's **730-day window** means it (and any serving table rolling off the fresh feed)
  under-represents the universe; the canonical fixes the *source*, but the window is a separate per-table
  tunable.
- Item #9 in the future-migration-targets table (`govcon_prime_trajectories.py`) was flagged "confirm ref"
  in the plan — verify it actually reads the prime feed before migrating it (deferred, separate effort).
- The canonical pipeline was planned as a **Modal app** in the original plan; §d.8 strongly argues for the
  **local-CLI** pattern instead. Re-decide at kickoff.
