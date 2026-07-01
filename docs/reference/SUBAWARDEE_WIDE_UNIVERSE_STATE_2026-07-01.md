# Subawardee Wide-Universe Build — State, Comparison, and Next Steps

> **Canonical state record as of 2026-07-01.** Every count below was verified live against
> R2 (`s3://data-sink/active/`) and the HQX ops ledger during the build session, read-only.
> Reproduction recipes are in §7. Trust the verified column, re-read `.schema` before any
> `$`/code query — live schema drifts from committed loaders.
>
> **Scope of this session:** (1) refreshed the API-fresh subaward feed 40 days; (2) shipped a
> `build_wide` mode that materializes the full subawardee universe since 2021-01-01 into a NEW
> table `subawardee_work_profile_wide`, canonical `subawardee_work_profile` untouched. This doc
> records what exists now, how it compares to the prior state, and the concrete chain to close
> the one open gap (the capability card does not yet reflect the wide universe).

---

## 0. TL;DR

| | |
|---|---|
| **Shipped** | PR [#854](https://github.com/bencrane/core-x/pull/854) — `build_wide` mode on `pipelines/usaspending/subawardee_work_profile.py`. Merged to `main` at `512ef52`. Default `build` path is value-identical (canonical never touched). |
| **New table** | `s3://data-sink/active/subawardee_work_profile_wide/` — **191,693** firms (vs canonical **25,450**), floor **2021-01-01**, 34 cols, BTREE `subawardee_uei`/`subawardee_parent_uei`/`subawardee_state_code`. |
| **Feed refreshed** | `usaspending_api_fresh/contract_subaward` via `daily 40` (append): **199,901 → 321,204** rows, distinct subawardees **25,450 → 27,610**, recent edge **2026-06-05 → 2026-06-29**. |
| **Open gap** | The capability card (`capability_profile`) does **not** auto-reflect the wide universe. Its recommendation leg (`capability_lanes`) is fed by a narrow 25K table (`subaward_naics_psc`); it covers only **27,459 / 191,693 (14%)** of the wide firms. Not a blocker — a bounded 3-rebuild chain (§6). |
| **Inherited ceilings** | Prime rail freshness caps at **2026-04-23** (FPDS bulk-mirror horizon; the API-fresh prime feed exists but is unwired). Sentinel future sub-dates ride through the source (clamp `≤ 2026-12-31` on read). |

---

## 1. What shipped this session

### 1.1 `build_wide` mode (code — PR #854, `512ef52`)

`pipelines/usaspending/subawardee_work_profile.py` gains a `build_wide` entrypoint. Diff: +71 / −23, one file.

- **Population re-seed.** Recent mode (default): population = distinct subawardees in the 90-day API-fresh feed (25,450). Wide mode: population = distinct subawardees in `subaward_search` with `sub_action_date ≥ 2021-01-01` **∪** the API-fresh feed → **191,693**. Identity (name / state / country) for net-new firms is carried from `subaward_search` (`sub_awardee_or_recipient_legal`, `sub_legal_entity_state_code`, `sub_legal_entity_country_code`).
- **Fixed floor.** `WIDE_FLOOR = 2021-01-01` (env `SUBAWARDEE_WORK_PROFILE_WIDE_FLOOR`) replaces the rolling `today − 5y`.
- **Distinct target.** Writes `PROFILE_WIDE_URI = subawardee_work_profile_wide/`. The canonical `subawardee_work_profile` URI is never referenced by the wide path.
- **Default path preserved.** In recent mode `pop` is a pure projection of `entities`, so the assembly is value-identical to the prior builder; canonical rebuilds are unchanged.

Run: `doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' python3 pipelines/usaspending/subawardee_work_profile.py build_wide` (isolate scratch with `SWP_SCRATCH=/tmp/subawardee_work_profile_wide`).

### 1.2 API-fresh subaward feed refresh (`daily 40`)

`pipelines/usaspending/usaspending_api_subaward_fresh.py daily 40` — append-only, trailing 40-day `last_modified` window. Chunked against USAspending's slow `elasticsearch_sub_awards` backend; two 1-day windows (2026-06-11, 2026-06-29) hit the poison-day floor and are bounded GAPs (ledgered, catchable on the next daily run). Ops ledger row: `success`.

---

## 2. The source matrix — ground truth (verified rowcounts)

Every subaward/prime table splits on two axes: **tier** (prime vs sub) × **provenance** (full bulk mirror vs API-fresh windowed slice).

| | **Bulk mirror** — USAspending full DB download; complete historical archive | **API-fresh** — `bulk_download/awards` API on a `last_modified` window; a *slice*, not an archive |
|---|---|---|
| **PRIME** | `usaspending/transaction_search_fpds` — **107,250,527** (transaction grain) · `usaspending/award_search` — **78,636,657** (award grain) | `usaspending_api_fresh/contract_prime_txn` — **1,986,682** |
| **SUB** | `usaspending/subaward_search` — **9,801,723** | `usaspending_api_fresh/contract_subaward` — **321,204** (was 199,901) |

Key facts that recur below:
- **The two API-fresh feeds are recent-modification slices, not archives.** `contract_subaward` was built from a single ~90-day `last_modified` pull (backfill 2026-03-09→06-07 + gapfill 2026-05-26→06-03, per ops ledger). Its action dates span **2001-05-13 → 2026-06-29** (66,011 rows pre-2021), but only because a `last_modified` window on *prime awards* drags in each award's full multi-year subaward tail — an incidental byproduct, not deliberate coverage. **Complete 2021+ subaward coverage lives in `subaward_search`, not this feed.**
- **A subaward carries no NAICS/PSC of its own.** `subaward_search` records the *prime's* NAICS (`naics`) but **not** the prime's PSC; FSRS does not collect subaward-level codes. The prime's PSC is only obtainable by joining a subaward → its prime award → FPDS.

---

## 3. `subawardee_work_profile_wide` vs the canonical table

Both are 34-col, one-row-per-UEI, same schema. They differ in **population** and **floor**.

| Property | canonical `subawardee_work_profile` | **`subawardee_work_profile_wide`** |
|---|--:|--:|
| Rows / distinct UEIs | 25,450 | **191,693** |
| Population seed | 90-day API-fresh feed subawardees | `subaward_search` ≥ 2021-01-01 **∪** API-fresh (189,158 ∪ 27,610) |
| Window floor | rolling 5y (2021-06-09 at 06-08 snapshot) | **fixed 2021-01-01** |
| Snapshot date | 2026-06-08 | 2026-07-01 |
| With sub history | 23,059 | **189,581** |
| With prime history | 14,248 (56%) | **36,646 (19%)** |
| Recent-90d cohort embedded | 25,450 (all) | 27,610 |
| Named | — | 191,658 / 191,693 |
| Prime action range | → 2026-04-23 | 2021-01-01 → **2026-04-23** |
| Sub action range | (sentinel-dirty) | 2021-01-01 → (sentinel) |
| Indices | BTREE uei / parent_uei / state_code | same |

**Reading.** The wide table is 7.5× the population at the same shape. Note **only 19% (36,646) have prime history** and **only 27,610 are in the recent-90d cohort** — so ~155K of the 191K are "cold" firms (sub history since 2021, no prime activity, not recent). That sparsity is correct and expected; it is also why a naive downstream reflow needs care (§5).

**Rails and their sources (identical logic in both tables):**
- Sub rail ← `subaward_search` (batched, indexed on `sub_awardee_or_recipient_uei`) **∪** the API-fresh feed. Carries `sub_top_naics` (the prime-subbed-under's NAICS) — **no sub PSC**.
- Prime rail ← `transaction_search_fpds` only (batched, indexed on `recipient_uei`). Carries `prime_top_naics` + `prime_top_psc` (the firm's *own* prime awards). Freshness capped at the FPDS mirror horizon **2026-04-23**.

---

## 4. The two "primes" — a load-bearing distinction

A subawardee firm has two unrelated prime footprints. Conflating them is the main source of confusion:

| | What it is | Columns | Full (NAICS, PSC) combo? |
|---|---|---|---|
| **Firm-as-prime** | contracts the firm won *directly* from the government | `prime_top_naics` + `prime_top_psc` | **Yes** — on the work_profile (from FPDS, `recipient_uei = firm`) |
| **Prime-subbed-under** | the parent contract (won by *another* company) the firm did a piece of | `sub_top_naics` only | **No** — NAICS only on the work_profile; PSC is absent because `subaward_search` doesn't carry it |

The prime-subbed-under's full (NAICS, PSC) combo **does** exist — but in a *different, narrow* table: **`subaward_naics_psc`** (199,901 rows, **25,450** distinct subawardees), which carries `prime_naics_code` + `prime_psc_code` (PSC joined from FPDS at build time) and **no** sub-own codes. This has always been true — it was never on `subawardee_work_profile`, canonical or wide.

---

## 5. Why the wide table does not auto-reflow into the capability card

`capability_profile` (the per-firm card, **78,219** rows) is a pure assembly, snapshot/overwrite, of four sources:

| Card leg | Source | Coverage of the wide universe |
|---|---|---|
| identity + activity (`wp`) | **canonical** `subawardee_work_profile` (25,450) | reads canonical, **not** the wide table |
| `recommended_lanes[]` | `capability_lanes` (77,532 distinct UEIs) | **27,459 / 191,693 = 14%** |
| `is_dsbs` | `sba_dsbs_certified_firms` | roster join |
| `designations[]` | `govcon_subawardee_designations` | sub-scoped |

Two independent reasons it will not reflect the wide universe without work:

1. **Wiring.** `scripts/build_capability_profile.py:52` reads the *canonical* URI. Nothing in the repo reads `subawardee_work_profile_wide` yet. And it is a snapshot build — even canonical changes only appear on re-run.
2. **The recommender is narrow.** `capability_lanes`' "subbed" population comes from `subaward_naics_psc` (25,450). Of the 191,693 wide firms, only **27,459 (14%)** have ≥1 lane; **164,234 (86%) would be lane-less shells** if `wp` were swapped naively — sub rollups but no recommendations, and (since 81% have no prime history) mostly an empty prime panel too. That breaks the card's "firms with something to show" invariant and buries the useful rows.

**The catalyst route / serving path is *not* a concern:** it is a read-only indexed point-lookup by UEI; 191K vs 78K is irrelevant to a BTREE lookup and the schema is unchanged (no redeploy forced).

---

## 6. Recommendations — the chain to close the gap

**To make the capability card reflect the wide universe (not a blocker — three bounded rebuilds):**

1. **Widen `subaward_naics_psc` from 25K → 191K.** Rebuild it over the `subaward_search` ≥ 2021 population instead of the narrow API-fresh feed. The one non-trivial step: to get the **prime's PSC** (absent from `subaward_search`), join each subaward → its prime award → FPDS: `subaward_search.unique_award_key → transaction_search_fpds.generated_unique_award_id → product_or_service_code` (+ `naics_code`). This is the *same* indexed FPDS join already run for the wide build's prime rail — routine, similar runtime, no new pattern. Overwrite in place so downstream reads pick it up unchanged.
2. **Re-run `capability_lanes`** — no code change; it reads the now-wider `subaward_naics_psc` and emits lanes for the full universe.
3. **Re-run `capability_profile`** with its `wp` source pointed at `subawardee_work_profile_wide` (one-line change at `scripts/build_capability_profile.py:52`) — after step 2, so the card gains both the wide activity panel and wide recommendations together, avoiding the 86%-shell state.

> Do steps 1→2→3 **in order**. Pointing `wp` at the wide table *before* widening the recommender produces the 164K lane-less shells; the lockstep is the whole point.

**Independent follow-ups (not required for the reflow, but the next real gaps):**

- **Prime-rail freshness.** Both work_profile tables source the prime rail only from `transaction_search_fpds` (horizon 2026-04-23). The API-fresh prime feed `contract_prime_txn` (1,986,682) exists but is **unwired**. Unioning it into the prime rail (mirroring how the sub rail already unions its API-fresh feed) would lift the prime ceiling toward current. Requires the same dedup-on-identity treatment the sub rail uses.
- **Sentinel hygiene.** `sub_last_action_date` carries source garbage (2106/6010 sentinels). Consumers must clamp `≤ 2026-12-31` on read (as every query in this session did). A source-side clamp in the sub scan would remove the footgun permanently.
- **Refresh cadence.** The API-fresh subaward feed is a manual `daily N` run today. Per the architecture, cadence belongs to the control plane (Trigger.dev) with a durable token callback — a scheduled `daily` keeps the recent edge current without manual runs and closes the two poison-day gaps automatically via window overlap.

---

## 7. Verification recipe (reproduce every number)

Read-only. Use a scratch dir, clean up. Storage-options keys per `pipelines/fdic/ingest.py:_storage_options` (`aws_access_key_id` / `aws_secret_access_key` / `endpoint` / `region="auto"`).

```
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' python3 <script>
# ALWAYS read .schema before a $/code query — committed loaders drift from live.
# date cols are date32 → filters need DATE 'YYYY-MM-DD'.
# clamp sub_last_action_date <= DATE '2026-12-31' (source sentinels).
```

Spot checks used this session:
- Wide table: `lance.dataset("s3://data-sink/active/subawardee_work_profile_wide/", storage_options=so).count_rows()` → 191,693; `with prime_awards_5y>0` → 36,646; `min(profile_window_start)` → 2021-01-01.
- Lane coverage: distinct `capability_lanes.uei` (77,532) ∩ distinct `subawardee_work_profile_wide.subawardee_uei` (191,693) → 27,459.
- Narrow combo table: `subaward_naics_psc` → 199,901 rows / 25,450 distinct subawardees / `prime_naics_code`+`prime_psc_code` present / no sub-own codes.
- Feed provenance: `SELECT run_mode, window_start, window_end, rows_written, table_rows_after, status FROM ops.usaspending_api_subaward_fresh_runs ORDER BY executed_at`.

---

## 8. Pointers / index

**Code (paths relative to repo root, `origin/main` @ `512ef52`):**
- `pipelines/usaspending/subawardee_work_profile.py` — `build` (canonical) + **`build_wide`** (this session). Population, floor, and output URI are mode-selected.
- `pipelines/usaspending/usaspending_api_subaward_fresh.py` — `backfill` / `daily` / `verify` for the API-fresh subaward feed (append-only, chunked, zombie-split).
- `scripts/build_capability_profile.py` — the card assembly (reads canonical `subawardee_work_profile` at `:52`). Reflow target for §6 step 3.
- `scripts/build_capability_lanes.py` — the recommender (reads `subaward_naics_psc`). Re-run target for §6 step 2.

**Datasets (`s3://data-sink/active/<name>/`):**
- Work profiles: `subawardee_work_profile` (25,450, canonical) · **`subawardee_work_profile_wide`** (191,693, new).
- Sub sources: `usaspending/subaward_search` (9.80M, bulk) · `usaspending_api_fresh/contract_subaward` (321,204, API-fresh) · `subaward_naics_psc` (199,901, narrow combo w/ prime NAICS+PSC).
- Prime sources: `usaspending/transaction_search_fpds` (107.25M) · `usaspending/award_search` (78.64M) · `usaspending_api_fresh/contract_prime_txn` (1.99M, unwired).
- Card cluster: `capability_profile` (78,219) · `capability_lanes` (77,532 UEIs) · `sba_dsbs_certified_firms` · `govcon_subawardee_designations`.

**Ops:** `ops.subawardee_work_profile_runs` (build ledger) · `ops.usaspending_api_subaward_fresh_runs` (feed ledger).

**Prior art:** `docs/plans/SUBAWARDEE_PROFILE_UNIVERSE_WIDENING_PLAN.md` (the 2026-06-16 scoping doc — its "Path B" widens `capability_profile` tags, a related but distinct effort from this table widening).
