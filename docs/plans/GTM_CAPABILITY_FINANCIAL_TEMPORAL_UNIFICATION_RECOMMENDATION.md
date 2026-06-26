# GTM Capability × Financial × Temporal Unification — Recommended Next Cycle

**Type:** Prescriptive proposal (the opposite of a state doc). This specifies, defines, and makes the case for a recommended next cycle of work on the `/ask` map. It is the prescriptive companion to the state doc `docs/reference/GTM_MAP_DATA_PLANE_STATE_AND_ORIENTATION_2026-06-26.md` — read that first for footing (grains, decoder contract, move-together invariant, coverage posture). This document references it rather than repeating it.

**Where this distinguishes *verified* (read in-repo this session) from *inferred* (structurally implied, not yet probed live), it says so.** The single most load-bearing fact — the join key — is **verified in source on both sides**; the things that must be confirmed *live* before building are enumerated in §8.

**Anchor checkouts** (per the state doc): core-x data plane worktree `/Users/benjamincrane/core-x/.claude/worktrees/nice-jackson-ac676d`; operator `main` checkout via `git worktree list | grep '\[main\]'`; app consumer `/Users/benjamincrane/rare-structure-hq`.

---

## 1. Thesis

**Unify capability/scope intelligence × money × timing into one queryable grain on the `/ask` map.**

The platform's differentiated asset — the thing that makes it more than a USAspending mirror — is the **SAM-extracted capability/scope intelligence**: clearance requirements, CMMC, controlled-vocab solicitation scope tags, and labor categories, extracted from solicitation attachments by an expensive pipeline (manifest → byte download → text/structured extraction → `govcon_doc_scope` + `govcon_award_requirements` → award-grain capability profiles). That intelligence answers *what a contractor can do and what its work requires*.

Today those capability axes are **physically queryable on exactly one map serving table: `winners`** — an entity rollup (one row per `winner_uei × winner_type`) whose dollar figure is a frozen build-window snapshot, not re-windowable (state doc §5). The **financial action grain** (`awards` — "won $X in the last N days") and the **forward temporal grain** (`active` — "recompetes in the next N days") are **separate datasets** that carry **no capability axes at all** (verified: the `active` decoder block, `apps/catalyst_api/src/map_decoders.py:270–330`, has zero `gated=True` FieldSpecs; the `awards` decoder block at `:361–584` likewise). 

Because a single `/ask` query routes to exactly one dataset and compiles against exactly one decoder (state doc §3), **the cross-axis prospecting questions that are the product's reason to exist are structurally impossible today.** You cannot ask for capability AND recent money in one query, because no single table carries both. The expensive extraction pipeline's entire payoff — being filterable *next to* money and timing — is stranded one dataset away.

**This cycle moves the award-grain capability axes onto `active` and `awards` by the award key**, exactly the way PR #722 moved the `naics_psc_vertical_map` label axes onto those same tables — a proven, low-risk, move-together LEFT-JOIN-and-light-up pattern.

---

## 2. Why this is strategic (not just a feature)

- **It is the raison d'être query class.** "Cleared aerospace subs with $1M+ won in the last 90 days" / "CMMC contractors whose contracts recompete in the next 90 days" are the questions a GTM operator actually asks. None of them compiles today.
- **It activates a sunk, differentiated cost.** The scope-extraction pipeline already ran. The capability profiles already exist as a durable Lance dataset (`govcon_award_solicitation_profiles`). This cycle is **a join, not a new extraction** — it spends serving-layer effort to unlock intelligence that already cost agent-extraction effort to produce.
- **The competitor (a USAspending mirror) structurally cannot answer these.** USAspending has no clearance/CMMC/scope-tag signal at all. The moment capability sits on the same row as windowed dollars and recompete-expiry, the product does something no public mirror can.
- **It composes downstream.** Once capability is on the action and contract grains, the same decoder fields feed aggregate tables (e.g. "top cleared primes by recompete-$ in the next 180d"), not just dot filters.

---

## 3. The query classes unblocked (with the decoder fields each compiles to)

Each row below is impossible today and becomes a single-dataset `/ask` after this cycle. Field names are the **query-name dict keys** (LLM-facing) that map to physical columns per the move-together pattern.

| Query (natural language) | Routes to | Compiles to (query-name fields) |
|---|---|---|
| "cleared aerospace subs with $1M+ won in the last 90 days" | `awards` | `requires_clearance = true` (gated) ∧ `vertical = 'Aerospace & Defense'` ∧ `award_amount >= 1_000_000` ∧ `days_since_action <= 90` |
| "CMMC contractors whose contracts recompete in the next 90 days" | `active` | `requires_cmmc = true` (gated) ∧ `days_until_expiry <= 90` |
| "TS/SCI-cleared incumbents expiring in 180 days in Virginia" | `active` | `req_clearance_level_max in ['TOP_SECRET','TS_SCI']` (gated) ∧ `days_until_expiry <= 180` ∧ `state = 'VA'` |
| "electrical-scope awards over $500k won this quarter" | `awards` | `solicitation_scope_tag has 'electrical_systems'` (gated) ∧ `award_amount >= 500_000` ∧ `days_since_action <= 90` |
| "cleared IT recompetes over $5M next year" | `active` | `requires_clearance = true` (gated) ∧ `vertical = 'Information Technology & Software'` ∧ `contract_current_value >= 5_000_000` ∧ `days_until_expiry <= 365` |

The clearance/CMMC/scope-tag/labor decoder fields, enums, and synonym lexicon **already exist verbatim on the `winners` decoder** (`map_decoders.py:180–210` fields, `:218–226` synonyms) — they are lifted onto `active`/`awards`, not authored fresh.

---

## 4. The mechanism, grounded (the crux)

### 4.1 The bridge dataset and the join key — VERIFIED in source

The award-grain capability bridge is **`govcon_award_solicitation_profiles`**:

- **URI:** `s3://data-sink/active/govcon_award_solicitation_profiles/` (`govcon_gtm_schemas.py:57`, `CAPABILITY_PROFILES_URI`).
- **Grain:** one row per **exploded `contract_award_unique_key`** (`build_award_capability_profiles.py:13–18` docstring; the build asserts `rows == distinct_exploded_keys`, `:404`).
- **Join key index:** `BTREE(contract_award_unique_key)` + `BTREE(recipient_uei)` (`build_award_capability_profiles.py:76`).
- **Capability columns carried** (frozen schema, `govcon_gtm_schemas.py:194–226`): `has_extracted_scope` (bool), `requires_clearance` (bool), `req_clearance_level_max` (string enum: TS_SCI/TOP_SECRET/SECRET/CONFIDENTIAL/PUBLIC_TRUST), `requires_cmmc` (bool), `solicitation_scope_tags` (list<string>, controlled vocab), `top_labor_categories` (list<string>), `req_cert_tags` (list<string>). It carries **no verbatim chunk text** by construction (CUI egress invariant, `build_award_capability_profiles.py:30–39`) — every column is structured/controlled-vocab and safe to surface.

**The join key exists on every relevant grain — this is the load-bearing fact, and it is VERIFIED by reading the materializers, not inferred:**

| Serving table | Grain | `contract_award_unique_key` present in source scan? | Join feasibility |
|---|---|---|---|
| `active` (`materialize_active_awards_map.py`) | award (1/`contract_award_unique_key`) | **YES** — scanned + aliased `contract_award_unique_key AS award_id` (`:98`, `:113`). The grain key IS the join key. | **Direct.** LEFT JOIN by the award key, one-to-one. |
| `awards` (`materialize_awards_map.py`) | action (1/`contract_transaction_unique_key`) | **NOT YET PROJECTED** — but the raw source `contract_prime_txn` carries it (proven: `build_award_capability_profiles.py:164` reads `contract_award_unique_key` from the same feed; the winners materializer reads it from `contract_prime_txn` at `materialize_winners_map.py:113`). | **Feasible after adding the column to the projection.** Many actions share one award key → many-to-one LEFT JOIN (every action of an award inherits that award's capability profile). |
| `winners` (`materialize_winners_map.py`) | entity rollup | already joined (`:125–129` registers `prof`, rolls up per `winner_uei` over `contract_award_unique_key AS award_key`, `:193`) | **Done** (precedent for this whole cycle). |

**The precedent is exact.** `materialize_winners_map.py:122–129` already opens `govcon_award_solicitation_profiles`, scans only the structured/controlled-vocab columns, and rolls them up. This cycle does the *simpler* version of that join — onto `active` (1:1 by award key, no rollup) and `awards` (many:1 by award key, no rollup) — instead of the entity rollup. The hard part (the bridge exists, is populated, is CUI-safe, and is BTREE-keyed) is already solved.

### 4.2 Award-joinable vs entity-only — which axes land where

| Axis | Provenance grain | `active` | `awards` | `winners` (today) |
|---|---|---|---|---|
| `has_extracted_scope` (the gate) | award | ✅ join | ✅ join | ✅ |
| `requires_clearance` / `req_clearance_level_max` | award | ✅ join | ✅ join | ✅ |
| `requires_cmmc` | award | ✅ join | ✅ join | ✅ |
| `solicitation_scope_tags` | award | ✅ join | ✅ join | ✅ |
| `top_labor_categories` | award | ✅ join | ✅ join | ✅ |
| `req_cert_tags` | award | ✅ join (optional) | ✅ join (optional) | ✅ |
| **teaming** (`teaming_dollars_5y`, `n_teaming_primes`, `teaming_prime_names`) | **sub_uei entity** (`govcon_subawardee_profiles`) | ❌ entity-only | ❌ entity-only | ✅ (sub rows) |
| **self-reported** (`subaward_description_tags`) | **sub_uei entity** | ❌ entity-only | ❌ entity-only | ✅ (sub rows) |

**Verified:** the teaming + self-reported axes are sourced from `govcon_subawardee_profiles` at `sub_uei` grain (`materialize_winners_map.py:132–142`; schema `govcon_gtm_schemas.py:281–368`). They have **no award key** and therefore **cannot** join onto the action/contract grains. They stay entity-only on `winners`. This is an architectural boundary, not a gap to close in this cycle — and it is exactly why "subs that teamed with Lockheed, ranked by recent obligations" remains a `winners`-shaped query (the teaming axis lives there; rank-by-recent-obligations is the `winners` window snapshot or an `awards` aggregate by `winner`), not an `active`/`awards` query.

### 4.3 The move-together pattern (cite PR #722 as the template)

This is the **same atomic sequence** the state doc §3 calls the MOVE-TOGETHER invariant, executed in PR #722 (`git show 7c1a710`) for the vertical axes. Per dataset (`active` first, then `awards` — each its own squash PR, no stacking):

1. **Materializer — LEFT JOIN + select + index.**
   - Open the bridge: `prof = lance.dataset(PROFILES_URI, storage_options=so)`; register a scanner projecting **only** `contract_award_unique_key` + the structured capability columns (mirror `materialize_winners_map.py:125–129` — never scan `scope_summary` verbatim or any non-controlled column; CUI posture).
   - For `active`: `LEFT JOIN prof ON keyed.award_id = prof.contract_award_unique_key` (1:1). For `awards`: first **add `contract_award_unique_key` to the prime scan + the `keyed` projection** (it is not selected today — `materialize_awards_map.py:113–123`), then `LEFT JOIN prof ON keyed.contract_award_unique_key = prof.contract_award_unique_key` (many:1).
   - `SELECT` the capability columns through, exactly as the vertical join selects `v.vertical, v.work_type, ...` (`materialize_active_awards_map.py:141`, `:146`).
   - Add the bool/string capability columns to `BITMAP_INDEXES` and any range axis to `BTREE_INDEXES` (mirror the vertical additions at `materialize_active_awards_map.py:55–57` / `materialize_awards_map.py:68–69`). Lance derives the scalar index name as `{column}_idx` for free on overwrite — no separate rename step (state doc §3).
2. **Decoder (both `apps/catalyst_api/src/map_decoders.py` AND `apps/edge_api/src/map_decoders.py`, byte-identical):**
   - Lift the capability `FieldSpec`s verbatim from the `winners` block (`map_decoders.py:182–192`): `has_extracted_scope` (BITMAP, ungated — it is the gate), `requires_clearance`/`requires_cmmc`/`req_clearance_level_max` (BITMAP, `gated=True`), `solicitation_scope_tag`/`labor_category` (list, `gated=True`, enum'd).
   - Add them to `decoder.properties` (catalyst) and the aggregate `dims` where a group-by is wanted (e.g. group recompete-$ by `req_clearance_level_max`).
   - Lift the capability synonym lexicon (`map_decoders.py:218–226`: "cleared", "secret clearance", "top secret", "cmmc", "electrical", "electricians").
   - **Bump the version** (`active.v3 → active.v4`; `awards.v10 → awards.v11`) and the edge `ROUTER_VERSION` — the version string is the translate-memo cache-busting key (state doc §3).
3. **Contract fixtures** (`apps/catalyst_api/tests/test_contract_check.py`): extend `ACTIVE_COLS`/`ACTIVE_IDX` (and `AWARDS_COLS`/`AWARDS_IDX`) with the new columns + Bitmap index entries (fixtures live at `:109–127`; PR #722 added `AWARDS_COLS +4 / AWARDS_IDX +3 Bitmap` — the same shape).
4. **Rematerialize** over R2 (Doppler recipe, state doc §8) and run the **live boot contract check** — 0 violations on all 4 decoders or `/healthz` 503s.
5. **App reads** (`rare-structure-hq`): the wire JSON key == the serving column name and the app reads columns by string literal off an untyped `Record` (state doc §3, the WIRE pass-through). New capability columns flow through automatically as feature properties, but any column the app *displays or filters on* must be added to its read sites — and a **visual check** is mandatory (capability filters return non-empty, gated map not silently empty). TypeScript cannot catch a missing key.

### 4.4 The gate comes for free (critical safety property — VERIFIED)

The scope-coverage safety gate is **decoder-driven and dataset-agnostic**: `compile_map_filter` (state doc §3, `lance_store.py`) deterministically ANDs `has_extracted_scope = true` whenever **any** `gated` clause is present — it keys off the `FieldSpec.gated` flag, not off the dataset. So the moment the gated capability FieldSpecs land on `active`/`awards`, the ~1% scope-coverage gate applies there automatically. One LLM omission cannot leak the full table through to an empty map. **No new gate logic is written this cycle** — it is inherited by adding `gated=True` FieldSpecs. (Verified: the gate is in the shared compiler, and the winners gated fields already exercise it.)

---

## 5. Scope of the FIRST cycle vs deferrable

**FIRST cycle (ship this):**
- **`active` × the award-grain capability axes** — highest strategic value (recompete radar × clearance/CMMC is the sharpest prospecting query), and the *cleanest* join (1:1 by the grain key already in the source scan, no projection change). One squash PR: `active.v4`.
- **`awards` × the award-grain capability axes** — second PR: `awards.v11`. Requires the one extra step of projecting `contract_award_unique_key` into the awards scan (it is action-grain; the key is in `contract_prime_txn` but not currently selected). Many:1 join.

Ship `active` first to prove the pattern end-to-end on the simplest join, then `awards`. **No stacked PRs** — each against `main` directly (squash drops later-added commits; state doc §8).

**Deferrable (explicitly out of this cycle):**
- **Teaming + self-reported axes on `active`/`awards`** — structurally impossible (no award key; §4.2). These stay entity-only on `winners`. Not a follow-on; an architectural boundary.
- **Aggregate group-by on the new capability dims** beyond the minimal set — add `req_clearance_level_max` as an aggregate dim if "recompete-$ by clearance level" is wanted; defer the rest until a query demands it (simplicity-first).
- **`req_cert_tags` / `top_labor_categories` on `active`/`awards`** — joinable (award-grain), but lower-signal than clearance/CMMC/scope-tag. Include if cheap (same join, two more SELECTed columns); defer if it widens the PR.
- **A capability aggregate metric** (e.g. count of cleared recompetes) — the aggregate plumbing already exists; wire only what a query class in §3 needs.

---

## 6. Coverage caveats — honest, not silently filtered

Same posture as the vertical axes (state doc §6). **This lights up a high-value SLICE, not the whole map. Do not oversell.**

- **Scope-extraction reaches ~1% of awards.** Per the state doc, ~0.96% of awards have extracted solicitation text (~4,220 scope-extracted winners). The capability profile build measured ~35,028 award rows over ~18,404 resources (`build_award_capability_profiles.py:16`), of which the `has_extracted_scope`/`requires_clearance`/`requires_cmmc` subsets are smaller still — **confirm the live counts (§8) before writing coverage copy.**
- **The gate makes the slice honest, not hidden.** When a gated clause is present, `has_extracted_scope = true` is ANDed in — so a capability query returns only the extracted slice, by design, never a silently-filtered full map. Rows without an extracted profile surface as "not applied" on ungated queries (LEFT JOIN → NULL capability columns), never silently dropped — identical to how unlabeled vertical rows behave.
- **Coverage copy must be dollar-and-row honest.** Mirror the vertical-axis correction (PR #726): phrase coverage as the measured fraction of awards/recompetes with an extracted profile, not a vibe. The edge guard forbidding stale "90 days" prose (state doc §7.3) applies — phrase windows as the live build window.
- **Profile is award-snapshot, not live.** The capability profile is an overwrite snapshot pinned to its source versions (`build_award_capability_profiles.py:378–383`). A contract's capability reflects its solicitation at profile-build time. Acceptable (capability requirements don't change post-award), but state it.

---

## 7. Why this over the alternatives

Deeper extraction (raising the ~1% coverage) compounds a sunk cost without unlocking a single new *query shape* — the cross-axis join is still impossible until the serving tables carry capability, so coverage work is strictly downstream of this cycle and lower-leverage now. A brand-new fused dataset (a fifth serving table joining capability+money+timing) duplicates three existing tables, triples rebuild/contract surface, and violates the platform's one-decoder-per-grain model for zero capability the LEFT-JOIN doesn't already deliver. Outreach/activation (wiring these prospects into the GTM/Close machinery) is real downstream value but is *gated on this cycle existing* — there is nothing to activate until the queries compile. **The join is the highest-leverage next move: it is mostly-built (the bridge exists, the gate is shared, the pattern shipped in #722), it unblocks the entire raison-d'être query class, and it makes every later investment — deeper extraction, activation — pay off against a live surface.** The operator may still redirect; this is the load-bearing recommendation, not a mandate.

---

## 8. Risks / unknowns — verify live BEFORE building

Ordered by how badly a wrong assumption hurts.

1. **#1 — Join-key cardinality + format match on `awards` (the only inferred link).** VERIFIED in source that `contract_prime_txn` carries `contract_award_unique_key` (the profile builder and winners materializer both read it from that feed). NOT yet verified live that, after adding it to the awards projection, the **string format is byte-identical** to the bridge's key (both come from the same raw feed, so this is *expected* — but a transform/trim divergence would silently zero the join). **Probe:** sample-join a handful of award keys from `usaspending_awards_map_serving`'s source against `govcon_award_solicitation_profiles` and confirm a non-trivial match rate before wiring the materializer. For `active` the key is the grain key already in the scan — lower risk, but probe the match rate too.
2. **Is the capability profile current and populated?** The bridge is an overwrite snapshot. **Probe:** `python pipelines/sam_gov/build_award_capability_profiles.py verify` (or a direct `count_rows`) — confirm rows > 0, `has_extracted_scope` / `requires_clearance` / `requires_cmmc` counts are non-zero, and `built_at` is recent. A stale or empty bridge makes the whole cycle cosmetic. (Note: `govcon_gtm_schemas.py:24` + `build_award_capability_profiles.py:73` reference a *dropped legacy shell* URI `govcon_award_capability_profiles_90day` — do NOT join that; the live SoR is `govcon_award_solicitation_profiles`. Verified the active builder writes the un-suffixed URI.)
3. **Live coverage numbers for the copy.** The ~1% / ~4,220 figures are from the state doc; confirm the *award-grain* counts that will actually appear on `active`/`awards` after the join (not the winners-rollup counts) before writing user-facing coverage strings.
4. **`active` source key coverage.** `materialize_active_awards_map.py` filters `recipient_uei IS NOT NULL` (`:130`); confirm `contract_award_unique_key` is non-null across that set (the grain claim implies it, but a NULL key would orphan the join row — harmless LEFT-JOIN NULL, but it affects the coverage denominator).
5. **Boot-contract + visual-check discipline.** The move-together invariant is unforgiving: a column/index/fixture that lags flips `/healthz` to 503; an app read that lags shows silent `$0`/empty. Run the live contract check after each rematerialize and do the visual check (state doc §3, §8). Non-negotiable, not a risk to "accept" — a step to execute.

---

## 9. Definition of done (per dataset PR)

1. Materializer LEFT-JOINs `govcon_award_solicitation_profiles` by the award key, SELECTs the capability columns, indexes them (BITMAP for bools/enums); rebuilt over R2 with rows > 0 and a measured capability-coverage > 0.
2. Both decoders carry the lifted gated capability FieldSpecs + synonyms, byte-identical; version + `ROUTER_VERSION` bumped; catalyst `properties` + aggregate dims updated.
3. Contract fixtures extended; `pytest` green (compile + contract + parity); ruff clean.
4. Live boot contract check returns **0 violations on all 4 decoders**.
5. A §3 query class executes end-to-end on the live map and returns a non-empty, gated, dollar-correct result (visual check).
6. Coverage copy is honest (measured fraction; no stale "90 days").
7. Merged, then **pulled into the operator's `main` checkout** — disk truth matches the merged commit (state doc §8; "merged" ≠ "done").

---

## Appendix — fast file index (this cycle's touch surface)

| Concern | Path |
|---|---|
| Bridge dataset (award-grain capability) | `s3://data-sink/active/govcon_award_solicitation_profiles/` |
| Bridge schema (frozen) | `pipelines/sam_gov/govcon_gtm_schemas.py:174–226` (`capability_profiles_schema`) |
| Bridge builder (probe with `verify`) | `pipelines/sam_gov/build_award_capability_profiles.py` |
| Precedent join (winners ⋈ profiles) | `pipelines/serving/materialize_winners_map.py:122–229` |
| Precedent vertical join (PR #722 template) | `git show 7c1a710`; `pipelines/serving/materialize_active_awards_map.py:106–147`, `materialize_awards_map.py:125–182` |
| `active` materializer (join target #1) | `pipelines/serving/materialize_active_awards_map.py` |
| `awards` materializer (join target #2; add award key to scan) | `pipelines/serving/materialize_awards_map.py:113–123` |
| Capability FieldSpecs + synonyms to lift | `apps/catalyst_api/src/map_decoders.py:180–226` (mirror in `apps/edge_api/src/map_decoders.py`) |
| Decoder dataclasses + gate flag | `apps/catalyst_api/src/map_decoders.py:43` (`gated`), `:56` (`AggregateSpec`) |
| Shared gate logic (inherited, not written) | `apps/catalyst_api/src/lance_store.py` (`compile_map_filter`) |
| Contract fixtures to extend | `apps/catalyst_api/tests/test_contract_check.py:109–127` (`ACTIVE_*`/`AWARDS_*`) |
| App read sites + visual check | `rare-structure-hq/apps/platform-app/src/demo/data.ts`, `federalApi.ts:152` |
</content>
</invoke>
