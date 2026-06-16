# Subawardee-Capability Surface — End-to-End Build-Out Plan

**Date:** 2026-06-16 · **Status:** Decision-ready (operator review; not yet built) · **Ground truth:** live R2 probes + repo verification 2026-06-16 (paths + dataset versions cited inline). Supersedes the brief's pointer numbers where live data corrected them — every count below is re-measured.

---

## 0. Verdict

The sub-side capability spine is **80% built and mostly idle**. `govcon_subawardee_capability_profiles` (v49, 6,586 rows) already fuses sub-work ⊕ prime-scope ⊕ teaming ⊕ POC at sub_uei grain and its `query` CLI is live end-to-end (verified: "electrical" → 313 matched subs with cited work, prime scope they operated under, teaming primes, and a POC). What is missing is **serving and recall**, in this order of leverage:

1. **No semantic search of subs** — `govcon_sub_capability_vectors_90day` is an empty frozen shell (0 rows, no index). This is the single headline gap and the only one that unlocks "find subs whose past work is like X."
2. **No served drill-down** — catalyst_api dossier/overview/active-contracts are PRIME-only (`entity_profile_gold`). A consumer cannot open one sub and see capabilities + prime-contract history through the typed gateway.
3. **Map subawardee rows carry zero capability** — winners map v26 has 1,689 subawardee rows, **all `has_extracted_scope=false`, all capability_tags empty**, because the Phase-3 rollup joins `ON winner_type = 'prime_recipient'` only. The columns exist; the subawardee leg is unfed.

Everything load-bearing for the structured legs is DONE: the profiles table, the requirements/doc_scope enrichment, teaming edges (115,366 over the 9.8M-row 5y `usaspending/subaward_search`), the BGE-large embed harness (proven live on the scope sink), and the deterministic sub-targeting builder. **The build is wiring and one embed run, not new data engineering.**

---

## 1. Goal-state + unlocked consumer queries

| Goal | Surface | Unlocked by |
|---|---|---|
| (a) Semantic search of subs by what they do | gtm_mcp `search_subawardee_capabilities("substation electrical upgrade")` → ranked sub_ueis + cited descriptions | Gap 1 (vectors) + Gap 5 (MCP leg) |
| (b) List/filter all subs on the full capability axis | catalyst_api `POST /map/winners/query {winner_type=subawardee, capability_tag has electrical_systems, requires_clearance=true}` → GeoJSON; OR gtm_mcp raw-SQL over profiles | Gap 3 (map sub leg). Raw-SQL path is **already live**. |
| (c) Drill into ONE sub: capabilities + prime contracts won under | catalyst_api `GET /api/v1/entities/{uei}/subaward-profile` → profile + prime-contract history | Gap 4 (endpoint) |

Concrete calls the build enables:

```
# (a) fuzzy-X over sub work  (gtm_mcp, Phase-5 ANN leg)
search_subawardee_capabilities(query="aircraft avionics depot repair", k=25, naics=["4881","3364"])
  → [{sub_uei, sub_name, _distance, matched_description, n_subawards, total_subaward_amount}]

# (b) the full axis, served  (catalyst_api map EXECUTE — subawardee leg)
POST /api/v1/map/winners/query
  {"filters":[{"field":"winner_type","op":"=","value":"subawardee"},
              {"field":"capability_tag","op":"has","value":"electrical_systems"},
              {"field":"req_clearance_level_max","op":"in","value":["SECRET","TOP_SECRET","TS_SCI"]}]}
  → FeatureCollection (has_extracted_scope=true auto-ANDed by the gate)

# (c) one sub, full drill-down  (catalyst_api — NEW route)
GET /api/v1/entities/YZTLALWM4UC7/subaward-profile
  → {capabilities:{capability_tags, req_clearance_level_max, req_cert_tags, top_labor_categories,
                   scope_summary, has_extracted_scope},
     subaward_history:[{prime_uei, prime_name, subaward_amount, subaward_description, action_date, naics}…],
     teaming:[{prime_uei, prime_name, dollars_5y, count_5y}…], poc:{…}}
```

---

## 2. Live ground truth (probed 2026-06-16 — use these, no others)

| Metric | Live value | Note (vs. brief) |
|---|---|---|
| `usaspending_api_fresh/contract_subaward` | v12, 199,901 rows, 25,450 distinct sub_uei | the sub→prime fact |
| DISTINCT `subaward_description` (non-null, global) | **66,358** | brief's "130,011" is the **5y `subaward_search`** distinct-subaward count, NOT this 90-day feed — corrected |
| DISTINCT (sub_uei, description) pairs | **99,314** | the per-(sub,desc) chunking grain |
| subs with ≥1 description | **25,449** of 25,450 | description fill 99.99% |
| distinct-description char-len | p50=41 · p90=164 · p99=1,211 · max=11,319 · **671 over 1,200 (1.0%)** | the ≤1,200 chunk + 512-tok ceiling bites only the 1% tail |
| per-sub CONCAT char-len | p50=671 · p90=8,340 · p99=78,225 · max=78,225 · **1,080 subs over 1,200** | **naive one-row-per-UEI concat truncates ~half the prolific subs at bge's 512-tok ceiling** — confirmed; per-(sub,desc) chunking is mandatory |
| `govcon_sub_capability_vectors_90day` | v1, **0 rows, no index** | schema correct; empty shell |
| `govcon_subawardee_capability_profiles` | v49, **6,586 rows** · 4,220 has_extracted_scope · 2,497 requires_clearance · 6,103 poc_available · 6,586 teaming · all 7 indices live | the sub spine — built, idle |
| `govcon_sub_targeting_90day` | v9, 165,974 rows (award×sub) | deterministic v1 matcher |
| `govcon_teaming_edges_90day` | v4, 115,366 rows, 23,006 distinct sub_uei | source = `usaspending/subaward_search` (9.8M rows, 5y) |
| `usaspending_winners_map_serving` | v26, 39,738 rows · 38,049 prime / **1,689 sub** · 8,235 has_extracted_scope (**0 of them sub**) | capability columns exist; sub leg unfed |
| `usaspending_awards_map_serving` | v15, 386,363 rows · 383,750 prime / 2,613 sub · **no capability columns** (awards.v1) | event grain |
| `govcon_scope_vectors_90day` (harness proof) | v286, 1,481,167 rows · IVF_PQ on `embedding` + BTREE/BITMAP scalars LIVE · 326,866 still NULL | the reuse harness is real and resumable |

---

## 3. Critical assessment per gap (worth-building, order, MVP vs full, what's done)

### Gap 1 — Sub-capability vectors (BUILD, P0, headline)
**Worth it: yes — the only gap that unlocks goal (a).** Without it, "fuzzy X" is impossible and the targeting `capability_match` edge stays a keyword LIKE. **Already done:** frozen schema (`sub_capability_vectors_schema`), the BGE-large embed/IVF_PQ harness (`sam_attachment_embed_90day.py` + `_modal.py`), and the proof it works (scope sink live). **To build:** a thin builder that dedups → chunks → stamps text into the vectors dataset, then the EXISTING embed harness fills + indexes it. **Blast radius: isolated** — overwrite-snapshot of an empty dataset; no live reader until Gap 5 wires it.

> **Grain decision (forced by live data):** per-(sub_uei, distinct-description) chunking → ~99,314 base rows, ~103,223 chunks at ≤1,200 chars. Global-dedup-then-attribute (66,358 desc → 67,421 chunks) loses the sub attribution and forces a join-back; reject it. **Per-UEI concat is rejected outright** (1,080 subs truncate). The `description_chunk_ix` grain is per-sub ordinal across that sub's chunks.

### Gap 2 — Targeting ANN edge upgrade (DEFER)
**Worth it: not now — cut from the critical path.** The plan §3.4/§5 frames this as Phase-5 swapping `capability_match` from deterministic to ANN max-sim. But the *live* `materialize_sub_targeting.py:168-204` matcher is **already a labor-token LIKE over a 2,000-char `string_agg` concat** keyed on NAICS-4 family — it works and produces 165,974 rows. Upgrading it to ANN is a recall refinement, not a goal-state unlock: goals (a)/(b)/(c) are served by the profiles + vectors + map + endpoint, none of which read `sub_targeting`. **Defer until a consumer demands award×sub ANN recall.** When built: it's a snapshot-overwrite rebuild reading the new vectors, same `edge_type` enum, no schema change. Document it as a follow-on, don't block on it.

### Gap 3 — Map materialization (BUILD, P1)
**Worth it: yes — unlocks goal (b) on the deployed portal.** The winners decoder (`winners.v3`) already exposes the full capability axis with `winner_type` filterable to `subawardee`; the ONLY defect is the materializer feeds capability to primes only. **MVP = full build here:** change the rollup join so subawardee rows pull from `govcon_subawardee_capability_profiles` (sub_uei → winner_uei). The awards map (`awards.v1`, event grain) is **out of scope** — it's per-action and has no capability columns; adding them is a separate decoder bump with no goal traceability. **Blast radius: medium** — overwrite rebuild + redeploy-free (catalyst reads by URI, schema unchanged), but the decoder contract check + winners coord-rate must stay green.

### Gap 4 — catalyst_api drill-down endpoint (BUILD, P1)
**Worth it: yes — the only typed served path to goal (c).** Today every entity route keys off `entity_profile_gold` (prime spine); a sub with no prime profile 404s. **MVP = one new route** `/api/v1/entities/{uei}/subaward-profile` binding `govcon_subawardee_capability_profiles` (the capabilities, already assembled) + `contract_subaward` (the prime-contract history, point-lookup on `subawardee_uei`). **Blast radius: medium** — new URI const (config.py) + new lookups (lance_store) + new model (models.py) + route (main.py) + **catalyst redeploy** (hardcoded URIs). No decoder, no map.

### Gap 5 — gtm_mcp ANN leg + typed sub tool (BUILD, P2, gated on Gap 1)
**Worth it: yes — the served form of goal (a).** A new typed tool `search_subawardee_capabilities` mirroring `search_govcon_scopes` exactly (filter→ANN over the new vectors). **Cut:** do NOT add a new sub-pivot leg to `capability.py` — the profiles `query` CLI already does the structured sub conjunction, and the raw-SQL audience path covers the rest. The one piece worth building is the vector tool. **Blast radius: small** — gtm_mcp auto-registers `govcon_sub_capability_vectors_90day` by name (no redeploy for discovery; redeploy only to ship the new tool fn). Apply the plan's `nprobes` fix (5-10% of partitions, not the 1.7% default).

**Net: BUILD 1 → 3 → 4 → 5. DEFER 2. Awards-map capability = cut.**

---

## 4. Per-component build spec (data + derived)

### 4.1 `govcon_sub_capability_vectors_90day` (Gap 1)

- **URI/grain:** `s3://data-sink/active/govcon_sub_capability_vectors_90day/` · grain `(subawardee_uei, description_chunk_ix)`.
- **Schema:** REUSE frozen `sub_capability_vectors_schema()` (`govcon_gtm_schemas.py:262-276`) verbatim — no change. `chunk_id = sha256(subawardee_uei|chunk_ix|text)[:24]`, `embedding fixed_size_list<float32>[1024]`, `model_id`/`model_revision` pinned per row, `n_source_subawards` = dedup provenance count.
- **New module:** `pipelines/sam_gov/build_sub_capability_vectors.py` — text-stamp builder only (NOT the embedder; the embed harness owns vectors). CLI: `build` (dedup→chunk→overwrite text+NULL embedding) · `verify` · `verify --content-hash`.
- **Source query (DuckDB over a bounded `contract_subaward` scan — the profile builder's `_duck()` pattern):**
  ```sql
  WITH pairs AS (   -- per-(sub, distinct-description); reject per-UEI concat (1,080 subs truncate)
    SELECT subawardee_uei AS sub_uei, subaward_description AS d,
           count(*) AS n_source_subawards
    FROM contract_subaward
    WHERE subawardee_uei IS NOT NULL AND subaward_description IS NOT NULL
      AND length(trim(subaward_description)) > 0
    GROUP BY 1, 2),
  -- ≤1,200-char chunking: 99% of descriptions are one chunk (p99=1,211); the 671 over-1,200
  -- descriptions split on whitespace at ≤1,200 (overlap-free — these are flat work-item lists,
  -- not prose; no boundary-quote validation needed). chunk_ix is the per-sub ordinal.
  chunked AS (SELECT sub_uei, chunk_text, n_source_subawards
              FROM pairs, LATERAL split_to_1200(d) AS chunk_text),  -- impl in Python, see note
  ord AS (SELECT sub_uei, chunk_text,
                 row_number() OVER (PARTITION BY sub_uei ORDER BY length(chunk_text) DESC, chunk_text)-1
                   AS description_chunk_ix,
                 n_source_subawards
          FROM chunked)
  SELECT sub_uei AS subawardee_uei, description_chunk_ix,
         /* sha256(...|...|...)[:24] in Python */ chunk_text AS text,
         length(chunk_text) AS char_len, n_source_subawards
  FROM ord;
  ```
  **512-token ceiling handling:** chunk at ≤1,200 *chars* (≈300 tokens) so no chunk approaches the 512-token wall; the 671 over-1,200 descriptions split, the other 65,687 pass through whole. Deterministic ordering (`length DESC, text`) makes `chunk_ix` and the content-hash stable across re-runs.
- **Embedding harness reuse (no new embed code):** run `sam_attachment_embed_modal.py`'s exact recipe against this sink — add `"sub_caps": "…/govcon_sub_capability_vectors_90day/"` to its `SINKS` map (or a 3-line sibling). Pinned **`BAAI/bge-large-en-v1.5` rev as `model.revision`**, fp16 inference, passages without instruction, L2-normalize float32 at write, `merge_insert("chunk_id").when_matched_update_all()`. Worklist `embedding IS NULL` (no char_len filter). **IVF_PQ params lifted verbatim from the live scope build:** `metric="cosine"`, `num_sub_vectors=64`, `num_partitions = round(sqrt(n)) ≈ 321` (for ~103K rows), `accelerator="cuda"` with CPU-kmeans fallback (`_modal.py:124-132`). Resume = re-select `embedding IS NULL`.
- **Scalar indices:** **BTREE(`subawardee_uei`)** only — it's the sole point-lookup/prefilter key. **NO BTREE(`chunk_id`)** (anti-pattern #7 — nothing point-looks-up it; re-arms #3177 on every refresh; the egress chain resolves chunk_id by string-split, exactly as the scope sink). No BITMAP needed (no low-cardinality filter axis on this sink).
- **Idempotency/CUI:** overwrite-snapshot text build → single-committer embed under `SinkCommitLease` → `assert_schema` before first commit. **CUI invariant: `subaward_description` is the sub's own SAM subaward report — sub-self-reported, NOT solicitation CUI — safe to embed and surface** (state it; the profile builder's docstring already asserts this). No doc_scope-derived field enters this sink.
- **DoD:** rows ≈ 103,223; `embedding IS NULL == 0`; index list = `[embedding_idx (IVF_PQ), subawardee_uei_idx (BTREE)]`; ANN spot-check "substation electrical upgrade" returns electrical sub descriptions with sane cosine distances; content-hash stable on re-run.

### 4.2 Winners map subawardee leg (Gap 3)

- **URI/grain:** unchanged — `usaspending_winners_map_serving`, `(winner_uei, winner_type)`. Overwrite rebuild; schema unchanged (columns already exist).
- **Source change:** register `govcon_subawardee_capability_profiles` and add a `cap_sub` rollup CTE keyed on `sub_uei`; LEFT JOIN it onto subawardee rows (`k.winner_type = 'subawardee'`) the way `cap`/`cap_tags`/`cap_keys` join onto prime rows. The sub profile already carries `has_extracted_scope`, `requires_clearance`, `requires_cmmc`, `req_clearance_level_max`, `capability_tags`, `top_labor_categories` → map them to the serving columns directly (1:1, no recompute). `covered_award_count` ← `n_scope_solicitations`; `covered_award_keys` ← `source_notice_ids` (capped 50).
- **DoD:** subawardee rows with `has_extracted_scope=true` rises from 0 toward ~4,220 (the profile's enriched count, intersected with the 90-day window); decoder contract green; coord-rate unchanged; `winner_type='subawardee' AND capability_tag has electrical_systems` returns a non-empty served set.

### 4.3 catalyst_api subaward-profile route (Gap 4)

- New URI const `SUBAWARDEE_CAPABILITY_PROFILES_URI` in `config.py` (default `s3://data-sink/active/govcon_subawardee_capability_profiles/`). Add to `_SURFACE_DATASETS` for boot reachability.
- New lance_store lookups: `subaward_profile_by_uei(uei)` — BTREE point-lookup on `sub_uei` (index live); `subaward_history_by_uei(uei, limit)` — point-lookup on `contract_subaward.subawardee_uei` (projecting prime_award_unique_key/piid, prime_awardee_name/uei, subaward_amount, subaward_description, subaward_action_date, prime_award_naics_code, usaspending_permalink), sorted by amount desc, hard-capped.
- New model `SubawardProfileResponse` in `models.py` (camelCase wire): `capabilities` block (from the profile row) + `subaward_history` list + `teaming` (from profile's `teaming_*` lists) + `poc` (from profile's `poc_*`).
- New route in `main.py`. **CUI invariant:** the profile carries only structured/controlled-vocab + sub-self-reported text — no solicitation chunk verbatim; `source_chunk_ids`/`source_resource_ids` are IDs (drill pointers), safe. Assert the response model never selects `evidence_quote`/`requirement_detail` (they aren't on the profile schema — egress-safe by construction).
- **DoD:** `GET /entities/YZTLALWM4UC7/subaward-profile` returns capabilities + ≥1 prime-contract history row + teaming + POC for a known sub; 404 for an unknown sub_uei.

### 4.4 gtm_mcp sub vector tool (Gap 5, gated on 4.1)

- New module `apps/gtm_mcp/src/tools/sub_capability.py` mirroring `govcon.py::search_govcon_scopes`: `DATASET = "govcon_sub_capability_vectors_90day"` (auto-registered by name), `_vector_index_present` guard, `embeddings.embed_query` (same BGE model — already pinned), `scanner(filter=<sub_uei/naics prefilter or None>, prefilter=True, nearest={column:"embedding", q, k, nprobes})`. **Apply the plan's nprobes fix: `nprobes ≈ max(20, round(0.07 * num_partitions))`** (~22 for 321 partitions) so recall isn't starved at the 1.7% default. `register(mcp)` in `main.py`. Until the vectors are embedded the tool returns `vector_index_absent` (the scope tool's graceful-degradation pattern) — never a brute-force scan.
- **DoD:** `search_subawardee_capabilities("aircraft avionics depot repair")` returns ranked sub_ueis with `_distance` + the matched description text.

---

## 5. Serving change-points table

| File:line | Current | Change | Why |
|---|---|---|---|
| `pipelines/sam_gov/build_sub_capability_vectors.py` | does not exist | NEW text-stamp builder (`build`/`verify`/`verify --content-hash`); dedup→≤1,200 chunk→overwrite text+NULL embedding | Gap 1 — the vectors dataset has a frozen schema but no builder |
| `pipelines/sam_gov/sam_attachment_embed_modal.py:20-24` (`SINKS`/`UNMARKED`) | scope + unknown only; `UNMARKED` filters on `content_marking` | add `sub_caps` sink; for it, worklist = bare `embedding IS NULL` (sub text has no `content_marking` column — it's not CUI) | Gap 1 — reuse the GPU embed/IVF_PQ harness; sub sink has no marking bracket |
| `pipelines/serving/materialize_winners_map.py:46-47, 110-114` | `PROFILES_URI` = award profiles; registers only `prof` | add `SUB_PROFILES_URI` + register `sub_prof` scan (sub_uei + capability columns) | Gap 3 — feed the subawardee leg |
| `pipelines/serving/materialize_winners_map.py:163-225` (rollup CTEs + final SELECT) | `cap*` CTEs join `ON winner_type='prime_recipient'`; sub rows get `coalesce(...,false)` defaults | add `cap_sub` CTE on `sub_uei`; LEFT JOIN onto sub rows; `coalesce(prime-rollup, sub-rollup, default)` in final SELECT | Gap 3 — 1,689 sub rows currently all `has_extracted_scope=false` |
| `apps/catalyst_api/src/config.py:90` (after `ENTITY_AWARD_LINES_GOLD_URI`) | no sub-profile URI | add `SUBAWARDEE_CAPABILITY_PROFILES_URI` (env-overridable, default active root) | Gap 4 — hardcoded-URI consumer; **redeploy required** |
| `apps/catalyst_api/src/lance_store.py:722-731` (`_SURFACE_DATASETS`) | 8 surfaces | add `subawardee_capability_profiles` | Gap 4 — boot reachability + /healthz visibility |
| `apps/catalyst_api/src/lance_store.py` (new fns near `entity_award_lines_by_uei`) | none | add `subaward_profile_by_uei(uei)` (BTREE sub_uei) + `subaward_history_by_uei(uei, limit)` (BTREE on `contract_subaward.subawardee_uei`) | Gap 4 — the two point-lookups behind the route |
| `apps/catalyst_api/src/models.py` (new classes after `EntityDossierResponse`) | none | add `SubawardProfileResponse` + `SubawardHistoryItem` (camelCase via `_Model`) | Gap 4 — response contract |
| `apps/catalyst_api/main.py:257-352` (entity routes block) + `_info()` endpoints map | dossier/overview/active-contracts (prime-only) | add `GET /api/v1/entities/{uei}/subaward-profile` (`require_operator`, `_require_uei`) | Gap 4 — the served drill-down |
| `apps/gtm_mcp/src/tools/sub_capability.py` | does not exist | NEW `search_subawardee_capabilities` tool (mirror `govcon.py`; nprobes=7%) | Gap 5 — fuzzy-X over sub work; **redeploy to ship the fn** (dataset auto-registers by name) |
| `apps/gtm_mcp/main.py:63` (after `govcon.register`) | govcon vector tool only | add `sub_capability.register(mcp)` | Gap 5 |
| `pipelines/serving/materialize_sub_targeting.py:168-204` | deterministic labor-token LIKE matcher | **NO CHANGE NOW** (Gap 2 deferred) | not on the goal path; documented follow-on |

**Redeploy implications:** gtm_mcp resolves datasets by a runtime `name→uri` registry that re-lists `active/` on restart — `govcon_sub_capability_vectors_90day` becomes queryable with **no code change** once committed; only shipping the new tool *function* needs a redeploy. catalyst_api uses **hardcoded URIs in config.py** — the new sub-profile URI + route require a **catalyst redeploy**. The winners map rebuild needs **no redeploy** (catalyst reads it by URI; schema unchanged) but staleness is bounded by the `CATALYST_DATASET_TTL_SECONDS` (300s) warm-handle TTL.

---

## 6. Build-order DAG

```
                    ┌─────────────────────────────────────────────┐
   [DONE] profiles ─┤ govcon_subawardee_capability_profiles (v49)  │
   [DONE] teaming  ─┤ govcon_teaming_edges_90day (v4)              │
   [DONE] embed     │ sam_attachment_embed_{90day,modal}.py        │
          harness  ─┴──────────────┬──────────────────────────────┘
                                   │
   ┌───────────────────────────────┼───────────────────────────────┐
   │ Gap 3 (winners sub leg)       │ Gap 4 (catalyst route)         │  ← both depend ONLY on
   │  materialize_winners_map.py   │  config+lance_store+models+main│    the DONE profiles table;
   │  → rebuild + verify           │  → redeploy catalyst           │    fully independent, parallel
   └───────────────────────────────┤                                │
                                   │                                 │
   Gap 1 (vectors) ────────────────┼─→ Gap 5 (gtm_mcp ANN tool)      │  ← Gap 5 HARD-depends on Gap 1
   build_sub_capability_vectors.py │   sub_capability.py + redeploy  │    (vector_index_absent until embedded)
   → embed (GPU) → IVF_PQ          │                                 │
                                   │                                 │
   Gap 2 (targeting ANN) ── DEFERRED (reads Gap 1 vectors; no consumer demands it yet)
```

- **Independent / parallel:** Gap 1, Gap 3, Gap 4 share no edges — all read the already-built profiles + subaward feed. Run them concurrently.
- **Sequenced:** Gap 5 must follow Gap 1 (the tool returns `vector_index_absent` until the IVF_PQ index exists).
- **Critical path to goal-state:** Gap 1 → Gap 5 is the longest chain (one GPU embed run + redeploy). Gaps 3/4 land independently and deliver goals (b)/(c) without waiting on the embed.

---

## 7. Risks / blockers

1. **GPU embed (Gap 1).** ~103K chunks at ~99 passages/s on an A10G ≈ **~17 min** (vs. the scope sink's 1.5M); A100 faster. Cost <$2. MPS-local fallback ~2.4h. Risk is low — the harness is proven and resumable (`embedding IS NULL`). Single-committer lease binds the writer; no other pipeline writes this sink.
2. **The 512-token truncation (Gap 1).** Confirmed real but narrow: only 671/66,358 descriptions (1.0%) exceed 1,200 chars; per-UEI concat (rejected) would have truncated 1,080 subs. The per-(sub,desc) ≤1,200 chunking sidesteps it entirely. Residual risk: the 671 over-1,200 descriptions are flat material-line lists (verified in the live `query` output, e.g. Sterling Computers' SKU dump) — whitespace-split is lossless, no prose boundary-quote concern.
3. **ANN prefilter recall starvation (Gap 5).** The scope tool's `nprobes=20` is 1.7% of *its* 1,162 partitions; the sub sink has only ~321, so `nprobes=20` is ~6% (acceptable) but pin the formula `max(20, round(0.07*partitions))` per plan §9 to keep `prefilter=True` from starving when a tight `subawardee_uei`/NAICS prefilter shrinks the candidate set.
4. **winners.v3 decoder gates capability behind `has_extracted_scope=true` (Gap 3).** `compile_map_filter` auto-ANDs `has_extracted_scope=true` whenever a `gated` field is present. So sub rows MUST carry `has_extracted_scope=true` or the capability filter silently excludes them — exactly the current state (0 enriched subs). Gap 3's rollup is what makes the served sub-capability filter non-empty. Verify the enriched-sub count post-build before any demo.
5. **Map-serving redeploy/TTL (Gap 3/4).** Winners rebuild is visible to catalyst within ~`CATALYST_DATASET_TTL_SECONDS` (300s) via stale-while-revalidate — no redeploy. The catalyst sub-profile **route** needs a redeploy (new URI/route in a hardcoded-URI service); boot is fail-closed on the auth token and runs the decoder contract check — sequence: commit profile is already live, so just ship the code.
6. **Single-committer lease contention.** The embed harness holds `SinkCommitLease` on the vectors sink; it's a net-new dataset with no live writer, so contention is nil. The profiles overwrite-rebuild (if re-run) must not race the vector build — but the vector build reads `contract_subaward`, not the profiles, so they're independent.
7. **Window semantics (Gap 1).** `contract_subaward` is the rolling 90-day fresh feed. The vectors inherit that window. A sub whose only descriptions fell out of the 90-day window has no vector. If goal (a) wants the full 5y sub history, source the vector text from `usaspending/subaward_search` (9.8M rows) instead of `contract_subaward` — **operator decision below.**

---

## 8. Open decisions for the operator

1. **Vector text source = 90-day `contract_subaward` (199,901 rows, the demo window) vs. 5y `usaspending/subaward_search` (9,801,723 rows, the full sub resume).** The 90-day feed is the proven substrate and matches the teaming-edge window the profiles already use; the 5y corpus gives deeper "what has this sub ever done" recall but is 49× larger (embed cost/time scale linearly; ~50 GB vectors). **Recommendation: ship 90-day first** (it's what the profiles + map + endpoint already key to), widen to 5y only if recall proves thin. This is a window-as-data choice — re-embedding from the wider source is an overwrite, not a new table.
2. **Ship the gtm_mcp `search_subawardee_capabilities` tool (Gap 5) vs. stay raw-SQL.** The structured sub conjunction is already covered by the live profiles `query` CLI + the audience raw-SQL path; the *only* thing the typed tool adds is open-vocabulary semantic recall that keyword SQL provably can't do (same rationale as `search_govcon_scopes`). **Recommendation: build it** — it's the served form of goal (a) and the marginal cost is one mirror-module + redeploy. Skip only if no agent/consumer will issue free-text sub searches.
3. **Defer the targeting ANN edge upgrade (Gap 2) — confirm.** The live deterministic matcher works and no goal-state surface reads `sub_targeting`. **Recommendation: defer** until a consumer needs award×sub ANN recall; revisit after Gap 1 lands (the vectors it needs will already exist). Confirm this is acceptable or promote it to the build set.
