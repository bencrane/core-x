# GovCon Scope Processing & GTM Query Layer — Canonical Build Plan

**Date:** 2026-06-12 · **Status:** Authoritative — supersedes the three candidate plans; incorporates all red-team/reality-check findings and the completeness-critic P0–P2 fixes · **Ground truth:** live R2 probes 2026-06-12 + repo verification (paths cited inline)

---

## 0. Verdict

**Backbone: structured-extraction-first** (candidate "structured-first" ordering), grafted with the entity-graph plan's early spine repair and the hybrid plan's verified retrieval mechanics. Embeddings ship **last**, as a recall layer with consumers already attached. The reasons are structural, not stylistic: (1) the north-star query — "companies that do X and need A,B,C" — is a conjunction of hard predicates plus one fuzzy match, and conjunctions are cross-chunk by construction (clearance in Section H, labor categories in Section C); no ANN hit can witness A∧B∧C, and cosine is polarity- and threshold-blind. Spec §10 already codifies this ("exhaustive — NOT gated on a vector similarity query, which is lossy") and names the structured table THE PRODUCT. (2) The live portal (TRANSLATE→EXECUTE) consumes only enum-bounded scalars over indexed Lance — structured fields drop into the existing `/ask` box via a decoder bump; vectors cannot enter the request path (latency + CUI). (3) Outreach demands citations; structured rows carry `source_chunk_ids` + `evidence_quote` natively. (4) Both independent reality-checks confirmed this ordering survives contact with the system, while the hybrid plan's cache-hit key was provably uncomputable (the key contains an LLM output). Phase-5-before-Phase-4 inversion of spec §11 is legitimate and deliberate: §10 needs no vectors, and embedding before a consumer exists is the orphan-fragment failure this plan exists to prevent.

---

## 1. Canonical numbers (probed 2026-06-12 — use these, no others)

| Metric | Value |
|---|---|
| Chunks (scope / unknown / pricing) | 1,348,983 / 1,042,059 / 102,809 = 2,493,851 (embeddable 2,391,042; embedding 100% NULL) |
| Resources (document-disjoint across sinks) | 8,377 / 16,477 / 2,174 = 27,028 |
| LLM-lane input (scope-sink ∪ unknown lexicon-hit ∪ pricing) | 17,849 resources; **canonical Phase-2 set = that union ∪ Phase-1 regex-hit residue** (lexicon-miss unknown docs with regex hits join at Phase-1 close; measured, expected small) |
| Awards, inline scalar key, 3-sink union | 10,729 (7,191 UEIs); embeddable 2-sink union 10,214 (6,880 UEIs); scope-only 4,890 (3,807 UEIs) |
| Awards, manifest `award_keys` exploded | 35,002 over the 17,849-doc input; 38,039 over all text-covered resources — **4.1× the scalar join; the exploded grain is canonical** |
| Marked content (`content_marking` ≠ []) | 42,307 chunks / 433 resources / 345 awards (288 resources inside the LLM input; values degenerate `['unspecified']` only) |
| Coverage ceiling | ~0.96% of 1,119,355 trailing-90d prime awards have text; "no match" = no document, not no need |
| Subaward substrate | 130,011 distinct subawards / 25,450 subawardee UEIs; `subaward_description` 99.995% filled; POC coverage 89.3% (probed live; re-verify before any demo claim) |
| Text↔sub edge | award-grain (fanout): 320 awards; company-grain: ~380 text-covered prime UEIs with in-window sub reporting → ~14.6K subs (probe variance vs 575/17,377 is parent-UEI matching; **company-grain matching rule fixed now, not at build: roll to `recipient_parent_uei` where present, `recipient_uei` fallback** — re-derive the counts at build under that rule) |
| Chunk skew | chunks/award p50 26 / p99 3,948 / max 16,846; top-1% of awards = 25.5% of corpus |
| Bridge | `subawardee_solicitations_bridge` = 670 rows (grain defect); post-fix band 15–20K; hop-3 manifest does not exist |
| /ask latency | ~4.6s cold (one Opus TRANSLATE call); EXECUTE sub-second; cold Lance handle 1.5–3.5s (not 60s) |

**Coverage metric, defined once:** "awards reachable by extracted requirements" = distinct exploded `award_keys` of requirement-bearing resources, resolved against `contract_prime_txn` (resolution % measured at every profile build — the existing 100% proof covers the scalar key only). Quote 35,002 as the ceiling, 10,729 as the inline-key floor; never mix.

---

## 2. Artifact map

| Artifact | Location | Status |
|---|---|---|
| Extractor module | `pipelines/sam_gov/sam_labor_demand_extract_90day.py` | **Net-new** (spec §17 fixed name; extends Stage-4 ledger/`_ensure_dataset`/merge-insert patterns; LLM batch harness runs on the Modal orchestrator, same as Stage-4) |
| `govcon_award_requirements_90day` | `s3://data-sink/active/govcon_award_requirements_90day` | **Net-new** (widening of spec §3.6) |
| `govcon_labor_demand_90day` | `s3://data-sink/active/govcon_labor_demand_90day` | **Extends spec §3.6 verbatim** (derived from same pass) |
| `govcon_requirements_extract_ledger_90day` | `s3://data-sink/active/govcon_requirements_extract_ledger_90day` | **Net-new** (mirrors Stage-4 event ledger; carries `batch_id` for Batch-API resume) |
| `govcon_award_capability_profiles_90day` | `s3://data-sink/active/govcon_award_capability_profiles_90day` | **Net-new** (overwrite-mode build) |
| Winners map v2 columns | `s3://data-sink/active/usaspending_winners_map_serving` via `pipelines/serving/materialize_winners_map.py::_assemble` | **Extends** (overwrite build — schema evolution free); decoder bump `winners.v1→v2` in BOTH `apps/catalyst_api/src/map_decoders.py` and `apps/edge_api/src/map_decoders.py`; parity test `apps/edge_api/tests/test_map_ask.py` |
| Bridge fix + hop-3 manifest | `pipelines/usaspending/subawardee_solicitations.py`; `s3://data-sink/active/subawardee_solicitations_manifest` | **Fix + net-new materialization** |
| `govcon_teaming_edges_90day` | `s3://data-sink/active/govcon_teaming_edges_90day` | **Net-new** (sources `usaspending/subaward_search` 5y + `subawardee_work_profile`; frozen schema in Phase 0) |
| `govcon_sub_targeting_90day` | `s3://data-sink/active/govcon_sub_targeting_90day` via `pipelines/serving/materialize_sub_targeting.py` | **Net-new** (snapshot-overwrite; frozen schema in Phase 4) |
| Embed writer | `pipelines/sam_gov/sam_attachment_embed_90day.py` | **Net-new** (executes spec §9 verbatim; model pinned by `apps/gtm_mcp/src/embeddings.py`) |
| `govcon_sub_capability_vectors_90day` | `s3://data-sink/active/govcon_sub_capability_vectors_90day` | **Net-new** (frozen schema in Phase 5) |
| Console query tools | `apps/gtm_mcp/src/tools/capability.py`; edits to `apps/gtm_mcp/src/tools/govcon.py` | **Net-new + extends.** `capability.py` implements §3's legs on a phase schedule: hard-predicate queries (live at Phase 1), grain-ladder conjunction to award/UEI grain (Phase 2), sub pivot via `govcon_sub_targeting_90day` + `sam_pocs` join (Phase 4), fuzzy-X ANN leg (Phase 5). `govcon.py` edits — sink parametrization, `nprobes`, the marking filter leg — activate at Phase 5 |

---

## PHASE 0 — Substrate truth & pre-flight hardening (no LLM, no GPU)

**Ships:** sound CUI markings on the chunk rows themselves, dedup-verified sinks, frozen schemas committed, and a repaired prime→sub spine usable for manual outreach immediately.

1. **Uniqueness pre-flight.** Per sink: assert `count(*) == count(DISTINCT chunk_id)` and the §12 `ledger.n_chunks` reconcile. On violation, run `phase_finalize` — but only after item 2.
2. **Patch `phase_finalize` (`sam_attachment_extract_90day.py:1596–1634`).** Today it dedups via `take()` + `mode="overwrite"` — once vectors exist that materializes ~10 GB into one Arrow table and **drops every index**. Change to row-id `delete()`-based dedup; add a hard assert "never overwrite a sink that has a vector index"; add a per-sink single-committer lease so the extractor, embed writer, and finalize can never commit concurrently (spec D3).
3. **Full-body marking pre-pass (discrete, before any external call ever).** Re-run the marking regexes (`sam_attachment_extract_90day.py:120–141`) over re-assembled full text **including pricing `cells`** for all 27,028 resources; use uppercase-only acronym matching for `\bEAR\b`/`\bCUI\b` to limit full-body false positives (over-marking is safe but shrinks the API-eligible pool). **Back-propagate promotions onto the chunk rows** via `merge_insert` on chunk_id — the key is still unindexed, so the #3177 window is open; this is the only moment this write is cheap and safe. Chunk-level `content_marking` is the **single enforcement point** for every egress decision; the ledger's `marking_full_body` is **audit provenance** of how each promotion was decided — not a second gate. Without the write-back, every existing chunk-level gate (gtm_mcp, future portal evidence) carries a permanent false-negative hole.
4. **Freeze schemas.** Commit explicit pyarrow schemas for every new sink via Phase-0-style existence-guarded `_ensure_dataset` — **every column either lane will ever write, nullable, day one** (the `sensitivity→content_marking` burn class: Lance rejects type changes on append; `_ensure_dataset` silently no-ops on existing datasets, so a `ds.schema == FROZEN_SCHEMA` assert runs on every open as the only drift detector). The frozen set is exactly the column/type tables in this document: Phase-1 requirements, Phase-2 profiles, Phase-0 teaming edges, Phase-4 targeting, Phase-5 sub-capability vectors, §5 ledger. `confidence` is `float32` everywhere; regex rows write `1.0`, never a string.
5. **Spine repair (parallelizable, additive).** Fix `subawardee_solicitations.py:346`: partition by `(subawardee_uei, prime_award_unique_key, subaward_number)` (fix both raw and `solnum_normalized` joins); elect the highest `base_type` tier **with attachments** (pure tier ranking picks an attachment-empty notice ~14% of the time). Materialize `subawardee_solicitations_manifest` at `(notice_id, resource_id)` grain — no `rn=1` sub collapse; UEI mapping stays many-to-many in the bridge — with a JSONL checkpoint + resume (the current code holds everything in memory). Build `govcon_teaming_edges_90day` (dollars/count from the 5y `usaspending/subaward_search` corpus — the 90-day `contract_subaward` feed is a reporting frontier and cannot source "5y" fields; dedup descriptions to 130,011 distinct subawards; rebuild chained immediately after any `subawardee_work_profile` rebuild).

### `govcon_teaming_edges_90day` (grain `(prime_uei, sub_uei)`, overwrite rebuild, frozen)

| Column | Type | Notes |
|---|---|---|
| `prime_uei`, `sub_uei` | string | grain; **BTREE both** |
| `prime_name`, `sub_name` | string | latest-reported |
| `edge_dollars_5y` | float64 | sum over 5y `subaward_search` |
| `edge_count_5y`, `distinct_awards_5y` | int32 | |
| `first_action_date`, `last_action_date` | date32 | recency for outreach framing |
| `top_naics` | string | mode over edge subawards |
| `run_id`, `built_at` | string, timestamp | |

**Definition of done / anti-orphan gate:** uniqueness asserts green; marking write-back row count reconciles against the pre-pass promotion list; all new datasets exist with frozen schemas and zero rows; bridge row count in the 15–20K band with grain-uniqueness assert; manifest rows > 0 with distinct-notice coverage ≈ bridge notices. Consumer attached: "prime P's habitual sub bench + POCs" runs in gtm_mcp today.

---

## PHASE 1 — Deterministic requirement extraction (regex lane)

**Ships:** queryable hard-requirement rows with mechanical citations, immediately usable in the operator console.

**Input: all 27,028 resources — all three sinks, ALL unknown docs** (regex is ~free; gating on `lexicon_hit` here would silently drop non-labor requirements in the 9,179 lexicon-miss docs and cut award coverage; the lexicon gate applies to the LLM lane only). Per resource: re-assemble text in `chunk_ix` order using **overlap-aware suffix→prefix matching** — the chunker whitespace-snaps and `.strip()`s, so the overlap is ~180 chars, never exactly 180; fixed-width stripping corrupts text and later mass-quarantines good LLM rows. Build a char-offset→chunk_id interval map during reassembly; that map is how regex spans populate `source_chunk_ids`/`evidence_quote`. Pricing lane: read the doc-level `cells` grid **once per resource_id** and validate pricing evidence against `cells`, not `text`.

Extractors: clearance levels, cert names (CMMC/ISO/EM 385/AS9100), MIL-STD/NIST/UFC/ASTM citations, FTE/headcount, wage-determination numbers, PoP dates, state licenses. Rows write `confidence=1.0`, `validated=true` (evidence is mechanically derived). **Regex-lane idempotency (same discipline as the LLM lane):** before each resource's merge, scoped `delete("resource_id == '<id>' AND extractor LIKE 'regex:%'")` on the requirements sink and `delete("resource_id == '<id>'")` on `govcon_labor_demand_90day` — extractor evolution shifts `value_norm` (→ new `requirement_id`) and any chunking/reassembly change shifts `demand_id`; pure content-hash merge strands stale orphans (anti-pattern #3 applied to its own lane). Same pass emits `govcon_labor_demand_90day` (§3.6 exact; `demand_id` ordinal assigned deterministically by rank over `(labor_category_norm, first chunk_ix)` — the spec's `<n>` is otherwise a latent merge bug) and computes `lexicon_hit_fullbody` for scope docs (the §12 acceptance gate; `lexicon_hit` exists only on the unknown sink today).

### `govcon_award_requirements_90day` (requirement grain, merge_insert sink, frozen)

| Column | Type | Notes |
|---|---|---|
| `requirement_id` | string | `sha256(resource_id\|requirement_type\|value_norm)[:24]` — content hash, never ordinal |
| `resource_id`, `notice_id`, `solicitation_number`, `naics_code` | string | join keys |
| `contract_award_unique_key` | string | inline primary key — **convenience only; profile joins use manifest explode** |
| `requirement_type` | string | enum: certification·clearance·labor_category·standard_compliance·license·equipment_capability·past_performance·deliverable·insurance_bonding·staffing_constraint·vehicle_constraint |
| `requirement_value`, `requirement_detail` | string | value normalized; `clearance_level` enum-locked; **`requirement_detail` NULL at write for marked resources** — it is free text that can carry verbatim marked content, and gtm_mcp serves requirement rows directly, so the gate is write-side, same rule as `evidence_quote` |
| `mandatory` | bool | "shall" vs "should" |
| `headcount` | int32? | |
| `clearance_level` | string? | enum-locked |
| `pop_start`, `pop_end` | date32? | |
| `place_of_performance_text` | string? | doc-stated |
| `wage_floor` | float64? | §3.6 superset requirement — without it the batch-§10 promise breaks |
| `source_chunk_ids` | list\<string\> | grounding, non-negotiable |
| `evidence_quote` | string? | ≤300 verbatim; **NULL for marked resources** (verbatim CUI text never leaves the requirements row for serving) |
| `validated`, `marked_resource`, `coverage_truncated` | bool | row-level trust boundary |
| `extractor`, `extractor_version` | string | `regex:<id>` or `model@prompt_hash` |
| `confidence` | float32 | regex = 1.0 |
| `run_id`, `created_at` | string, timestamp | |

Indices after Phase-1 merges settle: BTREE(`resource_id`, `contract_award_unique_key`), BITMAP(`requirement_type`, `clearance_level`, `mandatory`, `validated`). `requirement_id` stays unindexed until Phase-2 merges complete (#3177); run `optimize_indices` after every later merge wave or queries silently degrade to scans on new fragments.

**DoD / gate:** every input resource terminal in the ledger's regex lane; sampled evidence substring-asserts green; consumer attached (gtm_mcp answers "awards requiring TS-SCI + CMMC L2 in NAICS 5415" with quotes).

---

## PHASE 2 — LLM lane + AwardCapabilityProfile

**Ships:** the complete "need A,B,C" leg — scope summaries, labor categories, capability tags, residual-null fill — rolled to award grain.

**Input (one set, stated once, supersedes any other phrasing):** all scope-sink resources ∪ unknown lexicon-hit ∪ pricing ∪ Phase-1 regex-hit residue = the canonical 17,849 plus the measured regex-hit lexicon-miss unknowns. Scope-sink docs enter **unconditionally** — SOW/PWS documents are the substance; gating them on lexicon or regex hits would strip `scope_summary`/`capability_tags` from exactly the "do X" leg this phase exists to ship. Temperature 0, schema-constrained JSON, enums locked, prompt hash in `extractor`. **Idempotency mechanism (pick one, this is it):** scoped `ds.delete("resource_id == '<id>'")` for the resource's prior LLM-lane rows before each merge — LLM `value_norm` drifts across runs, so content-hash ids alone cannot prevent stale-row accumulation; the delete-then-merge plus ledger run stamping makes the crash window between requirements-write and ledger-write harmless. Per-resource token budget caps (top-1% of awards = 25.5% of corpus); p99 docs exceed the 200K context — **sliding-window is mandatory for the tail**, merged on the content-hash key, `coverage_truncated=true` when capped, surfaced in every disclaimer denominator.

**Anti-hallucination gate (hard):** post-pass validation asserts `evidence_quote` substring-matches the **concatenation of cited `source_chunk_ids` in chunk_ix order using the same shared reassembly function** (boundary-straddling quotes are otherwise systematically false-quarantined). Failures → `validated=false` + ledger quarantine; never silently kept. **Run gate: ≥98% pass rate per run or the run is quarantined wholesale.** Consumers filter `WHERE validated` — the row flag is the trust boundary; the ledger is ops signal.

**CUI routing:** marked resources (288 in-input head-marked + Phase-0 promotions) → self-hosted model only, or skipped with ledger `marked_local_only`; their rows carry `marked_resource=true` and NULL `evidence_quote`/`requirement_detail`. The unmarked remainder is **one named operator policy decision, signed once**: posture "SAM.gov public attachments are public" → commercial Batch API; posture "absence ≠ public" → everything local. Residual false-negative surface (image-rendered markings, phrasing outside the 7-regex set) survives the full-body re-scan — the policy call is a risk acceptance, not a data-proven safe set.

**Cost (honest):** ~600–710M input / ~15–35M output tokens. Haiku-4.5 Batch ($0.50/$2.50 per MTok) ≈ **$350–450**; Sonnet-class batch ≈ $1.3K; a rented A100/H100 running an 8B–32B instruct model also clears the input set and collapses the policy question. Batch mechanics: 100K requests / 256 MB per batch → ~10+ batches. **Batch harness + crash-resume:** the submit/poll/fetch loop runs on the Modal orchestrator (the Stage-4 pattern this extractor extends); the ledger carries `batch_id` per resource and `llm_state` passes through `submitted → results_fetched → done` — a crash between submit and fetch (the dominant wall-clock window) resumes by polling stored `batch_id`s, **never resubmitting**; re-running without batch tracking double-pays.

### `govcon_award_capability_profiles_90day` (award grain, overwrite build)

| Column | Type | Source |
|---|---|---|
| `contract_award_unique_key` | string | grain key — **populated by exploding manifest `award_keys[]` per resource, never the inline scalar** (4.1× coverage) |
| `recipient_uei/name/parent_uei`, `naics_code`, `product_or_service_code`, `type_of_set_aside`, `awarding_agency_name`, PoP fields, `pop_start/end`, value fields | per txn schema | `contract_prime_txn` collapsed to award grain (`row_number() OVER (PARTITION BY key ORDER BY last_modified_date DESC)=1`) — **never LLM-extracted** |
| `scope_summary` | string | LLM, validated rows only |
| `capability_tags` | list\<string\> | controlled vocabulary |
| `requires_clearance` bool · `req_clearance_level_max` string · `requires_cmmc` bool · `req_cert_tags` list\<string\> · `top_labor_categories` list\<string\> | | rollup of validated requirement rows |
| `n_requirements`, `n_validated` | int32 | |
| `source_resource_ids` | list\<string\> | drill-down pointer |
| `has_extracted_scope` | bool | true by construction |
| `marked_award`, `coverage_truncated`, `is_primary_target` | bool | |
| `txn_snapshot_run_id`, `built_at` | string, timestamp | window-decay tracking |

Indices: BTREE(`contract_award_unique_key`, `recipient_uei`); BITMAP(`req_clearance_level_max`, `requires_clearance`).

**DoD / gates:** evidence-validation ≥98%; **exploded distinct-award count ≥ inline-key count, asserted** (a result near 10,729 means the fan-out got dropped); award-key→txn resolution rate ≥ threshold with the consumed txn snapshot stamped (the rolling 90-day window decays the join — alert, don't silently shrink). **Degraded north-star gate, run verbatim before this phase closes:** "companies tagged `electrical_systems` under awards requiring SECRET clearance + CMMC L2" returns a non-empty award/prime set with citations — the controlled-vocab degraded form of "do X" is demonstrable now, not merely architecturally reachable after embeddings.

---

## PHASE 3 — Portal serving (the live demo)

**Ships:** the north-star filter on the Railway map `/ask`. Pure leverage: join profiles → `winner_uei` in `materialize_winners_map.py::_assemble` (overwrite build; keep the `replace=True` index recreation).

**New serving columns** (grain `(winner_uei, winner_type)`; aggregation semantics explicit): `has_extracted_scope` (= `bool_or` derived **from the manifest×sinks fanout at materialize time**, never from profile presence), `covered_award_count` int32, `covered_award_keys` list\<string\> (capped; the drill-down pointer that keeps every map dot explainable), `req_clearance_level_max` (max over covered awards), `requires_clearance` / `requires_cmmc` / `requires_cleared_trades` (per-cert/per-trade **booleans** — `FieldSpec` has no list-contains op; lists never enter decoders), `top_labor_category` (single controlled-enum value — this is what makes the electricians demo expressible; **aggregation rule: mode over validated labor_category rows across the winner's covered awards, ties broken by larger summed `headcount`, then lexicographic — deterministic**), `capability_tag_top` (same mode/tie rule over `capability_tags`).

**Hard safety mechanics (not prompt rules):**
- **EXECUTE-side injection:** in `apps/catalyst_api/src/lance_store.py::compile_map_filter`, if any `requires_*`/`req_*`/`top_labor_category` field appears in the compiled filter, deterministically AND `has_extracted_scope = true`. The TRANSLATE prompt rule is UX phrasing, not the safety mechanism — one LLM omission otherwise filters 1.25M winners through a 0.96% ceiling to an empty map, live.
- **Egress invariant, tested:** `evidence_quote`/`requirement_detail` (chunk-derived text) must never appear in either `map_decoders.py` — extend the edge↔catalyst parity test to assert it. Write-side NULLing for marked rows (Phase-1 schema) is the primary gate; this decoder assert is defense in depth. The portal egresses structured fields and metadata only.
- **Deploy order:** materialize the table with new columns first → read-probe verify → then merge the decoder bump (`winners.v2`, both files, parity test green). Reverse order fails the boot contract check and bricks `/ask`.
- **Window-mismatch pre-demo measurement:** `_assemble` filters primes by `action_date >= cutoff` while the corpus is keyed to `last_modified_date` — measure `COUNT(map rows WHERE has_extracted_scope)` overall and per-NAICS before any demo; widen the map window for text-covered awards if thin. Decided from the measured number, before the call.

**Acceptance test (restated to expressible columns):** `/ask` "construction winners in Texas with a covered award requiring Secret clearance and cleared electrical trades" → one TRANSLATE call (~4.6s cold) → deterministic indexed scan → dots. Demo framing is the coverage disclaimer: "of N winners in this NAICS, M had readable solicitation documents; here is what those documents demand."

**DoD / gate:** parity test green; boot contract green; measured map∩scope denominator recorded; no requirement-filtered query path lacks the injected gate.

---

## PHASE 4 — Subawardee targeting (closes the north star)

**Ships:** the outreach list — "do X" companies under primes that "need A,B,C," every hop cited.

The award-grain text↔sub edge is thin by reporting lag (320 fanout-grain awards) — **the lag is the GTM feature**: outreach lands in the window between prime win and sub selection. The company-grain pivot carries the query (parent-UEI rollup with direct-UEI fallback, per §1): text-covered prime UEIs → in-window subs ∪ 5y teaming edges (`govcon_teaming_edges_90day`) ∪ capability match.

**Edge semantics, deterministic v1 (every mechanism defined here, no vectors):**
- `direct_subaward` / `teaming_history` — structural edges (in-window subaward report; 5y teaming corpus). `matched_requirement_ids` = the covered award's validated requirement rows: the edge witnesses "worked under P," the requirement rows witness "P needs A,B,C." Both legs of the outreach line cite.
- `capability_match` — deterministic matcher: candidate qualifies iff **4-digit NAICS-family equality** (`sub_top_naics` prefix vs award `naics_code`) **AND ≥1 token hit** of the award's controlled-vocab `capability_tags` ∪ normalized `requirement_value` terms against `subaward_description`, evaluated as DuckDB FTS/ILIKE over the deduped 130,011 descriptions. `matched_requirement_ids` = exactly the requirement rows whose terms hit; zero-hit candidates never write a row — the Phase-4 DoD is therefore verifiable for the full enum. Phase 5 upgrades this edge's recall to ANN max-sim over `govcon_sub_capability_vectors_90day` (same enum value; the snapshot-overwrite rebuild swaps the implementation).

### `govcon_sub_targeting_90day` (grain `(contract_award_unique_key, candidate_sub_uei)`, snapshot-overwrite, frozen)

| Column | Type | Notes |
|---|---|---|
| `contract_award_unique_key` | string | grain; BTREE — award drill-in |
| `candidate_sub_uei` | string | grain; BTREE — sub view |
| `prime_uei`, `prime_name` | string | BTREE(`prime_uei`) — bench view (Path-C catalyst point-lookup precedent) |
| `edge_type` | string | enum: direct_subaward · teaming_history · capability_match |
| `edge_dollars_5y` | float64? | NULL for capability_match |
| `edge_count_5y` | int32? | NULL for capability_match |
| `last_subaward_action_date` | date32? | |
| `matched_requirement_ids` | list\<string\> | per edge semantics above; never empty |
| `sub_top_naics` | string? | |
| `capability_evidence` | string? | `subaward_description` excerpt — sub-self-reported text, not marked-doc egress |
| `poc_available` | bool | precomputed flag only |
| `built_at` | timestamp | |

Cap per-award fanout for mega-IDIQ umbrella docs. **POC payload path:** POC fields never materialize here — console outreach assembly joins `candidate_sub_uei → sam_pocs` at query time (89.3% fill, re-verified before demo); `poc_available` is the precomputed bool that keeps the bench view filterable without the join.

The outreach line writes itself and cites both sides: "Prime P won award W requiring A,B,C [requirement rows → quotes]; your firm did X for P / for primes like P [`subaward_description`, teaming edge]." Reach = `sam_pocs` (89.3%) + prime phone (98%) — re-verify both fills before they enter a live-call claim.

**DoD / gate:** every targeting row resolves `matched_requirement_ids` → validated requirement rows; bench view returns named POCs for a sampled prime; consumer attached (console outreach assembly). **End-to-end degraded north-star, run verbatim:** the Phase-2 gate query extended one hop — "companies tagged `electrical_systems` under awards requiring SECRET + CMMC L2 → sub bench with POCs" — returns named, citeable outreach targets.

---

## PHASE 5 — Embeddings (spec §9 verbatim) + sub-capability vectors

**Ships:** open-vocabulary "do X" retrieval and a recall backstop, into tools that already exist.

`pipelines/sam_gov/sam_attachment_embed_90day.py`: model **de-facto pinned** `BAAI/bge-large-en-v1.5` D=1024 (assert against the literal model id, not the env-overridable echo; persist model id + revision in the run ledger) — passages without instruction, queries with the BGE prefix, fp16 inference, L2-normalize float32 at write. **Worklist `WHERE embedding IS NULL` — no `char_len` filter** (the 228 degenerate chunks are noise; filtering them makes the §12 IS-NULL==0 gate unsatisfiable by construction). Embed **both** sinks (unknown carries 5,324 additional awards — roughly half the covered universe); all marked rows embed (gate is consumption, not embedding; rented-GPU vs strictly-local MPS is a named posture decision for the 41.5K marked chunks). Batching: iterate `ds.to_batches(columns=[...], filter="embedding IS NULL")` in natural scan order, 50–200K-row `merge_insert` batches with full-row sources (`when_matched_update_all` rejects subset schemas — smoke the exact merge on the `_smoke_*` sinks first). Single-committer lease from Phase 0 binds this writer; no extractor/finalize runs concurrently.

**Ordering gates, exact:** (1) re-assert chunk_id uniqueness + n_chunks reconcile; (2) assert `embedding IS NULL == 0` per sink; (3) `compact_files(1_048_576)` + `cleanup_old_versions(older_than=longest plausible warm-handle session)`; (4) IVF_PQ per sink: cosine, `num_sub_vectors=64`, partitions ≈1,162/1,021, `accelerator` on; (5) scalar index campaign: BTREE(`resource_id`, `contract_award_unique_key`), **BTREE/BITMAP(`naics_code`)** (the named prefilter — missing from §3.3), BITMAP(`header_class`), BITMAP(`lexicon_hit`) on unknown only, plus the INVERTED/FTS index on `text` if the lexical leg ships (sequenced in this same window or it full-scans). **No BTREE(chunk_id), ever** — nothing point-looks-up chunk_id (evidence resolves by string-split to resource_id), and indexing it re-arms #3177 against every future embed refresh. **Re-entry runbook in the module docstring:** new chunks (OCR, harvest growth) re-open IS-NULL per build → merge NULLs → `optimize_indices` → queries between append and reindex are correct-but-slower via brute-forced tail (acceptable, say so).

Also: `govcon_sub_capability_vectors_90day` — ≤1,200-char chunks of the **deduped 130,011** descriptions (one-row-per-UEI concatenation silently truncates at bge's 512-token ceiling), same pinned model, max-sim aggregation at query time, overwrite rebuild per window. This build upgrades Phase-4's `capability_match` edge from the deterministic v1 matcher to ANN max-sim (same `edge_type`; snapshot-overwrite swaps the targeting build).

### `govcon_sub_capability_vectors_90day` (grain `(subawardee_uei, description_chunk_ix)`, overwrite per window, frozen)

| Column | Type | Notes |
|---|---|---|
| `subawardee_uei` | string | BTREE |
| `description_chunk_ix` | int32 | grain |
| `chunk_id` | string | `sha256(subawardee_uei\|chunk_ix\|text)[:24]` |
| `text` | large_string | ≤1,200 chars, deduped descriptions |
| `char_len` | int32 | |
| `n_source_subawards` | int32 | dedup provenance |
| `embedding` | fixed_size_list\<float32\>[1024] | L2-normalized at write |
| `model_id`, `model_revision` | string | pinned |
| `run_id`, `created_at` | string, timestamp | |

Console wiring (`apps/gtm_mcp`): parametrize `govcon.py`'s hardcoded single sink and `nprobes=20` (1.7% of partitions — too low for recall legs; 5–10% for candidates); **add the `array_length(content_marking) = 0` filter leg** (verified Lance pushdown; `len()` is DuckDB-only) to any path feeding an external API — this leg does not exist in `_build_filter` today and is concrete new code. ANN runs only in warm-handle gtm_mcp; never catalyst, never the portal.

**Compute:** ~30–60 min on a rented A10G/4090 (<$10) or MPS overnight ($0); +9.8 GB vectors on R2; IVF_PQ build is IO-bound against R2.

**DoD / gate:** gates 1–5 green in order; ANN sanity probe ("substation electrical upgrade" → 236220-family awards with sane quotes); consumer attached before build (Phases 1–4 shipped).

---

## 3. The hybrid query mechanic (companies that do X and need A,B,C)

1. **Hard legs (A,B,C):** predicates over `govcon_award_requirements_90day` / profiles — `WHERE validated AND requirement_type='clearance' AND clearance_level='SECRET' AND mandatory` etc. Deterministic, citeable, polarity-correct. Never ANN. (`capability.py`, live at Phase 1.)
2. **Fuzzy X:** Phase-5 ANN over both chunk sinks, `scanner(nearest={...k≈1000}, filter=..., prefilter=True)` — prefilter=True or the filter silently starves recall. Award-attribute filters (agency, place) are **not on chunks**: derive `resource_id` IN-lists from manifest × `contract_prime_txn` in DuckDB and push those. Optional lexical FTS leg for exact tokens ("EM 385-1-1") hedges cosine's polarity blindness. Until Phase 5, X is expressible in degraded controlled-vocab form via `capability_tags` (the Phase-2/Phase-4 verbatim gates).
3. **Grain ladder:** chunk hits → `resource_id` → manifest `award_keys` explode → award (txn collapsed to award grain first) → `recipient_uei`. Per-award score = **max-of-top-m chunk similarities, never raw counts** — the 16,846-chunk monsters flood k otherwise. Conjunction enforced at award grain in DuckDB over the materialized Arrow table (`federal.py:120–137` scan-then-aggregate pattern): `GROUP BY contract_award_unique_key HAVING <A> AND <B> AND <C>`. (`capability.py`, Phase 2.)
4. **Sub pivot:** award set → prime UEIs → `govcon_sub_targeting_90day` (direct ∪ teaming ∪ capability match — deterministic v1 at Phase 4, ANN over sub-capability vectors at Phase 5) → `sam_pocs` join at query time. (`capability.py`, Phase 4.)
5. **Explainability (every result, non-negotiable):** matched-because = requirement rows with `evidence_quote` + `source_chunk_ids`; chain `chunk_id → resource_id → blob CAS → source document` (document-level + Ctrl-F-able quote — **page-level citation is explicitly out of contract**; no char→page map exists). Coverage disclaimer with the measured denominator, including the boilerplate hole (Section-H-style clauses dropped at triage are unreachable by any leg) and `coverage_truncated` docs. Until Phase 5's indices exist, chunk-sink point lookups are full scans — the drill-through to raw chunk text is post-Phase-5; the portal serves entirely from materialized fields.

---

## 4. Tradeoffs, cost, and the coverage fork

- **Deepen vs expand — recommendation: deepen.** The demo needs precision inside the 0.96% slice, not breadth. Expansion is a harvest problem (17.3% solicitation_identifier fill, 81,887 out-of-scope skips, 1,241 OCR-deferred, container expansion unrun) and a separate track; the fixed bridge adds new documents (attachments on ~700 resolved notices, 96% new vs the prime manifest) as the cheapest first expansion. New chunks re-enter via ledger `pending` states and the per-build IS-NULL gate.
- **Self-hosted vs API:** embeddings are self-hosted regardless of policy (model-pinned query path + CUI). LLM lane: marked → local always; unmarked → one signed policy decision ($350–450 Haiku batch vs local A100). Every external call carries the chunk-level marking predicate; `[]` ≠ public is permanent.
- **What this ordering forgoes:** open-vocabulary semantic search arrives last; early phases answer X only in controlled-vocab degraded form. Accepted — the structured legs are the demo and the spec's product; retrieval without verification was never outreach-grade.

## 5. Idempotency & re-run story

Ledger `govcon_requirements_extract_ledger_90day` (resource grain, merge on `resource_id`): **per-lane states** `regex_state` ∈ {pending, done, quarantined, failed} and `llm_state` ∈ {pending, **submitted, results_fetched**, done, quarantined, failed, truncated} (a resource can be regex-done and LLM-quarantined simultaneously), **`batch_id` string?** (Batch-API resume key — crash between submit and fetch re-polls stored ids, never resubmits), `marking_full_body`, `lexicon_hit_fullbody`, `n_requirements_*`, `validation_pass_rate`, `model`, `prompt_hash`, `extractor_version`, `run_id`, `completed_at`. Worklist = ledger predicate; crash-resume = re-select non-terminal. **Re-extraction = scoped delete-by-resource_id then merge in BOTH lanes** — LLM lane deletes its prior rows; regex lane deletes `extractor LIKE 'regex:%'` rows plus the resource's `govcon_labor_demand_90day` rows (content-hash ids dedupe within a run; the scoped delete is what kills cross-version orphans). Embed worklist = `embedding IS NULL` (free resume). Every dataset write is `_ensure_dataset`-guarded + schema-asserted; single-committer lease per sink; profile/serving builds are overwrite-mode snapshots stamped with consumed upstream run_ids.

## 6. Anti-patterns — how this fails (from the red teams)

1. **Schema drift on merge sinks** (the `sensitivity→content_marking` burn): any column added later = re-materialize. Frozen schema + open-time assert, or burn.
2. **`phase_finalize` take+overwrite on an embedded sink** — drops every index, OOMs on vectors. Patched in Phase 0 or the demo dies silently between prep day and call day.
3. **Ordinal/LLM-output identity keys** — `<resource_id>:<n>` and cache keys containing `value_norm` duplicate rows on every re-run or are uncomputable. Content-hash + scoped delete-before-merge, **in both lanes** — regex normalization drift orphans rows exactly like LLM drift.
4. **Prompt rules as safety gates** — the empty-map gate lives in `compile_map_filter`, deterministically.
5. **Inline scalar award key as the join spine** — 4.1× under-coverage; explode `award_keys`, assert the count.
6. **Worklist ≠ completion gate** — a `char_len` filter on the embed worklist makes IS-NULL==0 unsatisfiable.
7. **BTREE(chunk_id)** — re-arms #3177 against every refresh for an index nothing uses.
8. **Fixed-width overlap stripping / single-chunk quote validation** — corrupts reassembly and false-quarantines boundary-spanning evidence; shared reassembly function + adjacent-chunk validation.
9. **Uniform per-award budgets skipped** — W912WJ-class awards (16,846 chunks) eat cost, latency, and top-k.
10. **Treating `[]` marking as public, or letting verbatim text from marked docs reach serving** — the gate is a hard-block list, the detector has false negatives, and `evidence_quote` **and `requirement_detail`** are both egress (both NULL at write for marked resources).
11. **Unmeasured demo denominators** — the action_date/last_modified window mismatch and the 0.96% ceiling must be measured before the call, or the map looks broken instead of precise.
