# SCA ↔ SOC Occupation-Join Feasibility Audit — Clinical Diagnostic

**Question posed.** The presentation layer must render the wage-arbitrage squeeze on a **single row**: the statutory federal floor (Axis 2 — SCA) directly beside the local market wage (Axis 1 — OEWS/SOC) for the **exact same job role**. This requires a join between `sca_wd_rates` and `soc_priced_skilled` / `soc_state_wage` on occupation identity. This document audits whether that join is mechanically possible, and if not, specifies the fix.

**Verdict (BLUF).** The join is **structurally impossible** as written. The SCA and SOC occupation namespaces are two independent federal numbering authorities with **zero shared key, zero title overlap, and no authoritative public crosswalk**. Every join strategy — direct key equality, format coercion, family-prefix, and raw title-string matching — was tested against live ground truth and **fails**. The single-row comparison is unblockable until a purpose-built, precision-guarded `sca_soc_crosswalk` Lance dim is materialized. Its architecture is specified in §8.

**Provenance.** Every count below is live-probed from `s3://data-sink/active/*` via DuckDB-over-Lance (2026-07-02). Precision/coverage figures in §8 are design-agent **estimates** for an unbuilt pipeline and are labeled as such. Prior-art and adversarial verification: multi-agent workflow `wr1z1qp7o` (8 agents, 2026-07-02).

---

## 1. The two occupation namespaces at a glance

| | **Axis 2 — SCA** | **Axis 1 — SOC/OEWS** |
|---|---|---|
| Authority | DOL Wage & Hour Division (SCADD) | OMB standard, maintained by BLS |
| Priced dataset | `sca_wd_rates` (371,408 rows) | `soc_priced_skilled` (830), `soc_state_wage` (35,223) |
| Dictionary | `dol_sca_occupations` (502) | `soc_priced_skilled` + O*NET |
| Occupation key | `occupation_code` | `soc_code` |
| Key format | **5-char, 100% all-digit** (e.g. `23370`) | **7-char `dd-dddd`** (e.g. `49-9071`) |
| Distinct keys (priced) | 424 | 830 |
| Title convention | Roman-numeral skill levels `I/II/III`, `(Occupational Base)` suffixes | plain occupational titles |
| SOC linkage | **none** — no `soc_code` column anywhere in the SCA plane | native |

These are disjoint numbering systems **by construction**. SCA rates are *derived* from BLS survey data at issuance, but DOL does not publish the resulting code-to-code correspondence.

---

## 2. Schema Inspection — the primary occupation keys

### 2.1 SCA plane (Axis 2)

`sca_wd_rates` (371,408 rows; grain `wd_id` × `occupation_code`):

| Field | Type | Probed fact |
|---|---|---|
| `occupation_code` | `string` | 424 distinct; **length histogram = {5: 371,408}** (every value exactly 5 chars); **100% all-digit** (`regexp_full_match('[0-9]+')` = 1.0). Samples: `01011` Accounting Clerk I, `23370` General Maintenance Worker. |
| `title` | `string` | carries Roman-numeral skill levels + `(Occupational Base)` markers |
| `hourly_wage` | `double` | the statutory floor to be compared |

`dol_sca_occupations` (502 rows — the SCADD dictionary):

| Field | Type | Probed fact |
|---|---|---|
| `occupation_code` | `string` | 502 distinct, **length histogram = {5: 502}** |
| `occupation_title` | `string` | authoritative SCA titles |
| `occupation_definition` | `string` | **100% populated** — rich prose functional definitions |
| `family_code` | `string` | `dd000` form; 25 families (`01000`…`99000`) |
| — | — | **No `soc_code` column. No SOC mapping of any kind.** |

Register↔dictionary coverage: of the 424 priced codes, **401 are in the dictionary, 23 are not** (the register carries codes beyond the parsed SCADD).

### 2.2 SOC plane (Axis 1)

`soc_priced_skilled` (830 rows) and `soc_state_wage` (35,223 rows):

| Field | Type | Probed fact |
|---|---|---|
| `soc_code` | `string` | 830 distinct; **length histogram = {7: 830}**; **100% match `^[0-9]{2}-[0-9]{4}$`** (share = 1.0). Samples: `11-1011` Chief Executives, `49-9071` Maintenance and Repair Workers, General. |
| `soc_title` | `string` | BLS/O*NET occupational titles |
| `h_median`, `a_median`, `h_pct10…h_pct90` | `double` | the market ladder to be compared |
| `onet_description` | `string` | 98.6% populated (in `soc_priced_skilled`) |

---

## 3. Join Feasibility — mechanical evaluation

The intended query is a plain equi-join on occupation identity:

```sql
-- INTENDED (the arbitrage single row) — DOES NOT RESOLVE
SELECT r.occupation_code, r.hourly_wage,          -- SCA statutory floor
       w.soc_code, w.h_median, w.h_pct25, w.h_pct75 -- OEWS market ladder
FROM   sca_wd_rates r
JOIN   soc_state_wage w
       ON r.occupation_code = w.soc_code           -- ✗ no shared domain
WHERE  ... ;
```

Every mechanical binding was tested live. **All produce zero valid edges:**

| Join strategy tested | Result | Status |
|---|---|---|
| `occupation_code = soc_code` (raw) | **0 matches** | fails — length 5 vs 7, charset numeric vs hyphenated |
| `occupation_code = replace(soc_code,'-','')` (6-digit core) | **0 matches** | fails — 5 vs 6 digits, disjoint numbering |
| `'0' \|\| occupation_code = soc 6-digit core` (left zero-pad) | **0 matches** | fails |
| `occupation_code = left(soc_core, 5)` (lossy substring) | 3 spurious hits | **false friends** (see §4.2) |
| `occupation_code = right(soc_core, 5)` (lossy substring) | 16 spurious hits | **false friends** (see §4.2) |
| SCA `family_code[:2]` = SOC major group `soc_code[:2]` | 11 numeric coincidences | **semantic false friends** (see §4.3) |
| normalized `occupation_title` = normalized `soc_title` | **0 matches** | fails (see §5) |

**No key-based path exists at any width or coercion.** The only non-zero results come from lossy substring extraction, and every one is semantically incoherent.

---

## 4. The Disconnect — anatomy of the namespace mismatch

### 4.1 Root cause
Two sovereign coding systems, never reconciled by their issuers:
- **SCADD** (SCA Directory of Occupations, 5th ed., DOL WHD): 5-digit all-numeric; first two digits = occupational **family** (`dd000`), trailing three = specific occupation; skill level is encoded in the **title** (`I/II/III/IV`), not the code.
- **SOC** (Standard Occupational Classification, OMB/BLS): `dd-dddd` where the leading two digits are a **major group** on an entirely different taxonomy.

The keys collide on neither length (5 vs 6/7) nor structure nor semantics. This is why §3 returns zero.

### 4.2 The substring false-friend trap
Extracting a 5-digit window from the SOC core to force a length match manufactures matches that are pure digit-string coincidence across two unrelated authorities:

| SCA code / title | Spurious SOC hit | SOC actually means |
|---|---|---|
| `27101` Guard I | `27-101x` | Art Directors / Fine Artists |
| `47401` Ordinary Seaman-Tanker | `47-4011` | Construction & Building Inspectors |
| `14071` Computer Programmer I | `51-4071` | Foundry Mold & Coremakers |
| `99095` Embalmer | `49-9095` | Mobile Home Installers |

Each is one-to-many and cross-domain. **Substring coercion is disqualified.**

### 4.3 The family-prefix false friend
11 of the 25 SCA two-digit family prefixes numerically coincide with a SOC major group — but the semantics are unrelated:

| SCA family | SCA meaning | Same-number SOC major | SOC meaning |
|---|---|---|---|
| `23` | Mechanics & Maintenance/Repair | `23` | **Legal Occupations** |
| `01` | Administrative Support / Clerical | — | *(no SOC major 01)* |
| `99` | Miscellaneous | — | *(no SOC major 99)* |

Joining on the prefix routes maintenance workers into legal occupations. **The prefix is not a key.**

---

## 5. Title-Matching Evaluation — is a string-match fallback viable?

Tested normalized (uppercased, non-alphanumerics collapsed) title matching across every available title corpus:

| Match surface | Result |
|---|---|
| SCA dictionary titles (502) ∩ SOC titles (830) — exact normalized | **0** |
| SCA titles ∩ SOC `onet_title` — exact normalized | **0** |
| SCA titles vs O*NET alternate `job_title` universe (46,567 distinct lay titles) | **133 / 502** SCA codes hit *any* alt-title |
| …of those 133 hits, **unique** (resolve to exactly one SOC) | **74** (≈15% of 502) |
| …of those 133 hits, **ambiguous** (resolve to >1 SOC) | **59** (≈12%) |
| SCA codes with **no** alt-title hit at all | **369** (≈74%) |

**Even the "clean" 15% is not safe.** Adversarial check against the internal co-classification: of the 74 uniquely-resolving codes, 63 are checkable, and **9/63 (14.3%) resolve to a SOC that is *not* in that SCA code's co-classified SOC set** — i.e. the best-case slice is wrong roughly 1-in-7.

- Largest defensibly-safe subset = **70/424 = 16.5%** of the priced register, and even that carries the ~14% semantic-error rate.
- Loosening to fuzzy/token/Jaccard **monotonically inflates the ambiguous population** (SCA titles are skill-graded compound strings absent from the SOC namespace) — it raises recall by lowering precision, the opposite of what entity resolution requires.

**Conclusion: title-matching is viable only as low-confidence candidate *generation*, never as a join key.** It cannot be the primary key and cannot be trusted unadjudicated.

---

## 6. The only existing co-residence — and why it is not a crosswalk

`naics_psc_labor_profile_categories` (45,333 rows) is the **sole place** `sca_code` and `soc_code` sit on the same row, LLM co-classified (`claude-opus-4-8`). It is **not** a reusable crosswalk:

- **N:M, not a function.** One SCA code fans to many SOC codes: `23181 ELECTRONICS TECHNICIAN MAINTENANCE I` → **17** distinct SOC; `30083 ENGINEERING TECHNICIAN III` → 17; `01113 GENERAL CLERK III` → 13; `23370 GENERAL MAINTENANCE WORKER` → 10.
- **Combo-scoped.** The mapping is conditioned on the NAICS × PSC context of each row, not on the SCA occupation itself — it answers "which SOC in *this* contract context," not "the SOC for this occupation."
- **Incomplete.** Covers only **271 / 424 (64%)** of priced SCA codes; 36% have no co-classification row at all.

It is a strong **candidate signal**, not an answer. Collapsing it to a canonical 1:1 requires an explicit, guarded disambiguation rule (§8).

---

## 7. Prior-art — must we build, or can we ingest?

Exhaustive search (DOL WHD, SAM/WDOL, acquisition.gov, 29 CFR Part 4 / e-CFR, O*NET crosswalk catalog, BLS SOC crosswalks). **No authoritative public SCA→SOC crosswalk exists — high confidence, verdict: must build.**

| Source | What it is | Usable as |
|---|---|---|
| SCADD 5th ed. (DOL WHD) | The SCA code authority. **No SOC column.** | Definitions feedstock (already landed: 100% `occupation_definition`) |
| O*NET Crosswalk catalog | Covers MOC/CIP/DOT/RAPIDS/OOH/SOC/ESCO — **SCA is not among them** | Negative evidence |
| BLS SOC Crosswalks | Covers SOC↔ISCO/Census/CIP — **no SCA** | Negative evidence |
| ACF/ORR "SCA Wage Classifications" | HHS-position → SCA-position. **Wrong axis; zero SOC codes.** | SCA title/definition corpus only |
| **NAVAIR SLC Guide v2.0** | The **only** doc pairing 5-digit SCA ↔ SOC (`23370`→`49-9071`, `30083`→`17-3029`). | **Seed/validation set only:** ~46 codes (vs 424/502), skill-collapsed, Navy-scoped, non-authoritative |

The NAVAIR guide independently **confirms the N:M reality** (SCA `30080–30086` Engineering Technician I–VI all collapse to a single SOC `17-3029`) and is valuable as a small hand-verified anchor/validation set — but it covers <11% of the register and destroys skill-level granularity, so it cannot serve as the crosswalk.

---

## 8. Proposed Architecture — the deterministic bridge

**Deliverable:** a permanent, append-only Lance dim `s3://data-sink/active/sca_soc_crosswalk` (+ an N:M audit sidecar `sca_soc_candidates`) that puts each SCA `occupation_code` and its canonical `soc_code` on **one row**, so the arbitrage query becomes a plain equi-join. Precision is **guard-enforced, not sampled**; false positives are structurally impossible-by-construction, then class-proven at a fail-closed gate. Zero Anthropic API spend (in-session Opus subagents, the `naics_psc_labor_profile` house precedent).

This is the workflow-judged synthesis: **Design 1 (enum-confined LLM definitional adjudication)** as the spine — the only design whose precision guarantee is structural rather than threshold-dependent — with **Design 3's FPDS-dollar-weighted deterministic tier** grafted as a no-LLM T1 and **Design 2's BGE embedding retrieval** grafted as a recall net.

### 8.1 Pipeline stages

```
STAGE 0  build_manifest (deterministic, frozen → _sca_soc_crosswalk_manifest)
   • Spine = UNION(dol_sca_occupations 502, priced sca_wd_rates 424); 23 register-only
     codes carried register_only=true, in_scadd=false, definition NULL (complete
     LEFT-joinable spine — the wage join never silently drops a priced code).
   • base_key = title with Roman-numeral level + "(Occupational Base)" stripped
     (adjudicate the occupation ONCE, distribute SOC across levels; redirect only
     where a static SENIORITY_SOC set splits seniority).
   • Bounded candidate set K≤12 per code = UNION+dedupe of FOUR deterministic generators,
     priority-ordered:
       (1) naics_psc_labor_profile_categories co-classification (271/424 coverage)
       (2) O*NET alt-title exact hits            (candidates only, never answers)
       (3) family-scoped BM25 top-K over soc onet_description, restricted by a STATIC
           hand-verified sca_family→allowed_soc_major map (NOT numeric-prefix identity)
       (4) BGE-large-en-v1.5 (D=1024, self-hosted) brute-force exact cosine top-k over
           the 830 SOC vectors — the recall net for archaic SCADD prose vs modern O*NET.
   • Freeze candidates + candidates_sha256 + prompt_sha256. Empty-candidate codes →
     adjudicable=false → publish unmatched by construction, never sent to a model.

STAGE 1  dollar-weighted pre-collapse → T1 (NO LLM)  [deterministic]
   • Co-resident subset only: join each (sca,soc,naics,psc) co-class edge to its combo,
     pull n_awards + total_dollars_obligated; aggregate per (sca,soc): dollar_weight,
     award_weight, combo_support, off_pattern_share, mean_conf.
   • Rank by (dollar_weight DESC, award_weight DESC, combo_support DESC,
     off_pattern_share ASC, mean_conf DESC, soc_code ASC  — total, reproducible).
   • Promote to T1 ONLY if rank1 dollar_weight ≥ 2.0× rank2 AND rank1 > 0 AND
     off_pattern_share ≤ 0.5 AND rank1 SOC is inside the frozen enum.  Near-parity /
     zero-dollar / off-pattern-dominated → tie_out → Stage 2 (bias refused, not laundered).

STAGE 2  render + adjudicate → T2  [in-session Opus 4.8/xhigh, waves of 4, zero API]
   • Worklist = tie_out ∪ non-co-resident ∪ no-clean-T1.
   • One prompt per base_key: shared system preamble (tier rules + explicit NULL-bias:
     "no exact-concept SOC match ⇒ return null, never a nearest neighbor") + payload
     (SCADD title + full definition, bounded candidate list soc_code|title|onet_description).
   • Output json_schema: soc_code field is an ENUM of exactly this code's frozen candidate
     soc_codes PLUS literal null  → the model PHYSICALLY CANNOT emit an off-candidate SOC.

STAGE 3  retrieve / load  [deterministic, fail-closed — ONLY writer]
   • Resolve each custom_id STRICTLY against the frozen manifest; a soc_code outside that
     code's frozen enum ⇒ HARD BUILD FAILURE (server-side enum re-validation).
   • T2 acceptance also requires: token-overlap(SCADD def, ONET desc) ≥ OVERLAP_MIN AND
     the SOC major group not on the SEMANTIC_FALSE_FRIEND denylist (unless prose-corroborated).
     Guard failure ⇒ unmatched (NULL), never a guess.
   • Distribute base_key SOC across skill levels; confirm every soc_code exists in
     soc_priced_skilled (absent ⇒ forced unmatched). Build MAIN (1:1) + SIDECAR (N:M).
   • Run the publish gate (§8.3). write_dataset mode='overwrite' (byte-identical replace,
     never a doubled append); build indices; write ops ledger row.

STAGE 4  verify  [standalone, in-Lance, no LLM / no source re-join] — assert §8.3, fail loud.
```

### 8.2 Output schema, cardinality, indices

**MAIN `sca_soc_crosswalk`** — canonical **1 row per SCA `occupation_code`** (a strict function `occupation_code → {soc_code | NULL}`):

`occupation_code` (BTREE) · `occupation_title` · `base_key` · `occupation_definition` · `family_code` (BTREE) · `family_title` · **`soc_code` (nullable, BTREE — the forward join key)** · `soc_title` · `tier` (`T1`|`T2`|`unmatched`, BITMAP) · `method` (`fpds_weighted_majority`|`llm_definition_adjudicated`) · `corroborator_source` · `confidence` (BITMAP) · `definition_overlap` · `dominance_ratio` · `primary_dollar_weight` · `cosine_sim` · `candidate_soc_count` · `seniority_resolved` · `in_scadd` · `register_only` (BITMAP) · `in_soc_priced_skilled` · `rationale_span` · `prompt_version` · `embed_model_version` · `model_id` · `source` · `ingested_at`.

**SIDECAR `sca_soc_candidates`** — **N:M, 1 row per `(occupation_code, candidate_soc_code)`**: `rank`, `is_selected`, `is_primary`, `source_generator` (`co_class`|`onet_alt_title`|`bm25`|`bge`), `cosine_sim`, `dollar_weight`, `confirm`, `tier`. Preserves the full fan-out (a consumer that legitimately wants the N:M reads the sidecar, not the dim) and makes a k-widen / floor-lower re-run cheap off the frozen candidates.

**Cardinality contract.** MAIN is canonical 1:1; SIDECAR holds the multiplicity; the MAIN primary is always the SIDECAR rank-1 for that SCA. **SOC is coarser than SCA** — skill-level SCA codes legitimately share one SOC; the wage comparison therefore compares the SCA skill-level `hourly_wage` against the SOC **decile ladder** (`soc_priced_skilled` pct10…pct90), which is why `soc_priced_skilled` is retained as a join target alongside `soc_state_wage`.

**Indices.** MAIN: BTREE `occupation_code`, BTREE `soc_code`, BTREE `family_code`, BITMAP `tier` / `confidence` / `register_only`. SIDECAR: BTREE `occupation_code`, BTREE `candidate_soc_code`, BITMAP `is_selected` / `source_generator`.

### 8.3 Fail-closed publish gate (precision is proven, not sampled)

Raises + writes nothing on any single breach; re-asserted read-only in Stage 4:

| Guard | Assertion |
|---|---|
| **G1 Enum provenance** | every published `soc_code` appears in that code's frozen SIDECAR candidate set with `is_selected=true` — no SOC exists that no generator surfaced |
| **G2 Namespace disjointness** | every non-null `soc_code` matches `^[0-9]{2}-[0-9]{4}$` AND is not a 5-digit all-digit SCA string; `count(soc_code that is 5-char all-digit) = 0` |
| **G3 Referential integrity** | every non-null MAIN + SIDECAR `soc_code` exists in `soc_priced_skilled` (guarantees the wage join lands) |
| **G4 No-guess invariant** | `unmatched` ⇒ `soc_code` NULL; `T1`/`T2` ⇒ non-null; zero violations both directions |
| **G5 Corroboration** | every T1: `dominance_ratio ≥ 2.0` AND `off_pattern_share ≤ 0.5`; every T2: `definition_overlap ≥ OVERLAP_MIN` AND off the false-friend denylist |
| **G6 False-friend** | hard assert no family-23/47/01/99 code resolved into its forbidden SOC major group (family-23 Mechanics ∉ SOC-23 Legal) |
| **G7 Grain** | MAIN `distinct(occupation_code) = row_count`; SIDECAR `distinct(occupation_code, candidate_soc_code) = row_count` |
| **G8 Anchors** | hand-verified set holds exactly (`23370 → 49-9071` T1); a pin is honored only if it is itself a frozen candidate surviving G1–G6 |
| **G9 Coverage bands** | `t1`/`t2`/`unmatched` counts inside measured bands; collapse-to-~0 or runaway >95% match trips the gate as a regression |

### 8.4 Idempotency / ledger
- **Frozen manifest** `_sca_soc_crosswalk_manifest` is the resume spine — every downstream stage resolves against it, never live data; Stage 3 is a pure function of `(manifest, agent-results)`.
- **`ops.sca_soc_crosswalk_runs`** (Postgres via `HQX_DB_URL_POOLED`, best-effort): stage, status, `prompt_version`, `model_id`, `manifest_sha256`, `agent_results_sha256`, tier counts, `guard_failures`, `indexes_built[]`, timestamps; `UNIQUE(dataset, manifest_sha256, stage)` makes a same-manifest re-run a detectable no-op. Only a `prompt_version` bump mints a fresh `manifest_sha256`. Publish is `mode='overwrite'` (byte-identical replace, never a doubled append).

### 8.5 Projected outcome (design estimates — NOT measured)
- Precision: **T1 ≈ 0.98** (enum-confined + dollar-dominant + false-friend-gated); **T2 ≈ 0.93–0.96** (definition-corroborated); blended matched-row precision **≈ 0.96–0.98**.
- Coverage: **≈ 380–400 of 424 priced codes** carry a non-null SOC (~90–94%); the residual is deliberately **NULL (unmatched), never guessed**. The 23 register-only codes (no definition feedstock) are structurally lower-recall.
- The design **spends recall to buy precision**, per the mission mandate that false positives in occupation resolution are unacceptable; recall is cheaply recoverable via k-widen / floor-lower re-runs off the frozen sidecar.

### 8.6 The unblocked single-row query (post-build)

```sql
SELECT r.occupation_code,
       r.title              AS sca_title,
       r.hourly_wage        AS sca_floor_hourly,          -- Axis 2, statutory floor
       x.soc_code,
       w.h_median           AS oews_state_median,          -- Axis 1, market (state)
       p.h_pct25, p.h_median, p.h_pct75                    -- Axis 1 decile ladder (national)
FROM   sca_wd_rates r
JOIN   sca_soc_crosswalk x  ON r.occupation_code = x.occupation_code   -- the new bridge
LEFT   JOIN soc_state_wage  w ON x.soc_code = w.soc_code
      AND w.prim_state = :perf_state
LEFT   JOIN soc_priced_skilled p ON x.soc_code = p.soc_code
WHERE  x.tier IN ('T1','T2')
       AND r.wd_id = :governing_wd;
-- one row: SCA floor beside OEWS market for the same role. The squeeze, visualized.
```

---

## 9. Summary of findings

| Execution step | Finding |
|---|---|
| Schema inspection | SCA `occupation_code` = 5-digit all-numeric (424); SOC `soc_code` = `dd-dddd` 7-char (830). No `soc_code` anywhere in the SCA plane. |
| Join feasibility | Direct equality and every format coercion return **0 valid edges**. |
| The disconnect | Two sovereign federal numbering authorities (DOL WHD SCADD vs OMB/BLS SOC); disjoint by length, structure, and semantics. Substring and family-prefix "matches" are false friends. |
| Title matching | Exact normalized SCA∩SOC = 0. O*NET alt-titles resolve ≈15% uniquely, and even that is wrong ~1-in-7. **Candidate generation only, never a key.** |
| Prior art | No authoritative public crosswalk. NAVAIR SLC guide (~46 codes) is a seed/validation set only. **Must build.** |
| Proposed fix | Precision-guarded `sca_soc_crosswalk` Lance dim: 4-generator bounded candidates → dollar-weighted deterministic T1 → enum-confined Opus T2 → 9-guard fail-closed publish. Canonical 1:1 MAIN + N:M audit sidecar. |

**Status:** the gap is diagnosed and the bridge is specified. `sca_soc_crosswalk` is the next build — it unblocks the §5.2 BLOCKED arbitrage unlock in `01_LABOR_PRICING_FOUNDATION.md` and the single-row squeeze the presentation layer requires.

---

*Ground truth live-probed from `s3://data-sink/active/*` (DuckDB-over-Lance, 2026-07-02). Prior-art + adversarial verification: multi-agent workflow `wr1z1qp7o`. Precision/coverage in §8 are design estimates for an unbuilt pipeline. Companion: `docs/reference/01_LABOR_PRICING_FOUNDATION.md`.*
