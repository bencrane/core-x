# Subawardee Profile Universe Widening — Scoping Plan

**Date:** 2026-06-16 · **Status:** Scoping cycle (decision-ready; NOT yet built) · **Ground truth:** live R2 probes + repo verification 2026-06-16. Every count below is re-measured against the active sink; dataset versions cited inline.

---

## 0. Verdict (read first)

The operator's framing — *"widen the profiles since we already have the data?"* — splits into two paths with **opposite verdicts**:

- **Path A (extend the bridge, same provenance) is near-exhausted.** Measured ceiling: of the 25,449 subs with a description, **4,020** can reach a validated `govcon_doc_scope_90day` resource through their own `prime_award_unique_key`. The profiles already hold **6,586** subs (3,854 of them landing validated scope today). Path A's ceiling is *below* the current profile row count — the bridge is already maximal for the harvested-solicitation corpus. **Widening via Path A buys ~166 incremental enriched subs and zero net new subs.** Dead end without harvesting more solicitations.
- **Path B (derive tags from the sub's OWN subaward descriptions, new provenance) covers the full universe and is live-viable.** Coverage ceiling: **25,449** subs (every sub with a non-empty `subaward_description`; 18,864 of them are NOT in the profiles today). The substrate is already built and fully embedded — `govcon_sub_capability_vectors_90day` v8 holds **102,937 chunks / 25,449 subs, 0 embeddings NULL, IVF_PQ live**. An embedding-similarity-to-anchor spot-check returns clean controlled-vocab clusters (cosine 0.15–0.26) for every tag tested. **This is where the real widening lives.**

**Recommendation: Path B, embedding-similarity mechanism, as a SEPARATE provenance leg — tags only, written to a new `self_reported_capability_tags` field, NOT unioned into the scope-derived `capability_tags`.** Path A is documented as near-exhausted and parked behind "harvest more solicitations." Clearance / cert / labor stay bridge-only (Path B cannot populate them from short descriptions).

---

## 1. Ground truth — "do we already have the data?"

### 1.1 Path A ceiling (bridge-extension reach)

The bridge is built `subaward.prime_award_unique_key → FPDS → solicitation_identifier → sam-gov-opps → notice_id → manifest resource_id` (`pipelines/usaspending/subawardee_solicitations.py:6-9`). A sub gets scope-derived tags **only** if one of its prime awards maps to a solicitation resource that has validated `doc_scope` rows.

| Measure | Live value | Source |
|---|---:|---|
| `contract_subaward` (v12) distinct subs | 25,450 | probe |
| subs with ≥1 non-empty `subaward_description` | **25,449** | probe |
| `contract_subaward` distinct `prime_award_unique_key` | 6,347 | probe |
| `subawardee_solicitations_bridge` (v9) distinct subs | 6,586 | probe |
| bridge distinct notices / primes | 692 / **818** | probe |
| `prime_award_unique_key` overlap: csub ∩ bridge | **818 of 6,347** | probe |
| bridge subs landing **validated** doc_scope today | 3,854 | probe |
| **PATH A CEILING** — desc-subs whose csub prime key → validated doc_scope | **4,020** | probe |
| `govcon_subawardee_capability_profiles` (v49) rows | 6,586 | probe |
| └ `has_extracted_scope=true` / `capability_tags` non-empty | 4,220 / 3,732 | probe |

**Reading.** Only **818 of 6,347** csub prime keys (12.9 %) are in the bridge at all — the bridge's notice election is the bottleneck the diagnostic predicted (`docs/reference/SUBAWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md` §1: Stage-2 solnum→notice resolution ≈22 % subaward-weighted, then the harvested-solicitation subset is smaller still). The Path-A ceiling (4,020) is **lower than the rows already in the profiles (6,586)** and only ~166 above the currently-enriched 3,854. The profiles table is ALREADY at/above the Path-A ceiling because it carries the full bridge universe (6,586) regardless of whether each sub lands scope. **Path A cannot widen the universe — it can only marginally raise the enriched fraction inside the existing universe, and only if more solicitations are harvested into doc_scope.** It is exhausted for the current corpus.

> Note the `has_extracted_scope` discrepancy: the profile reports 4,220 enriched but the live re-probe of the `validated` doc_scope chain yields 3,854 — the profile's flag also counts subs with a requirements-only signal (no validated scope summary). Either way both are far below 25,449 and the conclusion is unchanged.

### 1.2 Path B coverage (own-description reach)

Path B derives "what the sub says it did" from `subaward_description` — sub-self-reported, present for nearly every sub.

| Measure | Live value | Source |
|---|---:|---|
| subs with ≥1 non-empty `subaward_description` | **25,449** | probe |
| subs already in profiles | 6,586 | probe |
| desc-subs already profiled (∩) | 6,585 | probe |
| **desc-subs NOT in profiles (the widening prize)** | **18,864** | probe |
| profiled subs with no description (edge) | 1 | probe |
| `govcon_sub_capability_vectors_90day` (v8) chunks / distinct subs | 102,937 / 25,449 | probe |
| └ embedding filled / NULL | **102,937 / 0** | probe |
| IVF_PQ vector index | **LIVE** | probe |
| `govcon_doc_scope_90day` distinct validated tags vs `_CAPABILITY_TAGS` | **77 = 77, exact set match** | probe |

**Reading.** Path B's coverage is **25,449** subs — the full universe, +18,864 net new vs today. The entire embedding substrate the mechanism needs is already built and indexed (the PHASE-5 sub-vector leg shipped: `govcon_sub_capability_vectors_90day` is fully embedded, 0 NULL). The widening is a **derivation + write**, not new data engineering or a GPU run.

### 1.3 Path B mechanism viability (live spot-check)

Embedding-similarity-to-anchor (mechanism (a) in the brief) tested live against the IVF_PQ index. Per-tag anchor text → cosine ANN → nearest sub descriptions:

| Tag anchor | nearest neighbors (cosine distance) |
|---|---|
| `electrical_systems` | ELECTRICAL EQUIPMENT AND INSTALLATION (0.152), ELECTRICAL INSTALLATION (0.170), ELECTRICAL WIRING (0.178) |
| `hvac_mechanical` | HVAC MAINTENANCE AND REPAIRS (0.206), HVAC INSTALLER FOR MECHANICAL EQUIPMENT (0.206), HVAC SYSTEM (0.214) |
| `it_services` | DBA/HELP DESK/SYSTEM ADMIN SERVICES (0.167), … (0.171) |
| `aircraft_maintenance` | AIRCRAFT PARTS AND SERVICE (0.181) |
| `custodial_janitorial` | JANITORIAL SERVICES (0.158), ONSITE JANITORIAL SERVICES (0.179) |
| `food_services` | CAFETERIA SERVICES (0.196), DINING FACILITY (0.228), CATERED MEAL SUPPORT (0.245) |

Clean, tight, correct clusters for every tag. **Embedding-similarity is viable and reuses the live index with zero new GPU cost.**

**Threshold caveat (load-bearing for the mechanism design).** A negative-control query (`"underwater basket weaving for lunar colonists"` — a genuinely absent capability) still returns its nearest neighbors at d≈0.30 (space/ocean R&D descriptions — the true nearest, correctly). So a **single global cosine cutoff is unsafe**; each tag's natural distance band differs (electrical clusters at 0.15, food at 0.20). Distinct-sub yield for the `electrical_systems` anchor by threshold: **213 subs @ 0.25 · 440 @ 0.30 · 1,223 @ 0.35 · 2,298 @ 0.40.** The mechanism must use **per-tag calibrated thresholds**, not one global number — see §3.2.

---

## 2. Recommendation

**Build Path B as a separate-provenance, tags-only widening leg. Park Path A.**

**Why Path B, not A.** Path A's measured ceiling (4,020) sits below the current profile row count — it cannot add subs, only marginally lift the enriched fraction, and only behind a fresh solicitation-harvest run. Path B reaches all 25,449 (+18,864 net new), the substrate is already built and embedded, and the live spot-check proves the mechanism works today.

**Why embedding-similarity, not LLM-classify (now).** The vectors + IVF_PQ are built and free to query; the spot-check shows controlled-vocab tags fall out cleanly. LLM controlled-vocab classification (mechanism (b)) is more accurate on ambiguous/multi-capability descriptions and is the natural V2, but it costs tokens/GPU and adds a new extraction stage that mirrors doc_scope — **defer it; start with the deterministic-ish, zero-new-cost embedding path.** Lexicon/keyword (mechanism (c)) is rejected: it provably misses paraphrase (the whole reason the vectors exist) and the controlled vocab's tags are concepts, not keywords.

**Why separate provenance.** A scope-derived tag means *"this capability appeared in the prime solicitation the sub teamed under"*; a description-derived tag means *"the sub itself reported doing this."* These are different claims with different trust and different CUI posture. Collapsing them into one `capability_tags` field destroys the distinction every downstream consumer would need to reason about (and silently changes what the winners-map filter and the catalyst route already serve). Keep them in distinct fields with a `tag_source` marker. See §4.

**Cost / blast radius.** Zero new GPU (index already built). One new derivation module + one frozen-schema change (new fields) + re-materialize the profiles + propagate the new field through 3 consumers. Blast radius is **medium-high** because it touches the frozen schema and all three consumers — quantified in §5.

---

## 3. Build spec (recommended path)

### 3.1 Universe redefinition + grain invariant (the blast)

Today: `build_subawardee_capability_profiles.py:14-19, 201, 500-503` defines `UNIVERSE = distinct subawardee_uei in subawardee_solicitations_bridge` and asserts the hard invariant **`profile_rows == distinct bridge sub UEIs`** (6,586). Widening Path B changes the universe to **`distinct subawardee_uei in contract_subaward with a non-empty subaward_description`** (25,449), so the invariant must change to:

```
profile_rows == distinct (csub desc-subs ∪ bridge subs)   # 25,449 (bridge ⊂ desc-subs except 1 edge sub)
```

The bridge is no longer the universe — it becomes an **enrichment join** (the scope leg), exactly like teaming/POC already are. The 1 profiled sub with no description must be retained (union, not replace), so the universe is the union; in practice 25,449 + 1 = 25,450.

**This is the single biggest structural change.** Every LEFT JOIN in `_assemble` already keys on `sub_uei` from a `universe` CTE, so widening the universe CTE from `bridge` to `csub desc-subs` is mechanically small — but it means **18,864 rows will have NULL/empty scope-leg fields** (`has_extracted_scope=false`, `capability_tags=[]`, `requires_clearance=false`, etc.), which is correct and already the representation for non-enriched subs. The DoD invariant assertion (`build_subawardee_capability_profiles.py:500-503`) and the `verify` parity checks (`:549, 558`) must be rewritten to the new universe or they will hard-fail the build.

### 3.2 Tag-derivation mechanism (embedding-similarity, per-tag calibrated)

New derivation, run as a stage of the profile build (or a precomputed sidecar table read by the build):

1. **Anchor embeddings.** For each of the 77 `_CAPABILITY_TAGS`, embed a short canonical anchor phrase with the pinned `BAAI/bge-large-en-v1.5` (L2-normalized, passage form — same model the vectors used). Anchors are authored once and version-pinned in the module (the spot-check anchors in §1.3 are the seed set). **Author 1–3 anchor phrases per tag** and take the min distance across a tag's anchors (handles polysemous tags like `it_services`).
2. **Per-tag calibrated threshold.** Do NOT use a global cutoff (negative-control proved it leaks). Calibrate each tag's threshold against the doc_scope-tagged subs that already carry that tag (the 6,585 profiled subs are the labeled set): pick the per-tag distance that maximizes agreement with the existing scope-derived tag on the overlap population (precision-favoring; this is a deterministic calibration pass, re-runnable). Store the calibrated thresholds in the module as a frozen dict so the build is idempotent.
3. **Assign.** For each sub, for each tag, if the sub has ≥1 description chunk within the tag's calibrated threshold of any of the tag's anchors → assign `self_reported_capability_tags += tag`. Cap at `TAG_CAP` (30, reuse the existing cap). Cosine ANN over the live IVF_PQ index, batched per tag (77 anchor queries, k large, collapse to distinct sub).
4. **Determinism.** Anchors + thresholds frozen in code; ANN over a fixed index is stable; tag order sorted. The `verify --content-hash` idempotency proof holds (the new field is deterministic given pinned anchors/thresholds/index version).

**This is "deterministic-ish":** stable given a pinned index version + frozen anchors/thresholds. Re-embedding the vectors (new index version) would shift distances — gate the build on the vectors `run_id`/version so a vector rebuild forces a recompute (record the vectors version in `snapshot_run_id`, which already stamps upstream versions: `build_subawardee_capability_profiles.py:475-479`).

### 3.3 What Path B can and cannot populate

| Field | Path B can derive? | Why |
|---|---|---|
| `self_reported_capability_tags` (NEW) | **Yes** | the whole point — controlled-vocab tags from description ANN |
| `scope_summary`, `capability_tags` (scope-derived) | No — stays bridge-only | derived from solicitation scope, not the sub's words |
| `requires_clearance`, `req_clearance_level_max`, `requires_cmmc` | **No — stays bridge-only** | clearance is a *solicitation* requirement; a 40-char description ("JANITORIAL SERVICES") carries no clearance signal. Asserting clearance from descriptions would be fabrication. |
| `req_cert_tags`, `top_labor_categories` | **No — stays bridge-only** | same: certs/labor categories are extracted from solicitation requirement text, absent in short self-descriptions |

**Call it out plainly: Path B is tags-only.** The clearance/cert/labor axis remains a bridge-only signal. A sub widened by Path B will have `self_reported_capability_tags` populated but `requires_clearance=false` / `capability_tags=[]` — and that is the honest representation.

### 3.4 Schema impact (frozen-schema change — the schema is the law)

`subawardee_capability_profiles_schema()` (`govcon_gtm_schemas.py:279-352`) is frozen and asserted before every commit. Path B requires **adding fields** — a frozen-schema change that mandates a full re-materialize (Lance rejects type/field changes on append; `assert_schema` will hard-fail otherwise):

```python
# add to subawardee_capability_profiles_schema(), in the ENRICH block:
("self_reported_capability_tags", pa.list_(pa.string())),   # Path B: derived from the sub's OWN
                                                            # subaward_description via embedding-sim
("n_self_reported_tags", pa.int32()),
("tag_source", pa.string()),  # provenance marker: 'scope' | 'self_reported' | 'both' | 'none'
("self_reported_tag_conf", pa.list_(pa.float64())),  # OPTIONAL parallel-to-tags min cosine distance
```

`tag_source` is a per-row rollup describing which provenance populated the row's tag axes (drives consumer reasoning). The OVERWRITE build mode (`:511`) already re-materializes, so the schema change is a frozen-schema bump + one rebuild, not a migration.

### 3.5 CUI posture

**Path B is CUI-safe by construction.** `subaward_description` is the firm's own SAM subaward report — sub-self-reported, NOT solicitation CUI (already asserted in both builders' docstrings: `build_subawardee_capability_profiles.py:36`, `build_sub_capability_vectors.py:22-25`). The derived tags are controlled vocabulary (no verbatim text). The existing write-side CUI gates (`build_subawardee_capability_profiles.py:148-159`) are untouched — Path B adds no marked-resource dependency. No new egress risk.

### 3.6 Idempotency / indices / DoD

- **Idempotency:** OVERWRITE snapshot; anchors + per-tag thresholds frozen in code; ANN over a pinned index version → stable. `verify --content-hash` extended to cover the new business columns; re-run over the same upstream + same vectors version = zero-delta.
- **Indices:** add **BITMAP(`tag_source`)** (low-cardinality: scope/self_reported/both/none). `self_reported_capability_tags` is a `list<string>` filtered by Lance `array_has` — no scalar index (same as `capability_tags` today). No new BTREE.
- **DoD:** `profile_rows == 25,450`; `self_reported_capability_tags` non-empty for a meaningful fraction of the 18,864 net-new subs (target ≥ X% per calibration); `tag_source` distribution sane (`both` ⊂ the 6,585 overlap; `scope` only where bridge-enriched; `self_reported` for net-new; `none` for subs the ANN couldn't tag); the existing scope-derived `capability_tags`/clearance/cert/labor **byte-identical** for the 6,586 bridge subs (Path B must not perturb the scope leg — content-hash the scope columns separately to prove it); CUI checks still PASS; spot-check: a known electrical sub NOT in the bridge now carries `self_reported_capability_tags=[electrical_systems]`.

---

## 4. The load-bearing design decision — same field vs separate

**DECISION: separate field (`self_reported_capability_tags`) + `tag_source` marker. Do NOT union into `capability_tags`.**

Rationale (this is the call that ripples through all three consumers):

- The two tag sets answer **different questions**. `capability_tags` (scope) = "the solicitation scope the sub teamed under named this capability." `self_reported_capability_tags` (Path B) = "the sub reported doing this." A buyer-side targeter weighs these differently (scope-derived is corroborated by a real procurement; self-reported is the firm's own claim). Collapsing them erases that.
- **It silently changes what's already served.** The winners-map `winners.v3` decoder gates `capability_tag` filtering behind `has_extracted_scope=true` (`apps/catalyst_api/src/map_decoders.py:113-124`, `compile_map_filter` auto-ANDs the gate). If Path B tags were unioned into `capability_tags`, a self-reported-only sub would have tags but `has_extracted_scope=false` → the gate would **exclude it anyway**, making the union useless on the map *unless* `has_extracted_scope` were also flipped true — which would be a lie (no scope was extracted). Separate fields avoid corrupting the gate's meaning.
- The catalyst route's `SubawardCapabilities` model (`apps/catalyst_api/src/models.py:551+`) already only populates `capabilities` when `profiled=true`. A separate field lets the route expose `self_reported_capability_tags` for ALL subs (the 18,864 net-new included) without faking the scope block.

**Downstream impact of the separate-field choice:**

| Consumer | Impact |
|---|---|
| **winners-map leg** (`pipelines/serving/materialize_winners_map.py:218-249`) | The serving `capability_tags` column stays scope-derived (gated). To surface Path B tags on the map, add a NEW serving column `self_reported_capability_tags` + a NEW `winners.v3→v4` decoder field that is **NOT gated by `has_extracted_scope`** (it has its own provenance). This is a decoder bump — assess whether the map demand justifies it, or leave Path B tags off the map initially (catalyst route + gtm_mcp deliver the value without a decoder change). |
| **catalyst route** (`apps/catalyst_api/src/models.py:551+`, `main.py:400-417`) | Add `self_reported_capability_tags` + `tag_source` to `SubawardCapabilities`; populate for ALL subs (not gated on `profiled`). Surfaces capability for the 18,864 net-new subs that today return `profiled=false` with an empty capability block. Requires catalyst redeploy (hardcoded URI/model). |
| **gtm_mcp enrichment** (`apps/gtm_mcp/src/tools/sub_capability.py:54-56, 140-153, 279`) | `_PROFILE_COLUMNS` adds `self_reported_capability_tags`/`tag_source`; the `profiled` flag stays (it now means "scope-enriched"), and the enriched result gains the self-reported tag axis for the universal population. The tool's own docstring already anticipates this — it notes the profiles cover only 6,586 while vectors cover 25,449 (`:41-47`); widening the profiles to 25,450 makes `_enrich_profile` near-universal and the `profiled`/universal split collapses (worth simplifying). |

---

## 5. Change-points table

| File:line | Current | Change | Why |
|---|---|---|---|
| `pipelines/sam_gov/govcon_gtm_schemas.py:279-352` | frozen profiles schema, no self-reported tag fields | **+`self_reported_capability_tags`, `n_self_reported_tags`, `tag_source`, (opt) `self_reported_tag_conf`** | frozen-schema change → full re-materialize |
| `pipelines/sam_gov/build_subawardee_capability_profiles.py:14-19, 201` | UNIVERSE = distinct bridge sub_uei | UNIVERSE = distinct csub desc-subs ∪ bridge subs (25,450); bridge becomes an enrichment join | Path B widening — the universe redefinition |
| `…:500-503, 549, 558` | invariant `rows == distinct bridge sub UEIs` | invariant `rows == distinct (desc-subs ∪ bridge)`; rewrite verify parity | the grain invariant changes or the build hard-fails |
| `…/build_subawardee_capability_profiles.py` (`_assemble`) | no Path B leg | NEW: anchor-embed 77 tags, per-tag calibrated ANN over `govcon_sub_capability_vectors_90day`, assign `self_reported_capability_tags`; compute `tag_source` | the tag-derivation mechanism (§3.2) |
| `…:524-530` (`_content_hash`) | hashes existing business cols | include new cols; add a separate scope-column hash to prove the scope leg is unperturbed | idempotency DoD |
| `…:72-73` (`BITMAP_INDEXES`) | 6 bitmap cols | +`tag_source` | low-cardinality filter axis |
| `apps/catalyst_api/src/models.py:551+` (`SubawardCapabilities`/`SubawardProfileResponse`) | scope-only capability block, gated on `profiled` | +`self_reported_capability_tags`, `tag_source`; populate for all subs | surface capability for the 18,864 net-new subs |
| `apps/catalyst_api/src/lance_store.py` (`_SUB_PROFILE_COLS`) | scope cols only | + new fields to the point-lookup projection | route reads them |
| `apps/catalyst_api/main.py:400-417` | route returns scope-only block | unchanged logic, richer payload | catalyst **redeploy** (hardcoded URI) |
| `apps/gtm_mcp/src/tools/sub_capability.py:54-56, 279-283` | `_PROFILE_COLUMNS` scope-only; `profiled` split | + new fields; `profiled` now = "scope-enriched"; near-universal enrich | gtm_mcp redeploy to ship |
| `apps/catalyst_api/src/map_decoders.py:91-124` (`winners.v3`) | gated `capability_tag` only | **OPTIONAL** `winners.v4`: ungated `self_reported_capability_tag` field + serving column | only if map demand justifies a decoder bump |
| `pipelines/serving/materialize_winners_map.py:218-249` (`cap_sub`) | maps scope `capability_tags` to sub rows | **OPTIONAL** map `self_reported_capability_tags` to a new ungated serving column | paired with the decoder bump above |

**Build order.** (1) schema bump + anchors/thresholds calibration → (2) rebuild profiles with Path B leg + verify (scope leg byte-identical, universe=25,450) → (3) catalyst model/lance_store/route + redeploy → (4) gtm_mcp enrichment + redeploy → (5) OPTIONAL winners-map decoder v4 + serving column + rebuild. Steps 3/4 are parallel and depend only on step 2. Step 5 is gated on a map-demand decision.

---

## 6. Risks

1. **Threshold calibration is the make-or-break.** A global cutoff leaks (negative-control proved it). Per-tag calibration against the 6,585 overlap subs is the right method but the overlap is the scope-derived label, which is itself noisy (it's the *solicitation's* capability, not the sub's). Mitigate: precision-favoring thresholds + manual spot-check of a sample per tag before committing the frozen threshold dict. A bad threshold either over-assigns (every sub "does electrical") or under-assigns (defeats the widening).
2. **Multi-tag descriptions.** A description like "DBA, SYSTEM ADMIN AND HELP DESK" should map to one tag; a true multi-capability sub may need several. Embedding-sim assigns independently per tag — acceptable, but watch for tag-bleed (an "ELECTRICAL AND HVAC" description tagging both, which is correct, vs. spurious bleed). This is the case for the LLM-classify V2.
3. **Vector index version coupling.** Path B distances depend on the IVF_PQ index. A vector rebuild silently shifts assignments. Mitigate: stamp the vectors version into `snapshot_run_id` and gate recompute on it (§3.2).
4. **Frozen-schema change + full re-materialize.** The OVERWRITE build already re-materializes, so this is a bump + rebuild, not a migration — but every consumer reading by hardcoded URI (catalyst) must redeploy in lockstep or it reads a column it doesn't expect (forward-compatible: new columns are additive, old consumers ignore them, so the order is rebuild-then-redeploy with no downtime).
5. **Semantic dilution of the profiles table's meaning.** Today "a row in profiles" = "a sub that teamed under a tracked solicitation." After widening, "a row" = "any sub with a description," and the bridge/scope signal becomes one optional axis. Consumers that implicitly assumed the bridge universe (e.g. anything counting `profile_rows` as "subs connected to tracked solicitations") will silently change meaning. Audit for such assumptions — `tag_source` and `has_extracted_scope` preserve the ability to filter back to the bridge subset.

---

## 7. Open decisions for the operator

1. **Mechanism: embedding-similarity now, or jump straight to LLM controlled-vocab classification?** Recommendation: embedding-sim first (zero new cost, proven viable, the index is built); add LLM-classify as V2 only if calibration precision proves thin on multi-capability descriptions. Confirm.
2. **Map exposure: ship the OPTIONAL `winners.v4` ungated self-reported-tag field, or keep Path B tags off the map?** A decoder bump is the heaviest consumer change and the map gate (`has_extracted_scope`) is purpose-built for the scope axis. Recommendation: ship Path B to the catalyst route + gtm_mcp first (no decoder change), measure map demand, bump the decoder later. Confirm or promote step 5 into the initial build.
3. **Profiles universe = 90-day `contract_subaward` (25,449, the proven window matching the vectors) or widen the vector substrate to 5y `usaspending/subaward_search` first?** The vectors are sourced from the 90-day feed today (`build_sub_capability_vectors.py:54-55`); widening the profiles to 25,449 matches that. Going to 5y is a separate, larger decision (re-embed 9.8M-row corpus) already flagged in `SUBAWARDEE_CAPABILITY_BUILDOUT_PLAN.md` §8.1. Recommendation: 90-day first; revisit per that plan's open decision.
