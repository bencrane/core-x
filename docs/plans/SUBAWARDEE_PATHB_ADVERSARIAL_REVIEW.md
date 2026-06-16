# Subawardee Path B — Adversarial Review

**Date:** 2026-06-16 · **Stance:** adversarial (flawed-until-proven) · **Ground truth:** live R2 probes + 4 experiments run 2026-06-16 against `govcon_sub_capability_vectors_90day` (102,937 chunks / 25,449 subs, IVF_PQ live) and `claude-haiku-4-5`. All probe scripts under `_pathb_probe/` (read-only, not committed).

---

## 0. Verdict (read first)

**The embedding-similarity-to-anchor mechanism is the wrong tool and should be replaced by LLM controlled-vocab classification.** It is not "viable-after-fixes." Every proposed fix was tested live and either failed or made it worse:

- The brief's headline fix hypothesis (the BGE **query-instruction asymmetry bug**, brief §4) is **real but inverted** — adding the query prefix to anchors makes coverage *collapse* from 5,327 → 1,546 subs and pushes mean best-tag distance 0.319 → 0.392. The prototype's no-prefix form is already the better of the two. There is no recall to recover here.
- The brief's calibration fallback (**fixed distance bands**, §3.2 / my own "re-run with ~0.22–0.26") is unsafe: terse defense-jargon descriptions — which are the bulk of the long tail — sit at d ≥ 0.35 from their correct tag, *outside any band that isn't already polluted by cross-tag noise*. A band wide enough to catch them assigns garbage.
- The brief's anchor-improvement idea (**centroid-of-known-positives**, brief §3.2 step 1 / brief §3 "exemplars") was tested and **collapses entirely** — the scope-tagged subs' own descriptions are dominated by generic teaming boilerplate ("EQUIPMENT IN SUPPORT OF CONTRACT", "SUPPLYING HARDWARE"), so every tag's centroid converges to the *same* generic blob and tags ~4,000 identical subs regardless of tag.

Meanwhile the alternative the brief defers to V2 — **LLM controlled-vocab classification** — is **~$13 one-time on Haiku** over all 67,091 distinct descriptions and resolves precisely the inputs embedding-sim cannot (POL→fuel_supply, OCONUS→logistics, TO68 SWITCHGEAR→electrical_systems). The "embedding-sim is zero-cost, do it first" reasoning (plan §0, §2, §7.1) is a **false economy**: it ships a low-coverage, low-precision tag set to dodge a $13 spend.

**Highest-impact finding:** the centroid/calibration collapse + the LLM cost measurement together prove the cheaper mechanism is also the worse product. Replace, don't tune.

---

## 1. The mechanism — embedding-similarity is the wrong tool for terse controlled-vocab classification

The corpus is short and abbreviation-dense (`_pathb_probe/exp2`): mean **114 chars / 28.6 tokens** per chunk, **p50 = 39 chars**, 67,091 distinct chunk-texts. These are SKU/work-item fragments and defense acronyms, not prose.

BGE-large maps clean, fully-spelled capability strings tightly (e.g. `JANITORIAL SERVICES` → authored anchor at d=0.118; `ELECTRICAL INSTALLATION` at d=0.082). But the moment the description is an acronym or jargon, the embedding has nothing to latch onto. Tested 8 obvious-but-terse exemplars against their correct authored-anchor tag (`exp3`):

| description (correct tag obvious to a human) | nearest authored-anchor tag | distance |
|---|---|---:|
| `TO68 SUBSTATION SWITCHGEAR` (electrical) | electrical_systems | **0.36** |
| `RAD HARD PARTS` (electronics/supply) | facilities_management (WRONG) | 0.44 |
| `FF&E` (furniture/fixtures supply) | fuel_supply (WRONG) | 0.43 |
| `POL PRODUCTS` (petroleum/fuel) | supply_commodities (near-miss) | 0.41 |
| `DLA TROOP SUPPORT` (logistics) | logistics_transportation | 0.47 |
| `MWR SUPPORT` (morale/welfare/events) | facilities_management (WRONG) | 0.43 |
| `OCONUS LOGISTICS` | logistics_transportation | **0.30** |

Only one of eight lands inside even a generous 0.30 band, and several nearest tags are wrong. The negative-control leak the plan flagged (plan §1.3, absent capability → neighbors at d≈0.30) is the *same phenomenon*: at d≈0.30+ the metric is no longer discriminative, it is returning "least-far" not "correct." A band drawn anywhere ≥0.28 (needed to reach the terse tail) is past the point where distance means relevance.

**The same 20 items through `claude-haiku-4-5` (`exp4`, single batched call, 956 in / 517 out tokens):** every terse acronym resolved correctly (TO68→electrical_systems, POL→fuel_supply, OCONUS→logistics_transportation, MWR→event_conference_support, FF&E→supply_commodities), multi-tag where warranted (`SUBCONTRACTOR WORK ENTAILS SUPPORT TO SAIC` → surveying_mapping_gis + data_management_analytics), and — critically — **returned `[]` for non-informative strings** (`PER PC.`, `PROCURED GOODS BETWEEN 25-OCT-23`) that a fixed-band classifier would silently mis-assign to its nearest neighbor.

### LLM-classify cost is not a real obstacle (`exp2`)

Per distinct chunk-text (67,091 of them), batched with a cached 77-tag system prompt:

| model | est. one-time cost (full corpus) |
|---|---:|
| Haiku-class | **~$13** (less when batched 20/call) |
| Sonnet-class | ~$48 |

Total input across all distinct descriptions is **2.94M tokens**. This is a one-time derivation, re-run only on a corpus refresh — not a per-request cost. **The entire economic premise for choosing embedding-sim over LLM-classify (plan §2 "zero-new-cost", §7.1) does not survive contact with the measured numbers.**

---

## 2. The calibration — no defensible threshold exists without a gold label, and the proposed labels don't work

**The scope-derived overlap label is the wrong objective — confirmed, and worse than the brief argues.** The brief's diagnosis ("the scope tag means the *solicitation* named the capability, not that the sub did") is correct, but the live evidence shows *why* it's fatal: I built per-tag centroids from the descriptions of scope-tagged subs (`exp3`). If those descriptions were capability-discriminative, the centroid would be a strong anchor. Instead, for **electrical_systems, custodial_janitorial, AND software_development**, the centroid's nearest neighbors are the *identical* generic strings (`SOFTWARE AND EQUIPMENT`, `SUPPLYING HARDWARE`, `EQUIPMENT IN SUPPORT OF CONTRACT DELIVERY`), and each centroid tags ~3,855–4,201 subs at d≤0.24 — i.e. **all three "different" tags converge to the same ~4,000-sub blob.** The scope-tagged population's self-descriptions carry almost no capability signal; calibrating *or* anchoring against them is calibrating against noise. This is the root cause of the 70-tag floor.

**Fixed global/near-global band — tested, rejected (`exp1`, `exp2`).** Per the any-tag coverage sweep (no-prefix anchors): d≤0.20 → 2,662 subs, d≤0.24 → 4,511, d≤0.28 → 7,678, d≤0.30 → 9,841. Eyeballing the 0.24–0.28 marginal ring (`exp2`) shows the precision cliff: at this band `custodial_janitorial` ingests `WASTE REMOVAL SERVICE`, `RENTAL AND LAUNDRY SERVICES`, `CARPENTRY AND PLUMBING SERVICES`; `food_services` ingests `SERVICE, CONSULTING`, `SERVICES, EVENT SPACE`. The band that finally widens coverage is the band that destroys precision. There is no global number that is both wide enough to widen and tight enough to be true.

**The only defensible calibration is a minted gold label, and once you have it you don't need embedding-sim.** To calibrate any threshold you must LLM-label a stratified sample (say 1,500 descriptions across the tag space) and calibrate per-tag against *that*. But the same LLM call that produces the gold label can classify the whole 67k-distinct corpus for ~$13. **Minting the gold label IS the LLM-classify path.** Spending the LLM budget to calibrate a worse mechanism instead of just using the LLM output is strictly dominated.

---

## 3. Anchors — authored phrases are fine; the anchor is not the failure point

The brief asks whether 1–2 authored phrases are the weak link and whether centroid/exemplar anchors would beat them. **Authored phrases are the best of the three and are not the problem (`exp3`):** authored anchors hit exact-match capability strings at d=0.08–0.14 and assign clean, tight sets (electrical 166 subs @0.24, janitorial 21, software 172). Centroid anchors are strictly worse (collapse, §2 above). The failure point is not anchor quality — it is that (a) the *corpus* descriptions for most subs are terse/abbreviated and unreachable by any anchor, and (b) the threshold has no honest calibration target. Improving anchors cannot fix either. **Reject the centroid-anchor remediation.**

---

## 4. The asymmetry "bug" — real BGE convention, but applying it here HURTS (do not fix)

Confirmed the corpus embedding form: `sam_attachment_embed_modal.py:75-76` embeds passages as `model.encode([t or " " for t in ...text...], normalize_embeddings=True)` — **raw text, NO instruction prefix** (correct BGE passage form). The live scope-search tool `apps/gtm_mcp/src/embeddings.py:42-45,78` prepends `"Represent this sentence for searching relevant passages: "` to **queries** only. The prototype (`sub_tag_classifier_proto.py:102`) embeds anchors with no prefix — i.e. as passages.

The brief hypothesizes this depresses recall and that anchors *should* carry the query prefix. **Tested directly (`exp1`) — the opposite is true:**

| anchor form | tags above floor | subs ≥1 tag (calibrated) | mean best-tag dist | d≤0.24 coverage |
|---|---:|---:|---:|---:|
| NO prefix (prototype) | 14/77 | **5,327** | **0.319** | 4,511 |
| WITH query prefix (live tool) | 12/77 | 1,546 | 0.392 | 748 |

Adding the prefix **degrades everything**. Reason: BGE's query instruction is tuned for *question→document* asymmetry (a natural-language question retrieving a long passage). Here both sides are short same-register capability phrases (`janitorial services` vs `JANITORIAL SERVICES`); the anchor-as-passage already matches the corpus register, and the instruction shifts it into the wrong subspace. **This is not a fixable bug — the prototype's choice is already correct. Do not apply the prefix.** (It does mean the *live scope tool's* query/anchor handling is not a template to copy for this task.)

---

## 5. Distance metric / normalization — clean, no bug

Confirmed (`exp1`): corpus vectors are L2-normalized (sampled norm min 0.9995 / max 1.0005 / mean 1.0002). The prototype computes `dist = 1 - dot` over normalized vectors = cosine distance, matching Lance's cosine convention and the embed harness's `normalize_embeddings=True`. No metric/normalization mismatch. This axis is not where the problem lives.

---

## 6. Downstream design — a weak classifier makes the separate-field + universe-widening design actively harmful

The plan's separate-field/`tag_source`/universe-redefinition-to-25,450 design is *architecturally* sound (the gating analysis in plan §4 re: `map_decoders.py:113-124` is correct — unioning into `capability_tags` would corrupt the `has_extracted_scope` gate). **But the design assumes a classifier worth shipping. With the measured one, it isn't:**

- **Coverage is thin.** The 7 tags that actually calibrate above the floor reach only **1,338 of 18,864 net-new subs (7%)** at a defensible d≤0.24 (`exp3`). The "widening" widens by ~7% of the prize, and only into 7 generic professional-services tags (program mgmt, engineering, software, R&D, IT, MRO, cyber). The entire trades/facilities long tail the plan's spot-check showcased (electrical/hvac/janitorial/food, plan §1.3) assigns **single-digit-to-low-double-digit** subs each — exactly the clusters the plan claimed were "clean."

- **Low precision is a product liability, not just a miss.** `self_reported_capability_tags` is surfaced to a **buyer-side targeter** (catalyst route `SubawardProfileResponse`, `apps/catalyst_api/src/models.py:551+`; gtm_mcp `sub_capability.py`). A false `electrical_systems` tag on a sub that merely bought a power strip is a false "this firm does electrical work" claim driving outreach/targeting. At the band needed for non-trivial coverage (≥0.28), the marginal ring is full of these (`exp2` §2). **Shipping it degrades the product's trust surface** — a wrong capability claim is worse than an absent one for a targeting tool.

- **The universe redefinition is a large, irreversible blast for a 7%-filled column.** Plan §3.1/§5 change the hard grain invariant (`build_subawardee_capability_profiles.py:500-503`), rewrite verify parity (`:549,558`), bump the frozen schema (`govcon_gtm_schemas.py:279-352`), force a full re-materialize, and require lockstep redeploys of catalyst (hardcoded URI) + gtm_mcp. That is the correct cost *if the column is worth it.* For a 7%-populated, low-precision column it is a lot of frozen-schema risk for little value.

**The separate-field design is right; the thing to put in the field is wrong.** Keep the schema/provenance design exactly as planned — but fill the field from LLM-classify, and only widen the universe once the classifier earns it.

---

## 7. Other end-to-end issues

- **Determinism / idempotency of an LLM path (plan §3.2 step 4, §3.6).** An LLM classifier breaks the `verify --content-hash` zero-delta proof unless the LLM output is **materialized as a versioned sidecar table** (keyed by `chunk_text` hash → tags, stamped with model id + prompt version) and the profile build reads that table deterministically. Do NOT call the LLM inside the profile build. Mint a `govcon_sub_self_reported_tags` derivation table (input = 67,091 distinct chunk-texts, dedup-keyed), classify once, freeze; the build joins it. Re-run only on corpus/model/prompt-version change, gated by stamping those into `snapshot_run_id`. This preserves idempotency AND is cheaper (classify distinct texts, not 102,937 chunks or 25,449 subs).
- **CUI (plan §3.5).** LLM-classify shifts posture: `subaward_description` is sub-self-reported (CUI-safe, asserted in both builders' docstrings) — but it now egresses to the Anthropic API. The plan's "no new egress risk" claim (§3.5) is **false for the LLM path**: descriptions leave the host. This is almost certainly acceptable (self-reported, non-marked, no solicitation CUI) but it is a *new egress decision* that must be made explicitly, not assumed. The embedding path kept everything on-host; the LLM path does not. Document and get sign-off.
- **Multi-tag (plan §6.2).** Embedding-sim assigns each tag independently with no notion of "this description is fundamentally about X, tangentially Y." The LLM handles this natively (`exp4` returned `[]`, single, and multi-tag appropriately), which removes the plan's own risk #2.
- **`gtm_mcp` `profiled` split (plan §4).** `sub_capability.py:54-56` `_PROFILE_COLUMNS` is scope-only and `profiled` means "bridge-enriched." This is independent of the mechanism choice and the plan's handling is fine — just note `profiled` should stay meaning "scope-enriched," and the new self-reported axis is orthogonal regardless of how it's produced.

---

## 8. Concrete remediation (ordered)

1. **Replace the mechanism: LLM controlled-vocab classification, not embedding-sim.** Where: a new derivation `govcon_sub_self_reported_tags` (sidecar table, grain = distinct `chunk_text` hash). Classify all 67,091 distinct descriptions through Haiku-class with the 77-tag controlled vocab as a cached system prompt + acronym glossary (proven in `exp4`), batched ~20/call. Expected effect: full-universe coverage with honest precision; terse/jargon descriptions resolved; `[]` for non-informative text. One-time ~$13.
2. **Roll chunk-text tags → per-sub tags deterministically in the profile build.** Where: `build_subawardee_capability_profiles.py` `_assemble`, LEFT JOIN the sidecar on the sub's description chunks, union tags, cap at `TAG_CAP`. No LLM call in the build → `verify --content-hash` idempotency preserved (model id + prompt version stamped into `snapshot_run_id`). Expected: deterministic rebuilds.
3. **Keep the plan's schema/provenance design verbatim.** Where: `govcon_gtm_schemas.py:279-352` add `self_reported_capability_tags`, `n_self_reported_tags`, `tag_source`; `+BITMAP(tag_source)`. The separate-field + gate analysis (plan §4 vs `map_decoders.py:113-124`) is correct and survives the mechanism swap. Expected: no corruption of the `has_extracted_scope` gate; consumers reason on provenance.
4. **Make the universe widening contingent on a coverage gate.** Where: `build_subawardee_capability_profiles.py:500-503` invariant. Only widen to 25,450 if the LLM classifier fills a confirmed-meaningful fraction (set a DoD floor, e.g. ≥60% of net-new subs get ≥1 tag). If LLM coverage is also thin, keep the universe = bridge and add the field as enrichment-only. Expected: no large frozen-schema blast for a sparse column.
5. **Make the API-egress decision explicit (CUI).** Where: plan §3.5. Record that LLM-classify egresses self-reported `subaward_description` to Anthropic; confirm acceptable (non-marked, sub-self-reported) and document. Expected: closes the egress gap the embedding path didn't have.
6. **Drop the embedding-sim path entirely** — including the prototype's per-tag threshold dict and the query-prefix "fix." Where: `sub_tag_classifier_proto.py` (retire). Expected: no calibration debt, no centroid/band tuning rabbit hole.

---

## 9. The one decision for the operator

**Spend ~$13 to LLM-classify all 67,091 distinct subaward descriptions once (Haiku, materialized to a frozen sidecar), instead of shipping the embedding-similarity classifier — accepting that self-reported descriptions now egress to the Anthropic API (sub-self-reported, non-CUI, but a new egress).** The embedding path is cheaper only in the sense that $0 < $13; it is more expensive in every dimension that matters (coverage 7% vs near-full, precision wrong-on-the-tail vs correct, plus a frozen-schema blast for a column it can't fill). If the egress is unacceptable, the honest fallback is **not** the embedding classifier — it is to **not ship `self_reported_capability_tags` at all** and keep Path B parked, because a low-coverage/low-precision self-reported tag set actively degrades the buyer-side targeting product.
