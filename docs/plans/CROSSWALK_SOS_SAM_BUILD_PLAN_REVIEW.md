# `crosswalk_sos_sam` — Build Plan, Adversarial Design Review

Reviewer stance: hostile. Goal is to find what is wrong, under-specified, or unsafe before a
data-plane write lands. Plan under review:
[`CROSSWALK_SOS_SAM_BUILD_PLAN.md`](CROSSWALK_SOS_SAM_BUILD_PLAN.md). Lineage read: the sidecar plan
+ its review ([`SAM_NORMALIZED_ENTITIES_BUILD_PLAN.md`](SAM_NORMALIZED_ENTITIES_BUILD_PLAN.md),
[`_REVIEW.md`](SAM_NORMALIZED_ENTITIES_BUILD_PLAN_REVIEW.md)), the geo-locus correction
([`FEC_SAM_EMPLOYER_BRIDGE_A_DIAGNOSTIC.md`](../reference/FEC_SAM_EMPLOYER_BRIDGE_A_DIAGNOSTIC.md)),
and the code: `core/name_norm.py`, `pipelines/resolution/crosswalk_hmda_gleif.py`,
`pipelines/resolution/crosswalk_sam_usaspending.py`, `pipelines/sam_gov/sam_normalized_entities.py`.

Every load-bearing number below is a **live read-only probe** of `s3://data-sink/active/` on
2026-06-05 (harnesses `/tmp/sos_sam_review.py` [P1], `/tmp/sos_sam_review2.py` [P2]; pylance 7 /
duckdb 1.5 / canonical `core.name_norm` via `PYTHONPATH`; **zero mutation** — no `write_dataset`, no
`create_*index`, no DDL, no git). Code patterns re-verified against the checked-in workers, not the
plan's self-description.

---

## 1. Verdict

**Ship with amendments.** The match *design* is right — the EXACT-primary / base-fallback key ladder
is the correct inverse of the FEC-employer call, geo-as-score-not-gate is correctly carried over from
the sidecar review's B2, and candidate-set + canonical-pick is the right multiplicity discipline. The
two schema traps in §0 are real and correctly designed around. **But the plan ships one gate that will
hard-fail every run and block the build, mis-bounds two of its emitted tiers, and leaves the single
highest-precision disambiguator (zip) and the only real precision floor (the base-only-no-geo junk
tier) unaddressed.** Specifically:

1. **Gate 5 is mis-specified and will fail every build (Blocker).** It asserts `state_confirms` ≈
   **73%** over "tier∈{1,3}-eligible exact+base matches," but that 73.03% was measured in §0 on
   *clean 1:1 exact matches only* — a denominator the build never computes. The actual full-join
   `state_confirms` rate is **34.44%** [P2]; exact-only is **48.38%** [P2]. No subset of the build's
   computed metrics yields 73%. As written this gate hard-fails on the first run and stops the ship.
2. **Tier 4 (base, no geo) is 32% of output and is overwhelmingly noise (Major).** Base-ONLY pairs
   confirm physical state at **9.86%** vs exact's 50.48% [P1] — over-peeled-base collisions across
   genuinely different companies. Emitting it unflagged at the same grain as tier 1 pollutes the
   crosswalk.
3. **Zip is the strongest disambiguator and the tier ladder ignores it (Major).** Adding zip to the
   tier-1 predicate cuts the multi-UEI collision rate from **1.72% → 0.79%** [P1] — a >2× precision
   lift — yet `match_tier` is built from `exact_name × state_confirms` only; zip feeds ranking but not
   the tier a consumer thresholds on.

Everything else is minor or resolves in the plan's favor. The base-superset join claim (§4) is
**correct and verified** — zero exact matches are lost (seed concern #2 is disproven, twice). The
redundancy charge (seed #7) resolves as ENDORSE — there is no existing SoS→UEI path. The rollback,
ops, and worker-skeleton mechanics are faithful clones of proven workers. Adopt §4's deltas and ship.

---

## 2. Headline findings

| # | Finding | Severity | One-line fix |
|---|---|---|---|
| **C1** | Gate 5 asserts `state_confirms` ≈73% over a denominator the build never computes; the real full-join rate is **34.44%** — gate hard-fails every run | **Blocker** | Re-spec Gate 5 to a real metric: `state_confirms / count(*)` over exact-name pairs ≈ **48%** (±5pp), or drop the % gate and floor `tier1_rows ≥ 200,000` |
| **C2** | Tier 4 (base, no geo) = 32.1% of rows but only **9.86%** state-confirm — over-peel noise emitted at tier-1 grain | **Major** | Demote: rename `match_tier 4`→`match_confidence='unsafe'`, **exclude tier 4 from `is_canonical` eligibility**, and gate `tier4 canonical ≤ 5%` |
| **C3** | Zip — the strongest disambiguator (collision 1.72%→**0.79%** when added) — never enters `match_tier`; only ranks | **Major** | Insert a zip tier: tier 1 = exact+state+zip, tier 2 = exact+state, shift the rest down; re-letter `match_confidence` |
| **C4** | Canonical tiebreak falls through to arbitrary `uei ASC` on **3.30%** of canonical rows (20,479 entities); 7,554 have a distinct `sam_extract_label` recency could break | **Major** | Insert `sam_extract_label DESC` (recency) into the canonical `ORDER BY` *before* `uei ASC`; carry the label into the output schema |
| **C5** | `entity_status` carried but unused; **16,167** TERMINATED + thousands FORFEITED/SUSPENDED land in tier-1 canonical with no demotion signal | **Minor** | Add `status_is_active` boolean (ACTIVE/GOOD STANDING) and insert it into the canonical `ORDER BY` after geo, before recency |
| **C6** | Gate 4 "coverage ±10% of 623,147 SoS entities / 400,325 UEIs" — probe says **620,317 / 400,325** post-dedup; fine, but `distinct uei` of **400,325** is the *base-superset* figure, not tier-1 | **Minor** | Keep the gate; clarify it floors total (all-tier) distinct, and add a separate `tier1 distinct_uei ≈ 211,350` observability line |
| **C7** | `match_confidence='high'` label attached to tier 1, but 1.72% of tier-1 entities still fan out to >1 UEI (max **75**) — "high" overstates single-entity certainty for branch/franchise names | **Minor** | Document in §10: tier-1 `high` = high *name+geo* precision, **not** a uniqueness guarantee; branch-heavy names (dialysis, electrical supply) legitimately fan out — consumer still filters `is_canonical` |
| **C8** | Sizing computed on raw 17.93M SoS rows; plan dedups first. Post-dedup recompute: **771,932** pairs / **620,317** canonical (vs plan's 775,517 / 623,147) | **Nit** | Update §0/§2/§7 baselines to the post-dedup figures; `ROW_FLOOR=500_000` holds comfortably |
| **C9** | Redundancy vs `crosswalk_hmda_gleif`'s "joinable to SoS master" claim | **Nit (resolve as ENDORSE)** | That crosswalk keys on **`lei`, not `uei`** — a different spine; no SoS→UEI path exists. `crosswalk_sos_sam` is distinct and justified — state it in §2 |

---

## 3. Per-finding detail

### C1 — Gate 5 will hard-fail every build: the 73% target has no matching denominator — **Blocker**

**Problem.** §7 Gate 5 reads: *"`state_confirms` rate among tier∈{1,3}-eligible exact+base matches ≈
73% (±5pp vs probe); guards against a locus regression."* The 73.03% figure comes from §0, where it
was explicitly measured on **218,195 clean 1:1 name matches** (one SoS row AND one SAM UEI per name).
That is a deliberately de-fanned subset. The build's emitted output is the full base-superset join,
where the `state_confirms` rate is dominated by the many-to-many fan-out and the base-only tier — it
is **nowhere near 73%**. A gate that asserts a value the dataset cannot produce fails closed on the
first run and blocks the ship, exactly the failure §9 warns against ("a gate failure stops the ship").

**Evidence** [P2]:
- `state_confirms` over **all** base-superset pairs: **265,838 / 771,932 = 34.44%**.
- `state_confirms` over **exact-name** pairs only: **242,037 / 500,233 = 48.38%**.
- The §0 73.03% is reproducible *only* on the clean-1:1 exact subset — which the build never
  materializes as a metric (the dry-run/`assert_pre_write_gates` path computes tier counts, not a
  1:1-restricted geo rate).

**Fix.** Re-spec Gate 5 to a metric the build actually computes. Two acceptable forms:
- **(preferred)** `state_confirms among exact-name pairs within 48% ±5pp` — i.e. compute
  `count(*) FILTER (state_confirms AND exact_name) / count(*) FILTER (exact_name)` on the Arrow table
  and floor it at `[0.43, 0.53]`. This is a real locus-regression guard: if someone wires
  `sos.source_state` (jurisdiction) instead of `sos.state` (physical), this rate drops measurably (the
  jurisdiction locus agrees less — §0's own 64.06% vs 73.03% on the clean subset).
- **(alternative)** Drop the percentage gate entirely and replace with a `tier1_rows ≥ 200,000` floor
  (probe: tier-1 = **242,037** pairs [P2]) plus the C6 observability. Simpler, equally catches a locus
  break (a wrong locus collapses tier-1).

Either way, **delete the "73%" assertion** — it is the single defect that stops the build from
running.

### C2 — Tier 4 (base, no geo) is 32% of the output and is mostly noise — **Major**

**Problem.** §3/§4 emit four tiers at one grain, and `is_canonical` is eligible across all of them.
Tier 4 = `legal_name_base` match with **no** state confirm and **no** exact-name confirm. The sidecar
review's B8 already flagged that `legal_name_base` over-peels (`CO` truncation, `INC`/`INCORPORATED`
non-collapse asymmetry); on this join that over-peel manifests as near-random cross-company collisions.

**Evidence** [P1]:
- Base-ONLY pairs (matched on base, **not** exact-name) confirm physical state at **9.86%**
  (23,801 / 241,447 geo-evaluable) — versus exact-name pairs at **50.48%**. A 9.86% state-agreement
  rate is barely above the ~1/50-states random floor scaled by population; these are predominantly
  *different companies that happen to share a peeled base*.
- Tier 4 is **247,898 pairs (32.1% of output)** and supplies the canonical UEI for **174,493 SoS
  entities (28.1% of canonical rows)** [P2] — i.e. nearly a third of the crosswalk's "answers" rest on
  the weakest, geo-unconfirmed, over-peeled signal.

**Fix.** Tier 4 is recall, but it must not masquerade as a resolved link:
1. Keep emitting tier-4 pairs (audit/recall value) but **exclude tier 4 from `is_canonical`
   eligibility** — change the canonical `row_number()` to `... WHERE match_tier < 4` semantics, or add
   `match_tier = 4` as the last (worst) sort key and additionally set `is_canonical = FALSE` whenever
   the winning tier is 4 and a consumer wants a "safe" pick. Cleanest: emit `is_canonical` only over
   tiers 1–3, and add a separate `is_canonical_incl_unsafe` if the full-coverage pick is ever needed.
2. Relabel `match_confidence` for tier 4 from `'low'` to **`'unsafe'`** so no consumer thresholds it as
   a weak-but-real link.
3. Add a gate: `tier-4 canonical share ≤ 5%` (it will be ~0 once excluded) — a regression tripwire.

### C3 — Zip is the strongest disambiguator and the tier ladder discards it — **Major**

**Problem.** §4 builds `match_tier` from `exact_name × state_confirms` only; `zip_confirms` feeds the
canonical *ranking* (§5 `ORDER BY ... zip_confirms DESC ...`) but not the *tier* a consumer thresholds
on. The §0 traps correctly establish geo-as-score; this finding is narrower — *within* the geo score,
zip is materially more discriminating than state and deserves its own tier.

**Evidence** [P1]:
- Tier-1 (exact + state) multi-UEI **collision rate = 1.72%** (4,043 / 234,637 SoS entities map to >1
  distinct UEI even after the state confirm), worst fan-out **75 UEIs** for one SoS entity.
- Adding **zip** to the tier-1 predicate (exact + state + zip): collision rate drops to **0.79%**
  (1,423 / 180,177) — a **2.2× precision improvement**, the single largest disambiguation lever short
  of NAICS.
- Tier-1 zip *agreement* itself is **75.15%** (181,897 / 242,037 pairs) — high enough that an
  exact+state+zip tier retains the bulk of tier-1 while shedding the collisions.

**Fix.** Promote zip into the tier ladder. New ladder:

| tier | predicate | `match_confidence` |
|---|---|---|
| 1 | exact + state + zip | `high` |
| 2 | exact + state (zip differs/null) | `medium_high` |
| 3 | exact (no state) | `medium` |
| 4 | base + state | `low` |
| 5 | base (no geo) | `unsafe` (C2: not canonical) |

SQL: `CASE WHEN exact_name AND state_confirms AND zip_confirms THEN 1 WHEN exact_name AND
state_confirms THEN 2 WHEN exact_name THEN 3 WHEN state_confirms THEN 4 ELSE 5 END`. The canonical
`ORDER BY` then leads with `match_tier ASC` and the redundant `zip_confirms DESC` / `state_confirms
DESC` terms can stay as belt-and-suspenders. Update the `match_tier int8` doc, the `match_confidence`
CASE, and the ops `tier1_rows` definition (now exact+state+zip; probe: **180,177** canonical-eligible
entities at the new tier 1).

### C4 — Canonical tiebreak falls through to arbitrary `uei ASC` on 3.3% of rows — **Major**

**Problem.** §5 ranks canonical by `match_tier ASC, state_confirms DESC, zip_confirms DESC,
sam_is_active DESC, uei ASC`. When ≥2 candidate UEIs tie on (tier, state, zip, is_active) — which
happens for genuine multi-location/branch entities sharing one legal name in one state+zip — the
*only* remaining discriminator is alphabetical `uei`. That is a coin flip that silently picks one
branch UEI over equally-valid siblings, and it is not rare.

**Evidence** [P2]:
- **20,479 SoS entities (3.30% of all canonical rows)** have their canonical UEI decided *solely* by
  `uei ASC` — i.e. their top rank-group (best tier/state/zip/is_active tuple) contains >1 distinct UEI.
- Of those, **7,554** have ≥2 distinct `sam_extract_label` values in the tied group — recency is
  available as a non-arbitrary tiebreak for over a third of them.
- The collision drivers are real branch/franchise registrations: `KNICKERBOCKER DIALYSIS INC` (75
  UEIs, NY), `CITY ELECTRIC SUPPLY COMPANY` (46, TX), `LEIDOS INC` (41, VA) [P1] — each location a
  distinct UEI under one legal name.

**Fix.**
1. Add `sam_extract_label DESC` to the canonical `ORDER BY` **immediately before `uei ASC`**:
   `... sam_is_active DESC, sam_extract_label DESC, uei ASC`. This resolves 7,554 ties to the
   most-recently-seen UEI (the live registration, not a stale branch) instead of an alphabetical
   accident. `uei ASC` stays as the final deterministic backstop for true ties.
2. **Carry `sam_extract_label` into the output schema** (the sidecar already exposes it; the plan's §3
   schema drops it). It is needed both for the tiebreak and for consumer audit of *which* vintage the
   canonical pick came from. Add `sam_extract_label string` to §3 and the `SELECT`.
3. Note in §10 that a branch-name canonical pick is "a representative UEI for this legal name in this
   geo," not "the one true UEI" — consumers needing all branches read the full candidate set.

### C5 — `entity_status` is carried but never used; terminated registrations win canonical — **Minor**

**Problem.** §3 carries `sos_entity_status` (audit) but §5's canonical ranking ignores it. A
TERMINATED/FORFEITED/SUSPENDED SoS registration that matches an active SAM UEI is a valid *historical*
link, but when an entity has both an ACTIVE and a TERMINATED registration matching, the pick should
prefer the live one. More importantly, status is a free precision signal the ranking discards.

**Evidence** [P1] (tier-1 canonical SoS entities by status):
- ACTIVE **162,911** + GOOD STANDING **37,668** = ~200k live.
- TERMINATED **16,167**, SUSPENDED-FTB **4,676**, FORFEITED-FTB **3,433**, INACTIVE **3,136**,
  DELINQUENT 1,479, MERGED/CONVERTED OUT ~1,795 — i.e. **~30k tier-1 canonical picks rest on a
  dead/dormant SoS registration**, with no signal distinguishing them from live ones.

**Fix.** Add a derived `status_is_active = sos_entity_status IN ('ACTIVE','GOOD STANDING')` boolean to
the output, and insert it into the canonical `ORDER BY` **after geo, before recency**:
`... zip_confirms DESC, status_is_active DESC, sam_is_active DESC, sam_extract_label DESC, uei ASC`.
When a SoS entity has multiple registrations matching, the live one wins the canonical slot. Do **not**
filter on status (dead registrations are legitimate historical links — the candidate set keeps them).

### C6 — Coverage gate denominators conflate all-tier and tier-1 distinct-UEI — **Minor**

**Problem.** §7 Gate 4 floors `distinct uei` within ±10% of **400,325** and `distinct sos_entity_key`
within ±10% of **623,147**. The 400,325 is the *base-superset* (all-tier) distinct-UEI count; a reader
could misread it as the trustworthy reach. Post-dedup the entity figure is **620,317**, not 623,147
(the plan's number was on raw rows).

**Evidence** [P1/P2]: base-superset `distinct uei = 400,325`, `distinct sos_entity = 620,317`. Tier
breakdown of distinct UEIs: tier 1 = **211,350**, tier 2 = 157,198, tier 3 = 19,336, tier 4 = 130,381
[P2] (these overlap across tiers; the 400,325 is the union).

**Fix.** Keep Gate 4 but (a) update the entity baseline to **620,317**, (b) annotate that 400,325 is
the all-tier union (recall ceiling), and (c) add a non-failing observability line for tier-1 distinct
UEIs (~211,350) so the *trustworthy* reach is visible alongside the recall reach.

### C7 — `match_confidence='high'` overstates single-entity certainty — **Minor**

**Problem.** §3 maps tier 1 → `match_confidence='high'`. But 1.72% of tier-1 SoS entities still map to
>1 UEI [P1]; "high" reads as "this is *the* UEI," which is false for branch-heavy names.

**Evidence** [P1]: max tier-1 fan-out **75 UEIs** (KNICKERBOCKER DIALYSIS); the collided names are
systematically multi-location franchises (dialysis chains, electrical-supply distributors, defense
primes with many registered subsidiaries).

**Fix.** Documentation only — no schema change beyond C3's relabel. In §10 state: *"`match_tier=1`
(`high`) denotes high name+geo precision, not a uniqueness guarantee; multi-location entities (≈1.7% of
tier-1) legitimately fan out to N branch UEIs under one legal name. Filter `is_canonical` for a single
representative; read the full set for all branches."* This is the same honesty the sidecar review's B3
demanded of the FEC contract.

### C8 — Sizing was on raw 17.93M rows; the plan dedups first — **Nit**

**Problem.** §0's 775,517-pair / 623,147-entity figures were computed on raw `sos_normalized_master`
rows; §5 dedups to 1-row/entity (QUALIFY latest `snapshot_date`) *before* the join. The figures shift
slightly.

**Evidence** [P1/P2] (recomputed on the deduped-to-entity left side):
- base-superset pairs: **771,932** (vs plan 775,517).
- canonical / distinct matched entities: **620,317** (vs plan 623,147).
- exact-name pairs: **500,233** (vs plan 502,102); shared exact keys 306,413 (matches).
- The deltas are <0.5% — the raw-vs-deduped gap is small because SoS is already near-1-row-per-entity
  (17,926,543 rows / 17,843,031 entities [P1]).

**Fix.** Update the §0 table, §2 "Est. rows ~775k," and §7 Gate-4 baselines to **771,932 / 620,317**.
`ROW_FLOOR=500_000` is unaffected (comfortable margin). The `±25%` Δ-guard (Gate 7) is unaffected.

### C9 — No redundancy with `crosswalk_hmda_gleif` — **Nit (resolve as ENDORSE)**

**Problem.** `crosswalk_hmda_gleif.py`'s docstring says its `normalized_legal_name` is "joinable to the
cross-state SoS master," which could read as an existing SoS→federal-entity path overlapping this
build.

**Evidence** (code): `crosswalk_hmda_gleif.py` carries **`lei`** as its spine key (`BTREE_INDEXES =
["lei", "normalized_legal_name"]`, line 76) and **no `uei`** (grep: zero `uei` references). Its
SoS-joinability is on the *name* axis to land HMDA/GLEIF entities on the GLEIF LEI spine — an entirely
different identifier than SAM's UEI. Repo grep confirms **no existing dataset joins SoS to UEI**
(`pipelines/resolution/`: only `crosswalk_sam_usaspending` [USAspending-anchored, no SoS],
`recon_ca_ucc_sos` [CA-only UCC recon], `sam_fmcsa_domain_spine` [FMCSA]).

**Fix.** Add one line to §2: *"Not redundant with `crosswalk_hmda_gleif` (keys on LEI, not UEI) or
`crosswalk_sam_usaspending` (USAspending-recipient-anchored, no SoS side). `crosswalk_sos_sam` is the
only SoS-registration → federal-UEI path in the fleet."* Mirrors the sidecar review's B6 defense.

---

## 4. Amended-plan delta (the precise edits)

Apply to `CROSSWALK_SOS_SAM_BUILD_PLAN.md`. None changes the build's *shape*; they fix the failing
gate, the tier ladder, the canonical ranking, and the baselines.

**§3 — Output schema**
- Add `sam_extract_label string` (audit + C4 recency tiebreak) — drop from the sidecar, carry through.
- Add `status_is_active bool` = `sos_entity_status IN ('ACTIVE','GOOD STANDING')` (C5).
- Re-doc `match_tier int8` for the 5-tier ladder (C3): `1=exact+state+zip · 2=exact+state · 3=exact ·
  4=base+state · 5=base`.
- Re-doc `match_confidence`: `high(1) / medium_high(2) / medium(3) / low(4) / unsafe(5)` (C2/C3).
- Re-doc `is_canonical`: "best UEI per `sos_entity_key` **over tiers 1–4 only** (tier 5 = base-no-geo
  is recall, never canonical, C2)."

**§4 — Match design**
- Note the base-superset claim is **verified**: 0 exact matches lost at entity grain (P2) — every name
  that is purely a peeled designator would have to *be* a designator, which does not occur in SoS.
- Rewrite the geo paragraph: geo enters the **tier** (zip is tier-1-defining, C3), not only the rank.

**§5 — Transform SQL**
- `match_tier` CASE → the 5-tier ladder (C3 SQL above).
- Canonical `ORDER BY` →
  `match_tier ASC, state_confirms DESC, zip_confirms DESC, status_is_active DESC, sam_is_active DESC,
  sam_extract_label DESC, uei ASC` (C4 + C5).
- `is_canonical` window restricted to `match_tier <= 4` (compute `row_number()` over a `WHERE
  match_tier < 5` subset, or set `is_canonical = (rn = 1 AND match_tier <= 4)`).
- Add `m.sam_extract_label AS sam_extract_label` and the `status_is_active` derivation to the `SELECT`.

**§7 — Validation gates (the substantive fix)**
- **Replace Gate 5 (C1 — Blocker).** Delete the "≈73%" assertion. New Gate 5:
  `state_confirms among exact-name pairs within [0.43, 0.53]` (computed on the Arrow table as
  `count FILTER(state_confirms AND exact_name) / count FILTER(exact_name)`; probe baseline **48.38%**).
  Locus-regression guard intact.
- **New Gate 5b (C2):** `tier-5 canonical share == 0` (tier 5 must never win canonical) and `tier-5
  rows ≤ 35% of total` (recall sanity; probe ~32%).
- **Update Gate 4 baselines (C8):** `distinct sos_entity_key` within ±10% of **620,317**; annotate
  400,325 as the all-tier union.
- **New observability:** tier-1 (exact+state+zip) distinct UEIs ≈ **180,177 entities / ~150k UEIs**;
  `uei ASC`-only-decided canonical count (C4, target trending toward 0 after the recency tiebreak);
  `status_is_active=FALSE` canonical share.
- Renumber the post-write gates (currently 8–10) if the new gates take 5b — keep them post-write.

**§8 — ops ledger**
- Redefine `tier1_rows` to the new tier-1 (exact+state+zip). Add `tier5_rows bigint` (the unsafe
  recall tier) so the noise tier is tracked.

**§2 / §10 — Contract + non-redundancy**
- Add the C9 non-redundancy line (LEI ≠ UEI; only SoS→UEI path).
- Add the C7 honesty note: tier-1 `high` is name+geo precision, not uniqueness; branch entities fan
  out legitimately.

**§0 — Sizing**
- Update to post-dedup figures (C8): 771,932 pairs / 620,317 canonical. Keep the §0 traps verbatim
  (they are correct), but **mark the 73.03% explicitly as "clean-1:1-exact subset only — not a
  gate-able full-join metric"** so the C1 trap is not re-introduced.

---

## 5. What I verified live vs. reasoned-but-unverified

**Verified by live probe (2026-06-05, zero mutation):**
- **Tier-1 collision (the crux, seed #1):** 1.72% of tier-1 SoS entities (4,043 / 234,637) map to >1
  UEI after the state confirm; max fan-out **75**; collisions are branch/franchise names
  (KNICKERBOCKER DIALYSIS, CITY ELECTRIC SUPPLY, LEIDOS) [P1].
- **Zip precision lift (seed #3):** exact+state collision 1.72% → exact+state+zip **0.79%** (2.2×);
  tier-1 zip agreement 75.15% [P1].
- **Base-only is noise (seed #4):** base-ONLY pairs state-confirm **9.86%** vs exact **50.48%** [P1].
- **Base-superset loses nothing (seed #2 — DISPROVEN):** 0 exact-name pairs with NULL base; 0
  exact-match SoS entities missing from the base-superset output at entity grain [P1/P2]. The plan's §4
  superset claim is correct.
- **Canonical tiebreak arbitrariness (seed #6):** 3.30% of canonical rows (20,479) decided by `uei
  ASC` alone; 7,554 have a distinct `sam_extract_label` recency could break [P2].
- **`entity_status` distribution (seed #5):** ~30k tier-1 canonical picks rest on dead/dormant SoS
  registrations (16,167 TERMINATED + FORFEITED/SUSPENDED/INACTIVE) [P1].
- **Gate-5 denominator (C1):** full-join `state_confirms` 34.44%, exact-only 48.38% — neither is the
  §0 73.03% (which is the clean-1:1 subset) [P2].
- **Post-dedup sizing (seed #8):** 771,932 pairs / 620,317 canonical (vs plan 775,517 / 623,147);
  per-tier pair + distinct-UEI breakdown [P1/P2].
- **Redundancy (seed #7):** `crosswalk_hmda_gleif` keys on `lei` not `uei`; no SoS→UEI path exists in
  `pipelines/resolution/` (grep) — ENDORSE.
- **Code patterns:** `crosswalk_sam_usaspending.py:520–598` has the `version=v_before).restore()`
  rollback (the plan correctly cites it); `crosswalk_hmda_gleif.py` is a faithful clonable skeleton;
  `core.name_norm.legal_name_base` peels `(LLC|INC|CORP|CO|LTD|PLC)+$` (read directly — confirms the
  `CO` over-peel and the all-suffix→NULL mechanism the seed #2 concern raised, which the data then
  showed does not bite SoS).

**Reasoned but NOT independently verified (flagged honestly):**
- **NAICS plausibility as a tie/precision signal** — not probed. The plan carries `sam_primary_naics`
  but no `sos` sector field to compare against, so a NAICS confirm would need a third source. I did
  **not** quantify whether NAICS agreement would further cut the 0.79% exact+state+zip collision rate;
  it is a plausible Phase-2 lever, not a blocker.
- **`snapshot_date` is the correct SoS dedup key** — I used it (it exists in the schema) and the
  deduped count (17,843,031) matches the operator's stated entity count (17,843,033, off by 2, likely
  a null-name drop in my `WHERE normalized_legal_name IS NOT NULL`), so the QUALIFY is sound. I did
  not audit whether a *different* tiebreak (e.g. `entity_status` recency) would change which SoS row
  survives per entity — immaterial to the join keys, possibly material to `sos_entity_status` accuracy.
- **The new Gate-5 band [0.43, 0.53]** is set ±5pp around the probed 48.38% exact-only rate; I did not
  stress whether a legitimate SAM/SoS vintage refresh could drift it past ±5pp. It is a reasonable
  starting band; widen to ±7pp if the first post-refresh run trips it.
- **Point-lookup latency (Gate 10's <2s ceiling)** — not benchmarked; inherited reasonable-expectation
  from the sidecar review's identical caveat.
- **The `sam_extract_label` recency tiebreak resolves the *right* UEI** — I verified it *can*
  disambiguate 7,554 ties (distinct labels exist), not that the most-recent label is always the
  correct branch. For a multi-branch entity "most recently re-registered in SAM" is a defensible
  proxy, but it is a heuristic, not ground truth.

---

## 6. Bottom line

The match design is sound and the schema traps are correctly handled — this is a well-reasoned plan,
not a flawed approach. But it **cannot run as written**: Gate 5 asserts a 73% `state_confirms` rate
that exists only in a clean-1:1 subset the build never computes, while the real full-join rate is
34.44% — the gate fails closed on run one (C1, Blocker). Beyond that, the plan emits a 32%-of-output
base-no-geo tier that confirms geo only 9.86% of the time as if it were a real link (C2), and it leaves
the strongest disambiguator — zip, which more than halves the tier-1 collision rate — out of the tier a
consumer thresholds on (C3). The canonical pick coin-flips on `uei ASC` for 3.3% of rows when recency
and SoS status are sitting right there unused (C4/C5). Fix the failing gate, promote zip into the tier
ladder, demote the base-no-geo tier out of canonical eligibility, and add recency+status to the
tiebreak. With those five edits the crosswalk is trustworthy and ships. **The verified good news:** the
base-superset join loses zero exact matches (seed #2 disproven), and there is no redundant SoS→UEI path
(seed #7 ENDORSE) — two of the seed concerns resolve cleanly in the plan's favor.
