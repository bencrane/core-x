# `sam_normalized_entities` — Sidecar Build Plan, Adversarial Design Review

Reviewer stance: hostile. Goal is to find what is wrong, under-specified, or unsafe before a
data-plane write lands. Plan under review:
[`docs/plans/SAM_NORMALIZED_ENTITIES_BUILD_PLAN.md`](SAM_NORMALIZED_ENTITIES_BUILD_PLAN.md).
Every load-bearing number below is a **live read-only probe** of `s3://data-sink/active/` on
2026-06-05 (harnesses `/tmp/probe_fanout.py`, `/tmp/probe_fec_match.py`; pylance 7 / duckdb 1.5 /
canonical `core.name_norm` imported via `PYTHONPATH`; **zero mutation** — no `write_dataset`, no
`create_*index`, no DDL, no git). Code patterns were re-verified against the checked-in workers, not
taken from the plan's self-description.

---

## 1. Verdict

**Ship with amendments.** The build mechanics are sound and almost entirely correct: the
sidecar-vs-mirror argument (§2) is right, the SQL is a faithful clone of the proven
`sos_normalized/normalize.py` idiom, the index set is appropriate, and the dataset *as a derived
projection* is cheap and safe to materialize. The problem is **not the build — it is the consumer
contract (§10) the build exists to serve**, and that contract is, as written, a false-positive and
false-negative engine. Three things must change before this sidecar is fit for the FEC employer
bridge it is being built for:

1. **The primary blocking key is the wrong one.** The plan makes `normalized_legal_name` primary and
   `legal_name_base` a "Pass-2" afterthought. Live FEC evidence inverts this: `legal_name_base`
   resolves **+11.98pp more contributions** (28.98% vs 17.0%) because donors type "GOOGLE", SAM
   stores "GOOGLE LLC". `legal_name_base` is the primary FEC-employer key; `normalized_legal_name` is
   the precision refinement. (B1)
2. **The §10 hard geo gate silently deletes a third of true matches.** `e.physical_state = f.state2`
   as a JOIN predicate is invalid: `f.state2` is the donor's **home** state, `e.physical_state` is the
   employer's **HQ** state. **32.67% of legitimate name-matches have home ≠ HQ** (55.5% agreement for
   national employers). Geo must be a *confirmatory score*, not a hard equality. (B2)
3. **`name → uei` is many-to-one and the contract has no multiplicity handling.** 37,598 names
   (2.56%) map to >1 UEI; **112,400 UEIs (7.29% of the dataset) sit under a non-unique name**; worst
   case fan-out is **2,184 UEIs** for one name (`THE SHERWIN WILLIAMS COMPANY`). The §10 join emits a
   silent cartesian fan-out with no candidate-set / confidence / canonical-pick discipline. (B3)

None of the three is a defect in *this* dataset's bytes — they are defects in the **join semantics
the plan ships as its reason to exist (§10)** and in the **key priority (§4)**. They are fixable with
column/gate/doc edits, not a rebuild of the approach. Everything else is minor. The sidecar is
genuinely *not* redundant with `crosswalk_sam_usaspending` (B6 — confirmed: 100% vs 51.6% name
coverage, and the crosswalk has no geo). Adopt the amendments in §4 and ship.

---

## 2. Headline findings

| # | Finding | Severity | One-line fix |
|---|---|---|---|
| **B1** | `normalized_legal_name` made primary; `legal_name_base` demoted to Pass-2 — but the peeled base resolves **+11.98pp** more FEC contributions | **Major** | Re-rank: `legal_name_base` = primary FEC-employer block, `normalized_legal_name` = precision tiebreak; document in §4/§10 |
| **B2** | §10 `e.physical_state = f.state2` hard JOIN predicate kills **32.67%** of true matches (donor home ≠ employer HQ) | **Major** | Demote geo from JOIN predicate to a `CASE`-scored confirmatory boost; never an equality filter for employer matching |
| **B3** | `name → uei` is many-to-one (37,598 names → >1 uei; max 2,184); §10 fans out one employer to N UEIs with no discipline | **Major** | Make the contract emit a **candidate set + confidence**, not a single row; document the canonical-pick rule (geo+is_active+recency) downstream |
| **B4** | Column names `physical_state`/`physical_zip5` diverge from `sos_normalized_master` (`source_state`/`zip_code`) — the deferred union is **not** a literal `UNION ALL` | **Minor** | Rename now: `physical_state`→`source_state`, `physical_zip5`→`zip_code`; keep a `source_dataset` discriminator (already planned) |
| **B5** | `mode="overwrite"` fires before Gates 8/9/10 (post-write only) — a `name_norm` regression overwrites the good dataset before the gate can catch it | **Minor** | Move all pre-write-computable asserts onto the **Arrow table before write**; wrap the write in the proven `version=v_before).restore()` rollback (crosswalk:576) |
| **B6** | Plan does not defend against the "redundant with `crosswalk_sam_usaspending`" charge | **Minor (resolve as ENDORSE)** | State the delta explicitly: 100% vs 51.58% name fill, +geo, all-time (incl. 759k historical) vs recipient-anchored 1.03M — the sidecar is justified |
| **B7** | `is_active` semantics under-specified: default §10 join matches deregistered employers; `is_active` alone disambiguates only **41.27%** of multi-uei names | **Minor** | Keep all rows (correct), but document: default consumer prefers `is_active`, falls back to most-recent; `is_active` is a tiebreak not a filter |
| **B8** | `legal_name_base` peel set includes `CO` → **11,147 rows** lose a real `CO` token (`WOODMANS BREWING CO` → `WOODMANS BREWING`) | **Minor** | Accept (small, and the *gain* is large) but log the count in the verify step; do **not** touch `core.name_norm` here (fleet-wide key) |
| **B9** | Gate 5 cardinality target hard-codes `1,466,764 ±5%`; the BTREE on `legal_name_base` has higher fan-out (3.43% multi-uei) than the plan implies | **Nit** | Confirmed `legal_name_base` distinct = 1,450,598; keep the gate, add a `legal_name_base` distinct floor |

---

## 3. Per-finding detail

### B1 — Wrong primary blocking key: `legal_name_base` beats `normalized_legal_name` by +11.98pp on the actual FEC traffic — **Major**

**Problem.** §4 ranks `normalized_legal_name` as the "primary blocking key" and §10 codes it as the
Pass-1 join (`e.normalized_legal_name = f.emp_key`), with `legal_name_base` relegated to a Pass-2
"drift" comment. This inverts the empirical reality for the one consumer the sidecar is built for. FEC
donors free-type the bare brand ("GOOGLE", "BOEING", "DELTA"); SAM stores the registered legal form
("GOOGLE LLC", "THE BOEING COMPANY"). The exact `normalized_legal_name` join therefore *misses* the
suffix the donor never typed.

**Evidence** (`/tmp/probe_fec_match.py`, full `entity_tp='IND'` scan, sentinels dropped — 3,805,077
distinct employer keys over 100,619,839 contributions):

| Right-side key | distinct employer keys matched | contributions matched |
|---|---|---|
| `normalized_legal_name` (exact) | 126,495 (3.32%) | 17,105,160 (**17.0%**) |
| `legal_name_base` (peeled) | 284,508 (7.48%) | 29,162,366 (**28.98%**) |
| **incremental** (base catches, exact misses) | 158,013 (4.15%) | 12,057,206 (**+11.98pp**) |

The peeled base key resolves **70% more contributions** than the exact key. Making it "Pass-2" means
the bridge's Pass-1 leaves the single largest recall lever on the floor.

**Fix.** Re-rank in §4 and §10:
- **Primary block = `legal_name_base`** for the free-text FEC employer surface. The consumer joins
  `sam.legal_name_base = legal_name_base(f.emp_key)` first.
- `normalized_legal_name` becomes the **precision refinement**: among the `legal_name_base` candidate
  set, an exact `normalized_legal_name` hit is the highest-confidence tier.
- Keep both BTREE-indexed (the plan already does — no schema change, only key-priority + the §10
  contract doc). This is a documentation/contract correction, not a build change.

Note this raises the collision question (B8) — `legal_name_base` fans out more (3.43% vs 2.56%
multi-uei) — which is exactly why the candidate-set discipline in B3 is mandatory, not optional.

### B2 — The §10 hard geo gate is a false-negative engine: 32.67% of true matches have donor-home ≠ employer-HQ — **Major**

**Problem.** §10 (and the §3 diagnostic it inherits from) makes geo a **hard JOIN predicate**:
`AND e.physical_state = f.state2`. The diagnostic even asserts ZIP is "a **hard tiebreaker**" for
Bridge A because "both sides are the corporate address." **That premise is false on the FEC side.**
The seed brief is correct and the diagnostic is wrong: `f.state2`/`f.zip5` are the donor's **home**
address (FEC itemized individual records carry the *contributor's* residence), while
`e.physical_state` is the *employer's HQ*. An engineer in Texas who donates while employed by a
California-HQ firm has `state2='TX'`, `physical_state='CA'` — a true match the hard gate deletes.

**Evidence** (`/tmp/probe_fec_match.py`, matched contributions where the SAM name is unique to a
single UEI and single HQ state, so the comparison is unambiguous — 11,099,118 matched contributions):

- Donor home state == employer HQ state: **7,473,083 (67.33%)**.
- **→ 32.67% of legitimate, unambiguous name-matches would be silently dropped by the hard state
  gate.**
- Segmented by the employer's donor footprint (how many states its donors live in):

  | Employer footprint | matched contributions | home==HQ agreement |
  |---|---|---|
  | 1 state (hyper-local) | 1,708,785 | 89.82% |
  | 2–5 states | 2,035,907 | 73.09% |
  | 6–20 states | 2,876,562 | 68.31% |
  | 20+ states (national) | 4,477,864 | **55.50%** |

For national employers — exactly the high-value, high-volume names — the hard gate is barely better
than a coin flip and destroys nearly half the true links.

**Fix.** Geo is a **confirmatory score, never an equality JOIN predicate** for employer matching
(contrast Bridge B's POC bridge, where both sides *are* the corp address and the diagnostic's
hard-gate logic would hold). Rewrite the §10 contract:

```sql
-- block on the NAME (legal_name_base primary, normalized_legal_name precision);
-- geo is a SCORE over the candidate set, not a filter.
SELECT f.sub_id, e.uei, e.is_active, e.primary_naics,
       (e.normalized_legal_name = f.emp_key)        AS exact_name,   -- precision tier
       (e.physical_state        = f.state2)         AS state_confirms,
       (e.physical_zip5         = f.zip5)           AS zip_confirms
FROM   fec_left f
JOIN   sam_normalized_entities e
  ON   e.legal_name_base = legal_name_base(f.emp_key);   -- BLOCK on name only
-- rank candidates by (exact_name, state_confirms, zip_confirms, is_active, recency);
-- geo BOOSTS confidence, it does NOT gate membership.
```

State agreement raises confidence; its **absence does not exclude**. This is the single most
important correction in the review — the current contract would publish a bridge that looks precise
and is quietly missing a third of its truth set.

### B3 — `name → uei` is many-to-one; the contract has no fan-out discipline — **Major**

**Problem.** The plan's §3 table says "1 row / `uei` (pure passthrough)" and treats the dataset as a
clean lookup. It is clean *on the uei axis*, but the **join axis is the name**, and names are not
unique to a UEI. §10 joins `ON e.normalized_legal_name = f.emp_key` and emits one FEC row × every UEI
sharing that name — a silent cartesian fan-out. The plan never quantifies this or prescribes how the
bridge collapses it.

**Evidence** (`/tmp/probe_fanout.py`, full 1,541,566-row projection):

- distinct `normalized_legal_name` = **1,466,764** (confirms the seed figure exactly).
- Names mapping to **>1 uei: 37,598 (2.563% of names)**.
- **UEIs sitting under a multi-uei name: 112,400 (7.291% of all rows)** — this is the right-side blast
  radius: 1-in-14 UEIs is reachable by an ambiguous name.
- uei-per-name distribution: p50=1, p99=2, p99.9=6, **max=2,184**.
- Worst offenders: `THE SHERWIN WILLIAMS COMPANY` (2,184 UEIs — store/branch registrations),
  `TOTAL RENAL CARE INC` (836), `CITY ELECTRIC SUPPLY COMPANY` (490). These are real multi-location
  franchise/branch entities, each location a distinct UEI under one legal name.
- On the **`legal_name_base`** axis the plan should adopt as primary (B1), fan-out is *worse*: 49,710
  base keys (3.43%) are multi-uei.

Geo collapses this substantially **as a scoring/ranking tool** (not as the hard gate B2 forbids):
`(nln, state)` blocks drop to 1.379% ambiguous; `(nln, state, zip5)` to **0.703%** (p99 fan-out = 1,
max = 38). So geo *is* the right disambiguator — it just has to be applied as a ranker over the
candidate set, not as a join filter.

**Fix.** The §10 contract must define an explicit **candidate-set → canonical-pick** discipline; the
sidecar stays 1/uei (correct) but the *bridge* must:
1. Block on `legal_name_base` (B1) → candidate UEI set per employer.
2. Score each candidate: `exact_name` (B1) + `state_confirms` + `zip_confirms` (B2) + `is_active`
   (B7) + extract recency.
3. Resolve to either (a) a **single canonical UEI** by deterministic priority
   (`exact_name DESC, state_confirms DESC, zip_confirms DESC, is_active DESC, last_seen_label DESC`),
   or (b) a **candidate set with a confidence column** when the top tier is still tied — never a
   silent N-row fan-out.
4. Emit a `match_confidence` / `match_method` column so downstream consumers can threshold.

This belongs in the **`crosswalk_fec_sam_employer` plan** (the §10 consumer), but §10 must stop
implying a clean single-row join. Add one paragraph to §10: *"`normalized_legal_name` is not unique
to a UEI (2.56% of names, max 2,184); the bridge MUST resolve the per-name candidate set via the
scoring rule above and MUST NOT emit an unranked fan-out."*

### B4 — Spine-misalignment: column names block the deferred `sos_normalized_master` union — **Minor**

**Problem.** §12 defers "Union of `sam_normalized_entities` + `sos_normalized_master` into a
cross-source name spine — later." But §4 names the geo columns `physical_state` / `physical_zip5`,
while the existing spine (`pipelines/sos_normalized/normalize.py:80`, schema lines 13–18) uses
`source_state` / `zip_code`. A future union is therefore **not** a literal `UNION ALL` — it needs a
rename/migration pass, which (per the entity-master review's B10/B13) is exactly the kind of silent
break renames cause.

**Evidence.** `sos_normalized_master` blocking schema: `normalized_legal_name`, `legal_name_base`,
**`zip_code`**, plus `source_state` in the address block and `source_state`/`source_dataset`
provenance. The plan's sidecar: `normalized_legal_name`, `legal_name_base`, **`physical_zip5`**,
`physical_state`. Two of the three blocking/geo columns have different names.

**Fix.** Adopt the spine's exact names **now**, at zero cost (this is a greenfield dataset):
- `physical_state` → **`source_state`**
- `physical_zip5` → **`zip_code`**

Keep the already-planned `source_dataset` constant so a future `UNION ALL` discriminates SAM rows from
SoS rows. The §10 contract and §4 schema table update accordingly. One caveat to document: the spine
lacks `uei` (it is SoS-only) and the sidecar lacks the spine's `original_entity_id`/`entity_status`;
the union is a name-spine with nullable source-specific columns, which `source_dataset` already
handles. This makes the §12 union a literal append, not a migration.

### B5 — Publish ordering: `overwrite` precedes the only gates that can catch a `name_norm` regression — **Minor**

**Problem.** §5/§6 do `lance.write_dataset(... mode="overwrite")` then `create_scalar_index`, and §7
Gates 8 (indices present), 9 (KIPPER round-trip), 10 (point-lookup) are explicitly **post-write**
("Run on the dry-run first … then re-assert in the build before publish"). But Gates 8–10 can only
run *after* the overwrite has already replaced the good dataset. If a future `core.name_norm` change
regresses the key (the exact failure mode the module's own docstring exists to prevent), the bad
dataset is already live before any post-write gate fires. The plan asserts Lance versioning is
implicitly a safety net but never wires a rollback.

**Evidence.** The repo **already has the correct pattern** and the plan does not cite it:
`crosswalk_sam_usaspending.py:520–598` (`patch_normalized_name`) computes the key, then runs an
integrity gate (`normalized_legal_name IS DISTINCT FROM {macro}` mismatch count, row-count stability,
index presence) and on failure does `lance.dataset(DATASET_URI, version=v_before).restore()` before
re-raising. That is the model.

**Fix.** Two changes, both cheap:
1. **Pre-write asserts.** Every gate computable on the in-memory Arrow table — Gates 1 (row floor),
   2 (1:1 passthrough), 3 (uei uniqueness), 4 (key fill ≥99.9%), 5 (cardinality), 6 (geo co-fill) —
   must run **on the Arrow table before `write_dataset`**, and hard-fail the build *before* any
   overwrite. The dry-run already proves the counts; the build must re-prove them in-memory, not just
   trust the dry-run.
2. **Restore-on-failure wrapper.** Capture `v_before = lance.dataset(uri).version` before the write;
   if any post-write gate (8/9/10) fails, `lance.dataset(uri, version=v_before).restore()` and
   re-raise — verbatim the crosswalk pattern. For a net-new dataset `v_before` is empty, but this is
   the durable guard for every *subsequent* rebuild, which is when a `name_norm` regression actually
   bites.

### B6 — Redundancy charge vs `crosswalk_sam_usaspending`: not redundant — but the plan must say so — **Minor (resolve as ENDORSE)**

**Problem.** `crosswalk_sam_usaspending` already exposes a BTREE `normalized_legal_name → uei`. A
reviewer (or a future engineer) will reasonably ask why a second normalized-name→UEI surface is
justified. The plan never defends it, leaving the sidecar open to a "delete the duplicate" challenge.

**Evidence** (probes + `crosswalk_sam_usaspending.py`):

| | `crosswalk_sam_usaspending` | `sam_normalized_entities` (proposed) |
|---|---|---|
| `normalized_legal_name` fill | **51.58%** (530,359 of 1.03M rows; 503,719 distinct) | **100%** (1,466,764 distinct) |
| `legal_name_base` | **absent** | present + BTREE |
| geo (state/zip) | **none** | `source_state` 96.17%, `zip_code` 98.2% |
| universe | 1.03M recipient-anchored UEIs (USAspending-spend-having) | **1,541,566** all-time SAM (incl. 759,023 historical) |
| anchor | USAspending recipient_lookup spine | SAM registrant universe |

The crosswalk's name key exists only where a UEI also has USAspending spend *and* a coalesced legal
name survived — half the rows are null, and it carries **no geo**, so it cannot run the B2 geo score
or the B3 disambiguation at all. The sidecar is the *only* surface with 100% name coverage + geo +
the historical tail (which is where past-employer matches live).

**Fix.** Add one line to §2 or §10: *"Not redundant with `crosswalk_sam_usaspending`'s
`normalized_legal_name` BTREE: that key is 51.6% fill, recipient-anchored, and geo-less; the sidecar
is 100% fill over the full 1.54M registrant universe with inline geo — the only surface that can run
the geo-scored, fan-out-disciplined employer match."* No build change; this is a defensive
justification the plan currently lacks.

### B7 — `is_active` inclusion is right, but the default match semantics are under-specified — **Minor**

**Problem.** The sidecar correctly includes all 1,541,566 rows (782,543 active + 759,023 historical).
§4 BITMAP-indexes `is_active`. But §10's default join has no `is_active` predicate, so a donor matches
long-deregistered employers with no preference signal — and the plan implies `is_active` cleanly
resolves multiplicity, which it does not.

**Evidence** (`/tmp/probe_fanout.py`, the 37,598 multi-uei names):
- Exactly 1 of the colliding UEIs is active (so `is_active` picks a unique winner): **15,515
  (41.27%)**.
- 0 active (all historical — no active winner to prefer): **8,544 (22.72%)**.
- >1 active (still ambiguous after the `is_active` filter): **13,539 (36.01%)**.

So `is_active` resolves only ~41% of fan-out cleanly; for 36% it leaves multiple active candidates
(this is the Sherwin-Williams branch case — many active UEIs, same name), and for 23% there is no
active candidate at all (historical-only employer — still a valid past-employer match).

**Fix.** Keep all rows (do **not** ship an active-only variant — historical employers are legitimate
past-employer links). Document in §10: *(1)* `is_active` is a **tiebreak in the candidate ranking
(B3), not a membership filter**; *(2)* when ≥1 active candidate exists, prefer active; *(3)* when 0
active, fall back to most-recent (`last_seen_label`); *(4)* the bridge should ideally match
**employer-as-of-contribution-date** (compare `f.transaction_dt`/`cycle_year` against the entity's
active window) rather than current `is_active` — note this as a Phase-2 refinement, since the sidecar
carries `sam_extract_label` but not per-entity active-date ranges (those live on the golden
`sam_master_entities` via `first_seen_label`/`last_seen_label`, joinable by `uei`).

### B8 — `legal_name_base` over-peels real `CO` tokens — material but the net is strongly positive — **Minor**

**Problem.** `core.name_norm.legal_name_base` peels `(LLC|INC|CORP|CO|LTD|PLC)`. `CO` is a real
trailing token in legitimate names ("...BREWING CO", "...TELEPHONE CO"), so the base key truncates
them. With B1 promoting `legal_name_base` to the primary FEC key, this peel now sits on the hot path.

**Evidence** (`/tmp/probe_fanout.py`): **11,147 rows** (10,619 distinct `normalized_legal_name`) end
in a bare ` CO` token that `legal_name_base` strips — e.g. `WOODMANS BREWING CO → WOODMANS BREWING`,
`BARNESVILLE TELEPHONE CO → BARNESVILLE TELEPHONE`, `CASS CO → CASS`. That is 0.72% of the dataset.
The collision tax overall: `legal_name_base` multi-uei rate 3.43% vs `normalized_legal_name` 2.56%
(B3), and 15,533 base keys merge >1 distinct normalized name.

**Fix.** **Accept it, do not patch `core.name_norm`.** The peel is a *fleet-wide* key
(`sos_normalized_master`, the crosswalk, seven workers all import it — the module docstring forbids
re-inlining a tweak), and the +11.98pp recall gain from base-key blocking (B1) dwarfs the 0.72%
over-peel cost. The correct move is **transparency, not surgery**: have `verify_sam_normalized_entities`
log the count of ` CO`-peeled rows and the base-key collision rate so the trade is visible. If
`CO`-over-peel ever proves to inject false positives in the *bridge*, the fix is a bridge-side rule
(require `exact_name` confirmation when the only signal is a `CO`-peeled base match), **not** a change
to the shared macro. Flag this in §8/§4 as a known, measured property.

### B9 — Gate 5 hard-codes the `normalized_legal_name` cardinality but omits `legal_name_base` — **Nit**

**Problem.** Gate 5 asserts `distinct normalized_legal_name within ±5% of 1,466,764`. With
`legal_name_base` now primary (B1), its cardinality is equally load-bearing and ungated.

**Evidence.** Live: distinct `legal_name_base` = **1,450,598** (probe). The plan never floors it.

**Fix.** Add to Gate 5: `distinct legal_name_base within ±5% of 1,450,598`. Cheap, and it catches a
peel-set regression (e.g. someone adds `COMPANY` to the peel set and base cardinality collapses).

---

## 4. Amended-plan delta (the precise edits)

Apply these to `SAM_NORMALIZED_ENTITIES_BUILD_PLAN.md`. None changes the build's *shape* — they
correct keys, gates, column names, and the consumer contract.

**§4 — Output schema**
- Rename `physical_state` → **`source_state`**; `physical_zip5` → **`zip_code`** (B4). Both still
  unindexed inline geo; the comment stays accurate.
- Re-label the index roles: `legal_name_base` is the **primary free-text blocking key**;
  `normalized_legal_name` is the **precision/exact-confirmation key** (B1). Both remain BTREE.
- Add a one-line note: `legal_name_base` over-peels ` CO` on 11,147 rows / 10,619 names (B8) —
  measured, accepted, surfaced in verify.

**§5 — Transform**
- No SQL change (the projection is correct), **but** the two geo columns rename to `source_state` /
  `zip_code` in the `SELECT`. The `legal_name_base(normalized_legal_name)` alias chain stays
  (matches `sos_normalized/normalize.py:389-390`).

**§7 — Validation gates**
- **Split the gate phase explicitly (B5):** Gates 1–6 run **on the Arrow table before write** and
  hard-fail pre-overwrite. Gates 8–10 run post-write, and on failure trigger
  `lance.dataset(uri, version=v_before).restore()` + re-raise (clone `crosswalk:576`). Capture
  `v_before` before the write.
- **Gate 5 addition (B9):** `distinct legal_name_base within ±5% of 1,450,598`.
- **New Gate 11 (B8 transparency):** verify logs `legal_name_base` collision rate (multi-uei %) and
  ` CO`-peel count — non-failing, observability only.

**§10 — Consumer contract (the substantive rewrite)**
- **Primary block on `legal_name_base`** (`e.legal_name_base = legal_name_base(f.emp_key)`), with
  `normalized_legal_name` exact as the highest-confidence tier — not the reverse (B1).
- **Remove `AND e.source_state = f.state2` from the JOIN.** Geo becomes scored boolean columns
  (`state_confirms`, `zip_confirms`) over the candidate set; **never an equality predicate** (B2).
- **Add the multiplicity paragraph (B3):** state that `normalized_legal_name`/`legal_name_base` are
  not unique to a UEI (2.56%/3.43% multi-uei, max 2,184), and that the bridge MUST resolve the
  per-name candidate set by the deterministic ranking
  (`exact_name DESC, state_confirms DESC, zip_confirms DESC, is_active DESC, last_seen_label DESC`)
  and MUST emit `match_confidence`/`match_method` — not an unranked fan-out.
- **`is_active` semantics (B7):** tiebreak not filter; prefer active when present, else most-recent;
  note employer-as-of-date as a Phase-2 refinement (join the golden by `uei` for active-date ranges).

**§2 — Why a sidecar**
- Add the non-redundancy line vs `crosswalk_sam_usaspending` (B6): 100% vs 51.6% fill, +geo, full
  1.54M universe incl. historical.

**§12 — Out of scope**
- The `sos_normalized_master` union note can now say "literal `UNION ALL` after the B4 rename"
  instead of implying a migration.

---

## 5. What I verified live vs. reasoned-but-unverified

**Verified by live probe (2026-06-05, zero mutation):**
- Fan-out distribution: 37,598 multi-uei names (2.56%), 112,400 UEIs under multi-uei names (7.29%),
  max 2,184, p99=2, p99.9=6 (`/tmp/probe_fanout.py`).
- Geo collapse as a ranker: `(nln,state)` 1.379% ambiguous, `(nln,state,zip5)` 0.703%, max 38.
- Geo as a hard gate is invalid: 32.67% of single-HQ name-matches have donor-home ≠ HQ; 55.5%
  agreement for national employers (`/tmp/probe_fec_match.py`).
- Key choice: exact `normalized_legal_name` matches 17.0% of contributions, `legal_name_base` 28.98%,
  +11.98pp incremental (over the full 100.6M `entity_tp='IND'` set, sentinels dropped).
- `is_active` disambiguation: 41.27% of multi-uei names resolve to exactly one active UEI; 36.01%
  remain multi-active; 22.72% have zero active.
- `legal_name_base` collision: 3.43% multi-uei (vs 2.56% for `normalized_legal_name`); distinct
  `legal_name_base` = 1,450,598.
- `CO` over-peel: 11,147 rows / 10,619 distinct names.
- Re-confirmed seed figures: rows = distinct uei = 1,541,566; distinct `normalized_legal_name` =
  1,466,764 (exact); 44.45% of `legal_business_name` strings change under `name_norm` (685,152 /
  1,541,566).
- Code patterns: `crosswalk_sam_usaspending.py:520-598` has the integrity-gate + `restore()` rollback;
  `sos_normalized/normalize.py:80,389-390` uses `source_state`/`zip_code` and the alias-chain idiom;
  the golden `sam_master_entities` exposes `is_active` (the projection's assumption holds — the probe
  read it directly).

**Reasoned but NOT independently verified (flagged honestly):**
- The downstream `crosswalk_fec_sam_employer` worker does not yet exist; the B1/B2/B3 fixes are
  prescriptions for *its* contract (§10), validated against data but not against a built bridge. The
  match-rate figures are an upper bound on *blocking* recall — they do not account for the
  precision loss the candidate-set ranking will (correctly) impose. The true *resolved* match rate
  after B3 disambiguation will be lower than 28.98% and is not measured here.
- The 38-char FEC `employer` truncation (cited in the diagnostic) was **not** re-probed for its
  prefix-collision impact on the `legal_name_base` block; I accepted the diagnostic's figure. A
  prefix pass for `len(employer)=38` rows is plausibly needed but is a *bridge* concern, out of scope
  for the sidecar build — note it in the `crosswalk_fec_sam_employer` plan, not here.
- I did not benchmark the actual point-lookup latency (Gate 10's <100ms claim) — it is a reasonable
  expectation for a BTREE seek on a 1.5M-row Lance dataset but unverified.
- Memory envelope: the plan's `memory_limit='12GB'`/`threads=8` is lighter than the wide-`pipe_fields`
  workers (24GB) but the sidecar reads the *already-projected* golden (8 narrow columns, no
  `pipe_fields` unnest), so 12GB is plausibly fine; not stress-tested.

---

## 6. Bottom line

The dataset is a correct, cheap, well-patterned projection — **the build is fine and safe to run.**
The danger is entirely in §10: as written, the consumer contract would ship a FEC employer bridge
that **hard-gates away 32.67% of true matches** (B2), **blocks on the weaker key** (B1, leaving
+11.98pp recall unclaimed), and **fans out one employer to up to 2,184 UEIs with no ranking** (B3).
Fix the three contract defects (re-rank to `legal_name_base`-primary, demote geo to a confirmatory
score, mandate candidate-set + confidence), rename two columns for the spine union (B4), and wrap the
overwrite in the restore-on-failure guard the repo already has (B5). With those, ship it.
