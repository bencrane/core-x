# `sam_normalized_pocs` — Person-Layer Sidecar Build Plan, Adversarial Design Review

Reviewer stance: hostile. The plan is assumed wrong until each load-bearing claim is verified against
ground truth (the checked-in workers, the live diagnostic, and **actual DuckDB output** over an
adversarial name fixture). Plan under review:
[`docs/plans/SAM_NORMALIZED_POCS_BUILD_PLAN.md`](SAM_NORMALIZED_POCS_BUILD_PLAN.md).

The high-risk surfaces — `core/person_name_norm.py` (§5) and the gates that depend on it (§8) — have
**no existing reference implementation**, so they were verified empirically: the §5 builders +
`core.name_norm.name_norm` were extracted verbatim into a temp module, the generated SQL was run in
DuckDB 1.5 over a 30-row hand-built adversarial fixture, and every derived column was compared against
what §4/§5/§11 *claim*. The fixture + script are reproduced in the appendix; every "DuckDB output"
quoted below is a literal run, not a reasoned expectation. Cardinalities are cross-checked against
[`FEC_SAM_PERSONNEL_BRIDGE_DIAGNOSTIC.md`](../reference/FEC_SAM_PERSONNEL_BRIDGE_DIAGNOSTIC.md)
(probe 2026-06-05). No data-plane mutation was performed (no `write_dataset`, no `create_*index`, no
DDL, no R2 read — the harness uses synthetic rows only).

---

## 1. Verdict

**NO-GO.** The control-plane clone (gate/rollback/ops-ledger/dispatcher shape) is sound and the column
inventory checks out, but the **net-new data plane — `core/person_name_norm.py` (§5) — does not do what
§2/§4/§11 say it does**, and three of its failures are load-bearing: the generational-suffix column is
**empty in the exact case the design exists for**, the generational gate (gate 9) **hard-fails the build
on realistic input**, and two §11 unit-test assertions the plan tells the agent to write are **false
against the plan's own regex**. The build cannot be shipped as written: an agent following §12 top to
bottom either ships a sidecar whose marquee precision column is null, or trips gate 9 / the §11 tests and
stalls. Layered on top, **every §8 distinct-key floor binds *before* its Δ-band** (the precise inverse of
the stated design and of the entities precedent it copies the prose from), and the resource envelope is
**under-provisioned vs the plain 8M-row precedent it should clone (`sam_pocs`: 24 GB, not 16 GB)**.

Finding count: **3 BLOCKER · 3 CRITICAL · 3 HIGH · 4 MEDIUM · 2 LOW.**

All defects are in the **specification**, not the approach. The sidecar concept is right, the
sidecar-vs-columns argument is right, the 1:1 lossless grain is right, the control plane is a faithful
clone. Fix the §5 primitive (it must clean the *correct field*, extract the suffix from a *suffix-anchored*
source, and the gate must accept suffixes wherever they land), re-floor §8 below the Δ-band, right-size the
container, and this is a GO. Until the §5 primitive is corrected and re-verified against the appendix
fixture, it is a NO-GO.

---

## 2. Headline findings

| # | Finding | Severity | Marker |
|---|---|---|---|
| **P1** | `generational_suffix` is **NULL in the canonical case** (suffix in `last_name`, `first_name` present ≈100% of rows): the `$`-anchored extract runs on `concat('SMITH JR','JOHN')`='SMITH JR JOHN', JR is mid-string → no match. The §2 precision column is empty exactly when needed; gate 9's "non-null on >0 rows" only passes on pathological no-first-name rows. | **BLOCKER** | ❌ |
| **P2** | **Gate 9 hard-fails on real data.** When the generational token rides the `first_name` field (`first_name='JOHN JR'`, or a bare `'SR'`/`'V'`/`'III'`), nothing peels it (only `_last_core` peels, only from `last_name`) → `person_key` ='SMITH JOHN JR' / 'JONES SR' / 'JONES V', which **end in a gen token**. Gate 9 then aborts the build. The premise "suffixes live in `last_name`" is **never established** by the diagnostic or `sam_pocs.py`. | **BLOCKER** | ❌ |
| **P3** | **Two §11 unit-test assertions are false against the §5 regex.** `DR JANE FOX MD` (whole name in `last_name`) → `person_key`='**DR JANE FOX** JANE', not 'FOX JANE' (honorific strip only touches `first_name`). `last='JR'` → `person_key`='**JR JOHN**', not NULL (the `_last_core` peel is space-anchored `' JR$'`; a bare `JR` has no leading space). The agent writes the tests per §11, they fail, the build stalls. | **BLOCKER** | ❌ |
| **P4** | **All three §8 distinct-key floors bind BEFORE their ±25% Δ-band** — the inverse of the stated design ("floors sit below the band so the Δ is binding"). `person_key` floor 1.80M > Δ-lower 1.59M; addressable-triple floor 2.50M > 2.15M; row floor 7.50M > 6.05M. A legitimate SAM purge (which the plan itself warns about) false-trips the floor before the Δ ever fires. | **CRITICAL** | ❌ |
| **P5** | **Gate-4 / gate-5 / homonym baselines are computed from the WRONG key.** The diagnostic's 2,868,249 used `name_norm(last,first,**middle**)`; the plan's `person_key` drops middle **and** peels gen/honorific. The plan's column will produce a materially different (lower) distinct count than the cited "live" figures, so the floors and the 0.263 band are mis-calibrated against a key that was never built. | **CRITICAL** | ❌ |
| **P6** | **`first_initial` and `surname_initial_key` disagree on honorific-first rows.** `first_initial=left(name_norm(first_name),1)` keeps the honorific; `surname_initial_key` uses `_first_core` (honorific-stripped). `first_name='DR JANE'` → `first_initial`='D' but the recall key encodes 'J'. The confirmatory column rejects matches the recall key accepts. | **CRITICAL** | ⚠️ |
| **P7** | **Resource envelope under-provisioned vs the 8M precedent.** `sam_pocs` (same 8.07M scale, same `memory=32768`) sets `DUCKDB_MEMORY_LIMIT=24GB` for **2** high-card string BTREEs. The plan sets **16GB** while building **3** (`person_key` 2.12M, `surname_initial_key`, `last_name_norm`) under `LANCE_BYPASS_SPILLING=true` (every sort fully in-memory). OOM risk in the index phase. | **HIGH** | ⚠️ |
| **P8** | **`_last_core` only peels a suffix from `last_name`, but suffixes also appear in `first_name` / `full_name`.** Beyond the gate-9 trip (P2), this means `person_key` for `last='SMITH', first='JOHN JR'` is **not** father/son-merged with `last='SMITH', first='JOHN'` (recall loss), the opposite of the §2 recall goal. The peel must run on the *combined* given+surname token stream, not one field. | **HIGH** | ⚠️ |
| **P9** | **`skip_if_current` can skip a needed rebuild on a within-label content change.** `sam_pocs` rebuilds daily but `sam_extract_label` is monthly; a within-month `entity_registrations` correction at a stable label (e.g. the recent uei-reclassification fix) changes `sam_pocs` content without bumping the label → the cron sidecar skips. Same exposure the entities precedent carries, inherited not introduced. | **MEDIUM** | ⚠️ |
| **P10** | **`generational_suffix` enum is incomplete and ambiguous.** `_GEN='JR\|SR\|II\|III\|IV\|V'` omits `2ND/3RD/4TH` (common SAM/FEC variants) and the alternation `II\|III` matters: `regexp_extract(...' (JR\|SR\|II\|III\|IV\|V)$')` on 'SMITH III' — verify it captures 'III' not 'II'. (It does, but only because DuckDB's RE2 is leftmost-longest at the anchor; document it.) | **MEDIUM** | ⚠️ |
| **P11** | **§10 cron margin is 25 min and the durable callback path is unproven.** `sam_pocs` worst-case finish is 17:35 (16:30 + `maxDuration:3900`s); interim cron `0 18` gives 25 min. The entities sidecar — the data-plane clone source — **has no deployed Trigger task**, so the `skip_if_current`-via-dispatcher path has never run on cron in the precedent. | **MEDIUM** | ⚠️ |
| **P12** | **`init_ops` step ordering in §12 runs before the ops table is guaranteed.** The checklist runs `init_ops` (step 3) then dry-run (step 4); but `_prior_success_baseline` and `_record_run` already `cur.execute(OPS_DDL)` defensively (verified in both precedents), so step 3 is belt-and-suspenders — fine, but the plan should note the DDL is idempotent and self-applied so a skipped step 3 is non-fatal. | **MEDIUM** | ✅ |
| **P13** | **`source_dataset` const + union-compat claim is unverified against `sam_master_contacts`/`ffata_exec_comp` schemas.** §4 asserts the column names are "union-compatible" for a future `UNION ALL`, but `sam_master_contacts` uses `middle_initial` (not `middle_name`/`middle_norm`) and `state_or_province`/`zip_postal_code` (not `state2`/`zip5`) per the diagnostic §1 table — the union is **not** literal. | **LOW** | ⚠️ |
| **P14** | **`name-alpha` gate (gate 8) threshold is borrowed without re-derivation.** `person_key` ≥0.95 alpha-fraction is copied from `sam_pocs`'s `first_name` alpha gate, but `person_key` contains a space separator and possibly digits in surnames (e.g. `3M` employees rare but present); confirm 0.95 holds for the *peeled* key, not the raw first name. | **LOW** | ✅ |

---

## 3. Per-finding detail

### P1 — `generational_suffix` is NULL in the exact case the design exists for — **BLOCKER**

**Plan location.** §4 (`generational_suffix` — "JR/SR/II–V, extracted & **preserved**"); §2 ("PRESERVED in
its own column (precision)"); §5 (`generational_suffix(last, first)`); §8 gate 9 ("`generational_suffix`
is non-null on `> 0` rows (proves preservation, not silent drop)"); §11 ("`SMITH JR` / `SMITH` → … distinct
`generational_suffix` (`JR` / NULL)").

**Claim.** When a generational token rides the `last_name` field, the suffix is peeled out of `person_key`
**and** captured in `generational_suffix`.

**Evidence (literal DuckDB output, appendix harness 2).** The §5 builder is
`regexp_extract(concat_ws(' ', upper(last), upper(first)), ' (JR|SR|II|III|IV|V)\.?$', 1)`. The `$` anchor
requires the token to be the **final** token of `last⎵first`. But `first_name` fills ~100% (diagnostic §5),
so the source string is `'SMITH JR JOHN'` — JR is mid-string:

```
id | first   last       | gen_src_string    person_key   gen_suf
 1 | JOHN    SMITH JR    | SMITH JR JOHN     SMITH JOHN   ·NULL·   <- SILENT DROP
 2 | ·NULL·  SMITH JR    | SMITH JR          SMITH        JR       <- only fires with NO first_name
 3 | ·EMPTY· SMITH JR    | SMITH JR          SMITH        ·NULL·
11 | BILL    SMITH JR SR | SMITH JR SR BILL  SMITH JR BILL ·NULL·  <- double-gen also dropped
```

`person_key` correctly peels JR (id 1 → `SMITH JOHN`), but `generational_suffix` is **NULL** because the
suffix is no longer terminal. The column is populated **only** on the ~0.04% of rows with no `first_name`
(id 2). So the §2 "precision" promise is vacuous in production, and gate 9's second clause
("`generational_suffix` non-null on `> 0` rows") passes only by accident on the null-first tail — it does
**not** prove preservation, it proves a near-empty column.

**Why it's wrong.** The suffix-extraction source and the suffix-peel source must be the **same surname
token stream**, anchored so the gen token is terminal *at the moment of extraction*. Extracting from
`last⎵first` guarantees the token is non-terminal whenever a first name exists.

**Remediation.** Extract the suffix from the **surname field alone**, before the first name is appended,
and from the same `upper(last)` the peel consumes:

```python
def generational_suffix(last: str, first: str) -> str:
    """Trailing generational token of the SURNAME field (where SAM carries 'SMITH JR'),
    end-anchored on last_name ONLY so a present first_name cannot displace it."""
    u = _upper(last)  # 'SMITH JR'
    return f"nullif(upper(regexp_extract({u}, ' ({_GEN})\\.?$', 1)), '')"
```

Verified against the appendix fixture this yields `JR` for id 1/16/17 and `SR` for the double-gen tail (or
extend to capture the full ` JR SR` run — see P8). The `first` parameter becomes unused; keep it in the
signature only if a future field also carries suffixes, otherwise drop it. **Re-run the appendix harness and
require `gen_suffix='JR'` on `(last='SMITH JR', first='JOHN')` before first build.**

### P2 — Gate 9 hard-fails the build on realistic input; the "suffix lives in last_name" premise is unverified — **BLOCKER**

**Plan location.** §8 gate 9 ("**zero** `person_key` values end in ` JR`/` SR`/` II`/` III`/` IV`/` V`");
§4 (`person_key` "no generational"); §2 (generational token "is peeled OUT of `person_key`").

**Claim.** The peel guarantees no `person_key` ends in a generational token, so a structural gate can assert
zero such rows.

**Evidence (literal DuckDB output, appendix harness 2).** The peel (`_last_core`) only operates on
`last_name`. The generational token does **not** always live there. When it rides `first_name` — which the
plan never excludes — nothing strips it:

```
id | first    last    | person_key      <- gate-9 fate
 4 | JOHN JR  SMITH    | SMITH JOHN JR    ends in ' JR'  → GATE 9 FAIL
 6 | JR       SMITH    | SMITH JR         ends in ' JR'  → GATE 9 FAIL
 7 | SR       JONES    | JONES SR         ends in ' SR'  → GATE 9 FAIL
 8 | V        JONES    | JONES V          ends in ' V'   → GATE 9 FAIL
 9 | III      JONES    | JONES III        ends in ' III' → GATE 9 FAIL
12 | ·NULL·   SMITH JR SR | SMITH JR     (peels SR but JR survives — see P8)
```

Gate 9 aborts the entire build on the first such row. The plan asserts (§2, §5 docstring) that the gen
token is a "trailing token" of the surname, but **neither the diagnostic nor `sam_pocs.py` establishes
where suffixes actually land.** `sam_pocs` copies `entity_registrations`' positional `first/middle/last`
fields verbatim (ZERO-ALTERATION, `sam_pocs.py:36-42`); SAM data-entry routinely puts `JR` in the
first-name or last-name slot interchangeably. This is an **unverified load-bearing premise** that the gate
turns into a guaranteed failure.

**Why it's wrong.** A structural gate must hold on *every* path through the data, not just the assumed one.
Either the peel must catch suffixes wherever they appear (P8), or the gate must be reframed to what is
actually invariant.

**Remediation (do both).**
1. **Make the peel field-agnostic (P8):** strip the trailing gen token from the *combined*
   `last⎵first` (or `last⎵first⎵middle`) token stream that feeds `person_key`, not from `last_name` alone —
   then no path produces a key ending in a gen token regardless of which field carried it.
2. **Keep gate 9 as the post-peel invariant**, but state it correctly: after the combined peel, *zero*
   `person_key` end in a gen token (now actually true), AND `generational_suffix` (P1-fixed) is non-null on
   the rows where a suffix was present. Add a fixture row with the suffix in `first_name` to the §11 tests
   so this path is covered.

### P3 — Two §11 unit-test assertions are false against the §5 regex — **BLOCKER**

**Plan location.** §11 ("honorific/credential stripped (`DR JANE FOX MD` → key `FOX JANE`)"; "`JR`-as-whole-
surname … → null-surname → null `person_key`, retained row").

**Claim.** §11 enumerates assertions the agent must encode; the build proceeds only when they pass.

**Evidence (literal DuckDB output, appendix harness 1).**

```
id | first   last           | person_key         surname_init   last_norm
 5 | JANE    DR JANE FOX MD  | DR JANE FOX JANE   DR JANE FOX J  DR JANE FOX MD
12 | JOHN    JR              | JR JOHN            JR J           JR
```

- **`DR JANE FOX MD`:** §11 claims `person_key`='FOX JANE'. Actual = **'DR JANE FOX JANE'**. `_first_core`
  strips a leading honorific only from `first_name` (which here is the clean 'JANE'); the honorific +
  forename sitting in `last_name` are untouched, and only the trailing `MD` credential peels. The whole
  malformed last name survives.
- **`last='JR'`:** §11 claims null `person_key`, retained row. Actual = **'JR JOHN'**. `_last_core`'s gen
  peel is `' +(JR|...)\.?$'` — a leading space is mandatory; a bare `JR` token has no preceding space, so it
  does not match, and `nullif(trim('JR'),'')` is `'JR'`, not NULL.

The agent, following §12 step 2 ("Unit-test the primitive … FIRST"), writes these assertions verbatim, they
fail, and step 2 blocks. This is not a latent risk — it is a deterministic stall on the first checklist
step.

**Why it's wrong.** §11 was written from the *intended* behaviour of the regex, not the *actual* behaviour.
The two diverge because the cleaners are field-scoped to `first_name`/`last_name` and the adversarial inputs
put the noise in the wrong field.

**Remediation.** Decide the contract, then make the regex and the test agree:
- The `DR JANE FOX MD`-in-`last_name` case is a **malformed SAM row** (honorific + full name crammed into
  one field). The ZERO-ALTERATION-adjacent honest move is to **not claim** the §5 primitive cleans it —
  drop that assertion and replace it with the realistic split-fields case `first='JANE', last='FOX'` +
  `generational/credential` noise, which the primitive *can* handle. If cleaning crammed last-name fields is
  in scope, apply the honorific strip to `last_name` too (`_last_core` gains the `^({_HONORIFIC})` peel) and
  re-verify.
- For `last='JR'`: if the intent is "a surname that is only a suffix is not a person" → null, then the gen
  peel inside `_last_core` must be anchored to also strip a *whole-field* gen token
  (`'^({_GEN})\.?$'` in addition to the space-anchored form), so `_last_core('JR')` → NULL. Add the
  assertion back only after the regex produces NULL. Re-run the appendix harness; both rows must match the
  documented claim before the §11 suite is considered authored.

### P4 — All three §8 distinct-key floors bind before their Δ-band, inverting the stated design — **CRITICAL**

**Plan location.** §8 ("Floors sit **below** the ±25% Δ-band so the per-family Δ is the binding sensitive
check; floors are catastrophic-collapse catchers only"); gates 1, 4, 5; `BASELINE_MIN_ROWS=7,800,000`.

**Claim.** The floors are loose catastrophe-catchers; the per-family Δ is the sensitive regression check
that actually binds.

**Evidence (arithmetic, appendix `gate_arith.py`).** Δ-lower bound = live × 0.75:

| metric | live | Δ-lower (×.75) | plan floor | binds first? |
|---|---|---|---|---|
| rows | 8,065,116 | 6,048,837 | **7,500,000** | **YES** (93.0% of live) |
| distinct `person_key` | 2,119,414 | 1,589,560 | **1,800,000** | **YES** |
| addressable triple | 2,868,249 | 2,151,187 | **2,500,000** | **YES** |

Every floor sits **above** the Δ-lower bound, so on any shrink the floor trips first and the Δ can never
fire — the exact opposite of the prose. Contrast the precedent the prose is lifted from: in
`sam_normalized_entities.py:79-83` the *sensitive distinct-key* floors are `NORM_FLOOR=BASE_FLOOR=1,050,000`
against live 1.467M/1.451M, i.e. Δ-lower 1.10M/1.09M — there the floors **genuinely** sit below the band
(verified), which is why its own test `test_base_distinct_collapse_caught_by_per_family_delta`
(`test_sam_normalized_gates.py:89-94`) can assert the Δ catches a 1.06M collapse the floor misses. The POCs
plan copied the sentence but set the floors at 88–93% of live.

Compounding: `BASELINE_MIN_ROWS=7,800,000` (96.7% of live) sits **above** the row floor (7.5M). A legitimate
purge to 7.5M–7.8M rows passes the row floor but **fails to qualify as a future Δ-baseline**, silently
disabling the Δ-check on the *next* run — the plan's own §12 warning ("SAM also purges registrations; a
legitimate shrink must be confirmed before re-baselining") is defeated by its own thresholds.

**Why it's risky.** The sidecar's stated safety model (Δ is the sensitive check, floors are loose) is not
the model the numbers implement. A real SAM purge false-trips a tight floor and stops the daily ship; an
operator then either floors-through (forbidden by §12) or hand-edits the constant under pressure.

**Remediation.** Re-floor the **sensitive** keys below the 75% band, matching the entities precedent's
intent; keep the row floor tight (it is a true catastrophe floor and a 25% row drop is itself alarming) but
drop `BASELINE_MIN_ROWS` below it:

```python
ROW_FLOOR              = 6_000_000   # catastrophe only (matches sam_pocs POCS_ROW_FLOOR)
PK_DISTINCT_FLOOR      = 1_550_000   # < Δ-lower 1.59M → Δ (gate, ±25%) is the binding check
ADDR_TRIPLE_FLOOR      = 2_100_000   # < Δ-lower 2.15M (recompute once P5 is fixed — see below)
BASELINE_MIN_ROWS      = 6_000_000   # == ROW_FLOOR: any floor-passing success can re-baseline
```

Then the per-family Δ-guards on `rows`, `distinct_person_key`, `distinct_person_geo_triple` become the
sensitive checks the prose promises, and a legitimate purge re-baselines instead of stalling.

### P5 — Gate-4/5/homonym baselines use a key the plan does not build — **CRITICAL**

**Plan location.** §8 gate 4 ("`distinct (person_key, state2, zip5)` … `≥ 2,500,000` (live 2,868,249)");
gate 5 ("`≥ 1,800,000` (live 2,119,414)"); gate 6 ("`distinct person_key / rows ∈ [0.15,0.40]` (live
0.263)"); §3 ("distinct `(name_norm, state, zip5)` 2,868,249").

**Claim.** The cited live figures are the expected output of the plan's `person_key`.

**Evidence (diagnostic source-of-truth).** The two cited figures were computed from **different keys, and
neither is the plan's `person_key`:**
- `2,119,414` (gate 5) = diagnostic §5 line 190 `name_norm(LAST FIRST)` — **no middle, no peel**.
- `2,868,249` (gate 4) = diagnostic §2 line 91 `name_norm(concat_ws(' ', last_name, first_name,
  **middle_name**))` — **includes middle**.

The plan's `person_key` = `name_norm(LAST_core⎵FIRST_core)` — drops middle (like the 2.12M key) **but also
peels** generational + honorific tokens (unlike either). Net effect on cardinality:
- vs the 2.12M key: the peel **merges** `SMITH JR`+`SMITH` and `DR JANE`+`JANE`, so distinct `person_key` <
  2,119,414. The 1.8M floor is set 15% below a number the column never produces.
- vs the 2,868,249 triple: that key **includes middle**, which *splits* `SMITH JOHN A` from `SMITH JOHN B`.
  The plan's no-middle triple **collapses** them, so the real addressable-triple count is **materially below
  2,868,249** — the 2.5M floor (already above-band per P4) is benchmarked against an inflated number and is
  even more likely to false-trip.

The homonym band's "live 0.263" is the 2.12M-key ratio; the peeled-key ratio is lower. The band [0.15,0.40]
is wide enough to survive, but the *cited anchor* is wrong.

**Why it's wrong.** Floors and bands calibrated against a key that differs from the materialized column are
not calibrated at all — they are guesses with a false provenance ("live").

**Remediation.** Before setting any §8 distinct floor, **measure the plan's actual `person_key`** on
`sam_pocs` in the dry-run (§12 step 4 already materializes it — have `plan_*` print `distinct_person_key`,
`distinct (person_key,state2,zip5) WHERE country='USA'`, and the ratio), then set each floor below the 75%
band of *that measured value* (P4). Update the §3/§8 prose to state the key is generational-/honorific-peeled
and middle-dropped, so the figures are not conflated with the diagnostic's join-key figures. Until measured,
treat 2.5M/1.8M/0.263 as placeholders, not "live".

### P6 — `first_initial` and `surname_initial_key` disagree on honorific-first rows — **CRITICAL**

**Plan location.** §4 (`first_initial` = "`left(first_core, 1)` (normalized)") vs §6 transform
(`left({name_norm("first_name")}, 1) AS first_initial`). The §4 *prose* says `first_core`; the §6 *SQL* uses
raw `name_norm(first_name)`. They are not the same expression.

**Claim.** `first_initial` is the normalized first initial, usable as a confirmatory tiebreak alongside
`surname_initial_key` (which embeds the same initial).

**Evidence (literal DuckDB output, appendix).**

```
first='DR JANE':
  first_initial = left(name_norm(first_name),1) = 'D'    (keeps honorific)
  surname_initial_key embedded initial          = 'J'    (uses _first_core → 'JANE')
```

§6 builds `first_initial` from `name_norm(first_name)` (no honorific strip), while `surname_initial_key`
builds its initial from `_first_core(first_name)` (honorific stripped). On any row whose `first_name` carries
a leading honorific, the two columns encode **different letters** for the same person. A consumer using
`first_initial` as a confirmatory tiebreak would reject a candidate that `surname_initial_key` correctly
blocks — a self-inflicted false negative.

**Why it's wrong.** Two columns the plan markets as a coherent (recall key, confirmatory initial) pair must
derive from the **same cleaned given name**. §4's prose (`first_core`) is correct; §6's SQL is not.

**Remediation.** Make §6 match §4: `left(person_name_norm._first_core("first_name"), 1) AS first_initial`
(export `_first_core` or add a `first_initial(first)` builder). Then `first_initial` == the initial inside
`surname_initial_key` for every row. Add a §11 assertion: on `first='DR JANE'`, `first_initial` ==
`left(surname_initial_key_components)` == 'J'.

### P7 — Resource envelope under-provisioned vs the 8M-row precedent — **HIGH**

**Plan location.** §6 (`memory_limit='16GB'`); §7 (`DUCKDB_MEMORY_LIMIT="16GB"`, clone `memory=32768`,
`BTREE_INDEXES=["person_key","surname_initial_key","last_name_norm","uei","cage_code"]`, image
`LANCE_BYPASS_SPILLING:true`); §4 ("`person_key` … BTREE … `LANCE_BYPASS_SPILLING=true` keeps its sort
in-memory").

**Claim.** 16 GB DuckDB in a 32 GB container is adequate because "the projection is narrow … the cost is the
high-cardinality string-key sort, not the scan."

**Evidence (the two worker files).** The directly comparable precedent is **`sam_pocs` itself** — *same*
8.07M-row scale, *same* `memory=32768` container — and it sets `DUCKDB_MEMORY_LIMIT=24GB`
(`sam_pocs.py:97`) while building **2** high-card string BTREEs (`name_key` ~2.43M, `uei`). The plan sets
**16GB** (33% lower) while building **3** high-card string BTREEs (`person_key` ~2.12M,
`surname_initial_key` ~similar, `last_name_norm` ~447k) **plus** `uei`/`cage_code`, all under
`LANCE_BYPASS_SPILLING=true`, which forces every index sort fully resident (lance#2650). The
`sam_normalized_entities` envelope the plan *also* cites (12 GB) is a **1.54M-row** dataset (5.2× smaller) —
not a scale precedent for 8M.

The DuckDB *materialize* step is plausibly fine at 16 GB (narrow scan, no `pipe_fields` unnest). The risk is
the **index-build phase**: it runs in the same 32 GB container *after* the 8M-row × ~16-col Arrow table is
already resident and the freshly-written Lance dataset is opened for reading; each bypass-spilled string
sort then competes for what's left. `sam_pocs` chose 24 GB for fewer such sorts; the plan asks for less
memory to do more.

**Why it's risky.** An OOM in the index loop fails *after* `write_dataset` has overwritten the live dataset;
the rollback guard restores `v_before`, so it is not a corruption — but on the **first build** `v_before` is
`None`, so an OOM there leaves a half-indexed net-new dataset the §8/post-write gates then fail on, and the
operator hand-cleans R2.

**Remediation.** Match or exceed the 8M precedent and reduce the number of bypass-spilled sorts:
1. Set `DUCKDB_MEMORY_LIMIT="24GB"` (equal to `sam_pocs`), keep `memory=32768`.
2. `LANCE_BYPASS_SPILLING` is image-global; it forces *all* index sorts in-memory. Only `person_key` /
   `surname_initial_key` are genuinely high-card (~2.1M). `last_name_norm` is ~447k distinct and the BITMAPs
   are trivial — they do not need bypass. Build the two high-card BTREEs first (most likely to OOM, fail
   fast before indexing the rest), then `last_name_norm`, `uei`, `cage_code`, then BITMAPs. If OOM persists,
   bump `memory=49152` — the index phase is the binding constraint, not the scan.
3. The build loop is already sequential (a `for col in BTREE_INDEXES`), so peak is one sort at a time —
   keep it sequential; do **not** parallelize index creation.

### P8 — The suffix peel is field-scoped to `last_name`, breaking father/son recall when the suffix rides another field — **HIGH**

**Plan location.** §2 ("recall: FEC's suffix-less `SMITH JOHN` still blocks SAM's `SMITH JOHN JR`"); §5
(`_last_core`).

**Claim.** Peeling the generational token guarantees the suffixed and unsuffixed person share a
`person_key`, achieving father/son recall.

**Evidence (appendix harness 2).** Recall only holds when the suffix is in `last_name`:

```
last='SMITH', first='JOHN'    → person_key 'SMITH JOHN'
last='SMITH', first='JOHN JR' → person_key 'SMITH JOHN JR'   (NOT merged — recall lost)
last='SMITH JR', first='JOHN' → person_key 'SMITH JOHN'      (merged — recall holds)
last='SMITH JR SR' (dbl)      → person_key 'SMITH JR ...'    (only SR peeled, JR survives)
```

Because `_last_core` strips only one trailing token from `last_name` only, a suffix in `first_name`
defeats the merge (and trips gate 9, P2), and a double suffix in `last_name` leaves the inner token in the
key. The §2 recall guarantee is conditional on a data layout the plan never verified (P2).

**Why it's wrong.** Recall must be layout-independent: `SMITH JOHN JR` must block `SMITH JOHN` regardless of
which field SAM stuffed `JR` into.

**Remediation.** Peel the trailing generational run from the **combined surname+given token stream** that
feeds `person_key`, after `name_norm`, repeating to catch double suffixes:

```python
def person_key(last, first):
    core = name_norm(f"concat_ws(chr(32), {_last_core(last)}, {_first_core(first)})")
    # strip a trailing run of generational tokens wherever they landed in the combined stream
    return (f"CASE WHEN {_last_core(last)} IS NULL THEN NULL ELSE "
            f"nullif(regexp_replace({core}, '( ({_GEN}))+$', '', 'g'), '') END")
```

This makes gate 9 actually invariant (P2), restores father/son recall on `first`-field suffixes, and handles
the double-gen tail. Re-verify on appendix ids 4/6/7/8/9/11/12 — all must lose their trailing gen
token(s); pair with the P1 suffix-extraction fix so the peeled tokens are still captured.

### P9 — `skip_if_current` can skip a needed rebuild on a within-label content change — **MEDIUM**

**Plan location.** §7 (`skip_if_current` snap-key compare on `sam_extract_label`); §10 ("`skip_if_current=
True` guarding against a stale or duplicate run").

**Claim.** Comparing `max(snap_key(sam_extract_label))` source-vs-sidecar safely no-ops when the sidecar
already reflects the current vintage.

**Evidence (the worker + trigger comments).** `sam_extract_label` is the SAM **monthly** extract label
(`sam_labels.py:1-7`, `YYYYMMDD`/`YYYY_MMM`). `sam_pocs` rebuilds **daily** (`sam_pocs.ts:33` cron `30 16`)
as an idempotent overwrite, but its content only changes when `entity_registrations` changes — a monthly /
manual-backfill event (`sam_pocs.ts:13` "SAM monthly extract, manual backfill"). In steady state the label
moves when content moves, so the skip is usually safe. The hole: a **within-month correction at a stable
label** — exactly the kind of event the codebase just had (`sam_pocs.py:13-18`: the
`MONTHLY_*_MODIFIED` uei-reclassification fix in `entity_registrations_bulk.py`) — changes `sam_pocs`
content under the *same* `YYYY_MMM` label. The sidecar's `skip_if_current` then compares equal keys and
skips the needed rebuild; the sidecar serves stale `person_key`s until the next label bump.

This is the **same exposure the entities precedent carries** (its source `sam_master_entities` is equally
label-keyed), so it is inherited, not introduced — but the entities review did not flag it and the POCs
plan's daily cadence makes a stale window more visible.

**Why it's risky.** A correctness fix to the upstream that doesn't bump the label is silently not propagated
to the resolution sidecar.

**Remediation.** Cheapest correct guard: compare a **content fingerprint** alongside the label — e.g.
`max(snap_key) || ':' || count_rows()` of the source vs the sidecar's recorded `(sam_label, rows_written)`
in `ops.sam_normalized_pocs_runs`. A row-count change at a stable label forces a rebuild. Document that the
**manual `modal run` path uses `skip_if_current=False`** (the entities `build` entrypoint does, per
`sam_normalized_entities.py:613-615`), so an operator who re-ingests `entity_registrations` mid-month must
trigger the sidecar manually to bypass the skip — and make the new `src/trigger` task and §12 say so.

### P10 — `generational_suffix` enum incomplete; alternation-order behaviour undocumented — **MEDIUM**

**Plan location.** §5 (`_GEN="JR|SR|II|III|IV|V"`).

**Evidence.** The enum omits the ordinal spellings `2ND`/`3RD`/`4TH` that appear in US name data alongside
`II`/`III`/`IV`; a `BAILEY 3RD` keeps `3RD` in `person_key` and never populates `generational_suffix`.
Separately, the appendix confirms `regexp_extract('SMITH III', ' (JR|SR|II|III|IV|V)\.?$',1)` returns `III`
(not `II`) — DuckDB's RE2 anchors at `$` so the longest terminal match wins — but this is load-bearing and
should be asserted, not assumed.

**Remediation.** Extend `_GEN` to `"JR|SR|II|III|IV|V|2ND|3RD|4TH"` (order longer/ordinal forms so they
can't be shadowed) and add §11 assertions: `SMITH III`→suffix `III`, `SMITH 3RD`→`3RD`. Keep the `name_norm`
strip of the trailing-period (`JR.`) — the appendix shows `SMITH JR.`→`SMITH JOHN`, so periods are handled.

### P11 — §10 cron margin is 25 min; the dispatcher `skip_if_current` path is unproven in the precedent — **MEDIUM**

**Plan location.** §10 (interim cron `0 18`, "margin after `sam_pocs` worst-case"); §7 (clone control plane).

**Evidence.** `sam_pocs` worst case = 16:30 + `maxDuration:3900`s (`sam_pocs.ts:34`) = **17:35**; interim
cron `0 18` UTC gives **25 min** of margin. If `sam_pocs` runs long or retries, the sidecar reads the
prior-day dataset; `skip_if_current` then *correctly* skips (label unchanged), so the sidecar simply lags a
day — not a corruption, but a silent staleness the plan's "margin" framing understates. Additionally, **no
`src/trigger/sam_normalized_entities.ts` exists** (verified: `src/trigger/` has `crosswalk_sam_usaspending`,
`sam_opps_bulk`, `sam_pocs`, `sam_spine_refresh` only), so the `skip_if_current`-via-dispatcher orchestration
the plan clones has **never been exercised on cron** — the entities `build` entrypoint hard-codes
`skip_if_current=False`. The dispatcher kwargs path is nonetheless sound: `sam_pocs.ts` sends `kwargs:{}`,
`modal_dispatcher.py:53` spreads `fn.spawn(**req.kwargs, trigger_callback_url=...)`, and the new worker
defaults `skip_if_current=True`, so the empty-kwargs cron call gets the intended default (verified).

**Remediation.** Adopt the §10 "durable form" (chain off `sam_pocs`'s success callback) sooner rather than
"next cycle" — it removes both the 25-min race and the unproven-cron concern. If the interim cron ships
first, widen it to `30 18` (55 min margin) and add an ops alert when `skip_if_current` skips two consecutive
days (a proxy for an upstream that never finished). Smoke-test the dispatcher path once with `modal run`
before relying on cron.

### P12 — `init_ops` ordering is belt-and-suspenders (non-fatal) — **MEDIUM (resolve as OK)**

**Evidence.** §12 step 3 runs `init_ops` before the dry-run. Both precedents already call
`cur.execute(OPS_DDL)` inside `_prior_success_baseline` and `_record_run`
(`sam_normalized_entities.py:304,330`; `sam_pocs.py:442,469`), so the table is self-created on first use; a
skipped step 3 is non-fatal. **Remediation:** note in §12 that `OPS_DDL` is idempotent and self-applied, so
step 3 is optional hygiene, not a prerequisite — prevents an agent from blocking on a Postgres perms hiccup
at step 3.

### P13 — "union-compatible" claim is false against the actual sibling schemas — **LOW**

**Evidence.** §4 ("Column names are chosen **union-compatible** with `sam_master_contacts` / `ffata_exec_comp`
so a future person spine is a literal `UNION ALL`"). The diagnostic §1 table shows `sam_master_contacts`
uses `first_name`/`middle_initial`/`last_name` and `state_or_province`/`zip_postal_code`/`zip_code_4`; the
sidecar uses `middle_name`/`middle_norm` and `state2`/`zip5`. `ffata_exec_comp` carries a single opaque
`officer_name` (no split parts) and **no geo** at all. A future union is **not** literal — it needs a
select-list alias pass. **Remediation:** soften §4 to "column names follow the `state2`/`zip5` convention; a
future union with `sam_master_contacts`/`ffata_exec_comp` requires an explicit alias projection (those
carry `middle_initial`/`state_or_province` and, for File-E, an unsplit `officer_name` + no geo)." This
mirrors B4 in the entities review (column-name drift blocking a claimed-literal union).

### P14 — `name-alpha` gate threshold borrowed without re-derivation — **LOW (resolve as OK)**

**Evidence.** §8 gate 8 ("`person_key` alpha-char fraction `≥ 0.95`") reuses `sam_pocs`'s `NAME_ALPHA_MIN`
(`sam_pocs.py:114`), which measures `first_name` alpha-fraction. `person_key` is a multi-token string with a
space separator; the alpha-fraction over a space-containing key differs from a single first name.
**Remediation:** confirm in the dry-run that the *peeled key's* alpha-fraction clears 0.95 (it should —
human surnames are alphabetic — but `3M`/`P3` style employer-name contamination, if any POC surnames are
junk, would dent it); if it runs ~0.93, set the floor at 0.90 with a logged actual. Cheap, observability.

---

## 4. What's correct / well-founded (do not "fix" these)

- **Sidecar-not-columns (§2)** is the right call for the same reasons as the entity sidecar: volatile key
  policy, contract purity of the ZERO-ALTERATION `sam_pocs`, and Lance BTREEs a stored column not an
  expression. Verified against `sam_pocs.py:36-42`.
- **1:1 lossless grain, no filter, foreign POCs retained with nullable geo (§3)** is correct — the
  `country='USA'` filter is rightly a consumer concern. The diagnostic's per-row geo (state 98.72%, zip5
  99.06%, co-present 98.08%) supports the denormalized geo-on-row design.
- **Every `sam_pocs` column §4/§6 scans actually exists** in the `unpacked` SELECT output (`uei`,
  `cage_code`, `poc_type`, `source_family`, `first_name`, `middle_name`, `last_name`, `full_name`, `state`,
  `zip5`, `country`, `sam_extract_label` — verified via `build_pocs_sql`'s `_POC_FIELD_NAMES` and the
  `unpacked` projection, `sam_pocs.py:198-288`). No phantom columns.
- **The composition of `core.name_norm` is sound** — `person_key`/`surname_initial_key`/`last_name_norm`/
  `middle_norm` all call `name_norm(...)` and never re-inline the regex; the §5 docstring correctly forbids
  re-inlining. `name_norm` itself behaves as documented on the fixture (`O'BRIEN`→`OBRIEN`,
  `SMITH-JONES`→`SMITH JONES`, `&`→`AND`, case-fold, punctuation strip). This is the part the plan got most
  right.
- **The control-plane clone is faithful and the cited line range is accurate.** The rollback guard at
  `sam_normalized_entities.py:449-494` (`v_before` capture → write+index+post-write gates →
  `version=v_before).restore()` on failure → re-raise; `None` on net-new) is exactly the pattern §8
  prescribes, and the §8 reference "`sam_normalized_entities.py:450-494`" lands on it. Pre-write-gates-on-
  Arrow-then-write ordering is correct.
- **`snap_key_sql` import is valid:** `pipelines.sam_gov.reference.sam_labels.snap_key_sql` exists and
  `sam_pocs` carries `sam_extract_label` (verified `sam_pocs.py:282`), so the §7 import + `skip_if_current`
  compare are wired correctly (the *staleness window* is P9, not a wiring bug).
- **The dispatcher kwargs spread is safe** (P11 evidence): empty-kwargs cron + defaulted `skip_if_current=
  True` yields the intended behaviour; no positional-collision bug.
- **Slot-fanout gate 10 (max ≤6) is structurally valid:** `sam_pocs` is 1/(entity, slot) with exactly 6
  defined slots (`_POC_TYPE_BY_SLOT`, `sam_pocs.py:202-206`); partitioning by
  `coalesce(uei,'CAGE:'||cage_code)` is the right key and every row carries at least one of uei/cage
  (`sam_pocs.py:256`). The six poc_type counts sum to 8,065,116 (verified).
- **Gate 3 deletion / homonym reframing (§2)** is the correct person-vs-company inversion; a degenerate
  `person_key` is normal and the sensitive check rightly moves to the addressable triple.

---

## 5. Remediation summary (ordered punch-list for the executing agent)

Apply **before** the first build; the build is NO-GO until items 1–4 are done and re-verified against the
appendix harness.

1. **[P1+P8 — BLOCKER] Fix the generational mechanism in `core/person_name_norm.py`.** (a) Extract
   `generational_suffix` from `last_name` alone, end-anchored (not from `last⎵first`). (b) Peel the trailing
   gen *run* from the combined `name_norm(last_core⎵first_core)` stream inside `person_key`, repeating for
   double suffixes. Re-run appendix harness; require `gen_suffix='JR'` on `(last='SMITH JR', first='JOHN')`
   and zero `person_key` ending in a gen token across all fixture rows.
2. **[P2 — BLOCKER] Make gate 9 a true post-peel invariant** (now satisfiable after item 1) and add a §11
   fixture row with the suffix in `first_name`. Do **not** ship gate 9 against the current field-scoped peel.
3. **[P3 — BLOCKER] Reconcile §11 with reality.** Drop or rewrite the `DR JANE FOX MD → FOX JANE` and
   `last='JR' → NULL` assertions to match the regex's actual behaviour, OR extend `_last_core` (honorific
   strip on `last_name`; whole-field gen strip) to make them true — then assert. The §11 suite must pass on
   first authoring (§12 step 2).
4. **[P6 — CRITICAL] Make §6 `first_initial` use `_first_core`**, not raw `name_norm(first_name)`, so it
   agrees with `surname_initial_key`. Add the `first='DR JANE'→'J'` assertion.
5. **[P5 — CRITICAL] Measure the real `person_key` cardinalities in the dry-run** (`distinct_person_key`,
   the `country='USA'` triple, the ratio) and recalibrate §8 floors against *those* numbers, not the
   diagnostic's differently-keyed 2.12M / 2.868M. Update §3/§8 prose to state the key drops middle and peels
   gen/honorific.
6. **[P4 — CRITICAL] Re-floor §8 below the Δ-band:** `PK_DISTINCT_FLOOR≈1.55M`, `ADDR_TRIPLE_FLOOR≈2.1M`
   (recompute from item 5), `ROW_FLOOR=6.0M`, `BASELINE_MIN_ROWS=6.0M` (== row floor, so a legitimate purge
   can re-baseline). Verify each sensitive floor < live×0.75 before shipping.
7. **[P7 — HIGH] Set `DUCKDB_MEMORY_LIMIT="24GB"`** (match the 8M `sam_pocs` precedent), keep `memory=32768`
   (bump to 49152 if the first index build OOMs), keep index creation sequential, and order the two high-card
   string BTREEs first. Note that bypass-spilling is genuinely needed only for `person_key`/
   `surname_initial_key`.
8. **[P9 — MEDIUM] Add a content fingerprint to `skip_if_current`** (`max(snap_key)`+`count_rows()` vs the
   ops-ledger `(sam_label, rows_written)`) so a within-label correction forces a rebuild; document the manual
   `skip_if_current=False` escape hatch in §12 and the trigger task.
9. **[P10 — MEDIUM] Extend `_GEN`** to include `2ND|3RD|4TH`; assert `III`/`3RD` extraction in §11.
10. **[P11 — MEDIUM] Prefer the callback-chain control plane**; if interim cron ships, widen to `30 18` and
    alert on two consecutive `skip_if_current` skips. Smoke-test the dispatcher path with `modal run` first.
11. **[P12/P13/P14 — MEDIUM/LOW] Doc/threshold polish:** note `OPS_DDL` is self-applied (P12); soften the
    "literal `UNION ALL`" claim to "alias projection required" (P13); confirm gate-8 alpha-fraction on the
    peeled key in the dry-run and floor it at the measured value (P14).

---

## 6. Empirical verification harness (appendix — reproducible)

Two ephemeral DuckDB scripts (`uv run --with 'duckdb>=1.5,<2' python …`). They import the §5 builders +
`core.name_norm.name_norm` **verbatim** and run the generated SQL over synthetic rows. **Zero R2 / Lance /
ops mutation.** Findings P1/P2/P3/P6/P8/P10 are reproduced directly by re-running these; P4/P5 by the
arithmetic script.

### Harness 1 — full adversarial fixture → all derived columns

`/tmp/pnn_harness.py`: defines `name_norm` (copied from `core/name_norm.py:39-60`) and the §5 builders
(`_GEN/_HONORIFIC/_CREDENTIAL`, `_upper`, `_last_core`, `_first_core`, `person_key`, `surname_initial_key`,
`generational_suffix`) copied verbatim from the plan, builds the §6 `SELECT`, and runs it over 30 rows.
Fixture (id, first, middle, last, note) covers: `SMITH`/`SMITH JR`/`SMITH SR`/`SMITH III`; `DR JANE FOX MD`;
`BAILEY`+`C.E.`; `VAN DER BERG`/`DE LA CRUZ`/`O'BRIEN`; suffix-looking surnames `MAY`/`VI`/`JR`/`MAYO`;
honorific-only first `DR`; null/empty surname; mixed case; `SMITH JR.`; `JRINKINS`; gen `V`/`IV`/`II`;
null first; two-token first `MARY JO`; credential surnames `MD`/`PE`; particle `ST CLAIR`; hyphen
`SMITH-JONES`; double-gen `SMITH JR SR`; `VINCENT`. Literal output table reproduced under P3/P6/P8 above;
the PLAN-CLAIM CHECKS block prints `XX` for each false §11 claim.

### Harness 2 — where the suffix lives (gate-9 + preservation)

`/tmp/pnn_harness2.py`: imports the same builders, exposes the raw `gen_src_string` and `last_core`, and
runs 12 rows that vary *which field* carries the suffix (last only / last+no-first / first only / bare
`JR`/`SR`/`V`/`III` first / double-gen). It scans for `person_key` ending in a gen token (gate-9 violations)
and for rows where `person_key` peeled a suffix but `generational_suffix` is NULL (silent drop). Literal
output reproduced under P1/P2 above — 6 gate-9 violations, 3 silent suffix drops.

### Harness 3 — gate arithmetic (P4/P5)

`/tmp/gate_arith.py`: for `rows`/`distinct_person_key`/`addressable triple`, computes Δ-lower = live×0.75
and flags floors that bind first; analyses `BASELINE_MIN_ROWS` vs the row floor. Output table reproduced
under P4. (P5's key-mismatch is established from the diagnostic source lines 91 vs 190, not a run.)

**Run all three:**
```bash
cd /tmp
uv run --with 'duckdb>=1.5,<2' python pnn_harness.py      # harness 1 — full fixture + claim checks
uv run --with 'duckdb>=1.5,<2' python pnn_harness2.py     # harness 2 — gate-9 + suffix preservation
uv run python gate_arith.py                               # harness 3 — §8 floor-vs-Δ arithmetic
```

All three are deterministic and self-contained (the builders are inlined at the top of harness 1; harness 2
imports from harness 1). An agent applying §3 fixes 1–4 re-runs harnesses 1–2 and must see: every
PLAN-CLAIM CHECK `OK`, zero gate-9 violations, zero silent suffix drops, and `gen_suffix` populated on the
`first_name`-present suffix rows.
