# Goods Lane — `naics_psc_labor_profile` (make-vs-resell) — Execution Plan

> **Cycle:** the 2,361 `psc_is_service=false` combos of the subaward-gap worklist, run through the L2
> labor-profile classifier as a new append-only vintage. Sibling of the just-landed service lane
> (`source_vintage=subaward_gap_service_3061`, PR #911 / `8d5937f`).
>
> **Provenance to stamp:** `source_vintage=subaward_gap_goods_2361`, `prompt_version=goods_profile_v1`,
> `model_id=claude-opus-4-8:in-session` (zero Anthropic API — in-session Opus/xhigh subagents, waves of 4).
>
> **Status:** design complete and adversarially reviewed (3 reviewers, verdict `ship-with-fixes`; all
> fixes folded in below). Awaiting operator sign-off on the four locked decisions in §10, then execute.

---

## 1. Objective & scope

Produce one labor profile per distinct goods `(naics_code, psc_code)` combo that flows to subawardees
but has no profile yet — **2,361 combos, 339 distinct NAICS, $30.1B of subaward flow**. Append to the
existing two datasets without touching the govcon (`govcon_active_awards_2026-07-01`, 8,690) or service
(`subaward_gap_service_3061`, 3,061) vintages.

The service head asks *"which labor categories deliver this service."* The goods head must answer a
**prior binary** the service head only gestures at (its rule 4): **does winning this product combo put
dedicated labor on payroll to _make/transform/integrate_ the good, or is the good bought finished and
_passed through_ (resale / lease / license)?** That make-vs-pass-through decision is the crux of this
cycle; category selection flows from it.

**Why the rails already exist.** `NPLP_LANE=product` (worklist lane filter), the directive field-map
(`n_subawards`→`n_awards`, `subaward_dollars`→`total_dollars`, etc.), the `psc_category` column
(BTREE+BITMAP indexed), the vintage-scoped upsert, and the fail-closed gate all landed with #911. The
goods lane adds **one new prompt head + a single-knob resolver that binds head/version/vintage/manifest
to `NPLP_LANE`**, and reframes one hardcoded harness preamble. No schema change, no new dataset.

**Out of scope.** The parallel *deliverable* track (`subaward_head_5422`, `what_was_done`+`work_type`)
is independent and composes downstream on `(naics, psc)` — see §12.

---

## 2. Ground truth (verified live against the CSV and the module, 2026-07-02)

| Fact | Value | Source |
|---|---|---|
| Worklist | `~/Desktop/hq/subaward_gap_5422.csv` (goods = `psc_is_service=false`) | file probe |
| Goods combos | **2,361** distinct `(naics,psc)`, 1:1, across **339** NAICS | CSV |
| Subaward dollars | **$30.1B** | CSV |
| Lane-filter values | raw `False`/`True`; `lower('False')='false'` matches `product` (module L305) | CSV + module |
| NAICS sector mix | **33=1,598, 31=74, 32=47 → ~71% Manufacturing**; 54=233, 42=109, 51=78, 56=51, 23=38, 81=37, 44/45=43, 48=15, 61=9, 92=7, 22=5 | CSV |
| PSC leading digit | 5=515, 6=346, 7=298, 4=255, 1=438, 2=228, 3=136, 8=112, 9=33 | CSV |
| `psc_category` | **sourced from `psc_reference` (R2), NOT the CSV column** (module L440 `pr.get("psc_category")`) — no backfill needed *iff* `psc_reference` covers every goods PSC | module |
| Run shape | **386 calls** (≤20 PSC/call) → **26 slices** (15 calls/slice) → **7 waves @ C=4** | computed |
| Largest single call | NAICS `339999` → 87 PSCs → 5 calls | CSV |

**Top goods combos by dollars** (the hard cases the head must get right):

| combo | $ | note |
|---|---|---|
| `336992 × 6505` | $7.06B | **FPDS mis-code**: armored-vehicle NAICS × DRUGS AND BIOLOGICALS → `off_pattern` |
| `927110 × 1677` | $4.22B | **sector-92** (National Security) × SPACE VEHICLE REMOTE CONTROL → integration, `off_pattern` |
| `541712 × 1410` | $1.40B | **build-to-print**: R&D NAICS × GUIDED MISSILES (candidates skew biotech → `off_pattern`) |
| `423430 × 7050` | $1.21B | **clean RESELL**: computer-peripheral WHOLESALE × IT COMPONENTS → `is_labor_play=false` |
| `336413 × 7610` | $0.67B | aircraft-parts × BOOKS → likely mis-code → `off_pattern` |

---

## 3. The crux — `SYSTEM_RULES_GOODS` (final head, verbatim)

Add as a new module constant immediately after `SYSTEM_RULES`. It **replaces** the service framing for
the goods lane; the service head stays the default. Every reviewer fix is folded in (catch-all sector
bucket, cross-discipline `off_pattern`, value-add wholesale carve-out, collapsed lease bucket).

```python
SYSTEM_RULES_GOODS = """You classify the labor demand implied by U.S. federal contract awards for PHYSICAL GOODS (product
PSCs). Each task gives you ONE NAICS industry (with its real BLS-OEWS occupational staffing pattern as
CANDIDATES) and a list of PRODUCT PSC codes awarded under that NAICS. For EACH PSC, FIRST decide whether
winning that combo puts dedicated labor on payroll to PRODUCE / TRANSFORM / INTEGRATE the good (a
"make"), or whether the good is bought finished and passed through (resale / lease / license — negligible
dedicated labor); THEN, for a make, select the labor categories the winner must staff.

THE CENTRAL DECISION — make vs. pass-through (decide FIRST, per PSC). The SAME product PSC is
labor-bearing or not depending on the NAICS context and the scope text. Use these buckets:

  MAKE (is_labor_play=true — dedicated labor on payroll):
    - Manufacturing NAICS (sectors 31-33) x a product PSC: the winner fabricates / assembles / machines /
      welds / finishes the good. Staff the production floor.
    - Engineering / R&D / technical-services NAICS (sector 54, e.g. 541330 Engineering Services,
      541712/541715 R&D) x a hardware / end-item PSC: this is BUILD-TO-PRINT / prototype / systems
      integration -- the firm designs AND builds (or builds to a furnished design). Labor-bearing: the
      correct engineering discipline PLUS the production trades that integrate the article. NOT resale.
    - Overhaul / remanufacture / rebuild / retrofit / depot-reset of an existing article (any NAICS,
      including repair NAICS 81): physically transforming an article is labor (maintenance 49-xxxx +
      production 51-xxxx + inspection/QC).
    - ANY OTHER NAICS sector (telecom 51, facilities 56, construction 23, transport 48-49, national
      security 92, utilities 22, education 61) x a product PSC where the deliverable is genuinely produced,
      integrated, or operated: DEFAULT is_labor_play=true; classify by the PSC DELIVERABLE via off_pattern
      (rule 6), not by the orthogonal NAICS candidates. (E.g. 927110 Space Research x 1677 SPACE VEHICLE
      REMOTE CONTROL SYSTEMS is space-C2 integration labor, not government administration.)

  PASS-THROUGH (is_labor_play=false -- categories MUST be []):
    - Wholesale-trade NAICS (sector 42) x a product PSC, DEFAULT: a distributor sourcing a finished good
      and reselling it (COTS). false -- UNLESS the scope text shows install / kit / assembly / maintenance
      value-add, then is_labor_play=true with thin labor.
    - Retail-trade NAICS (sectors 44-45) x a product PSC, DEFAULT: bought finished and resold. false --
      same value-add exception.
    - Lease / rental / license / subscription of a good, or a hardware-as-a-service delivery (any NAICS),
      where the scope names no operations / maintenance labor: false. If the scope names a managed or
      operated service around the good, treat it as a make (labor-bearing).
    - A firm coded to a MANUFACTURING NAICS can still be pass-through when the PSC + scope make clear the
      good is bought finished and drop-shipped (a mfg-coded distributor). The DELIVERABLE governs, not the
      NAICS.

Rules -- follow exactly:
1. is_labor_play -- apply the make-vs-pass-through decision above, reading the scope text when NAICS/PSC
   alone are ambiguous. true = the winner makes / fabricates / assembles / integrates / remanufactures /
   operates the good; false = resale / drop-ship / COTS / lease / license / pure distribution (categories
   MUST be []). When genuinely ambiguous AND the scope shows no value-add, let the NAICS sector break the
   tie: 31-33 / 54 -> make; 42 / 44-45 -> pass-through. Bias to is_labor_play=true when value-add signal
   is present -- a false permanently erases the combo.
2. soc_code MUST come from the DETAILED SOC VOCABULARY below. For a make, the core_deliverable labor is
   the PRODUCTION occupations (SOC 51-xxxx -- assemblers, machinists, welders, fabricators, machine
   operators, tool & die, foundry, plating/finishing) and inspectors/testers (51-9061). Prefer the
   industry CANDIDATES (they ARE the real OEWS staffing pattern for a manufacturing NAICS); set
   off_pattern only per rule 6.
3. For a make, also staff -- where the article and contract scale require it -- the SUPPORT labor the line
   cannot run without: design/manufacturing engineers of the CORRECT discipline (SOC 17-xxxx -- mechanical
   17-2141, electrical 17-2071, industrial 17-2112, aerospace 17-2011) when the good is engineered /
   build-to-print; production/logistics support (17-3026 industrial-eng techs, 53-xxxx material movers);
   and the OVERHEAD the contract forces (PM, contracts/procurement, quality-system admin). List overhead
   only when contract scale plainly requires it.
4. sca_code MUST come from the SCA VOCABULARY below, or null when none fits. Production and maintenance
   trades map WELL to SCA (blue-collar / technician heavy) -- prefer a real SCA code for 51-xxxx / 49-xxxx
   roles; professional engineers / scientists (17-xxxx / 19-xxxx) usually have no SCA equivalent (null is
   correct there).
5. Rank categories by centrality to PRODUCING / DELIVERING the good (array order = rank 1..N, max 10).
   role_class: core_deliverable = the labor that physically makes / transforms the good; support =
   enabling engineering / test / material-handling labor; overhead = PM / contracts / QA-system labor the
   contract forces on payroll.
6. off_pattern -- product PSCs frequently sit on an orthogonal or wrong-domain NAICS. TWO cases, both set
   off_pattern=true: (a) FPDS MIS-CODE -- the NAICS and PSC are from unrelated domains (e.g. an
   armored-vehicle NAICS awarded a DRUGS PSC, or an aircraft-parts NAICS awarded a BOOKS PSC): classify by
   the PSC DELIVERABLE (what the government actually obtained), reaching the correct occupations in the
   full SOC vocabulary rather than forcing an irrelevant candidate. (b) CANDIDATE-DOMAIN MISMATCH -- the
   NAICS resolves to a staffing pattern dominated by a DIFFERENT technical domain than the deliverable
   (e.g. 541712 R&D resolves to medical-scientist candidates but the PSC is GUIDED MISSILES): set
   off_pattern=true to reach BOTH the correct engineering discipline (17-2011 aerospace, 17-2141
   mechanical, ...) AND the 51-xxxx production the build-to-print article needs, even though they sit
   outside the candidate list. A services / R&D NAICS awarded a hardware PSC is legitimate build-to-print,
   not garbage -- never zero it out as resale.
7. work_summary: <=20 words, concrete -- for a make, what the winner physically produces / integrates /
   overhauls; for a pass-through, note the good is sourced-finished and resold / leased.
8. confidence: high = obvious make (mfg NAICS x matching product PSC) or obvious resale (wholesale/retail
   NAICS x COTS PSC); medium = build-to-print, mfg-coded distributor, or value-add wholesale; low = thin
   signal or heavy reliance on a mis-code interpretation.
Select ONLY codes from the vocabularies. Output must satisfy the JSON schema exactly."""
```

The output JSON schema (`_output_schema`, module L491) is **lane-agnostic and unchanged**: `soc_code`
enum = 830 detailed SOC (production 51-xxxx are already members), `sca_code` = nullable 502-SCA enum. No
schema edit.

---

## 4. Combo taxonomy (data-grounded — drives the head and the audit)

| Bucket | ~count | `is_labor_play` | `off_pattern` | Example combos |
|---|---|---|---|---|
| **MAKE — manufacturing (31-33) × product PSC** | ~1,600–1,700 | true | false (candidates ARE the staffing pattern) | `336419×1427` GM subsystems $753M; `332992×1390` fuzes/primers $527M; `332995×1440` missile launchers $424M |
| **SERVICES-NAICS INTEGRATION (54) × product PSC** | 233 | true (engineering-weighted) | often **true** (candidate-domain mismatch) | `541712×1410` R&D × GUIDED MISSILES $1.40B; `541330×1240` optical sighting $723M |
| **RESELL / COTS (42 wholesale, 44-45 retail)** | ~152 | **false** default (thin-true if value-add) | n/a when false | `423430×7050` computer-wholesale × IT components $1.21B (clean resell) |
| **OVERHAUL / REMANUFACTURE / DEPOT (81 + remanufacture keywords)** | ~37+ | true always | mostly false | `811219×5998` electronic-assemblies repair $33M; `326291×2640` tire rebuilding |
| **CATCH-ALL — other sectors (51/56/23/48-49/61/92/22) × product PSC** | ~220 | true default (classify by deliverable) | usually **true** | `927110×1677` space C2 $4.22B (sector-92) |
| **LEASE / LICENSE / aaS pass-through** | ~55–80 | **false** (true iff scope names managed/operated service) | n/a when false | explicit LEASE/LICENSE/RENTAL in `psc_name`; HW-as-a-service |
| **FPDS MIS-CODE (domain-mismatched NAICS×PSC)** | ~20–60 | true (classify to PSC deliverable) | **true** by definition | `336992×6505` DRUGS $7.06B; `336413×7610` BOOKS $0.67B |

**Expected distribution to pre-declare** (so validators do **not** false-alarm — see §8): goods will run
a **much higher `is_labor_play=false` rate (~110–206 placeholder combos)** and a **much higher
`off_pattern` rate (est. 40–60% of category rows)** than the service vintage. This is correct, not drift.
There is **no ratio gate** anywhere — the only hard gate is 2,361 combos present.

---

## 5. Code changes — single-knob, lane-bound resolver

**Design principle:** `NPLP_LANE=product` is the *single* switch that drives the entire goods cycle —
worklist filter (exists), **manifest URI**, **system prompt head**, **prompt_version**, **default source
vintage**, and a **hard desync guard**. This closes the two mechanics blockers the review found
(shared-overwrite manifest; silent lane/vintage/version desync) and keeps the service default
byte-identical.

### 5.1 `pipelines/reference/materialize_naics_psc_labor_profile.py`

**(a) New constants + resolver** (after `SYSTEM_RULES`, ~L153; and near the URI/version block, ~L81-92):

```python
SYSTEM_RULES_GOODS = """...see §3..."""
PROMPT_VERSION_GOODS = "goods_profile_v1"
DEFAULT_VINTAGE_GOODS = "subaward_gap_goods_2361"

def _lane() -> str:
    return os.environ.get("NPLP_LANE", "").strip().lower()

def _prompt_head() -> tuple[str, str]:
    """(system_rules, prompt_version) keyed on NPLP_LANE. Unset/service -> byte-identical service."""
    if _lane() == "product":
        return SYSTEM_RULES_GOODS, PROMPT_VERSION_GOODS
    return SYSTEM_RULES, PROMPT_VERSION
```

**(b) Lane-aware DEFAULTS for the two shared mutable resources** (so one knob isolates them; explicit
env override still wins):

```python
# MANIFEST_URI — goods writes a SEPARATE manifest so `manifest` never overwrites the service manifest
# (build_manifest calls _write WITHOUT upsert_vintage => mode="overwrite" — a shared-overwrite hazard).
MANIFEST_URI = os.environ.get("NPLP_MANIFEST_URI") or (
    A + ("_naics_psc_labor_profile_manifest_goods/" if _lane() == "product"
         else "_naics_psc_labor_profile_manifest/"))

# SOURCE_VINTAGE — default binds to the lane so it can't silently stay govcon under lane=product.
SOURCE_VINTAGE = os.environ.get("NPLP_SOURCE_VINTAGE") or (
    DEFAULT_VINTAGE_GOODS if _lane() == "product" else "govcon_active_awards_2026-07-01")
```

**(c) Route head + version through the resolver** (three sites, so provenance is coherent on the success
path, the manifest ledger, AND the fail-closed gap ledger):

- `_build_requests()` system text (~L526-529): use `_prompt_head()[0]` in place of `SYSTEM_RULES`.
- `prov()` (~L633): use `_prompt_head()[1]` in place of `PROMPT_VERSION`.
- **all three** `_record_run` calls — manifest (~L458), retrieve **gap** branch (~L735), retrieve
  **success** branch (~L755): stamp `prompt_version=_prompt_head()[1]`.

**(d) Hard desync guard at the top of `retrieve()`** (self-contained — the frozen manifest carries no
lane column, so the guard keys off the two env-derived constants directly):

```python
if _lane() == "product" and not SOURCE_VINTAGE.startswith("subaward_gap_goods"):
    raise RuntimeError(f"lane=product but SOURCE_VINTAGE={SOURCE_VINTAGE!r} is not a goods vintage "
                       "— refusing to write (provenance would desync).")
if _lane() != "product" and SOURCE_VINTAGE.startswith("subaward_gap_goods"):
    raise RuntimeError(f"goods vintage {SOURCE_VINTAGE!r} but lane!=product (head/version would stamp "
                       "service) — refusing to write.")
```

### 5.2 `pipelines/reference/labor_profile_insession/gen_workflow.py` — neutralize the hardcoded preamble

`slicePrompt()` hardcodes a **service-framed** preamble independent of `system.txt`. Two lane-neutral edits
(they defer framing to `system.txt`, which now renders the lane-correct head):

- **L54** — replace `'You classify the labor demand implied by U.S. federal contract awards.\n\n'` with a
  lane-neutral opener that points at `system.txt` as authoritative: *"You classify what U.S. federal
  contract awards imply for the winning contractor. The authoritative task framing and the TWO controlled
  vocabularies are in `<SYSTEM_TXT>`. READ THAT FILE ONCE, IN FULL, FIRST — it governs."*
- **L67** — neutralize the service-specific clause `"the core_deliverable labor that IS the service"` →
  `"the core_deliverable labor that IS the primary work"`. Rest of the ranking sentence unchanged.

**No `prompt_head.txt`** (the deep-dive proposed one; rejected — it is redundant with `system.txt`, and its
`split('\n\n',1)[0]` derivation risks truncating a multi-paragraph head). `system.txt` already carries the
full lane-correct head via `_build_requests()` → `prep.py`. **`prep.py` therefore needs no edit.**

### 5.3 `pipelines/reference/labor_profile_insession/RUNBOOK.md`

Document the single-knob goods cycle: `NPLP_LANE=product` must stay exported across **manifest → prep →
retrieve** (it drives head, version, vintage, and manifest URI); the goods manifest is a **separate,
overwrite-mode** dataset; and the R2 checkpoint **resume recipe** (§6, step 5R).

### 5.4 No change required

`validate.py` (no ratio gate — only coverage/defects/vocab), `assemble.py` (`psc`→`psc_code` normalize),
`verify_datasets.py` (reconciliation `non-play == null-soc placeholder` holds **cross-vintage**; a two-value
`prompt_version` group-by is expected). An *optional* `NPLP_SOURCE_VINTAGE` filter in `verify_datasets.py`
would give a cleaner vintage-scoped read but is not required.

---

## 6. Run sequence (exact)

```bash
# 0. From repo root. ONE export block for the whole cycle (NPLP_LANE drives head/version/vintage/manifest):
export NPLP_WORKLIST_CSV=/Users/benjamincrane/Desktop/hq/subaward_gap_5422.csv
export NPLP_LANE=product
export NPLP_SCRATCH=/tmp/nplp_goods
export NPLP_SLICE_SIZE=15
export NPLP_CONCURRENCY=4                      # HARD cap — a wider fan-out tripped session HTTP 429
export NPLP_CHECKPOINT_PREFIX=staging/nplp_insession_goods
# (NPLP_SOURCE_VINTAGE and NPLP_MANIFEST_URI default correctly from NPLP_LANE=product — do not set them.)

# 1. Manifest over the goods lane. EXPECT: "worklist (external ...subaward_gap_5422.csv, lane=product): 2,361 combos"
doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' --with 'pyarrow>=17' \
  --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
  python3 pipelines/reference/materialize_naics_psc_labor_profile.py manifest
#    -> 2,361 combos / 339 NAICS / 386 calls is the fail-closed gate target. Manifest lands at
#       _naics_psc_labor_profile_manifest_goods/ (service manifest untouched).

# 2. Prep — renders system.txt from the GOODS head (because NPLP_LANE=product) + calls/ + cids/slices/npsc/expected.
doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.prep
#    GATE: `head -5 $NPLP_SCRATCH/system.txt` MUST show the make-vs-pass-through framing. Expect
#    calls=386 slices=26 waves@C=4=7.

# 3. Generate + run the sliced workflow in-session, waves of 4, batches of ~4 slices, checkpoint between.
python3 pipelines/reference/labor_profile_insession/gen_workflow.py 0 4     # then 4 8, 8 12, ... 24 26
#    Run each emitted $NPLP_SCRATCH/workflow_<a>_<b>.mjs in-session (opus/xhigh). Loop-until-complete (3 rounds).

# 4. Validate after each batch — require 0 defects / 0 absent / 0 field-missing / 0 SOC-bad / 0 SCA-bad.
doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.validate

# 5. Checkpoint to R2 (goods prefix) after each batch.
doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.checkpoint_r2
#  5R. RESUME after a crash: download s3://data-sink/staging/nplp_insession_goods/opus_results_latest.jsonl
#      and re-explode each line (keyed by custom_id) back into $NPLP_SCRATCH/results/<cid>.json, then
#      re-run gen_workflow for the remaining slices. (~7 waves at C=4 — a crash mid-cycle is plausible.)

# 6. Assemble -> agent_results.json (normalizes psc->psc_code, per-call completeness).
doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.assemble

# 7. Retrieve — fail-closed 2,361 gate; desync guard; vintage-scoped upsert (deletes ONLY
#    source_vintage=subaward_gap_goods_2361 then appends); stamps goods_profile_v1 + in-session model_id.
doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' --with 'pyarrow>=17' \
  --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
  python3 pipelines/reference/materialize_naics_psc_labor_profile.py \
  retrieve --agent-results $NPLP_SCRATCH/agent_results.json

# 8. Verify (reads whole dataset; two prompt_versions is EXPECTED).
doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.verify_datasets
```

---

## 7. Fail-closed gate & reconciliation math

- **Gate:** `retrieve` computes `len(seen_combos)`; if `!= 2,361` → `status=partial`, **no write**,
  `sys.exit(2)`. Every combo present or nothing writes.
- **Play vs placeholder (estimate, not a quota):** ~2,150–2,250 play / ~110–206 placeholder (one null-code
  row each). Point estimate ≈ **2,211 play / 150 placeholder**.
- **Category rows (estimate):** play combos emit 1–10 rows; goods staffing patterns run leaner than
  services (service avg ≈ 3.96 rows/combo). Est. **~8,200–9,600 goods category rows**.
- **Post-append SoR totals** (vintage-scoped upsert; other vintages byte-untouched):
  - profiles: 11,751 → **14,112** (exactly 2,361 rows carry `source_vintage=subaward_gap_goods_2361`)
  - categories: 38,822 → **~47,000–48,400**
- **Idempotent re-run:** `_write` does `DELETE WHERE source_vintage='subaward_gap_goods_2361'` then append
  — safe to re-run; never duplicates; never touches govcon/service vintages.

---

## 8. Pre-flight gold set + top-$ review gate (de-risking the $7B mis-code)

A single mislabeled mega-combo poisons dollar-weighted downstream analytics, and rollback — though cheap
via the vintage-scoped delete — is worse than catching it up front.

1. **Gold-set pre-flight (before the full fan-out).** Hand-classify the **top ~6 by dollars** —
   `336992×6505` ($7.06B, mis-code), `927110×1677` ($4.22B, sector-92 integration), `541712×1410`
   ($1.40B, build-to-print), `423430×7050` ($1.21B, clean resell), `336413×7610` ($0.67B, mis-code),
   `336419×1427` ($0.75B, clean make) — against the head. These six concentrate the hardest cases (mis-code,
   build-to-print candidate-mismatch, clean resell, catch-all sector). If any misfires, tune the head and
   re-check, **then** fan out the remaining 380 calls.
2. **Top-$ review gate (before trusting the write).** After classification, spot-review the `off_pattern`
   outputs for the top ~5 mis-code / off-domain combos. `retrieve` will not have written yet if the gate
   is unmet; review these before accepting the materialized vintage.

---

## 9. Verification checklist (post-write)

- [ ] `worklist ... lane=product: 2,361 combos` at manifest; **manifest landed at the `_goods/` URI**.
- [ ] **`psc_category` population checked on the FROZEN MANIFEST** (not the CSV) — it is sourced from
      `psc_reference`; any goods PSC missing from `psc_reference` nulls its category. Pre-flight: confirm
      `psc_reference` covers all goods PSCs (else the null is legitimate and the schema-drift guard still
      passes since the column is present).
- [ ] `head -5 system.txt` shows make-vs-pass-through framing before any batch runs.
- [ ] validate → 0 defects / 0 absent / 0 field-missing / 0 SOC-bad / 0 SCA-bad; coverage 2,361/2,361.
- [ ] retrieve gate passed; desync guard did not fire; **exactly 2,361** rows carry
      `source_vintage=subaward_gap_goods_2361`.
- [ ] verify: profiles == 14,112; provenance 100% `claude-opus-4-8:in-session`; `prompt_version` shows
      `labor_profile_v2` (11,751) + `goods_profile_v1` (2,361) — **two versions is correct**.
- [ ] reconciliation (cross-vintage): 0 combos missing either direction; non-play profiles == null-soc
      placeholder rows; every play combo ≥1 real category.
- [ ] enum validity: SOC/SCA out-of-vocab == 0. Indexes present incl. `psc_category` (BTREE+BITMAP).
- [ ] **off_pattern / is_labor_play=false rates recorded** in the run record as the expected goods skew
      (~40–60% off_pattern, ~150 placeholder) — pre-declared so a future reviewer does not misread it as drift.

---

## 10. Locked decisions (opinionated — operator can override)

| # | Decision | Locked call | Rationale |
|---|---|---|---|
| **D1** | Build-to-print production weight under R&D NAICS (541712 candidates skew biotech) | **Allow `off_pattern=true` to reach BOTH correct engineering discipline AND 51-xxxx production** (baked into head rule 6) | Honest signal — an R&D firm's average staffing genuinely omits a production line; no manifest change; cleanest fix |
| **D2** | Gold-set pre-flight before full fan-out | **Yes — hand-classify top ~6 $ combos, tune head, then fan out** | 6 combos concentrate the hardest cases + a $7B row; cheap insurance against a systematic head misfire |
| **D3** | Human-review gate on top-$ mis-codes before the write is trusted | **Yes — spot-review top ~5 `off_pattern` outputs pre-`retrieve`** | A single $7B mislabel poisons dollar-weighted analytics; reviewing 5 combos beats discovering it downstream |
| **D4** | Wholesale/retail resell heuristic | **Sector-2 heuristic + scope-text value-add read; no hard allowlist** | Rule-1 value-add carve-out + bias-to-true-on-ambiguity already flips genuine value-add distributors; a hard allowlist is premature — revisit only if the post-run audit shows systematic false-false on retail |

---

## 11. Blast radius & rollback

- **Append-only, vintage-scoped.** `retrieve`'s `_write` deletes only `source_vintage=subaward_gap_goods_2361`
  before appending; `govcon_active_awards_2026-07-01` and `subaward_gap_service_3061` are byte-untouched.
- **Manifest isolated.** Goods manifest writes a **separate** `_goods/` URI — the service manifest
  (overwrite-mode, shared by default) is never clobbered.
- **Rollback = re-delete the vintage.** `existing.delete("source_vintage = 'subaward_gap_goods_2361'")` on
  both datasets removes the goods vintage cleanly; re-running the cycle is idempotent.
- **Honest service-path invariant.** The service `SYSTEM_RULES` / `PROMPT_VERSION` / default vintage /
  output are **byte-identical** (resolver returns the exact current literals when `NPLP_LANE≠product`). The
  shared `gen_workflow.py` preamble text **does change for both lanes** (a refactor that defers framing to
  `system.txt`) — verify it is a behavioral no-op by re-running one service slice and diffing the result.
  This is the one place "byte-identical" does *not* strictly hold; it is a deliberate, tested refactor.

---

## 12. Parallel track (FYI — not this cycle's job)

The same 5,422 subaward-gap set runs through the **deliverable** classifier (`what_was_done`+`work_type`)
→ appended to `naics_psc_deliverable` (`source_vintage=subaward_head_5422`; a separate goods deliverable
vintage also exists). Both cycles key on `(naics, psc)` and compose downstream — a subawardee's inherited
combo resolves to *both* a deliverable and a labor profile. Independent; runs without waiting on this lane.

---

## 13. Carried-forward risks

- **`541712` build-to-print off_pattern will be high** (candidate ladder is biotech-skewed). Expected;
  pre-declared. The gold set (D2) confirms the head reaches aerospace/mechanical engineers + production.
- **Catch-all sector combos (~220, incl. the $4.22B sector-92 row)** rely on `off_pattern` + scope-text
  reading with weak NAICS candidates. The head names `927110×1677` explicitly; the gold set covers it.
- **`psc_reference` coverage gap** would null `psc_category` for uncovered goods PSCs — check on the frozen
  manifest (§9), not the CSV.
- **Concurrency temptation.** Hold `NPLP_CONCURRENCY=4`. The larger goods set is still only 7 waves.

---

## Appendix A — paste-ready operator directive (lift to `~/Desktop/hq/` at execution time)

> **Handoff — goods-lane combos → `naics_psc_labor_profile` (L2).** Paste as the first message to a fresh
> labor-profile agent. Self-contained.
>
> **Set:** worklist `~/Desktop/hq/subaward_gap_5422.csv`; `NPLP_LANE=product` (drives the goods head,
> `prompt_version=goods_profile_v1`, `source_vintage=subaward_gap_goods_2361`, and a separate goods
> manifest URI — keep it exported across manifest → prep → retrieve). Scratch `/tmp/nplp_goods`,
> checkpoint prefix `staging/nplp_insession_goods`, `NPLP_CONCURRENCY=4`.
>
> **Target:** 2,361 goods combos (339 NAICS, 386 calls, 26 slices, 7 waves @ C=4). Fail-closed gate =
> 2,361. Append-only; do not touch the govcon (8,690) or service (3,061) vintages. In-session Opus/xhigh
> only — zero Anthropic API.
>
> **Head:** the make-vs-pass-through classifier (`SYSTEM_RULES_GOODS`, §3 of
> `docs/plans/GOODS_LANE_LABOR_PROFILE_PLAN.md`). Decide `is_labor_play` FIRST (make vs resale/lease),
> then categories. Manufacturing/54/81/other-sector → make (off_pattern for mis-codes and
> candidate-domain mismatch); wholesale-42/retail-44-45/lease → pass-through (thin-true on value-add).
>
> **Pre-flight:** hand-classify the top 6 $ combos, confirm the head, then fan out. Review the top-5
> mis-code `off_pattern` outputs before trusting `retrieve`.
>
> **Parallel track (FYI, not your job):** the deliverable classifier runs the same set as
> `subaward_head_5422`; independent, composes on `(naics, psc)`.

---

*Provenance of this plan: designed via a multi-agent workflow (two prompt-head lenses — manufacturing
economist + federal acquisition — plus code-touchpoints and combo-shape deep-dives), synthesized, and
adversarially reviewed by three independent reviewers (correctness / mechanics / completeness; verdict
`ship-with-fixes`). All flagged fixes are folded in. Live facts verified against `subaward_gap_5422.csv`
and `materialize_naics_psc_labor_profile.py` @ `7c37463`.*
