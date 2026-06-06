# Adversarial Review — `MSHA_REMAINING_INGEST_PLAN.md`

Principal-engineer adversarial review of the 12-archive MSHA remaining-ingest plan.
Scope: find what is wrong, unverified, or risky. Every finding below is backed by a
`file:line`, a live R2 probe result, or arithmetic. Where the plan is correct it is
verified and stated concisely; the effort is spent on what is wrong or unproven.

---

## Verdict

**Executable-with-fixes — but the fixes are non-trivial and three of them are BLOCKERs that
will silently produce wrong/corrupt datasets, not loud failures.** The plan's inventory,
counting, arithmetic, resource posture, overwrite-idempotency defense, and worker
decomposition are sound and largely verified-true. But the plan's central design claim —
that `MinesProd{Q,Y}` "unite under `UNION ALL BY NAME` … mirrors the ContractorProd
precedent **exactly**" — is **false by live probe**: the two files share only 3 of 13/11
column names, so the union yields a 21-column half-NULL sparse table (the exact anti-pattern
the plan forbids for samples). Separately, **the per-dataset "anchor grain / native key"
column assertions are wrong for at least 4 of the 11 datasets** (`EVENT_NO`/`VIOLATION_NO`
asserted on archives that do not contain those columns; `DOCKET_NO`/`EVENT_NO`/sample-id
asserted as the grain where they are demonstrably 1:many), and because every one of these is
a single-file ingest, the plan's `lance_rows == spine_rows` integrity gate **passes
trivially and cannot catch any of it**. Finally, the `OrdersIssued` recipe the plan
prescribes (`skip=3`) is verified a **no-op** against the live file, and the real defect the
plan missed — a literal `\t` tab prefixing every value, plus column names containing
spaces/parens/`@` — is unhandled and breaks the keying + the verify harness's
bare-identifier assumption.

**Confidence: HIGH.** This is not code-read-only; every material claim was live-probed
against R2 (`landing/msha/` + `active/`) on 2026-06-06 by replicating the workers' exact
acquire path (boto3 GET → largest-member extract → CP1252→UTF-8 transcode → DuckDB
`read_csv` with the canonical recipe → `DESCRIBE` / `count(DISTINCT)`), including full-file
pulls of the two production files and Inspections, and partial leading-chunk inflation of
the three giants.

---

## Method & evidence base

**Read in full:** the plan; `pipelines/ingest_msha/materialize_msha.py` (base worker);
`pipelines/ingest_msha/materialize_msha_extensions.py` (union worker);
`MSHA_DATA_PROFILING_REPORT.md`; `MSHA_LEGAL_ENTITY_SCHEMA_DIAGNOSTIC.md`;
`MSHA_LANCE_STATE_DIAGNOSTIC.md`; `core/name_norm.py`; `ARCHITECTURE.md`;
`pipelines/ingest_msha/ops_msha_ingest_runs.sql`.

**Live probes** (read-only; `doppler run -p core-x -c prd -- uv run --python 3.12 …`;
scripts in `/tmp/probe{1..6}_*.py`):

| Probe | What it did | Headline result |
|---|---|---|
| 1 | boto3 list `landing/msha/` + `active/` | 20 `.zip` + `.keep`; **5** `msha_*` active datasets |
| 2 | acquire + `DESCRIBE` + key uniqueness on 9 archives | `EVENT_NO`/`VIOLATION_NO` **absent** from OrdersIssued/Conferences/ContestedViolations; DOCKET_NO 1:many |
| 3 | OrdersIssued `skip=3` vs no-skip; sample-id uniqueness | `skip=3` is a **no-op**; sample "id" is inconsistent across the 5 sample files |
| 4 | leading-chunk inflate of the 3 giants | real headers for CoalDust (30), Inspections (45), MinesProdQ (13) |
| 5 | ContractorProd Q/Y vs MinesProd Q/Y; simulate the union | ContractorProd shares **6** names; MinesProd shares **3** → 21-col 80/20 NULL split |
| 6 | Inspections EVENT_NO grain; OrdersIssued dequote | EVENT_NO **1:many**; Mine ID dequotes to `'\t3605466'`; non-`[A-Z0-9_]` columns |

All probes were strictly read-only (boto3 GET/list + DuckDB SELECT/DESCRIBE over downloaded
landing bytes). No `.lance` write, no DDL, no `ops.*` row, no index build.

---

## Claim-verification table

| # | Plan claim (with line) | Verdict | Evidence |
|--:|---|---|---|
| 1 | "20 archives, 8 ingested, 12 remaining" (L12-13) | **VERIFIED-TRUE** | Probe 1: 20 `.zip` in `landing/msha/`; 5 active datasets (`msha_mines`, `msha_corporate_history`, `msha_enforcement_ledger`, `msha_contractors`, `msha_accidents`) covering 8 source archives (Mines, AddressofRecord, ControllerOperatorHistory, Violations, AssessedViolations, ContractorProdQ, ContractorProdY, Accidents). 20−8=12. |
| 2 | Per-archive compressed sizes (table §0, L21-32) | **VERIFIED-TRUE** | Probe 1 sizes match to 0.1 MiB (e.g. Violations 114.06, CoalDust 105.39, Inspections 69.33, MinesProdQ 53.71, AssessedViolations 103.28, OrdersIssued 0.18). |
| 3 | Row counts (table §0) | **VERIFIED-TRUE** | Probe 2/4/5/6 exact: Inspections 1,147,232; ContestedViolations 448,158; CivilPenalty 479,439; Conferences 161,623; OrdersIssued 3,830; MinesProdQ 2,714,840; MinesProdY 657,546; CoalDust 2,985,614; PHS 310,908; Noise 274,645; Quartz 167,238; Area 8,368. Σ = 9,359,441 ≈ "9.36 M" (L36). |
| 4 | `mine_production` union = 3,372,386 = 2,714,840+657,546 (L60, L246) | **VERIFIED-TRUE** (the arithmetic) | Probe 5 union row count = exactly 3,372,386. **But see BLOCKER-1: the union *shape* is wrong even though the *count* is right.** |
| 5 | "5 → 16 active datasets"; "8/20 → 20/20" (L5) | **VERIFIED-TRUE** (arithmetic) | 5 existing + 11 new = 16; 8+12 = 20; 12 archives → 11 datasets (MinesProd Q+Y collapse). All consistent. |
| 6 | `msha_inspections` anchor grain = `EVENT_NO` (L55) | **FALSE (grain) / TRUE (column exists)** | Probe 6: `EVENT_NO` exists but is **1:many** — 1,147,231 distinct / 1,147,232 rows (one dup). Not a PK. |
| 7 | `msha_contested_violations` anchor = `VIOLATION_NO` (L56) | **FALSE** | Probe 2: ContestedViolations header has **no `VIOLATION_NO`**. Cols: `CITATION_NO`, `DOCKET_NO`, … `CITATION_NO` is the citation key. A BTREE on `VIOLATION_NO` will fail (column absent). |
| 8 | `msha_penalty_dockets` anchor = `DOCKET_NO` (L57) | **FALSE (grain)** | Probe 2: `DOCKET_NO` exists but is **1:many** — 94,014 distinct / 479,439 rows. Grain is per (ASSESS_CASE_NO, VIOLATION_NO, decision) row, not per docket. |
| 9 | `msha_conferences` anchor = `EVENT_NO` (L58) | **FALSE** | Probe 2: Conferences header has **no `EVENT_NO`**. Cols: `CONFERENCE_NO`, `ISSUANCE_NO`, … Grain is per `ISSUANCE_NO` within `CONFERENCE_NO` (1:many on CONFERENCE_NO). |
| 10 | `msha_orders_issued` anchor = `EVENT_NO` (L59) | **FALSE** | Probe 2: OrdersIssued header is human-label Excel export — `Coal or Metal`, `Mine ID`, `Violation No.`, … **no `EVENT_NO`**, no `[A-Z0-9_]` column at all. |
| 11 | OrdersIssued: real header line 4, read with `skip=3` (L139-144) | **PARTLY FALSE** | Probe 3: header IS on line 4, but `skip=3` and **no-skip produce byte-identical output** (DuckDB consumes `sep=|` as a dialect directive + sniffs the header). `skip=3` is a no-op; worse, it risks double-skipping if a future reader also honors `sep=`. The real, unmentioned defects: every value is tab-prefixed (`'\t3605466'`) and column names contain spaces/`()`/`@`/`.` (BLOCKER-3). |
| 12 | CoalDustSamples values "bare/unquoted numeric"; `quote=''` parses cleanly (L148-150) | **VERIFIED-TRUE** | Probe 4: numeric fields unquoted (`481.4`, `.337`, `2.494`); string fields quoted; 30 cols as profiled. `quote=''` reads it; the `trim(BOTH '"')` dequote is a harmless no-op on numerics. |
| 13 | "These are the only two that deviate" (L40-41) | **FALSE** | Inspections has 4 columns named `SUM(TOTAL_INSP_HOURS)` etc. (parentheses) — a third deviation class the plan does not flag (Probe 6). It does not break the read, but breaks the verify harness's "every MSHA column is `[A-Z0-9_]`" assumption (`materialize_msha.py:711-712`). |
| 14 | "unite … mirrors the ContractorProd precedent exactly" (L47-49) | **FALSE** | Probe 5: ContractorProd Q/Y share **6** column names (incl. the entity key + name); MinesProd Q/Y share **only 3** (`MINE_ID`, `CURR_MINE_NM`, `SUBUNIT_CD`). Not the same shape. (BLOCKER-1.) |
| 15 | "all 12 are far below the ~100 M-row Giants threshold" (L37-38) | **VERIFIED-TRUE** | `ARCHITECTURE.md:137-139` defines Giants as "~100M+ rows on a load-bearing column"; largest here is CoalDust 2.99 M and the union 3.37 M — all 1.5–2 orders of magnitude under. Direct-R2 path applies. |
| 16 | `mode="overwrite"` is the correct idempotency posture for full-snapshot republished source (§6 L200-210) | **VERIFIED-TRUE** | Matches `ARCHITECTURE.md:65-76` (Lance SoR, overwrite for non-incremental) and the shipped workers (`materialize_msha.py:442`, both use `mode="overwrite"`). Append would duplicate full history each run. Defense is sound. |
| 17 | Resource config 32 GiB / spill / `LANCE_BYPASS_SPILLING` adequate (§6 L214-222) | **VERIFIED-TRUE (reused, proven)** | Identical to the shipped workers (`materialize_msha.py:610-617,250`) which materialized the 3.08 M-row ledger + 1.43 GB Violations join cleanly (`MSHA_LANCE_STATE_DIAGNOSTIC.md:55-61`). Heaviest new write (CoalDust 2.99 M × 30, union 3.37 M × 21) is lighter than the shipped giant. |
| 18 | "Total ≈ 9.36 M rows" (L36) | **VERIFIED-TRUE** | Σ = 9,359,441 (Probe arithmetic). |
| 19 | DoD: "Update `MSHA_LANCE_STATE_DIAGNOSTIC.md` (5 → 16 datasets)" (L306) | **MISLEADING** | That doc currently documents **3** datasets (`MSHA_LANCE_STATE_DIAGNOSTIC.md:8-9,40-43`); it predates PR #144 and never recorded `msha_contractors`/`msha_accidents`. "5 → 16" silently skips the un-recorded jump from 3 → 5. (MINOR-2.) |
| 20 | Verify harness comment: "Every MSHA column is `[A-Z0-9_]`, safe as a bare identifier" (cloned from `materialize_msha.py:711-712`) | **FALSE for the new archives** | Probe 6: OrdersIssued has 13 columns with spaces/`()`/`@`/`.`; Inspections has 4 `SUM(...)` columns. Bare-identifier `count_rows(filter=…)` and any index ref on these will error. (BLOCKER-3 / MAJOR-2.) |

---

## Findings, severity-ranked

### BLOCKER-1 — The `MinesProd{Q,Y}` union is the sparse-NULL anti-pattern the plan forbids; "mirrors ContractorProd exactly" is false

**Defect.** The plan unites `MinesProdQuarterly` (13 cols) and `MinesProdYearly` (11 cols)
into one `msha_mine_production` via `UNION ALL BY NAME`, asserting it "mirrors the shipped
`ContractorProd{Q,Y} → msha_contractors` precedent **exactly**" (L47-49, L60). It does not.

**Evidence (Probe 5, live).**

```
ContractorProd shared-by-name (6): AVG_EMPLOYEE_CNT, COAL_METAL_IND, CONTRACTOR_ID,
                                   CONTRACTOR_NAME, SUBUNIT, SUBUNIT_CD
MinesProd     shared-by-name (3): CURR_MINE_NM, MINE_ID, SUBUNIT_CD

MinesProdQuarterly(13): MINE_ID, CURR_MINE_NM, STATE,      SUBUNIT_CD, SUBUNIT,
                        CAL_YR,  CAL_QTR, FISCAL_YR, FISCAL_QTR, AVG_EMPLOYEE_CNT,
                        HOURS_WORKED, COAL_PRODUCTION, COAL_METAL_IND
MinesProdYearly(11):    MINE_ID, CURR_MINE_NM, STATE_ABBR, SUBUNIT_CD, SUBUNIT_DESC,
                        CALENDAR_YR, ANNUAL_HRS, ANNUAL_COAL_PROD, AVG_ANNUAL_EMPL,
                        AVG_EMPLOYEE_HOURS, C_M_IND

UNION ALL BY NAME → 21 columns. NULL split (live):
  COAL_METAL_IND 80.5% / C_M_IND 19.5%      ← same fact, two columns
  STATE          80.5% / STATE_ABBR 19.5%
  CAL_YR         80.5% / CALENDAR_YR 19.5%
  AVG_EMPLOYEE_CNT 80.5% / AVG_ANNUAL_EMPL 19.5%
```

MSHA gave the *same semantic field different column names* between the two cadences. `BY
NAME` therefore aligns only `MINE_ID`, `CURR_MINE_NM`, `SUBUNIT_CD` and scatters everything
else into pairs of ~80%/~20%-filled columns. This is precisely the "sparse NULL-filled
mega-table … do not do it" the plan rejects for the samples (L50-51) — applied correctly to
samples, contradicted for MinesProd. The plan's own `INDEX_PLAN` proves the bug: `{"BTREE":
["MINE_ID"], "BITMAP": ["COAL_METAL_IND", "SUBUNIT_CD"]}` (L191) indexes `COAL_METAL_IND`,
which is NULL on every Yearly row (19.5% of the table), and never indexes the Yearly
equivalent `C_M_IND`.

**Why the integrity gate won't catch it.** `lance_rows == spine_rows` = 3,372,386 holds
exactly (Probe 5) — the union drops no rows. The gate validates row count, not schema
coherence, so a semantically-broken 21-col table passes "OK".

**Blast radius.** `msha_mine_production` becomes the single hardest dataset to query: every
downstream consumer must `COALESCE(COAL_METAL_IND, C_M_IND)`, `COALESCE(CAL_YR,
CALENDAR_YR)`, etc., and the BITMAP/BTREE indexes cover only the quarterly slice. This is the
"firmographic master" for mine production — the one most likely to be joined to the spine.

**Remediation (pick one; A preferred).**
- **A — column-harmonize before union (the real ContractorProd parity).** In each side's
  projection, alias the Yearly columns to the canonical (Quarterly) names so `BY NAME`
  collapses them, and carry a `period` discriminator instead of relying on `source_file`:
  ```sql
  -- Yearly projection: rename to the quarterly canon + add NULL period-cols
  SELECT ... AS MINE_ID, ... AS CURR_MINE_NM,
         STATE_ABBR        AS STATE,
         SUBUNIT_DESC      AS SUBUNIT,
         CALENDAR_YR       AS CAL_YR,
         NULL              AS CAL_QTR,        -- yearly has no quarter
         ANNUAL_HRS        AS HOURS_WORKED,
         ANNUAL_COAL_PROD  AS COAL_PRODUCTION,
         AVG_ANNUAL_EMPL   AS AVG_EMPLOYEE_CNT,
         C_M_IND           AS COAL_METAL_IND,
         AVG_EMPLOYEE_HOURS,                  -- yearly-only, keep verbatim
         'Y' AS PERIOD_GRAIN
  FROM MinesProdYearly
  UNION ALL BY NAME
  SELECT ..., 'Q' AS PERIOD_GRAIN FROM MinesProdQuarterly  -- (CAL_QTR populated)
  ```
  This is a **named-projection** union, not a blind `read_csv * UNION ALL BY NAME`, so it can
  no longer be "cloned verbatim" from the extensions worker — the executor must write
  explicit per-side column maps. **NOTE:** this aliasing violates the plan's "Columns stay
  verbatim UPPERCASE" guardrail (L68-69) for the Yearly side; that trade-off (verbatim
  fidelity vs. a usable single grain) must be decided in Phase 0, not discovered mid-build.
- **B — ship two separate datasets** `msha_mine_production_quarterly` /
  `_yearly` (12 archives → 12 datasets, "5 → 17"). Zero NULL scatter, every column native and
  fully populated, each independently indexable. Loses the "one production table" convenience
  but is the lower-risk, fidelity-preserving choice and is consistent with the plan's own
  argument for keeping the 5 sample sets separate.

Either way, the plan's row-3 §0/§7 claim that this is the "lowest risk, proven union shape
that should ship first" (L85-90, L241) is inverted: it is the **highest-schema-risk** dataset
and should not be cloned blindly.

---

### BLOCKER-2 — Per-dataset "anchor grain / native key" is wrong for ≥4 datasets; the integrity gate cannot detect it

**Defect.** §0 and §1 assert a native key per dataset (L21-32, L55-65). Live `DESCRIBE` shows
the asserted key column is **absent** from three archives and **1:many** on two more. Because
all of these are single-file `single`-kind ingests, `_materialize_one` sets `spine_rows =
count(*)` of the one file and writes every row — so `lance_rows == spine_rows` is true **by
construction regardless of the key**, and the "hard correctness check" (L126-129) verifies
nothing about grain.

**Evidence (Probes 2, 6).**

| Dataset | Plan's anchor (L55-65) | Live reality |
|---|---|---|
| `msha_contested_violations` | `VIOLATION_NO` | **column absent.** Header: `CITATION_NO`, `DOCKET_NO`, … |
| `msha_conferences` | `EVENT_NO` | **column absent.** Header: `CONFERENCE_NO`, `ISSUANCE_NO`, … |
| `msha_orders_issued` | `EVENT_NO` | **column absent.** Header: `Mine ID`, `Violation No.`, … |
| `msha_penalty_dockets` | `DOCKET_NO` | present but **1:many** (94,014 / 479,439) |
| `msha_inspections` | `EVENT_NO` | present but **1:many** (1,147,231 / 1,147,232) |

**Blast radius.** Two distinct failure modes:
1. **Index build will error** where the column is absent — `create_scalar_index("VIOLATION_NO")`
   / `("EVENT_NO")` on ContestedViolations / Conferences / OrdersIssued. This is caught by the
   best-effort `try/except` in `_create_indexes` (`materialize_msha.py:462`), so the dataset
   still commits — but **with zero usable resolution index**, recorded only as a logged miss.
   The plan's DoD "Every BTREE/BITMAP committed (or explicitly logged as skipped + why)"
   (L292) would be satisfied by a dataset that has *no working key index at all*.
2. **The "anchor grain" framing misleads the executor** into believing the integrity gate
   proves uniqueness. It does not. A dataset asserted as "1 row per DOCKET_NO" that is
   actually 5:1 will ship "grain_ok=true".

**Remediation.**
- Replace the asserted keys with the **live-probed** keys before locking `INDEX_PLAN`:
  - `msha_contested_violations`: BTREE `CITATION_NO`, `DOCKET_NO`, `MINE_ID` (Probe 2).
  - `msha_conferences`: BTREE `CONFERENCE_NO`, `ISSUANCE_NO` (no MINE_ID column exists —
    confirm in Phase 0).
  - `msha_penalty_dockets`: BTREE `DOCKET_NO`, `ASSESS_CASE_NO`, `VIOLATION_NO`, `MINE_ID`,
    `VIOLATOR_ID` (all present, Probe 2); document grain as per-(case,violation,decision) row.
  - `msha_inspections`: BTREE `EVENT_NO` (near-unique), `MINE_ID`, `CONTROLLER_ID`,
    `OPERATOR_ID` (all present, Probe 4); BITMAP `COAL_METAL_IND`.
- Stop calling the integrity gate a grain proof for single-file ingests. For datasets where a
  true uniqueness claim matters, add an explicit `count(*) == count(DISTINCT key)` assertion in
  Phase 0 (it is read-only and cheap) rather than relying on `lance_rows == spine_rows`.
- The plan must drop "sample id" as a generic anchor (L61-65): the sample-id column **differs
  per file** (Probe 3): `AreaSamples.SAMPLE_NO` unique; `PersonalHealthSamples.SAMPLE_NO`
  unique; `QuartzSamples` has **no** unique `CASSETTE_NO` (167,235/167,238) — `LABORATORY_NO`
  is the unique one; `NoiseSamples` has **no** single unique id (`SURVEY_NO`=15 distinct,
  `FORM_NO`=72,396 — grain is composite). `CoalDustSamples` candidate is `CASS_NUM` (verify in
  Phase 0). Each sample worker needs a per-file key map, not a blanket "sample id".

---

### BLOCKER-3 — `OrdersIssued` is mis-specified: the prescribed `skip=3` is a no-op, and the real defects (tab-prefixed values + non-identifier column names) are unhandled

**Defect.** The plan's only remediation for OrdersIssued is "read with `skip=3`" (L143). Live:

- `skip=3` and **no-skip produce byte-identical output** (Probe 3): DuckDB treats line 1
  `sep=|` as a dialect directive and its header sniffer lands on the real line-4 header
  on its own. So the prescribed fix changes nothing; and the plan's premise "The standard
  `header=true` read will ingest preamble as data" (L142) is **false** (verified).
- The actual, unmentioned corruption: every value is **tab-prefixed**. The canonical dequote
  `nullif(trim(BOTH '"' FROM col), '')` (`materialize_msha.py:363`) strips only `"`, so:
  ```
  raw "Mine ID" = '"\t3605466"'  →  dequoted = '\t3605466'   (len 8, leading TAB retained)
  ```
  Every `Mine ID`, `Violation No.`, `Controller ID @ Violations`, etc. carries a leading
  `\t`. BTREE point-lookups and any future join on these IDs silently miss.
- Column names are Excel report labels with spaces/parens/`@`/period: `Coal or Metal`,
  `Operator Name (Violations)`, `Controller ID @ Violations`, `Violation No.` (Probe 6) —
  none are `[A-Z0-9_]`. This breaks the verify harness's bare-identifier `count_rows(filter=
  f"{col} IS NOT NULL")` (`materialize_msha.py:711-714`) and violates the "verbatim
  UPPERCASE native key" guardrail (L67-69): there are no native UPPERCASE keys here.

**Blast radius.** Contained to `msha_orders_issued` (3,830 rows). The plan itself notes
(via the profiling report, `MSHA_DATA_PROFILING_REPORT.md:154-156`) that this file is
**redundant** — the authoritative order universe is already in the live `msha_enforcement_
ledger.CIT_ORD_SAFE='Order'` (89,319 rows, `MSHA_LANCE_STATE_DIAGNOSTIC.md:205`). So the
blast radius is a small, redundant dataset — but the plan ships it anyway and would ship it
corrupted.

**Remediation.**
- If kept: (1) drop `skip=3` (no-op); (2) add a control-char strip to the dequote for this
  file — `nullif(trim(BOTH '"' FROM regexp_replace(col, '[\t\r\n]', '', 'g')), '')` — or a
  dedicated `trim(both from replace(col,chr(9),''))`; (3) **rename the columns** to
  `[A-Z0-9_]` canon in the projection (e.g. `"Mine ID" → MINE_ID`, `"Violation No." →
  VIOLATION_NO`), which again is an explicit per-column map, not a verbatim clone, and is a
  documented exception to the verbatim guardrail; (4) set the key to `VIOLATION_NO` (post-
  rename) — there is no `EVENT_NO`.
- **Recommended: drop `msha_orders_issued` from scope.** It is a redundant pre-filtered
  107(a) slice of data already materialized and indexed in `msha_enforcement_ledger`. Shipping
  it adds a bespoke parser, a guardrail exception, and a corruption risk for zero net signal.
  Reduces scope to 11 archives → 10 datasets ("5 → 15") and removes the only ⚠️ deviation
  from the enforcement worker, simplifying Phase 2.

---

### MAJOR-1 — Phase 0 is where the real schema work lives, but the plan defers the load-bearing decisions to "clone the recipe"

**Defect.** The plan's posture is "read-the-recipe-then-clone … you do **not** invent a new
ingestion pattern" (L7-8) and the `single` path *is* cleanly clonable. But BLOCKER-1/2/3
show the three things that actually vary per archive — (a) the union column harmonization, (b)
the per-file true key, (c) the OrdersIssued column-rename + tab-strip — are **not** clonable
and require bespoke per-dataset projection code. Phase 0 (L237-239) lists "lock CASTS +
INDEX_PLAN" but not "lock the union column-map" or "resolve the true grain key per file" or
"handle non-identifier column names." The plan under-scopes Phase 0.

**Evidence.** The skeleton at L184-194 supplies `cast_key: "mineprod"` but **no
`CASTS["mineprod"]` dict**, and the real column names (`CALENDAR_YR` vs `CAL_YR`, `ANNUAL_HRS`
vs `HOURS_WORKED`, `C_M_IND` vs `COAL_METAL_IND`) mean the contractor worker's single shared
cast map (`materialize_msha_extensions.py:127-133`) **cannot** be reused symmetrically — that
worker worked only because ContractorProd Q/Y share the metric names; MinesProd do not.

**Blast radius.** If Phase 0 is treated as "DESCRIBE + pick casts" the executor will reach
Phase 1 with a blind `read_csv * UNION ALL BY NAME` and ship the BLOCKER-1 sparse table.

**Remediation.** Expand Phase 0 deliverables to explicitly include, per worker: (1) the
union side-by-side column map + chosen canonical names + `period` discriminator; (2) a
`count(*) == count(DISTINCT key)` probe result for each asserted key (read-only); (3) the
list of non-`[A-Z0-9_]` columns per file and the rename map; (4) the per-file sample-id key.
None of these can be deferred to "the recipe."

---

### MAJOR-2 — The cloned `verify_datasets` bare-identifier assumption will throw on the new archives

**Defect.** The plan says to copy `verify_datasets`/`_committed_index_names` verbatim
(L116-118). That function filters on bare column names with the inlined assumption "Every MSHA
column is `[A-Z0-9_]`, safe as a bare identifier" (`materialize_msha.py:711-712`). For the
existing 5 datasets that is true; for the new ones it is **false** — OrdersIssued (13 spaced/
parenthesized cols) and Inspections (`SUM(TOTAL_INSP_HOURS)` ×4) (Probe 6).

**Blast radius.** `verify` on `msha_inspections`/`msha_orders_issued` will populate
`{col}__non_null` with `"err: …"` for those columns (the inner `try/except` at
`materialize_msha.py:715` catches it), degrading the read-back proof to noise for exactly the
datasets that most need scrutiny. It will not crash, but the DoD's "non-null fill on every
indexed key" (L287) becomes unverifiable through the cloned harness.

**Remediation.** When cloning `verify_datasets`, quote identifiers for the filter using the
Lance-correct escaping. Lance's filter parser treats double-quoted tokens as **string
literals** (per the same comment), so the fix is backtick-quoting if supported, or restrict
the verify filters to the (renamed) `[A-Z0-9_]` keys only and skip raw non-identifier
columns. Simplest: ensure BLOCKER-3's column rename lands so `msha_orders_issued` has clean
identifiers, and exclude the 4 `SUM(...)` columns from `INDEX_PLAN`/verify (they are not keys).

---

### MAJOR-3 — `ops.msha_ingest_runs` shared across 3 workers + 2 shipped workers blurs per-run observability

**Defect.** All three new workers reuse `feed='msha'` and write to the single
`ops.msha_ingest_runs` table (L116-118, plan §6 L230). That is consistent with the two
shipped workers (`materialize_msha.py:103`, `materialize_msha_extensions.py:103`), so it is
*precedented* — but the precedent already has a known weakness the plan inherits and
amplifies: the ledger has **no worker/app discriminator column**. The DDL
(`ops_msha_ingest_runs.sql` → mirrored at `materialize_msha.py:215-236`) keys rows by `feed`,
`source_prefix`, and a `datasets` jsonb blob; with 5 workers all writing `feed='msha'`,
`source_prefix='landing/msha/'`, distinguishing "which worker / which run produced this row"
requires parsing the `datasets` jsonb. `show_ledger` (L92, L118) just lists recent rows by id.

**Blast radius.** Observability/idempotency-audit only — not correctness. But the plan's DoD
"`ops.msha_ingest_runs` carries a terminal `success` row per worker run" (L293) becomes hard
to assert mechanically: a `--only` run of one dataset writes a row whose `datasets` blob has
one key; a full-worker run writes a row with N keys; nothing records *which of the 5 workers*.
Retry-safety reasoning ("did enforcement's OrdersIssued attempt fail?") requires jsonb
spelunking.

**Remediation (low-effort, high-value).** Add a `worker text` (or `app text`) column to the
ledger DDL and set it per worker (`'msha-production'`, `'msha-enforcement'`,
`'msha-samples'`). The column is additive (`ALTER TABLE … ADD COLUMN IF NOT EXISTS`), back-
compatible with the 2 shipped workers, and makes per-worker run filtering a `WHERE worker=…`.
Decide this NOW so all three new workers ship with it rather than retrofitting.

---

### MINOR-1 — "3 workers" is justified, but the production worker owning a single dataset is thin

**Defect/assessment.** The 3-worker split (L79-90) is defensible: domain grouping per
`ARCHITECTURE.md:56-59`, and isolating the two ⚠️ parsers into different apps is real
blast-radius containment. **Verified-sound** with one caveat: `materialize_msha_production.py`
owns exactly **one** dataset (`msha_mine_production`). A whole Modal app + worker file for one
union is on the edge of sprawl. Given BLOCKER-1 (the union needs bespoke harmonization
anyway) and the option to split it into Q/Y, the production worker could plausibly fold into
the samples or a "firmographics" worker. Not a blocker — the isolation argument holds — but
"ship production first as the lowest-risk proven shape" (L241) is wrong (it is the
highest-schema-risk), so its first-mover justification evaporates.

**Remediation.** Keep 3 workers if Q/Y stay unified; if BLOCKER-1 is resolved via split-into-
two (option B), reconsider whether a dedicated production app earns its keep vs. a
`msha-firmographics-pipelines` app holding both production sets. Re-sequence so the genuinely
lowest-risk worker ships first — that is the **samples** worker minus CoalDust, or the
enforcement worker minus OrdersIssued, both of which are clean `single` clones.

---

### MINOR-2 — DoD "update MSHA_LANCE_STATE_DIAGNOSTIC.md (5 → 16)" is built on a stale doc

**Defect.** `MSHA_LANCE_STATE_DIAGNOSTIC.md` documents **3** datasets and was written before
PR #144 (`MSHA_LANCE_STATE_DIAGNOSTIC.md:8-9` lists only `msha_mines`,
`msha_corporate_history`, `msha_enforcement_ledger`; §1.1 table L40-43 has 3 rows; headline
L26 says "3 of 20 source archives materialized"). The plan's "5 → 16" (L306) assumes the doc
already reflects 5. It does not — it reflects 3.

**Blast radius.** Documentation accuracy. The executor updating "5 → 16" will either leave the
3→5 gap unrecorded or have to first reconcile the doc to 5.

**Remediation.** The PR set must first bring `MSHA_LANCE_STATE_DIAGNOSTIC.md` current to the
post-#144 state (add `msha_contractors`, `msha_accidents`) and *then* extend to 16 — or
explicitly re-baseline the doc to "16 datasets" wholesale with a note that 3→5 landed in #144.

---

### NIT-1 — OrdersIssued row count "~3,829" vs live 3,830

Plan L25 says `~3,829`; live is 3,830 (Probe 2/3). The "~" covers it and it traces to the
profiling report's pre-`sep=|`-handling estimate. Cosmetic.

### NIT-2 — `data_storage_version="2.1"` vs ARCHITECTURE's "LanceDB v2.0"

Plan L222 and both shipped workers pin `"2.1"` (`materialize_msha.py:121`); `ARCHITECTURE.md`
header and §4 say "LanceDB v2.0". The workers already diverge from the doc here and it is
fine (2.1 is the current default, as the worker comment notes), but the doc/worker version
string mismatch is a latent confusion. Cosmetic; flag for an ARCHITECTURE doc touch-up.

---

## Implementation recommendations (prioritized)

1. **Fix the MinesProd union (BLOCKER-1) before writing any worker code.** In Phase 0,
   produce the explicit Q/Y column-harmonization map (option A) or decide to split into two
   datasets (option B). Do not clone `_union_sql` blind. Whichever path: update `INDEX_PLAN`
   so the indexed categorical (`COAL_METAL_IND`) is the harmonized column, not the 80%-filled
   one. This also forces an explicit ruling on the verbatim-UPPERCASE guardrail vs. a usable
   single grain — make that ruling in writing.
2. **Re-derive every `INDEX_PLAN`/anchor key from live `DESCRIBE` (BLOCKER-2).** Replace
   `VIOLATION_NO`→`CITATION_NO` (contested), `EVENT_NO`→`CONFERENCE_NO`/`ISSUANCE_NO`
   (conferences), `EVENT_NO`→(post-rename `VIOLATION_NO`) (orders), and the per-file sample
   keys (`SAMPLE_NO` / `LABORATORY_NO` / composite / `CASS_NUM`). Add a read-only
   `count(*)==count(DISTINCT key)` check to Phase 0 for any key whose uniqueness is asserted.
3. **Resolve OrdersIssued (BLOCKER-3): prefer dropping it.** It is redundant with the live
   ledger. If kept, drop `skip=3`, add a tab/control-char strip, and rename the 13 columns to
   `[A-Z0-9_]`. Removing it shrinks the enforcement worker to a pure clean clone.
4. **Patch the cloned `verify_datasets` for non-identifier columns (MAJOR-2)** and exclude the
   4 Inspections `SUM(...)` columns from any index/verify path.
5. **Add a `worker`/`app` column to `ops.msha_ingest_runs` (MAJOR-3)** now, so all three new
   workers (and, idempotently, the two shipped ones) record provenance.
6. **Re-sequence the phases.** Ship the genuinely lowest-risk worker first: the **samples**
   worker excluding CoalDust, or the **enforcement** worker excluding OrdersIssued — both are
   clean `single` clones. Move MinesProd (now known-risky) to last. The plan's "production
   first, lowest risk" ordering is inverted.
7. **Phase 0 must emit, per worker: column map (union), true keys + uniqueness, rename maps,
   cast dicts.** Treat these as gating artifacts, not recipe details.
8. **Reconcile `MSHA_LANCE_STATE_DIAGNOSTIC.md` to the post-#144 baseline (MINOR-2)** before
   extending it to 16.

**Confirmed-sound (do not re-litigate):** the 12-count + inventory; all sizes and row
counts; the overwrite idempotency posture and its defense; the sub-Giants classification and
the 32 GiB/spill/`LANCE_BYPASS_SPILLING` resource config; the CP1252→UTF-8 + `quote=''`
read contract for the 10 standard archives; the CoalDust bare-numeric handling; the no-bridge
/ verbatim guardrail (`core/name_norm.py` correctly *not* invoked); per-dataset `try/except`
+ `--only` blast-radius containment for the `single` path.

---

## Open questions (with the exact probe that would resolve each)

1. **CoalDustSamples true grain key.** Likely `CASS_NUM`, but not uniqueness-probed (only the
   header chunk was inflated). Resolve in Phase 0 with a full-file
   `SELECT count(*), count(DISTINCT CASS_NUM) FROM read_csv(<coaldust>, <recipe>)` (the file
   is 105 MiB compressed / ~1 GiB inflated — pull whole only for this one check, or sample).
2. **Conferences MINE_ID presence.** Its header (Probe 2) has no `MINE_ID` — confirm whether
   any mine/entity key exists for indexing, or whether `CONFERENCE_NO`/`ISSUANCE_NO` are the
   only resolution keys. Probe: already have the full DESCRIBE; decision is a design call, not
   a further probe.
3. **CoalDust / Inspections / ContestedViolations interior-delimiter or quote-shift defects**
   (the ContractorProd A5304 class, `materialize_msha_extensions.py:41-48`). Not probed at
   full scale here. Resolve in Phase 0 by running the worker's `_spine_count` vs a
   `count(*) WHERE <last_col> IS NOT NULL`-style sentinel on the full file, or eyeball N
   sampled rows per the plan's own §4 guidance (L152-154) — a *shifted* parse passes the
   row-count gate, so a sentinel-column null-rate check is the only mechanical catch.
4. **`AreaSamples`/`NoiseSamples` MINE_ID·EVENT_NO·SUBUNIT composite grain** for any future
   join — the plan asserts "sample id · MINE_ID · EVENT_NO" (L28, L32) but those are 1:many
   individually (Probe 2/3); whether the *tuple* is unique was not probed. Resolve with
   `count(*) == count(DISTINCT (col_a, col_b, …))` in Phase 0 if a uniqueness guarantee is
   needed downstream.
