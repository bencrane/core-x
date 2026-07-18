# NPPES Analytical Layer — Adversarial Review

**Artifact under review:** [`docs/analysis/nppes_analytical_implementation_plan.md`](nppes_analytical_implementation_plan.md)
**Evidence base:** [`docs/analysis/nppes_structural_diagnostic.md`](nppes_structural_diagnostic.md) · `pipelines/nppes/ingest.py` · convention check across `pipelines/*/materialize_*.py`
**Method:** read-only live-data verification against `s3://data-sink/active/nppes/snapshot=2026-05/` (9,551,447 rows, 334 cols, v4, Lance v2.1) via the pinned `/tmp/nppes_diag_venv` (`pylance 7.0.0`, `duckdb 1.5.3`); local Lance micro-datasets built to prove fragment-pruning + metadata round-trip mechanics. Every empirical claim shows the command and literal output. No mutation of the SoR; no git.
**Date:** 2026-06-06

---

## Headline verdict

**SHIP-WITH-FIXES.** The plan's analytical model is correct and, on the load-bearing decisions, *verified against the live data*: dates parse losslessly as `%m/%d/%Y` (zero failures across all five columns), the G5 date-range count reproduces to the row (3,292,670), primary-taxonomy switch semantics are exactly one `'Y'` per provider-with-taxonomy with no zero/multi anomalies, the switch-coalesce primary-code derivation is materially correct (1,106,232 providers would be mislabeled by a slot-1-only shortcut), row-count estimates are accurate (taxonomy 11,952,809; identifier 2,759,800; provider 9,551,447), cleaning macros behave correctly on real distributions, and BITMAP-index fragment pruning on the `(taxonomy_code, npi)` sort is **real and measured** (a single-specialty filter reads 1 fragment / 25.67 KB / 1 IOP). The plan is *not* ship-as-is because it carries two defects that will make a correct build either crash or silently lose data, plus a join-pruning claim that is overstated: (1) the worker hardcodes `/mnt/nvme/...` for the DuckDB temp dir and out-of-core database — a path that **does not exist** in the Modal runtime and is used by **zero** sibling pipelines; (2) the chosen Lance write path (streaming `to_arrow_reader`) **silently drops the Arrow schema-metadata provenance** the plan relies on (D9); (3) the G6/G7 "join uses the npi BTREE on both sides + prunes fragments" claim is false as stated — DuckDB resolves the join with a *min/max dynamic range filter + hash probe*, not an indexed take, and the latency gates G3/G6 are mis-calibrated for cold R2 (a correct build fails them on cold-start round-trips alone). None of these is a model defect; all are mechanical and fixable before build.

---

## Findings (severity-ranked)

### BLOCKER

#### B1 — `/mnt/nvme` temp/staging paths do not exist in the Modal runtime; zero pipelines use them
- **Claim (plan §3.1, §5):** `con = duckdb.connect('/mnt/nvme/nppes_build.duckdb')`; `SET temp_directory='/mnt/nvme/duck_spill';` and the §5 bullet "the staging `.duckdb` on **NVMe** (never the root FS)". The §8 narrative and diagnostic §4-C also reference `/mnt/nvme/duck_spill`.
- **What I found:** there is **no `/mnt/nvme` mount in the Modal execution environment**, and **no pipeline in the repo references it**. Modal exposes ephemeral disk at the container root (and the fleet convention is to spill under `/tmp`). The plan's own parent pipeline, `pipelines/nppes/ingest.py`, spills to `/tmp/nppes/duckdb_spill`. A `duckdb.connect('/mnt/nvme/...')` against a non-existent directory raises at connect time (or, worse, `SET temp_directory='/mnt/nvme/duck_spill'` silently no-ops/errors on first spill) — a hard build failure.
- **Evidence:**
  ```
  $ grep -rn "/mnt/nvme" pipelines/ | wc -l
  0
  ```
  Convention truth (the spill/scratch path every sibling actually uses — all under `/tmp`, on Modal ephemeral disk):
  ```
  $ grep -rn "SPILL_DIR\|SCRATCH_DIR\|temp_directory" pipelines/ingest_epa/materialize_epa.py pipelines/ingest_msha/materialize_msha.py pipelines/sam_gov/*.py
  pipelines/ingest_epa/materialize_epa.py:70:SPILL_DIR = "/tmp/duckdb_spill"
  pipelines/ingest_epa/materialize_epa.py:71:SCRATCH_DIR = "/tmp/epa"
  pipelines/ingest_msha/materialize_msha.py:117:SCRATCH_DIR = "/tmp/msha"
  pipelines/ingest_msha/materialize_msha.py:692:    con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
  pipelines/sam_gov/sam_master.py:49:SPILL_DIR = "/tmp/ddspill"
  pipelines/sam_gov/sam_entity_master.py:60:SPILL_DIR = "/tmp/duckdb_spill"
  ...
  ```
  And the parent NPPES pipeline itself (`pipelines/nppes/ingest.py:130`, `:687`, `:740`):
  ```python
  SCRATCH_DIR = "/tmp/nppes"
  os.makedirs(os.path.join(SCRATCH_DIR, "duckdb_spill"), exist_ok=True)
  con.execute("SET temp_directory='/tmp/nppes/duckdb_spill';")
  ```
- **Verified against live data?** No — environment/convention, verified by inspection of repo `grep` + file:line. The non-existence of `/mnt/nvme` in Modal is the documented runtime reality (Modal mounts ephemeral disk at the container root; the `ephemeral_disk=524288` setting on the function — `ingest.py:671` — provisions the 512 GiB floor *on that root volume*, not at `/mnt/nvme`).
- **Remediation (concrete):** Define `SCRATCH_DIR = "/tmp/nppes_analytical"` and `SPILL_DIR = os.path.join(SCRATCH_DIR, "duck_spill")` as module constants (mirror `ingest.py:130`). Replace every `/mnt/nvme/...` literal:
  - `duckdb.connect(os.path.join(SCRATCH_DIR, "nppes_build.duckdb"))`
  - `SET temp_directory='{SPILL_DIR}'`
  - local Lance stage under `os.path.join(SCRATCH_DIR, "<name>_lance")`.
  `os.makedirs(SPILL_DIR, exist_ok=True)` at function entry. Keep `ephemeral_disk=524288` on the `@app.function` (already the fleet floor; ≫ the ~12 GiB stage + ~35 GiB spill). The diagnostic's §4-C `/mnt/nvme` references are aspirational ("local NVMe") and must not be copied as literal paths.

---

### MAJOR

#### M1 — Arrow schema-metadata provenance (D9) silently does NOT round-trip through the chosen write path
- **Claim (plan D9 + §3.5):** "carry `source_snapshot_uri`/`source_member` as Arrow schema metadata" and "stream the sorted DuckDB table to a local Lance dataset via `to_arrow_reader(131072)`." Idempotency mandate item 9 asks directly whether this round-trips.
- **What I found:** with a **`RecordBatchReader`** source (exactly what `to_arrow_reader` returns), `lance.write_dataset(reader, ..., schema=sch_with_metadata)` **drops the schema-level KV metadata** — `ds.schema.metadata == {}` after write and after index. With a materialized **`pa.Table`** source, `replace_schema_metadata` *does* round-trip. So the plan's mandated bounded-RSS streaming path is precisely the one that loses provenance; provenance vanishes with no error.
- **Evidence:**
  ```
  # reader source (the plan's path) — metadata attached to reader.schema:
  after write, schema metadata: {}
  after index, schema metadata: {}
  PROVENANCE ROUND-TRIPS THROUGH WRITE+INDEX? NO
  # pa.Table source with replace_schema_metadata:
  pa.Table-source metadata after write: {b'source_snapshot_uri': b's3://x', b'pipeline': b'mat'}
  ```
  Explicit APIs exist on the dataset (pylance 7.0.0):
  ```
  Dataset methods mentioning meta/config: ['config','delete_config_keys','metadata',
   'replace_field_metadata','replace_schema_metadata','schema_metadata','tags',
   'update_config','update_field_metadata','update_metadata','update_schema_metadata']
  ```
- **Verified against live data?** Verified empirically on local Lance datasets with `pylance 7.0.0` (the pinned version). The R2 publish step is irrelevant to the defect — `boto3` copies whatever files exist verbatim; the metadata is already gone before publish because it never persisted to the manifest.
- **Remediation (concrete):** after the streaming `lance.write_dataset(...)` and **before** indexing/publish, set metadata explicitly on the committed dataset:
  ```python
  ds = lance.dataset(local_stage)
  ds.replace_schema_metadata({
      "source_snapshot_uri": RAW_URI,
      "source_member": source_member,
      "pipeline": "materialize_analytical",
      "snapshot_month": snapshot_month,
  })
  ```
  (or `update_config(...)` for dataset-level config keys). Verify in the §8 gate by reopening and asserting `ds.schema_metadata.get("source_snapshot_uri")` is non-empty per published dataset. Without this, the only provenance carrier is the `ops.nppes_analytical_runs` ledger row — acceptable as a fallback, but D9's on-dataset provenance claim is false as written.

#### M2 — G6/G7 join claim is overstated: the join does NOT use the `npi` BTREE on both sides; it range-prunes + hash-probes
- **Claim (plan §8 G6/G7, §0):** the `provider ⋈ taxonomy` join "pushes the `BITMAP(taxonomy_code)` filter AND uses the `npi` BTREE on both sides AND prunes fragments"; G7 "a 1,000-`npi` batch join touches < all fragments."
- **What I found:** Half-right, and the wrong half matters. The `taxonomy_code` BITMAP **does** push through the DuckDB `register(LanceDataset)` boundary (taxonomy scan returns only the matched rows). But DuckDB then builds a **min/max dynamic range filter** from the small side's npi values and pushes *that* (not a set-membership BTREE take) to the provider scan. The provider side therefore scans **every npi in the min..max range**, not the specific matched npi. In my representative local build (2,728 matched taxonomy rows → npi range `1000000000..1001999965`), the provider scan emitted **39,216 rows**, not a 2,728-row indexed take. The `npi` BTREE on the *provider* side is not exercised by the join; it accelerates only standalone point/`IN` lookups. Fragment pruning on the *taxonomy* side via the BITMAP is genuine (see What's Sound S5); the "BTREE on both sides" framing is the false part.
- **Evidence (DuckDB `EXPLAIN ANALYZE`, local representative datasets, join shape = G6):**
  ```
  HASH_JOIN  Conditions: npi = npi   -> 53 rows
   ├─ TABLE_SCAN tax   Filters: taxonomy_code='100000100X'
   │    Dynamic Filters: optional: npi>='1000000000' AND optional: npi<='1001999965'
   │    2,728 rows                       <- BITMAP pushed: only matched taxonomy rows
   └─ TABLE_SCAN prov  Filters: practice_state='TX'
        39,216 rows                      <- range-pruned scan, NOT a 2,728-npi BTREE take
  ```
  The taxonomy-side BITMAP + fragment-prune is real (separate measurement, `analyze_plan`): `fragments_scanned=1, rows_scanned=2.73K, bytes_read=25.67K, iops=1`.
- **Verified against live data?** The mechanism is verified on local Lance datasets with the pinned `duckdb 1.5.3` + `pylance 7.0.0` (the derived datasets do not yet exist on R2, so a representative local build is the correct probe; the diagnostic itself only proved *single-predicate* pushdown, never join-context). The *correctness* of the join result is unaffected — only the *cost model* the gate assumes.
- **Remediation (concrete):**
  1. Reword G6/G7 to the true mechanism: "the `taxonomy_code` BITMAP pushes into the taxonomy scan and prunes to the fragment(s) holding that code; the join then range-prunes the provider side via a dynamic npi min/max filter." Drop "uses the `npi` BTREE on both sides."
  2. For G7, assert what is actually true and measurable: a standalone `provider` scan with `npi IN (<1000 ids>)` pushed *into the Lance scanner* (`ds.scanner(filter="npi IN (...)")`) prunes fragments via the sort+zone-maps — test that path, not the hash-join path. If the intent is a genuinely fragment-pruned *batch join*, drive it as a Lance-scanner `IN`-filter take on the npi-sorted provider table, then join the small results in DuckDB — do not rely on the hash join to prune the large side.
  3. Keep the provider `ORDER BY npi` sort (it is what makes the standalone `IN` path prune); it just doesn't help the *hash-join* path.

#### M3 — Latency gates G3 (<800 ms) and G6 (<1.5 s) are mis-calibrated for cold R2; a correct build fails them
- **Claim (plan §8):** G3 "**< 800 ms**, index used"; G6 "**< 1.5 s**, both predicates indexed." Footnote concedes thresholds are "environment-relative" but they are nonetheless **build-failing assertions** ("fail the build if any assertion fails").
- **What I found:** cold R2 reads from a low-latency vantage already blow these thresholds on round-trips alone — dataset open is 552 ms before any query; a cold indexed BITMAP `count` is 862 ms (> G3's 800 ms); a cold 3-column projection with one indexed + one unindexed predicate is 3,011 ms (> G6's 1,500 ms). Warm, the same shapes are 4–25 ms (far under). So the gates are achievable *warm* and unachievable *cold*, and the plan specifies no warm-vs-cold protocol — meaning a perfectly correct build can fail the gate purely on cold-start latency (and a Modal→R2 path may be slower than this vantage).
- **Evidence (live raw dataset; raw has only `npi` BTREE + `state` BITMAP, the closest available proxy since the derived datasets don't exist yet):**
  ```
  dataset open: 552ms
  COLD count WHERE state=WY (BITMAP): 17,972 in 862ms        <- exceeds G3 (<800ms)
  WARM count WHERE state=WY: 17,972 in 7ms
  COLD count WHERE npi=X (BTREE) incl reopen: 1 in 1822ms
  COLD project 3 cols WHERE state=WY AND tax_1=X: 505 rows in 3011ms   <- exceeds G6 (<1.5s)
  --- warm ---
  G3 shape count WHERE state=CA (BITMAP): 24-483ms (median 25ms)
  point npi (BTREE): 4-5ms (median 4ms)
  ```
- **Verified against live data?** Yes — live R2 reads against the raw SoR.
- **Remediation (concrete):** make latency *measured-and-recorded* but not absolute-fail (the plan's own footnote already says correctness G1–G5/G8/G9 are the absolute gates). Specifically:
  1. Run one warm-up query per index before timing (open dataset + one count on each indexed column), then time the *warm* query; assert warm G3 < 250 ms, warm G6 < 600 ms (defensible against the 4–25 ms warm floor with headroom).
  2. Record the cold figure to `ops.nppes_analytical_runs` for trend visibility, but gate on warm.
  3. Alternatively, keep cold thresholds but raise them to the measured cold envelope + margin (G3 ≤ 1.5 s, G6 ≤ 4 s) and label them explicitly "cold, single-shot." Either is defensible; the current sub-second cold numbers are not.

---

### MINOR

#### m1 — `is_active` re-deactivation edge case is correct *for this snapshot* but the plan doesn't bound the assumption
- **Claim (plan D5, §2.1, §3.3):** `is_active = deactivation_date IS NULL OR (reactivation_date IS NOT NULL AND reactivation_date >= deactivation_date)`. The mandate asks specifically about a deactivated→reactivated→deactivated provider (NPPES keeps only the latest pair) being misclassified as active.
- **What I found:** in `snapshot=2026-05` the hazard does **not** materialize: all 18,264 rows with `react >= deact` carry populated `entity_type_code`, taxonomy, and address (i.e., they are genuinely reactivated, not re-deactivated stubs); `react < deact` = 0 rows. The branch census is internally consistent and matches the diagnostic's 343,321 stub cohort exactly. The logic is correct *today*. But correctness rests on the empirical fact that CMS, when it re-deactivates, currently clears the descriptive fields (producing a stub with `entity_type_code IS NULL`) — not on the boolean alone. If a future snapshot ever carries a re-deactivated provider whose latest `react >= deact` *and* descriptive fields are retained, this boolean would mark it active.
- **Evidence:**
  ```
  deact_null 9,189,862 | deact_notnull 361,585 | deact_no_react 343,321
  deact_and_react 18,264 | react_after_deact 18,264 | react_before_deact 0
  is_active_TRUE 9,208,126 | is_active=FALSE 343,321
  re-deactivation stub probe (react>=deact AND null descriptive): 0 / 0 / 0
  reconciliation: deact_no_react 343,321 == entity_type_code NULL 343,321 == diag stub cohort
  ```
- **Verified against live data?** Yes.
- **Remediation:** keep the boolean (it is correct on real data). Add one defensive clause that makes it robust to the edge case and self-documenting: treat a row as inactive if it is a deactivation stub regardless of dates — e.g. `is_active = (deactivation_date IS NULL) OR (reactivation_date IS NOT NULL AND reactivation_date >= deactivation_date AND entity_type_code IS NOT NULL)`. Add a gate assertion `count(*) WHERE NOT is_active == 343,321` (== `entity_type_code IS NULL`) so a future snapshot where the two diverge trips the build for human review.

#### m2 — `clean_state` allow-list omits the freely-associated-state USPS codes `FM`, `MH`, `PW`
- **Claim (plan §3.2, line 148):** allow-list = 50 states + DC + AS GU MP PR VI UM + AA AE AP (60 codes).
- **What I found:** the list is otherwise complete and G9 passes (59 distinct kept; 0.052% of rows nulled). The only *valid USPS* codes present in the data but absent from the allow-list are the Compact-of-Free-Association states — `FM` (Federated States of Micronesia, 13 rows), `MH` (Marshall Islands), `PW` (Palau) — which would be nulled. Every *other* "missing" 2-letter code is a foreign province/country (BC/ON/QC/MX/UK/JP/…) and is *correctly* nulled. Total impact of the FM/MH/PW omission is on the order of ~15–30 rows. Not data loss of any US state.
- **Evidence:**
  ```
  clean distinct states kept: 59  (G9 needs <=~60)  -> PASS
  rows nulled by clean_state: 4,802 of 9,208,124 nonnull (0.052%)
  clean [A-Z]{2} in data NOT in allow-list: ['AB','BC','ON','QC','MX','FM',...]  (mostly foreign)
    FM: 13 rows would be NULLED   (valid USPS — freely-associated state)
  ```
- **Verified against live data?** Yes.
- **Remediation:** add `'FM','MH','PW'` to the allow-list if those territories matter for GTM coverage (one-line edit); otherwise document the deliberate exclusion. Either is defensible given ~15–30 rows; flagging so it's a decision, not an accident.

#### m3 — Index-cardinality citations are slightly stale (HLL over-estimate); index *types* are correct
- **Claim (plan §4):** taxonomy_code "NDV ~1,104"; state "~57 clean."
- **What I found:** exact long-form `taxonomy_code` NDV is **873** (the diagnostic's ~1,104 was the HLL slot-1 estimate, which the diagnostic itself flagged as +14% biased high); clean practice-state distinct is **59**; `identifier_type_code` distinct is **2**. All three remain squarely in BITMAP territory, so the index *plan* is correct — only the cited numbers drift.
- **Evidence:**
  ```
  identifier_type_code distinct: 2
  taxonomy_code distinct (long form, exact): 873
  enumeration_year range: 2005..2026  -> int16 safe? YES
  ```
- **Verified against live data?** Yes.
- **Remediation:** update §4 rationale to "taxonomy_code NDV 873 (exact, long form)"; no code change.

---

### NIT

#### n1 — `practice_zip5` drops the +4 on 9-digit ZIPs; full value is retained in `practice_zip` (no loss), worth a one-line note
- **What I found:** 8,491,473 rows carry a 9-digit ZIP; `zip5` correctly takes the first 5 and `practice_zip` passthrough retains the full string, so there is no information loss. 1,252 foreign/non-numeric postals (`'IP27 9PN'`, `'M5G 1X8'`, `'NONE'`, `'AE'`) correctly become NULL `zip5`. Behavior is correct. (`practice_zip5` nonnull source 9,208,118; empty-extract 1,252.)
- **Verified against live data?** Yes.
- **Remediation:** none required; optionally note in the schema table that `practice_zip5` is the 5-digit prefix and `practice_zip` is the full as-stored value.

#### n2 — `provider_name` is NULL for the 343,321 deactivated stubs (expected, not a defect)
- **What I found:** stubs have `entity_type_code IS NULL` and both name fields NULL, so the `ELSE coalesce(org_name, last_name)` branch yields NULL `provider_name`. Only 24 *active* orgs have a NULL legal name; individuals always have last+first. This is the correct, expected outcome (stubs are flagged `is_active=false`), and `concat_ws` skips NULLs without error.
- **Verified against live data?** Yes.
- **Remediation:** none; the §8 gate could optionally assert `provider_name IS NULL ⊆ (entity_type_code IS NULL)` to lock the invariant.

#### n3 — Partial-failure atomicity across the three publishes is unspecified (reasoned by inspection)
- **What I found (by inspection, not live):** §6 says "re-running a month wipes + republishes that month's three prefixes" and §10 says a materialize failure "pages but never rolls back the raw SoR" — but there is no story for the *intra-run* state where publish 1 of 3 succeeds and publish 2 crashes. The three datasets live at independent R2 prefixes (`nppes_provider`, `nppes_provider_taxonomy`, `nppes_provider_identifier`); a mid-run crash leaves a published `nppes_provider` with no matching taxonomy/identifier for that month. Because each publish is itself idempotent (`_replace_r2_prefix` wipes-then-uploads) and the layer is a pure function of one raw month, a **re-run heals it** — but a *downstream consumer reading between the crash and the re-run* sees a torn cross-dataset state. The `ops` ledger is the only signal of partial completion.
- **Verified against live data?** No — design reasoning over §6/§10 + the `_replace_r2_prefix` pattern in `ingest.py:519`.
- **Remediation:** acceptable for v1 given idempotent re-run + ledger, but make the torn state detectable: (a) write the ledger row `status='partial'` with the list of published-vs-pending datasets on any mid-run failure (mirror `materialize_epa.py`'s `run_epa_ingest` partial/error status at `:892`); (b) have `verify` assert all three prefixes share the same `snapshot_month` and that `count(DISTINCT npi)` in taxonomy/identifier is ⊆ provider npi, so a torn publish fails verification rather than silently serving.

---

## What's sound (survives attack — credit where due)

- **S1 — Date typing (D3).** The single most-cited assumption holds exactly: **zero** `%m/%d/%Y` parse failures across **all five** date columns (`provider_enumeration_date`, `last_update_date`, `npi_deactivation_date`, `npi_reactivation_date`, `certification_date`), on 9.2M / 361k / 18k / 4.9M non-null values respectively. G8 (<0.0001) passes with room to spare. `try_strptime(...)::DATE` is the right tool and `date32` is correct. Evidence: per-column fail rate `0.00000000` for all five.
- **S2 — G5 reproduces to the row.** `count(*) WHERE enumeration_date >= 2020-01-01` = **3,292,670**, exactly the plan's claim. The temporal-axis fix delivers the correct answer the raw string layout silently returns as 0.
- **S3 — Primary-taxonomy semantics (D2, §3.3, G4) are correct AND non-trivially better than the diagnostic's Tier-1.** Every provider-with-taxonomy has **exactly one** `switch='Y'` (9,208,126); **zero** have none-with-taxonomy; **zero** have multiple; max per NPI = 1. So G4 (≤1 primary) holds absolutely. Crucially, the switch-coalesce primary-code is right where a slot-1 shortcut would be wrong: **1,106,232** providers (~12%) have their true primary in a slot whose code differs from `code_1`, and **1,468,355** have `switch_1='N'` with the real primary in a later slot — the coalesce resolves all of them; the literal `code_1` fallback **never** fires as a real tiebreak (0 rows lack a `'Y'` anywhere). This is the load-bearing decision of the cycle and it is verified correct.
- **S4 — Row-count + fragment estimates are accurate.** taxonomy long = **11,952,809** (plan ~12M), identifier long = **2,759,800** (plan ~2.7M), provider = **9,551,447** (exact). Fragment counts at `max_rows_per_file=1048576`: provider 10, taxonomy 12, identifier 3 — exactly what the plan implies.
- **S5 — Fragment pruning on the `(taxonomy_code, npi)` sort is real and measured.** A local representative taxonomy dataset sorted by `(taxonomy_code, npi)` produces *disjoint* per-fragment code zone maps (frag0 codes 000–384, frag1 384–768, frag2 768–1099); a `WHERE taxonomy_code=X` filter resolves through the BITMAP and reads **1 fragment, 25.67 KB, 1 IOP** (`fragments_scanned=1` in `analyze_plan`). This is the direct fix for the diagnostic's 6.65 s 15-column scan, and the sort decision is vindicated.
- **S6 — UNION-ALL unpivot is the right mechanic.** The 15-way / 50-way `UNION ALL` (each arm a NULL-filtered projection carrying its parallel switch/license/group) is correct and cheap (sub-second per representative pass); the plan correctly rejects blind `UNPIVOT`, which would orphan the parallel switch columns from their codes.
- **S7 — Cleaning macros behave correctly on real distributions.** `clean_state` G9 passes (59 distinct, 0.052% nulled, foreign-only tail); `zip5` correctly handles 9-digit ZIPs and nulls foreign postals; `provider_name` handles org/individual/stub branches without error; `enumeration_year` range 2005–2026 is `int16`-safe.
- **S8 — Column mapping is exact.** All **308** source columns referenced in §2/§3 resolve against the live raw schema; all seven drop-targets exist. No KeyError / typo risk.
- **S9 — The R2 multipart / `LANCE_BYPASS_SPILLING` / local-stage-then-boto3-publish transport (D8, §5) is the proven fleet pattern** (`ingest.py:184–197`, `:472–540`; `materialize_epa.py:84`), correctly carried forward. The only error is the *path* (B1), not the transport.

---

## Prioritized remediation sequence (apply before building)

1. **B1 (BLOCKER): purge `/mnt/nvme`.** Add `SCRATCH_DIR="/tmp/nppes_analytical"`, `SPILL_DIR=os.path.join(SCRATCH_DIR,"duck_spill")`; point the DuckDB `.duckdb`, `temp_directory`, and all local Lance stages there; `os.makedirs` at entry. (Mirrors `ingest.py:130`.) Without this the worker cannot run on Modal.
2. **M1 (MAJOR): add explicit `replace_schema_metadata` after each streaming write,** before index/publish; assert non-empty provenance in `verify`. Otherwise D9 provenance is silently lost.
3. **M3 (MAJOR): re-cast the latency gates as warm-measured-with-warmup** (assert warm G3 < 250 ms, warm G6 < 600 ms; record cold to the ledger), or relabel as cold with the measured envelope + margin. Keep correctness gates G1–G5/G8/G9 absolute.
4. **M2 (MAJOR): correct the G6/G7 wording and test the right path.** State the true mechanism (taxonomy BITMAP push + provider dynamic-range prune); for the batch-`npi` claim, exercise a Lance-scanner `npi IN (...)` prefilter on the npi-sorted provider table and assert fragment pruning *there*, not via the hash join.
5. **m1 (MINOR): harden `is_active`** with the `AND entity_type_code IS NOT NULL` stub clause and add the `count WHERE NOT is_active == 343,321` invariant gate.
6. **m2 (MINOR): decide on `FM`/`MH`/`PW`** in the allow-list (add or document).
7. **m3 / n2 (MINOR/NIT): emit `status='partial'` + cross-dataset `snapshot_month`/npi-subset assertions in `verify`** so a torn 3-publish fails verification.
8. **m3-cardinality (MINOR): refresh §4 NDV citations** (taxonomy_code 873, state 59, identifier_type_code 2).

---

### Appendix — verification provenance
- Toolchain: `/tmp/nppes_diag_venv` (`pylance 7.0.0`, `duckdb 1.5.3`, `pyarrow`, `boto3`), secrets via `doppler run --project core-x --config prd`.
- Live reads: `lance.dataset("s3://data-sink/active/nppes/snapshot=2026-05/", storage_options=...)` (read-only); DuckDB over `con.register("raw", ds)` for census/parse/G4/G5/G9; `ds.count_rows(filter=...)` / `ds.scanner(filter=...)` for cold/warm latency.
- Mechanism probes (fragment pruning, join pushdown, schema-metadata round-trip): local Lance micro-datasets built and indexed with the pinned pylance, inspected via `analyze_plan()` / `explain_plan()` and DuckDB `EXPLAIN ANALYZE`.
- No DDL, no writes, no mutation of the raw SoR. No git operations.
