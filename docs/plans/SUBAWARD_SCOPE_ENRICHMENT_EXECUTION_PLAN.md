# Subaward Scope-Enrichment — EXECUTION RUNBOOK (build plan)

**Date:** 2026-06-15 · **Status:** EXECUTION PLAN — buildable runbook for a green-lit job · **Companion:** `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_LIFT_PLAN.md` (verified root cause, worklist, blast analysis — treated as established; this doc converts it to ordered, executable steps). · **Ground truth:** live R2 read-only probes 2026-06-15 over `core-x/prd` (`s3://data-sink/active/`), repo `file:line` re-verified against current HEAD.

> **Job (decided):** unstick the **3,969** subawardee-solicitation PDFs frozen at `skipped_out_of_scope` in the shared prime extraction ledger, extract/chunk them via a **default-OFF id-filter + gate-bypass + throwaway-route**, append the chunks to the shared sinks idempotently by `chunk_id`, mark/embed/extract over only the new ids, and rebuild `govcon_subawardee_capability_profiles` so its solicitation-scope leg lifts from **50.1% toward ~71.4%** (+1,274 subs). **No mutation of shared prime ledger state.**

---

## 0. Live-state deltas vs the LIFT_PLAN (re-probed 2026-06-15 — read before executing)

These are the only places live ground truth sharpens the LIFT_PLAN. Everything else in that doc holds.

| # | Sharpening | Consequence for the runbook |
|---|---|---|
| D1 | **`govcon_scope_vectors_90day`** has `embedding_idx` (vector), **1,348,983 rows**. **`govcon_unknown_90day`** has `embedding_idx`, **1,042,059 rows**. | Marking-pass blocker (R4) is LIVE for **scope + unknown**. The §B marking patch is mandatory before Phase F can touch those two sinks. |
| D2 | **`govcon_pricing_90day`** has **NO** indices (102,809 rows). | The marking pass already runs **unpatched** on the pricing sink. The patch must therefore be **per-sink conditional** (allow the subset-merge write-back only when the sink carries a vector index AND the call is the subset-column path), not a blanket removal of the guard. |
| D3 | `_sublift_*` namespace probed CLEAR (all five datasets `Not found`). | No collision; Phase C may create them fresh. |
| D4 | `phase1_route` current signature is `(*, so, run_id, max_files=0)` (extract:531). `phase15_expand` is `(*, so, run_id, max_files=0)` (extract:656). `_build_tasks` is `(so, lanes, run_id)` (extract:1408). | The id-filter param is threaded into **three** functions + the CLI, exactly as §B specifies. |
| D5 | The `routed` CTE wraps `canon` in a derived subquery `(SELECT *, lower(...) AS file_name_l FROM canon)` (extract:601). | The id predicate goes in the **`canon` WHERE** (extract:590), not the outer SELECT. |
| D6 | `sam_labor_demand_extract_90day.py` supports `--resource-ids` (comma-split, CLI:2107/2151) but **NOT `--resource-ids-file`**. | Phase H regex/LLM lanes need a file-flag add **or** an `xargs` feed. The runbook adds `--resource-ids-file` to that module too (small, parallel to the extract change) — see §B.4. |
| D7 | `_ALL_EXTRACT_LANES = ["L1_scope","L4_structured","L3_triage"]` (extract:1933); `container` is handled by the **separate** `--phase expand`, not by `--phase extract --lane all`. | Phase C runs `route` then `expand`; Phase D runs `extract --lane all`. The 86 zip containers expand in Phase C, their inner files route into the lanes, and Phase D extracts them. |
| D8 | The extract CLI has **no `--resource-ids*` flag today**; `--ckpt` defaults to `CKPT_PATH` (extract:1946). The LIFT_PLAN's `--ckpt /tmp/sublift_extract_ckpt.jsonl` is a *new* explicit value, valid once passed. | Add both id-flags to the extract CLI (§B.3). |

---

## 1. Top-line execution sequence (ordered; cost-class; PR boundary)

| Ph | Name | Engine / cost-class | Wall-clock class | Shared-state write? | PR boundary |
|---|---|---|---|---|---|
| **B** | Code: id-filter + gate-bypass (extract) + marking-pass index-safe relax + labor `--resource-ids-file` + unit tests | code only | — | **none** | **PR #1 — code, merged before ANY data run** |
| A | Derive the 3,969 allow-list → `$IDS` | local CPU | seconds | none (read-only) | — (artifact, no PR) |
| C | Throwaway route + expand → `_sublift_*` | local CPU | minutes | none (throwaway only) | — (data run) |
| D | Throwaway extract + finalize → `_sublift_*` | **local CPU (no cap)** | **multi-hour (dominant)** | none (throwaway only) | — (data run) |
| E | Append `_sublift_*` chunks → SHARED sinks (`merge_insert chunk_id`) | local CPU | minutes | **YES — first shared write** | **PR #2 — append script `subaward_scope_append.py`** (code reviewed, then run) |
| F | CUI marking full-body pass scoped effect (whole-sink scan, promotion-only) | local CPU | **multi-hour** | YES (`content_marking` subset-merge) | — (uses PR #1 patch) |
| G | Embed-refresh + reindex touched sinks | **self-hosted GPU/MPS** | **multi-hour (~387K new vec; pin cuda)** | YES (`embedding` + indices) | — (existing module) |
| H1 | Regex lane scoped to `$IDS` | local CPU | minutes–hour | YES (requirements/labor sinks) | — (existing module + PR #1 file-flag) |
| H2 | **LLM grind scoped (measure at census)** | **session-agent (account-burning)** | **1–2 grind sessions, resumable** | YES (requirements sink) | — (existing module + grind harness) |
| I | Rebuild sub capability profile (overwrite) | local CPU | minutes | YES (profile overwrite) | — (existing module) |
| J | Throwaway cleanup (`_sublift_*` delete) | terminal op | seconds | none | — |

**PR count: four (two planned + two discovered-in-execution).** PR #1 (gate-bypass id-filter + marking relax + file-flag + tests, merged `5ab7366` / #478) lands before any data. PR #2 (append script, merged `29c502e` / #480) is the shared write. **PR #1b** (merged `a9a4435` / #479) was forced by a runbook gap: no `--inner-uri` isolation (expand wrote the SHARED inner worklist) + a latent reserved-word `JOIN inner` parser error — both reverted/fixed before any extract chunk landed. **PR #3** (F–J pre-execution hardening) adds the mechanical H2 CUI gate (`_assert_marking_complete`) + the parent-aware structural scoping guard + tests, and corrects the §9/§11 CLIs (positional verb, not `--phase`), the F→G/F→H2 orderings, the §9 embed cost, and the §12/§13 framing. Every other phase is a *run* of already-merged code.

**Critical ordering invariants:** B → A → C → D → E → **F (marking)** → G → H1 → **[F reconcile PASS gate, mechanical]** → H2 → I → J. Two hard orderings: **(1) F MUST precede H2** — the CUI egress gate (an in-session grind agent reading a marked chunk = egress), now enforced in code via `_assert_marking_complete` (PR #3), not convention. **(2) F MUST precede G** — `embed` buckets on LIVE `content_marking`; a chunk embedded before F marks it lands an indexed vector for a marked doc and corrupts the `null_unmarked==0` accounting. Gate G on F `reconcile_overall == PASS` (operational check). Reindex follows the last sink write within G.

---

## 2. Constants / environment (paste block)

```bash
# Dependency invocation (matches extract module docstring lines 36-41). soffice must be on PATH.
RUN='doppler run --project core-x --config prd -- uv run \
  --with pylance --with pyarrow --with duckdb --with boto3 --with "psycopg[binary]" \
  --with pypdfium2 --with python-docx --with openpyxl --with xlrd --with pdfplumber \
  --with striprtf --with charset-normalizer python'
# Probe-only invocation (read-only R2; NO uv deps needed):
PYR='doppler run --project core-x --config prd -- /Users/benjamincrane/core-x/.venv/bin/python'

# Throwaway namespace (all under active/_sublift_*, deletable after Phase E append):
EXT_TW=s3://data-sink/active/_sublift_extract_ledger/
SCOPE_TW=s3://data-sink/active/_sublift_scope/
PRICE_TW=s3://data-sink/active/_sublift_pricing/
UNK_TW=s3://data-sink/active/_sublift_unknown/
DEDUP_TW=s3://data-sink/active/_sublift_dedup/

# Shared sinks (the append targets — the ONLY shared write):
SCOPE=s3://data-sink/active/govcon_scope_vectors_90day
UNK=s3://data-sink/active/govcon_unknown_90day
PRICE=s3://data-sink/active/govcon_pricing_90day

# Worklist + checkpoints (survive interruption):
IDS=/tmp/sublift_ids.txt                       # the 3,969 newline-delimited resource_ids
EXTRACT_CKPT=/tmp/sublift_extract_ckpt.jsonl   # Phase D JSONL resume
APPEND_CKPT=/tmp/sublift_append_ckpt.json      # Phase E per-sink high-water
```

> **R2 creds for probes** (`R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`/`R2_ENDPOINT`) come from Doppler. **Run every probe script from inside the repo working dir** — a `/tmp`-resident script puts `/tmp` on `sys.path[0]` and a stray `/tmp/inspect.py` shadows stdlib `inspect`, breaking `import numpy/pyarrow` (LIFT_PLAN §9). Pass scripts via heredoc (`python - <<'PY'`) from the repo root.

---

## 3. PHASE B — Code change (PR #1; lands before any data run)

**This is the highest-risk step and is fully decoupled from data.** Three edits to `sam_attachment_extract_90day.py`, two `_assert_no_vector_index` relaxations in `sam_marking_fullbody_90day.py`, one CLI flag add in `sam_labor_demand_extract_90day.py`, plus a pure-function unit test. **Default-OFF: with no `--resource-ids*` flag, every byte of existing behavior is unchanged.**

### B.1 — Extract: extract a testable SQL-fragment builder + subset assertion (new pure helpers)

To keep the test pure-function (no R2 — mirroring `tests/test_sam_attachment_finalize_dedup.py`), factor the id logic into two module-level helpers near `_read_resolution` (after extract:514):

```python
def _id_filter_sql(only_resource_ids: set[str] | None, *, col: str) -> str:
    """SQL AND-fragment restricting `col` to an explicit id allow-list. None → '' (default OFF).
    Empty set → HARD RAISE (an empty filter would fall through to the full corpus — anti-pattern)."""
    if only_resource_ids is None:
        return ""
    if not only_resource_ids:
        raise RuntimeError("id-filter resolved to an EMPTY set; refusing to run — an empty filter "
                           "would select the full corpus.")
    ids = ",".join("'" + i.replace("'", "''") + "'" for i in sorted(only_resource_ids))
    return f"AND {col} IN ({ids})"

def _assert_routed_subset(routed_ids, only_resource_ids: set[str] | None) -> None:
    """GUARD #2: after routing, every routed resource_id MUST be in the allow-list. Raises on any
    leak — makes 'accidentally route the 11,067-prime backlog' structurally impossible."""
    if only_resource_ids is None:
        return
    leak = set(routed_ids) - only_resource_ids
    if leak:
        raise RuntimeError(f"ROUTE LEAK: {len(leak)} routed ids outside the allow-list "
                           f"(e.g. {sorted(leak)[:5]}). Aborting before any extract.")
```

### B.2 — Extract: thread the param through `phase1_route` / `phase15_expand` (gate forced OFF when set)

**`phase1_route`** — signature (extract:531) → `def phase1_route(*, so, run_id, max_files=0, only_resource_ids=None)`. Then:

1. **Gate-OFF coupling** — replace extract:575 `scope = None if max_files else _read_scope_gate(so)` with:
   ```python
   # GATE BYPASS: an explicit id set IS the scope decision — the GTM gate must NOT re-skip them.
   scope = None if (max_files or only_resource_ids is not None) else _read_scope_gate(so)
   ```
   This is the core of the fix: under a throwaway ledger the gate would otherwise re-derive `skipped_out_of_scope` for all 3,969 (LIFT_PLAN §2.3, all 3,969 are `out_of_scope` in the gate).
2. **`canon` WHERE** (extract:590) — append the filter to the existing predicate:
   ```python
   idf = _id_filter_sql(only_resource_ids, col="f.resource_id")
   # ... WHERE f.status = 'downloaded' AND d.resource_id = d.canonical_resource_id {seen} {scope_keep} {idf}
   ```
3. **`noncanon` select** (extract:605-609) — append `_id_filter_sql(only_resource_ids, col="d.resource_id")` to its WHERE so no out-of-set `dropped_duplicate` event is emitted into the throwaway ledger.
4. **`oos` select** (extract:616-623) — gate is forced OFF so `scope is None` and this block does not run; no change needed, but with the filter the block is dead anyway.
5. **dedup map** (extract:547-559) — the dedup pre-pass scans ALL downloaded files and writes `DEDUP_URI`. With `DEDUP_URI` pointed at `$DEDUP_TW` this is harmless (throwaway), **but** to keep the throwaway dedup map scoped, add `{_id_filter_sql(only_resource_ids, col="f.resource_id")}` to the `dedup` CTE WHERE (extract:551). (Belt-and-suspenders: even unscoped it only writes the throwaway dataset.)
6. **GUARD #2** — immediately after `routed = con.execute(...).fetchall()` (extract:602), add:
   ```python
   _assert_routed_subset((r[0] for r in routed), only_resource_ids)
   ```

**`phase15_expand`** — signature (extract:656) → add `only_resource_ids=None`; thread the same `_id_filter_sql(..., col="...resource_id")` into its container-selection scan so only the 86 in-set zips expand. (Inner files inherit parent lineage; they are NOT in `$IDS`, so do **not** re-apply the subset assertion to expanded inner ids — the assertion is parent-route-only.)

### B.3 — Extract: `_build_tasks` + CLI

**`_build_tasks`** — signature (extract:1408) → `def _build_tasks(so, lanes, run_id, only_resource_ids=None)`. In the `cand` CTE (extract:1420-1423):
```python
extra = _id_filter_sql(only_resource_ids, col="resource_id")
cand = con.execute(f"""
    SELECT resource_id, parent_resource_id, lane FROM res
    WHERE state IN ('routed','extract_failed') AND lane IN ({lane_pred}) {extra}
""").to_arrow_table()
```
With a throwaway ledger this is belt-and-suspenders (`res` only holds routed targets) but makes the filter authoritative regardless of ledger. **Caveat:** inner files from expanded zips have synthetic ids `<rid>::<inner>` not present in `$IDS` — so for the extract phase, **pass `only_resource_ids=None` to `_build_tasks`** (the throwaway `res` is already scoped to the targets + their own inner files) OR widen the allow-list with the expanded inner ids read from `$INNER` (`_sublift` inner worklist). The runbook chooses **`None` at extract** (throwaway ledger is the scope) and keeps the hard filter only at `route`/`expand` where the leak risk against the shared files-SoR is real.

**CLI** — after extract:1952 (the URI overrides block) add:
```python
p.add_argument("--resource-ids", default=None, help="comma-separated ids; route/extract ONLY these (gate forced OFF)")
p.add_argument("--resource-ids-file", default=None, help="newline-delimited id file (preferred for 3,969 ids)")
```
Resolve once after `a = p.parse_args()`:
```python
only_ids = None
if a.resource_ids_file:
    only_ids = {l.strip() for l in open(a.resource_ids_file) if l.strip()}
elif a.resource_ids:
    only_ids = {s.strip() for s in a.resource_ids.split(",") if s.strip()}
```
Thread: `phase1_route(..., only_resource_ids=only_ids)` (extract:1979), `phase15_expand(..., only_resource_ids=only_ids)` (extract:1981), and `phase2_extract(..., only_resource_ids=only_ids)` (extract:1986) → which passes it to `_build_tasks` (extract:1501) per the §B.3 caveat (extract phase uses `None`; route/expand use `only_ids`). Add `only_resource_ids=None` to the `phase2_extract` signature (extract:1481) and pass through.

### B.4 — Labor demand: add `--resource-ids-file` (D6 gap)

`sam_labor_demand_extract_90day.py` has `--resource-ids` (CLI:2107) comma-split (CLI:2151) but no file flag. Add beside it:
```python
p.add_argument("--resource-ids-file", default=None, help="newline-delimited id file (preferred for 3k+ ids)")
```
and in the resolve block (CLI:2151):
```python
if args.resource_ids_file:
    args.resource_ids = [l.strip() for l in open(args.resource_ids_file) if l.strip()]
else:
    args.resource_ids = ([s.strip() for s in args.resource_ids.split(",") if s.strip()]
                         if args.resource_ids else None)
```
(Alternative without the code change: feed via `xargs`/env — but a 3,350-id comma argv approaches shell limits and is fragile. The file flag is the clean path and parallels the extract change in the same PR.)

### B.5 — Marking pass: per-sink index-safe relax (the R4 / D1 / D2 blocker)

`sam_marking_fullbody_90day.py` calls `_assert_no_vector_index(ds, uri, action=...)` at **marking:288** (read phase) and **marking:314** (write phase). Both RAISE on `govcon_scope_vectors_90day` / `govcon_unknown_90day` (D1 — both carry `embedding_idx`). The write-back is ALREADY the index-safe subset pattern (`merge_insert("chunk_id").when_matched_update_all()` with a `(chunk_id, content_marking)`-only source, marking:316-321) — identical to the embed module's proven path (`sam_attachment_embed_90day.py:115`). The guard is over-broad: it was written for the overwrite/compaction path, not this subset write-back.

**Patch** — introduce a guarded helper and swap the two call sites:
```python
def _assert_marking_writeback_safe(ds, uri: str) -> None:
    """The full-body marking write-back is a SUBSET-column merge_insert('chunk_id')
    .when_matched_update_all() — it rewrites ONLY (chunk_id, content_marking), never `text`/
    `embedding`, and lance 7.0.0 leaves the vector index covering all unmatched rows (the same
    index-safe path as sam_attachment_embed_90day._flush:115). This path is therefore SAFE on a
    vector-indexed sink; the broad _assert_no_vector_index guard is for overwrite/compaction only.
    No-op here — kept as the single documented deviation point."""
    return
```
Replace `_assert_no_vector_index(ds, uri, action="full-body marking merge_insert write-back")` at **marking:288** and **marking:314** with `_assert_marking_writeback_safe(ds, uri)`. **Do NOT** touch any other `_assert_no_vector_index` call (the extract module's finalize/compaction guards stay intact — anti-pattern #2 still holds for overwrite). Document the deviation inline with the docstring above. Pricing (D2, no index) is unaffected either way.

### B.6 — Unit test (pure-function; mirrors `test_sam_attachment_finalize_dedup.py`)

New file `pipelines/sam_gov/tests/test_sam_attachment_id_filter.py`:
```python
import pytest
from pipelines.sam_gov.sam_attachment_extract_90day import _id_filter_sql, _assert_routed_subset

def test_none_is_default_off():           assert _id_filter_sql(None, col="f.resource_id") == ""
def test_empty_set_raises():
    with pytest.raises(RuntimeError): _id_filter_sql(set(), col="f.resource_id")
def test_builds_quoted_in_clause_sorted():
    assert _id_filter_sql({"b","a"}, col="x") == "AND x IN ('a','b')"
def test_sql_injection_escaped():
    assert "''" in _id_filter_sql({"a'b"}, col="x")
def test_subset_assertion_passes_on_subset():  _assert_routed_subset(["a","b"], {"a","b","c"})
def test_subset_assertion_raises_on_leak():
    with pytest.raises(RuntimeError): _assert_routed_subset(["a","z"], {"a","b"})
def test_subset_assertion_noop_when_off():      _assert_routed_subset(["anything"], None)
```

### Phase B verification gate (DoD)
1. `python -m pytest pipelines/sam_gov/tests/test_sam_attachment_id_filter.py -q` → all green.
2. **Default-OFF proof (smoke, throwaway sinks, NO id flag):** `--phase route --max-files 5 --extraction-uri $EXT_TW --scope-uri $SCOPE_TW --pricing-uri $PRICE_TW --unknown-uri $UNK_TW --dedup-uri $DEDUP_TW` → prints `phase1: GTM gate ON` is **suppressed only by `--max-files`** (existing behavior), routed ≤5 events. (This proves the no-flag path is byte-unchanged.) Then **delete `$EXT_TW`/`$DEDUP_TW`** before Phase C so the real route starts clean.
3. **Filter-ON proof:** `--phase route --resource-ids <2 known ids from $IDS> --extraction-uri $EXT_TW … (throwaway)` → emits exactly ≤2 `routed` events, prints **no** `phase1: GTM gate ON` line (gate forced OFF), and the subset assertion passes. Delete the throwaway ledger after.
4. **Marking patch:** import-time smoke — `python -c "import pipelines.sam_gov.sam_marking_fullbody_90day"` succeeds; grep confirms exactly two call sites swapped and every other `_assert_no_vector_index` intact.

**Blast radius:** code only, zero data. **Guardrail:** test #2 is the regression proof that no existing run (prime extraction) is affected.
**PR boundary:** **PR #1.** Commit, push, open against `main`, self-verify (tests green + the two route smokes), merge `--squash --delete-branch`, then `git pull` in the operator checkout. **No data run happens until PR #1 is merged and pulled.**

---

## 4. PHASE A — Materialize the 3,969 allow-list → `$IDS`

**Action (read-only probe, run from repo root):**
```bash
cd /Users/benjamincrane/core-x && $PYR - <<'PY' > /tmp/sublift_ids.txt
import lance, duckdb, os
so={"aws_access_key_id":os.environ["R2_ACCESS_KEY_ID"],"aws_secret_access_key":os.environ["R2_SECRET_ACCESS_KEY"],
    "aws_endpoint":os.environ["R2_ENDPOINT"],"aws_region":"auto","virtual_hosted_style_request":"false"}
B="s3://data-sink/active/"
con=duckdb.connect()
# manifest sub-solicitation resources
man=lance.dataset(B+"subawardee_solicitations_manifest",storage_options=so).to_table(columns=["resource_id","notice_id"])
con.register("man",man)
files=lance.dataset(B+"sam_attachment_files_90day",storage_options=so).to_table(columns=["resource_id","status"])
con.register("files",files)
# resolution view (terminal-first / attempt-desc / completed-desc, excluding marking_fullbody)
led=lance.dataset(B+"sam_attachment_extraction_90day",storage_options=so).to_table(
    columns=["resource_id","lane","state","attempt","completed_at"])
con.register("led",led)
INTER="'routed','extracted_spreadsheet','requires_ocr'"
res=con.execute(f"""
  SELECT resource_id,state,lane FROM (
    SELECT resource_id,state,lane,
      row_number() OVER (PARTITION BY resource_id
        ORDER BY (state NOT IN ({INTER})) DESC, attempt DESC, completed_at DESC) rn
    FROM led WHERE state NOT IN ('marking_fullbody')) WHERE rn=1
""").arrow()
con.register("res",res)
# chunk sinks (membership only — project resource_id)
for s,a in [("govcon_scope_vectors_90day","sc"),("govcon_unknown_90day","un"),("govcon_pricing_90day","pr")]:
    con.register(a, lance.dataset(B+s,storage_options=so).to_table(columns=["resource_id"]))
ids=con.execute("""
  SELECT DISTINCT m.resource_id FROM man m
  JOIN files f ON m.resource_id=f.resource_id AND f.status='downloaded'
  JOIN res r ON m.resource_id=r.resource_id AND r.state='skipped_out_of_scope' AND r.lane='out_of_scope'
  WHERE m.resource_id NOT IN (SELECT resource_id FROM sc)
    AND m.resource_id NOT IN (SELECT resource_id FROM un)
    AND m.resource_id NOT IN (SELECT resource_id FROM pr)
  ORDER BY m.resource_id
""").fetchall()
import sys
for (r,) in ids: sys.stdout.write(r+"\n")
sys.stderr.write(f"derived {len(ids)} ids\n")
PY
wc -l /tmp/sublift_ids.txt```

**Inputs/outputs:** in = `subawardee_solicitations_manifest` × `sam_attachment_files_90day` (`status='downloaded'`) × resolution-view (`state='skipped_out_of_scope'`, `lane='out_of_scope'`) minus all three chunk sinks. out = `$IDS` (newline-delimited, sorted, deduped).
**Cost class:** local CPU, seconds. **Idempotency:** deterministic query; re-run overwrites `$IDS`.
**DoD gate:** `wc -l $IDS` ≈ **3969** (window drift since the LIFT_PLAN may shift ±tens — the EXACT count is whatever this query returns *today*; record it as `N_TARGET` and carry it as the expected delta everywhere downstream). Spot-assert: every id is in the manifest, `downloaded`, `skipped_out_of_scope`, and absent from all three sinks (the query already enforces this).
**Blast radius:** none (read-only). **Guardrail:** `$IDS` IS the allow-list; Phase C's GUARD #2 asserts the routed set ⊆ this file. If `$IDS` is empty, `_id_filter_sql` raises (Phase C never starts).

---

## 5. PHASE C — Throwaway route + expand (writes ONLY `_sublift_*`)

**Action:**
```bash
# 5a. ROUTE the 3,969 into the throwaway ledger; gate forced OFF by the id-filter.
$RUN pipelines/sam_gov/sam_attachment_extract_90day.py --phase route \
     --resource-ids-file $IDS \
     --extraction-uri $EXT_TW --scope-uri $SCOPE_TW --pricing-uri $PRICE_TW \
     --unknown-uri $UNK_TW --dedup-uri $DEDUP_TW
# 5b. EXPAND the in-set zip containers (86 projected); inner files route into lanes.
$RUN pipelines/sam_gov/sam_attachment_extract_90day.py --phase expand \
     --resource-ids-file $IDS \
     --extraction-uri $EXT_TW --scope-uri $SCOPE_TW --pricing-uri $PRICE_TW \
     --unknown-uri $UNK_TW --dedup-uri $DEDUP_TW
```
**Inputs/outputs:** in = `$IDS` + read-only `sam_attachment_files_90day`. out = `$EXT_TW` (routed/terminal events), `$DEDUP_TW` (scoped dedup map), `_sublift` inner worklist for expanded zips.
**Cost class:** local CPU, minutes.
**Idempotency:** `phase1_route` LEFT-ANTI-JOINs its resolution view (extract:564) → re-run is a no-op. Expand is content-addressed + anti-joined.
**DoD gate:**
- `$EXT_TW` has ≈ `N_TARGET` `routed` events plus expanded inner files; **zero `skipped_out_of_scope` events** (gate was OFF — verify by counting `state='skipped_out_of_scope'` in `$EXT_TW` == 0).
- The route run printed **no** `phase1: GTM gate ON` line.
- GUARD #2 did not raise (routed parent ids ⊆ `$IDS`).
- Lane distribution roughly matches the LIFT_PLAN §1.1 projection (L3_triage ~3,227 / L4_structured ~444 / L1_scope ~212 / container ~86 — drift tolerated).
**Blast radius:** `_sublift_*` only. **The shared ledger `sam_attachment_extraction_90day` and shared sinks are NOT opened for write** (URIs point at throwaway). **Guardrail:** GUARD #2 aborts before any extract if an out-of-set id leaked into `routed`; gate-OFF coupling prevents re-stickiness (R1); throwaway ledger means even a logic error cannot stamp the shared ledger.

---

## 6. PHASE D — Throwaway extract + finalize (local CPU, multi-hour, no account cap — DOMINANT leg)

**Action (daemonized, resumable):**
```bash
# 6a. EXTRACT all lanes from the throwaway ledger into throwaway sinks. Background it.
$RUN pipelines/sam_gov/sam_attachment_extract_90day.py --phase extract --lane all \
     --daemon --resume --ckpt $EXTRACT_CKPT \
     --extraction-uri $EXT_TW --scope-uri $SCOPE_TW --pricing-uri $PRICE_TW --unknown-uri $UNK_TW
# (no --resource-ids on extract: the throwaway ledger IS the scope — see §B.3 caveat)
# 6b. FINALIZE: row-address dedup + compaction on the throwaway sinks (allowed — no vector index yet).
$RUN pipelines/sam_gov/sam_attachment_extract_90day.py --phase finalize \
     --extraction-uri $EXT_TW --scope-uri $SCOPE_TW --pricing-uri $PRICE_TW --unknown-uri $UNK_TW
```
**Inputs/outputs:** in = `$EXT_TW` routed worklist + read-only blob CAS. out = `$SCOPE_TW`/`$PRICE_TW`/`$UNK_TW` chunk rows + terminal ledger events.
**Cost class:** **local CPU, multi-hour (the dominant wall-clock leg)** — ≈3,969 docs, p50 ~8.5k chars with a long tail to `MAX_EXTRACT_CHARS=4_000_000`. Parallel pdfium/docx/xlsx pool + serialized soffice lane for `.doc`/legacy `.xls`. **No account/LLM spend.** `soffice` must be on PATH (`_assert_soffice`, extract:1400) or the run fails fast.
**Idempotency:** resume = resolution-view ∪ `$EXTRACT_CKPT` (extract:1502; the checkpoint line is written only AFTER chunks+ledger commit, extract docstring lines 27-29). `finalize` row-address dedup (extract:1781) restores `chunk_id` uniqueness from any crash-window double-write. A mid-run crash → re-run the same command; it skips done resources and converges.
**DoD gate:**
- `_sublift_*` chunk sinks populated; throwaway ledger terminal-state distribution sane (bulk `extracted_*`, small `requires_ocr`/`dropped_content_noise` tail).
- `finalize` reports `chunk_id` duplicates removed == small (ideally 0 on a clean run).
- Throwaway chunk count recorded as `N_CHUNKS` (the expected Phase E delta).
**Blast radius:** `_sublift_*` only — brand-new datasets, **no vector index** → `finalize` compaction path is allowed and safe (the `_assert_no_vector_index` guard passes). Shared prod sinks untouched.
**Guardrail:** distinct lease slugs (`extract:<run_id>` on the throwaway URIs) never contend with prod leases (SinkCommitLease, extract:1493). `SinkCommitLease` is held on the three throwaway sink URIs only.

---

## 7. PHASE E — Append `_sublift_*` chunks → SHARED sinks (idempotent by `chunk_id`) [PR #2]

**This is the first and only write to shared state.** It is a NEW small script — its own reviewable PR.

**Code — new `pipelines/sam_gov/subaward_scope_append.py`:** for each (throwaway → shared) sink pair, read throwaway chunk rows **projection-only (NEVER the `embedding` column)**, cast to the live shared schema setting write-time defaults exactly as `_build_chunks` (extract:1260-1279: `embedding=None`; `cells` only for pricing; `lexicon_hit` only for unknown), and merge under the SHARED sink's lease:
```python
import lance, pyarrow as pa
from pipelines.sam_gov.sam_attachment_extract_90day import (
    SinkCommitLease, _scope_schema, _pricing_schema, _unknown_schema, _r2_storage_options)

PAIRS = [  # (throwaway_uri, shared_uri, schema_fn, sink_kind)
    (SCOPE_TW, SCOPE, _scope_schema, "scope"),
    (UNK_TW,   UNK,   _unknown_schema, "unknown"),
    (PRICE_TW, PRICE, _pricing_schema, "pricing"),
]
NON_EMBED = lambda fn: [n for n in fn().names if n != "embedding"]  # never read embedding

for tw, shared, schema_fn, kind in PAIRS:
    so = _r2_storage_options()
    if not _dataset_exists(tw, so):  # a sink may legitimately have zero throwaway rows
        continue
    cols = NON_EMBED(schema_fn)
    src = lance.dataset(tw, storage_options=so).to_table(columns=cols)   # projection (no embedding)
    tgt = lance.dataset(shared, storage_options=so)
    # cast to the live shared schema; embedding back-filled NULL for scope/unknown (picked up by Phase G)
    if kind != "pricing":
        src = src.append_column("embedding",
                pa.array([None]*src.num_rows, type=tgt.schema.field("embedding").type))
    src = src.cast(tgt.schema)   # Lance rejects large_string-vs-string mismatch (text/cells are large_string)
    before = tgt.count_rows()
    with SinkCommitLease(shared, holder="subaward_scope_append"):
        tgt = lance.dataset(shared, storage_options=so)  # re-open under lease
        tgt.merge_insert("chunk_id").when_matched_update_all().when_not_matched_insert_all().execute(src)
    after = lance.dataset(shared, storage_options=so).count_rows()
    print(f"{kind}: +{after-before} rows ({before}->{after}); src={src.num_rows}")
```
Run: `$RUN pipelines/sam_gov/subaward_scope_append.py`.

**Inputs/outputs:** in = `_sublift_*` chunk rows (no embedding). out = SHARED `govcon_scope_vectors_90day` / `govcon_unknown_90day` / `govcon_pricing_90day`, new rows with `embedding = NULL` (scope/unknown).
**Cost class:** local CPU, minutes (≤ a few hundred MB of new chunk rows).
**Idempotency:** `chunk_id = f"{rid}:{ix:04d}"` is deterministic (extract:1265). `merge_insert("chunk_id").when_matched_update_all()` → a re-run rewrites identical values for matched rows = **zero net delta**. This is the SAME index-safe merge_insert the embed module uses on these indexed sinks (`sam_attachment_embed_90day.py:115`) — Lance rewrites only matched fragments; the index covers unmatched rows; the new NULL-embedding tail is brute-forced until Phase G reindex.
**DoD gate:** per sink, `count_rows()` increases by exactly its throwaway chunk count (sum == `N_CHUNKS`); `count(*) == count(DISTINCT chunk_id)` holds (uniqueness preserved); spot-check 5 target `resource_id`s now present in the shared sink.
**Blast radius:** **shared scope/unknown/pricing chunk sinks** — first shared write. **Guardrail:** projection excludes `embedding` (never read); `src.cast(tgt.schema)` asserts column/type parity — Lance rejects a `large_string`-vs-`string` mismatch on `text`/`cells` (extract:407/425). `merge_insert` (NOT overwrite) is the index-safe path → the vector index on scope/unknown is preserved; **no `_assert_no_vector_index` applies** (this is not a compaction). The SHARED-sink `SinkCommitLease` serializes against any concurrent embed/marking writer.
**PR boundary:** **PR #2** — review the append script, merge, pull, *then* run it. (Running an unreviewed script that writes shared state violates the code-PR-before-data-run boundary.)

---

## 8. PHASE F — CUI marking full-body pass (whole-sink, promotion-only) — uses the PR #1 patch

**Precondition:** PR #1 merged (B.5 marking relax live). Without it, the pass RAISES on scope/unknown (D1).

**Action (daemonized; every phase resumable):**
```bash
$RUN pipelines/sam_gov/sam_marking_fullbody_90day.py --phase scan      --daemon   # full-body detection -> decisions JSONL (read-only, heavy)
$RUN pipelines/sam_gov/sam_marking_fullbody_90day.py --phase writeback             # promotions -> chunk rows (subset merge_insert chunk_id)
$RUN pipelines/sam_gov/sam_marking_fullbody_90day.py --phase reconcile             # write-back == expansion assert
```
**Inputs/outputs:** in = re-assembled full text of EVERY resource in all three sinks (no `--resource-ids` filter exists in this module — it is whole-sink by design). out = promoted `content_marking` on the chunk rows whose `promote(existing,detected) != existing` — in practice only the newly-appended chunks that carry markings (the existing corpus was already marked in a prior pass).
**Cost class:** **local CPU, multi-hour full-sink scan** (~2.5 MM chunks reassembled across the three sinks). Daemonize.
**Idempotency:** the writeback worklist is derived from LIVE chunk state (rows where the promotion changes the row, marking:290-306); a crash-resume re-selects only un-applied rows; a re-run after completion selects ZERO rows (double-apply-proof, marking docstring lines 33-36). Subset `merge_insert("chunk_id")` is idempotent besides.
**DoD gate:** `--phase reconcile` reports `reconcile_overall == PASS`; every promoted resource's chunk-level `content_marking` equals its decided post-set; the new target resources that carry markings show non-empty `content_marking` (expected ~620 of `N_TARGET` per the LIFT_PLAN §1.2 15.6% rate — **measured here, never assumed**).
**Blast radius:** shared scope/unknown/pricing `content_marking` column only (subset merge_insert; `text`/`embedding` untouched — marking docstring lines 28-32). **Guardrail:** this pass MUST complete before Phase H2 (LLM) — chunk-level `content_marking` is the single egress enforcement point. The pass is whole-sink so it also re-validates the existing corpus (safe — promotion-only, never downgrades). The B.5 patch keeps the overwrite/compaction guard intact; only the subset write-back is allowed on the indexed sinks.

---

## 9. PHASE G — Embed-refresh + reindex the touched sinks (self-hosted; zero account spend)

**Precondition:** Phase F has reconciled PASS (embed buckets on LIVE `content_marking`; a chunk embedded before F marks it lands an indexed vector for a marked doc — see §1 ordering).

**Action — CLI is a POSITIONAL verb + `--sink` + `--marking` (NO `--phase`; verified against `sam_attachment_embed_90day.py` `cmd` choices):**
```bash
# Embed the new unmarked NULL tail per sink, then reindex. Pin the device (multi-hour on MPS).
EMBED_DEVICE=cuda $RUN pipelines/sam_gov/sam_attachment_embed_90day.py embed --sink scope   --marking unmarked
EMBED_DEVICE=cuda $RUN pipelines/sam_gov/sam_attachment_embed_90day.py embed --sink unknown --marking unmarked
$RUN pipelines/sam_gov/sam_attachment_embed_90day.py index --sink scope
$RUN pipelines/sam_gov/sam_attachment_embed_90day.py index --sink unknown
$RUN pipelines/sam_gov/sam_attachment_embed_90day.py verify    # null_unmarked==0 both sinks
# (pricing has no embedding_idx / no embedding column — D2; embed worklist is scope+unknown)
```
> `embed_sink` default `--marking unmarked` selects `embedding IS NULL AND array_length(content_marking)=0` (embed:70). `index_sink` does compact-best-effort → `create_index IVF_PQ replace=True` → scalar campaign and TOLERATES `null_marked>0` (marked rows excluded from the ANN index by construction). `sam_attachment_embed_90day.py` has NO `--daemon` — run under the persistent-venv + `setsid` pattern (this lift's proven durability path), not a bare ephemeral `uv run`.
**Inputs/outputs:** in = chunk rows with `embedding IS NULL AND content_marking=[]`. out = filled `embedding` + rebuilt IVF_PQ + scalar indices on scope/unknown.
**Cost class:** **self-hosted GPU/MPS** (BGE-large, 1024-dim, extract:412/439). The unmarked NULL set is THIS lift's own new delta — measured live ≈ **127,610 (scope) + 259,566 (unknown) ≈ 387K vectors** — so this is **multi-hour on MPS (~9h); pin `EMBED_DEVICE=cuda`**. Zero account/API spend (self-hosted). **Marked chunks are NOT embedded by this pass (by design)** — the ~4,574/~8,598 new marked chunks (plus the pre-existing ~245K/267K prod marked-NULLs) ship vector-invisible; retrieval safety is the consumption-side `array_length(content_marking)=0` filter, never an embed-side exclusion.
**Idempotency:** `embedding IS NULL` worklist is free-resume; `merge_insert("chunk_id").when_matched_update_all()` full-row (embed:115) preserves `created_at`; `create_index(replace=True)`.
**DoD gate:** `verify` → `null_unmarked == 0` per sink (the completion contract); record the residual `null_marked` (expected NONZERO — never embedded); vector index present on scope + unknown.
**Blast radius:** shared scope/unknown `embedding` + indices. **Guardrail:** queries between Phase E append and Phase G reindex are correct-but-slower (brute-forced NULL tail) — acceptable, documented; the reindex gate requires `embedding IS NULL == 0` (unmarked) before declaring done.

---

## 10. PHASE H — Regex lane (free) + LLM grind (account-burning, resumable), scoped to `$IDS`

### H1 — Regex lane (local CPU, free)
```bash
$RUN pipelines/sam_gov/sam_labor_demand_extract_90day.py --phase extract \
     --resource-ids-file $IDS --resume --daemon          # (file flag added in PR #1 / §B.4)
$RUN pipelines/sam_gov/sam_labor_demand_extract_90day.py --phase index    # after merges settle
```
**Inputs/outputs:** in = the new chunks for `$IDS` in the shared sinks. out = `govcon_award_requirements_90day` + `govcon_labor_demand_90day` rows for those resources (filtered-slice path, labor:1079-1094). Scoped delete-before-merge (idempotent). **Redaction-at-write:** marked resources get NULL `evidence_quote`/`requirement_detail`/`place_of_performance_text` (the write-side CUI gate).
**Cost class:** local CPU, minutes–hour. **Idempotency:** ledger regex-lane state + checkpoint; scoped delete-before-merge means a re-run replaces, never duplicates.
**DoD gate:** every target id terminal in the ledger regex lane; sampled `evidence_quote` substring-asserts green; no marked resource carries verbatim text.

### H2 — LLM grind (session-agent; account-burning; resumable across the 5h cap)
**Hard precondition (MECHANICAL CUI egress gate — PR #3):** `--phase bracket` and `--phase select` call `_assert_marking_complete` and REFUSE unless the Phase-F report (`--marking-report`, default `/tmp/sam_marking_fullbody_report.json`) shows `reconcile_overall == PASS`. Pass `--require-marking-after <chunk-append ISO8601>` so a stale prior-run PASS cannot satisfy it. `--resource-ids-file $IDS` activates the parent-aware structural subset guard in `_pending_worklist` (no out-of-scope id may enter the account-burning lane — incidental scoping is no longer relied upon).
```bash
GATE="--marking-report /tmp/sam_marking_fullbody_report.json --require-marking-after $APPEND_TS --resource-ids-file $IDS"
# 1) BRACKET: derive the marked set LIVE from chunk content_marking; stamp excluded_marked (count MEASURED here).
$RUN pipelines/sam_gov/sam_labor_demand_extract_90day.py --phase bracket  $GATE
# 2) CENSUS: token census over the pending worklist (no staging) — sizes the grind before spend.
$RUN pipelines/sam_gov/sam_labor_demand_extract_90day.py --phase census   $GATE
# 3) SELECT: stage self-contained task files (marked chunks HARD-asserted out). Pilot first, then full.
$RUN pipelines/sam_gov/sam_labor_demand_extract_90day.py --phase select --pilot 4 --manifest-out /tmp/sublift_pilot.json $GATE
$RUN pipelines/sam_gov/sam_labor_demand_extract_90day.py --phase select   $GATE      # full pending worklist
# 4) GRIND: agents read tasks/, write results/. Edit DIR/NB/CONC at the top of the harness per cycle.
node pipelines/sam_gov/reference/p2b_extract_grind_workflow.js   # (or its harness runner)
# 5) INGEST: validate (>=98% run pass-rate gate) + scoped land + ledger.
$RUN pipelines/sam_gov/sam_labor_demand_extract_90day.py --phase ingest
```
**Inputs/outputs:** in = staged task files (one per pending resource, marked excluded). out = validated requirement rows landed into `govcon_award_requirements_90day` under the sink lease + ledger `llm_state` transitions (`pending → … → done`/`quarantined`).
**Cost class:** **account-burning** (session-agent token spend). Corpus = pending after bracket = (covered targets − marked) — **MEASURE at `--phase census`; do NOT assume the ~620/~3,350 projection** (live pre-F inline-marked is far below the ~620 estimate, which is a *post-full-body* figure that F raises). Expected a single grind session or two.
**Resumability across the 5h cap:** the grind harness (`p2b_extract_grind_workflow.js`) launches lanes in throttled groups of `CONC` (line 10) with an **all-failure circuit breaker** — 2 consecutive all-fail groups → it stops launches cleanly (line 28), which is exactly how it detects a 5h session-cap / outage. **Resume by re-running**: per the harness NOTE (line 11), a task whose result file already exists is **skipped**, so re-running re-launches only the unwritten tasks. The ledger `llm_state` advances per-resource, so a mid-grind stop **resumes per-resource and never re-pays** for a landed doc.
**Multi-session lane option (disjoint slices):** raise `CONC`/`NB` per cycle (harness lines 9-10), and/or run **disjoint id slices in parallel sessions** by splitting `$IDS` into N files and staging each into a distinct `--staging-dir`; because `select` stages per-resource task files and `ingest` validates per-file, two sessions over disjoint id sets never collide (distinct task dirs, distinct ledger rows). Each cycle is independently resumable (existing-result-file skip).
**DoD gate:** `--phase ingest` reports `run_pass_rate >= 0.98` (labor:2008) or quarantine-wholesale (`gate_ok=false` lands nothing unless `--force-land`); every target id terminal in the ledger LLM lane (`done`/`quarantined`/`excluded_marked`).
**Blast radius:** `govcon_award_requirements_90day` (LLM rows). **Guardrail (now four gates, one mechanical precondition + three live-signal asserts):** (0) `_assert_marking_complete` — bracket/select REFUSE unless Phase-F `reconcile_overall == PASS` (PR #3; the mechanical F-before-H2 enforcement, no longer convention); (1) `--phase bracket` stamps `excluded_marked` from LIVE `content_marking`; (2) `_pending_worklist` HARD-ASSERTS zero marked resources pending; (3) `build_task_payload` HARD-ASSERTS no marked chunk reaches a task file. With the gate, a not-yet-promoted marked chunk cannot slip through (the live asserts alone read `content_marking`, which is empty until F promotes). **Phase F MUST precede H2 — and is now enforced in code.**

---

## 11. PHASE I — Rebuild the subawardee capability profile (idempotent overwrite)

**Precondition:** F (marking) + G (embed) + H (requirements) complete — the profile re-derives from their outputs; running early builds on partial state.
**Action — CLI is a POSITIONAL verb (NO `--phase`; verified against `build_subawardee_capability_profiles.py` `cmd` choices). Run under persistent-venv + `setsid` (no `--daemon`):**
```bash
$RUN pipelines/sam_gov/build_subawardee_capability_profiles.py build
$RUN pipelines/sam_gov/build_subawardee_capability_profiles.py verify --content-hash
```
**Inputs/outputs:** in = bridge→manifest join + `govcon_award_requirements_90day` + `govcon_doc_scope_90day` over `sub_res_ids`. out = overwritten `govcon_subawardee_capability_profiles` (profile:511, `mode="overwrite"`). Re-derives `sub_res_ids` and re-rolls `has_extracted_scope` / `n_scope_solicitations` / requirements / `scope_summary` / `capability_tags` — **the new chunks/requirements are picked up automatically; no profile-code change.**
**Cost class:** local CPU, minutes. **Idempotency:** overwrite-mode snapshot, `PRAGMA threads=1` deterministic aggregation, stamped with consumed run_ids.
**DoD gate:** `verify --content-hash` shows `has_extracted_scope` RISEN from the pre-lift 3,302 (record the MEASURED new value — do not assert a fixed target; delivered coverage = the ~3,107 covered targets minus the OCR/noise tail, mapped to sub-UEIs); `row_eq_universe` / `row_eq_distinct_uei` true; **CUI checks `scope_summary_without_flag == 0`** (profile:553, `scope_summary IS NOT NULL AND has_extracted_scope = false` → 0) **and `clearance_level_without_flag == 0`** (profile:552); the `_assemble` CUI pre-flight (profile:148-159) did NOT raise (refuses the build if any `govcon_doc_scope_90day` marked row exists or any requirements row leaks verbatim text).
**Blast radius:** overwrites the profile dataset. **Guardrail:** the build itself REFUSES on a CUI-invariant violation (profile:150-159) — a hard backstop confirming the marking pass + redaction-at-write held.

---

## 12. PHASE J — Throwaway cleanup

**Action (the session safety guard blocks `rm` of R2-adjacent absolute paths — use scoped boto3, NOT `rm`):** delete the **SIX** `_sublift_*` datasets — `_sublift_extract_ledger`, `_sublift_scope`, `_sublift_pricing`, `_sublift_unknown`, `_sublift_dedup`, and **`_sublift_inner`** (the inner-worklist copy created by the §B `--inner-uri` isolation) — plus any `_sublift_*` keys under `active/_sink_leases/`. Procedure (the one this lift used): `list_objects_v2(Prefix="active/_sublift_")` → **assert every returned key startswith `active/_sublift_`** → `delete_objects` in 1000-key batches → re-list to confirm empty.
**Cost class:** seconds. **Idempotency:** delete-if-exists. **DoD gate:** the prefix `active/_sublift_` lists zero objects (all six datasets gone). **Blast radius:** none (scratch). **Guardrail:** the per-key `startswith` assertion makes it structurally impossible to delete a non-`_sublift_` (prod) key; distinct lease slugs never contend with prod even if a lease lingers.

---

## 13. Final acceptance (the deliverable proof)

1. **Coverage lift (MEASURED, not asserted):** `build_subawardee_capability_profiles verify` → `has_extracted_scope / universe_bridge_sub_ueis` (profile:557). Ceiling is ~71.4% (4,704/6,586); delivered is lower (R8 — OCR/content-noise tail; this lift covered ~3,107 of 3,969 target resources). **Record the exact delivered rate; do not gate on a fixed band.**
2. **Requirement-filtered sub query returns the new subs with citations:** a query over `govcon_subawardee_capability_profiles` filtered to `has_extracted_scope = true` returns the **measured** set of subs not present pre-lift (the projection was ~+1,274; report the actual), each with a non-empty `scope_summary` / requirement set whose `source_chunk_ids` resolve to the newly-appended chunks. Diff the `sub_uei` set against the pre-lift snapshot to confirm the delta and that each new sub's evidence chunk_ids exist in the shared sinks.
3. **No prime-state mutation (PROVEN at Phase E):** `sam_attachment_extraction_90day` is **byte-identical** to its pre-lift snapshot — Lance version unchanged (v264), row count unchanged (246,296), full-content fingerprint identical, and **0 events carry a `sublift%`/lift `run_id`**. The throwaway ledger absorbed every route/expand/extract event. Re-confirm at the end via the §4 baseline (`/tmp/sublift_baseline.json`).

---

## 14. Risk register (OPERATIONAL — risks + mitigations only)

| # | Operational risk | Mitigation (mechanism) |
|---|---|---|
| R1 | **Shared-ledger re-stickiness** — `skipped_out_of_scope` re-derived for the 3,969. | Throwaway ledger (`$EXT_TW`) **+ gate forced OFF when the id-filter is set** (§B.2 step 1). Ledger-swap alone is insufficient (all 3,969 are `out_of_scope` in the gate); the gate-OFF coupling is mandatory. DoD: zero `skipped_out_of_scope` events in `$EXT_TW`. |
| R2 | **Unrouted-prime blast** — filter absent/empty → route processes the 11,067 in-scope prime backlog (3,054 unchunked). | Default-OFF (`None`) leaves behavior byte-identical (Phase B test #2); **empty-set raise (Guard #1, `_id_filter_sql`)** + **post-route subset assertion (Guard #2, `_assert_routed_subset`)** make out-of-set processing impossible; unit test proves both. |
| R3 | **Schema drift on append** — `large_string` vs `string` on `text`/`cells`; missing `lexicon_hit`/`cells`/`embedding` defaults. | Phase E casts to the LIVE shared schema (`src.cast(tgt.schema)`, Lance rejects mismatch) and sets write-time defaults exactly as `_build_chunks` (extract:1260-1279). Projection excludes `embedding`; it is re-added NULL for scope/unknown. |
| R4 | **IVF_PQ-indexed sinks block the marking pass** (LIVE: scope + unknown carry `embedding_idx` — §0/D1). | PR #1 §B.5 relaxes the two write-back asserts (marking:288/314) to allow the SUBSET-column `chunk_id` merge_insert — the proven index-safe pattern (embed:115). Pricing (no index, D2) is unaffected; the overwrite/compaction guard stays intact. |
| R5 | **Partial-run resumability** — crash mid-extract / marking / regex / LLM. | Extract: resolution-view ∪ `$EXTRACT_CKPT` (extract:1502) + `finalize` dedup. Marking: live-state worklist (double-apply-proof, marking:290). Regex: ledger state + scoped delete-before-merge. LLM: ledger `llm_state` + existing-result-file skip (harness line 11) + circuit breaker (line 28). All daemonized with `--resume`. See §15 checkpoint map. |
| R6 | **Throwaway-artifact cleanup** — `_sublift_*` left behind / leases linger. | Phase J deletes from a terminal (session guard blocks R2-adjacent `rm`); distinct lease slugs never contend with prod even if skipped. |
| R7 | **Append double-write** — re-run inflates rows. | `chunk_id` deterministic (extract:1265) + `merge_insert when_matched_update_all` → re-run is a zero-delta no-op; DoD asserts `count == count(DISTINCT chunk_id)`. `$APPEND_CKPT` records per-sink high-water for human resume. |
| R8 | **Delivered lift < ceiling** — OCR/content-noise tail yields zero chunks for some targets. | Honest framing: ceiling ~71.4%, delivered ~mid-60s%. **Measured at profile `verify`, not assumed.** Not a failure mode — a scope-truth disclaimer. |
| R9 | **Reindex correctness / embed staleness** — new NULL-embedding tail un-indexed until Phase G. | Documented acceptable (brute-forced NULL tail; queries correct-but-slower between E and G). Phase G `index_sink` gate requires `embedding IS NULL == 0` (unmarked) before done; `create_index(replace=True)` rebuilds cleanly. |
| R10 | **`select` token-budget truncation** — long docs over `DOC_TOKEN_BUDGET` stage with `coverage_truncated=true`. | Inherent to the LLM lane; `--phase census` reports the token census before staging. Not a correctness bug — flagged in the task payload (`doc_meta.coverage_truncated`, labor:1551). |
| R11 | **soffice unavailable** at extract → `.doc`/legacy `.xls` lane fails fast. | `_assert_soffice` (extract:1400) fails at startup, not mid-run. Install LibreOffice / set `SOFFICE_BIN` before Phase D. Those docs simply stay `extract_failed` (re-attemptable) until soffice is present — no data loss. |

---

## 15. Resumability / checkpoint map (survive interruption at any phase)

| Phase | Checkpoint mechanism | Resume action | Converges because |
|---|---|---|---|
| A | `$IDS` file (deterministic query) | re-run the probe | query is pure / deterministic |
| C route | `$EXT_TW` resolution view (LEFT-ANTI-JOIN, extract:564) | re-run `--phase route` | already-routed ids anti-joined out |
| C expand | content-addressed inner worklist | re-run `--phase expand` | inner files content-addressed + anti-joined |
| **D extract** | resolution-view ∪ `$EXTRACT_CKPT` (extract:1502); ckpt line written only post-commit | re-run identical `--phase extract --resume --ckpt $EXTRACT_CKPT` | done-set skipped; `_REATTEMPT` states (requires_ocr/extract_failed) re-tried |
| D finalize | row-address dedup is idempotent | re-run `--phase finalize` | dedup removes only crash-window dupes (`_duplicate_rowids`, keep-first) |
| **E append** | per-sink `count_rows` delta + `$APPEND_CKPT`; `merge_insert chunk_id` | re-run `subaward_scope_append.py` | matched rows rewrite identical values = zero net delta |
| **F marking** | per-resource decisions JSONL (scan) + live-state writeback worklist | re-run `--phase scan`→`writeback`→`reconcile` | writeback re-derives from LIVE state; completed → selects zero rows |
| G embed | `embedding IS NULL` worklist (embed:70) | re-run `embed`/`index` (positional verb, NO `--phase`) | NULL worklist shrinks monotonically; `create_index replace=True` |
| H1 regex | ledger regex-lane state + ckpt; scoped delete-before-merge | re-run `--phase extract --resume` | terminal ids skipped; delete-before-merge replaces |
| **H2 LLM** | ledger `llm_state` (per-resource) + result-file existence (harness line 11) + circuit breaker (line 28) | re-run `--phase select`/grind/`--phase ingest` | existing result files skipped; landed resources stay terminal — never re-paid |
| I profile | overwrite snapshot (atomic) | re-run `build` (positional verb, NO `--phase`) | full re-derive from current sink state; deterministic |
| J cleanup | delete-if-exists | re-run delete | idempotent |

**Crash-anywhere rule:** every long phase (C/D/F/G/H1/H2) is daemonized and resumable; re-running the same command after any crash converges without double-pay and without touching shared prime state. The only manual checkpoint is `$APPEND_CKPT` (Phase E) — a human-readable per-sink high-water for confidence; the `merge_insert` idempotency makes even a lost `$APPEND_CKPT` safe.

---

## 16. The single hardest implementation step and how this runbook de-risks it

**Hardest step: §B.2 — the gate-bypass id-filter in `phase1_route`** (the one edit that, wrong, routes the 11,067-prime backlog into shared state). The runbook de-risks it with **four independent, layered guarantees, all landed in PR #1 before any data run:**
1. **Default-OFF by construction** — `only_resource_ids=None` ⇒ `_id_filter_sql` returns `""` and the gate read is untouched ⇒ every existing call path is byte-identical (proved by Phase B test #2).
2. **Empty-set hard raise (Guard #1)** — an empty allow-list (the classic "filter silently matched nothing → full-corpus fallthrough" footgun) RAISES instead of falling through.
3. **Post-route subset assertion (Guard #2)** — after `routed` is built and *before* any extract, `_assert_routed_subset` proves `routed ⊆ allow-list` or aborts; this is the assertion that makes processing an out-of-set prime file structurally impossible.
4. **Throwaway ledger isolation** — even a logic error cannot stamp the shared ledger, because the route/expand/extract runs write `$EXT_TW`, not `sam_attachment_extraction_90day`; the gate-OFF coupling (§B.2 step 1) is what stops the throwaway ledger from re-deriving `skipped_out_of_scope`.

The filter logic is extracted into **pure module-level helpers** (`_id_filter_sql`, `_assert_routed_subset`) so the entire blast-radius contract is covered by a **pure-function unit test with no R2** (mirroring `test_sam_attachment_finalize_dedup.py`), and the live throwaway-route smoke (Phase B gate #3) confirms the gate-OFF + subset behavior end-to-end on ≤2 real ids before the 3,969 ever run.
