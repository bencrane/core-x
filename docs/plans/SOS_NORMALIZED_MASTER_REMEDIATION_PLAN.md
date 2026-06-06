# SoS Normalized Master — Remediation Execution Plan

**Audience:** an autonomous engineering agent executing end-to-end with no prior context.
**Authority:** the diagnostic `docs/reference/SOS_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md` (read-only proof).
**Scope:** two independent fixes to the Secretary-of-State entity-resolution layer.
**Mutation class:** Task 1 is a **data-plane** rebuild (no code change). Task 2 is a **code change + index rebuild**.

---

## 0. Why (the proof, in one paragraph)

The live R2 Lance system-of-record `s3://data-sink/active/sos_normalized_master/` (v4,
17,926,543 rows) stores a `normalized_legal_name` blocking key that was materialized by an
**older version of `core/name_norm.py`**. That old macro stripped `&` as plain punctuation
(dropped it) and concatenated across hyphens. The **current** canonical macro converts
`&` → ` AND ` and dash → space. Proven consequence: **8.036% of stored keys (1,440,646 rows)
disagree with today's `core.name_norm`; 100.000% of the gap is that old `&`/dash rule (0
residual); 1,367,567 distinct current-macro keys (8.12%) have no matching row in the BTREE.**
Any spine/bridge that imports `core.name_norm` and exact-joins the master silently drops every
conjunction-named (`HERMAN & ROOF` → looks up `HERMAN AND ROOF`, stored is `HERMAN ROOF`) and
hyphenated (`COCA-COLA` → `COCA COLA` vs stored `COCACOLA`) entity. Separately, the
`legal_name_base` column + BTREE that `pipelines/sos_normalized/normalize.py` declares is
**absent** from live v4 (the dataset has 11 columns, not 12). A third, smaller defect:
`fl_sos_corporations.registered_agent_name` (968,025 distinct, 76.8%) is the one high-cardinality
resolution key in the SoS corpus with **no BTREE** — point-lookups full-scan 1.26M rows.

**The corrective for the master is a WRITE-TIME re-materialization, not query routing** — the
stored values are wrong, so the data plane must be rewritten. The pipeline code is already
correct (it imports the current macro and projects `legal_name_base`); running it fixes both
the staleness and the missing key in one overwrite.

---

## 1. Pre-flight (do not skip)

Run from the repo root of a `core-x` checkout on a fresh branch off `main`.

### 1.1 Tooling + auth
```bash
# Modal CLI authed to the workspace that owns the sos-normalized / fl-sos apps
modal profile current            # must print an authed profile, not an error
modal secret list | grep -E 'r2-credentials|hqx-postgres'   # both MUST exist

# Doppler authed for read-only verification (R2 creds for the probe venv)
doppler secrets --only-names --project core-x --config prd | grep -E 'R2_ACCESS_KEY_ID|R2_ENDPOINT|R2_SECRET_ACCESS_KEY'
```
If `modal profile current` errors, STOP — the operator must authenticate Modal; do not proceed.

### 1.2 Read-only verification venv (used in §4)
```bash
python3 -m venv /tmp/sosverify && \
/tmp/sosverify/bin/pip install -q "pylance>=7" "duckdb>=1.5" "pyarrow>=17" "boto3>=1.35"
/tmp/sosverify/bin/python -c "import lance,duckdb,pyarrow;print(lance.__version__,duckdb.__version__,pyarrow.__version__)"
# expect: 7.x  1.5.x  >=17  (diagnostic baseline was pylance 7.0.0 / duckdb 1.5.3 / pyarrow 24)
```

### 1.3 Capture the BASELINE (so success is measurable)
Save `/tmp/verify_sos_remediation.py` from the **Appendix (§7)** and run it BEFORE any change:
```bash
REPO=$(git rev-parse --show-toplevel)
PYTHONPATH="$REPO" doppler run --project core-x --config prd -- \
  /tmp/sosverify/bin/python /tmp/verify_sos_remediation.py --phase before
```
Expected BEFORE output (must roughly match — confirms you are pointed at the right datasets):
- `sos_normalized_master`: **v4, 11 cols, legal_name_base ABSENT, 3 indices**; drift CA≈7.9% NY≈8.6% FL≈9.8% CO≈6.9%.
- `fl_sos_corporations`: **registered_agent_name → NO INDEX**.

If the BEFORE state does not match (e.g. legal_name_base already present, or version > 4),
STOP and re-read the diagnostic — the world has changed since this plan was written.

### 1.4 Source-spine currency gate (the master rebuild reads these live)
`run_normalize` re-projects from the four state spines. Confirm they are fully loaded
(rebuilding from a half-loaded spine would corrupt the master). Expected row counts:

| spine | URI | expected rows |
|---|---|--:|
| CA | `s3://data-sink/active/ca_sos_entities/` | 9,389,688 |
| NY | `s3://data-sink/active/ny_sos/` | 4,219,360 |
| FL | `s3://data-sink/active/fl_sos_corporations/` | 1,260,599 |
| CO | `s3://data-sink/active/co_sos/` | 3,056,896 |

The verify script (§7) prints these. **If any spine deviates by >2% from the table above and
you cannot explain it (e.g. a legitimate fresher snapshot), STOP and surface to the operator.**
A larger, freshly-reloaded spine is fine; a *smaller* one means a partial/failed load — do not rebuild on top of it.

---

## 2. Task 1 — Re-materialize `sos_normalized_master` (data-plane; no code change)

The code at `pipelines/sos_normalized/normalize.py` is already correct:
- line 80 `MASTER_BTREE_INDEXES = ["normalized_legal_name", "legal_name_base", "zip_code"]`
- line 390 projects `legal_name_base` from the current `core.name_norm` chain.

So the fix is simply to **run it**. `run_normalize` does: read 4 spines → project (current macro)
→ `UNION ALL` → `lance.write_dataset(mode="overwrite")` → build all BTREE/BITMAP indexes.

> **Do NOT use `reindex()`** for the master. It rebuilds indexes only and cannot add the
> `legal_name_base` *column* — the column must be projected, which only `normalize`/`run_normalize` does.

### 2.1 Execute
```bash
modal run pipelines/sos_normalized/normalize.py::run_normalize
# (optional first: `modal deploy pipelines/sos_normalized/normalize.py` — only needed for the
#  scheduled/dispatcher path; `modal run` builds+runs the image ephemerally on its own.)
```
This is mode="overwrite": readers continue to see v4 until the new version commits atomically,
then see the corrected dataset. The container is 32 GiB / 8 CPU with `LANCE_BYPASS_SPILLING=true`
(the high-cardinality BTREE sorts run in memory). Runtime is dominated by the 17.9M-row
projection + four BTREE builds.

### 2.2 Expected terminal output (success)
```json
{
  "status": "success",
  "dataset_uri": "s3://data-sink/active/sos_normalized_master/",
  "rows_processed": <~17,900,000>,
  "per_state": {"CA": 9389688, "NY": 4219360, "FL": 1260599, "CO": 3056896},
  "indexes": ["BTREE:normalized_legal_name", "BTREE:legal_name_base", "BTREE:zip_code", "BITMAP:source_state"],
  "as_of": "2026-05-31"
}
```
**Gate:** `status == "success"` AND `indexes` contains **`BTREE:legal_name_base`**. If the
indexes list is missing `legal_name_base`, the build hit a per-index `WARN` — read the logs,
do not proceed to verification claiming success.

### 2.3 Contingency
- **OOM during a BTREE build** (rare; `legal_name_base` adds one more 17.9M string sort):
  re-run; if it recurs, bump `memory=` in the `@app.function` decorator on `normalize`
  (line ~482) from `32768` to `49152` and re-run. `LANCE_BYPASS_SPILLING` is already set.
- **A state spine failed to load** (per_state count near zero): abort — fix the spine first
  (its own `pipelines/<state>_sos` ingest), then re-run. `run_normalize` is idempotent.

---

## 3. Task 2 — Add `BTREE` on `fl_sos_corporations.registered_agent_name` (code change + reindex)

Independent of Task 1. Order does not matter; doing this first is a useful smoke test that Modal
auth + the index toolchain work before the larger master rebuild.

### 3.1 Code edit — exactly one line
File `pipelines/fl_sos/sunbiz.py`, the `INDEX_PLAN` literal (around line 142):
```python
# BEFORE
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "master": {"btree": ["document_number", "corporate_name"],
               "bitmap": ["status", "filing_type"]},
    "events": {"btree": ["document_number"], "bitmap": ["event_code"]},
}
# AFTER
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "master": {"btree": ["document_number", "corporate_name", "registered_agent_name"],
               "bitmap": ["status", "filing_type"]},
    "events": {"btree": ["document_number"], "bitmap": ["event_code"]},
}
```
Touch nothing else. `registered_agent_name` is already a materialized column (verified in the
diagnostic, §2); only the index declaration is missing.

### 3.2 Execute the reindex (no re-ingest)
```bash
modal run pipelines/fl_sos/sunbiz.py::reindex --target master
```
`reindex_target("master")` calls `_build_indexes("master", …)`, which rebuilds every index in
`INDEX_PLAN["master"]` with `replace=True` — the four existing ones (idempotent) plus the new
`registered_agent_name` BTREE. Expected output includes `"BTREE:registered_agent_name"` in the
`indexes` list.

---

## 4. Verification (read-only, objective PASS/FAIL)

Run the **same** Appendix script in `--phase after`. It asserts every success criterion and
exits non-zero on any failure.
```bash
REPO=$(git rev-parse --show-toplevel)
PYTHONPATH="$REPO" doppler run --project core-x --config prd -- \
  /tmp/sosverify/bin/python /tmp/verify_sos_remediation.py --phase after
echo "exit=$?"   # MUST be 0
```

### Success criteria (the script checks all of these)
**`sos_normalized_master`:**
1. Schema has **12 columns including `legal_name_base`** (string).
2. Committed indices = BTREE on `{normalized_legal_name, legal_name_base, zip_code}` + BITMAP on
   `source_state`; **every** index `num_indexed_rows == row_count` and `num_unindexed_rows == 0`.
3. **Drift gone:** rows where `normalized_legal_name IS DISTINCT FROM core.name_norm(source_entity_name)`
   is **≈ 0 for CA, NY, FL** (assert each `< 0.05%`). **CO is exempt** — it carries an expected
   residual from its documented pre-norm status-decoration scrub (`_co_status_scrub`); the script
   reports CO separately and does not fail on it.
4. **`&`/dash fix landed (direct check):** sampled names containing `&` now normalize with
   ` AND ` (not dropped); sampled hyphenated names split on the dash (not concatenated).
5. **Pushdown:** `normalized_legal_name = '<sampled>'` and `legal_name_base = '<sampled>'` each
   emit a `ScalarIndexQuery` node (`explain_plan`).

**`fl_sos_corporations`:**
6. `registered_agent_name` has a **BTREE** index, `num_indexed_rows == 1,260,599`, `unindexed == 0`.
7. **Pushdown:** `registered_agent_name = '<sampled>'` emits `ScalarIndexQuery` (was a full
   1.26M-row scan before).

If the script exits 0 and prints `ALL CHECKS PASSED`, the remediation is complete and correct.

---

## 5. Ship + close the loop

### 5.1 Task 2 is a code change → full git lifecycle
```bash
git add pipelines/fl_sos/sunbiz.py
git commit -m "feat(fl_sos): BTREE index on registered_agent_name (close SoS agent MSHA gap)

registered_agent_name (968,025 distinct, 76.8%) was the lone high-cardinality
resolution key in the SoS corpus without a scalar index — point-lookups full-scanned
1.26M rows. Adds it to INDEX_PLAN[master].btree; reindexed via reindex --target master.
Ref docs/reference/SOS_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md.

Co-Authored-By: Claude <noreply@anthropic.com>"
git push -u origin <branch>
gh pr create --base main --title "feat(fl_sos): BTREE on registered_agent_name (SoS agent MSHA gap)" --body "<summary + verify output>"
gh pr merge <num> --squash --delete-branch
```
Then **pull into the operator-facing `main` checkout** and verify with `git log -1 --oneline`.
(Note: in this environment `main` may be checked out in a sibling worktree, not the primary
path — locate it with `git worktree list | grep '\[main\]'` and `git -C <that-path> merge --ff-only origin/main`.)

### 5.2 Task 1 is data-plane only → no PR
The master rebuild changes no code. Its audit trail is the `ops.sos_normalized_runs` row written
automatically by `_record_run` (phase=`normalize`, status=`success`). Paste the `run_normalize`
JSON result and the `--phase after` verifier output into the operator report. Do **not** open a
PR for the data rebuild.

### 5.3 Final report to operator
State plainly: master rebuilt to v5 (or current+1), legal_name_base materialized + indexed,
drift on CA/NY/FL → 0, CO residual = `<n>` (expected scrub), FL agent BTREE live. Quote the
verifier's PASS line and the before/after drift numbers.

---

## 6. Rollback / safety

- **Master (Lance is versioned):** the overwrite creates a new version; v4 is still addressable.
  To revert: `lance.dataset(uri, storage_options=so).checkout_version(4)` then
  `.restore()` (rewrites the manifest pointer to v4 — no data copy). Only do this if §4 fails
  and cannot be fixed forward. Re-running `run_normalize` is the preferred fix (idempotent).
- **FL index:** `replace=True` is idempotent. To revert, restore the one-line code change and
  re-run `reindex --target master`; the extra index is dropped on the next full ingest, or leave
  it (an extra trained BTREE is harmless).
- **Atomicity:** Lance commits are atomic; concurrent readers never see a torn write. No
  maintenance window required. Downstream `epa_to_sos_bridge` joins keep working throughout
  (and *improve* — the 8.12% join-loss closes the moment v5 commits).
- **Blast radius:** Task 1 touches one dataset; Task 2 touches one dataset's index set. No other
  pipeline writes these paths.

---

## 7. Appendix — `/tmp/verify_sos_remediation.py` (save verbatim)

Read-only. Imports the canonical macro (`PYTHONPATH=<repo>`), so its `name_norm` is identical to
what the pipeline used. Exits non-zero on any failed assertion in `--phase after`.

```python
"""SoS remediation verifier — read-only. Usage:
  PYTHONPATH=<repo> doppler run --project core-x --config prd -- python verify_sos_remediation.py --phase {before|after}
Exits 0 only if all --phase after checks pass."""
from __future__ import annotations
import argparse, os, sys
import duckdb, lance
from core.name_norm import name_norm as _name_norm

def so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com" if os.environ.get("R2_ACCOUNT_ID") else None)
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}

B = "s3://data-sink/active"
SO = so()
ap = argparse.ArgumentParser(); ap.add_argument("--phase", choices=["before", "after"], default="after")
PHASE = ap.parse_args().phase
fail = []
def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{(' — ' + detail) if detail else ''}")
    if not ok: fail.append(name)

def idx_map(ds):
    m = {}
    for ix in ds.list_indices():
        d = ix if isinstance(ix, dict) else {"name": ix.name, "type": str(ix.type), "fields": ix.fields}
        for f in (d.get("fields") or []):
            st = ds.stats.index_stats(d["name"])
            m[f] = (str(d.get("type")), st.get("num_indexed_rows"), st.get("num_unindexed_rows"))
    return m

# ---- spine currency ----
print("== source spines ==")
for nm, exp in [("ca_sos_entities", 9389688), ("ny_sos", 4219360), ("fl_sos_corporations", 1260599), ("co_sos", 3056896)]:
    n = lance.dataset(f"{B}/{nm}/", storage_options=SO).count_rows()
    print(f"  {nm:22} rows={n:>12,} (baseline {exp:,}, delta {(n-exp)/exp*100:+.2f}%)")

# ---- master ----
print("== sos_normalized_master ==")
m = lance.dataset(f"{B}/sos_normalized_master/", storage_options=SO)
cnt = m.count_rows(); cols = {f.name for f in m.schema}; im = idx_map(m)
print(f"  version={m.version} rows={cnt:,} cols={len(m.schema)}")
has_lnb = "legal_name_base" in cols
if PHASE == "after":
    check("legal_name_base column present", has_lnb)
    for col in ("normalized_legal_name", "legal_name_base", "zip_code"):
        t = im.get(col); check(f"{col} BTREE trained", bool(t) and t[0]=="BTree" and t[1]==cnt and t[2]==0, str(t))
    t = im.get("source_state"); check("source_state BITMAP trained", bool(t) and t[0]=="Bitmap" and t[1]==cnt and t[2]==0, str(t))
else:
    print(f"  legal_name_base present? {has_lnb}  indices={ {k:v[0] for k,v in im.items()} }")

# drift by state (vs CURRENT macro)
con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=4; SET memory_limit='8GB';")
con.register("m", m.scanner(columns=["source_state","source_entity_name","normalized_legal_name"], batch_size=131072).to_reader())
expr = _name_norm("source_entity_name")
rows = con.execute(f"""SELECT source_state, count(*) r,
  count(*) FILTER (WHERE normalized_legal_name IS DISTINCT FROM {expr}) mism
  FROM m GROUP BY 1 ORDER BY 1""").fetchall()
print("  drift vs current name_norm:")
for st, r, mm in rows:
    pct = mm/r*100 if r else 0
    print(f"    {st}: {mm:,}/{r:,} = {pct:.3f}%")
    if PHASE == "after" and st in ("CA","NY","FL"):
        check(f"{st} drift ~0", pct < 0.05, f"{pct:.3f}%")
    if PHASE == "after" and st == "CO":
        print(f"    (CO residual {pct:.3f}% is the expected pre-norm status-decoration scrub — informational)")

# direct &/dash fix check (after only)
if PHASE == "after" and has_lnb:
    con.register("m2", m.scanner(columns=["source_entity_name","normalized_legal_name"]).to_reader())
    amp = con.execute("""SELECT source_entity_name, normalized_legal_name FROM m2
        WHERE strpos(source_entity_name,'&')>0 AND normalized_legal_name IS NOT NULL LIMIT 1""").fetchone()
    if amp: check("'&' -> ' AND '", " AND " in amp[1], f"{amp[0]!r}->{amp[1]!r}")
    con.register("m3", m.scanner(columns=["source_entity_name","normalized_legal_name"]).to_reader())
    dsh = con.execute("""SELECT source_entity_name, normalized_legal_name FROM m3
        WHERE regexp_matches(source_entity_name,'[A-Za-z]-[A-Za-z]') AND normalized_legal_name IS NOT NULL LIMIT 1""").fetchone()
    # a hyphen between word chars must become a space (two tokens), never concatenation
    if dsh: check("dash -> space", " " in dsh[1], f"{dsh[0]!r}->{dsh[1]!r}")

# pushdown (after only): ScalarIndexQuery on the materialized keys
if PHASE == "after":
    s = m.scanner(columns=["source_state"], filter="source_state = 'NY'", limit=1).to_table().to_pylist()
    samp = m.scanner(columns=["normalized_legal_name","legal_name_base"], limit=200).to_table().to_pylist()
    nl = next((r["normalized_legal_name"] for r in samp if r["normalized_legal_name"] and "'" not in r["normalized_legal_name"]), None)
    lb = next((r["legal_name_base"] for r in samp if r.get("legal_name_base") and "'" not in r["legal_name_base"]), None)
    if nl: check("master normalized_legal_name -> ScalarIndexQuery",
                 "ScalarIndexQuery" in m.scanner(columns=["normalized_legal_name"], filter=f"normalized_legal_name = '{nl}'").explain_plan(verbose=True))
    if lb: check("master legal_name_base -> ScalarIndexQuery",
                 "ScalarIndexQuery" in m.scanner(columns=["legal_name_base"], filter=f"legal_name_base = '{lb}'").explain_plan(verbose=True))
con.close()

# ---- FL agent ----
print("== fl_sos_corporations.registered_agent_name ==")
fl = lance.dataset(f"{B}/fl_sos_corporations/", storage_options=SO); fim = idx_map(fl); fcnt = fl.count_rows()
t = fim.get("registered_agent_name")
if PHASE == "after":
    check("registered_agent_name BTREE trained", bool(t) and t[0]=="BTree" and t[1]==fcnt and t[2]==0, str(t))
    rs = fl.scanner(columns=["registered_agent_name"], limit=200).to_table().to_pylist()
    ra = next((r["registered_agent_name"] for r in rs if r["registered_agent_name"] and "'" not in r["registered_agent_name"]), None)
    if ra: check("FL registered_agent_name -> ScalarIndexQuery",
                 "ScalarIndexQuery" in fl.scanner(columns=["registered_agent_name"], filter=f"registered_agent_name = '{ra}'").explain_plan(verbose=True))
else:
    print(f"  registered_agent_name index = {t}")

print()
if PHASE == "after":
    if fail:
        print(f"FAILED {len(fail)} CHECK(S): {fail}"); sys.exit(1)
    print("ALL CHECKS PASSED"); sys.exit(0)
print("BEFORE snapshot complete (no assertions).")
```

---

## 8. One-screen runbook (after pre-flight passes)

```bash
# Task 2 (small, smoke-tests Modal): edit pipelines/fl_sos/sunbiz.py INDEX_PLAN[master].btree
modal run pipelines/fl_sos/sunbiz.py::reindex --target master

# Task 1 (the load-bearing rebuild)
modal run pipelines/sos_normalized/normalize.py::run_normalize     # expect indexes incl BTREE:legal_name_base

# Verify (must exit 0, print ALL CHECKS PASSED)
REPO=$(git rev-parse --show-toplevel)
PYTHONPATH="$REPO" doppler run --project core-x --config prd -- /tmp/sosverify/bin/python /tmp/verify_sos_remediation.py --phase after

# Ship the code change (Task 2), pull main, report.
```
```
DONE = (1) master v>=5 with legal_name_base column+BTREE and CA/NY/FL drift ~0,
       (2) fl_sos_corporations.registered_agent_name BTREE live & emitting ScalarIndexQuery,
       (3) FL code change merged to main and pulled into the operator checkout,
       (4) verifier exits 0.
```
