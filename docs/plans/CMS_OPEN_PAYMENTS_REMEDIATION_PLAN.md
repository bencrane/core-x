# CMS Open Payments — Remediation & Hardening Execution Plan

**Status:** Ready to execute. **Owner of record:** authored from the live diagnostic in [`docs/cms_open_payments_structural_diagnostic.md`](../cms_open_payments_structural_diagnostic.md) (PR #210, commit `6c05379`).
**Scope:** `pipelines/cms_open_payments/ingest.py` (the only file with logic changes) + operational restore of `s3://data-sink/active/cms_general_payments/`.
**Execution model:** an AI agent executes phases **in order**. Each phase is independently shippable, has a hard acceptance gate, and a rollback. Phases 0→1 are mandatory (P0). Phase 2 is correctness-hygiene. Phase 3 is optional maintenance.
**Credentials:** all R2/Postgres access via `doppler run --project core-x --config prd -- <cmd>` (`R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `HQX_DB_URL_POOLED`). Modal worker uses secrets `r2-credentials` + `hqx-postgres`. Never persist secret values.

---

## 0. Current state — the concrete learnings this plan is built on

Measured, not assumed (all from the diagnostic + `ops.cms_open_payments_runs` run #58):

1. **`cms_general_payments` is destroyed.** R2 holds **71 orphaned `data/*.lance` fragments (16,086,469,644 B / 14.98 GiB)** and **zero** `_versions/`, `_indices/`, `_transactions/`. `lance.dataset()` raises `Not found: …/_versions`. Footers sum to **70,051,574 rows of 82,290,893** ingested (run #58) → **12,239,319 rows + ~12 data files never uploaded.** Unreadable, unindexed, and **unrecoverable from R2** (no manifest to identify valid fragments; ~12M rows simply absent). Local rebuild gone with the Modal container. → **must re-ingest from CMS.**
2. **Root cause = the publish primitive.** `_replace_r2_prefix()` (`ingest.py:431`) **deletes the whole prefix, then `os.walk`-uploads every data file with no retry and no atomic swap.** A single transient R2 error on file ~72 of ~83 aborted run #58 *after* the wipe and *before* `_versions/`/`_indices/`/`_transactions/` — leaving the corpse. This **violates the documented fleet rule** (`ARCHITECTURE.md` → "Giants — Volume-staged, append-only": *upload only the new files; never wipe or re-upload data files*). It re-uploads all 16 GiB every cycle, so its failure surface **scales with data size** — general is the worst case and **will recur**.
3. **`cms_research_payments` (5,936,454 rows) and `cms_ownership` (27,480 rows) are physically healthy** — 0 tombstones, v2.1, indices cover 100% of fragments (research 10/10→8/8; ownership 8/8→7/7), lean index:data (research 5.1%). No physical action needed.
4. **Logical defects (correctness-affecting):** `date_of_payment` dirt floor **`0002-11-30`** (research); literal **`"N/A"` sentinel** in `associated_device_or_medical_supply_pdi_*` (general ~1.2M rows) not nulled by the current `nullif(trim(x),'')`; `recipient_state` un-normalized (60–67 NDV vs ~51 USPS).
5. **Logical noise (NOT correctness, NOT disk):** ~15 fully-null + ~40 ≥98%-null columns (research PI 2–5 groups); `program_year` ⊂ `payment_year`; constant `payment_publication_date` / `delay_in_publication_indicator`. **Lance v2.1 already compresses these to ~0 on disk** (research 2.99× on an all-string schema) — chasing them is low-value (see §3 Rejected).
6. **Resolution-key reality:** `covered_recipient_npi` is **96.40% null in research** (real key `principal_investigator_1_npi`, 4.27% null) but **0.60% null in general** (correct key there). Per-family `npi_col` is already right; this is a *consumer-documentation* fact, not an index defect.
7. **Pushdown works** at the Lance scanner (`ScalarIndexQuery`, 1-row BTREE lookup 4 ms / 2.27 KB) **only when predicates reach `scanner(filter=…, prefilter=True)`**; an unfiltered DuckDB-over-Arrow scan full-scans 5.94M rows.
8. **Giant index build is multipart-safe today** (build local → boto3 uniform-part upload sidesteps R2 `400 InvalidPart`). The real giant risk is **spot-capacity preemption** of a 10 h `retries=0` job on `ephemeral_disk=512 GiB` (`ARCHITECTURE.md` explicitly warns against large `ephemeral_disk`).

---

## 1. Objectives & measurable success criteria

| # | Objective | Acceptance (binary, verifiable) |
|---|---|---|
| O1 | Publish can never again leave a corrupt/destroyed dataset | A killed publish leaves the **previous** version fully readable; a re-run **resumes** and completes; "success" is recorded only after a fresh R2 read-back verifies row + index counts |
| O2 | `cms_general_payments` restored | `lance.dataset(uri).count_rows()` == ledger sum (≈ **82,290,893**, ± CMS revision); `len(list_indices())` == **10**; a BTREE point-lookup on `covered_recipient_npi` returns; ledger `publish`+`verify` = `success`; **0 orphaned files remain** |
| O3 | Giant ingest survives preemption | general backfill runs Volume-staged; a mid-run kill + re-run with `resume=True` completes without re-downloading already-landed years |
| O4 | Correctness dirt removed (Phase 2) | post-rebuild: research `min(date_of_payment) ≥ 2013-01-01`; `count(*) where associated_device_*_pdi_* = 'N/A'` == 0; `recipient_state` ⊆ USPS∪{NULL} |
| O5 | Zero collateral damage | research/ownership untouched by Phases 0–1 (`version`, `count_rows`, `list_indices` unchanged before/after) |

---

## 2. Engineering decisions (baked in — rationale, including what we deliberately are NOT doing)

**D1 — Publish becomes non-destructive, append-only, resumable, verified.** Replace `_replace_r2_prefix` with `_publish_dataset(...)` that (a) **never wipes** a live dataset, (b) uploads **only files absent/size-mismatched on R2** (resume-by-skip), (c) enforces **body-before-manifest** ordering so a manifest is never visible before the data/index files it references, (d) **per-file retry with backoff**, (e) a post-upload **read-back verify gate** that opens the dataset fresh from R2 and asserts row + index counts before "success" is recorded. *Rationale:* directly implements the `ARCHITECTURE.md` giant rule and makes the manifest write the atomic linearization point. R2 is strongly read-after-write consistent, so the read-back gate is sound.

**D2 — General is Volume-staged with per-year checkpointing.** Stage general's local dataset on a **Modal Volume** (`modal.Volume`), not large `ephemeral_disk`. *Rationale:* network storage keeps the 10 h job off preemptible spot capacity (`ARCHITECTURE.md`), and the Volume persists across container restarts so `resume=True` skips already-landed years. `ephemeral_disk` is kept modest for DuckDB CSV spill only. research/ownership stay on ephemeral (small, fast).

**D3 — One-time clobber of the broken general prefix, guarded.** The restore deletes the 71 orphaned files **only after asserting the prefix has no live manifest** (`_versions/` object count == 0). *Rationale:* the no-wipe rule (D1) protects *valid* datasets; general's corpse has zero readers and ~12M missing rows — deleting it is the correct reclaim, but the guard prevents ever clobbering a healthy dataset by mistake.

**D4 — Do NOT salvage the 70M orphaned rows.** Re-ingest all 7 years from CMS. *Rationale:* without a manifest we cannot know which orphaned fragments are valid or de-duplicated, the idempotent-replace lineage is lost, and 12M rows are absent regardless. The CMS CSVs are the source of truth; a clean rebuild is the only trustworthy path.

**D5 — Fix sentinels/dirt at the field level; preserve every row.** Null `"N/A"` (and `"NA"`) **only** on the `associated_device_or_medical_supply_pdi_*` / `associated_drug_or_biological_ndc_*` columns; null `date_of_payment` values `< 2013-01-01` (Open Payments inception). **Never drop a row** — a dirty field does not invalidate a real payment record, and dropping rows would break the `payment_year` idempotent-replace contract. *Rationale:* targeted nulling fixes null-density/cardinality truth and the BTREE zone-map floor without a global `nullif('N/A')` that would corrupt free-text fields (`contextual_information`, `name_of_study`).

**D6 — Keep the dynamic full-fidelity projection. Do NOT hardcode-drop dead columns.** *Rationale:* the per-file dynamic projection is a deliberate robustness invariant (survives any CMS schema change). The dead columns cost ~0 on disk (Lance crushes nulls) and CMS may populate PI/drug groups in future years; a static drop-list would silently discard future real data for a non-benefit. (If ever desired, do it as a *dynamic* "drop columns 100% null across all landed years computed at publish time" — explicitly **out of scope** here.)

**D7 — Do NOT cluster/`ORDER BY` the resolution key.** *Rationale:* measured — a BTREE point lookup already returns in 4–19 ms and `rows_scanned` equals the true match count; the multi-fragment hits are because an NPI genuinely recurs across year-fragments (those rows must be read regardless). Global clustering would force a full external re-sort of an 82M-row giant on every quarterly refresh to buy negligible fragment-pruning at 7–83 fragments. The append-per-year topology is correct for this access pattern.

**D8 — Do NOT recast types for storage** (`record_id`→int64, categoricals→dictionary). *Rationale:* Lance v2.1 already dictionary/RLE-encodes physically (2.99× on all-string research); the win is the BITMAP indices, which already exist. `record_id` stays VARCHAR for consistency with the all-VARCHAR leading-zero-safety posture. Cross-consumer ripple isn't worth a marginal column narrowing.

**D9 — Optional cross-family index parity:** add BITMAP `form_of_payment_or_transfer_of_value` to research (general already indexes it; 5 NDV, ideal BITMAP fit). Low cost, index-only. Ship with Phase 2 or skip — non-blocking.

**D10 — Blast-radius isolation preserved.** Index builds and publishes remain separated from ingest; general's restore touches only the general prefix; Phase 2 rebuilds are per-family `overwrite`s through the same hardened publish.

---

## 3. Phase 0 — Harden the publish (CODE; mandatory; deploy before any data op)

All edits in `pipelines/cms_open_payments/ingest.py`. No data is mutated in this phase.

### 3.1 New helper — resumable upload with retry
```python
def _upload_file_with_retry(s3, local_path, bucket, key, *, attempts=5):
    import time
    last = None
    for i in range(attempts):
        try:
            s3.upload_file(local_path, bucket, key)   # boto3 = uniform-part multipart (R2-safe)
            return
        except Exception as exc:                       # noqa: BLE001 — transient R2/network
            last = exc
            time.sleep(min(2 ** i, 30))
    raise RuntimeError(f"upload failed after {attempts} attempts: {key}: {last}")
```

### 3.2 New helper — remote object index (for resume-by-skip + verify)
```python
def _remote_index(s3, prefix) -> dict[str, int]:
    """{relpath: size} for every object under prefix."""
    out, tok = {}, None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=prefix)
        if tok: kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            out[o["Key"][len(prefix):]] = o["Size"]
        if r.get("IsTruncated"): tok = r.get("NextContinuationToken")
        else: break
    return out
```

### 3.3 New primitive — `_publish_dataset` (replaces `_replace_r2_prefix`)
```python
_UPLOAD_RANK = {"data": 0, "_indices": 1, "_transactions": 2}  # _versions (manifests) = last

def _publish_dataset(s3, prefix, local_dir, *, allow_clobber=False) -> int:
    """Non-destructive, append-only, resumable publish.
    Invariants: (1) never wipe a live dataset; (2) every data/_indices/_transactions file
    is present + size-verified on R2 BEFORE any _versions/*.manifest uploads (manifest =
    atomic commit); (3) resume-by-skip (matching size); (4) per-file retry.
    allow_clobber: delete the prefix first — ONLY permitted when no live manifest exists
    (a broken-prefix restore). Asserts that precondition."""
    import os
    files = []  # (rel, abspath, size, rank)
    for root, _, fns in os.walk(local_dir):
        for fn in fns:
            ap = os.path.join(root, fn)
            rel = os.path.relpath(ap, local_dir).replace(os.sep, "/")
            top = rel.split("/", 1)[0]
            rank = 99 if top == "_versions" else _UPLOAD_RANK.get(top, 50)
            files.append((rel, ap, os.path.getsize(ap), rank))
    body      = sorted([f for f in files if f[3] != 99], key=lambda f: f[3])
    manifests = [f for f in files if f[3] == 99]

    if allow_clobber:
        if _remote_index(s3, prefix + "_versions/"):
            raise RuntimeError(f"refusing clobber: {prefix} has a live manifest")
        _delete_prefix(s3, prefix)                     # extracted from old _replace_r2_prefix

    remote = _remote_index(s3, prefix)
    for rel, ap, sz, _ in body:                        # resume-by-skip + retry
        if remote.get(rel) == sz: continue
        _upload_file_with_retry(s3, ap, BUCKET, prefix + rel)

    remote = _remote_index(s3, prefix)                 # BARRIER: verify body complete
    missing = [rel for rel, _, sz, _ in body if remote.get(rel) != sz]
    if missing:
        raise RuntimeError(f"publish abort: {len(missing)} body files missing post-upload, e.g. {missing[:3]}")

    for rel, ap, sz, _ in manifests:                   # COMMIT last
        _upload_file_with_retry(s3, ap, BUCKET, prefix + rel)
    return len(body) + len(manifests)
```
> `_delete_prefix(s3, prefix)` = the paginated `delete_objects` loop currently inside `_replace_r2_prefix` (lines 436–443), extracted. The `os.walk`-upload tail (444–451) is now obsolete — delete `_replace_r2_prefix` and repoint callers.

### 3.4 New gate — `_verify_published` (read-back from R2)
```python
def _verify_published(uri, expected_rows, expected_indices, npi_col, storage_options) -> dict:
    """Open the dataset FRESH from R2 (post-publish) and assert it is whole.
    Raises if row/index counts disagree or a BTREE probe fails. R2 is strongly
    read-after-write consistent, so this reflects the just-committed manifest."""
    import lance
    ds = lance.dataset(uri, storage_options=storage_options)
    rows = ds.count_rows()
    nidx = len(ds.list_indices())
    if rows != expected_rows:
        raise RuntimeError(f"verify fail {uri}: R2 rows={rows} != expected {expected_rows}")
    if nidx != expected_indices:
        raise RuntimeError(f"verify fail {uri}: R2 indices={nidx} != expected {expected_indices}")
    ds.scanner(filter=f"{npi_col} IS NOT NULL", columns=[npi_col], limit=1,
               prefilter=True).to_table()             # BTREE pushdown probe
    return {"rows": rows, "indices": nidx}
```
Add `EXPECTED_INDEX_COUNT = {"general": 10, "research": 10, "ownership": 8}` (derived from the registry: BTREE + BITMAP per family).

### 3.5 Caller edits
- **`refresh_all`** (`ingest.py:959`): replace the per-family `_replace_r2_prefix(...)` call (line 1011) with `_publish_dataset(s3, _family_prefix(family), local_ds)`; then call `_verify_published(...)` against `EXPECTED_INDEX_COUNT[family]` and the family's `npi_col`. Record a new `phase="verify"` ledger row. Only set `publish_status="success"` after verify passes. Add `resume: bool = False` param; when `resume` and a valid local dataset exists on the Volume, **do not `rmtree`** — skip years where `count_rows(filter=f"payment_year={Y}")>0`. Commit the Volume after each year (`vol.commit()`).
- **`ingest_family_year`** (`ingest.py:848`): replace `_replace_r2_prefix` (line 925) with `_publish_dataset(...)` + `_verify_published(...)`. (This path already stages from R2 → shares file lineage → the diff makes it upload only the changed year's fragments + new index/manifest. Big steady-state win.)
- **`reindex_family`** (`ingest.py:1067`): replace `_replace_r2_prefix` (line 1088) with `_publish_dataset(...)` + `_verify_published(...)`.

### 3.6 Volume staging (general only)
```python
cms_vol = modal.Volume.from_name("cms-op-staging", create_if_missing=True)
VOL_DIR = "/vol"
def _local_ds(family, *, on_volume=False):
    base = VOL_DIR if on_volume else SCRATCH_DIR
    return os.path.join(base, f"{family}_lance")
```
Mount `cms_vol` at `/vol` on `refresh_all` (and a new `reindex_family` Volume mount for general). Route **general** staging to the Volume; research/ownership to `SCRATCH_DIR`. Lower `refresh_all` `ephemeral_disk` from 524288 → e.g. 131072 (128 GiB, DuckDB CSV spill only) once general no longer stages there. Keep `retries=0` (resume is now explicit + safe via D2).

### 3.7 Phase 0 tests (acceptance — must pass before Phase 1)
1. **Unit** (pure, no network): `_publish_dataset` file partition/order — assert manifests sort last, `data` before `_indices` before `_transactions`; resume-skip when sizes match; clobber raises when `_versions/` non-empty.
2. **Integration on ownership** (tiny, safe, reversible): `modal run …::reindex_family --family ownership` through the new path → assert `count_rows`==27,480, `list_indices`==8, `version` advanced by exactly the index commits, **0 orphaned files** (object count sane), ledger `verify=success`.
3. **Kill-test on ownership:** start a publish, kill mid-body; confirm the dataset still opens at the **prior** version (no corruption); re-run; confirm it resumes and verify passes.

**Phase 0 gate:** all three pass → PR → merge → `modal deploy pipelines/cms_open_payments/ingest.py`.

---

## 4. Phase 1 — Restore `cms_general_payments` (OPS; P0; after Phase 0 deployed)

### 4.1 Pre-flight (re-confirm broken at execution time — the diagnostic is a snapshot)
```bash
doppler run --project core-x --config prd -- python - <<'PY'
import os, boto3
from botocore.config import Config
s3=boto3.client("s3",endpoint_url=os.environ["R2_ENDPOINT"],
  aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
  aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],region_name="auto",
  config=Config(s3={"addressing_style":"path"}))
p="active/cms_general_payments/"
v=s3.list_objects_v2(Bucket="data-sink",Prefix=p+"_versions/").get("KeyCount",0)
d=s3.list_objects_v2(Bucket="data-sink",Prefix=p+"data/").get("KeyCount",0)
print(f"_versions={v} data={d} -> {'BROKEN (proceed)' if v==0 and d>0 else 'NOT broken — STOP, re-assess'}")
PY
```
**If `_versions != 0` → STOP** (someone restored it; do not clobber). Else proceed.

### 4.2 Restore
`refresh_all` with `allow_clobber=True` for general's first publish (passed through to `_publish_dataset`) restores in one shot: re-ingest 2018–2024 → Volume rebuild → reindex → guarded clobber of the orphaned prefix → hardened publish → verify gate.
```bash
# add a `--clobber-broken` local-entrypoint flag wired to allow_clobber on the general publish
modal run pipelines/cms_open_payments/ingest.py::backfill --only-family general --clobber-broken
# if preempted/killed mid-run, resume (Volume retains landed years):
modal run pipelines/cms_open_payments/ingest.py::backfill --only-family general --clobber-broken --resume
```

### 4.3 Phase 1 acceptance gate
```bash
doppler run --project core-x --config prd -- python - <<'PY'
import os, lance
so={"aws_access_key_id":os.environ["R2_ACCESS_KEY_ID"],"aws_secret_access_key":os.environ["R2_SECRET_ACCESS_KEY"],
    "aws_endpoint":os.environ["R2_ENDPOINT"],"aws_region":"auto","aws_virtual_hosted_style_request":"false"}
ds=lance.dataset("s3://data-sink/active/cms_general_payments/",storage_options=so)
n=ds.count_rows(); idx=[i["name"] for i in ds.list_indices()]
probe=ds.scanner(filter="covered_recipient_npi IS NOT NULL",columns=["covered_recipient_npi"],limit=1,prefilter=True).to_table().num_rows
print(f"rows={n:,} (expect ~82,290,893)  indices={len(idx)} (expect 10)  probe={probe}")
assert n>82_000_000 and len(idx)==10 and probe==1, "RESTORE INCOMPLETE"
print("RESTORE OK")
PY
modal run pipelines/cms_open_payments/ingest.py::show_ledger --limit 8   # general publish + verify = success
```
Also confirm **no orphaned files**: total object count ≈ data files + `_indices/` + `_versions/` + `_transactions/` consistent with research's shape (no leftover unreferenced data). **Gate:** assertion passes + ledger `verify=success`.

### 4.4 Phase 1 rollback
No destructive step until the guarded clobber, which only fires on an already-broken prefix → nothing valid to lose. If the run fails mid-way: the Volume holds landed years → `--resume`. If the clobber succeeded but publish failed: the prefix is empty (still broken state, same as start) → re-run `--clobber-broken`. **No state is worse than the pre-restore corpse at any point.**

---

## 5. Phase 2 — research/ownership correctness hygiene (CODE + OPS; not P0)

Fold the field-level fixes (D5) into the dynamic transform, then rebuild each healthy family through the hardened publish (per-family `overwrite`).

### 5.1 Transform edits (`_projection` / `_build_sql`, `ingest.py:570`/`606`)
- **Sentinel nulling (D5)** — extend the per-column expression for the targeted families of columns only:
  ```python
  _SENTINEL_COLS_PREFIXES = ("associated_device_or_medical_supply_pdi_",
                             "associated_drug_or_biological_ndc_")
  # in _projection, for alias matching a sentinel prefix:
  expr = "nullif(nullif(nullif(trim({q}),''),'N/A'),'NA')".format(q=q)
  ```
- **Date sanitization (D5)** — for `date_of_payment` only, wrap the existing cast:
  ```python
  # MM/DD/YYYY -> DATE, then null implausible (< Open Payments inception 2013-01-01)
  expr = ("CASE WHEN TRY_CAST(TRY_STRPTIME(nullif(trim({q}),''),'%m/%d/%Y') AS DATE) "
          ">= DATE '2013-01-01' THEN TRY_CAST(TRY_STRPTIME(nullif(trim({q}),''),'%m/%d/%Y') AS DATE) END")
  ```
- **State normalization (D5)** — `recipient_state` and `*_license_state_code*`: `upper(trim(...))`, map full state names→USPS via a small static dict, null non-US. Keep it a pure SQL `CASE`/`replace` chain in the projection (no row drops).
- **(D9 optional)** add `"form_of_payment_or_transfer_of_value"` to `FAMILIES["research"]["bitmap"]`.

### 5.2 Rebuild (per family, isolated)
```bash
modal run pipelines/cms_open_payments/ingest.py::backfill --only-family research
modal run pipelines/cms_open_payments/ingest.py::backfill --only-family ownership
```
Each: re-ingest → overwrite → reindex → hardened publish → verify. (Research is 3.4 GiB / 5.9M rows; ephemeral staging fine, no Volume needed.)

### 5.3 Phase 2 acceptance gate (O4)
```sql
-- via a DuckDB-over-lance probe (lance scanner filter, then count)
min(date_of_payment) >= 2013-01-01                                  -- research
count(*) where any associated_device_*_pdi_* = 'N/A' == 0           -- both
distinct recipient_state ⊆ (USPS ∪ {NULL})                          -- both
count_rows unchanged ± CMS revision; list_indices == EXPECTED       -- both
```

---

## 6. Phase 3 — Maintenance (OPTIONAL; after P0/P2 stable)

- **GC unreferenced files.** After several `overwrite` cycles, old data files accumulate (no-wipe by design). Add a `gc_family` entrypoint using `lance.dataset(uri, storage_options=so).cleanup_old_versions(older_than=timedelta(days=7))` — Lance's safe built-in. Run on a slow cadence, isolated from publish. *(This is the deferred half of the no-wipe trade.)*
- **Incremental quarterly refresh.** The scheduled `refresh_all` re-downloads all years. Optimize to skip years whose CMS `downloadURL` filename (carries publication date) is unchanged vs the last ledger row → re-ingest only new/revised years via `ingest_family_year`. Bounds the quarterly job from ~10 h to minutes in the common case.
- **Alerting.** Wire `core/ops_alert.py` to fire when a `refresh_all`/`verify` ledger row records `error` or `partial` (run #58 would have alerted). Gate on `phase IN ('publish','verify','refresh_all')`.

---

## 7. Sequencing, blast radius, change map

```
Phase 0 (code: publish hardening + verify gate + Volume + resume)  ── PR → merge → modal deploy
   │  test surface: ownership only (tiny, reversible). research/general untouched.
   ▼
Phase 1 (ops: restore general)  ── isolated to the general prefix; guarded clobber
   │  research/ownership untouched.
   ▼
Phase 2 (code+ops: hygiene transforms)  ── per-family overwrite; general untouched by these edits
   ▼
Phase 3 (optional maintenance)
```

| File | Change | Phase |
|---|---|---|
| `pipelines/cms_open_payments/ingest.py` | `_delete_prefix` (extract); `_upload_file_with_retry`; `_remote_index`; `_publish_dataset`; `_verify_published`; `EXPECTED_INDEX_COUNT`; delete `_replace_r2_prefix`; repoint 3 callers; Volume + `_local_ds(on_volume=)`; `resume`/`--clobber-broken` flags; lower general `ephemeral_disk` | 0 |
| `pipelines/cms_open_payments/ingest.py` | `_projection`/`_build_sql` sentinel + date + state hygiene; research bitmap parity | 2 |
| `pipelines/cms_open_payments/ingest.py` | `gc_family`; incremental refresh; ops_alert hook | 3 |
| `ops_cms_open_payments_runs.sql` | (no schema change — `phase` already free-text; `verify` rows fit) | — |

No changes outside `pipelines/cms_open_payments/`. `ops.cms_open_payments_runs` schema is unchanged (the `verify` phase reuses the existing `phase text` column).

---

## 8. Global rollback & safety

- **Phase 0** is code-only; revert the PR + redeploy to restore prior behavior. No data touched.
- **Phase 1** never destroys a valid dataset (D3 guard); worst case leaves the prefix in its already-broken state → re-run. The Volume retains progress.
- **Phase 2** is per-family `overwrite` through the no-wipe publish: the prior version remains readable until the new manifest commits + verifies; a failed rebuild leaves the previous good version live. To revert content, re-run `backfill --only-family <f>` off the prior transform (git revert the projection edit).
- **Invariant across all phases:** because the publish is no-wipe + manifest-last + verify-gated, **at no point does a partial failure produce an unreadable dataset** — the exact failure mode that destroyed general.

---

## 9. Explicitly out of scope (rejected with rationale — do not implement)

- **Hardcoded dead-column drops** — D6 (robustness invariant > ~0 disk; future-population risk).
- **Resolution-key clustering / `ORDER BY`** — D7 (negligible pruning at 7–83 fragments vs full quarterly re-sort of an 82M-row giant).
- **Type recasts** (`record_id`→int64, categorical→dictionary) — D8 (Lance already compresses; cross-consumer ripple).
- **Salvaging orphaned general fragments** — D4 (untrustworthy without a manifest; 12M rows absent).
- **Global `nullif(x,'N/A')`** — D5 (would corrupt legitimate free-text fields; targeted only).

---

### Appendix — quick reference
- Diagnostic: [`docs/cms_open_payments_structural_diagnostic.md`](../cms_open_payments_structural_diagnostic.md)
- Worker: `pipelines/cms_open_payments/ingest.py` · Ledger DDL: `pipelines/cms_open_payments/ops_cms_open_payments_runs.sql`
- Fleet rules: `ARCHITECTURE.md` ("Giants — Volume-staged, append-only"); `docs/reference/02_lancedb_storage.md`
- Registry index plan (source of `EXPECTED_INDEX_COUNT`): `FAMILIES` dict, `ingest.py:123`
- Probe stack reference (for verification scripts): `pylance 7.0.0`, `duckdb 1.5.3`, `boto3`, `doppler run --project core-x --config prd`
