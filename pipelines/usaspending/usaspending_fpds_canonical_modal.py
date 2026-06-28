"""USAspending FPDS CANONICAL — Modal GIANT runner (~107M rows).

This is the production execution harness that WRAPS the shipped, sample-proven merge
in ``pipelines/usaspending/usaspending_fpds_canonical.py``. The merge is NOT reimplemented:
``build_fn`` calls the shipped ``build()`` verbatim and ``verify_fn`` calls the shipped
``verify()`` verbatim. The ONLY net-new logic is ``index_fn`` — the Volume-staged,
append-only scalar index that replaces the shipped ``index()`` (which writes scalar
indices DIRECTLY to R2 and trips R2's "non-trailing parts must match" multipart rule at
107M, and sorts the BTREE in a bounded DataFusion pool that OOMs on a 100M+ row column).

WHY a wrapper and not edits to the shipped module:
  • ONE merge definition. The wrapper re-emits NO merge SQL and never calls the direct-R2
    shipped ``index()``. It imports ``build``/``verify``/``init_ops``/``COLUMN_SPEC``/
    ``BTREE_COLS``/``BITMAP_COLS``/``_s3``/``_r2_so``/``CANONICAL_URI`` from the shipped module.
  • env-before-import. The shipped module reads SCRATCH/DUCK_MEM/DUCK_TMP/DUCK_THREADS at
    IMPORT time (usaspending_fpds_canonical.py L85-88). Modal injects ``build_env``/``index_env``
    Secrets into ``os.environ`` BEFORE the function body runs, and the import of the shipped
    module happens INSIDE each function body — so the module-level reads resolve to Volume
    paths. There is deliberately NO top-level import of the canonical module in this wrapper.

═══════════════════════════════════════════════════════════════════════════════════════
RUN SEQUENCE (run as separate invocations — blast-radius split, design §6)
═══════════════════════════════════════════════════════════════════════════════════════

  # 0a) CHEAP smoke — validates packaging + secrets + import for pennies BEFORE the giant.
  modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::smoke_fn

  # 0b) one-time ops DDL. EITHER apply locally via doppler (preferred — no container spin-up):
  doppler run -p core-x -c prd -- python3 -m pipelines.usaspending.usaspending_fpds_canonical init_ops
  #     OR remotely (belt-and-suspenders; _record_run also self-bootstraps via to_regclass):
  modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::init_ops_main

  # 1) BUILD — full 107M merge → local Lance on the Volume → boto3 uniform-part publish.
  #    Prod build passes NO --since (full universe; --since is sample/debug ONLY).
  modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::build_fn

  # 2) INDEX — Volume-staged append-only BTREE/BITMAP. Runs AFTER build verifies clean.
  modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::index_fn

  # 3) VERIFY — read-back §5 assertions.
  modal run pipelines/usaspending/usaspending_fpds_canonical_modal.py::verify_fn

(Args: ``modal run …::build_fn --since 2025-10-01`` or ``--target-uri s3://…`` pass through
Modal's CLI. The bare function-target form above works because each is a plain ``@app.function``.)

═══════════════════════════════════════════════════════════════════════════════════════
SIZING (design §2)
═══════════════════════════════════════════════════════════════════════════════════════

  knob                          build_fn          index_fn          verify_fn
  ─────────────────────────────────────────────────────────────────────────────
  container memory              131072 (128 GiB)  49152 (48 GiB)    32768 (32 GiB)
  container cpu                 16.0              8.0               4.0
  timeout                       60*60*8 (8h)      60*60*4 (4h)      60*60 (1h)
  retries                       0                 0                 0
  Volume (400 GiB)              spill + stage     idx stage         verify_spill
  FPDS_CANONICAL_DUCKDB_MEM     64GB              —                 24GB
  FPDS_CANONICAL_DUCKDB_THREADS 8                 —                 4
  FPDS_CANONICAL_SCRATCH        /vol/.../stage    /vol/.../idx_stage —
  FPDS_CANONICAL_DUCKDB_TEMP_DIR /vol/.../spill   —                 /vol/.../verify_spill
  LANCE_BYPASS_SPILLING         true (harmless)   true (REQUIRED)   (module setdefault)

  NOTE verify_fn diverges from spec §3.3's "16 GiB, no Volume" because the SHIPPED verify()
  does NOT stream — it `CREATE TEMP TABLE c AS SELECT * FROM c_src` (full 107M materialize; the
  §5 checks multi-scan c). At the module-default 8GB memory_limit on a Volume-less box that spills
  to the small container root /tmp and crashes. 32 GiB box + 24GB DuckDB + Volume-backed spill.

═══════════════════════════════════════════════════════════════════════════════════════
d.8 / FLEET DISCIPLINES
═══════════════════════════════════════════════════════════════════════════════════════
  • retries=0 on EVERY function. A giant re-run is operator-initiated; overwrite idempotency
    only (design §6). No detach — the operator drives it foreground (``modal run``) so the
    Volume is never orphaned mid-write.
  • modal.Volume (network storage), NEVER ephemeral_disk. A large ephemeral_disk request
    forces the job onto preemptible spot capacity that gets killed and restarted repeatedly
    (usaspending_bulk.py giant lesson; ARCHITECTURE.md L143-149). The Volume does not push to spot.
  • index_fn uploads ONLY new index files (diff R2 key-set before/after). It NEVER wipes or
    re-uploads the ~50-80 GiB of data files. TWO FAIL-CLOSED gates run on the upload set BEFORE any
    byte is written: (1) no ``prefix+"data/"…*.lance`` in the diff (append-only-on-index holds),
    (2) every new file is in the {_indices/, _versions/, _transactions/} whitelist (design §6.5).
    The MUTABLE version pointer ``_versions/latest_version_hint.json`` (rewritten in place under the
    same key by create_scalar_index) is force-re-uploaded LAST so R2's hint never points at the old
    un-indexed manifest. Uploads are ordered: index payload → manifests → hint last.
  • Recurring rebuild: build_fn → index_fn → verify_fn after each FRESH advance. A build
    without a follow-up index leaves prod correct-but-unindexed. Full overwrite per run — the
    canonical is a reconciled read-model, never incrementally patched (whole-universe precedence).
"""

from __future__ import annotations

from pathlib import Path

import modal

# ── constants mirrored from the shipped module (must NOT drift) ─────────────────────────
BUCKET = "data-sink"
VOL_MOUNT = "/vol/fpds_canonical"
OPS_SQL_FILE = "ops_usaspending_fpds_canonical_runs.sql"

# Package dir = …/pipelines/usaspending. Source of the one co-located ops .sql shipped into the
# image (via add_local_file) at the same in-container path the .py lands at.
_PKG_DIR = Path(__file__).resolve().parent  # …/pipelines/usaspending

# ── fat image — same proven giant deps as usaspending_bulk.py (L197-205) ────────────────
# add_local_python_source("pipelines") ships the .py tree under /root/pipelines/… so
# `from pipelines.usaspending import usaspending_fpds_canonical` resolves. It ships .py ONLY
# (ignore=NON_PYTHON_FILES default). The co-located ops_*.sql is NOT a Python source, and
# _record_run/init_ops read it INSIDE the container via Path(fpds.__file__).parent / OPS_SQL_FILE
# — so the .sql MUST be shipped too. add_local_file overlays EXACTLY the one ops .sql this app
# needs, layered at the matching in-container path (surgical — does NOT ship the 11 unrelated
# ops_*.sql or sibling pipeline modules an add_local_dir of the whole package dir would).
#
# NOTE on namespace-package resolution: pipelines/usaspending/ has NO __init__.py (it is an
# implicit namespace sub-package; pipelines/ has one). add_local_python_source("pipelines")
# walks and ships the .py tree, and Python 3 resolves the implicit-namespace import fine; the
# add_local_file overlay does not create __init__.py and does not need to. This is validated
# EMPIRICALLY by smoke_fn (asserts both the in-container import AND .sql readability) — run it
# as a blocking gate before the giant (RUN SEQUENCE step 0a). Do NOT collapse the two image
# layers trusting one alone.
image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",       # to_arrow_reader; stays below the v2.0 break
        "lancedb>=0.15",
        "pylance>=7",           # provides `import lance`; lancedb does not re-export it
        "pyarrow>=17",
        "boto3>=1.35",          # R2 get/put/multipart
        "psycopg[binary]>=3.2",  # ops.* terminal state
    )
    .add_local_python_source("pipelines")  # .py tree → /root/pipelines/…
    .add_local_file(                        # overlay ONLY the one ops .sql, beside the .py
        str(_PKG_DIR / OPS_SQL_FILE),
        remote_path=f"/root/pipelines/usaspending/{OPS_SQL_FILE}",
    )
)

app = modal.App("usaspending-fpds-canonical", image=image)

# ── Volume — network storage, shared by build_fn (spill + stage) and index_fn (idx stage) ─
# 400 GiB ceiling (design §2.4): build peak = DuckDB spill (≤250) + local Lance stage (≤90).
# Each function wipes its staging sub-dir, so steady-state occupancy between runs is ~0.
vol = modal.Volume.from_name("fpds-canonical-vol", create_if_missing=True)

# ── env-knob Secrets — present in os.environ BEFORE the shipped module imports ───────────
# The shipped module reads SCRATCH/DUCK_TMP/DUCK_MEM/DUCK_THREADS at IMPORT time (L85-88).
build_env = modal.Secret.from_dict({
    "FPDS_CANONICAL_SCRATCH": f"{VOL_MOUNT}/stage",
    "FPDS_CANONICAL_DUCKDB_TEMP_DIR": f"{VOL_MOUNT}/duckdb_spill",
    "FPDS_CANONICAL_DUCKDB_MEM": "64GB",
    "FPDS_CANONICAL_DUCKDB_THREADS": "8",
    "LANCE_BYPASS_SPILLING": "true",
})
index_env = modal.Secret.from_dict({
    "FPDS_CANONICAL_SCRATCH": f"{VOL_MOUNT}/idx_stage",
    "LANCE_BYPASS_SPILLING": "true",  # REQUIRED — BTREE train sorts ~107M PK values in-RAM
})
# verify() runs `CREATE TEMP TABLE c AS SELECT * FROM c_src` — a FULL 107M-row × 75-col
# materialize (the six §5 checks multi-scan c, so it cannot stream). Without these knobs the
# shipped module's import-time defaults apply (memory_limit=8GB, temp_directory=/tmp,
# threads=4): an 8GB-limited 107M materialize spills hard to the container's ephemeral /tmp,
# which on a Volume-less box is the small root disk → DuckDB IO error / crash. Point DUCK_TMP
# at the Volume (room for the spill) and raise the DuckDB memory_limit under the bumped box.
verify_env = modal.Secret.from_dict({
    "FPDS_CANONICAL_DUCKDB_TEMP_DIR": f"{VOL_MOUNT}/verify_spill",
    "FPDS_CANONICAL_DUCKDB_MEM": "24GB",   # under the 32 GiB box; spill relief, not the driver
    "FPDS_CANONICAL_DUCKDB_THREADS": "4",
})


# ═══════════════════════════════════════════════════════════════════════════════════════
# smoke_fn — CHEAP packaging + secrets + import validation (run BEFORE the giant)
# ═══════════════════════════════════════════════════════════════════════════════════════
@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=120,
    retries=0,
)
def smoke_fn() -> dict:
    """Validate that the image packaging, the secrets, and the in-container import are all
    correct — for pennies, before committing to the multi-hour giant. Asserts:
      (a) the shipped module imports in-container;
      (b) len(COLUMN_SPEC) == 75 (the locked column contract);
      (c) the co-located ops .sql ships beside the .py and is readable in-container;
      (d) _r2_so() resolves to a dict with a non-empty endpoint + access key (creds present).
    """
    from pathlib import Path as _P

    # (a) import the shipped module IN-BODY.
    from pipelines.usaspending import usaspending_fpds_canonical as fpds

    # (b) column contract is exactly 75.
    n_cols = len(fpds.COLUMN_SPEC)
    assert n_cols == 75, f"COLUMN_SPEC has {n_cols} entries, expected 75"

    # (c) the co-located ops .sql shipped in the image and is readable beside fpds.__file__.
    sql_path = _P(fpds.__file__).parent / OPS_SQL_FILE
    sql_present = sql_path.exists()
    assert sql_present, f"co-located ops .sql NOT shipped in image: {sql_path}"

    # (d) R2 creds resolve to a usable client config.
    so = fpds._r2_so()
    assert isinstance(so, dict), f"_r2_so() returned {type(so)}, expected dict"
    endpoint_ok = bool(so.get("endpoint"))
    access_key_ok = bool(so.get("aws_access_key_id"))
    assert endpoint_ok, "R2 endpoint empty — r2-credentials secret not wired"
    assert access_key_ok, "R2 access key empty — r2-credentials secret not wired"

    return {
        "module_import_ok": True,
        "column_spec_len": n_cols,
        "column_spec_ok": n_cols == 75,
        "ops_sql_path": str(sql_path),
        "ops_sql_readable": sql_present,
        "r2_endpoint_present": endpoint_ok,
        "r2_access_key_present": access_key_ok,
        "btree_cols": len(fpds.BTREE_COLS),
        "bitmap_cols": len(fpds.BITMAP_COLS),
        "status": "ok",
    }


# ═══════════════════════════════════════════════════════════════════════════════════════
# build_fn — calls the shipped build() UNCHANGED (the proven merge, verbatim)
# ═══════════════════════════════════════════════════════════════════════════════════════
@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
        build_env,
    ],
    volumes={VOL_MOUNT: vol},
    memory=131072,  # 128 GiB — DuckDB memory_limit=64GB + Arrow drain + spill page cache slack
    cpu=16.0,       # the 107M per-key window-collapse is the compute hot path (DUCK_THREADS=8)
    timeout=60 * 60 * 8,  # 8h — deliberate slack over the multi-hour spilling build
    retries=0,            # d.8 — a giant re-run is operator-initiated
)
def build_fn(since: str | None = None, target_uri: str | None = None) -> dict:
    import os
    import shutil

    # Pre-create the Volume sub-dirs the shipped module's SCRATCH/DUCK_TMP point at, so a
    # stale prior run's tree does not collide (the module rmtrees SCRATCH in its finally).
    for sub in ("stage", "duckdb_spill"):
        os.makedirs(f"{VOL_MOUNT}/{sub}", exist_ok=True)

    # Import AFTER the build_env Secret is in os.environ (it is — it's a function arg), so the
    # module-level SCRATCH/DUCK_TMP/DUCK_MEM/DUCK_THREADS reads (L85-88) resolve to Volume paths.
    from pipelines.usaspending import usaspending_fpds_canonical as fpds

    uri = target_uri or fpds.CANONICAL_URI
    try:
        metrics = fpds.build(since=since, target_uri=uri)  # the proven merge — NOTHING re-emitted
    finally:
        # build() rmtrees SCRATCH (the stage) in its OWN finally, but the DuckDB spill dir is NOT
        # under SCRATCH and the module never cleans it → it would accrue across runs. Publish is
        # to R2 via boto3 (NOT the Volume), so there is nothing durable to vol.commit() here; the
        # only Volume residue is the spill, which we wipe so steady-state occupancy returns to ~0.
        shutil.rmtree(f"{VOL_MOUNT}/duckdb_spill", ignore_errors=True)
    return metrics


# ═══════════════════════════════════════════════════════════════════════════════════════
# index_fn — Volume-staged APPEND-ONLY scalar index (the ONLY net-new logic, design §3.2)
# ═══════════════════════════════════════════════════════════════════════════════════════
@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        index_env,  # LANCE_BYPASS_SPILLING=true + SCRATCH on the Volume
    ],
    volumes={VOL_MOUNT: vol},
    memory=49152,  # 48 GiB — in-RAM BTREE sort of the ~107M VARCHAR PK (32-64 GiB fleet rule)
    cpu=8.0,
    timeout=60 * 60 * 4,  # 4h
    retries=0,
)
def index_fn(target_uri: str | None = None) -> dict:
    """Mirror usaspending_bulk.build_table_indexes (download R2 → local → create_scalar_index →
    upload) with three giant-correct deltas: (a) the local FS is a Volume (no spot); (b) it uploads
    ONLY new index files via a before/after R2 key-set diff — NEVER re-uploading data files; (c) it
    force-re-uploads the MUTABLE version pointer (_versions/latest_version_hint.json) LAST, because
    create_scalar_index rewrites its content in place under the SAME key (so the pure set-diff would
    skip it and leave R2's hint pointing at the OLD un-indexed manifest — a stale-version corruption).

    Same column plan as the shipped index(): BTREE_COLS then BITMAP_COLS, same order, same split.

    FAIL-CLOSED, computed on the upload set BEFORE a single byte is written:
      1. NO prefix+"data/"…*.lance in the diff (append-only-on-index assumption holds), and
      2. every NEW file is in the {_indices/, _versions/, _transactions/} whitelist (design §6.5).
    Uploads are ORDERED so a reader never resolves a pointer to a not-yet-present referent:
    _indices/** + _transactions/** first, then _versions/*.manifest, then the hint LAST.
    Lance commits ONE manifest PER create_scalar_index → the 16-column plan emits ~16 new manifests
    (NOT one); the post-upload assertion is a >=1 lower bound, never an exact count.
    """
    import os
    import shutil

    import lance

    from pipelines.usaspending import usaspending_fpds_canonical as fpds

    s3 = fpds._s3()  # same R2 client (retries=10, checksum-when-required) the publish path uses
    uri = target_uri or fpds.CANONICAL_URI
    prefix = uri.replace(f"s3://{BUCKET}/", "")
    local_ds = f"{VOL_MOUNT}/idx_stage/canonical_lance"
    os.makedirs(f"{VOL_MOUNT}/idx_stage", exist_ok=True)
    shutil.rmtree(local_ds, ignore_errors=True)

    paginator = s3.get_paginator("list_objects_v2")

    # 1) Snapshot the R2 key set BEFORE indexing (the diff baseline → "new files only").
    before: set[str] = set()
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            before.add(o["Key"])
    if not before:
        return {"status": "dataset_not_found", "prefix": prefix}

    # 2) Download the published dataset to the Volume (local FS has no R2 multipart rule).
    staged = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            key = o["Key"]
            dst = os.path.join(local_ds, key[len(prefix):])
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            s3.download_file(BUCKET, key, dst)
            staged += 1
    if staged == 0:
        return {"status": "dataset_not_found", "prefix": prefix}

    # 3) Build BTREE + BITMAP on the LOCAL copy, in-RAM sort (LANCE_BYPASS_SPILLING=true).
    #    SAME plan as the shipped index(): import BTREE_COLS/BITMAP_COLS, presence-filter, same order.
    ds = lance.dataset(local_ds)
    rows = ds.count_rows()
    present = set(ds.schema.names)
    built: list[str] = []
    for col in [c for c in fpds.BTREE_COLS if c in present]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
        except TypeError:  # pylance version drift — older signature has no replace=
            ds.create_scalar_index(col, index_type="BTREE")
        built.append(col)
        print(f"  BTREE  ✓ {col}", flush=True)
    for col in [c for c in fpds.BITMAP_COLS if c in present]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        except TypeError:
            ds.create_scalar_index(col, index_type="BITMAP")
        built.append(col)
        print(f"  BITMAP ✓ {col}", flush=True)

    # 4) Compute the upload set against the before/after key diff, with ONE exception that the
    #    pure set-difference cannot see: _versions/latest_version_hint.json is the dataset's
    #    MUTABLE version pointer — create_scalar_index rewrites its CONTENT in place
    #    ({"version":N} → {"version":N+k}) but keeps the SAME R2 key, so `key in before` is True
    #    and a naive diff SKIPS it. Skipping it leaves R2's hint pointing at the OLD (un-indexed)
    #    manifest → readers resolve to the pre-index version and never see the new indices
    #    (verified empirically: pylance 7 v2.1 layout). So it is force-included as a mutable
    #    pointer and uploaded LAST. (rel == hint key is excluded from the data-file scan below.)
    HINT_REL = "_versions/latest_version_hint.json"
    upload_set: list[tuple[str, str]] = []  # (local_path, r2_key)
    hint_entry: tuple[str, str] | None = None
    for root, _dirs, files in os.walk(local_ds):
        for f in files:
            lp = os.path.join(root, f)
            rel = os.path.relpath(lp, local_ds).replace(os.sep, "/")
            key = prefix + rel
            if rel == HINT_REL:
                hint_entry = (lp, key)  # mutable pointer — always re-upload, ordered LAST
                continue
            if key in before:
                continue  # pre-existing, byte-stable data/manifest file — do NOT re-upload
            upload_set.append((lp, key))

    # FAIL-CLOSED #1 — append-only-on-index guard. A data file in the diff means Lance rewrote a
    # data fragment (assumption BROKEN). Anchor to the dataset's own data/ subdir (prefix+"data/"),
    # NOT a bare "/data/" substring, so an unrelated path segment can never false-match/miss.
    # Raise BEFORE any upload — never commit a manifest that would reference re-written data.
    data_in_diff = [
        key for _lp, key in upload_set
        if key.startswith(prefix + "data/") and key.endswith(".lance")
    ]
    if data_in_diff:
        shutil.rmtree(local_ds, ignore_errors=True)
        raise RuntimeError(
            "index_fn FAIL-CLOSED: data/*.lance files appear in the index upload diff — "
            "the append-only-on-index assumption is BROKEN; refusing to upload. "
            f"offending keys (first 5): {data_in_diff[:5]}"
        )

    # FAIL-CLOSED #2 — positive whitelist (design §6.5). Every NEW file MUST be an index payload,
    # a new manifest, or a new transaction. Anything else (a stray data file the prefix-anchored
    # check above could miss, or any unexpected class) trips this. Computed BEFORE upload.
    ALLOWED_PREFIXES = ("_indices/", "_versions/", "_transactions/")
    rogue = [
        key for _lp, key in upload_set
        if not any(key[len(prefix):].startswith(p) for p in ALLOWED_PREFIXES)
    ]
    if rogue:
        shutil.rmtree(local_ds, ignore_errors=True)
        raise RuntimeError(
            "index_fn FAIL-CLOSED: upload diff contains files outside the append-only index "
            f"whitelist {ALLOWED_PREFIXES}; refusing to upload. offending (first 5): {rogue[:5]}"
        )

    # Upload ORDERED so a reader never resolves a pointer to a not-yet-present referent:
    #   (1) _indices/** payload + _transactions/*.txn (the index data + commit log),
    #   (2) _versions/*.manifest (commit points that reference the _indices payload),
    #   (3) latest_version_hint.json LAST (the pointer-of-record; readers consult it to pick the
    #       latest manifest, which must already be on R2 with its _indices payload).
    def _rank(key: str) -> int:
        rel = key[len(prefix):]
        if rel.startswith("_versions/") and rel.endswith(".manifest"):
            return 1  # manifest after its index payload
        return 0      # _indices/** and _transactions/** first

    ordered = sorted(upload_set, key=lambda lp_key: _rank(lp_key[1]))

    uploaded: list[str] = []
    for lp, key in ordered:
        s3.upload_file(lp, BUCKET, key)
        uploaded.append(key[len(prefix):])
    # The mutable version hint LAST — only after every manifest + index payload it can point at.
    if hint_entry is not None:
        s3.upload_file(hint_entry[0], BUCKET, hint_entry[1])
        uploaded.append(hint_entry[1][len(prefix):])

    shutil.rmtree(local_ds, ignore_errors=True)

    # Post-upload structural assertions. A real index run adds _indices/ entries and >=1 new
    # _versions/*.manifest (Lance commits ONE manifest PER create_scalar_index, so the 16-column
    # plan emits ~16 — NOT exactly one; a hard `== 1` would crash after a fully-successful upload).
    indices_uploaded = [u for u in uploaded if u.startswith("_indices/")]
    new_manifests = sorted(u for u in uploaded if u.startswith("_versions/") and u.endswith(".manifest"))
    assert indices_uploaded, (
        f"index_fn produced NO _indices/ files in the upload set ({len(uploaded)} files "
        f"uploaded) — index build did not commit. uploaded(first 10)={uploaded[:10]}"
    )
    assert new_manifests, (
        f"index_fn produced NO new _versions/*.manifest ({len(uploaded)} files uploaded) — "
        f"index build did not commit a new version. uploaded(first 10)={uploaded[:10]}"
    )

    return {
        "target_uri": uri,
        "rows": int(rows),
        "indices_built": built,
        "n_indices_built": len(built),
        "files_uploaded": uploaded,
        "n_uploaded": len(uploaded),
        "new_manifests": new_manifests,
        "n_new_manifests": len(new_manifests),
        "version_hint_reuploaded": hint_entry is not None,
        "status": "ok",
    }


# ═══════════════════════════════════════════════════════════════════════════════════════
# verify_fn — calls the shipped verify() UNCHANGED (read-back §5 assertions)
# ═══════════════════════════════════════════════════════════════════════════════════════
@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        verify_env,  # DUCK_MEM=24GB + DUCK_TMP on the Volume — verify() materializes 107M rows
    ],
    volumes={VOL_MOUNT: vol},  # spill room for the 107M TEMP TABLE materialize (NOT container /tmp)
    memory=32768,  # 32 GiB — shipped verify() does CREATE TEMP TABLE c (full 107M materialize),
                   # NOT a stream; the §5 checks multi-scan c. 16 GiB + 8GB DuckDB on root /tmp crashes.
    cpu=4.0,
    timeout=60 * 60,  # 1h
    retries=0,
)
def verify_fn(target_uri: str | None = None) -> dict:
    import os

    # The shipped _duck() makedirs DUCK_TMP, but pre-create on the mount for clarity.
    os.makedirs(f"{VOL_MOUNT}/verify_spill", exist_ok=True)

    # Import AFTER verify_env is in os.environ so the module-level DUCK_MEM/DUCK_TMP reads
    # (usaspending_fpds_canonical.py L86-88) resolve to the Volume-backed spill + raised mem.
    from pipelines.usaspending import usaspending_fpds_canonical as fpds

    return fpds.verify(target_uri=target_uri or fpds.CANONICAL_URI)


# ═══════════════════════════════════════════════════════════════════════════════════════
# init_ops_fn / init_ops_main — remote ops DDL bootstrap (belt-and-suspenders;
# _record_run also self-bootstraps via to_regclass on first build).
# ═══════════════════════════════════════════════════════════════════════════════════════
@app.function(
    image=image,
    secrets=[modal.Secret.from_name("hqx-postgres")],
    timeout=120,
    retries=0,
)
def init_ops_fn() -> dict:
    """Apply the ops.usaspending_fpds_canonical_runs DDL by calling the shipped init_ops()
    in-container (it reads the co-located .sql shipped beside the .py — same path smoke_fn checks)."""
    from pipelines.usaspending import usaspending_fpds_canonical as fpds

    fpds.init_ops()
    return {"applied": True}


# ═══════════════════════════════════════════════════════════════════════════════════════
# Local entrypoints — so `modal run <file>::<name>` drives the remote functions.
# (The @app.function targets above are ALSO directly runnable as `modal run …::build_fn`;
#  these local entrypoints are the explicit, documented drivers.)
# ═══════════════════════════════════════════════════════════════════════════════════════
@app.local_entrypoint()
def init_ops_main() -> None:
    """One-time ops DDL via the remote init_ops_fn. (Preferred: apply locally via doppler —
    see module docstring — to avoid a container spin-up.)"""
    import json

    print(json.dumps(init_ops_fn.remote(), indent=2, default=str))


@app.local_entrypoint()
def smoke() -> None:
    """Cheap packaging/secrets/import validation before the giant."""
    import json

    print(json.dumps(smoke_fn.remote(), indent=2, default=str))


@app.local_entrypoint()
def build(since: str = "", target_uri: str = "") -> None:
    """Full 107M merge → local Lance on the Volume → boto3 publish. Prod build passes NO --since."""
    import json

    print(json.dumps(
        build_fn.remote(since=since or None, target_uri=target_uri or None),
        indent=2, default=str,
    ))


@app.local_entrypoint()
def index(target_uri: str = "") -> None:
    """Volume-staged append-only BTREE/BITMAP index. Runs AFTER build verifies clean."""
    import json

    print(json.dumps(index_fn.remote(target_uri=target_uri or None), indent=2, default=str))


@app.local_entrypoint()
def verify(target_uri: str = "") -> None:
    """Read-back §5 assertions."""
    import json

    print(json.dumps(verify_fn.remote(target_uri=target_uri or None), indent=2, default=str))
