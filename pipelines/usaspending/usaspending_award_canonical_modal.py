"""USAspending AWARD CANONICAL — Modal GIANT runner (~30.7M rows, 393-col award-summary spine).

This is the production execution harness that WRAPS the shipped, sample-proven merge in
``pipelines/usaspending/usaspending_award_canonical.py``. The merge is NOT reimplemented:
``build_fn`` calls the shipped ``build()`` verbatim and ``verify_fn`` calls the shipped
``verify()`` verbatim. The ONLY net-new logic is ``index_fn`` — the /tmp-staged, append-only
scalar index that replaces the shipped ``index()`` (which writes scalar indices DIRECTLY to R2 and
trips R2's "non-trailing parts must match" multipart rule at ~30M, and sorts the BTREE in a bounded
DataFusion pool that OOMs on a 30M+ row column).

WHY a wrapper and not edits to the shipped module:
  • ONE merge definition. The wrapper re-emits NO merge SQL and never calls the direct-R2 shipped
    ``index()``. It imports ``build``/``verify``/``init_ops``/``COLUMN_SPEC``/``BTREE_COLS``/
    ``BITMAP_COLS``/``_s3``/``_r2_so``/``CANONICAL_URI`` (+ the stage SQL builders for the smoke
    cross-toggle check) from the shipped module.
  • env-before-import. The shipped module reads SCRATCH/DUCK_MEM/DUCK_TMP/DUCK_THREADS at IMPORT
    time. Modal injects ``build_env``/``index_env``/``verify_env`` Secrets into ``os.environ`` BEFORE
    the function body runs, and the import of the shipped module happens INSIDE each function body —
    so the module-level reads resolve to the /tmp paths. There is deliberately NO top-level import of
    the canonical module in this wrapper.

═══════════════════════════════════════════════════════════════════════════════════════
RUN SEQUENCE (run as separate invocations — blast-radius split)
═══════════════════════════════════════════════════════════════════════════════════════
Drive via the LOCAL ENTRYPOINTS (``::smoke``/``::build``/``::index``/``::verify``/``::init_ops_main``),
NOT the bare ``::*_fn`` targets — the entrypoints coerce ``--since ""`` → ``None`` and parse
``--include-fresh`` via an explicit truthy check (a bare ``::build_fn`` with ``--since ""`` would
inject ``last_modified_date >= TIMESTAMP ''`` → SQL error; ``bool("false")`` is ``True`` → inverted
intent).

  # 0a) CHEAP smoke — packaging + secrets + import + col-count(393) + cross-toggle schema identity.
  modal run pipelines/usaspending/usaspending_award_canonical_modal.py::smoke

  # 0b) one-time ops DDL via doppler — MANDATORY pre-step (do NOT rely on _record_run's self-bootstrap:
  #     two concurrent first-run CREATEs can deadlock; pre-create the table once):
  doppler run -p core-x -c prd -- python3 -m pipelines.usaspending.usaspending_award_canonical init_ops

  # 1) BULK-only FIRST LANDING (FRESH-independent — lands the 30.7M spine without the fresh flip):
  modal run --detach pipelines/usaspending/usaspending_award_canonical_modal.py::build --include-fresh false

  # 2) INDEX — /tmp-staged append-only BTREE/BITMAP. Runs AFTER build verifies clean.
  modal run pipelines/usaspending/usaspending_award_canonical_modal.py::index

  # 3) VERIFY — read-back assertions. After a BULK-only build pass --include-fresh false (or omit it:
  #     verify_fn infers the mode from the latest status='success' ledger row). An explicit flag wins.
  modal run pipelines/usaspending/usaspending_award_canonical_modal.py::verify --include-fresh false

  # 4) RECONCILE-LATER FLIP (once FRESH landed + verified) — default include_fresh=True:
  modal run --detach pipelines/usaspending/usaspending_award_canonical_modal.py::build
  modal run pipelines/usaspending/usaspending_award_canonical_modal.py::index
  modal run pipelines/usaspending/usaspending_award_canonical_modal.py::verify

═══════════════════════════════════════════════════════════════════════════════════════
SIZING — corrected against the SHIPPED FPDS reference (192 GiB + 1.5 TiB ephemeral_disk;
"512 GiB floor exhausted by the 392-col stage-2 merge spill"), NOT down-extrapolated from row count.
This build is WIDER (~393 cols) but 0.28× the FPDS row count; the spill footprint scales with column
width, so the 30.7M row count does NOT license 64 GiB / 512 GiB (that reproduces the exact ENOSPC).
═══════════════════════════════════════════════════════════════════════════════════════

  knob                            build_fn                 index_fn                 verify_fn
  ─────────────────────────────────────────────────────────────────────────────────────────
  container memory                131072 (128 GiB)         49152 (48 GiB)           32768 (32 GiB)
  ephemeral_disk                  1_048_576 (1 TiB)        1_048_576 (1 TiB)        —
  container cpu                   16.0                     8.0                      4.0
  timeout                         60*60*6 (6h)             60*60*3 (3h)             60*60 (1h)
  retries                         0                        0                        0
  max_containers                  1 (HIGH-1 guard)         1 (HIGH-1 guard)         —
  AWARD_CANONICAL_DUCKDB_MEM      88GB                     —                        24GB
  AWARD_CANONICAL_DUCKDB_THREADS  16                       —                        4
  AWARD_CANONICAL_SCRATCH         $ROOT/stage              $ROOT/idx_stage          —
  AWARD_CANONICAL_DUCKDB_TEMP_DIR $ROOT/duckdb_spill       —                        $ROOT/verify_spill
  LANCE_BYPASS_SPILLING           true                     true (REQUIRED)          (module setdefault)

  ephemeral_disk=1 TiB provisioned DEFENSIVELY (above the failed 512 GiB ceiling, below FPDS's
  1.5 TiB): at ~393 cols three wide ~30.4M-row DuckDB intermediates co-reside (bulk_latest +
  core_union [UNION doubles the BULK core] + core_winner co-resident through the enrich JOIN). STEP 12
  measures the actual sample-build spill+stage footprint; if the sample extrapolation exceeds 1 TiB,
  raise build_fn/index_fn ephemeral_disk to 1.5 TiB (match FPDS) BEFORE the giant.

  verify_fn at 32 GiB / 24GB DUCK_MEM matches FPDS verify exactly — the SHIPPED verify() does NOT
  stream, it CREATE TEMP TABLE c AS SELECT * FROM c_src (a full 30.4M × 393-col VARCHAR-heavy
  materialize; the checks multi-scan c). 8GB module-default spills hard and crashes → 32 GiB is the
  floor, not a down-size.

═══════════════════════════════════════════════════════════════════════════════════════
d.8 / FLEET DISCIPLINES
═══════════════════════════════════════════════════════════════════════════════════════
  • retries=0 on EVERY function. A giant re-run is operator-initiated; overwrite idempotency only.
  • Spill + stage on the 1 TiB ephemeral_disk (/tmp) — NO modal.Volume (a network-backed Volume
    background-commits a high-churn spill dir every few seconds → risks the 6 h build timeout).
  • COMPLETION SENTINEL is two-source AND, not the ledger alone. The ops-ledger row is written only in
    build()'s finally:, which an OOM SIGKILL (or spot reap) SKIPS — so a killed run writes NO row and a
    ledger-only poller hangs (or reads a STALE prior 'success'). Decide completion on Modal app state
    (`modal app list`) AND a fresh `status='success'` ledger row: app stopped + fresh success = PASS;
    app stopped + NO fresh row = OOM/reap FAIL, re-run.
  • max_containers=1 on build_fn + index_fn — never two simultaneously on the same R2 prefix.
  • index_fn uploads ONLY new index files (before/after R2 key-set diff). It NEVER wipes or re-uploads
    the data files. TWO FAIL-CLOSED gates run on the upload set BEFORE any byte: (1) no
    ``prefix+"data/"…*.lance`` in the diff, (2) every new file is in the {_indices/, _versions/,
    _transactions/} whitelist. The MUTABLE ``_versions/latest_version_hint.json`` is force-re-uploaded
    LAST so R2's hint never points at the old un-indexed manifest. Uploads ordered: index payload →
    manifests → hint last.
  • NO Trigger.dev schedule/cron (NON-GOAL). Recurring rebuild is operator-initiated
    build_fn → index_fn → verify_fn after each FRESH advance. Full overwrite per run.
"""

from __future__ import annotations

from pathlib import Path

import modal

# ── constants mirrored from the shipped module (must NOT drift) ─────────────────────────
BUCKET = "data-sink"
SCRATCH_ROOT = "/tmp/award_canonical"  # spill + stage on the 1 TiB ephemeral_disk
OPS_SQL_FILE = "ops_usaspending_award_canonical_runs.sql"
COLUMN_SPEC_LEN = 393  # baked constant — asserted in smoke_fn (matches the shipped COLUMN_SPEC + DDL)

# Package dir = …/pipelines/usaspending. Source of the one co-located ops .sql shipped into the image.
_PKG_DIR = Path(__file__).resolve().parent

# ── fat image — same proven giant deps as the FPDS runner ────────────────────────────────
# add_local_python_source("pipelines") ships the .py tree under /root/pipelines/… (implicit-namespace
# import resolves). It ships .py ONLY; the co-located ops .sql is overlaid via add_local_file at the
# matching in-container path (surgical — does NOT ship the unrelated ops_*.sql or sibling modules).
# Validated EMPIRICALLY by smoke_fn (asserts both the in-container import AND .sql readability).
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

app = modal.App("usaspending-award-canonical", image=image)

# ── env-knob Secrets — present in os.environ BEFORE the shipped module imports ───────────
build_env = modal.Secret.from_dict({
    "AWARD_CANONICAL_SCRATCH": f"{SCRATCH_ROOT}/stage",
    "AWARD_CANONICAL_DUCKDB_TEMP_DIR": f"{SCRATCH_ROOT}/duckdb_spill",
    "AWARD_CANONICAL_DUCKDB_MEM": "88GB",   # 88GB on the 128 GiB box: less spill, finishes in time
    "AWARD_CANONICAL_DUCKDB_THREADS": "16",
    "LANCE_BYPASS_SPILLING": "true",
})
index_env = modal.Secret.from_dict({
    "AWARD_CANONICAL_SCRATCH": f"{SCRATCH_ROOT}/idx_stage",
    "LANCE_BYPASS_SPILLING": "true",  # REQUIRED — BTREE train sorts ~30.7M PK values in-RAM
})
verify_env = modal.Secret.from_dict({
    "AWARD_CANONICAL_DUCKDB_TEMP_DIR": f"{SCRATCH_ROOT}/verify_spill",
    "AWARD_CANONICAL_DUCKDB_MEM": "24GB",   # under the 32 GiB box; spill relief for the full materialize
    "AWARD_CANONICAL_DUCKDB_THREADS": "4",
})


# ═══════════════════════════════════════════════════════════════════════════════════════
# smoke_fn — CHEAP packaging + secrets + import + col-count + CROSS-TOGGLE schema identity
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
    """Validate — for pennies, before the multi-hour giant — that:
      (a) the shipped module imports in-container;
      (b) len(COLUMN_SPEC) == 393 (the locked column contract);
      (c) the co-located ops .sql ships beside the .py and is readable in-container;
      (d) _r2_so() resolves to a dict with a non-empty endpoint + access key (creds present);
      (e) CROSS-TOGGLE schema identity (P0-6): build the stage-1 SQL under BOTH include_fresh=True and
          False, DESCRIBE fresh_latest each, assert names+types byte-identical. This is the ONLY
          guarantee that the reconcile-later flip is schema-safe (the per-build
          _assert_collapse_schema_identity runs WITHIN one build, not across the toggle). Runs entirely
          in an in-memory DuckDB over 0-row typed stubs — NO R2, no scan.
    """
    from pathlib import Path as _P

    import duckdb

    # (a) import the shipped module IN-BODY.
    from pipelines.usaspending import usaspending_award_canonical as aw

    # (b) column contract is exactly 393.
    n_cols = len(aw.COLUMN_SPEC)
    assert n_cols == COLUMN_SPEC_LEN, f"COLUMN_SPEC has {n_cols} entries, expected {COLUMN_SPEC_LEN}"

    # (c) the co-located ops .sql shipped in the image and is readable beside aw.__file__.
    sql_path = _P(aw.__file__).parent / OPS_SQL_FILE
    sql_present = sql_path.exists()
    assert sql_present, f"co-located ops .sql NOT shipped in image: {sql_path}"

    # (d) R2 creds resolve to a usable client config.
    so = aw._r2_so()
    assert isinstance(so, dict), f"_r2_so() returned {type(so)}, expected dict"
    endpoint_ok = bool(so.get("endpoint"))
    access_key_ok = bool(so.get("aws_access_key_id"))
    assert endpoint_ok, "R2 endpoint empty — r2-credentials secret not wired"
    assert access_key_ok, "R2 access key empty — r2-credentials secret not wired"

    # (e) CROSS-TOGGLE schema identity. Build the FULL stage-1 SQL both ways over 0-row typed stubs and
    #     DESCRIBE fresh_latest each. The include_fresh=True path scans fresh_r; the False path scans
    #     range(0). bulk_r MUST be typed to the real matview types (its DUAL-core bulk_exprs are bare
    #     native refs inside abs()/CASE — an all-VARCHAR bulk_r would bind-fail on abs(VARCHAR)); FRESH +
    #     parent are all-VARCHAR (their exprs wrap in s()/TRY_CAST, so the DESCRIBE'd fresh_latest types
    #     are driven by COLUMN_SPEC duck_type, not the stub). BULK bare-ref cols are typed from their
    #     canonical duck_type; cols referenced only inside CASE/abs/CAST get a numeric/date/timestamp hint.
    import re as _re

    built_at_iso = "2026-07-04 00:00:00.000000"
    bulk_cols = list(aw._source_cols("bulk"))
    fresh_cols = list(aw._source_cols("feed"))
    parent_cols = list(aw._source_cols("parent"))

    _bulk_type: dict[str, str] = {}
    for _c in aw.COLUMN_SPEC:
        _be = _c["bulk_expr"]
        if _be and _re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", _be.strip()):
            _bulk_type[_be.strip()] = _c["duck_type"]  # bare native ref → its canonical duck_type
    _bulk_type.update({  # cols appearing only inside abs()/CASE/CAST — realistic native types
        "total_obligation": "DOUBLE", "base_and_all_options_value": "DOUBLE",
        "officer_1_amount": "DOUBLE", "officer_2_amount": "DOUBLE", "officer_3_amount": "DOUBLE",
        "officer_4_amount": "DOUBLE", "officer_5_amount": "DOUBLE",
        "last_modified_date": "TIMESTAMP", "ingested_at": "TIMESTAMP",
        "action_date": "DATE", "date_signed": "DATE", "ordering_period_end_date": "DATE",
        "period_of_performance_current_end_date": "DATE", "period_of_performance_start_date": "DATE",
    })

    def _stub_varchar(con, name, cols):
        sel = ", ".join(f'CAST(NULL AS VARCHAR) AS "{c}"' for c in cols) or "1 AS _x"
        con.execute(f"CREATE TEMP TABLE {name} AS SELECT {sel}")

    def _stub_bulk(con, cols):
        sel = ", ".join(f'CAST(NULL AS {_bulk_type.get(c, "VARCHAR")}) AS "{c}"' for c in cols) or "1 AS _x"
        con.execute(f"CREATE TEMP TABLE bulk_r AS SELECT {sel}")

    def _describe_fresh_latest(include_fresh: bool):
        con = duckdb.connect(":memory:")
        _stub_bulk(con, bulk_cols)
        _stub_varchar(con, "parent_r", parent_cols)
        if include_fresh:
            _stub_varchar(con, "fresh_r", fresh_cols)
        con.execute(aw._stage1_sql(built_at_iso, include_fresh=include_fresh))
        rows = con.execute("DESCRIBE fresh_latest").fetchall()
        con.close()
        return [(r[0], r[1]) for r in rows]

    sig_true = _describe_fresh_latest(True)
    sig_false = _describe_fresh_latest(False)
    cross_toggle_ok = sig_true == sig_false
    assert cross_toggle_ok, (
        "CROSS-TOGGLE schema mismatch: fresh_latest differs between include_fresh True/False. "
        f"first divergences: {[(a, b) for a, b in zip(sig_true, sig_false) if a != b][:5]}"
    )

    return {
        "module_import_ok": True,
        "column_spec_len": n_cols,
        "column_spec_ok": n_cols == COLUMN_SPEC_LEN,
        "ops_sql_path": str(sql_path),
        "ops_sql_readable": sql_present,
        "r2_endpoint_present": endpoint_ok,
        "r2_access_key_present": access_key_ok,
        "cross_toggle_schema_identity_ok": cross_toggle_ok,
        "fresh_latest_columns": len(sig_true),
        "btree_cols": len(aw.BTREE_COLS),
        "bitmap_cols": len(aw.BITMAP_COLS),
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
    memory=196608,          # 192 GiB — matches the FPDS giant precedent (plan STEP 10); DuckDB memory_limit=88GB,
    #                          the extra ~104 GiB is Arrow-drain + spill page-cache + OOM-SIGKILL protection slack.
    #                          Down-size only if the STEP-12 sample extrapolation proves the merge fits smaller.
    cpu=16.0,               # the ~30.4M per-gua window-collapse is the compute hot path (THREADS=16)
    timeout=60 * 60 * 6,    # 6h — deliberate slack over the multi-hour spilling build
    ephemeral_disk=1_048_576,  # 1 TiB — above the failed 512 GiB ceiling; STEP 12 raises to 1.5 TiB if
    #                            the sample spill+stage extrapolation exceeds it
    retries=0,              # d.8 — a giant re-run is operator-initiated
    max_containers=1,       # double-launch guard (HIGH-1) — never two builds on the same R2 prefix
)
def build_fn(since: str | None = None, target_uri: str | None = None,
             include_fresh: bool = True) -> dict:
    import os
    import shutil

    # Pre-create the /tmp sub-dirs the shipped module's SCRATCH/DUCK_TMP point at.
    for sub in ("stage", "duckdb_spill"):
        os.makedirs(f"{SCRATCH_ROOT}/{sub}", exist_ok=True)

    # Import AFTER build_env is in os.environ so the module-level SCRATCH/DUCK_* reads resolve to /tmp.
    from pipelines.usaspending import usaspending_award_canonical as aw

    uri = target_uri or aw.CANONICAL_URI
    try:
        metrics = aw.build(since=since, target_uri=uri, include_fresh=include_fresh)
    finally:
        # build() rmtrees SCRATCH in its own finally, but the DuckDB spill dir is NOT under SCRATCH →
        # wipe it so steady-state /tmp occupancy returns to ~0.
        shutil.rmtree(f"{SCRATCH_ROOT}/duckdb_spill", ignore_errors=True)
    return metrics


# ═══════════════════════════════════════════════════════════════════════════════════════
# index_fn — /tmp-staged APPEND-ONLY scalar index (the ONLY net-new logic)
# ═══════════════════════════════════════════════════════════════════════════════════════
@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        index_env,  # LANCE_BYPASS_SPILLING=true + SCRATCH on the local /tmp disk
    ],
    memory=49152,           # 48 GiB — in-RAM BTREE sort of the ~30.7M VARCHAR PK
    cpu=8.0,
    timeout=60 * 60 * 3,    # 3h
    ephemeral_disk=1_048_576,  # 1 TiB — the ~50-90 GiB staged dataset mirror + emitted index files
    retries=0,
    max_containers=1,       # double-launch guard (HIGH-1) — never two index runs on the same prefix
)
def index_fn(target_uri: str | None = None) -> dict:
    """Download R2 → local → create_scalar_index → upload ONLY new index files (before/after R2 key-set
    diff), NEVER re-uploading data files, force-re-uploading the MUTABLE version pointer
    (_versions/latest_version_hint.json) LAST (create_scalar_index rewrites its content in place under
    the SAME key, so a pure set-diff would skip it and leave R2's hint pointing at the OLD un-indexed
    manifest).

    Same column plan as the shipped index(): BTREE_COLS then BITMAP_COLS, same order, same split.

    FAIL-CLOSED, computed on the upload set BEFORE a single byte is written:
      1. NO prefix+"data/"…*.lance in the diff (append-only-on-index assumption holds), and
      2. every NEW file is in the {_indices/, _versions/, _transactions/} whitelist.
    Uploads are ORDERED so a reader never resolves a pointer to a not-yet-present referent:
    _indices/** + _transactions/** first, then _versions/*.manifest, then the hint LAST.
    Lance commits ONE manifest PER create_scalar_index → the post-upload assertion is a >=1 lower
    bound, never an exact count.
    """
    import os
    import shutil

    import lance

    from pipelines.usaspending import usaspending_award_canonical as aw

    s3 = aw._s3()  # same R2 client (retries=10, checksum-when-required) the publish path uses
    uri = target_uri or aw.CANONICAL_URI
    prefix = uri.replace(f"s3://{BUCKET}/", "")
    local_ds = f"{SCRATCH_ROOT}/idx_stage/canonical_lance"
    os.makedirs(f"{SCRATCH_ROOT}/idx_stage", exist_ok=True)
    shutil.rmtree(local_ds, ignore_errors=True)

    paginator = s3.get_paginator("list_objects_v2")

    # 1) Snapshot the R2 key set BEFORE indexing (the diff baseline → "new files only").
    before: set[str] = set()
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            before.add(o["Key"])
    if not before:
        return {"status": "dataset_not_found", "prefix": prefix}

    # 2) Download the published dataset to the local /tmp disk (local FS has no R2 multipart rule).
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
    for col in [c for c in aw.BTREE_COLS if c in present]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
        except TypeError:  # pylance version drift — older signature has no replace=
            ds.create_scalar_index(col, index_type="BTREE")
        built.append(col)
        print(f"  BTREE  ✓ {col}", flush=True)
    for col in [c for c in aw.BITMAP_COLS if c in present]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        except TypeError:
            ds.create_scalar_index(col, index_type="BITMAP")
        built.append(col)
        print(f"  BITMAP ✓ {col}", flush=True)

    # 4) Compute the upload set against the before/after key diff, with the mutable hint exception.
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
    # data fragment (assumption BROKEN). Anchor to the dataset's own data/ subdir. Raise BEFORE upload.
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

    # FAIL-CLOSED #2 — positive whitelist. Every NEW file MUST be an index payload, a new manifest, or
    # a new transaction. Anything else trips this. Computed BEFORE upload.
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

    # Upload ORDERED: (1) _indices/** + _transactions/** first, (2) _versions/*.manifest, (3) hint LAST.
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

    # Post-upload structural assertions (>=1 lower bounds — Lance commits ONE manifest per index).
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
# verify_fn — calls the shipped verify() UNCHANGED (read-back assertions)
# ═══════════════════════════════════════════════════════════════════════════════════════
@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),  # ledger read → forward rows_out_floor + bulk_scope
        verify_env,  # DUCK_MEM=24GB + DUCK_TMP on the local disk — verify() materializes 30.4M rows
    ],
    memory=32768,           # 32 GiB — shipped verify() does CREATE TEMP TABLE c (full materialize)
    cpu=4.0,
    timeout=60 * 60,        # 1h
    retries=0,
)
def verify_fn(target_uri: str | None = None, include_fresh: str | bool | None = None) -> dict:
    """Read-back verify. Forwards rows_out_floor + bulk_scope_rows from the latest status='success'
    ledger row so verify()'s check #2 (rows_out floor) AND check #8 (tail-grew ⇔ include_fresh) actually
    RUN in production (both short-circuit when their arg is None). When include_fresh is not explicitly
    passed, it is INFERRED from that ledger row's recorded mode — so an unflagged ::verify after a
    BULK-only build asserts the correct (FALSE) direction instead of false-failing gate #7. An explicit
    --include-fresh always wins over the inferred value."""
    import os

    os.makedirs(f"{SCRATCH_ROOT}/verify_spill", exist_ok=True)

    # Import AFTER verify_env is in os.environ so the module-level DUCK_MEM/DUCK_TMP reads resolve.
    from pipelines.usaspending import usaspending_award_canonical as aw

    # Explicit flag (from the entrypoint) wins; None means "infer from the ledger".
    explicit_fresh = _coerce_include_fresh(include_fresh) if isinstance(include_fresh, str) \
        else include_fresh

    rows_out_floor = None
    bulk_scope_rows = None
    ledger_include_fresh = None
    try:
        import psycopg

        dsn = os.environ.get("HQX_DB_URL_POOLED")
        if dsn:
            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT rows_out_floor, rows_in_bulk, include_fresh "
                    f"FROM {aw.OPS_TABLE} "
                    "WHERE feed = %s AND status = 'success' "
                    "ORDER BY recorded_at DESC LIMIT 1",
                    (aw.FEED,),
                )
                row = cur.fetchone()
                if row is not None:
                    rows_out_floor = row[0] or None       # 0/NULL floor (--since sample) → skip check #2
                    bulk_scope_rows = row[1]              # live BULK scope → drives check #8
                    ledger_include_fresh = row[2]
    except Exception as exc:  # noqa: BLE001 — ledger read is best-effort; verify still runs
        print(f"WARN: verify_fn ledger read failed ({exc}); "
              "checks #2/#8 skipped, include_fresh not inferred", flush=True)

    # Precedence: explicit flag → ledger-recorded mode → True (shipped default).
    if explicit_fresh is not None:
        resolved_fresh = explicit_fresh
    elif ledger_include_fresh is not None:
        resolved_fresh = bool(ledger_include_fresh)
    else:
        resolved_fresh = True

    return aw.verify(target_uri=target_uri or aw.CANONICAL_URI, include_fresh=resolved_fresh,
                     rows_out_floor=rows_out_floor, bulk_scope_rows=bulk_scope_rows)


# ═══════════════════════════════════════════════════════════════════════════════════════
# init_ops_fn / init_ops_main — remote ops DDL bootstrap
# ═══════════════════════════════════════════════════════════════════════════════════════
@app.function(
    image=image,
    secrets=[modal.Secret.from_name("hqx-postgres")],
    timeout=120,
    retries=0,
)
def init_ops_fn() -> dict:
    """Apply the ops.usaspending_award_canonical_runs DDL by calling the shipped init_ops()
    in-container (it reads the co-located .sql shipped beside the .py — same path smoke_fn checks)."""
    from pipelines.usaspending import usaspending_award_canonical as aw

    aw.init_ops()
    return {"applied": True}


# ═══════════════════════════════════════════════════════════════════════════════════════
# Local entrypoints — so `modal run <file>::<name>` drives the remote functions.
# ═══════════════════════════════════════════════════════════════════════════════════════
def _coerce_include_fresh(v: str) -> bool:
    """Explicit truthy parse — NOT bool(v). bool("false") is True → naive coercion inverts intent. A
    bare "" defaults to True."""
    if v is None or v == "":
        return True
    return v.lower() in ("1", "true", "yes", "on")


@app.local_entrypoint()
def init_ops_main() -> None:
    """One-time ops DDL via the remote init_ops_fn. (Preferred: apply locally via doppler — see module
    docstring — to avoid a container spin-up.)"""
    import json

    print(json.dumps(init_ops_fn.remote(), indent=2, default=str))


@app.local_entrypoint()
def smoke() -> None:
    """Cheap packaging/secrets/import/col-count/cross-toggle validation before the giant."""
    import json

    print(json.dumps(smoke_fn.remote(), indent=2, default=str))


@app.local_entrypoint()
def build(since: str = "", target_uri: str = "", include_fresh: str = "") -> None:
    """Full ~30.7M merge → local Lance on /tmp → boto3 publish. Prod first landing passes
    --include-fresh false; the reconcile-later flip passes the default (True). Prod passes NO --since."""
    import json

    print(json.dumps(
        build_fn.remote(since=since or None, target_uri=target_uri or None,
                        include_fresh=_coerce_include_fresh(include_fresh)),
        indent=2, default=str,
    ))


@app.local_entrypoint()
def index(target_uri: str = "") -> None:
    """/tmp-staged append-only BTREE/BITMAP index. Runs AFTER build verifies clean."""
    import json

    print(json.dumps(index_fn.remote(target_uri=target_uri or None), indent=2, default=str))


@app.local_entrypoint()
def verify(target_uri: str = "", include_fresh: str = "") -> None:
    """Read-back assertions. Pass --include-fresh matching the build mode so the fresh-won / tail-grew
    gates assert the correct direction. When --include-fresh is OMITTED, the mode is INFERRED from the
    latest status='success' ledger row (so a BULK-only landing verifies with the correct FALSE direction
    without a flag); an explicit flag always overrides. The raw string ("" when absent) is forwarded so
    verify_fn can distinguish "omitted → infer" from an explicit value."""
    import json

    print(json.dumps(
        verify_fn.remote(target_uri=target_uri or None,
                         include_fresh=include_fresh or None),
        indent=2, default=str,
    ))
