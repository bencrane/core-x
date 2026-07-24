"""Modal orchestrator for the FPDS L2 satellites (usaspending_fpds_l2.py).

``build_fn`` calls the shipped ``build()`` verbatim — the single shared award-partitioned window pass
that materializes BOTH usaspending_fpds_prime_award_state AND usaspending_fpds_mod_delta (indices
folded in, boto3 uniform-part published). ``index_fn`` / ``verify_fn`` are per-table (--table
state|delta) repair / read-back paths that call the shipped ``index()`` / ``verify()`` verbatim.

RUN SEQUENCE
  0a) modal run …::smoke        — CHEAP packaging + secrets + import gate (run BEFORE the giant;
                                  short + read-only → stays a sync .remote()).
  0b) modal run …::init_ops     — one-time ops DDL (or apply locally via doppler).
  0c) modal deploy pipelines/usaspending/usaspending_fpds_l2_modal.py — mandatory before the
      first spawn; thereafter the entrypoints deploy-if-stale automatically.
  1)  modal run …::build        — SPAWNS build_fn on the DEPLOYED app (ASYNC input — survives
      client loss; a tethered .remote() dies ~74–131 s after client loss, --detach or not),
      prints the fc-… id, and returns. Terminal truth = the worker's own
      ops.usaspending_fpds_l2_runs rows (both tables).
      Follow:  modal app logs usaspending-fpds-l2
      Result:  python3 -c "import modal; print(modal.FunctionCall.from_id('fc-…').get(timeout=0))"
  2)  modal run …::verify --table state   ;  …::verify --table delta
      — spawns per table; the verdict dict (pass + failures) exists ONLY behind the fc-id .get():
      FETCH AND READ IT — spawned-without-error ≠ verified. verify writes no ledger row.
  (repair only) modal run …::index --table {state|delta} — spawn ONLY after a clean
      build+verify; index writes NO ledger row, so the fc result dict (indices_built,
      files_published, orphans_pruned) is its only durable success record — collect it.

SIZING — the L2 pass is a single ~37-col-narrow projection windowed once over the 108M spine, lighter
than the spine's 392-col 3-wide merge. Spill + stage on the container's standard 512 GiB local disk
(/tmp), NO modal.Volume (a high-churn spill dir on a network-backed Volume risks the timeout).

  knob                    build_fn        index_fn        verify_fn
  container memory        98304 (96 GiB)  49152 (48 GiB)  32768 (32 GiB)
  container cpu           16.0            8.0             4.0
  timeout                 6h              3h              1h
  FPDS_L2_DUCKDB_MEM      72GB            —               24GB
  FPDS_L2_DUCKDB_THREADS  8               —               4
  LANCE_BYPASS_SPILLING   true (index sort is in-RAM: a >=40M-row BTREE OOMs the DataFusion spill pool)
"""
from pathlib import Path

import modal

BUCKET = "data-sink"
SCRATCH_ROOT = "/tmp/fpds_l2"
OPS_SQL_FILE = "ops_usaspending_fpds_l2_runs.sql"
_PKG_DIR = Path(__file__).resolve().parent  # …/pipelines/usaspending

# ── fat image — same proven giant deps as the spine app. add_local_python_source ships the .py tree;
# add_local_file overlays the ONE co-located L2 ops .sql (read in-container by _record_run/init_ops). ─
image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",
        "lancedb>=0.15",
        "pylance>=7",
        "pyarrow>=17",
        "boto3>=1.35",
        "psycopg[binary]>=3.2",
    )
    .add_local_python_source("pipelines")
    .add_local_file(
        str(_PKG_DIR / OPS_SQL_FILE),
        remote_path=f"/root/pipelines/usaspending/{OPS_SQL_FILE}",
    )
)

app = modal.App("usaspending-fpds-l2", image=image)

# env-knob Secrets — present in os.environ BEFORE the shipped module imports (it reads SCRATCH/DUCK_*
# at import time). Spill + stage on the container's standard 512 GiB /tmp, NOT a network Volume.
build_env = modal.Secret.from_dict({
    "FPDS_L2_SCRATCH": f"{SCRATCH_ROOT}/stage",
    "FPDS_L2_DUCKDB_TEMP_DIR": f"{SCRATCH_ROOT}/duckdb_spill",
    "FPDS_L2_DUCKDB_MEM": "72GB",
    "FPDS_L2_DUCKDB_THREADS": "8",
    "LANCE_BYPASS_SPILLING": "true",
})
index_env = modal.Secret.from_dict({
    "FPDS_L2_SCRATCH": f"{SCRATCH_ROOT}/idx_stage",
    "LANCE_BYPASS_SPILLING": "true",  # REQUIRED — BTREE train sorts tens of millions of keys in-RAM
})
verify_env = modal.Secret.from_dict({
    "FPDS_L2_DUCKDB_TEMP_DIR": f"{SCRATCH_ROOT}/verify_spill",
    "FPDS_L2_DUCKDB_MEM": "24GB",
    "FPDS_L2_DUCKDB_THREADS": "4",
})


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=120,
    retries=0,
)
def smoke_fn() -> dict:
    """Cheap packaging/secrets/import gate before the giant. Asserts: (a) the module imports
    in-container; (b) the co-located ops .sql ships beside the .py; (c) R2 creds resolve; (d) the
    L1 spine source-column contract is satisfiable (all SPINE_SCAN_COLS are on the live 392 schema)."""
    from pathlib import Path as _P

    import lance

    from pipelines.usaspending import usaspending_fpds_l2 as l2

    sql_path = _P(l2.__file__).parent / OPS_SQL_FILE
    assert sql_path.exists(), f"co-located ops .sql NOT shipped in image: {sql_path}"
    so = l2._spine._r2_so()
    assert so.get("endpoint") and so.get("aws_access_key_id"), "R2 creds not wired"
    spine = lance.dataset(l2.SPINE_URI, storage_options=so)
    present = set(spine.schema.names)
    missing = [c for c in l2.SPINE_SCAN_COLS if c not in present]
    assert not missing, f"spine missing L2 source columns: {missing}"
    return {"module_import_ok": True, "ops_sql_readable": True,
            "spine_manifest_version": int(spine.version), "spine_cols": len(present),
            "l2_scan_cols": len(l2.SPINE_SCAN_COLS), "missing_scan_cols": missing,
            "state_indices": len(l2.STATE_BTREE) + len(l2.STATE_BITMAP),
            "delta_indices": len(l2.DELTA_BTREE) + len(l2.DELTA_BITMAP), "status": "ok"}


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres"), build_env],
    memory=98304,   # 96 GiB — DuckDB memory_limit=72GB + Arrow drain + spill page cache slack
    cpu=16.0,       # the 108M award-partitioned window sort is the hot path (threads=8)
    timeout=60 * 60 * 6,  # 6h
    retries=0,            # d.8 — a giant re-run is operator-initiated
    max_containers=1,     # double-launch guard — never two builds on the same R2 prefixes
)
def build_fn(since: str | None = None, state_uri: str | None = None,
             delta_uri: str | None = None, build_date: str | None = None) -> dict:
    import os
    import shutil

    for sub in ("stage", "duckdb_spill"):
        os.makedirs(f"{SCRATCH_ROOT}/{sub}", exist_ok=True)
    from pipelines.usaspending import usaspending_fpds_l2 as l2
    try:
        return l2.build(since=since, state_uri=state_uri or l2.STATE_URI,
                        delta_uri=delta_uri or l2.DELTA_URI, build_date=build_date)
    finally:
        shutil.rmtree(f"{SCRATCH_ROOT}/duckdb_spill", ignore_errors=True)


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("r2-credentials"), index_env],
    memory=49152,   # 48 GiB — in-RAM BTREE sort of tens of millions of keys
    cpu=8.0,
    timeout=60 * 60 * 3,  # 3h
    retries=0,
    max_containers=1,
)
def index_fn(table: str, target_uri: str | None = None) -> dict:
    """Repair-path standalone index for ONE table (build() already folds indexing in). Calls the
    shipped index(), which mirrors R2→local, LOCAL-FS index build, append-only delta publish, GC."""
    import os

    from pipelines.usaspending import usaspending_fpds_l2 as l2

    os.makedirs(f"{SCRATCH_ROOT}/idx_stage", exist_ok=True)
    uri = target_uri or (l2.STATE_URI if table == "state" else l2.DELTA_URI)
    return l2.index(table=table, target_uri=uri)


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("r2-credentials"), verify_env],
    memory=32768,   # 32 GiB — verify materializes the read-back table (CREATE TEMP TABLE c)
    cpu=4.0,
    timeout=60 * 60,  # 1h
    retries=0,
)
def verify_fn(table: str, target_uri: str | None = None) -> dict:
    import os

    from pipelines.usaspending import usaspending_fpds_l2 as l2

    os.makedirs(f"{SCRATCH_ROOT}/verify_spill", exist_ok=True)
    uri = target_uri or (l2.STATE_URI if table == "state" else l2.DELTA_URI)
    return l2.verify(table=table, target_uri=uri)


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("hqx-postgres")],
    timeout=120,
    retries=0,
)
def init_ops_fn() -> dict:
    from pipelines.usaspending import usaspending_fpds_l2 as l2
    l2.init_ops()
    return {"applied": True}


# ── local entrypoints — `modal run <file>::<name>` ──────────────────────────────────────────
@app.local_entrypoint()
def smoke() -> None:
    import json
    print(json.dumps(smoke_fn.remote(), indent=2, default=str))


@app.local_entrypoint()
def init_ops_main() -> None:
    import json
    print(json.dumps(init_ops_fn.remote(), indent=2, default=str))


@app.local_entrypoint()
def build(since: str = "", state_uri: str = "", delta_uri: str = "", build_date: str = "") -> None:
    """The shared pass → both L2 tables (indices folded, published). Prod build passes NO --since.
    Spawns build_fn on the DEPLOYED app and returns the fc-… id; metrics live in the worker's
    ops.usaspending_fpds_l2_runs rows and behind FunctionCall.from_id(fc).get(timeout=0)."""
    from pipelines._shared.launch import spawn_deployed

    spawn_deployed("usaspending-fpds-l2", "build_fn", deploy_file=__file__,
                   since=since or None, state_uri=state_uri or None,
                   delta_uri=delta_uri or None, build_date=build_date or None)


@app.local_entrypoint()
def index(table: str = "state", target_uri: str = "") -> None:
    """Repair-path append-only index for ONE table. Spawn ONLY after a clean build+verify.
    index writes NO ledger row — collect the fc result dict; it is the only success record."""
    from pipelines._shared.launch import spawn_deployed

    spawn_deployed("usaspending-fpds-l2", "index_fn", deploy_file=__file__,
                   table=table, target_uri=target_uri or None)


@app.local_entrypoint()
def verify(table: str = "state", target_uri: str = "") -> None:
    """Spawns verify_fn; the verdict dict exists ONLY behind FunctionCall.from_id(fc).get() —
    fetch and read pass/failures. Spawned-without-error is not verified."""
    from pipelines._shared.launch import spawn_deployed

    spawn_deployed("usaspending-fpds-l2", "verify_fn", deploy_file=__file__,
                   table=table, target_uri=target_uri or None)
