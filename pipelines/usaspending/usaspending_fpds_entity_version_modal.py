"""Modal orchestrator for the FPDS entity-version dimension (usaspending_fpds_entity_version.py).

RUN SEQUENCE
  0) modal run …::smoke      — packaging + secrets + spine-column gate (run BEFORE the giant)
  1) modal run …::build      — the uei-partitioned window pass → entity_version (indices folded)
  2) modal run …::verify

SIZING — one 13-col projection of the 108M spine windowed once per UEI: ~1/3 of the L2 pass's
width. 64 GiB container / 48GB DuckDB / 512 GiB local /tmp spill; no modal.Volume.
"""
from pathlib import Path

import modal

SCRATCH_ROOT = "/tmp/fpds_entity"
OPS_SQL_FILE = "ops_usaspending_fpds_entity_version_runs.sql"
_PKG_DIR = Path(__file__).resolve().parent

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
    .add_local_file(str(_PKG_DIR / OPS_SQL_FILE),
                    remote_path=f"/root/pipelines/usaspending/{OPS_SQL_FILE}")
    .add_local_file(str(_PKG_DIR / "ops_usaspending_fpds_l2_runs.sql"),
                    remote_path="/root/pipelines/usaspending/ops_usaspending_fpds_l2_runs.sql")
)

app = modal.App("usaspending-fpds-entity-version", image=image)

build_env = modal.Secret.from_dict({
    "FPDS_ENTITY_SCRATCH": f"{SCRATCH_ROOT}/stage",
    "FPDS_ENTITY_DUCKDB_TEMP_DIR": f"{SCRATCH_ROOT}/duckdb_spill",
    "FPDS_ENTITY_DUCKDB_MEM": "48GB",
    "FPDS_ENTITY_DUCKDB_THREADS": "8",
    "LANCE_BYPASS_SPILLING": "true",
})
verify_env = modal.Secret.from_dict({
    "FPDS_ENTITY_DUCKDB_TEMP_DIR": f"{SCRATCH_ROOT}/verify_spill",
    "FPDS_ENTITY_DUCKDB_MEM": "16GB",
    "FPDS_ENTITY_DUCKDB_THREADS": "4",
})


@app.function(image=image,
              secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
              timeout=120, retries=0)
def smoke_fn() -> dict:
    from pathlib import Path as _P

    import lance

    from pipelines.usaspending import usaspending_fpds_entity_version as ev

    sql_path = _P(ev.__file__).parent / OPS_SQL_FILE
    assert sql_path.exists(), f"co-located ops .sql NOT shipped: {sql_path}"
    so = ev._spine._r2_so()
    assert so.get("endpoint") and so.get("aws_access_key_id"), "R2 creds not wired"
    spine = lance.dataset(ev.SPINE_URI, storage_options=so)
    present = set(spine.schema.names)
    missing = [c for c in ev.SPINE_SCAN_COLS if c not in present]
    assert not missing, f"spine missing entity source columns: {missing}"
    return {"module_import_ok": True, "ops_sql_readable": True,
            "spine_manifest_version": int(spine.version),
            "entity_scan_cols": len(ev.SPINE_SCAN_COLS), "missing_scan_cols": missing,
            "indices_planned": len(ev.BTREE) + len(ev.BITMAP), "status": "ok"}


@app.function(image=image,
              secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres"), build_env],
              memory=65536, cpu=8.0, timeout=60 * 60 * 4, retries=0, max_containers=1)
def build_fn(since: str | None = None, uri: str | None = None, build_date: str | None = None) -> dict:
    import os
    import shutil

    for sub in ("stage", "duckdb_spill"):
        os.makedirs(f"{SCRATCH_ROOT}/{sub}", exist_ok=True)
    from pipelines.usaspending import usaspending_fpds_entity_version as ev
    try:
        return ev.build(since=since, uri=uri or ev.ENTITY_URI, build_date=build_date)
    finally:
        shutil.rmtree(f"{SCRATCH_ROOT}/duckdb_spill", ignore_errors=True)


@app.function(image=image,
              secrets=[modal.Secret.from_name("r2-credentials"), verify_env],
              memory=16384, cpu=4.0, timeout=60 * 60, retries=0)
def verify_fn(uri: str | None = None) -> dict:
    import os

    from pipelines.usaspending import usaspending_fpds_entity_version as ev

    os.makedirs(f"{SCRATCH_ROOT}/verify_spill", exist_ok=True)
    return ev.verify(uri=uri or ev.ENTITY_URI)


@app.function(image=image, secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120, retries=0)
def init_ops_fn() -> dict:
    from pipelines.usaspending import usaspending_fpds_entity_version as ev
    ev.init_ops()
    return {"applied": True}


@app.local_entrypoint()
def smoke() -> None:
    import json
    print(json.dumps(smoke_fn.remote(), indent=2, default=str))


@app.local_entrypoint()
def init_ops_main() -> None:
    import json
    print(json.dumps(init_ops_fn.remote(), indent=2, default=str))


@app.local_entrypoint()
def build(since: str = "", uri: str = "", build_date: str = "") -> None:
    import json
    print(json.dumps(build_fn.remote(since=since or None, uri=uri or None,
                                     build_date=build_date or None), indent=2, default=str))


@app.local_entrypoint()
def verify(uri: str = "") -> None:
    import json
    print(json.dumps(verify_fn.remote(uri=uri or None), indent=2, default=str))
