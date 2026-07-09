"""query_sidecar_api — read-only HTTP-SQL gateway over the query-sidecar artifact.

Phase 3 of the query-sidecar plan (docs/plans/QUERY_SIDECAR_PHASE2_BENCHMARK.md
gate: GO). Serves the platform's ad-hoc/phrase-lane analytical queries against
the sorted DuckDB artifact built by pipelines/query_sidecar/build_query_sidecar.py.

Architecture (mirrors the apps/ read-only-gateway doctrine):
- BOOT: read s3://data-sink/query-sidecar/LATEST.json, download the versioned
  .duckdb artifact to local disk (DATA_DIR), open READ_ONLY. Fail-closed: no
  bearer token or no artifact -> refuse to boot.
- POST /api/v1/sql        {"sql": "...", "limit": 1000}  -> rows (SELECT/WITH only)
- GET  /api/v1/tables     -> the artifact's _sidecar_manifest (provenance per table)
- POST /api/v1/refresh    -> re-read LATEST.json, download, blue-green swap
- GET  /healthz           -> artifact key, built_at, table count (no auth)

Safety model: the DuckDB connection is opened read_only (hard backstop); on top,
a statement guard admits a single SELECT/WITH statement only, a deny-list blocks
side-effecting keywords, results are capped via fetchmany (no SQL rewriting),
and a watchdog interrupts queries that exceed QUERY_TIMEOUT_S. Writes are
impossible by construction: the artifact is a derived, disposable copy — the
Lance SoR is never touched by this service.
"""

from __future__ import annotations

import json
import os
import re
import secrets as _secrets
import threading
import time

import boto3
import duckdb
import uvicorn
from botocore.config import Config
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel

R2_BUCKET = "data-sink"
LATEST_KEY = "query-sidecar/LATEST.json"
DATA_DIR = os.environ.get("DATA_DIR", "/var/data")
DEFAULT_LIMIT = 1_000
MAX_LIMIT = 50_000
QUERY_TIMEOUT_S = float(os.environ.get("QUERY_TIMEOUT_S", "120"))

_TOKEN = os.environ.get("QUERY_SIDECAR_TOKEN")

_DENY = re.compile(
    r"\b(ATTACH|DETACH|COPY|INSTALL|LOAD|PRAGMA|SET|RESET|CREATE|INSERT|UPDATE|DELETE|"
    r"DROP|ALTER|EXPORT|IMPORT|CALL|VACUUM|CHECKPOINT|BEGIN|COMMIT|ROLLBACK|TRANSACTION|USE)\b",
    re.IGNORECASE,
)
_ALLOW_START = re.compile(r"^\s*(SELECT|WITH|DESCRIBE|SHOW)\b", re.IGNORECASE)


class _State:
    con: duckdb.DuckDBPyConnection | None = None
    path: str | None = None
    meta: dict = {}
    lock = threading.Lock()


S = _State()


def _s3():
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"),
    )


def _download_latest() -> tuple[str, dict]:
    s3 = _s3()
    latest = json.loads(s3.get_object(Bucket=R2_BUCKET, Key=LATEST_KEY)["Body"].read())
    os.makedirs(DATA_DIR, exist_ok=True)
    local = os.path.join(DATA_DIR, os.path.basename(latest["key"]))
    if not (os.path.exists(local) and os.path.getsize(local) == latest.get("file_bytes")):
        t0 = time.monotonic()
        s3.download_file(R2_BUCKET, latest["key"], local)
        print(f"[boot] downloaded {latest['key']} ({latest.get('file_bytes', 0)/2**30:.1f} GiB) "
              f"in {time.monotonic()-t0:.0f}s")
    else:
        print(f"[boot] artifact already on disk: {local}")
    return local, latest


def _attach(path: str, meta: dict) -> None:
    con = duckdb.connect(path, read_only=True)
    mem = os.environ.get("DUCKDB_MEMORY_LIMIT", "1500MB")
    con.execute(f"SET memory_limit='{mem}'; SET threads=2; SET temp_directory='{DATA_DIR}/spill';")
    old_con, old_path = S.con, S.path
    S.con, S.path, S.meta = con, path, meta
    if old_con is not None:
        # PARK the old connection instead of closing it under live cursors
        # (queries run on per-request cursors — see sql()); close + delete
        # after a grace window once in-flight work has drained.
        def _reap(c=old_con, p=old_path):
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass
            if p and p != path and os.path.exists(p):
                os.remove(p)
            print(f"[attach] reaped parked artifact {p}")
        threading.Timer(300, _reap).start()
    print(f"[attach] serving {meta.get('key')} (built {meta.get('built_at')}, "
          f"{len(meta.get('tables', []))} tables)")


def _require_token(request: Request) -> None:
    auth = request.headers.get("authorization", "")
    if not (auth.startswith("Bearer ") and _secrets.compare_digest(auth[7:], _TOKEN or "")):
        raise HTTPException(status_code=401, detail="invalid bearer token")


class SqlRequest(BaseModel):
    sql: str
    limit: int | None = None


app = FastAPI(title="query-sidecar-api", docs_url=None, redoc_url=None)


def _require_ready() -> None:
    if S.con is None:
        raise HTTPException(status_code=503, detail="hydrating — artifact download in progress")


@app.get("/healthz")
def healthz():
    return {"ok": True, "ready": S.con is not None, "artifact": S.meta.get("key"),
            "built_at": S.meta.get("built_at"), "tables": len(S.meta.get("tables", []))}


@app.get("/api/v1/tables", dependencies=[Depends(_require_token)])
def tables():
    _require_ready()
    cur = S.con.cursor()  # per-request cursor — concurrent with other queries
    try:
        rows = cur.execute(
            "SELECT table_name, dataset, tier, sort_key, lance_version, duck_rows "
            "FROM _sidecar_manifest ORDER BY table_name").fetchall()
    finally:
        cur.close()
    cols = ["table_name", "dataset", "tier", "sort_key", "lance_version", "rows"]
    return {"artifact": S.meta.get("key"), "tables": [dict(zip(cols, r)) for r in rows]}


@app.post("/api/v1/sql", dependencies=[Depends(_require_token)])
def sql(req: SqlRequest):
    _require_ready()
    q = req.sql.strip().rstrip(";").strip()
    if ";" in q:
        raise HTTPException(status_code=400, detail="single statement only")
    if not _ALLOW_START.match(q):
        raise HTTPException(status_code=400, detail="SELECT/WITH/DESCRIBE/SHOW only")
    if _DENY.search(q):
        raise HTTPException(status_code=400, detail="statement contains a blocked keyword")
    cap = max(1, min(req.limit or DEFAULT_LIMIT, MAX_LIMIT))

    # Per-request cursor: queries run CONCURRENTLY (FastAPI sync endpoints run in
    # a threadpool; a duckdb cursor is an independent connection handle). The
    # global lock is only held by attach swaps, never across a query.
    cur = S.con.cursor()
    timer = threading.Timer(QUERY_TIMEOUT_S, cur.interrupt)
    timer.start()
    t0 = time.monotonic()
    try:
        res = cur.execute(q)
        cols = [d[0] for d in res.description]
        rows = res.fetchmany(cap + 1)
    except duckdb.InterruptException:
        raise HTTPException(status_code=408, detail=f"query exceeded {QUERY_TIMEOUT_S}s")
    except duckdb.Error as exc:
        raise HTTPException(status_code=400, detail=str(exc)[:500])
    finally:
        timer.cancel()
        cur.close()
    elapsed_ms = round((time.monotonic() - t0) * 1000, 1)

    truncated = len(rows) > cap
    return {"columns": cols, "rows": [list(r) for r in rows[:cap]],
            "row_count": min(len(rows), cap), "truncated": truncated,
            "elapsed_ms": elapsed_ms, "artifact": S.meta.get("key")}


@app.post("/api/v1/refresh", dependencies=[Depends(_require_token)])
def refresh():
    """Async: kick the download+swap in the background and return immediately —
    a 31 GiB artifact download must never ride an HTTP request/response
    (the builder's notify hook timed out doing exactly that)."""
    def _do():
        try:
            path, meta = _download_latest()
            if path != S.path:
                with S.lock:
                    _attach(path, meta)
            else:
                print("[refresh] already current")
        except Exception as exc:  # noqa: BLE001
            print(f"[refresh] failed: {exc}")

    threading.Thread(target=_do, daemon=True, name="refresh").start()
    return {"refreshing": True, "serving": S.meta.get("key"),
            "note": "swap happens in background; poll /healthz for the new artifact stamp"}


def _hydrate_forever() -> None:
    """Background hydration: the port binds immediately (Render port-scan
    requirement); endpoints 503 until the artifact is attached. Retries on
    transient R2 failures rather than dying."""
    while S.con is None:
        try:
            path, meta = _download_latest()
            with S.lock:
                _attach(path, meta)
        except Exception as exc:  # noqa: BLE001
            print(f"[hydrate] failed ({exc}); retrying in 30s")
            time.sleep(30)


if __name__ == "__main__":
    if not _TOKEN:
        raise RuntimeError("QUERY_SIDECAR_TOKEN unset — refusing to boot an open SQL endpoint")
    threading.Thread(target=_hydrate_forever, daemon=True, name="hydrate").start()
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
