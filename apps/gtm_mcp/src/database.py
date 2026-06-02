"""Unified data context for the GTM MCP gateway — one shared DuckDB connection
over the Gen-3 R2 data sink, plus the Lance pushdown plumbing.

WHY THIS MODULE. The gateway combines two access shapes against the *same*
Cloudflare R2 sink:

  • Index pushdown — point lookups push their predicate straight into the Lance
    scanner so a load-bearing ``BTREE`` answers in sub-100 ms (audience.py). This
    path is pure Lance; DuckDB never touches it.
  • Raw SQL audiences — ``execute_audience_query`` runs arbitrary ANSI SQL over
    the datasets as named relations, for cross-layer joins (companies ⋈ awards).
    DuckDB does the compute; the Lance datasets are bridged in as Arrow streams.

Both share ONE DuckDB connection (the directive's "single, shared" mandate),
configured for R2 S3 so the SQL path can also read raw transport Parquet drops
(``read_parquet('s3://data-sink/...')``) directly when an agent needs to.

SECRET MAPPING (exact, per directive). The fleet exposes R2 credentials as
``R2_ACCESS_KEY_ID`` / ``R2_SECRET_ACCESS_KEY`` / ``R2_ENDPOINT`` (Render
service env vars, mirroring the ``r2-credentials`` Modal secret and the
``hq-x/prd`` Doppler config). Both the Lance ``storage_options`` and the DuckDB
``CREATE SECRET (TYPE S3)`` are built from exactly those three variables
(``R2_ACCOUNT_ID`` is accepted as a fallback to derive the endpoint, matching
every worker in ``pipelines/``).

FRESHNESS. Datasets are opened per call (never cached as long-lived handles) so
the gateway always reflects the latest committed Lance version — the pipeline
workers overwrite these datasets in place, and a stale handle would serve the
prior snapshot. A dataset open is a single manifest GET; the point-lookup path
stays sub-100 ms.
"""

from __future__ import annotations

import os
import threading
from typing import Any

# ── Dataset registry — the active Gen-3 sink (directive §2). Override per-dataset
# via env to point at a staging sink without a code change. ────────────────────
_ACTIVE = "s3://data-sink/active"
DATASETS: dict[str, str] = {
    "companies": os.environ.get("GTM_COMPANIES_URI", f"{_ACTIVE}/companies/"),
    "people": os.environ.get("GTM_PEOPLE_URI", f"{_ACTIVE}/people/"),
    "awards": os.environ.get(
        "CONTRACTOR_AWARD_SUMMARY_LANCE_URI",
        f"{_ACTIVE}/contractor_award_summary/",
    ),
}

# Hard ceiling on rows returned by the raw SQL path — an MCP tool result is JSON
# handed to an agent, not a bulk export channel. Truncation is reported, never silent.
MAX_QUERY_ROWS = 1000

_lock = threading.Lock()
_con: Any = None  # the single shared duckdb.DuckDBPyConnection (lazy singleton)


# ── R2 endpoint / credentials ───────────────────────────────────────────────
def _r2_endpoint() -> str:
    """Full ``https://…`` R2 endpoint (Lance ``storage_options`` form). Supplied
    directly via ``R2_ENDPOINT``, or derived from ``R2_ACCOUNT_ID`` — the fleet rule."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError(
            "Set R2_ENDPOINT (or R2_ACCOUNT_ID) — the gateway cannot reach the R2 sink."
        )
    return endpoint


def r2_storage_options() -> dict[str, str]:
    """object_store options for the Lance reader — byte-identical to the worker
    convention in ``pipelines/*`` (``_r2_storage_options``). Used by every
    ``lance.dataset(...)`` open in the gateway."""
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": _r2_endpoint(),
        "region": "auto",
    }


def open_dataset(name: str):
    """Open a registered dataset fresh (latest committed Lance version). ``name``
    is one of ``DATASETS``. The returned ``lance.LanceDataset`` exposes
    ``.scanner(filter=..., columns=...)`` for BTREE index pushdown."""
    import lance

    if name not in DATASETS:
        raise KeyError(f"unknown dataset {name!r}; expected one of {sorted(DATASETS)}")
    return lance.dataset(DATASETS[name], storage_options=r2_storage_options())


# ── Shared DuckDB connection ─────────────────────────────────────────────────
def _configure_r2_s3(con) -> None:
    """Map the R2 credentials onto DuckDB's S3 layer via a TYPE S3 secret. R2 is
    S3-compatible but requires path-style addressing; the endpoint is the host
    only (no scheme), TLS on, region ``auto``."""
    host = _r2_endpoint().split("://", 1)[-1].rstrip("/")
    key_id = os.environ["R2_ACCESS_KEY_ID"].replace("'", "''")
    secret = os.environ["R2_SECRET_ACCESS_KEY"].replace("'", "''")
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception:  # noqa: BLE001 — httpfs is autoloadable in modern duckdb wheels
        con.execute("LOAD httpfs;")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET r2 (
            TYPE S3,
            KEY_ID '{key_id}',
            SECRET '{secret}',
            ENDPOINT '{host}',
            URL_STYLE 'path',
            USE_SSL true,
            REGION 'auto'
        );
        """
    )


def get_connection():
    """The single, shared in-memory DuckDB connection, configured for R2 S3 on
    first use. Per-query work runs on a ``.cursor()`` of this connection so
    concurrent SSE tool calls never collide on one execution context while still
    sharing the catalog + the R2 secret."""
    global _con
    if _con is None:
        with _lock:
            if _con is None:
                import duckdb

                con = duckdb.connect(":memory:")
                con.execute("PRAGMA threads=4;")
                _configure_r2_s3(con)
                _con = con
    return _con


def _register_datasets(cur) -> None:
    """Bind every registered Lance dataset as a same-named DuckDB relation on a
    cursor. The ``LanceDataset`` is registered directly — NOT a one-shot
    ``RecordBatchReader``, which DuckDB exhausts after a single scan and then
    silently returns empty on a second reference (a self-join or reused CTE would
    quietly yield wrong rows). Registered as the dataset, DuckDB re-scans safely,
    stays lazy (an unreferenced dataset is never read — so registering all three
    on every call costs only a manifest open), and can push projections/filters
    into Lance. Each call opens the latest committed version."""
    so = r2_storage_options()
    import lance

    for name, uri in DATASETS.items():
        cur.register(name, lance.dataset(uri, storage_options=so))


def query(sql: str, max_rows: int = MAX_QUERY_ROWS) -> dict[str, Any]:
    """Execute raw ANSI SQL over the named datasets (``companies`` / ``people`` /
    ``awards``) and any ``s3://`` transport Parquet. Returns
    ``{"columns", "rows", "row_count", "truncated"}``. Runs on a fresh cursor with
    freshly-registered (latest-version) Lance relations; the cursor is closed after."""
    cur = get_connection().cursor()
    try:
        _register_datasets(cur)
        rel = cur.execute(sql)
        columns = [d[0] for d in rel.description] if rel.description else []
        fetched = rel.fetchmany(max_rows + 1)
        truncated = len(fetched) > max_rows
        rows = [dict(zip(columns, r)) for r in fetched[:max_rows]]
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
        }
    finally:
        cur.close()
