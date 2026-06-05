"""Compute worker — Parallel.ai WEB SEARCH (Directive 24, workflow 3 of 3).

The ``parallel-search`` Modal app. Endpoint-less; spawned by the Universal
Dispatcher. Uses the Search API ``POST /v1/search`` — SYNCHRONOUS: no run lifecycle, no
groups, no basis, no poll (§0). Contract: §0 of
docs/reference/DIRECTIVE_24_PARALLEL_ENRICHMENT_CAPABILITY.md (authoritative).

WHAT THIS WORKER DOES
    POSTs {objective, search_queries (≤5), mode:"advanced", source_policy?} and returns
    the ranked excerpts INLINE in the flat Trigger callback (the agent reads them in-loop —
    Search is evidence-gathering, not a column producer). Because /v1/search is synchronous
    there is no waitpoint poll: one HTTP call, results in hand.

    Optional ``persist=true`` lands the excerpts to s3://data-sink/active/parallel_search/
    (one row per excerpt, keyed run_id) for later reuse; default is inline-only (the
    directive's §11 decision: persist + inline for tests, inline for in-loop reasoning).

CONTROL PLANE: Trigger v4 durable callback — flat JSON to the waitpoint url; ops.parallel_runs.
Even though the call is synchronous, the worker keeps the trigger→dispatcher→Modal→callback
shape so the gtm-mcp surface is uniform across all three workflows (the mandate).

SECRETS: parallel-api · r2-credentials (only when persist=true) · hqx-postgres.
(parallel-api must be minted from Doppler core-x/prd before deploy.)

    modal deploy pipelines/parallel/search.py
"""

from __future__ import annotations

import os

import modal

from core.parallel_client import search as parallel_search

_ACTIVE = "s3://data-sink/active"
SEARCH_URI = os.environ.get("PARALLEL_SEARCH_URI", f"{_ACTIVE}/parallel_search/")

MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.parallel_runs (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spec_id            text NOT NULL,
    workflow           text NOT NULL CHECK (workflow IN ('enrich','deep_research','search')),
    run_kind           text NOT NULL CHECK (run_kind IN ('test','full')),
    group_id           text,
    audience_id        uuid,
    idempotency_key    text,
    requested          bigint,
    skipped_no_domain  bigint,
    completed          bigint,
    failed             bigint,
    failed_company_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
    processor          text,
    cost_estimate      numeric,
    cost_cap           numeric,
    dataset_uri        text,
    status             text NOT NULL,
    error              text,
    started_at         timestamptz,
    completed_at       timestamptz,
    recorded_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS parallel_runs_spec_idx     ON ops.parallel_runs (spec_id);
CREATE INDEX IF NOT EXISTS parallel_runs_workflow_idx ON ops.parallel_runs (workflow);
CREATE INDEX IF NOT EXISTS parallel_runs_status_idx   ON ops.parallel_runs (status);
CREATE INDEX IF NOT EXISTS parallel_runs_recorded_idx ON ops.parallel_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "requests>=2.32",
).env(
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.parallel_client")

# Own Modal app per workflow (see enrich.py) — distinct name so deploys don't clobber.
app = modal.App("parallel-search", image=image)


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _hqx_dsn() -> str | None:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return None
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _flatten_excerpts(resp: dict, run_id: str, objective: str, created: str) -> list[dict]:
    """Normalize the Search API response into a flat excerpt list. Parallel returns ranked
    results (each with url/title and one or more excerpts); shape varies, so this is
    defensive — every {url,title,excerpt} becomes one row keyed run_id."""
    rows: list[dict] = []
    results = resp.get("results") or resp.get("data") or []
    for rank, r in enumerate(results if isinstance(results, list) else [], start=1):
        if not isinstance(r, dict):
            continue
        url = r.get("url") or r.get("link")
        title = r.get("title") or r.get("name")
        excerpts = r.get("excerpts") or r.get("excerpt") or r.get("snippets") or []
        if isinstance(excerpts, str):
            excerpts = [excerpts]
        if not excerpts:
            excerpts = [None]
        for ex in excerpts:
            rows.append({
                "run_id": run_id, "objective": objective, "rank": rank,
                "url": url, "title": title,
                "excerpt": ex if isinstance(ex, str) else (None if ex is None else str(ex)),
                "created_at": created,
            })
    return rows


# Fixed excerpt schema (explicit types so a batch where url/title/excerpt are all None can't
# infer an Arrow ``null`` column that breaks a later append). ``rank`` is int; the rest string.
_SEARCH_STRING_COLS = ("run_id", "objective", "url", "title", "excerpt", "created_at")


def _is_dataset_absent(exc: Exception) -> bool:
    """True iff ``lance.dataset(uri)`` failed because the dataset does not exist yet (vs. a
    credential / network / corrupt-manifest error, which must propagate — never overwrite over
    an existing-but-unreadable dataset)."""
    msg = str(exc).lower()
    needles = ("not found", "does not exist", "no such", "dataset not", "table not",
               "no version", "not a dataset", "could not find")
    return any(n in msg for n in needles)


def _write_search(rows: list[dict], so: dict) -> int:
    import lance
    import pyarrow as pa

    if not rows:
        return 0

    def _s(v):
        return None if v is None else (v if isinstance(v, str) else str(v))

    def _i(v):
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    schema = pa.schema(
        [pa.field(c, pa.string()) for c in _SEARCH_STRING_COLS] + [pa.field("rank", pa.int64())]
    )
    columns = {c: pa.array([_s(r.get(c)) for r in rows], type=pa.string()) for c in _SEARCH_STRING_COLS}
    columns["rank"] = pa.array([_i(r.get("rank")) for r in rows], type=pa.int64())
    table = pa.table(columns, schema=schema)

    # Probe existence: dataset-NOT-FOUND → create; any other open error propagates (never
    # overwrite an existing-but-unreadable dataset). Excerpts accumulate, so existing → append.
    exists = True
    try:
        lance.dataset(SEARCH_URI, storage_options=so)
    except Exception as exc:  # noqa: BLE001
        if _is_dataset_absent(exc):
            exists = False
        else:
            raise
    lance.write_dataset(
        table, SEARCH_URI, mode=("append" if exists else "create"),
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    ds = lance.dataset(SEARCH_URI, storage_options=so)
    if "run_id" in [f.name for f in ds.schema]:
        try:
            ds.create_scalar_index("run_id", index_type="BTREE", replace=True)
            print("  BTREE  ✓ run_id")
        except Exception as exc:  # noqa: BLE001
            print(f"  BTREE  ✗ run_id: {exc}")
    return table.num_rows


def _record_run(row: dict) -> None:
    import psycopg

    dsn = _hqx_dsn()
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.parallel_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.parallel_runs
                    (spec_id, workflow, run_kind, group_id, audience_id, idempotency_key,
                     requested, skipped_no_domain, completed, failed, processor, cost_estimate,
                     cost_cap, dataset_uri, status, error, started_at, completed_at)
                VALUES (%(spec_id)s, 'search', %(run_kind)s, %(group_id)s, %(audience_id)s,
                        %(idempotency_key)s, %(requested)s, 0, %(completed)s, %(failed)s,
                        NULL, NULL, %(cost_cap)s, %(dataset_uri)s, %(status)s, %(error)s,
                        %(started_at)s, %(completed_at)s)
                """,
                row,
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.parallel_runs write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print("Callback delivered.")
                return
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


@app.function(
    secrets=[
        modal.Secret.from_name("parallel-api"),
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 10,
    memory=2048,
    cpu=1.0,
)
def web_search(
    trigger_callback_url: str | None = None,
    run_id: str | None = None,
    spec_id: str = "",
    objective: str = "",
    search_queries: list | None = None,
    mode: str = "advanced",
    source_policy: dict | None = None,
    persist: bool = False,
    run_kind: str = "test",
    idempotency_key: str | None = None,
) -> dict:
    """Run one Parallel web search (synchronous /v1/search) → ranked excerpts INLINE.

    Returns the excerpts in the flat callback for in-loop agent reasoning. ``persist=true``
    also lands them to s3://data-sink/active/parallel_search/. No poll, no run lifecycle,
    no basis (§0). Terminal → ops.parallel_runs + Trigger callback."""
    import datetime as dt
    import uuid

    started = dt.datetime.now(dt.timezone.utc)
    run_id = run_id or uuid.uuid4().hex
    spec = (spec_id or "").strip() or f"search_{run_id[:8]}"
    queries = [str(q) for q in (search_queries or []) if str(q).strip()][:5]
    status, error = "failed", None
    excerpts: list[dict] = []
    landed = 0

    def _terminal(st, *, reraise_msg=None):
        finished = dt.datetime.now(dt.timezone.utc)
        _record_run({
            "spec_id": spec, "run_kind": run_kind, "group_id": run_id, "audience_id": None,
            "idempotency_key": idempotency_key, "requested": len(queries) or 1,
            "completed": len(excerpts), "failed": 0 if st == "success" else 1,
            "cost_cap": None, "dataset_uri": SEARCH_URI if persist else None,
            "status": st, "error": error, "started_at": started, "completed_at": finished,
        })
        _post_callback(trigger_callback_url, {
            "status": st, "run_id": run_id, "workflow": "search", "spec_id": spec,
            "objective": objective, "search_queries": queries,
            "excerpt_count": len(excerpts), "persisted": landed if persist else 0,
            "dataset_uri": SEARCH_URI if persist else None,
            # Inline payload — the agent reads these directly (capped to keep the callback sane).
            "excerpts": excerpts[:50], "error": error,
        })
        if reraise_msg:
            raise RuntimeError(reraise_msg)

    try:
        if not (objective or "").strip() and not queries:
            status, error = "rejected", "objective or search_queries is required"
            return _terminal(status) or {"status": status, "error": error}

        resp = parallel_search(objective=objective or None, search_queries=queries or None,
                               mode=mode, source_policy=source_policy)
        created = dt.datetime.now(dt.timezone.utc).isoformat()
        excerpts = _flatten_excerpts(resp, run_id, objective, created)

        if persist and excerpts:
            landed = _write_search(excerpts, _r2_storage_options())
            print(f"  persisted {landed} excerpt(s) → {SEARCH_URI}")

        status, error = "success", None
        return _terminal("success") or {"status": "success", "run_id": run_id,
                                        "excerpt_count": len(excerpts), "persisted": landed}

    except Exception as exc:  # noqa: BLE001
        status, error = "failed", str(exc)
        _terminal("failed", reraise_msg=f"parallel web search failed: {exc}")
