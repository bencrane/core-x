"""Unified data context for the GTM MCP gateway — one shared DuckDB connection
over the Gen-3 R2 data sink, a runtime-discovered dataset registry, and the Lance
pushdown plumbing.

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

DYNAMIC REGISTRY (Directive 16). There is NO hardcoded dataset list. At first use
the gateway *lists* the active sink (``s3://data-sink/active/``) and resolves every
committed Lance dataset — flat roots (``companies``) and the leaves nested under
multi-table namespaces (``usaspending/award_search``, ``fmcsa/carrier``) alike —
into an in-memory ``name → uri`` registry. The catalog therefore self-maintains:
a pipeline that drops a new dataset into the sink shows up on the next restart
with no code change. A small defensive seed guarantees the three indexed core
datasets the typed point-lookups depend on stay resolvable even if a scoped R2
token can ``GetObject`` but not ``ListBucket`` (so discovery returns nothing) —
the point-lookup path must never go dark.

JIT REGISTRATION (the performance gate). The directive's hard constraint: DuckDB
must not open all ~100 Lance manifests on every query. ``query`` registers ONLY
the datasets a caller names (resolved from the SQL by ``referenced_datasets``), so
a two-table join opens two manifests, not the whole plane.

SECRET MAPPING (exact, per directive). The fleet exposes R2 credentials as
``R2_ACCESS_KEY_ID`` / ``R2_SECRET_ACCESS_KEY`` / ``R2_ENDPOINT`` (Render
service env vars, mirroring the ``r2-credentials`` Modal secret and the
``hq-x/prd`` Doppler config). The Lance ``storage_options``, the boto3 listing
client, and the DuckDB ``CREATE SECRET (TYPE S3)`` are all built from exactly
those three variables (``R2_ACCOUNT_ID`` is accepted as a fallback to derive the
endpoint, matching every worker in ``pipelines/``).

FRESHNESS. Datasets are opened per call (never cached as long-lived handles) so
the gateway always reflects the latest committed Lance version — the pipeline
workers overwrite these datasets in place, and a stale handle would serve the
prior snapshot. A dataset open is a single manifest GET; the point-lookup path
stays sub-100 ms.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterable

log = logging.getLogger("gtm_mcp.database")

# ── Active sink coordinates ──────────────────────────────────────────────────
BUCKET = "data-sink"
ACTIVE_PREFIX = "active/"  # key prefix inside the bucket (boto3 listing)
ACTIVE_URI = f"s3://{BUCKET}/active"  # base for the s3:// dataset URIs Lance opens

# Back-compat alias: the awards point-lookup (and agents) name this ``awards``;
# the dataset on disk is ``contractor_award_summary``. The alias resolves to
# whatever the canonical dataset resolves to (discovery or seed/override).
ALIASES: dict[str, str] = {"awards": "contractor_award_summary"}

# Lance internal directories — never themselves a dataset name. Presence of
# ``_versions`` is what marks a prefix as a committed Lance dataset root.
_INTERNAL = {"_versions", "_indices", "_transactions", "_deletions", "data"}
_LANCE_MARKER = "_versions"
# All observed nesting is exactly one level (namespace → dataset). Cap the walk
# there; deeper trees would be a layout change and are surfaced, not silently dropped.
_MAX_DEPTH = 2
_DISCOVERY_WORKERS = 16

# Hard ceiling on rows returned by the raw SQL path — an MCP tool result is JSON
# handed to an agent, not a bulk export channel. Truncation is reported, never silent.
MAX_QUERY_ROWS = 1000

_lock = threading.Lock()
_con: Any = None  # the single shared duckdb.DuckDBPyConnection (lazy singleton)

_registry_lock = threading.Lock()
_registry: dict[str, str] | None = None  # discovered name → s3:// uri (lazy singleton)
_tls = threading.local()  # per-thread boto3 client (clients are not shared across threads)


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


def _s3_client():
    """A boto3 S3 client bound to R2, built from the SAME three credentials as the
    Lance reader. The checksum config mirrors every listing/transfer worker in
    ``pipelines/*`` — botocore's default flexible-checksum validation otherwise
    raises against R2. Clients are not shared across threads (one per thread)."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=_r2_endpoint(),
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _thread_s3():
    client = getattr(_tls, "s3", None)
    if client is None:
        client = _s3_client()
        _tls.s3 = client
    return client


def get_object_bytes(key: str) -> bytes | None:
    """Best-effort GET of one R2 object by key (e.g. ``active/catalog.json``).
    Returns the body, or ``None`` if the object is absent/unreadable — callers
    enrich with it when present and degrade gracefully when not."""
    try:
        return _s3_client().get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except Exception as exc:  # noqa: BLE001 — optional enrichment, never load-bearing
        log.warning("gtm-mcp: get_object %r failed: %s", key, exc)
        return None


# ── Dynamic dataset discovery (Directive 16 §1) ──────────────────────────────
def _child_dir_names(prefix: str) -> list[str]:
    """Immediate child *directory* names under ``prefix`` (one delimited LIST,
    paginated). Files in the prefix (e.g. ``active/catalog.json``) are ignored —
    only ``CommonPrefixes`` count. Errors degrade to "no children" so one bad
    prefix never aborts the whole walk."""
    s3 = _thread_s3()
    names: list[str] = []
    token: str | None = None
    try:
        while True:
            kw: dict[str, Any] = {"Bucket": BUCKET, "Prefix": prefix, "Delimiter": "/"}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for cp in resp.get("CommonPrefixes", []):
                names.append(cp["Prefix"][len(prefix):].rstrip("/"))
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
    except Exception as exc:  # noqa: BLE001 — a single prefix failing must not sink discovery
        log.warning("gtm-mcp: listing %r failed: %s", prefix, exc)
        return []
    return names


def discover_datasets() -> dict[str, str]:
    """List the active sink and resolve every committed Lance dataset to a
    ``name → s3:// uri`` entry. ``name`` is the dataset's path relative to
    ``active/`` (the catalog-canonical form: ``companies``,
    ``usaspending/award_search``).

    A prefix is a dataset root iff its children include ``_versions`` (the Lance
    manifest dir); such a prefix is registered and NOT descended. Otherwise it is
    a namespace and its non-internal children are walked (one level — the only
    nesting the sink uses). Levels are listed concurrently so a cold start over
    ~100 datasets stays well under a second.
    """
    out: dict[str, str] = {}
    pending: list[tuple[str, str]] = [("", ACTIVE_PREFIX)]
    depth = 0
    with ThreadPoolExecutor(max_workers=_DISCOVERY_WORKERS) as pool:
        while pending and depth <= _MAX_DEPTH:
            listings = pool.map(lambda item: (item, _child_dir_names(item[1])), pending)
            nxt: list[tuple[str, str]] = []
            for (rel, full), children in listings:
                if _LANCE_MARKER in children:
                    if rel:  # the active/ root itself is never a dataset
                        out[rel] = f"{ACTIVE_URI}/{rel}/"
                    continue
                for child in children:
                    if child in _INTERNAL or child.startswith("_"):
                        continue
                    child_rel = f"{rel}/{child}" if rel else child
                    nxt.append((child_rel, f"{full}{child}/"))
            pending = nxt
            depth += 1
    return out


def _build_registry() -> dict[str, str]:
    """Discover the sink, then guarantee the indexed core datasets resolve and
    honor explicit per-dataset staging overrides + the ``awards`` alias.

    Discovery is the source of truth for the ~100 datasets. The core seed below is
    NOT a static catalog — it is a resilience floor: should discovery come back
    empty (e.g. a token with object-read but not bucket-list permission), the
    typed point-lookups still resolve ``companies`` / ``people`` / awards against
    their known paths. Explicit ``*_URI`` env overrides win over discovery so a
    staging sink can be redirected per-dataset without a code change."""
    try:
        reg = discover_datasets()
    except Exception as exc:  # noqa: BLE001 — never let discovery failure sink the gateway
        log.warning("gtm-mcp: dataset discovery failed, falling back to core seed: %s", exc)
        reg = {}

    reg.setdefault("companies", f"{ACTIVE_URI}/companies/")
    reg.setdefault("people", f"{ACTIVE_URI}/people/")
    reg.setdefault("contractor_award_summary", f"{ACTIVE_URI}/contractor_award_summary/")

    for name, env_var in (
        ("companies", "GTM_COMPANIES_URI"),
        ("people", "GTM_PEOPLE_URI"),
        ("contractor_award_summary", "CONTRACTOR_AWARD_SUMMARY_LANCE_URI"),
    ):
        override = os.environ.get(env_var)
        if override:
            reg[name] = override

    for alias, target in ALIASES.items():
        if target in reg:
            reg[alias] = reg[target]

    log.info("gtm-mcp: registry holds %d datasets", len(reg))
    return reg


def get_registry(refresh: bool = False) -> dict[str, str]:
    """The in-memory dataset registry (``name → uri``), built once on first use.
    ``refresh=True`` rebuilds it (re-lists the sink) — the hook for picking up new
    drops without a process restart. Returns a copy so callers can't mutate it."""
    global _registry
    if _registry is None or refresh:
        with _registry_lock:
            if _registry is None or refresh:
                _registry = _build_registry()
    return dict(_registry)


def dataset_names() -> list[str]:
    """Sorted canonical dataset names (aliases excluded) — what ``list_datasets``
    advertises and what an agent names in SQL."""
    return sorted(name for name in get_registry() if name not in ALIASES)


def referenced_datasets(sql: str) -> set[str]:
    """The performance gate: which registered datasets does this SQL reference?

    Whole-token match of every registry name against the query text (the
    directive's "simple regex / whole-word match"). The boundary class rejects
    word chars, ``.`` and ``/`` on either side, so ``agency`` never matches inside
    ``subtier_agency`` and a flat name never matches a path suffix; nested names
    (``usaspending/award_search``) match when written double-quoted, as DuckDB
    requires. Ambiguous mentions over-match (a needless manifest open) rather than
    under-match (a broken query) — the safe direction."""
    found: set[str] = set()
    for name in get_registry():
        pattern = r"(?<![\w./])" + re.escape(name) + r"(?![\w./])"
        if re.search(pattern, sql, re.IGNORECASE):
            found.add(name)
    return found


def open_dataset(name: str):
    """Open a registered dataset fresh (latest committed Lance version). ``name``
    is any registry key (a canonical name or the ``awards`` alias). The returned
    ``lance.LanceDataset`` exposes ``.scanner(filter=..., columns=...)`` for BTREE
    index pushdown."""
    import lance

    reg = get_registry()
    uri = reg.get(name)
    if uri is None:
        raise KeyError(f"unknown dataset {name!r}; call list_datasets to see what is registered")
    return lance.dataset(uri, storage_options=r2_storage_options())


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


def _register_datasets(cur, names: Iterable[str]) -> None:
    """JIT-bind ONLY the named Lance datasets as same-named DuckDB relations on a
    cursor — the performance gate. The ``LanceDataset`` is registered directly —
    NOT a one-shot ``RecordBatchReader``, which DuckDB exhausts after a single
    scan and then silently returns empty on a second reference (a self-join or
    reused CTE would quietly yield wrong rows). Registered as the dataset, DuckDB
    re-scans safely, stays lazy, and can push projections/filters into Lance. Each
    call opens the latest committed version. A name absent from the registry is
    skipped — DuckDB then raises a clear "table not found" for it, never a silent
    empty."""
    reg = get_registry()
    so = r2_storage_options()
    import lance

    for name in names:
        uri = reg.get(name)
        if uri is None:
            continue
        cur.register(name, lance.dataset(uri, storage_options=so))


def query(sql: str, datasets: Iterable[str] = (), max_rows: int = MAX_QUERY_ROWS) -> dict[str, Any]:
    """Execute raw ANSI SQL with ONLY ``datasets`` bound as Lance relations (plus
    any ``s3://`` transport Parquet the SQL reads directly). Returns
    ``{"columns", "rows", "row_count", "truncated"}``. Runs on a fresh cursor with
    freshly-registered (latest-version) Lance relations; the cursor is closed after."""
    cur = get_connection().cursor()
    try:
        _register_datasets(cur, datasets)
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
