"""Unified data context for the GTM MCP gateway — one shared DuckDB connection
over the Gen-3 R2 data sink, a runtime-discovered dataset registry, the Lance
pushdown plumbing, and the live hq-x control-plane Postgres attached in-session.

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

HQ-X POSTGRES ATTACH (Directive 18). The same shared connection ATTACHes the live
hq-x control-plane Postgres as the ``hqx`` catalog (``HQX_DB_URL_POOLED``, TLS
enforced), so an agent can JOIN a Lance R2 dataset against ``hqx.ops.*`` relational
state in a single statement (companies ⋈ ops.exclusions to subtract a suppression
list, etc.). ``referenced_datasets`` strips ``hqx.*`` references before Lance-name
matching, so a Postgres-qualified table is resolved by the attached engine over the
wire — never opened as a Lance manifest. SAFETY: the attach is read/write per the
directive, but the agent-facing raw-SQL path is read-only-gated (``assert_read_only``
in ``query``) — a single read statement only, no mutation / DDL / ATTACH / COPY /
extension-load / txn-control / ``;``-chaining. The ONLY write path into hq-x is the
structured, parameterized, transaction-bounded upsert in ``tools/ops.py``.

SECRET MAPPING (exact, per directive). The fleet exposes R2 credentials as
``R2_ACCESS_KEY_ID`` / ``R2_SECRET_ACCESS_KEY`` / ``R2_ENDPOINT`` (Render
service env vars, mirroring the ``r2-credentials`` Modal secret and the
``hq-x/prd`` Doppler config). The Lance ``storage_options``, the boto3 listing
client, and the DuckDB ``CREATE SECRET (TYPE S3)`` are all built from exactly
those three variables (``R2_ACCOUNT_ID`` is accepted as a fallback to derive the
endpoint, matching every worker in ``pipelines/``).

SERVING-TIER STATE (warm, process-resident — NOT per-call fresh). The gateway is a
serving mirror over the Lance/R2 system of record, so the hot artifacts are held in
memory and reused across tool calls; the SoR itself is never mutated. Four objects carry
that state, each a lazy singleton built once and shared by every tool:

  • ``_con`` — the single in-memory DuckDB connection (R2 S3 secret + hq-x Postgres
    ATTACH configured once); per-query work runs on a ``.cursor()`` of it, never a new
    connection (``get_connection``).
  • ``_registry`` — the discovered ``name → uri`` map, built once and self-refreshed on a
    coarse TTL (``GTM_REGISTRY_TTL_S``) so a newly landed dataset / ``snapshot=YYYY-MM``
    partition is picked up without a restart (``get_registry``).
  • ``_tls.s3`` — a per-thread boto3 R2 client (clients are fork/thread-unsafe to share).
  • ``_handle_cache`` — warm, index-resident ``LanceDataset`` handles keyed by uri
    (``_cached_dataset``). pylance does NOT share a resident scalar index across handles,
    so reusing the OPENED handle is what keeps point lookups in tens of ms instead of
    re-loading the BTREE from R2 every call (~78% of a cold lookup — see
    ``GTM_MCP_RECALL_ARCHITECTURE_DIAGNOSTIC.md``).

FRESHNESS is bounded by a PER-DATASET TTL tiered on mutation cadence (``_ttl_for``):
snapshot-partitioned roots (immutable within a month → a new month is a new uri, a natural
miss) get a long TTL; flat in-place datasets (rewritten every few days) a short one. On a
miss the dataset's ``_versions/`` commit log is listed twice and required EQUAL before the
handle is trusted — a guard against the non-atomic publish swap (a mid-swap partial is
served fresh and left UNcached, never adopted). The SoR is durability-first; this tier is a
latency mirror with cadence-matched invalidation, not a per-request re-validation.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
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

# Attached hq-x control-plane Postgres catalog name (Directive 18). The live
# operational DB is ATTACHed to the shared DuckDB connection under this alias, so
# an agent can JOIN Lance R2 datasets against ``hqx.ops.*`` relational state in one
# session. The relational read/join path is DuckDB-over-the-attach; the only WRITE
# path is the structured, parameterized upsert in tools/ops.py (psycopg).
HQX_ALIAS = "hqx"

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

# ── Read-only guard for the raw-SQL path (Directive 18 §4) ───────────────────
# The gateway now ATTACHes a LIVE read/write production Postgres. The agent-facing
# raw-SQL tool (execute_audience_query → query()) is therefore confined to a SINGLE
# read-only statement: the only legal entry points are these leading keywords, and
# the deny-list below is rejected as a whole-token anywhere in the (string/comment-
# stripped) text — so no mutation, DDL, extension load, transaction control, or
# ``;``-chained second statement can reach DuckDB or the attached engine. The ONLY
# mutation path into hq-x is the structured upsert in tools/ops.py.
_RO_ALLOW_LEAD = frozenset(
    {"SELECT", "WITH", "FROM", "TABLE", "VALUES", "EXPLAIN", "DESCRIBE", "DESC",
     "SHOW", "SUMMARIZE", "PIVOT", "UNPIVOT"}
)
_RO_DENY = frozenset(
    {"INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "DROP", "ALTER", "CREATE",
     "TRUNCATE", "REPLACE", "ATTACH", "DETACH", "COPY", "INSTALL", "LOAD", "SET",
     "RESET", "PRAGMA", "CALL", "EXPORT", "IMPORT", "GRANT", "REVOKE", "VACUUM",
     "CHECKPOINT", "USE", "BEGIN", "START", "COMMIT", "ROLLBACK", "PREPARE",
     "EXECUTE", "DEALLOCATE"}
)

_lock = threading.Lock()
_con: Any = None  # the single shared duckdb.DuckDBPyConnection (lazy singleton)
_hqx_attached = False  # set under _lock the first time get_connection() runs

_registry_lock = threading.Lock()
_registry: dict[str, str] | None = None  # discovered name → s3:// uri (lazy singleton)
_registry_built_at = 0.0  # monotonic stamp of the last registry build (TTL self-refresh)
_tls = threading.local()  # per-thread boto3 client (clients are not shared across threads)

# ── Warm dataset-handle cache (serving-tier latency, Directive: recall) ───────
# WHY. The per-call "open fresh" path below reloads the Lance scalar index from R2
# on EVERY tool call — ~78% of a cold point lookup, and pylance does NOT share a
# resident index across LanceDataset handles (only object reuse avoids the reload).
# The load-bearing datasets mutate at days-to-monthly cadence, so a per-request
# re-validation is a continuous tax against a rare event. This cache holds the
# OPENED handle (index resident) keyed by URI, refreshed on a coarse TTL — turning
# point lookups from ~1.5 s into tens of ms co-located, gateway-wide (every tool
# funnels through open_dataset / _register_datasets). It is a SERVING-tier mirror;
# the Lance/R2 system of record is untouched.
#
# FRESHNESS. Bounded by a PER-DATASET TTL tiered on mutation cadence (see _ttl_for).
# On a miss the dataset's _versions/ commit log is listed TWICE and required equal
# before the handle is trusted: a guard against the non-atomic _publish_full_swap
# window (delete-then-recopy) — a mid-swap partial is served fresh+UNCACHED, never
# adopted. A new snapshot=YYYY-MM month is picked up because get_registry()
# self-refreshes on its own TTL and the new month resolves to a NEW uri (a fresh
# cache entry), while expired entries are swept on write.
_handle_cache: dict[str, tuple[float, Any]] = {}  # uri → (refresh_deadline_monotonic, LanceDataset)
_handle_lock = threading.Lock()
_REGISTRY_TTL_S = float(os.environ.get("GTM_REGISTRY_TTL_S", "1800"))

# Per-dataset handle TTL, tiered by mutation cadence (grounded in the active sink's
# last-write times). The URI shape encodes the mutation semantics:
#   * snapshot-partitioned (active/<feed>/snapshot=YYYY-MM/) is IMMUTABLE within a
#     partition — a new month lands at a NEW uri (natural miss via the registry
#     refresh), so same-uri revalidation only guards the rare same-month rebuild →
#     a long TTL is safe (GTM_HANDLE_TTL_SNAPSHOT_S, default 1h).
#   * flat / in-place (active/<feed>/) is rewritten AT THE SAME uri every few days to
#     hourly (companies/people/awards ~days; firmographics_blitz the hottest at ~daily)
#     → a short TTL bounds staleness (GTM_HANDLE_TTL_FLAT_S, default 10m).
# GTM_HANDLE_TTL_OVERRIDES gives surgical per-dataset control with no code change:
# a comma list of "uri-substring=seconds" (e.g. "firmographics_blitz=120,pdl_=300").
_TTL_SNAPSHOT_S = float(os.environ.get("GTM_HANDLE_TTL_SNAPSHOT_S", "3600"))
_TTL_FLAT_S = float(os.environ.get("GTM_HANDLE_TTL_FLAT_S", "600"))


def _parse_ttl_overrides(raw: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for part in raw.split(","):
        key, sep, val = part.strip().partition("=")
        if sep and key.strip():
            try:
                out[key.strip()] = float(val.strip())
            except ValueError:
                log.warning("gtm-mcp: ignoring bad GTM_HANDLE_TTL_OVERRIDES entry %r", part)
    return out


_TTL_OVERRIDES = _parse_ttl_overrides(os.environ.get("GTM_HANDLE_TTL_OVERRIDES", ""))


def _ttl_for(uri: str) -> float:
    """Resolve the handle TTL for a dataset by mutation cadence: an explicit override
    (substring match) wins; else the snapshot tier (immutable-in-partition, long) for a
    ``/snapshot=`` uri; else the flat tier (in-place rewrite, short)."""
    for substr, ttl in _TTL_OVERRIDES.items():
        if substr in uri:
            return ttl
    return _TTL_SNAPSHOT_S if "/snapshot=" in uri else _TTL_FLAT_S


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
    enrich with it when present and degrade gracefully when not.

    Uses the per-thread cached client (``_thread_s3``), matching the discovery /
    ``_versions`` listing paths — never a fresh per-call ``_s3_client()`` build."""
    try:
        return _thread_s3().get_object(Bucket=BUCKET, Key=key)["Body"].read()
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
    # REPOINT: the "people" relation resolves to the canonical dataset (one row per human,
    # source_platform extracted to the person_source_platforms sidecar). Discovery may also list
    # the raw "people_canonical" name; this override makes the documented "people" alias the SoR.
    reg["people"] = f"{ACTIVE_URI}/people_canonical/"
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
    """The in-memory dataset registry (``name → uri``), built on first use and
    self-refreshed every ``GTM_REGISTRY_TTL_S`` (default 30 min) so a newly landed
    dataset OR a new ``snapshot=YYYY-MM`` partition is discovered without a process
    restart — the prerequisite for the warm-handle cache to ever roll to a new
    month. ``refresh=True`` forces a rebuild now. Returns a copy so callers can't
    mutate it."""
    global _registry, _registry_built_at
    now = time.monotonic()
    if _registry is None or refresh or (now - _registry_built_at) > _REGISTRY_TTL_S:
        with _registry_lock:
            if _registry is None or refresh or (time.monotonic() - _registry_built_at) > _REGISTRY_TTL_S:
                _registry = _build_registry()
                _registry_built_at = time.monotonic()
    return dict(_registry)


def dataset_names() -> list[str]:
    """Sorted canonical dataset names (aliases excluded) — what ``list_datasets``
    advertises and what an agent names in SQL."""
    return sorted(name for name in get_registry() if name not in ALIASES)


# ── Read-only SQL guard + Postgres passthrough (Directive 18 §§3–4) ──────────
# A qualified reference into the attached Postgres: ``hqx.<schema>.<table>`` or
# ``hqx.<table>`` (each segment a bare or double-quoted identifier). Whitespace
# around the dots is tolerated; a leading ``[\w.]`` is rejected so this never fires
# mid-identifier.
_HQX_REF = re.compile(
    r"(?<![\w.])" + re.escape(HQX_ALIAS) + r"\s*\.\s*(?:\"[^\"]+\"|\w+)"
    r"(?:\s*\.\s*(?:\"[^\"]+\"|\w+))?",
    re.IGNORECASE,
)


def _strip_sql_noise(sql: str) -> str:
    """Blank out block/line comments and string + double-quoted-identifier literals
    so keyword/statement scanning sees only structural SQL — a column named
    ``update_date`` or a literal ``'DROP'`` can never trip the guard, and a ``;`` or
    keyword hidden inside a string can never evade it."""
    s = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)   # block comments
    s = re.sub(r"--[^\n]*", " ", s)                   # line comments
    s = re.sub(r"\$\$.*?\$\$", " ", s, flags=re.S)    # PG dollar-quoted bodies
    s = re.sub(r"'(?:''|[^'])*'", " ", s)             # single-quoted strings ('' escape)
    s = re.sub(r'"(?:""|[^"])*"', " ", s)             # double-quoted identifiers
    return s


def assert_read_only(sql: str) -> None:
    """Raise ``ValueError`` unless ``sql`` is a single read-only statement.

    The directive's safety rail (Directive 18 §4): the raw-SQL tool may read the
    Lance plane and the attached ``hqx`` catalog, but must never mutate. Enforcement
    is allow-list + deny-list over the noise-stripped text — the statement must begin
    with a read keyword (``_RO_ALLOW_LEAD``), must be the ONLY statement (no
    ``;``-chaining), and must not contain any forbidden token (``_RO_DENY``) anywhere
    (which also kills a data-modifying CTE like ``WITH x AS (DELETE …)``). All hq-x
    writes go through the structured ``save_campaign_audience`` upsert instead."""
    scrubbed = _strip_sql_noise(sql)
    statements = [s for s in scrubbed.split(";") if s.strip()]
    if not statements:
        raise ValueError("empty SQL")
    if len(statements) > 1:
        raise ValueError(
            "only a single read-only statement is permitted; "
            "';'-chained statements are blocked"
        )
    tokens = statements[0].replace("(", " ").split()
    lead = tokens[0].upper() if tokens else ""
    if lead not in _RO_ALLOW_LEAD:
        raise ValueError(
            f"the audience SQL path is read-only: a statement must begin with one of "
            f"{', '.join(sorted(_RO_ALLOW_LEAD))} (got {lead or '∅'!r}). "
            "Use save_campaign_audience for ops.* writes."
        )
    for kw in _RO_DENY:
        if re.search(r"(?<!\w)" + kw + r"(?!\w)", scrubbed, re.IGNORECASE):
            raise ValueError(
                f"blocked keyword {kw!r}: the audience SQL path is read-only — no "
                "INSERT/UPDATE/DELETE/DDL/ATTACH/COPY/extension-load/txn-control. "
                "Use save_campaign_audience for ops.* writes."
            )


def references_postgres(sql: str) -> bool:
    """True if the SQL names an attached-Postgres relation (``hqx.<schema>.<table>``
    or ``hqx.<table>``). Such a query is served by the attached engine over the wire,
    NOT the Lance plane — ``query()`` requires the attach to be live for it
    (Directive 18 §3)."""
    return _HQX_REF.search(_strip_sql_noise(sql)) is not None


def _strip_hqx_refs(sql: str) -> str:
    """Remove ``hqx.*`` qualified references before Lance-name matching so a
    Postgres-qualified table can never be mistaken for a Lance manifest and trigger a
    needless R2 open — the Directive 18 §3 passthrough guarantee. (The name-boundary
    class already rejects dotted matches; this makes the intent explicit and total.)"""
    return _HQX_REF.sub(" ", sql)


def referenced_datasets(sql: str) -> set[str]:
    """The performance gate: which registered datasets does this SQL reference?

    Whole-token match of every registry name against the query text (the
    directive's "simple regex / whole-word match"). The boundary class rejects
    word chars, ``.`` and ``/`` on either side, so ``agency`` never matches inside
    ``subtier_agency`` and a flat name never matches a path suffix; nested names
    (``usaspending/award_search``) match when written double-quoted, as DuckDB
    requires. Ambiguous mentions over-match (a needless manifest open) rather than
    under-match (a broken query) — the safe direction.

    ``hqx.*`` references are stripped first (Directive 18 §3) so a Postgres-qualified
    table is resolved by the attached engine, never as a Lance dataset."""
    scrubbed = _strip_hqx_refs(sql)
    found: set[str] = set()
    for name in get_registry():
        pattern = r"(?<![\w./])" + re.escape(name) + r"(?![\w./])"
        if re.search(pattern, scrubbed, re.IGNORECASE):
            found.add(name)
    return found


def _versions_listing(uri: str) -> list[tuple] | None:
    """Sorted ``(key, etag, size)`` of every object under the dataset's ``_versions/``
    commit log (the Lance manifest dir). The freshness fingerprint AND the swap-window
    probe. ``None`` on listing failure or an empty/absent log."""
    prefix = uri.replace(f"s3://{BUCKET}/", "", 1).rstrip("/") + "/_versions/"
    try:
        s3 = _thread_s3()
        out: list[tuple] = []
        token: str | None = None
        while True:
            kw: dict[str, Any] = {"Bucket": BUCKET, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                out.append((o["Key"], o.get("ETag"), o.get("Size")))
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
        return sorted(out) if out else None
    except Exception as exc:  # noqa: BLE001 — never let a listing hiccup break or poison the cache
        log.warning("gtm-mcp: _versions listing failed for %s: %s", uri, exc)
        return None


def _cached_dataset(uri: str):
    """Return a warm, TTL-validated ``LanceDataset`` for ``uri`` — reusing the cached
    handle (scalar index resident, no R2 reload) until its TTL lapses. On a miss the
    ``_versions/`` log is listed twice and required EQUAL before the new handle is
    cached (mid-swap partial → served fresh, not adopted). Concurrent first-callers may
    each open once (cost-only, self-correcting). The SoR is never touched."""
    import lance

    now = time.monotonic()
    with _handle_lock:
        hit = _handle_cache.get(uri)
        if hit is not None and hit[0] > now:
            return hit[1]

    a = _versions_listing(uri)
    stable = a is not None and a == _versions_listing(uri)
    ds = lance.dataset(uri, storage_options=r2_storage_options())
    if stable:
        with _handle_lock:
            # sweep expired entries (bounds growth as snapshots roll monthly), then cache
            for k in [k for k, v in _handle_cache.items() if v[0] <= now]:
                del _handle_cache[k]
            _handle_cache[uri] = (now + _ttl_for(uri), ds)
    return ds


def open_dataset_uri(uri: str):
    """Warm-cached open by explicit ``s3://`` URI — for snapshot-partitioned datasets
    (``provider_360/snapshot=YYYY-MM``) addressed directly rather than by registry name."""
    return _cached_dataset(uri)


def open_dataset(name: str):
    """Open a registered dataset (latest committed Lance version) via the warm-handle
    cache. ``name`` is any registry key (a canonical name or the ``awards`` alias). The
    returned ``lance.LanceDataset`` exposes ``.scanner(filter=..., columns=...)`` for
    BTREE/BITMAP index pushdown; reuse keeps the index resident across calls."""
    reg = get_registry()
    uri = reg.get(name)
    if uri is None:
        raise KeyError(f"unknown dataset {name!r}; call list_datasets to see what is registered")
    return _cached_dataset(uri)


def scan_table(
    name: str,
    *,
    columns: list[str] | None = None,
    filter: str | None = None,  # noqa: A002 — mirrors the Lance scanner kwarg name
    limit: int | None = None,
    offset: int | None = None,
):
    """Materialize a projected/filtered slice of a registered dataset as a pyarrow Table,
    pushing the predicate + projection through the LANCE scanner's own filter engine
    (BTREE/BITMAP pushdown) — NOT DuckDB's substrait pushdown.

    WHY (the correctness rail). Registering a ``LanceDataset`` as a DuckDB relation and
    letting DuckDB push a WHERE filter down into Lance routes the predicate through the
    DuckDB→substrait→Lance bridge, which PANICS on some predicate/column-index shapes
    (``lance-datafusion/substrait.rs`` index-out-of-bounds — observed on a
    ``physical_address_state`` equality over ``entity_profile_gold``). The proven path
    used everywhere else in the gateway (audience point-lookups, govcon ANN) is the Lance
    scanner directly: ``ds.scanner(filter=..., columns=...).to_table()``. This helper is
    that path, generalized — for the federal aggregations, which scan-then-GROUP-BY in
    DuckDB OVER the returned Arrow table (so DuckDB only ever sees a materialized Arrow
    relation, never a registered Lance manifest it would try to push into). Column
    projection also cuts the scan to the few columns an aggregation needs.

    ``filter`` is a Lance scanner predicate string (SQL-ish, e.g.
    ``"primary_naics LIKE '3364%' AND is_active = TRUE"``). The warm handle cache keeps
    the scalar index resident across calls."""
    ds = open_dataset(name)
    kwargs: dict[str, Any] = {}
    if columns is not None:
        kwargs["columns"] = columns
    if filter:
        kwargs["filter"] = filter
    if limit is not None:
        kwargs["limit"] = int(limit)
    if offset is not None:
        kwargs["offset"] = int(offset)
    return ds.scanner(**kwargs).to_table()


# ── hq-x Postgres attach (Directive 18 §1) ──────────────────────────────────
def _hqx_dsn() -> str | None:
    """The hq-x Postgres DSN (pooled / Supavisor) with TLS enforced — the exact form
    the DuckDB ``postgres`` scanner and psycopg both hand to libpq (Supabase requires
    TLS). ``None`` when ``HQX_DB_URL_POOLED`` is unset (local / R2-only dev). Byte-for-
    byte the same construction the data-plane workers use (pipelines/* ``_hqx_dsn``)."""
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return None
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def hqx_dsn() -> str | None:
    """Public accessor for the TLS-enforced hq-x DSN — the structured write path
    (tools/ops.py) opens its own psycopg connection on this, independent of the
    shared DuckDB attach."""
    return _hqx_dsn()


def hqx_attached() -> bool:
    """True once the hq-x Postgres has been ATTACHed to the shared DuckDB connection.
    The flag is set the first time ``get_connection()`` runs, so callers that need a
    definitive answer should ensure the connection is built first."""
    return _hqx_attached


def _attach_hqx(con) -> bool:
    """ATTACH the hq-x control-plane Postgres to ``con`` as the ``hqx`` catalog
    (Directive 18 §1) so hybrid Lance⋈Postgres joins resolve in one session.

    Read/write attach, exactly as the directive specifies (``ATTACH … (TYPE
    postgres)`` — no ``READ_ONLY``); mutation is nonetheless withheld from arbitrary
    SQL by the read-only guard on query(), and the sole write path is the structured
    upsert in tools/ops.py. The ``postgres`` extension is auto-installed on first use
    (same pattern as ``httpfs``).

    Best-effort: a missing DSN or an attach failure logs and leaves the gateway
    R2-only (every Lance tool keeps working); ``hqx.*`` queries then fail with a
    clear "not attached" error rather than taking down the whole plane. The DSN
    carries the password — it is single-quote-escaped into the literal and is NEVER
    logged."""
    dsn = _hqx_dsn()
    if not dsn:
        log.warning(
            "gtm-mcp: HQX_DB_URL_POOLED unset — hq-x not attached. Lance/R2 tools "
            "work; hqx.* relational joins and ops.* writes are unavailable."
        )
        return False
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(
            f"ATTACH '{dsn.replace(chr(39), chr(39) * 2)}' AS {HQX_ALIAS} (TYPE postgres);"
        )
        log.info("gtm-mcp: attached hq-x Postgres as '%s' (read/write).", HQX_ALIAS)
        return True
    except Exception as exc:  # noqa: BLE001 — a PG attach failure must not sink the R2 gateway
        log.warning("gtm-mcp: hq-x Postgres attach failed (Lance/R2 tools still up): %s", exc)
        return False


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


def _ensure_resource_limits(con) -> None:
    """Set an explicit ``memory_limit`` + ``temp_directory`` for out-of-core safety.

    The federal aggregations (tools/federal.py) GROUP BY over the full 1.54M-row
    ``entity_profile_gold`` scan; an unbounded in-memory connection can spike RSS on a
    small serving instance. Capping ``memory_limit`` and pointing ``temp_directory`` at
    spillable disk lets DuckDB stream the grouped aggregate out-of-core instead of OOMing.
    Both are env-tunable (``GTM_DUCKDB_MEMORY_LIMIT`` default 4GB,
    ``GTM_DUCKDB_TEMP_DIR`` default ``/tmp/gtm_mcp_duckdb``) and best-effort — a set
    failure logs and leaves DuckDB's defaults rather than sinking the gateway."""
    mem = os.environ.get("GTM_DUCKDB_MEMORY_LIMIT", "4GB")
    tmp = os.environ.get("GTM_DUCKDB_TEMP_DIR", "/tmp/gtm_mcp_duckdb")
    try:
        os.makedirs(tmp, exist_ok=True)
        con.execute(f"SET memory_limit = '{mem}';")
        con.execute(f"SET temp_directory = '{tmp}';")
    except Exception as exc:  # noqa: BLE001 — resource hint, never load-bearing
        log.warning("gtm-mcp: could not set DuckDB resource limits (%s); using defaults.", exc)


def get_connection():
    """The single, shared in-memory DuckDB connection, configured for R2 S3 AND with
    the hq-x Postgres ATTACHed (Directive 18) on first use. Per-query work runs on a
    ``.cursor()`` of this connection so concurrent SSE tool calls never collide on one
    execution context while still sharing the catalog, the R2 secret, and the ``hqx``
    attach (DuckDB attaches live on the database instance, so every cursor sees it)."""
    global _con, _hqx_attached
    if _con is None:
        with _lock:
            if _con is None:
                import duckdb

                con = duckdb.connect(":memory:")
                con.execute("PRAGMA threads=4;")
                _ensure_resource_limits(con)
                _configure_r2_s3(con)
                _hqx_attached = _attach_hqx(con)
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
    for name in names:
        uri = reg.get(name)
        if uri is None:
            continue
        # Register the warm-cached handle (index resident) — NOT a fresh open. DuckDB
        # re-scans the LanceDataset lazily and pushes projections/filters into Lance.
        cur.register(name, _cached_dataset(uri))


def query(
    sql: str,
    datasets: Iterable[str] = (),
    max_rows: int = MAX_QUERY_ROWS,
    *,
    read_only: bool = True,
) -> dict[str, Any]:
    """Execute raw ANSI SQL with ONLY ``datasets`` bound as Lance relations (plus any
    ``s3://`` transport Parquet the SQL reads directly), and with the attached ``hqx``
    Postgres catalog visible for hybrid joins. Returns ``{"columns", "rows",
    "row_count", "truncated"}``. Runs on a fresh cursor with freshly-registered
    (latest-version) Lance relations; the cursor is closed after.

    ``read_only`` (default True) enforces the Directive 18 §4 safety rail: a single
    read statement, no mutation/DDL/ATTACH/COPY/extension-load. A query naming
    ``hqx.*`` is served by the attached Postgres and requires the attach to be live —
    otherwise it fails fast with a clear message instead of a cryptic catalog error."""
    if read_only:
        assert_read_only(sql)
    con = get_connection()  # builds + attaches hqx on first use; sets _hqx_attached
    if references_postgres(sql) and not _hqx_attached:
        raise RuntimeError(
            "query references hqx.* but the hq-x Postgres is not attached "
            "(set HQX_DB_URL_POOLED). Lance/R2 datasets remain queryable."
        )
    cur = con.cursor()
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
