"""Compute worker — Parallel.ai ENRICHMENT (Directive 24, workflow 1 of 3).

The ``parallel-enrich`` Modal app. Endpoint-less; spawned by the Universal
Dispatcher (core/modal_dispatcher.py). Clean-room data plane: DuckDB does 100% of the
content+basis projection/cast, Lance is written straight to R2 (merge_insert on
``company_id``). No Iceberg, no Polaris. Contract: §0 of
docs/reference/DIRECTIVE_24_PARALLEL_ENRICHMENT_CAPABILITY.md (authoritative, settled).

WHAT THIS WORKER DOES
    Turns a resolved company audience into typed, cited per-entity columns. The
    gtm-agent authors an ``output_schema`` (pure JSON-Schema data columns) and a tier
    (lite|base|core). This worker:
      1. resolves the audience SQL (corex.audience) → company set {company_id,
         company_name, normalized_domain},
      2. builds one Parallel Task per company — input {company_name,
         company_website = normalized_domain (BARE host, NO scheme)}; null-domain rows
         go name-only and are COUNTED (never a fully-empty billing input),
      3. dispatches via Task Groups (default_task_spec = ONE wrapped
         {type:"json",json_schema:…}; ≤1000 inputs/POST),
      4. waits Parallel's way (§0): poll GET /v1/tasks/groups/{id} until
         is_active==false, then read task_run_status_counts, then fetch results via
         GET /runs?include_output=true,
      5. DuckDB projects content → typed columns + <field>__confidence (matched from
         Basis by field name) + _basis (full citations+reasoning) + spec/run/processor/
         enriched_at; arrays → LIST, nested → JSON; MISSING/renamed keys are null-filled
         (drift never throws mid-batch),
      6. lance.write_dataset(merge_insert "company_id") to
         s3://data-sink/active/enrichment/<spec>/ ; BTREE on company_id (+ normalized_domain).
         The COMPLETE Parallel result per group-run is ALSO captured VERBATIM to
         s3://data-sink/active/enrichment_raw/<spec>/ (keyed by parallel_run_id, append-only) —
         written independently of the typed projection so the provider's raw is never discarded,
      7. CONFIDENCE GATE — null/low/medium per-field cells → ops.parallel_review (the
         VALUE still lands); high is trusted,
      8. land completed rows, persist failed company_ids; ops.parallel_runs ledger;
         flat-JSON callback → Trigger waitpoint.

    Single-entity SMOKE path (``smoke_one``): POST /v1/tasks/runs + blocking
    GET /result?timeout=600 — for inline inspection of one company's content + Basis.

    EXPLICIT-LIST + OPERATOR-PROMPT path (govcon subawardee dossiers): ``enrich_companies``
    accepts ``companies_explicit`` (caller-keyed entity list — the key, e.g. a UEI, rides in
    the company_id slot end-to-end) and ``prompt_template`` (an operator-authored prompt
    rendered per entity via ``{{token}}`` substitution and sent as the STRING task input).
    Launcher: ``modal run pipelines/parallel/enrich.py::run_explicit`` with mode=dry|smoke|live
    (dry renders without spend; smoke bills one run; live dispatches the group).

TIER CEILING = ``core``. lite|base|core settle in seconds-to-minutes → bounded Modal
burst (the internal group poll). ``ultra`` (hours) needs Parallel's per-run webhook →
trigger token (deferred per §0/§9) and is REJECTED here.

CONTROL PLANE (Trigger v4 durable callback)
    Spawned with ``trigger_callback_url``. On terminal state it writes ops.parallel_runs
    via psycopg (HQX_DB_URL_POOLED) and POSTs a FLAT JSON body to the waitpoint url.

SECRETS
    parallel-api (PARALLEL_API_KEY) · r2-credentials (Lance writes) · hqx-postgres (ops.*).
    NOTE: the ``parallel-api`` Modal secret does not exist yet — mint it from Doppler
    core-x/prd (key PARALLEL_API_KEY) before ``modal deploy``.

    modal deploy pipelines/parallel/enrich.py
    modal run    pipelines/parallel/enrich.py::init_ops    # create ops.parallel_* (HQX)
"""

from __future__ import annotations

import os
from typing import Any

import modal

from core.parallel_client import (
    add_group_runs,
    create_task_group,
    create_task_run,
    get_group,
    get_group_runs,
    get_task_result,
    wrap_json_schema,
)

# ── Landing target (per-spec dataset root; env-overridable) ──────────────────────────────
_ACTIVE = "s3://data-sink/active"
ENRICHMENT_ROOT = os.environ.get("PARALLEL_ENRICHMENT_ROOT", f"{_ACTIVE}/enrichment")
# Raw capture — the COMPLETE Parallel result per group-run ({run, output, …}) lands here
# VERBATIM, nothing dropped. Per-spec root (parity with the typed dataset); keyed by the
# Parallel run_id (unique per run → append-only: a re-enrich never overwrites a prior run's
# raw). We do NOT discard what the provider returns; the raw payload is the SoR for a run.
ENRICHMENT_RAW_ROOT = os.environ.get("PARALLEL_ENRICHMENT_RAW_ROOT", f"{_ACTIVE}/enrichment_raw")

# Tier ceiling (§0/§9): 'ultra' needs the per-run webhook path (hours-long runs) and stays
# rejected. 'pro' is admitted for explicit-list dossier runs — it settles within the group
# poll budget + the widened fn timeout (pro is minutes-class, not hours-class; the original
# core cap was a latency choice, not a webhook constraint).
ALLOWED_PROCESSORS = ("lite", "base", "core", "pro")
PROCESSOR_CEILING = "pro"

# Group completion poll — bounded under the Modal fn timeout. lite/base/core settle in
# seconds-to-minutes; this is the icypeas short-poll class, NOT exa's 50-min hold (§0).
GROUP_POLL_BUDGET_S = int(os.environ.get("PARALLEL_GROUP_POLL_BUDGET_S", str(50 * 60)))
GROUP_POLL_BACKOFF_S = (5, 10, 15, 20, 30)

# Confidence gate (§7): land the value; flag these for review. 'high' is trusted.
REVIEW_CONFIDENCES = frozenset({None, "low", "medium"})

# Lance fragment sizing — fleet defaults (02_lancedb_storage.md §2.3); net-new → v2.1.
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"

# ── ops DDL — verbatim mirror of the .sql siblings; self-applied before every terminal
# write so the tables bootstrap on the first real run. ───────────────────────────────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.parallel_specs (
    spec_id       text PRIMARY KEY,
    workflow      text NOT NULL CHECK (workflow IN ('enrich','deep_research','search')),
    processor     text NOT NULL,
    output_schema jsonb,
    objective     text,
    grain         text,
    dataset_uri   text NOT NULL,
    result_key    text NOT NULL DEFAULT 'company_id',
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS parallel_specs_workflow_idx ON ops.parallel_specs (workflow);

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
CREATE INDEX IF NOT EXISTS parallel_runs_group_idx    ON ops.parallel_runs (group_id);
CREATE INDEX IF NOT EXISTS parallel_runs_recorded_idx ON ops.parallel_runs (recorded_at DESC);

CREATE TABLE IF NOT EXISTS ops.parallel_review (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spec_id     text NOT NULL,
    run_id      text,
    company_id  text NOT NULL,
    field       text NOT NULL,
    confidence  text,
    resolved    boolean NOT NULL DEFAULT false,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS parallel_review_spec_idx       ON ops.parallel_review (spec_id);
CREATE INDEX IF NOT EXISTS parallel_review_company_idx    ON ops.parallel_review (company_id);
CREATE INDEX IF NOT EXISTS parallel_review_unresolved_idx ON ops.parallel_review (resolved) WHERE NOT resolved;
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "psycopg[binary]>=3.2",  # ops.* state + audience resolve
    "requests>=2.32",        # Parallel HTTP (via core.parallel_client) + Trigger callback
    "boto3>=1.35",           # active-sink discovery for audience SQL resolution
).env(
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source(
    "core.parallel_client"  # ship the shared Parallel HTTP client into the container
)

# Own Modal app per workflow — the three workers deploy independently and each resolves
# under its own name in the Universal Dispatcher (app_name must match the trigger POST).
# Keeping the apps distinct is what lets all three coexist (one shared app name would let
# the last `modal deploy` clobber the others' function registry).
app = modal.App("parallel-enrich", image=image)


# ── R2 plumbing (byte-identical to the fleet convention) ─────────────────────────────────
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the r2-credentials secret."""
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
    """TLS-enforced hq-x DSN (HQX_DB_URL_POOLED). None when unset (manual local run)."""
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return None
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


# ── Domain anchor — IDENTICAL expression to the companies spine (bare host, no scheme). ──
def _bare_domain(raw: str | None) -> str | None:
    """lower/trim → strip scheme → strip leading www. → strip path/query → strip trailing
    dots → None if emptied. The bare host §0 wants as ``company_website`` (e.g. ``un.org``,
    not ``https://www.un.org/x``). Pure-Python mirror of pipelines' _normalized_domain."""
    import re

    d = (raw or "").strip().lower()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    d = d.split("/", 1)[0]
    d = d.rstrip(".")
    return d or None


# ── Explicit-list resolution (caller-supplied entity set; no audience round-trip) ─────────
def _normalize_explicit(companies_explicit: list[dict], limit: int) -> list[dict]:
    """Normalize a caller-supplied entity list → the worker's company shape.

    Each entry needs a key (``key`` | ``uei`` | ``company_id`` — first present wins) and a
    ``company_name``; ``domain``/``normalized_domain`` is optional. The key rides in the
    ``company_id`` slot END-TO-END (metadata round-trip, typed rows, merge_insert, BTREE) —
    for a UEI-keyed spec the ``company_id`` COLUMN of that spec's dataset carries the UEI.
    That is a documented per-spec key semantic, not a spine company_id. Every OTHER string
    field on the entry is preserved verbatim so ``{{token}}`` prompt rendering can use it
    (e.g. ``state``, ``federal_context``)."""
    out: list[dict] = []
    for e in companies_explicit[: int(limit)]:
        if not isinstance(e, dict):
            continue
        key = e.get("key") or e.get("uei") or e.get("company_id")
        if not key:
            continue
        row = {k: v for k, v in e.items() if isinstance(v, str)}
        row["company_id"] = str(key)
        row["company_name"] = e.get("company_name") or e.get("name") or ""
        row["normalized_domain"] = e.get("normalized_domain") or e.get("domain")
        out.append(row)
    return out


def _render_prompt(template: str, company: dict) -> str:
    """Render the operator's EXACT prompt for one entity: every ``{{field}}`` token whose
    field exists on the company dict is substituted (``company_website`` ← the bare
    domain); unknown tokens are left intact (visible in smoke output, never silently
    blanked). The template text itself is never altered beyond token substitution."""
    rendered = template
    values = dict(company)
    dom = _bare_domain(company.get("normalized_domain"))
    values["company_website"] = dom or ""
    for k, v in values.items():
        if isinstance(v, str):
            rendered = rendered.replace("{{" + k + "}}", v)
    return rendered


# ── Audience resolution (read corex.audience, run its SQL on the Lance plane) ─────────────
def _resolve_audience_companies(audience_id: str, limit: int) -> list[dict]:
    """Resolve an audience_id → company set [{company_id, company_name, normalized_domain}].

    Reads corex.audience.{source_sql,result_key} via psycopg (the gtm-mcp write store),
    then runs that SQL through a local DuckDB-over-R2 connection (the same engine the
    gateway uses) to get the company rows. result_key MUST resolve to company_id for the
    enrichment workflow. Capped at ``limit`` (the test gate / cost cap upstream already
    bounds it; this is a hard backstop)."""
    import psycopg

    dsn = _hqx_dsn()
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set — cannot resolve the audience.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source_sql, result_key FROM corex.audience WHERE audience_id = %s",
            (audience_id,),
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"unknown audience_id {audience_id!r} in corex.audience")
    source_sql, result_key = row[0], row[1]
    if result_key != "company_id":
        raise RuntimeError(
            f"enrichment requires a company_id audience; audience {audience_id} resolves to "
            f"{result_key!r}. Define a company_id audience."
        )
    rows = _run_lance_sql(source_sql, limit=limit)
    out: list[dict] = []
    for r in rows:
        cid = r.get("company_id")
        if cid is None:
            continue
        out.append({
            "company_id": str(cid),
            "company_name": r.get("company_name"),
            "normalized_domain": r.get("normalized_domain"),
        })
    return out


def _run_lance_sql(sql: str, *, limit: int) -> list[dict]:
    """Execute audience SQL over the committed Lance plane from inside the worker. Discovers
    the datasets the SQL names (whole-token match against active/ roots) and registers ONLY
    those as DuckDB relations — the same JIT gate the gateway enforces. Returns up to
    ``limit`` rows as dicts. Read-only by construction (audience SQL is stored data)."""
    import duckdb
    import lance

    so = _r2_storage_options()
    con = duckdb.connect(":memory:")
    try:
        con.execute("PRAGMA threads=4;")
        for name, uri in _discover_active_datasets().items():
            if _names_dataset(sql, name):
                try:
                    con.register(name, lance.dataset(uri, storage_options=so))
                except Exception as exc:  # noqa: BLE001 — a missing manifest must not abort resolve
                    print(f"  WARN could not register {name}: {exc}")
        rel = con.execute(sql)
        cols = [d[0] for d in rel.description] if rel.description else []
        fetched = rel.fetchmany(int(limit))
        return [dict(zip(cols, r)) for r in fetched]
    finally:
        con.close()


def _discover_active_datasets() -> dict[str, str]:
    """name → s3:// uri for every committed Lance dataset under active/ (one level of
    namespace nesting, the only nesting the sink uses). A prefix is a dataset iff its
    children include ``_versions``. boto3 list, byte-identical to the gateway's discovery."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    s3 = boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto",
        config=Config(request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"),
    )
    bucket = "data-sink"

    def _children(prefix: str) -> list[str]:
        names, token = [], None
        while True:
            kw = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for cp in resp.get("CommonPrefixes", []):
                names.append(cp["Prefix"][len(prefix):].rstrip("/"))
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
        return names

    out: dict[str, str] = {}
    pending = [("", "active/")]
    depth = 0
    while pending and depth <= 2:
        nxt = []
        for rel, full in pending:
            kids = _children(full)
            if "_versions" in kids:
                if rel:
                    out[rel] = f"s3://{bucket}/active/{rel}/"
                continue
            for child in kids:
                if child.startswith("_"):
                    continue
                child_rel = f"{rel}/{child}" if rel else child
                nxt.append((child_rel, f"{full}{child}/"))
        pending = nxt
        depth += 1
    return out


def _names_dataset(sql: str, name: str) -> bool:
    """Whole-token match of a registry name in the SQL (boundary rejects word chars, '.',
    '/' on either side) — the gateway's referenced_datasets rule."""
    import re

    return re.search(r"(?<![\w./])" + re.escape(name) + r"(?![\w./])", sql, re.IGNORECASE) is not None


# ── Parallel Task Group wait (§0) ────────────────────────────────────────────────────────
def _wait_group_terminal(group_id: str) -> dict:
    """Poll GET /v1/tasks/groups/{id} until is_active==false (§0), bounded by the budget.
    Returns the final group document (carries status.task_run_status_counts)."""
    import time

    waited, idx = 0.0, 0
    last = get_group(group_id)
    while last.get("is_active", True) and waited < GROUP_POLL_BUDGET_S:
        sleep = GROUP_POLL_BACKOFF_S[min(idx, len(GROUP_POLL_BACKOFF_S) - 1)]
        time.sleep(sleep)
        waited += sleep
        idx += 1
        last = get_group(group_id)
        counts = (last.get("status") or {}).get("task_run_status_counts") or {}
        print(f"  group +{int(waited)}s is_active={last.get('is_active')} counts={counts}")
    return last


def _extract_run_record(rec: dict) -> tuple[str | None, str, dict | None, list, str | None]:
    """From one group-run record → (company_id, status, content, basis, run_id).

    Defensive against shape variance: the run id / status live under ``run`` or at the top
    level; ``output`` is a TaskRunJsonOutput {type:"json", content, basis}. company_id is
    carried back via the input's company_name→id map upstream (here we surface the Parallel
    run_id; the worker keys content by input order using metadata or the returned input)."""
    run = rec.get("run") or rec
    status = str(run.get("status") or rec.get("status") or "").lower()
    run_id = run.get("run_id") or rec.get("run_id")
    output = rec.get("output") or run.get("output") or {}
    content = output.get("content") if isinstance(output, dict) else None
    basis = output.get("basis") if isinstance(output, dict) else []
    # company_id round-trips through metadata (we stamp it on each input's metadata).
    meta = run.get("metadata") or rec.get("metadata") or {}
    company_id = meta.get("company_id") if isinstance(meta, dict) else None
    return company_id, status, content, (basis or []), run_id


# ── Lance write (merge_insert on company_id) + index ─────────────────────────────────────
def _write_enrichment(uri: str, table, so: dict) -> int:
    """Write the enrichment rows. The dataset is CREATED only when it is genuinely absent;
    on every existing dataset the write is a ``merge_insert("company_id")`` (refresh existing,
    add new) — the per-spec lifecycle (§4.1). No-op on an empty table.

    A ``merge_insert`` failure RAISES (the run goes terminal-failed on the ledger) and is NEVER
    swallowed into an ``overwrite`` — that bare-except fall-through previously DESTROYED all
    prior rows on a schema/type collision while reporting success. The dataset-absent probe is
    the ONLY path that may create, so a transient read error can never masquerade as a first
    write."""
    import lance

    if table.num_rows == 0:
        return 0
    # Probe existence. A dataset-NOT-FOUND error → first write (create); any other open error
    # propagates (do not create over an existing-but-unreadable dataset).
    exists = True
    try:
        lance.dataset(uri, storage_options=so)
    except Exception as exc:  # noqa: BLE001 — only a genuine "not found" means create
        if _is_dataset_absent(exc):
            exists = False
        else:
            raise
    if not exists:
        lance.write_dataset(
            table, uri, mode="create", data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        return table.num_rows
    # Existing dataset → merge_insert; a failure here RAISES (terminal), never an overwrite.
    ds = lance.dataset(uri, storage_options=so)
    ds.merge_insert("company_id").when_matched_update_all().when_not_matched_insert_all().execute(table)
    return table.num_rows


def _is_dataset_absent(exc: Exception) -> bool:
    """True iff the exception from ``lance.dataset(uri)`` means the dataset does not exist yet
    (vs. a credential / network / corrupt-manifest error, which must propagate). Lance raises a
    variety of messages across versions; match the dataset-not-found signatures by string."""
    msg = str(exc).lower()
    needles = ("not found", "does not exist", "no such", "dataset not", "table not",
               "no version", "not a dataset", "could not find")
    return any(n in msg for n in needles)


def _index_enrichment(uri: str, so: dict) -> None:
    """BTREE on company_id (the resolution key, the directive's hard deliverable) +
    normalized_domain (human joins). Idempotent; an index miss never fails a good load."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    for col in ("company_id", "normalized_domain"):
        if col not in [f.name for f in ds.schema]:
            continue
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  BTREE  ✗ {col}: {exc}")


# ── Raw capture (the COMPLETE Parallel result per group-run, verbatim) ───────────────────
def _json_dump(v) -> str | None:
    """Stable JSON string for a value (verbatim raw capture). None stays None."""
    import json

    if v is None:
        return None
    return v if isinstance(v, str) else json.dumps(v, default=str)


def _evolve_columns(ds, want_cols, uri: str, so: dict):
    """Add any missing string columns to an EXISTING dataset BEFORE merge_insert. lance 7.x
    merge_insert REJECTS a superset schema ('Append with different schema … unexpected=[…]'),
    so a net-new column must be materialized first (existing rows → NULL). Idempotent;
    append-only metadata commit (no fragment rewrite)."""
    missing = [c for c in want_cols if c not in {f.name for f in ds.schema}]
    if not missing:
        return ds
    import lance

    ds.add_columns({c: "CAST(NULL AS STRING)" for c in missing})
    print(f"  schema-evolve: +{missing} → {uri}")
    return lance.dataset(uri, storage_options=so)


# Raw-capture columns — the COMPLETE per-run Parallel record ({run, output, …}) as a verbatim
# JSON string. ``parallel_run_id`` is the merge key (unique per run → append-only); ``run_id``
# is the dispatching run id and ``company_id`` rides along (NULL for a keying_miss run).
_RAW_COLUMNS = ("run_id", "parallel_run_id", "company_id", "spec_id", "raw_result",
                "processor", "created_at")


def _write_enrichment_raw(raw_uri: str, rows: list[dict], so: dict) -> int:
    """Land the COMPLETE Parallel result per group-run VERBATIM → enrichment_raw/<spec>/
    (merge_insert on ``parallel_run_id``, unique per run → append-only: a re-enrich never
    overwrites a prior run's raw). The entire {run, output, …} payload is kept so NOTHING the
    vendor returns (cost/usage/timing/citations/etc.) is ever discarded. Self-creating +
    schema-evolution-safe. A write failure RAISES (terminal) — the raw is the SoR for a run, so
    failing loud beats silently dropping it."""
    import lance
    import pyarrow as pa

    if not rows:
        return 0

    def _s(v):
        return None if v is None else (v if isinstance(v, str) else str(v))

    schema = pa.schema([pa.field(c, pa.string()) for c in _RAW_COLUMNS])
    columns = {c: pa.array([_s(r.get(c)) for r in rows], type=pa.string()) for c in _RAW_COLUMNS}
    table = pa.table(columns, schema=schema)

    # Probe existence: dataset-NOT-FOUND → create; any other open error propagates.
    exists = True
    try:
        lance.dataset(raw_uri, storage_options=so)
    except Exception as exc:  # noqa: BLE001
        if _is_dataset_absent(exc):
            exists = False
        else:
            raise
    if not exists:
        lance.write_dataset(
            table, raw_uri, mode="create", data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
    else:
        ds = lance.dataset(raw_uri, storage_options=so)
        ds = _evolve_columns(ds, _RAW_COLUMNS, raw_uri, so)
        ds.merge_insert("parallel_run_id").when_matched_update_all().when_not_matched_insert_all().execute(table)

    ds = lance.dataset(raw_uri, storage_options=so)
    names = [f.name for f in ds.schema]
    for col in ("parallel_run_id", "run_id", "company_id"):
        if col in names:
            try:
                ds.create_scalar_index(col, index_type="BTREE", replace=True)
                print(f"  BTREE  ✓ raw {col}")
            except Exception as exc:  # noqa: BLE001
                print(f"  BTREE  ✗ raw {col}: {exc}")
    return table.num_rows


# ── ops.* writes ─────────────────────────────────────────────────────────────────────────
def _record_run(row: dict) -> None:
    """Terminal run row → ops.parallel_runs (psycopg). Best-effort."""
    import psycopg
    from psycopg.types.json import Jsonb

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
                     requested, skipped_no_domain, completed, failed, failed_company_ids,
                     processor, cost_estimate, cost_cap, dataset_uri, status, error,
                     started_at, completed_at)
                VALUES (%(spec_id)s, 'enrich', %(run_kind)s, %(group_id)s, %(audience_id)s,
                        %(idempotency_key)s, %(requested)s, %(skipped_no_domain)s, %(completed)s,
                        %(failed)s, %(failed_company_ids)s, %(processor)s, %(cost_estimate)s,
                        %(cost_cap)s, %(dataset_uri)s, %(status)s, %(error)s, %(started_at)s,
                        %(completed_at)s)
                """,
                {**row, "failed_company_ids": Jsonb(row.get("failed_company_ids") or [])},
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.parallel_runs write failed: {exc}")


def _queue_review(spec_id: str, items: list[dict]) -> None:
    """Per-field low/medium/null-confidence cells → ops.parallel_review (the value still
    landed in Lance). Best-effort batch insert."""
    if not items:
        return
    import psycopg

    dsn = _hqx_dsn()
    if not dsn:
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.executemany(
                "INSERT INTO ops.parallel_review (spec_id, run_id, company_id, field, confidence) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(spec_id, it.get("run_id"), it["company_id"], it["field"], it.get("confidence"))
                 for it in items],
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.parallel_review write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint url. FLAT JSON body — no envelope."""
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


# ── Content/basis → typed Arrow rows (DuckDB-projected, drift-tolerant) ──────────────────
def _build_rows(companies: list[dict], results: dict[str, dict], *, schema_props: dict,
                spec_id: str, run_id_label: str, processor: str):
    """Project Parallel content+basis into one Arrow row per COMPLETED company.

    Built from an EXPLICIT, all-``string`` pyarrow schema derived DETERMINISTICALLY from the
    spec's ``output_schema`` field list — NOT inferred from the batch. This is load-bearing:
    ``pa.Table.from_pylist`` infers an Arrow ``null``-typed column whenever a field is None
    across the whole batch (routine on ``test_limit=3``, sparse fields, empty ``_basis``); a
    later run that populates that field would land it as ``string``, ``merge_insert`` would
    raise on the type change, and the caller would (previously) clobber the dataset. A stable
    string type per column makes every run schema-compatible.

    Per declared field: ``content[field]`` (drift: missing/renamed key → None); arrays/nested
    → JSON string; scalars/enums → ``str(value)``; ``<field>__confidence`` from
    ``basis[field].confidence``. ``company_id``/``normalized_domain``/``_basis``/provenance are
    all string too. Returns (arrow_table|None, review_items, completed_company_ids) — failed
    ids are computed separately in the worker body."""
    import datetime as dt
    import json

    import pyarrow as pa

    fields = list(schema_props.keys())
    enriched_at = dt.datetime.now(dt.timezone.utc).isoformat()
    review_items: list[dict] = []
    completed: list[str] = []
    records: list[dict] = []

    def _scalar_str(v) -> str | None:
        """Stringify a scalar for the all-string schema. JSON-encode arrays/objects so a
        typed array column is reconstructable downstream; bools/numbers → canonical str;
        None stays None (a typed-string null, not an Arrow null column)."""
        if v is None:
            return None
        if isinstance(v, (list, dict)):
            return json.dumps(v, default=str)
        if isinstance(v, bool):
            return "true" if v else "false"
        return v if isinstance(v, str) else str(v)

    for c in companies:
        cid = c["company_id"]
        res = results.get(cid)
        if not res or res.get("status") != "completed" or res.get("content") is None:
            continue
        content = res["content"] if isinstance(res["content"], dict) else {}
        basis = res.get("basis") or []
        conf_by_field = {b.get("field"): b.get("confidence")
                         for b in basis if isinstance(b, dict) and b.get("field")}
        rec: dict = {
            "company_id": str(cid),
            "normalized_domain": _scalar_str(c.get("normalized_domain")),
            "spec_id": spec_id,
            "run_id": res.get("run_id") or run_id_label,
            "processor": processor,
            "enriched_at": enriched_at,
            "_basis": json.dumps(basis, default=str) if basis else None,
        }
        for f in fields:
            rec[f] = _scalar_str(content.get(f, None))  # drift: missing key → None
            conf = conf_by_field.get(f)
            rec[f"{f}__confidence"] = _scalar_str(conf)
            if conf in REVIEW_CONFIDENCES:
                review_items.append({"company_id": str(cid), "field": f, "confidence": conf,
                                     "run_id": res.get("run_id") or run_id_label})
        records.append(rec)
        completed.append(str(cid))

    if not records:
        return None, review_items, completed

    # EXPLICIT schema (deterministic column order + stable string types). Keys + provenance,
    # then per declared field its value + confidence. Every run produces this exact schema
    # regardless of null density, so merge_insert never sees a type change.
    schema_fields = [
        pa.field("company_id", pa.string()),
        pa.field("normalized_domain", pa.string()),
        pa.field("spec_id", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("processor", pa.string()),
        pa.field("enriched_at", pa.string()),
        pa.field("_basis", pa.string()),
    ]
    for f in fields:
        schema_fields.append(pa.field(f, pa.string()))
        schema_fields.append(pa.field(f"{f}__confidence", pa.string()))
    schema = pa.schema(schema_fields)
    columns = {fld.name: pa.array([r.get(fld.name) for r in records], type=pa.string())
               for fld in schema}
    table = pa.table(columns, schema=schema)
    return table, review_items, completed


# ── The dispatched worker — batch enrichment ─────────────────────────────────────────────
@app.function(
    secrets=[
        modal.Secret.from_name("parallel-api"),
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 120,  # widened for pro-tier groups; poll budget stays the inner bound
    memory=8192,
    cpu=4.0,
)
def enrich_companies(
    trigger_callback_url: str | None = None,
    run_id: str | None = None,
    spec_id: str = "",
    audience_id: str = "",
    output_schema: dict | None = None,
    processor: str = "core",
    run_kind: str = "test",
    max_runs: int = 0,
    idempotency_key: str | None = None,
    cost_cap: float | None = None,
    companies_explicit: list[dict] | None = None,
    prompt_template: str | None = None,
) -> dict:
    """Enrich a resolved company audience into typed cited columns → per-spec Lance dataset.

    ``output_schema`` is the PURE JSON-Schema data-column object (wrapped here as §0
    requires). ``processor`` ∈ {lite,base,core} (ultra rejected). ``max_runs`` hard-caps the
    company set (the launch tool also enforces this pre-dispatch). Terminal state →
    ops.parallel_runs + Trigger callback. Re-raises only on genuine failure.

    EXPLICIT-LIST PATH: ``companies_explicit`` (entries per ``_normalize_explicit`` — key
    rides in the ``company_id`` slot end-to-end, e.g. a UEI) bypasses audience resolution;
    ``audience_id`` is then ignored. ``prompt_template`` switches the per-run task input
    from the structured ``{company_name, company_website}`` object to the template rendered
    per entity via ``{{token}}`` substitution (``_render_prompt``) — the path for running
    an operator-authored prompt VERBATIM. Both parameters compose with everything else
    (groups, basis, raw capture, confidence gate, ledger)."""
    import datetime as dt
    import uuid

    started = dt.datetime.now(dt.timezone.utc)
    run_id = run_id or uuid.uuid4().hex
    spec = (spec_id or "").strip()
    proc = (processor or "core").strip().lower()
    dataset_uri = f"{ENRICHMENT_ROOT}/{spec}/"
    raw_uri = f"{ENRICHMENT_RAW_ROOT}/{spec}/"
    counts = {"requested": 0, "skipped_no_domain": 0, "completed": 0, "failed": 0,
              "keying_miss": 0, "raw": 0}
    failed_ids: list[str] = []
    group_id = None
    status, error = "failed", None

    def _terminal(st, *, reraise_msg=None, **extra):
        finished = dt.datetime.now(dt.timezone.utc)
        # Fold the keying_miss count into the ledger error text (the table has no dedicated
        # column) so a metadata round-trip problem is visible on the run row, distinct from the
        # company-attributable failed_company_ids.
        ledger_error = error
        if counts["keying_miss"]:
            note = f"keying_miss={counts['keying_miss']} (runs with no recognizable company_id)"
            ledger_error = f"{error}; {note}" if error else note
        _record_run({
            "spec_id": spec, "run_kind": run_kind, "group_id": group_id,
            "audience_id": audience_id or None, "idempotency_key": idempotency_key,
            "requested": counts["requested"], "skipped_no_domain": counts["skipped_no_domain"],
            "completed": counts["completed"], "failed": counts["failed"],
            "failed_company_ids": failed_ids, "processor": proc, "cost_estimate": None,
            "cost_cap": cost_cap, "dataset_uri": dataset_uri, "status": st, "error": ledger_error,
            "started_at": started, "completed_at": finished,
        })
        _post_callback(trigger_callback_url, {
            "status": st, "run_id": run_id, "workflow": "enrich", "spec_id": spec,
            "group_id": group_id, "dataset_uri": dataset_uri, "raw_dataset_uri": raw_uri,
            "requested": counts["requested"], "skipped_no_domain": counts["skipped_no_domain"],
            "completed": counts["completed"], "failed": counts["failed"],
            "keying_miss": counts["keying_miss"], "raw_landed": counts["raw"],
            "failed_company_ids": failed_ids[:200], **extra,
        })
        if reraise_msg:
            raise RuntimeError(reraise_msg)

    try:
        # Guardrails — fail BEFORE any spend.
        if not spec:
            status, error = "rejected", "spec_id is required"
            return _terminal(status) or {"status": status, "error": error}
        if proc not in ALLOWED_PROCESSORS:
            status, error = "rejected", (
                f"processor {proc!r} not allowed for enrichment — tier ceiling is "
                f"{PROCESSOR_CEILING!r} (ultra needs the per-run webhook path, deferred per §0/§9)."
            )
            return _terminal(status) or {"status": status, "error": error}
        if not isinstance(output_schema, dict) or output_schema.get("type") != "object":
            status, error = "rejected", "output_schema must be a JSON-Schema object {type:'object',properties:…}"
            return _terminal(status) or {"status": status, "error": error}

        schema_props = output_schema.get("properties") or {}
        limit = max_runs if max_runs and max_runs > 0 else MAX_ROWS_PER_FILE

        # 1) Resolve the entity set — explicit list when supplied, else the audience.
        if companies_explicit:
            companies = _normalize_explicit(companies_explicit, limit=limit)
            if not companies:
                status, error = "rejected", "companies_explicit had no usable entries (key + name)"
                return _terminal(status) or {"status": status, "error": error}
        else:
            companies = _resolve_audience_companies(audience_id, limit=limit)
            if not companies:
                status, error = "success", "audience resolved to 0 companies"
                return _terminal("success") or {"status": "success", "companies": 0}
        counts["requested"] = len(companies)

        # 2) Build one input per company (bare-domain website; null-domain → name-only, counted).
        #    With a prompt_template the input is the rendered template STRING (§0 allows string
        #    or object input); otherwise the structured {company_name, company_website} object.
        wrapped = wrap_json_schema(output_schema)
        inputs: list[dict] = []
        for c in companies:
            dom = _bare_domain(c.get("normalized_domain"))
            if not dom:
                counts["skipped_no_domain"] += 1
            if prompt_template:
                payload_input: Any = _render_prompt(prompt_template, c)
            else:
                payload_input = {"company_name": c.get("company_name") or ""}
                if dom:
                    payload_input["company_website"] = dom
            inputs.append({
                "input": payload_input,
                "processor": proc,
                # company_id round-trips through metadata so results re-key without ordering risk.
                "metadata": {"company_id": str(c["company_id"]), "spec_id": spec},
            })

        # 3) Create the group + add runs (≤1000/POST), default_task_spec = ONE wrapped schema.
        grp = create_task_group(metadata={"spec_id": spec, "run_id": run_id, "run_kind": run_kind})
        group_id = grp.get("task_group_id") or grp.get("id")
        if not group_id:
            status, error = "failed", f"group create returned no id: {str(grp)[:200]}"
            return _terminal("failed", reraise_msg=error)
        default_task_spec = {"output_schema": wrapped}
        for i in range(0, len(inputs), 1000):
            add_group_runs(group_id, inputs[i:i + 1000],
                           default_task_spec=default_task_spec, beta_field_basis=True)
        print(f"  group {group_id}: queued {len(inputs)} runs (processor={proc})")

        # 4) Wait Parallel's way: poll group to is_active==false, then fetch results.
        final = _wait_group_terminal(group_id)
        if final.get("is_active", True):
            status, error = "partial", f"group still active after {GROUP_POLL_BUDGET_S}s poll budget"
            print(f"  WARN {error}")
        run_recs = get_group_runs(group_id, include_output=True, beta_field_basis=True)

        # 5) Index results by company_id; classify completed vs failed. A run that returned but
        #    carries no recognizable company_id in its metadata (the §0 echo failed to round-trip)
        #    is a DISTINCT 'keying_miss' — counted separately, not folded into generic 'failed' —
        #    so the first live run makes a metadata-keying problem obvious instead of silent.
        dispatched_ids = {c["company_id"] for c in companies}
        results: dict[str, dict] = {}
        raw_rows: list[dict] = []  # the COMPLETE per-run Parallel record, verbatim (raw capture)
        created_at = dt.datetime.now(dt.timezone.utc).isoformat()
        for rec in run_recs:
            cid, st, content, basis, rid = _extract_run_record(rec)
            # RAW CAPTURE — keep the ENTIRE {run, output, …} record verbatim, keyed by the
            # Parallel run_id, REGARDLESS of whether it re-keys to a dispatched company. A
            # keying_miss run still returned a full provider payload; we discard NOTHING.
            if rid:
                raw_rows.append({
                    "run_id": run_id, "parallel_run_id": rid, "company_id": cid,
                    "spec_id": spec, "raw_result": _json_dump(rec), "processor": proc,
                    "created_at": created_at,
                })
            if not cid or cid not in dispatched_ids:
                counts["keying_miss"] += 1
                continue
            results[cid] = {"status": st, "content": content, "basis": basis, "run_id": rid}
        for c in companies:
            cid = c["company_id"]
            r = results.get(cid)
            if not r or r.get("status") != "completed":
                failed_ids.append(cid)
        counts["failed"] = len(failed_ids)
        if counts["keying_miss"]:
            print(f"  WARN {counts['keying_miss']} run(s) returned with no recognizable "
                  "company_id metadata (keying_miss) — not attributable to a company.")

        so = _r2_storage_options()
        # 6) RAW CAPTURE — land the verbatim provider payload FIRST and INDEPENDENTLY of the
        #    typed projection. A raw-write failure RAISES (terminal, fail loud); because raw is
        #    committed before the projection runs, a later projection failure can NEVER lose it.
        if raw_rows:
            counts["raw"] = _write_enrichment_raw(raw_uri, raw_rows, so)
            print(f"  landed {counts['raw']} raw row(s) → {raw_uri}")

        # 7) Project content+basis → typed rows, write Lance (merge_insert), index.
        table, review_items, completed_ids = _build_rows(
            companies, results, schema_props=schema_props, spec_id=spec,
            run_id_label=run_id, processor=proc,
        )
        counts["completed"] = len(completed_ids)
        if table is not None and table.num_rows:
            _write_enrichment(dataset_uri, table, so)
            _index_enrichment(dataset_uri, so)
            print(f"  landed {table.num_rows} rows → {dataset_uri}")
        # 8) Confidence gate → review queue (value already landed).
        _queue_review(spec, review_items)

        if status != "partial":
            status = "success" if not failed_ids else "partial"
        error = None if status == "success" else (error or f"{len(failed_ids)} runs did not complete")
        return _terminal(status) or {
            "status": status, "run_id": run_id, "group_id": group_id,
            "dataset_uri": dataset_uri, "completed": counts["completed"],
            "failed": counts["failed"], "skipped_no_domain": counts["skipped_no_domain"],
        }

    except Exception as exc:  # noqa: BLE001 — terminal + re-raise so Modal marks failed
        status, error = "failed", str(exc)
        _terminal("failed", reraise_msg=f"parallel enrich failed: {exc}")


# ── Single-entity smoke path (§0: POST /runs + blocking GET /result) ─────────────────────
@app.function(secrets=[modal.Secret.from_name("parallel-api")], timeout=60 * 12)
def smoke_one(company_name: str, company_website: str = "", processor: str = "core",
              output_schema: dict | None = None, prompt_template: str | None = None,
              extra_fields: dict | None = None) -> dict:
    """One company through the SINGLE-run path: POST /v1/tasks/runs + blocking
    GET /result?timeout=600 (§0). Returns {run_id, status, content, basis} for inline
    inspection. No Lance write, no ledger — purely a latency/shape probe (it DOES bill one
    run, so it is gated behind a manual entrypoint, never auto-invoked).

    ``prompt_template`` mirrors the batch path: the input becomes the template rendered via
    ``{{token}}`` substitution over {company_name, company_website, **extra_fields} — the
    single-firm validation of an operator-authored prompt before a group dispatch."""
    import time

    if processor.strip().lower() not in ALLOWED_PROCESSORS:
        return {"status": "rejected",
                "error": f"processor must be one of {ALLOWED_PROCESSORS} (ultra deferred)"}
    dom = _bare_domain(company_website)
    inp: Any
    if prompt_template:
        company = {"company_name": company_name, "normalized_domain": dom,
                   **{k: v for k, v in (extra_fields or {}).items() if isinstance(v, str)}}
        inp = _render_prompt(prompt_template, company)
    else:
        inp = {"company_name": company_name}
        if dom:
            inp["company_website"] = dom
    task_spec = {"output_schema": wrap_json_schema(output_schema)} if output_schema else None
    run = create_task_run(input_=inp, processor=processor.strip().lower(),
                          task_spec=task_spec, beta_field_basis=True)
    rid = run.get("run_id")
    if not rid:
        return {"status": "failed", "error": f"create returned no run_id: {str(run)[:200]}"}
    # Blocking GET /result; re-call on 408 (still active) within ~10 min.
    deadline = time.monotonic() + 60 * 10
    while time.monotonic() < deadline:
        res = get_task_result(rid, timeout=300, beta_field_basis=True)
        if res.get("_status") == 408:
            continue
        output = res.get("output") or {}
        run_obj = res.get("run") or {}
        return {
            "status": str(run_obj.get("status") or "completed"),
            "run_id": rid,
            "content": output.get("content") if isinstance(output, dict) else None,
            "basis": output.get("basis") if isinstance(output, dict) else None,
        }
    return {"status": "timeout", "run_id": rid, "error": "no result within 10 min"}


# ── ops bootstrap + read-back ────────────────────────────────────────────────────────────
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.parallel_specs + ops.parallel_runs + ops.parallel_review (HQX)."""
    import psycopg

    dsn = _hqx_dsn()
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("ops.parallel_specs + ops.parallel_runs + ops.parallel_review ready.")
    return {"tables": ["ops.parallel_specs", "ops.parallel_runs", "ops.parallel_review"]}


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops DDL (HQX). Safe — no Parallel calls, no spend."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run_explicit(csv_path: str, spec: str, prompt_file: str, schema_file: str,
                 processor: str = "core", max_runs: int = 0,
                 mode: str = "dry") -> None:
    """Launch an EXPLICIT-LIST enrichment from a CSV — the operator-prompt dossier path.

    CSV columns: a key column (``uei`` | ``key`` | ``company_id``), a name column
    (``company_name`` | ``sub_name`` | ``name``), optional ``domain``; every other column
    rides along verbatim as a ``{{token}}`` for the prompt template. ``prompt_file`` is the
    operator's prompt with ``{{company_name}}`` / ``{{company_website}}`` (+ any extra
    column tokens); ``schema_file`` is the pure JSON-Schema object for typed output.

    ``mode`` is the spend gate:
      dry   — render + print entity count and the FIRST fully-rendered prompt; NO spend.
      smoke — bill ONE run (first row) via smoke_one; prints content + basis.
      live  — dispatch the full group (bills every row; ``max_runs`` caps).
    """
    import csv as _csv
    import json as _json

    with open(prompt_file, encoding="utf-8") as f:
        template = f.read()
    with open(schema_file, encoding="utf-8") as f:
        schema = _json.load(f)
    rows = list(_csv.DictReader(open(csv_path, encoding="utf-8")))
    explicit: list[dict] = []
    skipped_no_domain = 0
    for r in rows:
        e = {k: (v or "") for k, v in r.items()}
        e["company_name"] = r.get("company_name") or r.get("sub_name") or r.get("name") or ""
        # Operator ruling (2026-07-17): a firm with no domain is SKIPPED, never sent name-only.
        if not _bare_domain(e.get("domain") or e.get("normalized_domain")):
            skipped_no_domain += 1
            continue
        explicit.append(e)
    if max_runs and max_runs > 0:
        explicit = explicit[:max_runs]
    print(f"entities={len(explicit)} skipped_no_domain={skipped_no_domain} "
          f"spec={spec} processor={processor} mode={mode}")

    if mode == "dry":
        first = _normalize_explicit(explicit, limit=1)
        if first:
            print("── first rendered prompt ──")
            print(_render_prompt(template, first[0]))
        return
    if mode == "smoke":
        first = _normalize_explicit(explicit, limit=1)[0]
        out = smoke_one.remote(
            company_name=first.get("company_name") or "",
            company_website=first.get("normalized_domain") or "",
            processor=processor, output_schema=schema, prompt_template=template,
            extra_fields={k: v for k, v in first.items() if isinstance(v, str)},
        )
        print(_json.dumps(out, indent=2, default=str)[:8000])
        return
    if mode == "live":
        out = enrich_companies.remote(
            spec_id=spec, output_schema=schema, processor=processor,
            run_kind="live", max_runs=max_runs or 0,
            companies_explicit=explicit, prompt_template=template,
        )
        print(_json.dumps(out, indent=2, default=str)[:4000])
        return
    raise SystemExit(f"unknown mode {mode!r} — use dry | smoke | live")
