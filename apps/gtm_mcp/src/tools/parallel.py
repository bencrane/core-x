"""Parallel.ai launch tools — the gtm-agent's typed triggers for the three Parallel
workflows (Directive 24): ENRICH (typed columns), DEEP RESEARCH (cited report), and
WEB SEARCH (ranked excerpts). Plus ``define_enrichment_spec`` (persist a spec to
``ops.parallel_specs``) and the catalog-refresh tool.

This is the interactive kickoff surface, mirroring ``hydration.py`` EXACTLY: each launch
tool (1) validates inputs and (for the entity workflows) confirms the audience exists in
``corex.audience`` via read-only psycopg on ``database.hqx_dsn()``, persists the spec, then
(2) fires the matching Trigger task over Trigger.dev's REST trigger endpoint. The Trigger
task dispatches the Modal worker, which is the only thing that calls Parallel and writes the
dataset. These tools are a control-plane SIGNAL — they write no dataset and make no Parallel
call themselves.

THREE WORKFLOWS, THREE PURPOSE-NAMED TOOLS (the directive's mandate — NO single mode flag):
  • ``enrich_companies``  → Trigger ``parallel-enrich``        → Modal ``parallel-enrich``
  • ``deep_research``     → Trigger ``parallel-deep-research`` → Modal ``parallel-deep-research``
  • ``web_search``        → Trigger ``parallel-search``        → Modal ``parallel-search``

COST GOVERNANCE. Each launch carries an idempotencyKey (``f"{spec_id}:{audience_id}:
{run_kind}"``), a hard ``max_runs`` cost cap enforced in the Trigger task BEFORE dispatch,
and ``test_limit`` (default 3 inline) so the operator inspects a small sample first.

CONFIG. ``TRIGGER_SECRET_KEY`` (a Trigger.dev prod ``tr_…`` key) must be set on the gtm-mcp
service env — exactly as ``hydration.py`` needs. The trigger is issued over plain HTTPS with
the stdlib (no new dependency)."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any

from .. import database
from . import catalog

TRIGGER_BASE = os.environ.get("TRIGGER_API_URL", "https://api.trigger.dev").rstrip("/")

# Trigger task ids (match the `id` fields in src/trigger/parallel_*.ts).
TASK_ENRICH = "parallel-enrich"
TASK_DEEP_RESEARCH = "parallel-deep-research"
TASK_SEARCH = "parallel-search"

# Per-workflow tier ceilings (mirrored in the workers; validated here so a bad tier is
# rejected before it bills). enrich caps at core; deep_research caps at pro (ultra deferred).
ENRICH_PROCESSORS = ("lite", "base", "core")
RESEARCH_PROCESSORS = ("lite", "base", "core", "pro")

_SPEC_RE = re.compile(r"^[a-z0-9_]{3,64}$")
_ENRICHMENT_ROOT = "s3://data-sink/active/enrichment"
_RESEARCH_URI = "s3://data-sink/active/parallel_research/"

# Self-bootstrapping spec-registry DDL (mirror of pipelines/parallel/ops_parallel_specs.sql),
# applied idempotently before a spec write — the ops.py / corex.py idiom.
_SPECS_DDL = """
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
"""


# ── hq-x helpers (read-only validation + spec persist) ───────────────────────────────────
def _require_dsn() -> str:
    dsn = database.hqx_dsn()
    if not dsn:
        raise RuntimeError(
            "HQX_DB_URL_POOLED is not set — cannot reach hq-x to validate the audience / persist the spec."
        )
    return dsn


def _audience_row(audience_id: str) -> dict | None:
    """(result_key, source_sql) for an audience_id, or None. Read-only psycopg on the hq-x
    DSN — independent of the DuckDB attach (the ops.py split)."""
    import psycopg

    with psycopg.connect(_require_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT audience_id, result_key, source_sql FROM corex.audience WHERE audience_id = %s",
            (audience_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {"audience_id": str(row[0]), "result_key": row[1], "source_sql": row[2]}


def _persist_spec(spec_id: str, workflow: str, processor: str, *, output_schema: dict | None,
                  objective: str | None, grain: str | None, dataset_uri: str,
                  result_key: str = "company_id") -> None:
    """Upsert one ops.parallel_specs row (self-bootstrapping DDL). jsonb bound via Jsonb."""
    import psycopg
    from psycopg.types.json import Jsonb

    with psycopg.connect(_require_dsn()) as conn:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(_SPECS_DDL)
            cur.execute(
                """
                INSERT INTO ops.parallel_specs
                    (spec_id, workflow, processor, output_schema, objective, grain, dataset_uri, result_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (spec_id) DO UPDATE SET
                    workflow=EXCLUDED.workflow, processor=EXCLUDED.processor,
                    output_schema=EXCLUDED.output_schema, objective=EXCLUDED.objective,
                    grain=EXCLUDED.grain, dataset_uri=EXCLUDED.dataset_uri,
                    result_key=EXCLUDED.result_key, updated_at=now()
                """,
                (spec_id, workflow, processor,
                 Jsonb(output_schema) if output_schema is not None else None,
                 objective, grain, dataset_uri, result_key),
            )


def _trigger_run(task_id: str, payload: dict) -> dict[str, Any]:
    """POST the Trigger.dev REST trigger endpoint. Returns the parsed run handle
    (``{"id": "run_…"}``). Raises with the upstream body on a non-2xx. Mirror of
    hydration.py::_trigger_run."""
    key = os.environ.get("TRIGGER_SECRET_KEY")
    if not key:
        raise RuntimeError(
            "TRIGGER_SECRET_KEY is not set on the gtm-mcp service — cannot trigger the Parallel "
            "task. Provision the Trigger.dev prod key in the service env (Doppler holds it as "
            "TRIGGER_SHARED_SECRET)."
        )
    url = f"{TRIGGER_BASE}/api/v1/tasks/{task_id}/trigger"
    body = json.dumps({"payload": payload}).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:500]
        raise RuntimeError(f"Trigger.dev trigger failed ({exc.code}): {detail}") from None


# ════════════════════════════ SPEC AUTHORING ══════════════════════════════════════════════
def define_enrichment_spec(
    name: str,
    processor: str,
    output_schema: dict,
) -> dict[str, Any]:
    """Define and persist an ENRICHMENT spec — the typed-column contract a later
    ``enrich_companies`` run materializes into ``s3://data-sink/active/enrichment/<name>/``.

    Author ``output_schema`` as a PURE JSON-Schema *data-column* object — ``{"type":"object",
    "properties":{…}, "required":[…], "additionalProperties":false}``. Each property becomes a
    typed Lance column; provenance is NOT inlined (Parallel's Basis sidecar carries citations/
    reasoning/confidence per field, landed as ``<field>__confidence`` + ``_basis``). **Maximize
    fields** — Parallel is generous; ask for many and pick the processor that covers them
    (``core ≈10``, ``pro ≈20``). ``processor`` here is the enrichment tier — one of
    lite|base|core (the workflow caps at ``core``; ultra needs the deferred webhook path).

    The wire form is wrapped for you at dispatch (``{"type":"json","json_schema": <object>}``);
    a bare JSON-Schema object is NOT valid Parallel wire form. ``name`` is the spec id and the
    dataset name (``^[a-z0-9_]{3,64}$``; bump a suffix — ``_v2`` — on a schema change → a new
    dataset). Returns the spec id + dataset_uri.
    """
    spec_id = (name or "").strip()
    if not _SPEC_RE.match(spec_id):
        raise ValueError(f"name must match ^[a-z0-9_]{{3,64}}$ (got {spec_id!r})")
    proc = (processor or "core").strip().lower()
    if proc not in ENRICH_PROCESSORS:
        raise ValueError(f"processor must be one of {ENRICH_PROCESSORS} (enrichment caps at 'core')")
    if not isinstance(output_schema, dict) or output_schema.get("type") != "object":
        raise ValueError("output_schema must be a JSON-Schema object {type:'object',properties:…}")
    if not isinstance(output_schema.get("properties"), dict) or not output_schema["properties"]:
        raise ValueError("output_schema.properties must be a non-empty object of data columns")

    dataset_uri = f"{_ENRICHMENT_ROOT}/{spec_id}/"
    _persist_spec(spec_id, "enrich", proc, output_schema=output_schema, objective=None,
                  grain=None, dataset_uri=dataset_uri, result_key="company_id")
    return {
        "status": "ok", "spec_id": spec_id, "workflow": "enrich", "processor": proc,
        "dataset_uri": dataset_uri, "field_count": len(output_schema["properties"]),
    }


# ════════════════════════════ LAUNCH TOOLS ════════════════════════════════════════════════
def enrich_companies(
    spec_id: str,
    audience_id: str,
    processor: str | None = None,
    test_limit: int = 3,
    max_runs: int | None = None,
    max_usd: float | None = None,
) -> dict[str, Any]:
    """Launch Parallel ENRICHMENT — turn a saved company audience into typed, cited columns.

    Resolves ``audience_id`` (must be a ``company_id`` audience; the worker re-runs its stored
    SQL over the live Lance lake), enriches each company against the spec's ``output_schema``,
    and lands typed columns + ``<field>__confidence`` + ``_basis`` to
    ``s3://data-sink/active/enrichment/<spec>/`` keyed ``company_id`` (BTREE), merging on
    re-run. Define the spec first with ``define_enrichment_spec``.

    ``test_limit=3`` (default) sends a SMALL sample so rows land + are inspectable before a full
    release; ``test_limit=0`` runs the whole audience. ``max_runs`` is a HARD pre-dispatch cap on
    companies dispatched (enforced in the Trigger task, not just a ledger estimate); ``max_usd``
    is an advisory cap recorded on the ledger. ``processor`` overrides the spec's tier
    (lite|base|core; ultra rejected). Returns a Trigger run handle — the run executes async and
    lands LINKED to each company; then call ``refresh_catalog`` to make the dataset queryable.
    """
    sid = (spec_id or "").strip()
    if not _SPEC_RE.match(sid):
        raise ValueError(f"spec_id must match ^[a-z0-9_]{{3,64}}$ (got {sid!r})")
    aid = (audience_id or "").strip()
    if not aid:
        raise ValueError("audience_id is required")
    aud = _audience_row(aid)
    if not aud:
        raise ValueError(f"unknown audience_id {aid!r} — define it first with define_audience.")
    if aud["result_key"] != "company_id":
        raise ValueError(
            f"enrichment requires a company_id audience; {aid} resolves to {aud['result_key']!r}."
        )
    # Spec must exist + carry the output_schema (the column contract the worker wraps).
    spec = _load_spec(sid)
    if not spec or spec["workflow"] != "enrich":
        raise ValueError(f"no enrichment spec {sid!r} — call define_enrichment_spec first.")
    proc = (processor or spec["processor"] or "core").strip().lower()
    if proc not in ENRICH_PROCESSORS:
        raise ValueError(f"processor must be one of {ENRICH_PROCESSORS} (enrichment caps at 'core')")

    run_kind = "test" if (test_limit or 0) > 0 else "full"
    payload = {
        "specId": sid,
        "audienceId": aid,
        "outputSchema": spec["output_schema"],
        "processor": proc,
        "testLimit": max(0, int(test_limit or 0)),
        "idempotencyKey": f"{sid}:{aid}:{run_kind}",
    }
    if max_runs is not None:
        payload["maxRuns"] = int(max_runs)
    if max_usd is not None:
        payload["maxUsd"] = float(max_usd)
    handle = _trigger_run(TASK_ENRICH, payload)
    return {
        "status": "launched", "workflow": "enrich", "task_id": TASK_ENRICH, "spec_id": sid,
        "audience_id": aid, "processor": proc, "run_kind": run_kind,
        "run_id": handle.get("id"), "dataset_uri": spec["dataset_uri"],
    }


def deep_research(
    objective: str,
    grain: str = "topic",
    audience_id: str | None = None,
    spec_id: str | None = None,
    processor: str = "base",
    output_type: str = "text",
    test_limit: int = 3,
    max_runs: int | None = None,
    max_usd: float | None = None,
) -> dict[str, Any]:
    """Launch Parallel DEEP RESEARCH — commission a cited long-form report (NOT typed columns).

    ``grain="topic"`` runs ONE report for ``objective`` and lands it keyed ``run_id``.
    ``grain="per_entity"`` requires ``audience_id`` (a ``company_id`` audience) and runs one
    report PER company (``objective`` steers what to research about each), landed keyed
    ``company_id``. Both land in ``s3://data-sink/active/parallel_research/`` (``report_md`` +
    ``basis`` citations sidecar).

    ``processor`` ∈ lite|base|core|pro (default base). **``ultra`` is REJECTED** — it runs for
    hours and needs Parallel's per-run webhook → trigger-token path (deferred per §0/§9).
    ``output_type`` ∈ text|auto. ``test_limit=3`` (default, per_entity only) samples a few
    companies first; ``0`` runs all. ``max_runs`` hard-caps per_entity reports. Returns a
    Trigger run handle — the report(s) land async; query them by their key.
    """
    obj = (objective or "").strip()
    if len(obj) < 8:
        raise ValueError("objective must be at least 8 chars")
    g = (grain or "topic").strip().lower()
    if g not in ("topic", "per_entity"):
        raise ValueError("grain must be 'topic' or 'per_entity'")
    proc = (processor or "base").strip().lower()
    if proc == "ultra":
        raise ValueError(
            "processor 'ultra' is not supported by deep_research v1 — it runs for hours and "
            "requires Parallel's per-run webhook → trigger-token path (deferred per §0/§9). Use up to 'pro'."
        )
    if proc not in RESEARCH_PROCESSORS:
        raise ValueError(f"processor must be one of {RESEARCH_PROCESSORS}")
    if (output_type or "text").strip().lower() not in ("text", "auto"):
        raise ValueError("output_type must be 'text' or 'auto'")

    aid = (audience_id or "").strip()
    if g == "per_entity":
        if not aid:
            raise ValueError("grain='per_entity' requires audience_id")
        aud = _audience_row(aid)
        if not aud:
            raise ValueError(f"unknown audience_id {aid!r} — define it first with define_audience.")
        if aud["result_key"] != "company_id":
            raise ValueError(
                f"per_entity research requires a company_id audience; {aid} resolves to "
                f"{aud['result_key']!r}."
            )
    else:
        aid = ""

    sid = (spec_id or "").strip()
    if sid:
        if not _SPEC_RE.match(sid):
            raise ValueError(f"spec_id must match ^[a-z0-9_]{{3,64}}$ (got {sid!r})")
        _persist_spec(sid, "deep_research", proc, output_schema=None, objective=obj, grain=g,
                      dataset_uri=_RESEARCH_URI, result_key=("company_id" if g == "per_entity" else "run_id"))

    run_kind = "test" if (g == "per_entity" and (test_limit or 0) > 0) else "full"
    payload: dict[str, Any] = {
        "objective": obj, "grain": g, "processor": proc,
        "outputType": (output_type or "text").strip().lower(),
        "testLimit": max(0, int(test_limit or 0)),
        "idempotencyKey": f"{sid or 'research'}:{aid or 'topic'}:{run_kind}",
    }
    if sid:
        payload["specId"] = sid
    if aid:
        payload["audienceId"] = aid
    if max_runs is not None:
        payload["maxRuns"] = int(max_runs)
    if max_usd is not None:
        payload["maxUsd"] = float(max_usd)
    handle = _trigger_run(TASK_DEEP_RESEARCH, payload)
    return {
        "status": "launched", "workflow": "deep_research", "task_id": TASK_DEEP_RESEARCH,
        "grain": g, "processor": proc, "objective": obj, "audience_id": aid or None,
        "run_id": handle.get("id"), "dataset_uri": _RESEARCH_URI,
    }


def web_search(
    objective: str | None = None,
    search_queries: list[str] | None = None,
    mode: str = "advanced",
    source_policy: dict | None = None,
    persist: bool = False,
    spec_id: str | None = None,
) -> dict[str, Any]:
    """Launch Parallel WEB SEARCH — synchronous evidence-gathering; ranked excerpts come back
    INLINE for in-loop reasoning.

    Pass an ``objective`` and/or up to 5 ``search_queries``. Parallel's Search API returns ranked
    excerpts in one call (no run lifecycle, no Basis). The excerpts are returned inline in the
    Trigger run's output (read them directly). ``persist=true`` ALSO lands the excerpts to
    ``s3://data-sink/active/parallel_search/`` (keyed ``run_id``) for later reuse; default is
    inline-only. ``mode`` ∈ advanced|base; ``source_policy`` optionally scopes domains/date
    (``{include_domains?, exclude_domains?, after_date?}``). Returns a Trigger run handle whose
    output carries the excerpts.
    """
    obj = (objective or "").strip()
    queries = [str(q).strip() for q in (search_queries or []) if str(q).strip()][:5]
    if not obj and not queries:
        raise ValueError("objective or search_queries is required")
    if (mode or "advanced").strip().lower() not in ("advanced", "base"):
        raise ValueError("mode must be 'advanced' or 'base'")
    sid = (spec_id or "").strip()
    if sid and not _SPEC_RE.match(sid):
        raise ValueError(f"spec_id must match ^[a-z0-9_]{{3,64}}$ (got {sid!r})")

    payload: dict[str, Any] = {
        "objective": obj, "searchQueries": queries, "mode": (mode or "advanced").strip().lower(),
        "persist": bool(persist),
    }
    if sid:
        payload["specId"] = sid
    if source_policy:
        payload["sourcePolicy"] = source_policy
    handle = _trigger_run(TASK_SEARCH, payload)
    return {
        "status": "launched", "workflow": "search", "task_id": TASK_SEARCH,
        "objective": obj or None, "query_count": len(queries), "persist": bool(persist),
        "run_id": handle.get("id"),
    }


def refresh_catalog() -> dict[str, Any]:
    """Refresh the gtm-mcp dataset catalog so a just-landed Parallel dataset (e.g.
    ``enrichment/<spec>`` or ``parallel_research``) is FULLY visible to ``execute_audience_query``
    and ``describe_dataset`` — name AND columns.

    A registry refresh alone is insufficient: ``database.get_registry(refresh=True)`` re-lists
    the active sink, but ``catalog.py``'s module caches (``_lance_cache`` / ``_schema_cache``)
    would still serve a stale (or memoized-``None``) schema until restart. This calls BOTH —
    ``get_registry(refresh=True)`` and ``catalog.invalidate()`` — so the new dataset resolves
    cleanly. Call it once after a Parallel run lands. Returns the refreshed dataset count.
    """
    reg = database.get_registry(refresh=True)
    catalog.invalidate()
    return {
        "status": "ok",
        "dataset_count": len([n for n in reg if n not in database.ALIASES]),
        "detail": "registry re-listed and catalog schema caches cleared.",
    }


def _load_spec(spec_id: str) -> dict | None:
    """Read one ops.parallel_specs row (output_schema parsed) or None."""
    import psycopg

    with psycopg.connect(_require_dsn()) as conn, conn.cursor() as cur:
        cur.execute(_SPECS_DDL)
        cur.execute(
            "SELECT spec_id, workflow, processor, output_schema, objective, grain, dataset_uri, "
            "result_key FROM ops.parallel_specs WHERE spec_id = %s",
            (spec_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "spec_id": row[0], "workflow": row[1], "processor": row[2],
        "output_schema": row[3], "objective": row[4], "grain": row[5],
        "dataset_uri": row[6], "result_key": row[7],
    }


def register(mcp) -> None:
    """Mount the Parallel launch + spec + refresh tools onto the FastMCP server."""
    for fn in (
        define_enrichment_spec,
        enrich_companies,
        deep_research,
        web_search,
        refresh_catalog,
    ):
        mcp.add_tool(fn)
