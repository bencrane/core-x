"""Catalog tools — runtime schema discovery for the GTM Lance plane.

Two-level by design (progressive disclosure), mirroring the Postgres introspection
in tools/ops.py — so an agent narrows down instead of pulling every column of every
one of the ~125 discovered datasets in a single oversized result:

  • ``list_datasets`` — the cheap "look around": every discovered dataset name with a
    column COUNT (no column detail). One ``active/catalog.json`` GET, no Lance opens.
    The entry point.
  • ``describe_dataset`` — drill into ONE dataset's columns (with types). Columns come
    from the ``active/catalog.json`` manifest when present (free); for the few
    datasets the manifest omits (the GTM-materialized edge datasets like
    ``companies``), the schema is read straight off that one dataset's committed Lance
    manifest (a single open, memoized) — never the whole plane.

The registry these surface lives in database.py and is shared with the JIT query
path, so what ``list_datasets`` advertises is exactly what ``execute_audience_query``
can bind.
"""

from __future__ import annotations

import json
from typing import Any

from .. import database

_CATALOG_KEY = f"{database.ACTIVE_PREFIX}catalog.json"

_schema_cache: dict[str, list[dict[str, str]]] | None = None  # manifest: name → [{name,type}]
_lance_cache: dict[str, list[dict[str, str]] | None] = {}      # per-dataset Lance schema, memoized


def _catalog_schema() -> dict[str, list[dict[str, str]]]:
    """``dataset_name → [{"name", "type"}]`` parsed from ``active/catalog.json`` (a
    maintained manifest, grouped by domain). Cached for the process; best-effort — an
    absent or malformed manifest yields ``{}`` (datasets then resolve via Lance)."""
    global _schema_cache
    if _schema_cache is not None:
        return _schema_cache

    out: dict[str, list[dict[str, str]]] = {}
    raw = database.get_object_bytes(_CATALOG_KEY)
    if raw:
        try:
            domains = json.loads(raw).get("domains", {})
            for entries in domains.values():
                for entry in entries:
                    name = entry.get("dataset_name")
                    schema = entry.get("schema")
                    if name and isinstance(schema, dict):
                        out[name] = [{"name": col, "type": str(typ)} for col, typ in schema.items()]
        except Exception:  # noqa: BLE001 — enrichment only; never load-bearing
            out = {}
    _schema_cache = out
    return out


def _lance_schema_cols(name: str) -> list[dict[str, str]] | None:
    """``[{"name", "type"}]`` straight off one dataset's committed Lance schema.
    Best-effort — ``None`` if the dataset cannot be opened. Memoized for the process
    so a repeated drill-down never re-opens the manifest."""
    if name in _lance_cache:
        return _lance_cache[name]
    try:
        schema = database.open_dataset(name).schema  # pyarrow.Schema
        cols: list[dict[str, str]] | None = [
            {"name": f.name, "type": str(f.type)} for f in schema
        ]
    except Exception:  # noqa: BLE001
        cols = None
    _lance_cache[name] = cols
    return cols


def list_datasets() -> dict[str, Any]:
    """Look around the GTM Lance plane CHEAPLY: every dataset available to
    ``execute_audience_query`` with a column COUNT (no column detail). **Start here**,
    then call ``describe_dataset(name)`` to pull the columns of only the dataset(s) you
    need — instead of dumping every column of all ~125 datasets at once.

    The catalog is discovered at runtime from the active R2 sink — not a fixed list,
    so a newly landed dataset appears automatically. Flat names are bare identifiers
    (``companies``); a name containing ``/`` is nested under a source namespace and
    MUST be double-quoted in SQL, e.g. ``FROM "usaspending/award_search"``. ``aliases``
    maps friendly names to their canonical dataset (``awards`` →
    ``contractor_award_summary``).

    Returns ``{"dataset_count", "datasets": [{"name", "column_count"?}], "aliases",
    "usage"}``. ``column_count`` is present when known from the manifest; for the few
    datasets it omits, call ``describe_dataset`` (which reads the Lance schema).
    """
    names = database.dataset_names()
    catalog = _catalog_schema()
    datasets: list[dict[str, Any]] = []
    for name in names:
        entry: dict[str, Any] = {"name": name}
        cols = catalog.get(name)
        if cols is not None:
            entry["column_count"] = len(cols)
        datasets.append(entry)

    return {
        "dataset_count": len(datasets),
        "datasets": datasets,
        "aliases": dict(database.ALIASES),
        "usage": (
            "Call describe_dataset(name) for a dataset's columns. Reference any name "
            'as a relation in execute_audience_query; names containing "/" must be '
            'double-quoted, e.g. FROM "usaspending/award_search".'
        ),
    }


def describe_dataset(name: str) -> dict[str, Any]:
    """Drill into ONE Lance dataset's columns (with types) — the targeted companion to
    ``list_datasets``. Pass a name exactly as ``list_datasets`` advertises it
    (``companies``, ``"usaspending/award_search"`` without the SQL quotes here, or an
    alias like ``awards``).

    Columns come from the ``active/catalog.json`` manifest when present; otherwise the
    one dataset's committed Lance schema is read directly (a single manifest open,
    memoized). Returns ``{"name", "found", "column_count", "columns": [{"name",
    "type"}], "source": "catalog_manifest"|"lance_schema"}``. An unknown name returns
    ``found=false`` (call ``list_datasets`` for the valid names).
    """
    key = (name or "").strip()
    if key not in database.get_registry():
        return {
            "name": key,
            "found": False,
            "column_count": 0,
            "columns": [],
            "detail": "unknown dataset; call list_datasets for the available names.",
        }

    canonical = database.ALIASES.get(key, key)
    cols = _catalog_schema().get(canonical)
    source = "catalog_manifest"
    if cols is None:
        cols = _lance_schema_cols(key)  # open_dataset resolves the alias → uri
        source = "lance_schema"

    if cols is None:
        return {
            "name": key,
            "found": False,
            "column_count": 0,
            "columns": [],
            "detail": "dataset is registered but its schema could not be resolved.",
        }
    return {
        "name": key,
        "found": True,
        "column_count": len(cols),
        "columns": cols,
        "source": source,
    }


def register(mcp) -> None:
    """Mount the catalog tools onto the FastMCP server. Each function's signature +
    docstring becomes the tool's input schema + agent-facing contract."""
    for fn in (list_datasets, describe_dataset):
        mcp.add_tool(fn)
