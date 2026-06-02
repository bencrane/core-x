"""Catalog tool — runtime schema discovery for the GTM Lance plane.

``list_datasets`` lets an agent inspect what it can query before it writes SQL:
the dynamically discovered dataset names (database.py §1), enriched with each
dataset's columns when they can be resolved. Columns come from the maintained
``active/catalog.json`` manifest when present (one cheap GET covers ~100
datasets); for the few datasets that manifest does not carry (the GTM-materialized
edge datasets like ``companies``), the column list is read straight off the Lance
schema — bounded, so a missing manifest degrades to bare names rather than opening
the whole plane.

This is the only tool that surfaces the catalog; the registry itself lives in
database.py and is shared with the JIT query path, so what ``list_datasets`` shows
is exactly what ``execute_audience_query`` can bind.
"""

from __future__ import annotations

import json
from typing import Any

from .. import database

# Safety rail on direct Lance-schema reads. Direct reads only kick in when the
# manifest is PRESENT (so "missing" is the bounded set of edge datasets the
# manifest omits — companies, bridges, crosswalks — not the whole plane); this
# cap is a backstop against a malformed/partial manifest. Reads are memoized, so
# the cost is paid once per process.
_MAX_LANCE_SCHEMA_READS = 48

_CATALOG_KEY = f"{database.ACTIVE_PREFIX}catalog.json"

_columns_cache: dict[str, list[str]] | None = None
_lance_cols_cache: dict[str, list[str] | None] = {}


def _catalog_columns() -> dict[str, list[str]]:
    """``dataset_name → [column names]`` parsed from ``active/catalog.json`` (a
    maintained manifest, grouped by domain). Cached for the process; best-effort —
    an absent or malformed manifest yields ``{}``."""
    global _columns_cache
    if _columns_cache is not None:
        return _columns_cache

    out: dict[str, list[str]] = {}
    raw = database.get_object_bytes(_CATALOG_KEY)
    if raw:
        try:
            domains = json.loads(raw).get("domains", {})
            for entries in domains.values():
                for entry in entries:
                    name = entry.get("dataset_name")
                    schema = entry.get("schema")
                    if name and isinstance(schema, dict):
                        out[name] = list(schema.keys())
        except Exception:  # noqa: BLE001 — enrichment only; never load-bearing
            out = {}
    _columns_cache = out
    return out


def _lance_columns(name: str) -> list[str] | None:
    """Column names straight off a dataset's committed Lance schema. Best-effort —
    ``None`` if the dataset cannot be opened. Memoized for the process so repeated
    inspections never re-open a manifest."""
    if name in _lance_cols_cache:
        return _lance_cols_cache[name]
    try:
        cols: list[str] | None = list(database.open_dataset(name).schema.names)
    except Exception:  # noqa: BLE001
        cols = None
    _lance_cols_cache[name] = cols
    return cols


def list_datasets() -> dict[str, Any]:
    """List every dataset available to ``execute_audience_query``, with columns.

    The catalog is discovered at runtime from the active R2 sink — it is not a
    fixed list, so a newly landed dataset appears here automatically. Each entry
    carries the dataset ``name`` and, when resolvable, its ``columns``.

    Use a ``name`` verbatim as a relation in ``execute_audience_query``. Flat names
    are bare identifiers (``companies``); a name containing ``/`` is nested under a
    source namespace and MUST be double-quoted in SQL, e.g.
    ``FROM "usaspending/award_search"``. ``aliases`` maps friendly names to their
    canonical dataset (``awards`` → ``contractor_award_summary``).

    Returns ``{"dataset_count", "datasets": [{"name", "columns"?}], "aliases",
    "usage"}``.
    """
    names = database.dataset_names()
    catalog = _catalog_columns()
    missing = [n for n in names if n not in catalog]
    # Direct Lance reads only when the manifest is present (else "missing" is the
    # whole plane → degrade to bare names); capped as a backstop, reads memoized.
    read_from_lance = set(sorted(missing)[:_MAX_LANCE_SCHEMA_READS]) if catalog else set()

    datasets: list[dict[str, Any]] = []
    for name in names:
        columns = catalog.get(name)
        if columns is None and name in read_from_lance:
            columns = _lance_columns(name)
        entry: dict[str, Any] = {"name": name}
        if columns is not None:
            entry["columns"] = columns
        datasets.append(entry)

    result: dict[str, Any] = {
        "dataset_count": len(datasets),
        "datasets": datasets,
        "aliases": dict(database.ALIASES),
        "usage": (
            "Reference any name as a relation in execute_audience_query. Names "
            'containing "/" must be double-quoted, e.g. FROM "usaspending/award_search".'
        ),
    }
    if catalog and len(missing) > _MAX_LANCE_SCHEMA_READS:
        result["note"] = (
            f"{len(missing) - _MAX_LANCE_SCHEMA_READS} dataset(s) are listed by name "
            "only (schema-enrichment cap reached)."
        )
    return result


def register(mcp) -> None:
    """Mount the catalog tool onto the FastMCP server. The function's signature +
    docstring becomes the tool's input schema + agent-facing contract."""
    mcp.add_tool(list_datasets)
