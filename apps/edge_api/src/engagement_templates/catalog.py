"""Content catalog for the engagement-template path — discover + safely resolve (brand, path,
archetype, version) tuples to a repo-resident template directory.

A template is any ``<brand>/<path>/<archetype>/<version>/global_engagement_content/manifest.json``
under the content root (``apps/edge_api/content``). ``brand`` selects the content root subtree
(``active-operators`` | ``rare-structure`` | ``government-contracted``); the remaining segments arrive from the operator (via the
BFF / the content-source registry), so each is validated against a strict allowlist AND the resolved
directory is confirmed to live inside the root — defense-in-depth against path traversal.

``brand`` defaults to ``active-operators`` so the original three-segment call sites keep working.
"""
from __future__ import annotations

import json
import pathlib
import re
from dataclasses import dataclass

# apps/edge_api/src/engagement_templates/catalog.py -> parents[2] == apps/edge_api
_CONTENT_ROOT = (pathlib.Path(__file__).resolve().parents[2] / "content").resolve()
_DEFAULT_BRAND = "active-operators"
_CONTENT_SUBDIR = "global_engagement_content"
_MANIFEST = "manifest.json"

# The brands (content-root subtrees) the lane will discover/resolve. An explicit allowlist so a stray
# or unvetted directory under content/ can never surface as a selectable template — enforced in BOTH
# list_templates() and resolve(). Adding a brand is a one-line, code-reviewed change here.
_ALLOWED_BRANDS = frozenset({"active-operators", "rare-structure", "government-contracted"})

# One path SEGMENT: starts alnum, then alnum / dot / dash / underscore. No slash and no leading dot,
# so "..", "/", "", and absolute paths are all rejected before any filesystem touch.
_SAFE_SEG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class CatalogError(RuntimeError):
    """Unknown template, malformed selection segment, or unreadable manifest."""


@dataclass(frozen=True)
class TemplateEntry:
    brand: str
    path: str
    archetype: str
    version: str
    name: str
    default_style: str  # "plain" | "branded"
    styles_available: tuple[str, ...]  # the manifest's stylesheet keys


def _doc_entry(manifest_path: pathlib.Path) -> dict:
    """The single document object a manifest holds (one document per manifest, first/only entry)."""
    data = json.loads(manifest_path.read_text())
    if not isinstance(data, dict) or not data:
        raise CatalogError(f"empty or invalid manifest: {manifest_path}")
    entry = next(iter(data.values()))
    if not isinstance(entry, dict):
        raise CatalogError(f"malformed manifest entry: {manifest_path}")
    return entry


def _default_style(doc: dict) -> str:
    return "plain" if doc.get("plain", True) else "branded"


def list_templates() -> list[TemplateEntry]:
    """Every selectable (brand, path, archetype, version) under the content root, sorted. A family
    whose manifest does not sit at the ``<brand>/<path>/<archetype>/<version>/`` depth is naturally
    excluded (e.g. a brand root with only a ``.gitkeep`` placeholder)."""
    out: list[TemplateEntry] = []
    if not _CONTENT_ROOT.is_dir():
        return out
    for brand_dir in sorted(
        b for b in _CONTENT_ROOT.iterdir() if b.is_dir() and b.name in _ALLOWED_BRANDS
    ):
        for path_dir in sorted(p for p in brand_dir.iterdir() if p.is_dir()):
            for arche_dir in sorted(a for a in path_dir.iterdir() if a.is_dir()):
                for ver_dir in sorted(v for v in arche_dir.iterdir() if v.is_dir()):
                    manifest = ver_dir / _CONTENT_SUBDIR / _MANIFEST
                    if not manifest.is_file():
                        continue
                    try:
                        doc = _doc_entry(manifest)
                    except (CatalogError, json.JSONDecodeError, OSError):
                        continue
                    out.append(
                        TemplateEntry(
                            brand=brand_dir.name,
                            path=path_dir.name,
                            archetype=arche_dir.name,
                            version=ver_dir.name,
                            name=str(doc.get("name") or ver_dir.name),
                            default_style=_default_style(doc),
                            styles_available=tuple(sorted((doc.get("stylesheets") or {}).keys())),
                        )
                    )
    return out


def resolve(path: str, archetype: str, version: str, *, brand: str = _DEFAULT_BRAND) -> pathlib.Path:
    """The ``global_engagement_content`` dir for a (brand, path, archetype, version) tuple. Raises
    ``CatalogError`` on a bad segment, a path that escapes the content root, or a missing manifest."""
    for seg in (brand, path, archetype, version):
        if not _SAFE_SEG.match(seg):
            raise CatalogError(f"invalid selection segment: {seg!r}")
    if brand not in _ALLOWED_BRANDS:
        raise CatalogError(f"unknown brand: {brand!r}")
    content_dir = (_CONTENT_ROOT / brand / path / archetype / version / _CONTENT_SUBDIR).resolve()
    # defense-in-depth: the resolved dir MUST sit inside the content root
    if _CONTENT_ROOT not in content_dir.parents:
        raise CatalogError("resolved path escapes the content root")
    if not (content_dir / _MANIFEST).is_file():
        raise CatalogError(f"unknown template: {brand}/{path}/{archetype}/{version}")
    return content_dir


def manifest_doc(content_dir: pathlib.Path) -> dict:
    """The document object for an already-resolved content dir."""
    return _doc_entry(content_dir / _MANIFEST)
