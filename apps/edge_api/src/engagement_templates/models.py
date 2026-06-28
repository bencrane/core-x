"""Request/response models for the engagement-template render surface."""
from __future__ import annotations

from pydantic import BaseModel


class TemplateRef(BaseModel):
    brand: str  # "active-operators" | "rare-structure" — the content root the template lives under
    path: str
    archetype: str
    version: str
    name: str
    default_style: str  # "plain" | "branded"
    styles_available: list[str]  # selectable styles for this template


class RenderRequest(BaseModel):
    path: str
    archetype: str
    version: str
    brand: str = "active-operators"  # content root; defaulted so existing callers keep working
    style: str | None = None  # "plain" | "branded"; defaults to the manifest's style flag


class RenderResult(BaseModel):
    pdf_url: str
    expires_seconds: int
    brand: str
    path: str
    archetype: str
    version: str
    style: str
    pdf_bytes: int


class RenderPushRequest(BaseModel):
    path: str
    archetype: str
    version: str
    brand: str = "active-operators"  # content root; mirrors RenderRequest's default
    style: str | None = None  # "plain" | "branded"; defaults to the manifest's style flag


class RenderPushResult(BaseModel):
    documenso_template_id: str
    documenso_numeric_id: int | None
    brand: str
    path: str
    archetype: str
    version: str
    style: str
    source_kind: str
    pdf_bytes: int
    pdf_url: str | None  # short-lived R2 audit copy of the pushed bytes (best-effort)
