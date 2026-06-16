"""Request/response models for the engagement-template render surface."""
from __future__ import annotations

from pydantic import BaseModel


class TemplateRef(BaseModel):
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
    style: str | None = None  # "plain" | "branded"; defaults to the manifest's style flag


class RenderResult(BaseModel):
    pdf_url: str
    expires_seconds: int
    path: str
    archetype: str
    version: str
    style: str
    pdf_bytes: int
