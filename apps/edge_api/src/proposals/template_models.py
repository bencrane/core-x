"""Proposal-template authoring contracts — request/response shapes for the Settings surface."""
from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field


class TemplateConvertRequest(BaseModel):
    """Live markdown → HTML for the editor's right pane. Stateless (no DB, no DocRaptor)."""

    markdown: str = ""
    apply_brand: bool = True


class TemplateConvertResult(BaseModel):
    html: str
    detected_tokens: list[str]


class TemplatePreviewRequest(BaseModel):
    """Render the current editor content to a real PDF via DocRaptor. Stateless — operates on the
    inline content so the operator can preview UNSAVED work. ``token_values`` fills the
    {{handlebars}} for the preview render (the operator types one value per detected token)."""

    markdown: str = ""
    apply_brand: bool = True
    token_values: dict[str, str] = Field(default_factory=dict)


class TemplatePreviewResult(BaseModel):
    pdf_url: str               # short-lived presigned R2 GET URL — the browser opens it directly
    expires_seconds: int
    detected_tokens: list[str]


class TemplateDraftCreate(BaseModel):
    markdown: str = ""
    apply_brand: bool = True
    name: str | None = None
    created_by: str | None = None


class TemplateDraftUpdate(BaseModel):
    markdown: str | None = None
    apply_brand: bool | None = None
    name: str | None = None


class TemplatePublishRequest(BaseModel):
    name: str = Field(..., min_length=1)
    slug: str | None = None              # derived from name when omitted
    monthly_fee_cents: int | None = Field(default=None, gt=0)


class TemplateRow(BaseModel):
    """The projection the Settings tab renders (markdown included so a draft reopens populated)."""

    id: str
    slug: str | None
    name: str | None
    status: str
    markdown: str
    apply_brand: bool
    token_manifest: list[str]
    monthly_fee_cents: int | None
    created_by: str | None
    created_at: str | None
    updated_at: str | None
    published_at: str | None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "TemplateRow":
        def _iso(v: Any) -> str | None:
            return v.isoformat() if isinstance(v, _dt.datetime) else (v if isinstance(v, str) else None)

        manifest = r.get("token_manifest") or []
        return cls(
            id=r["id"],
            slug=r.get("slug"),
            name=r.get("name"),
            status=r["status"],
            markdown=r.get("markdown") or "",
            apply_brand=bool(r.get("apply_brand", True)),
            token_manifest=list(manifest) if isinstance(manifest, list) else [],
            monthly_fee_cents=r.get("monthly_fee_cents"),
            created_by=r.get("created_by"),
            created_at=_iso(r.get("created_at")),
            updated_at=_iso(r.get("updated_at")),
            published_at=_iso(r.get("published_at")),
        )


class TemplateSummary(BaseModel):
    """List-row projection for the Settings tab (no markdown body — lean)."""

    id: str
    slug: str | None
    name: str | None
    status: str
    monthly_fee_cents: int | None
    updated_at: str | None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "TemplateSummary":
        updated = r.get("updated_at")
        return cls(
            id=r["id"],
            slug=r.get("slug"),
            name=r.get("name"),
            status=r["status"],
            monthly_fee_cents=r.get("monthly_fee_cents"),
            updated_at=updated.isoformat() if isinstance(updated, _dt.datetime) else updated,
        )
