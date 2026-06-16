"""Request/response shapes for the engagement-doc pathway."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class GenerateMandateRequest(BaseModel):
    """Body the BFF posts to the public generate-mandate route — just the locked package key."""
    model_config = ConfigDict(populate_by_name=True)
    package_key: str = Field(alias="packageKey")


class RenderRequest(BaseModel):
    """Body the Trigger.dev task posts to the internal render route — the deal to render."""
    model_config = ConfigDict(populate_by_name=True)
    mandate_id: str = Field(alias="mandateId")
