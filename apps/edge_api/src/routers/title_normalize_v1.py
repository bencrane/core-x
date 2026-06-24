"""Job-title normalization — the pre-enrichment classification gate.

  POST /api/v1/titles/normalize   {"raw_title": "VP of Making Things Go"}   service-token gated

ONE forced-tool Anthropic Messages call compiles a single raw, scraped job title into the
strict internal taxonomy — exactly one Job Level and exactly one Job Function — so the
expensive enrichment tier downstream fans out on normalized, low-cardinality keys instead
of free-text titles. The forced tool's ``input_schema`` pins both fields to closed enums,
so the model cannot emit a category outside the taxonomy; a Python-side allowlist coerces
anything unexpected (or a missing field) to ``Other`` as defense-in-depth.

The taxonomy (6 levels × 22 functions) is the canonical internal vocabulary documented in
``docs/reference/BLITZ_API_CANONICAL_REFERENCE.md`` and is the single source of truth here —
the prompt text, the tool enum schema, and the coercion allowlist all derive from the two
tuples below. The public request/response contract is deliberately provider-neutral
(``raw_title`` / ``normalized_level`` / ``normalized_function``); the model behind it is
config-selectable (``TITLE_NORMALIZER_MODEL``, default ``claude-haiku-4-5``).

Stateless by design — a transform in front of enrichment, not a system of record (no DB,
no ledger). A hard model/transport failure surfaces as 502 (an infrastructure error the
caller retries), distinct from a low-confidence classification (which resolves to ``Other``).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from .. import config
from ..service_token import require_service_token
from ..services import anthropic_messages

log = logging.getLogger("edge_api.title_normalize")

router = APIRouter(prefix="/api/v1/titles", tags=["titles"])

# ── Canonical internal taxonomy — single source of truth for prompt + schema + coercion ──
# 6 seniority levels × 22 functions. Exact strings, published reference order. Changing a
# value here updates the model prompt, the enum the output is constrained to, AND the
# server-side allowlist in lock-step — they can never drift.
LEVELS: tuple[str, ...] = ("C-Team", "Director", "Manager", "Other", "Staff", "VP")

FUNCTIONS: tuple[str, ...] = (
    "Advertising & Marketing",
    "Art, Culture and Creative Professionals",
    "Construction",
    "Customer/Client Service",
    "Education",
    "Engineering",
    "Finance & Accounting",
    "General Business & Management",
    "Healthcare & Human Services",
    "Human Resources",
    "Information Technology",
    "Legal",
    "Manufacturing & Production",
    "Operations",
    "Other",
    "Public Administration & Safety",
    "Purchasing",
    "Research & Development",
    "Sales & Business Development",
    "Science",
    "Supply Chain & Logistics",
    "Writing/Editing",
)

_FALLBACK = "Other"
_MAX_TITLE_LEN = 512
_TOOL_NAME = "emit_normalized_title"

_SYSTEM_PROMPT = (
    "You are a B2B data normalization engine. You will be given ONE raw, scraped corporate "
    "job title. Map it to exactly one Job Level and exactly one Job Function from the strict "
    "lists below.\n\n"
    "Allowed Job Levels:\n- " + "\n- ".join(LEVELS) + "\n\n"
    "Allowed Job Functions:\n- " + "\n- ".join(FUNCTIONS) + "\n\n"
    "Rules:\n"
    "- Choose the single closest match on each axis.\n"
    "- Job Level is seniority (C-Team > VP > Director > Manager > Staff). Individual "
    "contributors, specialists, and analysts are 'Staff'.\n"
    "- Job Function is the department/discipline, independent of seniority.\n"
    "- If nothing fits an axis, use 'Other'.\n"
    "- Emit your answer ONLY by calling the emit_normalized_title tool."
)

# cache_control on the (identical-every-call) taxonomy block → prompt-cached across requests.
_SYSTEM_BLOCKS = [{"type": "text", "text": _SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}]

_TOOL = {
    "name": _TOOL_NAME,
    "description": "Emit the normalized job level and job function for the given raw job title.",
    "input_schema": {
        "type": "object",
        "properties": {
            "normalized_level": {
                "type": "string",
                "enum": list(LEVELS),
                "description": "The single best-matching seniority level from the allowed list.",
            },
            "normalized_function": {
                "type": "string",
                "enum": list(FUNCTIONS),
                "description": "The single best-matching department/function from the allowed list.",
            },
        },
        "required": ["normalized_level", "normalized_function"],
        "additionalProperties": False,
    },
}


class NormalizeTitleRequest(BaseModel):
    raw_title: str = Field(..., description="A single raw, scraped job title.")


class NormalizeTitleResponse(BaseModel):
    raw_title: str
    normalized_level: str
    normalized_function: str


@router.post("/normalize", response_model=NormalizeTitleResponse,
             dependencies=[Depends(require_service_token)])
async def normalize_title(body: NormalizeTitleRequest) -> NormalizeTitleResponse:
    """Normalize one raw job title → ``(normalized_level, normalized_function)`` drawn from the
    strict 6×22 taxonomy. The forced-tool enum schema guarantees in-vocabulary output; the
    allowlist coercion below is defense-in-depth (a missing field or unexpected value → Other).
    A hard model/transport failure is surfaced as 502 — an infrastructure error the caller
    retries, never a silently-persisted 'Other'."""
    raw_title = (body.raw_title or "").strip()
    if not raw_title:
        raise HTTPException(status_code=422, detail="raw_title required")
    if len(raw_title) > _MAX_TITLE_LEN:
        raise HTTPException(status_code=422, detail=f"raw_title exceeds {_MAX_TITLE_LEN} chars")
    if config.anthropic_api_key() is None:
        raise HTTPException(status_code=503, detail="title normalize unavailable: ANTHROPIC_API_KEY unset")

    try:
        out = await anthropic_messages.force_tool(
            model=config.title_normalizer_model(),
            system_blocks=_SYSTEM_BLOCKS,
            tool=_TOOL,
            tool_name=_TOOL_NAME,
            user_text=raw_title,
        )
    except anthropic_messages.AnthropicMessagesError as exc:
        log.warning("title normalize failed: %s", exc.message)
        raise HTTPException(status_code=502, detail="normalization failed")

    level = out.get("normalized_level")
    function = out.get("normalized_function")
    return NormalizeTitleResponse(
        raw_title=raw_title,
        normalized_level=level if level in LEVELS else _FALLBACK,
        normalized_function=function if function in FUNCTIONS else _FALLBACK,
    )
