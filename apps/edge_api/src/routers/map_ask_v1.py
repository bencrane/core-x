"""Map ``/ask`` — the TRANSLATE route. NL sentence → forced-tool Messages call →
constrained filter object → catalyst_api EXECUTE → GeoJSON. The single LLM touchpoint
of the portal map; deterministic from the filter object onward.

  POST /api/v1/map/{dataset}/ask   {"q": "<sentence>"}   service-token gated

Free-typed translations are memoized on ``(normalized_sentence, decoder_version, model)``
— the FILTER OBJECT only, never the GeoJSON — so a repeated sentence skips the LLM but
every hit still re-executes against live Lance (a decoder change bumps the version and
busts the key). Canned toggles bypass this route: the BFF POSTs a filter object straight
to catalyst EXECUTE.

This route never touches gtm_mcp or the gtm-agent — those are the operator console, a
different surface entirely.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .. import config, map_decoders
from ..service_token import require_service_token
from ..services import anthropic_messages, catalyst_client

log = logging.getLogger("edge_api.map_ask")

router = APIRouter(prefix="/api/v1/map", tags=["map"])

# (normalized_q, decoder_version, model) -> filter object. Process-local, cleared on
# deploy. Stores the FILTER OBJECT only (never GeoJSON) so every hit re-executes live.
_MEMO: dict[tuple[str, str, str], dict] = {}


class AskRequest(BaseModel):
    q: str


def _normalize(q: str) -> str:
    return " ".join((q or "").lower().split())


async def _translate(dataset: str, q: str) -> dict:
    decoder = map_decoders.DECODERS[dataset]
    model = config.map_compiler_model()
    key = (_normalize(q), decoder["version"], model)
    if key in _MEMO:
        return _MEMO[key]
    system_blocks = [{
        "type": "text",
        "text": map_decoders.render_decoder_prompt(dataset),
        "cache_control": {"type": "ephemeral"},
    }]
    tool = map_decoders.build_emit_filter_tool(dataset)
    filt = await anthropic_messages.emit_filter(
        model=model, system_blocks=system_blocks, tool=tool, user_text=q)
    _MEMO[key] = filt
    return filt


@router.post("/{dataset}/ask", dependencies=[Depends(require_service_token)])
async def ask(dataset: str, body: AskRequest) -> JSONResponse:
    """Translate the NL query → filter, execute it on catalyst_api, return the GeoJSON
    envelope plus the interpreted ``query`` (title + filters) for the UI to echo."""
    if dataset not in map_decoders.DECODERS:
        raise HTTPException(status_code=404, detail=f"unknown map dataset {dataset!r}")
    q = (body.q or "").strip()
    if not q:
        raise HTTPException(status_code=422, detail="q required")
    if config.anthropic_api_key() is None:
        raise HTTPException(status_code=503, detail="map /ask unavailable: ANTHROPIC_API_KEY unset")
    if config.catalyst_base_url() is None:
        raise HTTPException(status_code=503, detail="map /ask unavailable: CATALYST_API_BASE_URL unset")
    try:
        filt = await _translate(dataset, q)
    except anthropic_messages.AnthropicMessagesError as exc:
        log.warning("map /ask translate failed: %s", exc.message)
        raise HTTPException(status_code=502, detail="translation failed")
    try:
        envelope = await catalyst_client.execute(dataset, filt)
    except catalyst_client.CatalystError as exc:
        log.warning("map /ask execute failed: %s", exc.message)
        raise HTTPException(status_code=502, detail="map execute failed")
    return JSONResponse({**envelope, "query": filt})
