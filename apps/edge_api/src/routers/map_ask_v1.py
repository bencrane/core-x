"""Map ``/ask`` — the TRANSLATE route. NL sentence → forced-tool Messages call →
constrained filter object → catalyst_api EXECUTE → GeoJSON. The single LLM touchpoint
of the portal map; deterministic from the filter object onward.

  POST /api/v1/map/{dataset}/ask   {"q": "<sentence>"}   service-token gated

Free-typed translations are memoized on ``(normalized_sentence, decoder_version, model)``
— the FILTER OBJECT only, never the GeoJSON — so a repeated sentence skips the LLM but
every hit still re-executes against live Lance (a decoder change bumps the version and
busts the key). Relative-time clauses stay memo-safe by construction: ``days_ago``
filters carry a day COUNT, and EXECUTE resolves it against the request date. Canned
toggles bypass this route: the BFF POSTs a filter object straight to catalyst EXECUTE.

HONESTY CONTRACT: the compiler must never silently drop a user constraint. The forced
tool carries an ``unmapped`` array (verbatim phrases the allowlist cannot express); it
rides back to the BFF in ``query.unmapped`` so the cockpit can render "not applied".

QA LEDGER: every terminal state (success, translate failure, execute failure) is
recorded to ``ops.map_query_runs`` (HQX Postgres, DDL in ``apps/edge_api/sql/
ops_map_query_runs.sql``) as a fire-and-forget task — a ledger error can never block
or fail the response.

This route never touches gtm_mcp or the gtm-agent — those are the operator console, a
different surface entirely.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .. import config, map_decoders
from ..db import get_db_connection
from ..service_token import require_service_token
from ..services import anthropic_messages, catalyst_client

log = logging.getLogger("edge_api.map_ask")

router = APIRouter(prefix="/api/v1/map", tags=["map"])

# (normalized_q, decoder_version, model) -> filter object. Process-local, cleared on
# deploy. Stores the FILTER OBJECT only (never GeoJSON) so every hit re-executes live.
_MEMO: dict[tuple[str, str, str], dict] = {}

# Bounds on the model-emitted `unmapped` strings (defense against a runaway emission).
_UNMAPPED_MAX_ITEMS = 8
_UNMAPPED_MAX_LEN = 200


class AskRequest(BaseModel):
    q: str


def _normalize(q: str) -> str:
    return " ".join((q or "").lower().split())


def _sanitize_unmapped(raw: Any) -> list[str]:
    """Clamp the model-emitted ``unmapped`` array to a bounded list of short strings —
    it is echoed to the UI and written to the ledger, so its shape is enforced here."""
    if not isinstance(raw, list):
        return []
    out = [u.strip()[:_UNMAPPED_MAX_LEN] for u in raw if isinstance(u, str) and u.strip()]
    return out[:_UNMAPPED_MAX_ITEMS]


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
    filt["unmapped"] = _sanitize_unmapped(filt.get("unmapped"))
    _MEMO[key] = filt
    return filt


# ── ops.map_query_runs — the lagging QA stamp ─────────────────────────────────
# Held references so fire-and-forget tasks are not garbage-collected mid-flight.
_LEDGER_TASKS: set[asyncio.Task] = set()


async def _write_query_run(*, dataset: str, q: str, filt: dict | None, meta: dict | None,
                           status: str, error: str | None, latency_ms: int) -> None:
    """One INSERT into ops.map_query_runs. Best-effort by contract: any failure is a
    log line, never an exception that reaches the caller (the response already left)."""
    try:
        decoder_version = map_decoders.DECODERS[dataset]["version"]
        async with get_db_connection() as conn:
            await conn.execute(
                "INSERT INTO ops.map_query_runs (dataset, raw_query, title, compiled_filters, "
                "dropped, decoder_version, result_count, total_count, capped, latency_ms, "
                "status, error_message) VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, "
                "%s, %s, %s, %s)",
                (
                    dataset,
                    q,
                    (filt or {}).get("title"),
                    json.dumps((filt or {}).get("filters")) if filt else None,
                    json.dumps((filt or {}).get("unmapped")) if filt else None,
                    decoder_version,
                    (meta or {}).get("returned"),
                    (meta or {}).get("total"),
                    (meta or {}).get("capped"),
                    latency_ms,
                    status,
                    error,
                ),
            )
    except Exception:  # noqa: BLE001 — the ledger must never break the serving path
        log.warning("ops.map_query_runs write failed", exc_info=True)


def _record_query_run(**kwargs) -> None:
    """Schedule the ledger write without awaiting it (fire-and-forget)."""
    try:
        task = asyncio.create_task(_write_query_run(**kwargs))
        _LEDGER_TASKS.add(task)
        task.add_done_callback(_LEDGER_TASKS.discard)
    except Exception:  # noqa: BLE001 — scheduling failure is also non-fatal
        log.warning("ops.map_query_runs scheduling failed", exc_info=True)


@router.post("/{dataset}/ask", dependencies=[Depends(require_service_token)])
async def ask(dataset: str, body: AskRequest) -> JSONResponse:
    """Translate the NL query → filter, execute it on catalyst_api, return the GeoJSON
    envelope plus the interpreted ``query`` (title + filters + unmapped) for the UI to
    echo — ``unmapped`` is the honest "not applied" signal."""
    if dataset not in map_decoders.DECODERS:
        raise HTTPException(status_code=404, detail=f"unknown map dataset {dataset!r}")
    q = (body.q or "").strip()
    if not q:
        raise HTTPException(status_code=422, detail="q required")
    if config.anthropic_api_key() is None:
        raise HTTPException(status_code=503, detail="map /ask unavailable: ANTHROPIC_API_KEY unset")
    if config.catalyst_base_url() is None:
        raise HTTPException(status_code=503, detail="map /ask unavailable: CATALYST_API_BASE_URL unset")
    started = time.monotonic()

    def _ms() -> int:
        return int((time.monotonic() - started) * 1000)

    try:
        filt = await _translate(dataset, q)
    except anthropic_messages.AnthropicMessagesError as exc:
        log.warning("map /ask translate failed: %s", exc.message)
        _record_query_run(dataset=dataset, q=q, filt=None, meta=None,
                          status="translate_error", error=(exc.message or "")[:500], latency_ms=_ms())
        raise HTTPException(status_code=502, detail="translation failed")
    try:
        envelope = await catalyst_client.execute(dataset, filt)
    except catalyst_client.CatalystError as exc:
        log.warning("map /ask execute failed: %s", exc.message)
        _record_query_run(dataset=dataset, q=q, filt=filt, meta=None,
                          status="execute_error", error=(exc.message or "")[:500], latency_ms=_ms())
        raise HTTPException(status_code=502, detail="map execute failed")
    _record_query_run(dataset=dataset, q=q, filt=filt, meta=envelope.get("meta"),
                      status="success", error=None, latency_ms=_ms())
    return JSONResponse({**envelope, "query": filt})
