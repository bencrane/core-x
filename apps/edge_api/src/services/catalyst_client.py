"""Internal client: edge_api → catalyst_api map EXECUTE.

The TRANSLATE step (``anthropic_messages.emit_filter``) yields a ``{title, filters}``
object; this forwards it to catalyst_api's ``POST /api/v1/map/{dataset}/query`` (the
deterministic Lance-scanner EXECUTE) and returns its GeoJSON envelope verbatim. Bearer
is the shared ``CATALYST_API_TOKEN`` (catalyst's ``require_operator`` gate). Nothing in
this path touches gtm_mcp, the gtm-agent, DuckDB, or Postgres.
"""
from __future__ import annotations

from typing import Any

import httpx

from .. import config


class CatalystError(Exception):
    def __init__(self, *, status_code: int | None, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


async def execute(dataset: str, filter_obj: dict[str, Any], timeout: float = 12.0) -> dict[str, Any]:
    """POST the compiled filter object to catalyst EXECUTE; return its JSON envelope
    (``{"data": <FeatureCollection>, "meta": {...}}``). Raises ``CatalystError`` on a
    transport failure or a non-2xx from catalyst."""
    base = config.catalyst_base_url()
    if not base:
        raise CatalystError(status_code=None, message="CATALYST_API_BASE_URL is not configured")
    url = f"{base}/api/v1/map/{dataset}/query"
    headers = {"content-type": "application/json"}
    token = config.catalyst_service_token()
    if token:
        headers["authorization"] = f"Bearer {token}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.post(url, headers=headers, json=filter_obj)
        except httpx.HTTPError as exc:
            raise CatalystError(status_code=None, message=f"catalyst transport error: {exc}")
    if resp.status_code >= 400:
        raise CatalystError(status_code=resp.status_code,
                            message=f"catalyst EXECUTE HTTP {resp.status_code}: {resp.text[:500]}")
    return resp.json()
