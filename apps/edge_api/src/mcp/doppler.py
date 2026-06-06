"""MCP server exposing read access to the core-x Doppler config.

Mounted under ``/mcp/doppler`` on edge-api (the gtm-agent's edge). Lets the agent
read secret VALUES from Doppler project ``core-x`` (default config ``prd``) on
demand — e.g. ``MODAL_SECRET`` / ``MODAL_KEY`` / ``MODAL_DISPATCHER_URL``.

Auth to Doppler: ``DOPPLER_COREX_READ_TOKEN`` (a core-x service token, stored in
core-x/prd). Auth to this mount: the shared MCP bearer (``DMAAS_MCP_BEARER_TOKEN``)
at the ASGI boundary + the agent's vault credential — same as every other edge-api
mount.

SECURITY: every secret in ``core-x/<config>`` is readable via ``get_secret``.
Intentional (the operator wants the agent to use Modal etc.); the transport bearer
+ the vault credential scope it to the gtm-agent only.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from fastmcp import FastMCP

logger = logging.getLogger(__name__)

DOPPLER_API = "https://api.doppler.com/v3"
PROJECT = "core-x"

mcp = FastMCP(
    name="core-x Doppler",
    instructions=(
        "Read secrets from the core-x Doppler project (default config 'prd'). "
        "Call list_secret_names to discover what's available, then get_secret(name) "
        "to fetch a value — e.g. MODAL_SECRET, MODAL_KEY, MODAL_DISPATCHER_URL. "
        "Pass `config` to read a non-prod config (defaults to 'prd')."
    ),
)


def _headers() -> dict[str, str]:
    tok = os.environ.get("DOPPLER_COREX_READ_TOKEN")
    if not tok:
        raise RuntimeError(
            "DOPPLER_COREX_READ_TOKEN is not set on edge-api — cannot reach Doppler."
        )
    return {"Authorization": f"Bearer {tok}", "accept": "application/json"}


async def _guard(coro) -> dict[str, Any]:
    try:
        return await coro
    except Exception as e:  # noqa: BLE001 — surface as data, never tear down the session
        return {"error": "doppler_call_failed", "message": str(e)}


@mcp.tool
async def list_secret_names(config: str = "prd") -> dict[str, Any]:
    """List the NAMES of all secrets in the core-x Doppler config (default 'prd').
    Values are NOT returned — call get_secret to fetch a value."""

    async def _run() -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.get(
                f"{DOPPLER_API}/configs/config/secrets",
                headers=_headers(),
                params={"project": PROJECT, "config": config},
            )
        if r.status_code >= 400:
            return {"error": "doppler_api_error", "status_code": r.status_code, "body": r.text[:300]}
        names = sorted((r.json().get("secrets") or {}).keys())
        return {"project": PROJECT, "config": config, "count": len(names), "names": names}

    return await _guard(_run())


@mcp.tool
async def get_secret(name: str, config: str = "prd") -> dict[str, Any]:
    """Fetch the VALUE of one secret from the core-x Doppler config (default 'prd'),
    e.g. get_secret("MODAL_SECRET"). Returns {name, config, value}."""

    async def _run() -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.get(
                f"{DOPPLER_API}/configs/config/secret",
                headers=_headers(),
                params={"project": PROJECT, "config": config, "name": name},
            )
        if r.status_code >= 400:
            return {"error": "doppler_api_error", "status_code": r.status_code, "body": r.text[:300]}
        v = r.json().get("value") or {}
        value = v.get("computed") if v.get("computed") is not None else v.get("raw")
        return {"name": name, "config": config, "value": value}

    return await _guard(_run())
