"""Runtime configuration for edge_api.

Secrets come from the environment (Doppler ``core-x/prd`` locally and on the
deployed public service). Nothing is committed. edge_api needs no R2 / Lance —
only the service token + bind coordinates here; later phases read the same
``HQX_*`` Postgres and ``MANAGED_*`` / ``DMAAS_MCP_BEARER_TOKEN`` values already
present in ``core-x/prd``.
"""
from __future__ import annotations

import os


def service_token() -> str | None:
    """The shared secret the platform-api BFF (and Trigger.dev) present as
    ``Authorization: Bearer`` on the agent-runs + pipeline surface. When unset
    (local dev) the gate warns and allows; every deployed environment sets it."""
    return os.environ.get("EDGE_API_SERVICE_TOKEN")


def port() -> int:
    """Bind port. The deployed service injects ``$PORT``; default for a bare local run."""
    return int(os.environ.get("PORT", "8080"))


def host() -> str:
    """Bind address. Defaults to ``0.0.0.0`` — edge_api is a PUBLIC service
    (Anthropic's platform calls the MCP mounts; Trigger.dev calls the pipeline),
    unlike the private, IPv6-only catalyst_api. Override with ``HOST`` if needed."""
    return os.environ.get("HOST", "0.0.0.0")
