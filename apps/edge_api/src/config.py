"""Runtime configuration for edge_api.

Secrets come from the environment (Doppler ``core-x/prd`` locally and on the
deployed public service). Nothing is committed. The proposal-template authoring
surface uses R2 for preview PDFs (creds already in ``core-x/prd``); the rest of
edge_api reads the same ``HQX_*`` Postgres and ``MANAGED_*`` /
``DMAAS_MCP_BEARER_TOKEN`` values already present there.
"""
from __future__ import annotations

import os


def service_token() -> str | None:
    """The shared secret the platform-api BFF (and Trigger.dev) present as
    ``Authorization: Bearer`` on the agent-runs + pipeline surface. When unset
    (local dev) the gate warns and allows; every deployed environment sets it."""
    return os.environ.get("EDGE_API_SERVICE_TOKEN")


def documenso_api_key() -> str | None:
    """Documenso Cloud API key (format ``api_...``), server-side only. From ``core-x/prd``."""
    return os.environ.get("DOCUMENSO_API_KEY")


def documenso_api_url() -> str:
    """Documenso instance base URL. The ``host`` passed to the embed MUST match this — a doc
    created here cannot be signed against a different instance."""
    return os.environ.get("DOCUMENSO_API_URL", "https://app.documenso.com").rstrip("/")


def documenso_webhook_secret() -> str | None:
    """Shared secret Documenso echoes verbatim in the ``X-Documenso-Secret`` header. When unset
    the webhook route refuses (503) rather than accepting unverified events."""
    return os.environ.get("DOCUMENSO_WEBHOOK_SECRET")


def cal_webhook_secret() -> str | None:
    """Secret cal.com signs each delivery with — HMAC-SHA256 over the RAW body, sent as
    ``X-Cal-Signature-256``. When unset, ``/webhooks/cal`` refuses (503) rather than accepting
    unverified events. From ``core-x/prd`` (``CAL_WEBHOOK_SECRET``); must match the secret set on
    the cal.com webhook."""
    return os.environ.get("CAL_WEBHOOK_SECRET")


def docraptor_api_key() -> str | None:
    """DocRaptor API key. Used in LIVE mode (``test=false``) — test output is watermarked."""
    return os.environ.get("DOCRAPTOR_API_KEY")


def rs_signer_name() -> str:
    """Rare Structure's signatory name pre-rendered into the agreement (Managing Director)."""
    return os.environ.get("RS_SIGNER_NAME", "Benjamin Crane")


def partner_platform_base_url() -> str:
    """Public base URL of the consumer proposal page — used to build shareable proposal links."""
    return os.environ.get("PARTNER_PLATFORM_BASE_URL", "http://localhost:3000").rstrip("/")


# ── Cloudflare R2 (S3-compatible) — proposal preview/artifact storage ─────────────────
# edge_api's first object-storage use. Endpoint + creds already live in core-x/prd
# (the same R2 the data lake uses). Only the bucket is service-specific.
def r2_endpoint() -> str | None:
    """R2 S3 endpoint, e.g. ``https://<account>.r2.cloudflarestorage.com``."""
    return os.environ.get("R2_ENDPOINT")


def r2_access_key_id() -> str | None:
    return os.environ.get("R2_ACCESS_KEY_ID")


def r2_secret_access_key() -> str | None:
    return os.environ.get("R2_SECRET_ACCESS_KEY")


def r2_proposal_bucket() -> str:
    """Bucket for proposal preview PDFs. Defaults to the data-lake bucket; preview objects live
    under a segregated ``proposals/`` prefix (never the ``active/`` SoR namespace). Override with
    ``R2_PROPOSAL_BUCKET`` to point at a dedicated bucket."""
    return os.environ.get("R2_PROPOSAL_BUCKET", "data-sink")


def port() -> int:
    """Bind port. The deployed service injects ``$PORT``; default for a bare local run."""
    return int(os.environ.get("PORT", "8080"))


def host() -> str:
    """Bind address. Defaults to ``0.0.0.0`` — edge_api is a PUBLIC service
    (Anthropic's platform calls the MCP mounts; Trigger.dev calls the pipeline),
    unlike the private, IPv6-only catalyst_api. Override with ``HOST`` if needed."""
    return os.environ.get("HOST", "0.0.0.0")
