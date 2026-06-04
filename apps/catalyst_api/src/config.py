"""Runtime configuration for catalyst_api.

Secrets and dataset coordinates come from the environment (Doppler ``core-x/prd``
locally + on Render via the service env). Nothing is committed. Two concerns:

  • R2 credentials — identical convention to every worker in ``pipelines/*`` and
    to ``apps/gtm_mcp`` (``R2_ACCESS_KEY_ID`` / ``R2_SECRET_ACCESS_KEY`` /
    ``R2_ENDPOINT``, with ``R2_ACCOUNT_ID`` accepted as an endpoint fallback).
  • The operator service token (``CATALYST_API_TOKEN``) the platform-api BFF
    must present. The service is NOT public — the BFF is the only caller.

Dataset URIs are overridable per the worker convention (``*_LANCE_URI``) but
default to the active sink roots verified live.
"""

from __future__ import annotations

import os

# ── R2 / object-store endpoint ───────────────────────────────────────────────
def r2_endpoint() -> str:
    """Full ``https://…`` R2 endpoint (Lance ``storage_options`` form). Supplied
    directly via ``R2_ENDPOINT``, or derived from ``R2_ACCOUNT_ID`` — the fleet rule."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError(
            "Set R2_ENDPOINT (or R2_ACCOUNT_ID) — catalyst_api cannot reach the R2 sink."
        )
    return endpoint


def r2_storage_options() -> dict[str, str]:
    """object_store options for the Lance reader — byte-identical to the worker
    convention in ``pipelines/*`` and ``apps/gtm_mcp``. Passed to every
    ``lance.dataset(...)`` open."""
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": r2_endpoint(),
        "region": "auto",
    }


# ── Dataset coordinates (active sink) ─────────────────────────────────────────
# The domain→UEI resolver and the two federal award datasets the directive names.
# Overridable for staging / replays; defaults are the verified active roots.
FIRMOGRAPHICS_URI = os.environ.get(
    "FIRMOGRAPHICS_LANCE_URI", "s3://data-sink/active/firmographics_blitz/"
)
CONTRACTOR_AWARD_SUMMARY_URI = os.environ.get(
    "CONTRACTOR_AWARD_SUMMARY_LANCE_URI", "s3://data-sink/active/contractor_award_summary/"
)
AWARD_SEARCH_URI = os.environ.get(
    "AWARD_SEARCH_LANCE_URI", "s3://data-sink/active/usaspending/award_search/"
)


# ── Operator service token (BFF → catalyst_api) ──────────────────────────────
def operator_token() -> str | None:
    """The shared secret the platform-api BFF presents as ``Authorization: Bearer``.
    When unset (local dev) the token gate warns and allows; production sets it, so
    enforcement is live there. The BFF holds the same value as ``COREX_SERVICE_TOKEN``."""
    return os.environ.get("CATALYST_API_TOKEN")


def port() -> int:
    """Render (and local docker) inject ``$PORT``; default for a bare local run."""
    return int(os.environ.get("PORT", "8080"))
