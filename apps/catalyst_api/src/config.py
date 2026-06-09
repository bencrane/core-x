"""Runtime configuration for catalyst_api.

Secrets and dataset coordinates come from the environment (Doppler ``core-x/prd``
locally + on the ``catalyst-api`` Railway service via ``DOPPLER_TOKEN``). Nothing
is committed. Two concerns:

  • R2 credentials — identical convention to every worker in ``pipelines/*`` and
    to ``apps/gtm_mcp`` (``R2_ACCESS_KEY_ID`` / ``R2_SECRET_ACCESS_KEY`` /
    ``R2_ENDPOINT``, with ``R2_ACCOUNT_ID`` accepted as an endpoint fallback).
  • The operator service token (``CATALYST_API_TOKEN``) each consuming BFF must
    present. The gateway is public (shared, cross-project) — the bearer token is
    the auth boundary; boot is fail-closed in any deployed env (see ``main.py``).

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
# SAM.gov entity surfaces. ``sam_entity_master`` (1 row/active UEI, BTREE uei) carries
# the SAM identity + NAICS/PSC + raw business_types + physical city/state/zip5.
# ``sam_pocs`` (BTREE uei, BITMAP poc_type) carries the government POC slots — no
# email/phone columns exist at source.
SAM_ENTITY_MASTER_URI = os.environ.get(
    "SAM_ENTITY_MASTER_LANCE_URI", "s3://data-sink/active/sam_entity_master/"
)
SAM_POCS_URI = os.environ.get(
    "SAM_POCS_LANCE_URI", "s3://data-sink/active/sam_pocs/"
)
# The unified Gold Mirror (entity_profile_gold v2.1, 1 row/UEI, BTREE uei) — the
# SAM×USAspending write-time reconciliation. Pre-materializes lifetime/active
# obligation sums + award counts, so the Overview surface and the active/past
# count+total headlines are pure point-lookups (NO on-the-fly aggregate).
ENTITY_PROFILE_GOLD_URI = os.environ.get(
    "ENTITY_PROFILE_GOLD_LANCE_URI", "s3://data-sink/active/entity_profile_gold/"
)


# ── Operator service token (BFF → catalyst_api) ──────────────────────────────
def operator_token() -> str | None:
    """The shared secret the platform-api BFF presents as ``Authorization: Bearer``.
    When unset (local dev) the token gate warns and allows; production sets it, so
    enforcement is live there. The BFF holds the same value as ``COREX_SERVICE_TOKEN``."""
    return os.environ.get("CATALYST_API_TOKEN")


def auth_required() -> bool:
    """Fail-closed switch: when the service runs anywhere but a bare local dev box,
    an unset ``CATALYST_API_TOKEN`` is fatal at boot — the private gateway must never
    silently run unauthenticated against a live R2 sink. True when the deploy sets
    ``CATALYST_REQUIRE_AUTH`` truthy or Railway injects ``RAILWAY_ENVIRONMENT``."""
    if os.environ.get("CATALYST_REQUIRE_AUTH", "").strip().lower() in ("1", "true", "yes"):
        return True
    return bool(os.environ.get("RAILWAY_ENVIRONMENT"))


def port() -> int:
    """Railway injects ``$PORT`` (the service pins it to 8080); default for a bare
    local run."""
    return int(os.environ.get("PORT", "8080"))


def host() -> str:
    """Bind address. Defaults to ``::`` — Railway's private network is IPv6-only,
    so the co-located BFF can only reach an IPv6-bound listener (``0.0.0.0`` would
    be invisible on the private net). Dual-stack also accepts IPv4 for local runs.
    Override with ``HOST`` if a deploy target needs IPv4-only."""
    return os.environ.get("HOST", "::")
