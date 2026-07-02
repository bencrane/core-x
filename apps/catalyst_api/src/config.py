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
# SAM.gov entity surfaces. ``sam_master_entities`` (1.54M, 1 row/UEI, BTREE uei) carries
# the SAM identity + NAICS/PSC lists + raw bus_type_string + physical address city/state/zip
# + the is_active flag. ``sam_pocs`` (BTREE uei) carries the government POC slots — no
# email/phone columns exist at source.
#
# NOTE: the prior default ``active/sam_entity_master/`` does NOT exist in the sink (the live
# dataset is ``sam_master_entities`` — inverted words), so every /sam-profile 404'd as if the
# entity were unregistered. Corrected to the live root; the column projection is remapped to
# this schema in lance_store._SAM_ENTITY_COLS + models.SamProfileResponse.from_row.
SAM_ENTITY_MASTER_URI = os.environ.get(
    "SAM_ENTITY_MASTER_LANCE_URI", "s3://data-sink/active/sam_master_entities/"
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
# Per-UEI prime award line items Gold Mirror (entity_award_lines_gold v2.1, 1 row/UEI,
# BTREE uei) — built by pipelines/resolution/award_lines_gold.py. Carries the top-N active
# and closed contract line items as nested list<struct> columns, so active-contracts /
# past-performance are sub-second point-lookups instead of a ~80s cold scan of the 78.6M-row
# award_search on every request.
ENTITY_AWARD_LINES_GOLD_URI = os.environ.get(
    "ENTITY_AWARD_LINES_GOLD_LANCE_URI", "s3://data-sink/active/entity_award_lines_gold/"
)
# Per-firm capability profile card (capability_profile, 1 row/UEI, BTREE uei) - built by
# scripts/build_capability_profile.py: identity + designations + sub/prime activity +
# nested evidence-tiered recommended NAICS+PSC lanes. One point-lookup; subs + DSBS alike.
CAPABILITY_PROFILE_URI = os.environ.get(
    "CAPABILITY_PROFILE_LANCE_URI", "s3://data-sink/active/capability_profile/"
)
# Subawardee drill-down (the /entities/{uei}/subaward-profile route). The capability profile
# (sub_uei grain, BTREE sub_uei) carries the structured capability block; contract_subaward
# (the raw sub→prime fact) carries the prime-contract history the sub won work under. The
# profile covers only the ~6,586 bridge subs; history covers all ~25,449 — so the route serves
# either (404 only when BOTH are empty).
SUBAWARDEE_CAPABILITY_PROFILES_URI = os.environ.get(
    "GOVCON_SUB_CAPABILITY_PROFILES_LANCE_URI",
    "s3://data-sink/active/govcon_subawardee_profiles/"
)
CONTRACT_SUBAWARD_URI = os.environ.get(
    "CONTRACT_SUBAWARD_LANCE_URI", "s3://data-sink/active/usaspending_api_fresh/contract_subaward/"
)
# GTM people SoR (active/people_canonical, 1 row per canonical person, BTREE canonical_person_id +
# person_linkedin_url + company_id + normalized_domain). Carries the person's title (job title) +
# identity + the verbatim person_linkedin_url. Backs the /api/v1/people/by-linkedin point-lookup.
# source_platform now lives in the person_source_platforms sidecar, not on people.
PEOPLE_URI = os.environ.get(
    "PEOPLE_LANCE_URI", "s3://data-sink/active/people_canonical/"
)
# Provenance sidecar: (canonical_person_id × source_platform × legacy_person_id). BITMAP on
# source_platform, BTREE on canonical_person_id. The person-by-linkedin response joins this by
# canonical_person_id to surface every source_platform a person was observed under.
PERSON_SOURCE_PLATFORMS_URI = os.environ.get(
    "PERSON_SOURCE_PLATFORMS_LANCE_URI", "s3://data-sink/active/person_source_platforms/"
)

# ── Map serving tables (the portal map read surface) ─────────────────────────
# Denormalized, pre-geocoded read models (1 row per winner / per company), each
# carrying lat/lon + the indexed filter columns. The map EXECUTE endpoint filters
# these via a Lance scanner predicate (no DuckDB). Built by pipelines/serving/
# materialize_winners_map.py, materialize_company_map.py and materialize_awards_map.py.
WINNERS_MAP_URI = os.environ.get(
    "WINNERS_MAP_LANCE_URI", "s3://data-sink/active/usaspending_winners_map_serving/"
)
COMPANY_MAP_URI = os.environ.get(
    "COMPANY_MAP_LANCE_URI", "s3://data-sink/active/firmographics_company_map_serving/"
)
# Award-EVENT grain (1 row per positive-dollar award action, rolling ~90d) — the read
# model behind "won an award over $X in the last N days" (amount binds to the single
# action, never a lifetime/window rollup).
AWARDS_MAP_URI = os.environ.get(
    "AWARDS_MAP_LANCE_URI", "s3://data-sink/active/usaspending_awards_map_serving/"
)
# Active-award grain (1 row per active prime award), pre-geocoded — the FORWARD-looking read
# model behind "incumbents about to recompete" (contracts whose period of performance ends in the
# next N days). Carries pop_current_end as the query-driven expiry axis.
ACTIVE_MAP_URI = os.environ.get(
    "ACTIVE_MAP_LANCE_URI", "s3://data-sink/active/govcon_active_awards_map_serving/"
)
# Contract-AWARD grain (1 row per contract_award_unique_key = FPDS PIID+agency composite),
# pre-geocoded — the read model behind "a single contract with $X+ summed obligations". The
# transaction ledger is rolled to award grain (deduped, net-summed), so the $-threshold filters
# the SUMMED contract value, not one action (distinct from the awards EVENT grain). Prime-only.
CONTRACTS_MAP_URI = os.environ.get(
    "CONTRACTS_MAP_LANCE_URI", "s3://data-sink/active/usaspending_contracts_map_serving/"
)
MAP_DATASET_URIS = {"winners": WINNERS_MAP_URI, "company": COMPANY_MAP_URI,
                    "awards": AWARDS_MAP_URI, "active": ACTIVE_MAP_URI,
                    "contracts": CONTRACTS_MAP_URI}


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


def contract_check_strict() -> bool:
    """Hard-fail switch for the boot decoder schema/index contract check (R-09).
    UNLIKE ``auth_required`` this is gated ONLY on an explicit operator flag and is
    NOT auto-enabled by ``RAILWAY_ENVIRONMENT``: a contract violation may be a checker
    false-positive (e.g. a Lance metadata-format change), and auto-bricking every
    Railway deploy on one introspection edge case would re-create the exact EXECUTE
    outage R-09 prevents. Default (flag unset) is observe-only: log loud + /healthz 503,
    boot proceeds. Set ``CATALYST_CONTRACT_STRICT`` truthy to promote a violation to a
    fatal boot abort — only after the check has proven stable in observe mode."""
    return os.environ.get("CATALYST_CONTRACT_STRICT", "").strip().lower() in ("1", "true", "yes")


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
