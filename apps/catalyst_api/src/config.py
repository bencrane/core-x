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
    # repointed: reconciled BULK∪FRESH contract-subaward canonical (was usaspending_api_fresh/contract_subaward).
    # NOTE: if CONTRACT_SUBAWARD_LANCE_URI is set in Doppler/Railway, update it there too or the override wins.
    "CONTRACT_SUBAWARD_LANCE_URI", "s3://data-sink/active/usaspending_subaward_canonical/"
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


# ── Market query engine substrate (the spine-derived L2 datasets) ─────────────
# The deterministic entity-grain query surface (apps/catalyst_api/src/market_registry.py +
# market_store.py). All three are canonical spine-derived L2 Lance datasets — grain and
# universe semantics live in the registry field descriptions (the product surface).
# Schemas + indices probed live 2026-07-05.
#
# gtm_entity_behavior_rollup — 1 row/uei (261,316), BTREE uei. Contracts-only money
# rollups (IDV vehicle parents excluded), windows bind to action_date.
GTM_ENTITY_BEHAVIOR_ROLLUP_URI = os.environ.get(
    "GTM_ENTITY_BEHAVIOR_ROLLUP_LANCE_URI", "s3://data-sink/active/gtm_entity_behavior_rollup/"
)
# gtm_entity_code_lanes — 1 row/(uei, side, code_type, code) (1,670,905). BTREE uei +
# code; BITMAP side + code_type. The per-lane obligation windows behind lane predicates.
GTM_ENTITY_CODE_LANES_URI = os.environ.get(
    "GTM_ENTITY_CODE_LANES_LANCE_URI", "s3://data-sink/active/gtm_entity_code_lanes/"
)
# gtm_sam_entities — 1 row/uei (2,025,707). BTREE uei/normalized_domain/primary_naics;
# BITMAP in_sam/sam_is_active/in_dsbs/is_subawardee/is_prime_recipient/physical_state.
GTM_SAM_ENTITIES_URI = os.environ.get(
    "GTM_SAM_ENTITIES_LANCE_URI", "s3://data-sink/active/gtm_sam_entities/"
)
# gtm_entity_geo — 1 row/uei HQ geo sidecar (~1.45M; BTREE uei, BITMAP geo_precision),
# built by scripts/build_gtm_entity_geo.py: best-available coordinates per entity
# ('address' rooftop via geocode_xwalk, else 'county' dominant-ZCTA centroid; unmatched
# entities have NO row). LEFT-joined at market hydration for real map geometry.
GTM_ENTITY_GEO_URI = os.environ.get(
    "GTM_ENTITY_GEO_LANCE_URI", "s3://data-sink/active/gtm_entity_geo/"
)

# ── Code reference dimensions (the /market/codes typeahead) ──────────────────
# naics_reference — 1 row per NAICS code (2-6 digit, ~2,125): naics_code + naics_title.
NAICS_REFERENCE_URI = os.environ.get(
    "NAICS_REFERENCE_LANCE_URI", "s3://data-sink/active/naics_reference/"
)
# psc_reference — 1 row per (psc_code, effective period) (~6,108): psc_code + psc_name;
# is_active = current meaning (retired rows carry end_date).
PSC_REFERENCE_URI = os.environ.get(
    "PSC_REFERENCE_LANCE_URI", "s3://data-sink/active/psc_reference/"
)
# Capability-inference layer (scripts/build_gtm_capability_inference.py, all
# pre-weighting raw evidence). Matrix: (subbed_under_code -> primed_in_code) same-type
# cooccurrence over both-sider firms. Projections: per-entity inferred codes, BTREE
# uei + code, semi-joined by the entity-grain executor like the demonstrated lanes.
GTM_COOCCURRENCE_MATRIX_URI = os.environ.get(
    "GTM_SUBBED_UNDER_TO_PRIMED_IN_COOCCURRENCE_LANCE_URI",
    "s3://data-sink/active/gtm_subbed_under_to_primed_in_cooccurrence/"
)
GTM_INFERRED_PRIMEABLE_URI = os.environ.get(
    "GTM_ENTITY_INFERRED_PRIMEABLE_CODES_LANCE_URI",
    "s3://data-sink/active/gtm_entity_inferred_primeable_codes/"
)
GTM_INFERRED_SUBBABLE_URI = os.environ.get(
    "GTM_ENTITY_INFERRED_SUBBABLE_CODES_LANCE_URI",
    "s3://data-sink/active/gtm_entity_inferred_subbable_codes/"
)
# FPDS prime-award STATE table (82.87M rows, 1 row/contract_award_unique_key, topology-
# aware) — the prime_awards market grain. BTREE recipient_uei/award_id_piid/…; BITMAP
# award_topology/awarding_agency_code/…. NO place-of-performance columns (probed
# 2026-07-05) — map geometry comes from the recipient's gtm_entity_geo row.
FPDS_PRIME_AWARD_STATE_URI = os.environ.get(
    "FPDS_PRIME_AWARD_STATE_LANCE_URI", "s3://data-sink/active/usaspending_fpds_prime_award_state/"
)
# Canonical FPDS transaction spine (107.96M rows, 1 row/contract_transaction_unique_key)
# — the transactions market grain. BTREE action_date/federal_action_obligation/
# recipient_uei/naics/psc; BITMAP subcontracting_plan/awarding_agency_code.
FPDS_CANONICAL_TXN_URI = os.environ.get(
    "FPDS_CANONICAL_TXN_LANCE_URI", "s3://data-sink/active/usaspending_fpds_canonical_txn/"
)
# usaspending_award_canonical — the canonical prime-award fact (30.7M rows). The agency
# typeahead streams a DISTINCT over (awarding_agency_code, awarding_agency_name) from it
# (~136 pairs; no dedicated agency reference dimension exists yet) — lazy, once/process.
USASPENDING_AWARD_CANONICAL_URI = os.environ.get(
    "USASPENDING_AWARD_CANONICAL_LANCE_URI", "s3://data-sink/active/usaspending_award_canonical/"
)

# ── Subout-opportunities recipe substrate (subout_store.py) ───────────────────
# gtm_prime_subout_by_recipient_code — the two-sided sub-out history cube
# (1 row / (prime_awardee_uei, context_code_type, context_code, recipient_code_source,
# recipient_code_type, recipient_code); BTREE prime_awardee_uei / recipient_code /
# context_code). Built by scripts/build_gtm_prime_subout_by_recipient_code.py — raw
# history only (counts, sums, dates): read-time recipes own weights/thresholds.
GTM_PRIME_SUBOUT_BY_RECIPIENT_CODE_URI = os.environ.get(
    "GTM_PRIME_SUBOUT_BY_RECIPIENT_CODE_LANCE_URI",
    "s3://data-sink/active/gtm_prime_subout_by_recipient_code/",
)
# gtm_subaward_recipient_code_evidence — un-allocated sub-out destination evidence
# (1 row / (subaward_unique_key, code_source, code_type, code); BTREE prime_awardee_uei /
# subawardee_uei / code / subaward_unique_key). Serves the peers lookup: recipients
# sharing a code, via the BTREE on code.
GTM_SUBAWARD_RECIPIENT_CODE_EVIDENCE_URI = os.environ.get(
    "GTM_SUBAWARD_RECIPIENT_CODE_EVIDENCE_LANCE_URI",
    "s3://data-sink/active/gtm_subaward_recipient_code_evidence/",
)
# usaspending_award_pop_centroids — 1 row / generated_unique_award_id (BTREE), built by
# pipelines/serving/materialize_pop_centroids.py: place-of-performance latitude/longitude
# + geo_precision ('zip5' | coarser). LEFT-joined per opportunity for distance_mi.
USASPENDING_AWARD_POP_CENTROIDS_URI = os.environ.get(
    "USASPENDING_AWARD_POP_CENTROIDS_LANCE_URI",
    "s3://data-sink/active/usaspending_award_pop_centroids/",
)
# gtm_open_awards — 1 row per OPEN prime award (active PoP or open IDV ordering window,
# ~150-250K rows), pre-joined with the PoP centroid geo (latitude/longitude/geo_precision)
# and agency names. BTREE naics_code / product_or_service_code / recipient_uei. The
# subout-opportunities hot path loads this table INTO PROCESS MEMORY (lazy, TTL-refreshed)
# — per-request R2 scans of the 30.7M-row award spine are retired from the recipe.
GTM_OPEN_AWARDS_URI = os.environ.get(
    "GTM_OPEN_AWARDS_LANCE_URI", "s3://data-sink/active/gtm_open_awards/"
)
# gtm_primes_by_recipient_code — the PRE-AGGREGATED sub-out cube marginal (~1.7M rows,
# 1 row / (recipient_code_type, recipient_code, prime_awardee_uei); BTREE
# recipient_code + prime_awardee_uei): Σ subaward_edge_ct / Σ subaward_amt_total /
# MAX distinct_recipient_ct / MAX last_subaward_action_date. The subout cache loads
# THIS table at boot — never the 11.8M-cell cube (aggregating the cube in-process was
# 10-20x slower on Railway's throttled CPU than locally and starved the prewarm).
GTM_PRIMES_BY_RECIPIENT_CODE_URI = os.environ.get(
    "GTM_PRIMES_BY_RECIPIENT_CODE_LANCE_URI",
    "s3://data-sink/active/gtm_primes_by_recipient_code/",
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
