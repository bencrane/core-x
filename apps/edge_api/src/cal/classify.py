"""Booking-lane classification — is this domain a real sam.gov / USAspending entity?

One warm query against the query-sidecar at webhook time: the booking's normalized
domain is looked up in ``gtm_sam_entities`` (a domain can map to multiple UEIs — all are
aggregated, operator ruling 2026-07-20), and the entity's FY2023–FY2025 federal dollars
are summed across BOTH sides (prime obligations from ``gtm_entity_fy_won`` + subaward
dollars from ``subaward_canonical_slim_by_sub``). Combined > $1M ⇒ the SAM-ENTITY lane:
the booking gets the govcon deep-research prompt and SKIPS the sibling booking-enrich
task (that lane is the capital-provider setup). Anything else — no SAM match, thin
federal dollars, sidecar down, token unset — degrades to the default lane, exactly
today's behavior. Best-effort by construction: classification can never fail a webhook.
"""
from __future__ import annotations

import logging
import re

import httpx

from .. import config

logger = logging.getLogger(__name__)

SAM_FY_LO = 2023
SAM_FY_HI = 2025
SAM_DOLLAR_FLOOR = 1_000_000.0

_TIMEOUT = 10.0

# The sidecar SQL surface takes one raw statement (no bind params) — the domain is
# interpolated, so it must be a strict bare-domain shape. Anything else → default lane.
_DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,252}$")

_SQL = (
    "WITH u AS (SELECT uei FROM gtm_sam_entities WHERE normalized_domain = '{domain}'), "
    "p AS (SELECT coalesce(sum(won_obl), 0) w FROM gtm_entity_fy_won "
    "WHERE fy BETWEEN {fy_lo} AND {fy_hi} AND uei IN (SELECT uei FROM u)), "
    "s AS (SELECT coalesce(sum(subaward_amount_num), 0) w FROM subaward_canonical_slim_by_sub "
    "WHERE subaward_action_date_fiscal_year BETWEEN {fy_lo} AND {fy_hi} "
    "AND subawardee_uei IN (SELECT uei FROM u)) "
    "SELECT (SELECT count(*) FROM u) n_ueis, p.w prime_usd, s.w sub_usd FROM p, s"
)


async def is_sam_entity(domain: str | None) -> bool:
    """True iff ``domain`` resolves to ≥1 SAM entity whose combined FY23–25 prime + sub
    dollars exceed ``SAM_DOLLAR_FLOOR``. False on ANY doubt (no match, no token, bad
    domain shape, sidecar error) — the default lane is always the safe answer."""
    if not domain:
        return False
    domain = domain.strip().lower()
    if not _DOMAIN_RE.match(domain) or "'" in domain:
        logger.warning("sam classify: refusing non-bare domain %r", domain)
        return False
    token = config.query_sidecar_token()
    if not token:
        logger.warning("sam classify: QUERY_SIDECAR_TOKEN unset — default lane")
        return False
    sql = _SQL.format(domain=domain, fy_lo=SAM_FY_LO, fy_hi=SAM_FY_HI)
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(
                f"{config.query_sidecar_url()}/api/v1/sql",
                headers={"Authorization": f"Bearer {token}"},
                json={"sql": sql, "limit": 1},
            )
        if resp.status_code != 200:
            logger.warning("sam classify: sidecar %s: %s", resp.status_code, resp.text[:200])
            return False
        rows = resp.json().get("rows") or []
        if not rows:
            return False
        n_ueis, prime_usd, sub_usd = rows[0][0], float(rows[0][1] or 0), float(rows[0][2] or 0)
        hit = bool(n_ueis) and (prime_usd + sub_usd) > SAM_DOLLAR_FLOOR
        logger.info(
            "sam classify %s: ueis=%s prime=%.0f sub=%.0f -> %s",
            domain, n_ueis, prime_usd, sub_usd, "sam-entity" if hit else "default",
        )
        return hit
    except Exception as exc:  # noqa: BLE001 — classification never fails the webhook
        logger.warning("sam classify failed for %s: %s", domain, exc)
        return False
