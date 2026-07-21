"""cal.com booking → Parallel deep-research kickoff (Trigger.dev).

On a NEW booking we fire the Trigger.dev ``parallel-deep-research`` task for the
prospect's company — one topic report, cheapest processor. The Trigger run id (and the
prompt version used) is stamped back onto ``corex.bookings`` so the dossier can read the
result and we can attribute it to a prompt.

The prompt is NOT hardcoded: the ACTIVE version lives in ``corex.research_prompts``
(rendered with the prospect's ``company_name`` / ``domain``). ``_DEFAULT_TEMPLATE`` is
only a fallback so the kickoff never breaks if the registry row is missing — same
resilience as the proposal-template registry.

Idempotent on the COMPANY (``domain``, operator ruling 2026-07-19): repeat bookings for
the same firm reuse the existing research run instead of re-running and overwriting —
cal's duplicate deliveries collapse the same way (one Parallel spend per company). edge_api triggers via the Trigger.dev REST API (``trigger_dev_client``) — no
Modal webhook, no callback; the task itself dispatches to Modal downstream.
"""
from __future__ import annotations

import logging
from typing import Any

from ..services.trigger_dev_client import trigger_task

logger = logging.getLogger(__name__)

RESEARCH_TASK = "parallel-deep-research"
# Parallel processor tier for this deep-research run. 'pro' is the deep-research
# floor (operator ruling 2026-07-19: capital-provider institutional profiles run on
# pro; the SIBLING booking-enrich Task-API run stays 'lite' — do not conflate).
RESEARCH_PROCESSOR = "pro"

# The active prompt is the `company-brief` slug in corex.research_prompts.
RESEARCH_PROMPT_SLUG = "company-brief"

# SAM-ENTITY lane (operator ruling 2026-07-20): a booking whose domain classifies as a
# real sam.gov/USAspending entity (see cal/classify.py) uses this slug instead — the
# govcon dossier prompt run as deep research. Same task, same processor; only the prompt
# and the idempotency key differ (the key is lane-scoped so a company already researched
# under the WRONG lane still gets a fresh sam-entity run).
SAM_RESEARCH_PROMPT_SLUG = "sam-entity-brief"

# Fallback ONLY — used if no active row is in corex.research_prompts. The registry is the
# source of truth; this keeps the kickoff alive if the table/row is ever missing.
_DEFAULT_TEMPLATE = (
    "Research the company {company_name} ({domain}). Produce a concise B2B intelligence brief "
    "covering its products/services and unique capabilities, value proposition, target industries "
    "and customer segments, geographic regions, and notable signals relevant to a partnership. "
    "Cite sources."
)


def render(template: str, company_name: str | None, domain: str) -> str:
    """Fill the ``{company_name}`` / ``{domain}`` placeholders. Uses str.replace (NOT str.format)
    so arbitrary braces elsewhere in the prompt text can never break rendering."""
    return template.replace("{company_name}", company_name or domain).replace("{domain}", domain).strip()


async def trigger_research(
    *, ical_uid: str, company_name: str | None, domain: str, template: str | None = None,
    lane: str | None = None,
) -> str | None:
    """Fire ``parallel-deep-research`` for this company; return the Trigger run id.

    ``template`` is the active registry prompt; None → the built-in fallback. Best-effort:
    logs + returns None on any failure (the booking is already stored). The trigger-run
    idempotency key is derived from ``domain`` so repeat bookings for the same company (and
    cal's duplicate deliveries) resolve to ONE run. ``lane`` (e.g. ``"sam"``) scopes the
    key so a lane change re-researches a company the old lane already spent on."""
    idem = f"research:{lane}:{domain}" if lane else f"research:{domain}"
    payload: dict[str, Any] = {
        "objective": render(template or _DEFAULT_TEMPLATE, company_name, domain),
        "grain": "topic",
        "processor": RESEARCH_PROCESSOR,
        "outputType": "text",
        # Per-booking WAITPOINT key (a layer distinct from the run-enqueue idempotency_key below;
        # both are per-booking by ical_uid). Without it the task falls back to the shared constant
        # `research:topic:full` and every booking resolves on the first run's cached token. Makes
        # edge_api correct independent of the Trigger task deploy order; redundant once the task's
        # run-unique fallback ships, but harmless (same booking → same key both layers).
        "idempotencyKey": idem,
        # Carried for traceability in the Trigger run view (the task reads `objective`).
        "domain": domain,
        "company_name": company_name,
    }
    try:
        run = await trigger_task(RESEARCH_TASK, payload, idempotency_key=idem)
    except Exception as exc:  # noqa: BLE001 — best-effort kickoff; never fail the webhook
        logger.warning("parallel-deep-research trigger failed for %s: %s", ical_uid, exc)
        return None
    run_id = run.get("id") if isinstance(run, dict) else None
    logger.info("parallel-deep-research triggered for %s -> %s", ical_uid, run_id)
    return run_id
