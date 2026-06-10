"""cal.com booking → Parallel deep-research kickoff (Trigger.dev).

On a NEW booking we fire the Trigger.dev ``parallel-deep-research`` task for the
prospect's company — one topic report, cheapest processor. The Trigger run id (and the
prompt version used) is stamped back onto ``corex.bookings`` so the dossier can read the
result and we can attribute it to a prompt.

The prompt is NOT hardcoded: the ACTIVE version lives in ``corex.research_prompts``
(rendered with the prospect's ``company_name`` / ``domain``). ``_DEFAULT_TEMPLATE`` is
only a fallback so the kickoff never breaks if the registry row is missing — same
resilience as the proposal-template registry.

Idempotent on ``ical_uid``: cal double-delivers every event, but the trigger-run
idempotency key collapses duplicate deliveries to a SINGLE research run (one Parallel
spend). edge_api triggers via the Trigger.dev REST API (``trigger_dev_client``) — no
Modal webhook, no callback; the task itself dispatches to Modal downstream.
"""
from __future__ import annotations

import logging
from typing import Any

from ..services.trigger_dev_client import trigger_task

logger = logging.getLogger(__name__)

RESEARCH_TASK = "parallel-deep-research"
# Parallel TASK-API processor tier (lite|base|core|pro) — NOT the deep-research
# "Pro/Ultra" UI tiers. 'lite' is the cheapest; bump later if the brief needs depth.
RESEARCH_PROCESSOR = "lite"

# The active prompt is the `company-brief` slug in corex.research_prompts.
RESEARCH_PROMPT_SLUG = "company-brief"

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
    *, ical_uid: str, company_name: str | None, domain: str, template: str | None = None
) -> str | None:
    """Fire ``parallel-deep-research`` for this company; return the Trigger run id.

    ``template`` is the active registry prompt; None → the built-in fallback. Best-effort:
    logs + returns None on any failure (the booking is already stored). The trigger-run
    idempotency key is derived from ``ical_uid`` so cal's duplicate deliveries (and retries)
    resolve to ONE run."""
    payload: dict[str, Any] = {
        "objective": render(template or _DEFAULT_TEMPLATE, company_name, domain),
        "grain": "topic",
        "processor": RESEARCH_PROCESSOR,
        "outputType": "text",
        # Carried for traceability in the Trigger run view (the task reads `objective`).
        "domain": domain,
        "company_name": company_name,
    }
    try:
        run = await trigger_task(RESEARCH_TASK, payload, idempotency_key=f"research:{ical_uid}")
    except Exception as exc:  # noqa: BLE001 — best-effort kickoff; never fail the webhook
        logger.warning("parallel-deep-research trigger failed for %s: %s", ical_uid, exc)
        return None
    run_id = run.get("id") if isinstance(run, dict) else None
    logger.info("parallel-deep-research triggered for %s -> %s", ical_uid, run_id)
    return run_id
