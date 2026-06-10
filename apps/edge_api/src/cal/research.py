"""cal.com booking → Parallel deep-research kickoff (Trigger.dev).

On a NEW booking we fire the Trigger.dev ``parallel-deep-research`` task for the
prospect's company — one topic report, cheapest processor. The Trigger run id is
stamped back onto ``corex.bookings.research_run_id`` so the dossier can read the
result later.

Idempotent on ``ical_uid``: cal double-delivers every event, but the trigger-run
idempotency key collapses duplicate deliveries to a SINGLE research run (one Parallel
spend). edge_api triggers the task via the Trigger.dev REST API (``trigger_dev_client``)
— no Modal webhook, no callback; the task itself dispatches to Modal downstream.
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


def build_objective(company_name: str | None, domain: str) -> str:
    """The topic research prompt for one prospect company. Deliberately refinable."""
    who = f"{company_name} ({domain})" if company_name else domain
    return (
        f"Research the company {who}. Produce a concise B2B intelligence brief covering: "
        "(1) what the company does — its core products/services and unique capabilities; "
        "(2) its value proposition and differentiation; "
        "(3) the industries and customer segments it targets; "
        "(4) the geographic regions it operates in or serves; "
        "(5) notable signals (size, traction, recent developments) relevant to a partnership. "
        "Cite sources."
    )


async def trigger_research(*, ical_uid: str, company_name: str | None, domain: str) -> str | None:
    """Fire ``parallel-deep-research`` for this company; return the Trigger run id.

    Best-effort: logs + returns None on any failure (the booking is already stored).
    The trigger-run idempotency key is derived from ``ical_uid`` so cal's duplicate
    deliveries (and retries) resolve to ONE run."""
    payload: dict[str, Any] = {
        "objective": build_objective(company_name, domain),
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
