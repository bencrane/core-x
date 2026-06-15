"""cal.com booking → CRM opportunity materialization kickoff (Trigger.dev).

On a NEW booking we fire the Trigger.dev ``opportunity-materialize`` task, which calls
back into edge_api (``/internal/opportunities/materialize``) to project the booking into
``business.accounts`` + ``business.contacts`` + ``business.opportunities``.

SIBLING of ``research.py`` / ``enrich.py`` — same control-plane shape, same best-effort
contract (never fails the webhook). Routed through Trigger.dev (not done inline) for
run-level observability + retries, and because the DocRaptor render layers onto this
same task in phase 2.

Idempotent on ``ical_uid``: the Trigger-run idempotency key collapses cal's duplicate
deliveries to a SINGLE run, and the materialization itself is idempotent regardless.
"""
from __future__ import annotations

import logging

from ..services.trigger_dev_client import trigger_task

logger = logging.getLogger(__name__)

MATERIALIZE_TASK = "opportunity-materialize"


async def trigger_materialize(*, ical_uid: str) -> str | None:
    """Fire ``opportunity-materialize`` for this booking; return the Trigger run id.

    Best-effort: logs + returns None on any failure (the booking is already stored). The
    trigger-run idempotency key is derived from ``ical_uid`` so cal's duplicate deliveries
    (and retries) resolve to ONE run."""
    try:
        run = await trigger_task(
            MATERIALIZE_TASK,
            {"icalUid": ical_uid},
            idempotency_key=f"opportunity-materialize:{ical_uid}",
        )
    except Exception as exc:  # noqa: BLE001 — best-effort kickoff; never fail the webhook
        logger.warning("opportunity-materialize trigger failed for %s: %s", ical_uid, exc)
        return None
    run_id = run.get("id") if isinstance(run, dict) else None
    logger.info("opportunity-materialize triggered for %s -> %s", ical_uid, run_id)
    return run_id
