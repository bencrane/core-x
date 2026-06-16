"""Fire the ``engagement-doc-render`` Trigger.dev task — the durable/observable orchestrator the
public generate-mandate route hands off to. Reuses the generic Trigger.dev management API client
(not proposal-pathway code). Best-effort: a trigger failure is logged + returns None."""
from __future__ import annotations

import logging

from ..services.trigger_dev_client import trigger_task

logger = logging.getLogger(__name__)

RENDER_TASK = "engagement-doc-render"


async def trigger_render(*, mandate_id: str) -> str | None:
    """Enqueue a render run for a specific deal; return the Trigger run id. The idempotency key is the
    deal id (unique per Stage), so double-clicks of the SAME deal collapse to one run while each new
    Stage is its own deal + its own run."""
    try:
        run = await trigger_task(
            RENDER_TASK,
            {"mandateId": mandate_id},
            idempotency_key=f"engagement-doc-render:{mandate_id}",
        )
    except Exception as exc:  # noqa: BLE001 — caller decides how to surface; never crash the request
        logger.warning("engagement-doc-render trigger failed for deal %s: %s", mandate_id, exc)
        return None
    run_id = run.get("id") if isinstance(run, dict) else None
    logger.info("engagement-doc-render triggered for deal %s -> %s", mandate_id, run_id)
    return run_id
