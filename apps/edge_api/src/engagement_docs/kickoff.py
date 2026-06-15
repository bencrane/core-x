"""Fire the ``engagement-doc-render`` Trigger.dev task — the durable/observable orchestrator the
public generate-mandate route hands off to. Reuses the generic Trigger.dev management API client
(not proposal-pathway code). Best-effort: a trigger failure is logged + returns None."""
from __future__ import annotations

import logging

from ..services.trigger_dev_client import trigger_task

logger = logging.getLogger(__name__)

RENDER_TASK = "engagement-doc-render"


async def trigger_render(*, opportunity_id: str, package_key: str) -> str | None:
    """Enqueue a render run; return the Trigger run id. Idempotency key collapses double-clicks of the
    same (opportunity, package) selection to one run; changing the package fires a fresh run."""
    try:
        run = await trigger_task(
            RENDER_TASK,
            {"opportunityId": opportunity_id, "packageKey": package_key},
            idempotency_key=f"engagement-doc-render:{opportunity_id}:{package_key}",
        )
    except Exception as exc:  # noqa: BLE001 — caller decides how to surface; never crash the request
        logger.warning("engagement-doc-render trigger failed for %s: %s", opportunity_id, exc)
        return None
    run_id = run.get("id") if isinstance(run, dict) else None
    logger.info("engagement-doc-render triggered for %s -> %s", opportunity_id, run_id)
    return run_id
