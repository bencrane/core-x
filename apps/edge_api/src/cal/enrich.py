"""cal.com booking → per-booking company enrichment kickoff (Trigger.dev).

On a NEW booking we ALSO fire the Trigger.dev ``booking-enrich`` task for the prospect's
company — one Parallel Task run, cheapest processor, a few typed firmographic columns landed
to ``s3://data-sink/active/booking_enrichment/`` keyed by ``ical_uid``. The Trigger run id is
stamped onto ``corex.bookings.enrich_run_id`` so the booking joins its enrichment row.

This is the SIBLING of ``research.py`` (deep-research kickoff) — same control-plane shape, same
best-effort contract (never fails the webhook). It is a STANDALONE workflow: no audience, no
spec registry. The enrichment column set is fixed worker-side (pipelines/parallel/
booking_enrich.py ``_OUTPUT_SCHEMA``); edit it there to change what is pulled back.

Idempotent on ``ical_uid``: the Trigger-run idempotency key collapses cal's duplicate
deliveries to a SINGLE enrichment run (one Parallel spend).
"""
from __future__ import annotations

import logging
from typing import Any

from ..services.trigger_dev_client import trigger_task

logger = logging.getLogger(__name__)

ENRICH_TASK = "booking-enrich"
# Parallel TASK-API processor tier (lite|base|core). 'lite' is cheapest — firmographics are
# shallow and bookings are low-volume. Bump worker-side / here if a richer schema needs depth.
ENRICH_PROCESSOR = "lite"


async def trigger_enrich(*, ical_uid: str, company_name: str | None, domain: str) -> str | None:
    """Fire ``booking-enrich`` for this company; return the Trigger run id.

    Best-effort: logs + returns None on any failure (the booking is already stored). The
    trigger-run idempotency key is derived from ``ical_uid`` so cal's duplicate deliveries (and
    retries) resolve to ONE run."""
    payload: dict[str, Any] = {
        "icalUid": ical_uid,
        "domain": domain,
        "companyName": company_name,
        "processor": ENRICH_PROCESSOR,
    }
    try:
        run = await trigger_task(ENRICH_TASK, payload, idempotency_key=f"booking-enrich:{ical_uid}")
    except Exception as exc:  # noqa: BLE001 — best-effort kickoff; never fail the webhook
        logger.warning("booking-enrich trigger failed for %s: %s", ical_uid, exc)
        return None
    run_id = run.get("id") if isinstance(run, dict) else None
    logger.info("booking-enrich triggered for %s -> %s", ical_uid, run_id)
    return run_id
