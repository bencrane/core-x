"""Render orchestration — opportunity values + locked package → bound HTML → DocRaptor → R2 → ledger.

Invoked by the internal endpoint (which the Trigger.dev ``engagement-doc-render`` task calls). The
caller owns commit; a render failure is RECORDED (status='failed') and RETURNED (never raised), so the
caller can commit the failure row before surfacing a non-2xx for run-level observability.
"""
from __future__ import annotations

import logging
from typing import Any

from . import packages, queries, render, store

logger = logging.getLogger(__name__)

# The only engagement-content document this pathway renders today (term-only AO mandate).
SLUG = "active_operators_term_only"


def _full_name(first: str | None, last: str | None) -> str:
    return " ".join(p for p in (first, last) if p and p.strip()).strip()


async def render_mandate(conn, *, opportunity_id: str, package_key: str) -> dict[str, Any]:
    """Bind + render the mandate for one opportunity. Does NOT commit. Returns a result dict with a
    ``status`` of ``rendered`` | ``failed`` | ``skipped_no_opportunity``."""
    pkg = packages.get(package_key)
    if pkg is None:
        return {"action": "failed", "status": "failed", "opportunity_id": opportunity_id,
                "error": f"unknown package: {package_key!r}"}

    opp = await queries.read_opportunity_for_doc(conn, opportunity_id)
    if opp is None:
        return {"action": "skipped_no_opportunity", "status": "skipped_no_opportunity",
                "opportunity_id": opportunity_id}

    values = {
        "participant_name": (opp.get("company_name") or "").strip(),
        "participant_signer_name": _full_name(opp.get("first_name"), opp.get("last_name")),
        "participant_title": (opp.get("title") or "").strip(),
        "term_fee": packages.format_usd(pkg.term_fee_cents),
        "duration_in_months": str(pkg.duration_months),
    }
    style = render.style_for(SLUG)

    try:
        bound = render.substitute(render.assemble_html(SLUG), values)
        pdf = await render.render_pdf(bound, name=f"{SLUG}-{opportunity_id}.pdf")
        key = f"{store.MANDATE_PREFIX}{opportunity_id}/{SLUG}.pdf"
        await store.put_pdf(key, pdf)
        url = await store.presigned_get_url(key)
    except Exception as exc:  # noqa: BLE001 — record the failure, let the caller commit + surface it
        logger.warning("engagement-doc render failed for opp %s: %s", opportunity_id, exc)
        await queries.upsert_mandate(
            conn, opportunity_id=opportunity_id, package_key=package_key,
            term_fee_cents=pkg.term_fee_cents, duration_months=pkg.duration_months, slug=SLUG,
            style=style, status="failed", field_values=values, error=str(exc)[:500],
        )
        return {"action": "failed", "status": "failed", "opportunity_id": opportunity_id,
                "error": str(exc)[:500]}

    mandate = await queries.upsert_mandate(
        conn, opportunity_id=opportunity_id, package_key=package_key,
        term_fee_cents=pkg.term_fee_cents, duration_months=pkg.duration_months, slug=SLUG,
        style=style, status="rendered", pdf_r2_key=key, pdf_url=url, pdf_bytes=len(pdf),
        field_values=values,
    )
    logger.info("engagement-doc rendered for opp %s -> %s (%d bytes)", opportunity_id, mandate["id"], len(pdf))
    return {
        "action": "rendered", "status": "rendered", "opportunity_id": opportunity_id,
        "mandate_id": mandate["id"], "pdf_url": url, "pdf_bytes": len(pdf), "field_values": values,
    }
