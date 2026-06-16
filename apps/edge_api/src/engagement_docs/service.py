"""Render orchestration — a DEAL (already INSERTed as 'pending' at Stage) → bound HTML → DocRaptor →
R2 → DRAFT Documenso DOCUMENT → update the deal.

Invoked by the internal endpoint (which the Trigger.dev ``engagement-doc-render`` task calls) with the
``mandate_id`` of the deal to render. The caller owns commit; a failure at any step is RECORDED on the
deal (status='failed', keeping the rendered-PDF pointer when we have one) and RETURNED (never raised),
so the caller commits before surfacing a non-2xx for run observability.

``status='rendered'`` means: PDF rendered + stored AND a DRAFT Documenso document created (recipients
+ anchor SIGNATURE/DATE fields placed, NOT yet distributed). The document's ``externalId`` is the
OPPORTUNITY id, so many deals/documents can hang off one opportunity.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from . import documenso, packages, queries, render, store

logger = logging.getLogger(__name__)

# Provider = the Rare Structure signatory, pre-rendered into the PDF's Provider execution block.
_PROVIDER_NAME = "Benjamin J. Crane"


def _provider_email() -> str:
    return os.environ.get("PROVIDER_SIGNER_EMAIL", "benjaminjcrane@gmail.com")


def _full_name(first: str | None, last: str | None) -> str:
    return " ".join(p for p in (first, last) if p and p.strip()).strip()


async def render_mandate(conn, *, mandate_id: str) -> dict[str, Any]:
    """Render the deal identified by ``mandate_id`` and attach a DRAFT Documenso document. Does NOT
    commit. Returns a result dict with ``status`` of ``rendered`` | ``failed``."""
    deal = await queries.get_by_id(conn, mandate_id)
    if deal is None:
        return {"action": "failed", "status": "failed", "mandate_id": mandate_id,
                "error": "mandate not found"}

    opportunity_id = deal["opportunity_id"]
    slug = deal["document_slug"]
    term_fee_cents = deal["term_fee_cents"]
    duration_months = deal["duration_months"]

    async def _fail(error: str, *, pdf_key: str | None = None, pdf_url: str | None = None,
                    pdf_n: int | None = None, fv: dict[str, Any] | None = None) -> dict[str, Any]:
        await queries.update_mandate(
            conn, mandate_id=mandate_id, status="failed", pdf_r2_key=pdf_key, pdf_url=pdf_url,
            pdf_bytes=pdf_n, field_values=fv, error=error[:500],
        )
        return {"action": "failed", "status": "failed", "mandate_id": mandate_id,
                "opportunity_id": opportunity_id, "error": error[:500]}

    opp = await queries.read_opportunity_for_doc(conn, opportunity_id)
    if opp is None:
        return await _fail("opportunity not found")

    company = (opp.get("company_name") or "").strip()
    signer = _full_name(opp.get("first_name"), opp.get("last_name"))
    participant_email = (opp.get("email") or "").strip()
    values = {
        "participant_name": company,
        "participant_signer_name": signer,
        "participant_title": (opp.get("title") or "").strip(),
        "term_fee": packages.format_usd(term_fee_cents),
        "duration_in_months": str(duration_months),
    }

    # 1) render + store the PDF (per-DEAL key so sibling deals never overwrite each other)
    try:
        bound = render.substitute(render.assemble_html(slug), values)
        pdf = await render.render_pdf(bound, name=f"{slug}-{mandate_id}.pdf")
        key = f"{store.MANDATE_PREFIX}{opportunity_id}/{mandate_id}.pdf"
        await store.put_pdf(key, pdf)
        url = await store.presigned_get_url(key)
    except Exception as exc:  # noqa: BLE001 — record + return; caller commits then surfaces
        logger.warning("engagement-doc render/store failed for deal %s: %s", mandate_id, exc)
        return await _fail(str(exc), fv=values)

    # 2) the Participant must have an email to be a Documenso recipient
    if not participant_email:
        return await _fail(
            "opportunity contact has no email — cannot create a Documenso document",
            pdf_key=key, pdf_url=url, pdf_n=len(pdf), fv=values,
        )

    # 3) create the DRAFT Documenso DOCUMENT (externalId = opportunity_id; NOT distributed)
    title = f"Active Operators — Strategic Origination Agreement — {company or signer or 'Engagement'}"
    try:
        doc = await documenso.create_draft_document(
            pdf,
            title=title,
            participant_name=(signer or company or "Participant"),
            participant_email=participant_email,
            provider_name=_PROVIDER_NAME,
            provider_email=_provider_email(),
            external_id=opportunity_id,
        )
    except Exception as exc:  # noqa: BLE001 — the PDF is rendered; record the Documenso failure
        logger.warning("documenso draft create failed for deal %s: %s", mandate_id, exc)
        return await _fail(f"documenso: {exc}", pdf_key=key, pdf_url=url, pdf_n=len(pdf), fv=values)

    # 4) success — PDF rendered + stored AND a DRAFT Documenso document created
    await queries.update_mandate(
        conn, mandate_id=mandate_id, status="rendered", pdf_r2_key=key, pdf_url=url,
        pdf_bytes=len(pdf), field_values=values, documenso_envelope_id=doc.envelope_id,
        documenso_document_id=doc.document_id,
    )
    logger.info("deal %s rendered + DRAFT documenso doc %s (opp %s)", mandate_id, doc.envelope_id, opportunity_id)
    return {
        "action": "rendered", "status": "rendered", "mandate_id": mandate_id,
        "opportunity_id": opportunity_id, "pdf_bytes": len(pdf),
        "documenso_envelope_id": doc.envelope_id, "documenso_document_id": doc.document_id,
        "field_values": values,
    }
