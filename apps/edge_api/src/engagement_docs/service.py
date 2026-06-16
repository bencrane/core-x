"""Render orchestration — opportunity values + locked package → bound HTML → DocRaptor → R2 →
SIGNABLE Documenso DOCUMENT → ledger.

Invoked by the internal endpoint (which the Trigger.dev ``engagement-doc-render`` task calls). The
caller owns commit; a failure at any step is RECORDED (status='failed', keeping the rendered-PDF
pointer when we have one) and RETURNED (never raised), so the caller commits the failure row before
surfacing a non-2xx for run-level observability.

``status='rendered'`` means the FULL pipeline succeeded: PDF rendered + stored AND a signable
Documenso document created (recipients + anchor-placed SIGNATURE/DATE fields + distributed NONE).
"""
from __future__ import annotations

import logging
import os
from typing import Any

from . import documenso, packages, queries, render, store

logger = logging.getLogger(__name__)

# The only engagement-content document this pathway renders today (term-only AO mandate).
SLUG = "active_operators_term_only"

# Provider = the Rare Structure signatory, pre-rendered into the PDF's Provider execution block.
_PROVIDER_NAME = "Benjamin J. Crane"


def _provider_email() -> str:
    return os.environ.get("PROVIDER_SIGNER_EMAIL", "benjaminjcrane@gmail.com")


def _full_name(first: str | None, last: str | None) -> str:
    return " ".join(p for p in (first, last) if p and p.strip()).strip()


async def render_mandate(conn, *, opportunity_id: str, package_key: str) -> dict[str, Any]:
    """Bind + render the mandate, then create a signable Documenso document. Does NOT commit. Returns
    a result dict with ``status`` of ``rendered`` | ``failed`` | ``skipped_no_opportunity``."""
    pkg = packages.get(package_key)
    if pkg is None:
        return {"action": "failed", "status": "failed", "opportunity_id": opportunity_id,
                "error": f"unknown package: {package_key!r}"}

    opp = await queries.read_opportunity_for_doc(conn, opportunity_id)
    if opp is None:
        return {"action": "skipped_no_opportunity", "status": "skipped_no_opportunity",
                "opportunity_id": opportunity_id}

    company = (opp.get("company_name") or "").strip()
    signer = _full_name(opp.get("first_name"), opp.get("last_name"))
    participant_email = (opp.get("email") or "").strip()
    values = {
        "participant_name": company,
        "participant_signer_name": signer,
        "participant_title": (opp.get("title") or "").strip(),
        "term_fee": packages.format_usd(pkg.term_fee_cents),
        "duration_in_months": str(pkg.duration_months),
    }
    style = render.style_for(SLUG)

    async def _fail(error: str, *, pdf_key: str | None = None, pdf_url: str | None = None,
                    pdf_n: int | None = None) -> dict[str, Any]:
        await queries.upsert_mandate(
            conn, opportunity_id=opportunity_id, package_key=package_key,
            term_fee_cents=pkg.term_fee_cents, duration_months=pkg.duration_months, slug=SLUG,
            style=style, status="failed", pdf_r2_key=pdf_key, pdf_url=pdf_url, pdf_bytes=pdf_n,
            field_values=values, error=error[:500],
        )
        return {"action": "failed", "status": "failed", "opportunity_id": opportunity_id,
                "error": error[:500]}

    # 1) render + store the PDF
    try:
        bound = render.substitute(render.assemble_html(SLUG), values)
        pdf = await render.render_pdf(bound, name=f"{SLUG}-{opportunity_id}.pdf")
        key = f"{store.MANDATE_PREFIX}{opportunity_id}/{SLUG}.pdf"
        await store.put_pdf(key, pdf)
        url = await store.presigned_get_url(key)
    except Exception as exc:  # noqa: BLE001 — record + return; caller commits then surfaces
        logger.warning("engagement-doc render/store failed for opp %s: %s", opportunity_id, exc)
        return await _fail(str(exc))

    # 2) the Participant must have an email to be a Documenso signer
    if not participant_email:
        return await _fail(
            "opportunity contact has no email — cannot create a signable Documenso document",
            pdf_key=key, pdf_url=url, pdf_n=len(pdf),
        )

    # 3) create the SIGNABLE Documenso DOCUMENT from the rendered PDF (own client; type=DOCUMENT)
    title = f"Active Operators — Strategic Origination Agreement — {company or signer or 'Engagement'}"
    try:
        doc = await documenso.create_signable_document(
            pdf,
            title=title,
            participant_name=(signer or company or "Participant"),
            participant_email=participant_email,
            provider_name=_PROVIDER_NAME,
            provider_email=_provider_email(),
            external_id=opportunity_id,
        )
    except Exception as exc:  # noqa: BLE001 — the PDF is rendered; record the Documenso failure
        logger.warning("documenso document create failed for opp %s: %s", opportunity_id, exc)
        return await _fail(f"documenso: {exc}", pdf_key=key, pdf_url=url, pdf_n=len(pdf))

    # 4) success — PDF rendered + stored AND signable Documenso document created
    mandate = await queries.upsert_mandate(
        conn, opportunity_id=opportunity_id, package_key=package_key,
        term_fee_cents=pkg.term_fee_cents, duration_months=pkg.duration_months, slug=SLUG,
        style=style, status="rendered", pdf_r2_key=key, pdf_url=url, pdf_bytes=len(pdf),
        field_values=values, documenso_envelope_id=doc.envelope_id,
        documenso_document_id=doc.document_id, participant_signing_token=doc.participant_token,
        provider_signing_token=doc.provider_token,
    )
    logger.info(
        "engagement-doc rendered + documenso doc for opp %s -> %s / %s",
        opportunity_id, mandate["id"], doc.envelope_id,
    )
    return {
        "action": "rendered", "status": "rendered", "opportunity_id": opportunity_id,
        "mandate_id": mandate["id"], "pdf_url": url, "pdf_bytes": len(pdf),
        "documenso_envelope_id": doc.envelope_id, "documenso_document_id": doc.document_id,
        "participant_token": doc.participant_token, "provider_token": doc.provider_token,
        "field_values": values,
    }
