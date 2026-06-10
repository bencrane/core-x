"""Proposals — the reusable engagement-agreement + e-signature surface.

Three sub-surfaces, three trust boundaries:
  POST /api/v1/proposals                  service-token  — operator/BFF mints a proposal
  POST /api/v1/proposals/{ref}/provision  service-token  — (re)render PDF + create Documenso envelope
  GET  /api/v1/proposals                  service-token  — operator list
  GET  /api/v1/proposals/{ref}            PUBLIC (ref)   — the consumer React page's data source
  GET  /api/v1/proposals/{ref}/document   PUBLIC (ref)   — streams the sealed PDF once completed
  POST /api/v1/proposals/webhook          X-Documenso-Secret — Documenso advances status (authoritative)

Provisioning (render PDF → create envelope) is best-effort at create time and separately
re-runnable, so the row + PDF path are exercisable even before the Documenso Platform plan is
live; only the envelope step depends on it.
"""
from __future__ import annotations

import datetime as _dt
import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response

from .. import config
from ..db import get_db_connection
from ..payments import queries as pay_queries
from ..proposals import queries, template_queries
from ..proposals.agreement_template import render_agreement_html
from ..proposals.models import (
    DEFAULT_BILLING_CADENCE,
    DEFAULT_DURATION_MONTHS,
    SUCCESS_FEE_TIERS,
    Proposal,
    ProposalCreate,
    ProposalPublic,
    ProposalSummary,
    format_usd,
    total_cents,
)
from ..proposals.template_render import (
    proposal_token_values,
    render_template_html,
    substitute_tokens,
)
from ..service_token import require_service_token
from ..services import documenso_client, docraptor_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/proposals", tags=["proposals"])


def _title(p: Proposal) -> str:
    return f"Strategic Origination Mandate — {p.client_name}"


async def _agreement_html(conn, p: Proposal) -> str:
    """The agreement body for this proposal.

    If a PUBLISHED template is registered under ``p.template_id`` (authored via the Settings
    surface), render its markdown with the proposal's merge values. Otherwise — and on ANY registry
    error, including the table not yet existing — fall back to the built-in Strategic Origination
    Mandate (the original, unchanged default). The live signing path must never break because of
    the template registry."""
    tpl = None
    try:
        tpl = await template_queries.get_published_by_slug(conn, p.template_id)
    except Exception as exc:  # missing table / lookup failure → built-in default
        logger.warning("template registry lookup failed for %r (%s); using built-in", p.template_id, exc)
        try:
            await conn.rollback()
        except Exception:
            pass
    if tpl is not None:
        html = render_template_html(tpl["markdown"], apply_brand=tpl["apply_brand"])
        return substitute_tokens(html, proposal_token_values(p))
    return render_agreement_html(p)


async def _provision(conn, p: Proposal) -> tuple[bool, str | None]:
    """Render the legal PDF (DocRaptor, live) and create the Documenso envelope; bind it to the
    row (draft → sent). Returns (ok, error_message). Non-raising so create can be best-effort."""
    try:
        pdf = await docraptor_client.render_pdf(await _agreement_html(conn, p), name=p.ref)
        env = await documenso_client.create_signing_envelope(
            pdf, title=_title(p), signer_name=p.client_signer_name, signer_email=p.client_email,
            external_id=p.ref,
        )
        await queries.attach_envelope(conn, p.ref, env.envelope_id, env.client_token)
        return True, None
    except (docraptor_client.DocRaptorError, documenso_client.DocumensoError, httpx.HTTPError) as exc:
        # Includes transport failures (timeout/connect) — the common case. Non-raising so the
        # committed draft row survives and is re-provisionable, never stranding the caller.
        logger.warning("proposal %s provisioning deferred: %s", p.ref, exc)
        return False, str(exc)


@router.post("", dependencies=[Depends(require_service_token)])
async def create_proposal(body: ProposalCreate) -> dict[str, Any]:
    """Mint a proposal, persist it, then best-effort provision the signable envelope."""
    ref = queries.new_ref()
    effective_date = body.effective_date or _dt.date.today()
    rs_name = body.rs_signer_name or config.rs_signer_name()
    template_id = body.template_id or "strategic_origination_mandate"
    monthly_fee_cents = body.monthly_fee_cents
    duration_months = body.duration_months
    billing_cadence = body.billing_cadence
    success_fee_schedule = body.success_fee_schedule
    async with get_db_connection() as conn:
        # Pricing is dynamic-with-defaults: each unset field INHERITS the published template's
        # default (edge_api owns template→pricing so the BFF stays dumb). Best-effort: a registry
        # error just leaves the body values and the final fallbacks below.
        tpl = None
        try:
            tpl = await template_queries.get_published_by_slug(conn, template_id)
        except Exception as exc:
            logger.warning("template pricing lookup failed for %r (%s); using passed values", template_id, exc)
            try:
                await conn.rollback()
            except Exception:
                pass
        if tpl:
            if not monthly_fee_cents and tpl.get("monthly_fee_cents"):
                monthly_fee_cents = int(tpl["monthly_fee_cents"])
            duration_months = duration_months or tpl.get("duration_months")
            billing_cadence = billing_cadence or tpl.get("billing_cadence")
            success_fee_schedule = success_fee_schedule or tpl.get("success_fee_schedule")
        # Final fallbacks — the proposal is never left empty.
        duration_months = duration_months or DEFAULT_DURATION_MONTHS
        billing_cadence = billing_cadence or DEFAULT_BILLING_CADENCE
        success_fee_schedule = success_fee_schedule or list(SUCCESS_FEE_TIERS)
        if not monthly_fee_cents:
            raise HTTPException(status_code=422, detail="monthly_fee_cents required (no template default)")
        # total = monthly × duration; persisted into the legacy quarterly_total_cents for back-compat.
        total = total_cents(monthly_fee_cents, duration_months)
        field_values = {
            "clientName": body.client_name,
            "clientSignerName": body.client_signer_name,
            "clientTitle": body.client_title,
            "clientEmail": body.client_email,
            "effectiveDate": effective_date.isoformat(),
            "monthlyFee": format_usd(monthly_fee_cents),
            "duration": str(duration_months),
            "total": format_usd(total),
            "quarterlyTotal": format_usd(total),
            "rsName": rs_name,
        }
        p = await queries.insert_proposal(
            conn, ref=ref, body=body, template_id=template_id, effective_date=effective_date,
            monthly_fee_cents=monthly_fee_cents, duration_months=duration_months,
            billing_cadence=billing_cadence, success_fee_schedule=success_fee_schedule,
            quarterly_total_cents=total, rs_signer_name=rs_name, field_values=field_values,
        )
        ok, err = await _provision(conn, p)
        fresh = await queries.get_by_ref(conn, ref)
    return {
        "ref": ref,
        "path": f"/proposal/{ref}",
        "status": fresh.status if fresh else "draft",
        "provisioned": ok,
        "provision_error": err,
    }


@router.post("/{ref}/provision", dependencies=[Depends(require_service_token)])
async def provision_proposal(ref: str) -> dict[str, Any]:
    """(Re)render the PDF and create the Documenso envelope for an existing proposal."""
    async with get_db_connection() as conn:
        p = await queries.get_by_ref(conn, ref)
        if p is None:
            raise HTTPException(status_code=404, detail="proposal not found")
        if p.documenso_envelope_id is not None:
            # Already bound to an envelope — re-provisioning would orphan the live (possibly
            # signed) document. Refuse rather than silently rebind.
            raise HTTPException(status_code=409, detail="already provisioned")
        ok, err = await _provision(conn, p)
        fresh = await queries.get_by_ref(conn, ref)
    if not ok:
        raise HTTPException(status_code=502, detail=f"provisioning failed: {err}")
    return {"ref": ref, "status": fresh.status if fresh else p.status, "provisioned": True}


@router.get("", dependencies=[Depends(require_service_token)])
async def list_proposals(limit: int = 100) -> list[ProposalSummary]:
    async with get_db_connection() as conn:
        rows = await queries.list_recent(conn, min(max(limit, 1), 500))
    return [
        ProposalSummary(
            ref=p.ref, client_name=p.client_name, client_signer_name=p.client_signer_name,
            status=p.status, monthly_fee=format_usd(p.monthly_fee_cents),
            created_at=p.created_at.isoformat() if p.created_at else None,
        )
        for p in rows
    ]


@router.get("/{ref}")
async def get_proposal(ref: str) -> ProposalPublic:
    """PUBLIC — the ref is the bearer credential. The consumer React page's data source."""
    async with get_db_connection() as conn:
        p = await queries.get_by_ref(conn, ref)
        if p is None:
            raise HTTPException(status_code=404, detail="proposal not found")
        # Payment state is additive + isolated: any error (e.g. the payment migration not yet
        # applied) collapses to 'none' and NEVER breaks the public read / signing path.
        try:
            payment_status = await pay_queries.get_status_by_ref(conn, ref)
        except Exception as exc:  # noqa: BLE001
            logger.warning("payment status read failed for %s (%s); defaulting 'none'", ref, exc)
            try:
                await conn.rollback()
            except Exception:
                pass
            payment_status = "none"
        # Engagement-page blurb — read-time from the proposal's template (template-owned, org-keyed).
        # Isolated like payment: any miss (NULL column, unpublished/absent template, lookup error)
        # collapses to the built-in default in from_row and NEVER breaks the public read.
        exec_summary: str | None = None
        try:
            tpl = await template_queries.get_published_by_slug(conn, p.template_id)
            if tpl:
                exec_summary = tpl.get("exec_summary")
        except Exception as exc:  # noqa: BLE001
            logger.warning("exec_summary lookup failed for %s (%s); using default", ref, exc)
            try:
                await conn.rollback()
            except Exception:
                pass
    return ProposalPublic.from_row(p, payment_status=payment_status, exec_summary=exec_summary)


@router.get("/{ref}/document")
async def get_signed_document(ref: str) -> Response:
    """PUBLIC — stream the sealed PDF once the agreement is completed."""
    async with get_db_connection() as conn:
        p = await queries.get_by_ref(conn, ref)
    if p is None:
        raise HTTPException(status_code=404, detail="proposal not found")
    if p.status != "completed" or not p.documenso_envelope_id:
        raise HTTPException(status_code=409, detail="agreement not yet completed")
    pdf = await documenso_client.download_signed_pdf(p.documenso_envelope_id)
    return Response(
        content=pdf, media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{ref}.pdf"'},
    )


@router.post("/webhook")
async def documenso_webhook(
    request: Request, x_documenso_secret: str | None = Header(default=None),
) -> dict[str, Any]:
    """Documenso → status advance. Source of truth (never the client embed callback)."""
    if config.documenso_webhook_secret() is None:
        raise HTTPException(status_code=503, detail="webhook secret not configured")
    if not documenso_client.verify_webhook_secret(x_documenso_secret):
        raise HTTPException(status_code=401, detail="invalid webhook secret")

    body = await request.json()
    evt = documenso_client.normalize_event(body)
    if evt.status is None:
        return {"ok": True, "ignored": True, "event": evt.event}

    async with get_db_connection() as conn:
        # Resolve the proposal deterministically by externalId (the ref we stamped on the
        # envelope), falling back to the envelope id. Advance via the row's stored envelope id.
        proposal = None
        if evt.external_id:
            proposal = await queries.get_by_ref(conn, evt.external_id)
        if proposal is None and evt.envelope_id:
            proposal = await queries.get_by_envelope(conn, evt.envelope_id)
        if proposal is None or not proposal.documenso_envelope_id:
            return {"ok": True, "ignored": True, "event": evt.event, "reason": "no matching proposal"}
        signed_url = (
            f"/api/v1/proposals/{proposal.ref}/document" if evt.status == "completed" else None
        )
        applied = await queries.advance_status(
            conn,
            envelope_id=proposal.documenso_envelope_id,
            status=evt.status,
            signed_pdf_url=signed_url,
        )
    return {"ok": True, "applied": applied, "event": evt.event, "status": evt.status}
