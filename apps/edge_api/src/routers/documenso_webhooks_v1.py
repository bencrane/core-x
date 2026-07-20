"""Documenso webhook — RAW landing (system of record).

  POST /api/v1/documenso/webhook   X-Documenso-Secret — capture every Documenso event verbatim.

Documenso is repointed here from the legacy ``/api/v1/proposals/webhook`` (same shared
``DOCUMENSO_WEBHOOK_SECRET``). This route verifies the secret and append-inserts the RAW body into
``gc.documenso_webhook_events`` — EVERY event type, no filtering, no normalization, no
projection. The legacy proposals webhook is untouched (it simply stops receiving deliveries).

Projection of these raw events into engagement/signing state is a SEPARATE, revisable step decided
against the captured payloads — never inferred ahead of real data.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Query, Request

from .. import config
from ..db import get_db_connection
from ..documenso_projection import project_envelope_event
from ..documenso_webhooks import queries
from ..services import documenso_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/documenso", tags=["documenso-webhooks"])

# Documenso lifecycle events → business.agreements.status (the ~15-line projector ruled in
# the 2026-07-19 agreement-cycle plan §6). DOCUMENT_SIGNED fires per recipient; both it and
# the terminal DOCUMENT_COMPLETED map to 'signed' (the guard below never regresses terminals).
_AGREEMENT_EVENT_STATUS = {
    "DOCUMENT_SENT": "sent",
    "DOCUMENT_OPENED": "sent",
    "DOCUMENT_SIGNED": "signed",
    "DOCUMENT_COMPLETED": "signed",
    "DOCUMENT_CANCELLED": "void",
    "DOCUMENT_REJECTED": "void",
}


def _dig(obj: Any, *keys: str) -> Any:
    """First present, non-null key from a dict (defensive across payload-shape variants)."""
    if isinstance(obj, dict):
        for k in keys:
            if obj.get(k) is not None:
                return obj[k]
    return None


@router.post("/webhook")
async def documenso_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_documenso_secret: str | None = Header(default=None),
) -> dict[str, Any]:
    """Capture a Documenso webhook delivery RAW (system of record), then ACK 200. The ENVELOPE MIRROR
    projection runs AFTER the response is sent (FastAPI BackgroundTask) — the 200 never waits on the
    projector's live Documenso pull. Secret-gated; the raw insert is append-only."""
    if config.documenso_webhook_secret() is None:
        raise HTTPException(status_code=503, detail="webhook secret not configured")
    if not documenso_client.verify_webhook_secret(x_documenso_secret):
        raise HTTPException(status_code=401, detail="invalid webhook secret")

    try:
        body = await request.json()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="invalid JSON body") from exc
    # Always persist SOMETHING raw, even if the body isn't a JSON object.
    raw: Any = body if isinstance(body, dict) else {"_raw": body}

    # Best-effort lookup extracts ONLY — `payload` (the full body) is the system of record. Documenso
    # nests the entity under `payload`/`data`; the envelope handle is its `id` (prefixed envelope_…),
    # and `externalId` is what we stamp at originate (= opportunity_id).
    event = _dig(raw, "event")
    inner = _dig(raw, "payload", "data") or {}
    envelope_id = _dig(inner, "id", "documentId", "envelopeId") or _dig(raw, "id", "envelopeId")
    external_id = _dig(inner, "externalId") or _dig(raw, "externalId")

    async with get_db_connection() as conn:
        event_id = await queries.insert_event(
            conn,
            event=str(event) if event is not None else None,
            envelope_id=str(envelope_id) if envelope_id is not None else None,
            external_id=str(external_id) if external_id is not None else None,
            payload=raw,
        )
        # AGREEMENT STATUS PROJECTOR — one guarded statement advancing the agreement row this
        # document belongs to (matched by envelope OR numeric document id; unknown envelopes —
        # legacy traffic — match nothing). Terminal states never regress; timestamps stamp once.
        mapped = _AGREEMENT_EVENT_STATUS.get(str(event or "").upper())
        if mapped and envelope_id is not None:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE business.agreements
                           SET status = %(status)s,
                               sent_at   = CASE WHEN %(status)s = 'sent'   THEN COALESCE(sent_at,   now()) ELSE sent_at   END,
                               signed_at = CASE WHEN %(status)s = 'signed' THEN COALESCE(signed_at, now()) ELSE signed_at END,
                               voided_at = CASE WHEN %(status)s = 'void'   THEN COALESCE(voided_at, now()) ELSE voided_at END
                         WHERE (documenso_envelope_id = %(eid)s OR documenso_document_id::text = %(eid)s)
                           AND status NOT IN ('signed', 'void')
                        """,
                        {"status": mapped, "eid": str(envelope_id)},
                    )
                await conn.commit()
            except Exception:  # noqa: BLE001 — raw capture is the SoR; projection is best-effort
                logger.warning("agreement status projection failed for envelope %s", envelope_id)
    logger.info("documenso webhook captured: id=%s event=%s envelope=%s", event_id, event, envelope_id)

    # ENVELOPE MIRROR — schedule the async projector to run AFTER this 200 is sent (it pulls the FULL
    # live envelope and upserts gc.documenso_envelopes verbatim). Only for events that carry an
    # envelope: an event name present AND a numeric inner id. The 200 above does NOT wait on this.
    if event is not None and isinstance(inner, dict) and inner.get("id") is not None:
        background_tasks.add_task(project_envelope_event, str(event), raw)

    return {"ok": True, "id": event_id}


@router.get("/sign-state/{opportunity_id}/{document_id}")
async def read_sign_state(
    opportunity_id: str, document_id: str, signer: str = Query("client")
) -> dict[str, Any]:
    """PUBLIC prospect signing-state read — the POLL. Keyed by the ``(opportunity_id, document_id)``
    PAIR carried in the signing link (``/p/m/{opportunity_id}/{document_id}``).

    FULLY OFFLINE — ZERO Documenso calls. ``signed`` is derived at read time from the RAW webhook
    capture (``gc.documenso_webhook_events``) for the pair ``external_id = {opportunity_id} AND
    envelope_id = {document_id}``: true once the COUNTERPARTY (the prospect) has signed — a captured
    payload shows a SIGNED recipient whose email DOMAIN is not a provider domain — INDEPENDENT of
    whether the provider has countersigned (a terminal ``DOCUMENT_COMPLETED`` row also satisfies it).
    The prospect-facing ``MandateSignPage`` polls this while the embed is shown and advances to the
    signed-confirmation view when ``signed`` flips true.

    Security: the read requires the PAIR. ``opportunity_id`` is the opportunity's public 8-char handle
    (the access capability — 8 hex = 32 bits); ``document_id`` is Documenso's sequential/guessable
    numeric id, the unique lookup pin, valid only BEHIND a matching handle. A guessed numeric id with a
    wrong/missing handle → no matching rows → ``signed:false``.
    """
    async with get_db_connection() as conn:
        state = await queries.read_sign_state(
            conn, opportunity_id=opportunity_id, document_id=document_id, signer=signer
        )
    return {
        "opportunity_id": opportunity_id,
        "document_id": document_id,
        "signed": state["signed"],
        "latest_event": state["latest_event"],
        "status": state["status"],
    }


@router.get("/sign-token/{opportunity_id}/{document_id}")
async def read_sign_token(
    opportunity_id: str,
    document_id: str,
    signer: str = Query("client"),
) -> dict[str, Any]:
    """PUBLIC signing-TOKEN read — the ONE-TIME embed load. Keyed by the same
    ``(opportunity_id, document_id)`` pair as the poll.

    The signing token lives ONLY in Documenso, so this makes ONE live read at embed load:
    ``GET /api/v2/document/{document_id}`` (the numeric id resolves on the *document* endpoint; the
    prefixed ``envelope_…`` handle 400s there). It then PAIR-GATES — asserts the document's
    ``externalId == opportunity_id`` (the opportunity's public 8-char handle stamped at originate) —
    and only then returns a recipient signing token. A guessed numeric id whose ``externalId`` does
    not match the supplied handle → 404 (no token leaked). This is NOT in the poll loop — the poll is
    the offline ``/sign-state`` read above.

    ``signer`` selects WHICH recipient on a two-signer document:
      * ``client`` (default) — the prospect: the recipient bound to the opportunity's contact email.
      * ``originator`` — the other signer (you): the recipient whose email is NOT the contact email.
    A single-signer document has one token and ignores ``signer`` (falls through to ``signing_token``).
    """
    try:
        doc = await documenso_client.read_document(document_id)
    except documenso_client.DocumensoError as e:
        logger.info("sign-token: documenso read failed for document %s: %s", document_id, e)
        raise HTTPException(status_code=404, detail="document not found") from e

    # PAIR GATE — the document must belong to the opportunity in the link. Mismatch ⇒ 404, identical
    # to "not found" so a guessed numeric id reveals nothing about which opportunity it belongs to.
    if (doc.external_id or "") != opportunity_id:
        logger.info(
            "sign-token: pair mismatch — document %s externalId=%s != opportunity %s",
            document_id, doc.external_id, opportunity_id,
        )
        raise HTTPException(status_code=404, detail="document not found")

    # Pick the recipient's token. Single-token docs (or no email match) fall back to signing_token.
    token = doc.signing_token
    pairs = list(doc.recipient_tokens)
    if len(pairs) > 1:
        # The handle in the link is the agreement_handle on the /sign/{agreement_handle} flow and the
        # deal handle on older links — resolve the signatory through whichever world knows it.
        async with get_db_connection() as conn:
            contact = await queries.get_agreement_signatory_email(conn, opportunity_id)
            if not contact:
                contact = await queries.get_deal_signatory_email(conn, opportunity_id)
        contact_l = (contact or "").strip().lower()
        # CLIENT = the recipient bound to the signatory contact; ORIGINATOR = any other recipient.
        # BOTH selections require a resolved contact — with it unknown, picking "any other recipient"
        # for the originator would hand back an arbitrary slot (possibly the prospect's); fall back to
        # the document's own signing_token instead of guessing.
        client_tok = next((t for (e, t) in pairs if contact_l and e == contact_l), None)
        originator_tok = next((t for (e, t) in pairs if contact_l and e != contact_l), None)
        token = (originator_tok if signer == "originator" else client_tok) or token

    return {
        "signing_token": token,
        "status": doc.status,
        "documenso_host": config.documenso_api_url(),
    }
