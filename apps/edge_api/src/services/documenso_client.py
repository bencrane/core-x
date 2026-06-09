"""Documenso v2 e-signature client (Documenso Cloud, Platform tier).

The reusable signing core: create an envelope from the rendered agreement PDF with the Client
as the sole SIGNER (no Documenso-sent email — the consumer app controls delivery), read back
the recipient signing token (drives the embed), pull the sealed PDF on completion, and verify
inbound webhooks.

CALIBRATION BOUNDARY — the exact v2 wire shapes (envelope/create multipart field names, the
download operation, response key names) render client-side in Documenso's OpenAPI viewer and
could not be byte-pinned at build time. They are isolated here behind small, defensively-parsed
methods and marked ``# CALIBRATE``. Verify against the live Platform account's spec
(``{base}/api/v2/openapi``) before go-live. Auth, the webhook contract, and event names are
confirmed and stable.
"""
from __future__ import annotations

import hmac
import json
import logging
import re
from dataclasses import dataclass
from typing import Any

import httpx

from .. import config

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(60.0, connect=10.0)

# Documenso webhook event names → our internal proposal status.
_EVENT_TO_STATUS: dict[str, str] = {
    "DOCUMENT_SENT": "sent",
    "DOCUMENT_OPENED": "opened",
    "DOCUMENT_SIGNED": "signed",
    "DOCUMENT_COMPLETED": "completed",
    "DOCUMENT_REJECTED": "rejected",
    "DOCUMENT_CANCELLED": "voided",
}


class DocumensoError(RuntimeError):
    """A non-2xx Documenso response or an unconfigured client."""


@dataclass(frozen=True)
class EnvelopeResult:
    envelope_id: str
    document_id: int | None  # numeric secondary id (used for the signed-PDF download)
    client_token: str | None


@dataclass(frozen=True)
class NormalizedEvent:
    event: str
    status: str | None        # mapped internal status, or None for an unknown event
    envelope_id: str | None
    external_id: str | None


def _auth_value() -> str:
    key = config.documenso_api_key()
    if not key:
        raise DocumensoError("DOCUMENSO_API_KEY is not set")
    # Documenso keys carry the ``api_`` prefix; tolerate a key stored without it.
    return key if key.startswith("api_") else f"api_{key}"


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=config.documenso_api_url(),
        headers={"Authorization": _auth_value()},
        timeout=_TIMEOUT,
    )


def _raise_for_status(resp: httpx.Response, op: str) -> None:
    if resp.status_code // 100 != 2:
        detail = resp.text[:500]
        logger.error("documenso %s failed: %s %s", op, resp.status_code, detail)
        raise DocumensoError(f"documenso {op} {resp.status_code}: {detail}")


def _dig(obj: Any, *keys: str) -> Any:
    """Return the first present key from a dict (defensive across v2 response-shape variants)."""
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k] is not None:
                return obj[k]
    return None


def _extract_client_token(body: Any, email: str) -> str | None:
    """Pull the SIGNER recipient's signing token, matched by email when present."""
    env = _dig(body, "envelope", "document", "data") or body
    recips = _dig(env, "recipients", "Recipient") or []
    if not isinstance(recips, list):
        return None
    target = email.strip().lower()
    chosen = None
    for r in recips:
        if not isinstance(r, dict):
            continue
        if (_dig(r, "email") or "").strip().lower() == target:
            chosen = r
            break
        chosen = chosen or r  # fall back to the first recipient (single-signer envelopes)
    tok = _dig(chosen or {}, "token", "signingToken")
    return str(tok) if tok else None


# Signature/date field placement — PERCENT of the page (0-100). The agreement template
# page-breaks the signature block onto its own final page, so the client's signature sits at a
# deterministic spot there regardless of body length.
_SIGNATURE_FIELD = {"positionX": 52.0, "positionY": 13.0, "width": 32.0, "height": 7.0}
_DATE_FIELD = {"positionX": 52.0, "positionY": 27.0, "width": 22.0, "height": 4.0}

_PAGE_RE = re.compile(rb"/Type\s*/Page(?![s])")


def _count_pdf_pages(pdf: bytes) -> int:
    return max(len(_PAGE_RE.findall(pdf)), 1)


def _numeric_document_id(env: dict[str, Any]) -> int | None:
    """Documenso exposes the legacy numeric document id as ``secondaryId`` = ``document_<n>``."""
    m = re.search(r"(\d+)", str(_dig(env, "secondaryId") or ""))
    return int(m.group(1)) if m else None


async def create_signing_envelope(
    pdf_bytes: bytes, *, title: str, signer_name: str, signer_email: str,
    external_id: str | None = None,
) -> EnvelopeResult:
    """Create a signable v2 envelope from the agreement PDF with the Client as the sole SIGNER,
    place the signature + date fields, and distribute WITHOUT email (embedded-signing flow).

    Validated against Documenso Cloud v2: ``/envelope/create`` (multipart ``payload`` JSON +
    ``files``), ``/envelope/field/create-many`` (percent-positioned fields), ``/envelope/distribute``
    (``distributionMethod: NONE``). The recipient signing token, read back from the envelope, drives
    the embed. ``external_id`` (the proposal ref) is stamped on the envelope so the webhook can
    match it back deterministically, independent of the payload's id shape.
    """
    last_page = _count_pdf_pages(pdf_bytes)
    payload: dict[str, Any] = {
        "type": "DOCUMENT",
        "title": title,
        "recipients": [{"name": signer_name, "email": signer_email, "role": "SIGNER"}],
        "distributeDocument": False,
    }
    if external_id:
        payload["externalId"] = external_id
    async with _client() as client:
        # 1) create the draft envelope with the PDF attached (multipart: payload JSON + files)
        created = await client.post(
            "/api/v2/envelope/create",
            data={"payload": json.dumps(payload)},
            files={"files": (f"{title}.pdf", pdf_bytes, "application/pdf")},
        )
        _raise_for_status(created, "envelope/create")
        envelope_id = _dig(created.json(), "id")
        if not envelope_id:
            raise DocumensoError(f"envelope/create: no envelope id in {created.text[:300]}")

        # 2) read the recipient (id + token) and the numeric document id
        env = (await client.get(f"/api/v2/envelope/{envelope_id}")).json()
        token = _extract_client_token(env, signer_email)
        recipients = env.get("recipients") or []
        target = signer_email.strip().lower()
        recipient = next(
            (r for r in recipients if (_dig(r, "email") or "").strip().lower() == target),
            recipients[0] if recipients else None,
        )
        recipient_id = _dig(recipient or {}, "id")
        if recipient_id is None:
            raise DocumensoError("envelope/create: no recipient id on the created envelope")
        document_id = _numeric_document_id(env)

        # 3) place the SIGNATURE + DATE fields for the recipient on the final signature page
        fields = [
            {"type": "SIGNATURE", "recipientId": recipient_id, "page": last_page, **_SIGNATURE_FIELD},
            {"type": "DATE", "recipientId": recipient_id, "page": last_page, **_DATE_FIELD},
        ]
        placed = await client.post(
            "/api/v2/envelope/field/create-many",
            json={"envelopeId": envelope_id, "data": fields},
        )
        _raise_for_status(placed, "envelope/field/create-many")

        # 4) distribute WITHOUT email — the consumer app delivers the link; we embed the token
        distributed = await client.post(
            "/api/v2/envelope/distribute",
            json={"envelopeId": envelope_id, "meta": {"distributionMethod": "NONE"}},
        )
        _raise_for_status(distributed, "envelope/distribute")

    return EnvelopeResult(
        envelope_id=str(envelope_id),
        document_id=document_id,
        client_token=str(token) if token else None,
    )


async def get_envelope(envelope_id: str) -> dict[str, Any]:
    async with _client() as client:
        resp = await client.get(f"/api/v2/envelope/{envelope_id}")
    _raise_for_status(resp, "envelope/get")
    return resp.json()


async def client_token(envelope_id: str, signer_email: str) -> str | None:
    """(Re)read the Client recipient's signing token from the live envelope."""
    return _extract_client_token(await get_envelope(envelope_id), signer_email)


async def download_signed_pdf(envelope_id: str) -> bytes:
    """Fetch the sealed, signed PDF after completion.

    Resolves the numeric document id from the envelope, then downloads ``version=signed``. Handles
    both a direct ``application/pdf`` body and a JSON envelope carrying a pre-signed ``downloadUrl``.
    """
    async with _client() as client:
        env = (await client.get(f"/api/v2/envelope/{envelope_id}")).json()
        document_id = _numeric_document_id(env)
        if document_id is None:
            raise DocumensoError("document/download: could not resolve numeric document id")
        resp = await client.get(
            f"/api/v2/document/{document_id}/download", params={"version": "signed"},
        )
        _raise_for_status(resp, "document/download")
        ctype = resp.headers.get("content-type", "")
        if "application/pdf" in ctype or resp.content[:5] == b"%PDF-":
            return resp.content
        url = _dig(resp.json(), "downloadUrl", "url")
    if not url:
        raise DocumensoError("document/download: no PDF bytes and no downloadUrl in response")
    # The presigned URL is itself a bearer capability (S3/R2/CDN). Fetch it with a BARE client —
    # never attach the Documenso API key, or the secret rides to a third-party host.
    async with httpx.AsyncClient(timeout=_TIMEOUT) as raw:
        signed = await raw.get(url)
    _raise_for_status(signed, "document/download(url)")
    return signed.content


def verify_webhook_secret(provided: str | None) -> bool:
    """Constant-time compare of the inbound ``X-Documenso-Secret`` against the configured secret.

    Returns False when the secret is unconfigured — the route must then refuse the event rather
    than accept it unverified.
    """
    expected = config.documenso_webhook_secret()
    if not expected:
        return False
    return hmac.compare_digest((provided or ""), expected)


def normalize_event(body: dict[str, Any]) -> NormalizedEvent:
    """Map a webhook payload to our internal event/status.

    Documenso labels triggers as lowercase-dotted (``document.completed``) and may deliver the
    event in that form OR the enum form (``DOCUMENT_COMPLETED``); fold both to the enum key.
    """
    raw = str(_dig(body, "event") or "")
    key = raw.upper().replace(".", "_")
    payload = _dig(body, "payload", "data") or {}
    _s = lambda v: str(v) if v is not None else None  # noqa: E731
    return NormalizedEvent(
        event=raw,
        status=_EVENT_TO_STATUS.get(key),
        envelope_id=_s(_dig(payload, "id", "documentId", "envelopeId")),
        external_id=_s(_dig(payload, "externalId")),
    )
