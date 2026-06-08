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


def _extract_envelope_id(body: Any) -> str | None:
    env = _dig(body, "envelope", "document", "data") or body
    val = _dig(env, "id", "envelopeId", "documentId")
    return str(val) if val is not None else None


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


async def create_signing_envelope(
    pdf_bytes: bytes, *, title: str, signer_name: str, signer_email: str,
) -> EnvelopeResult:
    """Create a v2 envelope from the agreement PDF with one embedded SIGNER (the Client).

    ``distributeDocument`` is False — Documenso sends NO email; the consumer app delivers the
    proposal link itself. Signature/date fields are auto-created from the invisible anchor tags
    rendered in the PDF (see ``agreement_template``).
    """
    # CALIBRATE: confirm field names + multipart vs json against the live v2 spec.
    recipients = [{"name": signer_name, "email": signer_email, "role": "SIGNER"}]
    files = {"file": (f"{title}.pdf", pdf_bytes, "application/pdf")}
    data = {
        "title": title,
        "recipients": json.dumps(recipients),
        "distributeDocument": "false",
    }
    async with _client() as client:
        resp = await client.post("/api/v2/envelope/create", data=data, files=files)
    _raise_for_status(resp, "envelope/create")
    body = resp.json()

    envelope_id = _extract_envelope_id(body)
    if not envelope_id:
        raise DocumensoError(f"envelope/create: could not resolve envelope id from {body!r}"[:500])
    token = _extract_client_token(body, signer_email)
    return EnvelopeResult(envelope_id=envelope_id, client_token=token)


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

    Handles both shapes: a direct ``application/pdf`` body, or a JSON envelope carrying a
    pre-signed ``downloadUrl`` (the ``download-beta`` style).
    """
    # CALIBRATE: confirm the v2 download operation path/param against the live spec.
    async with _client() as client:
        resp = await client.get(
            f"/api/v2/document/{envelope_id}/download", params={"version": "signed"},
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
    """Map a v2 (or v1-fallback) webhook payload to our internal event/status."""
    event = str(_dig(body, "event") or "")
    payload = _dig(body, "payload", "data") or {}
    return NormalizedEvent(
        event=event,
        status=_EVENT_TO_STATUS.get(event),
        envelope_id=(lambda v: str(v) if v is not None else None)(_dig(payload, "id", "documentId", "envelopeId")),
        external_id=(lambda v: str(v) if v is not None else None)(_dig(payload, "externalId")),
    )
