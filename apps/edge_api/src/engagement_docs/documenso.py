"""Documenso v2 — create a SIGNABLE DOCUMENT from the rendered mandate PDF. STANDALONE.

Own client, own anchors, own recipient/field logic — NOT the proposal pathway's documenso_client.
A Documenso DOCUMENT (type=DOCUMENT), not a Template. Flow (confirmed against Documenso Cloud v2):

  1) POST /api/v2/envelope/create        multipart: payload JSON {type:DOCUMENT, recipients} + the PDF
  2) GET  /api/v2/envelope/{id}          read the recipient ids
  3) POST /api/v2/envelope/field/create-many   SIGNATURE/DATE placed BY PLACEHOLDER — findText resolves
                                         the [[...]] anchors the rendered PDF carries (no coordinates)
  4) POST /api/v2/envelope/distribute    distributionMethod NONE → expose tokens, Documenso emails no one
  5) GET  /api/v2/envelope/{id}          read each signer's token

Two SIGNER recipients: Provider (the RS signatory, pre-rendered "Benjamin J. Crane") and Participant
(the opportunity's contact). The Participant token is the prospect's signing capability; the app
delivers the link itself.

Auth: ``Authorization: api_<key>`` (DOCUMENSO_API_KEY); base = DOCUMENSO_API_URL — both already in
core-x/prd. Read straight from the environment so this pathway shares nothing with the proposal one.
"""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(90.0, connect=10.0)

# The [[anchors]] the rendered AO term-only PDF carries (own constants — not signing_anchors).
PROVIDER_SIGNATURE_ANCHOR = "[[PROVIDER_SIGNATURE]]"
PROVIDER_DATE_ANCHOR = "[[PROVIDER_DATE]]"
PARTICIPANT_SIGNATURE_ANCHOR = "[[PARTICIPANT_SIGNATURE]]"
PARTICIPANT_DATE_ANCHOR = "[[PARTICIPANT_DATE]]"

# Field box SIZE — percent of the page. Position comes from the placeholder (findText), not here.
_SIG_SIZE = {"width": 30.0, "height": 7.0}
_DATE_SIZE = {"width": 20.0, "height": 4.0}


class DocumensoError(RuntimeError):
    """An unconfigured client or a non-2xx Documenso response."""


@dataclass(frozen=True)
class DocumensoDocument:
    envelope_id: str
    document_id: int | None
    participant_token: str | None
    provider_token: str | None


def _api_key() -> str:
    key = os.environ.get("DOCUMENSO_API_KEY")
    if not key:
        raise DocumensoError("DOCUMENSO_API_KEY is not set")
    return key if key.startswith("api_") else f"api_{key}"


def _base() -> str:
    return os.environ.get("DOCUMENSO_API_URL", "https://app.documenso.com").rstrip("/")


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=_base(), headers={"Authorization": _api_key()}, timeout=_TIMEOUT)


def _dig(obj: Any, *keys: str) -> Any:
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k] is not None:
                return obj[k]
    return None


def _numeric_document_id(env: dict[str, Any]) -> int | None:
    m = re.search(r"(\d+)", str(_dig(env, "secondaryId") or ""))
    return int(m.group(1)) if m else None


def _recipient_id(env: dict[str, Any], email: str) -> Any:
    target = (email or "").strip().lower()
    for r in env.get("recipients") or []:
        if isinstance(r, dict) and str(_dig(r, "email") or "").strip().lower() == target:
            return _dig(r, "id")
    return None


def _token_for(env: dict[str, Any], email: str) -> str | None:
    target = (email or "").strip().lower()
    for r in env.get("recipients") or []:
        if isinstance(r, dict) and str(_dig(r, "email") or "").strip().lower() == target:
            tok = _dig(r, "token", "signingToken")
            return str(tok) if tok else None
    return None


def _raise(resp: httpx.Response, op: str) -> None:
    if resp.status_code // 100 != 2:
        raise DocumensoError(f"documenso {op} {resp.status_code}: {resp.text[:400]}")


async def create_signable_document(
    pdf_bytes: bytes,
    *,
    title: str,
    participant_name: str,
    participant_email: str,
    provider_name: str,
    provider_email: str,
    external_id: str | None = None,
) -> DocumensoDocument:
    """Create a signable Documenso DOCUMENT from the rendered PDF: two SIGNER recipients, SIGNATURE +
    DATE placed by anchor for each, distributed NONE (tokens exposed, no email). Returns the envelope
    id + each signer's token. Raises DocumensoError on any non-2xx."""
    payload: dict[str, Any] = {
        "type": "DOCUMENT",
        "title": title,
        "recipients": [
            {"name": provider_name, "email": provider_email, "role": "SIGNER"},
            {"name": participant_name, "email": participant_email, "role": "SIGNER"},
        ],
        "distributeDocument": False,
    }
    if external_id:
        payload["externalId"] = external_id

    async with _client() as client:
        # 1) create the DOCUMENT envelope with the PDF (multipart: payload JSON + files)
        created = await client.post(
            "/api/v2/envelope/create",
            data={"payload": json.dumps(payload)},
            files={"files": (f"{title}.pdf", pdf_bytes, "application/pdf")},
        )
        _raise(created, "envelope/create")
        envelope_id = _dig(created.json(), "id")
        if not envelope_id:
            raise DocumensoError(f"envelope/create: no id in {created.text[:200]}")

        # 2) recipient ids
        env = (await client.get(f"/api/v2/envelope/{envelope_id}")).json()
        provider_id = _recipient_id(env, provider_email)
        participant_id = _recipient_id(env, participant_email)
        if provider_id is None or participant_id is None:
            raise DocumensoError("envelope/create: could not resolve both recipient ids")

        # 3) place SIGNATURE + DATE per signer, BY ANCHOR (findText resolves position; we set size only)
        fields = [
            {"type": "SIGNATURE", "recipientId": provider_id, "placeholder": PROVIDER_SIGNATURE_ANCHOR, "matchAll": True, **_SIG_SIZE},
            {"type": "DATE", "recipientId": provider_id, "placeholder": PROVIDER_DATE_ANCHOR, "matchAll": True, **_DATE_SIZE},
            {"type": "SIGNATURE", "recipientId": participant_id, "placeholder": PARTICIPANT_SIGNATURE_ANCHOR, "matchAll": True, **_SIG_SIZE},
            {"type": "DATE", "recipientId": participant_id, "placeholder": PARTICIPANT_DATE_ANCHOR, "matchAll": True, **_DATE_SIZE},
        ]
        placed = await client.post(
            "/api/v2/envelope/field/create-many", json={"envelopeId": envelope_id, "data": fields}
        )
        _raise(placed, "field/create-many")

        # 4) distribute WITHOUT email — exposes the signing tokens; the app delivers the link
        distributed = await client.post(
            "/api/v2/envelope/distribute",
            json={"envelopeId": envelope_id, "meta": {"distributionMethod": "NONE"}},
        )
        _raise(distributed, "distribute")

        # 5) read each signer's token back off the live envelope
        env2 = (await client.get(f"/api/v2/envelope/{envelope_id}")).json()

    return DocumensoDocument(
        envelope_id=str(envelope_id),
        document_id=_numeric_document_id(env2),
        participant_token=_token_for(env2, participant_email),
        provider_token=_token_for(env2, provider_email),
    )
