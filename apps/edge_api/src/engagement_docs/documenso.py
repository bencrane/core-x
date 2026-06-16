"""Documenso v2 — create a DRAFT DOCUMENT from the rendered mandate PDF. STANDALONE.

Own client, own anchors, own recipient/field logic — NOT the proposal pathway's documenso_client.
A Documenso DOCUMENT (type=DOCUMENT), not a Template, left in **DRAFT** (NOT distributed). Flow:

  1) POST /api/v2/envelope/create        multipart: payload JSON {type:DOCUMENT, recipients} + the PDF
  2) GET  /api/v2/envelope/{id}          read the recipient ids
  3) POST /api/v2/envelope/field/create-many   SIGNATURE/DATE placed BY PLACEHOLDER — findText resolves
                                         the [[...]] anchors the rendered PDF carries (no coordinates)

We deliberately DO NOT distribute — the document stays in DRAFT until a later, explicit "send" action.
DRAFT has no signing tokens yet (they are exposed at distribution); we only record the envelope id.

``externalId`` is the OPPORTUNITY id (not the deal/mandate id) — so multiple documents can hang off
one opportunity, all tagged with it. The webhook (later) resolves the exact document by envelope id.

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
    document_id: int | None  # numeric secondaryId (for later download/distribute)


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


def _raise(resp: httpx.Response, op: str) -> None:
    if resp.status_code // 100 != 2:
        raise DocumensoError(f"documenso {op} {resp.status_code}: {resp.text[:400]}")


async def create_draft_document(
    pdf_bytes: bytes,
    *,
    title: str,
    participant_name: str,
    participant_email: str,
    provider_name: str,
    provider_email: str,
    external_id: str,
) -> DocumensoDocument:
    """Create a DRAFT Documenso DOCUMENT from the rendered PDF: two SIGNER recipients, SIGNATURE +
    DATE placed by anchor for each, **NOT distributed** (stays DRAFT). ``external_id`` is the
    opportunity id. Returns the envelope id (+ numeric document id). Raises on any non-2xx."""
    payload: dict[str, Any] = {
        "type": "DOCUMENT",
        "title": title,
        "externalId": external_id,  # the OPPORTUNITY id — many documents may share it
        "recipients": [
            {"name": provider_name, "email": provider_email, "role": "SIGNER"},
            {"name": participant_name, "email": participant_email, "role": "SIGNER"},
        ],
        "distributeDocument": False,  # stay DRAFT
    }

    async with _client() as client:
        # 1) create the DRAFT DOCUMENT envelope with the PDF (multipart: payload JSON + files)
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

        # 3) place SIGNATURE + DATE per signer, BY ANCHOR (findText resolves position; we set size only).
        #    Fields can be placed while DRAFT; they're ready for when the document is later sent.
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

        # NO distribute — the document stays in DRAFT until an explicit send action later.

    return DocumensoDocument(envelope_id=str(envelope_id), document_id=_numeric_document_id(env))
