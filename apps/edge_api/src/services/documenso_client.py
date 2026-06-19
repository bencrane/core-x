"""Documenso v2 e-signature client (Documenso Cloud, Platform tier).

The reusable signing core: create an envelope from the rendered agreement PDF with the Client
as the sole SIGNER (no Documenso-sent email — the consumer app controls delivery), read back
the recipient signing token (drives the embed), pull the sealed PDF on completion, and verify
inbound webhooks.

The SIGNATURE + DATE fields are placed by ANCHOR, not coordinates: the rendered agreement emits
the ``[[CLIENT_SIGNATURE]]`` / ``[[CLIENT_DATE]]`` markers (``proposals.signing_anchors``) on the
client execution lines, and ``field/create-many`` resolves each field's position from the marker
via Documenso's ``findText`` (it whites the marker out at sign-time). Position is therefore
independent of body length, page count, and template — no page math, no blind percentages.

CALIBRATION BOUNDARY — the placeholder field shape used here (``field/create-many`` with a
``placeholder`` string in place of ``page``/``positionX``/``positionY``) is CONFIRMED against
Documenso v2 source: ``packages/trpc/.../create-envelope-fields.types.ts`` defines the request as
a union of ``ZCoordinatePositionSchema`` and ``ZPlaceholderPositionSchema`` (``placeholder`` +
optional ``width``/``height`` + ``matchAll``), and
``packages/app-tests/e2e/api/v2/placeholder-fields-api.spec.ts`` exercises it over the public API.
The remaining ``# CALIBRATE`` shapes (envelope/create multipart field names, the download
operation, response key names) render client-side in Documenso's OpenAPI viewer and could not be
byte-pinned at build time; verify against the live Platform account's spec
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
from ..proposals.signing_anchors import CLIENT_DATE_ANCHOR, CLIENT_SIGNATURE_ANCHOR

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


def _str_or_none(v: Any) -> str | None:
    return str(v) if v is not None else None


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


def _extract_signer_token(body: Any) -> str | None:
    """Pull the SIGNER recipient's signing token from an envelope (the template-instantiated path).

    Unlike ``_extract_client_token``, there is no caller-supplied email to match on: a document
    instantiated from a template via ``/envelope/use`` (recipients omitted) carries the template's
    own recipients, whose email may be blank. Select by role (``SIGNER``), falling back to the first
    recipient — correct for the single-signer engagement template.
    """
    env = _dig(body, "envelope", "document", "data") or body
    recips = _dig(env, "recipients", "Recipient") or []
    if not isinstance(recips, list) or not recips:
        return None
    signer = next(
        (r for r in recips if isinstance(r, dict) and str(_dig(r, "role") or "").upper() == "SIGNER"),
        recips[0],
    )
    tok = _dig(signer if isinstance(signer, dict) else {}, "token", "signingToken")
    return str(tok) if tok else None


def _recipient_email_tokens(body: Any) -> tuple[tuple[str, str], ...]:
    """All ``(email_lowercased, token)`` pairs on the document — lets a caller pick a SPECIFIC signer's
    token when a document has more than one recipient (e.g. client + originator). Only recipients that
    actually carry a token are included; blank-email recipients keep an empty-string key."""
    env = _dig(body, "envelope", "document", "data") or body
    recips = _dig(env, "recipients", "Recipient") or []
    if not isinstance(recips, list):
        return ()
    out: list[tuple[str, str]] = []
    for r in recips:
        if not isinstance(r, dict):
            continue
        tok = _dig(r, "token", "signingToken")
        if tok:
            out.append(((_dig(r, "email") or "").strip().lower(), str(tok)))
    return tuple(out)


# Field SIZE overrides — PERCENT of the page (0-100). Position is NOT set here: Documenso resolves
# it from the anchor marker (``findText``). But the anchor's own text box (a thin one-line strip at
# sig-line font size) is too small to be a usable signature/date target, so we override the box to
# the same proportions the prior coordinate placement used (sig ~32×7, date ~22×4). ``width``/
# ``height`` are the only ``ZPlaceholderPositionSchema`` fields beyond ``placeholder``; omitting
# them would fall back to the marker's measured size.
_SIGNATURE_FIELD_SIZE = {"width": 32.0, "height": 7.0}
_DATE_FIELD_SIZE = {"width": 22.0, "height": 4.0}


def _numeric_document_id(env: dict[str, Any]) -> int | None:
    """Documenso exposes the legacy numeric document id as ``secondaryId`` = ``document_<n>``."""
    m = re.search(r"(\d+)", str(_dig(env, "secondaryId") or ""))
    return int(m.group(1)) if m else None


def _prefill_value_for_label(label: str, values: dict[str, Any]) -> str | None:
    """The prefill value for a template field LABEL. Exact key first; else fall back to the BASE name —
    a label split for multiple placements (e.g. ``participant_company_one`` / ``participant_company_two``)
    draws from the single ``participant_company`` value. Empty/None yields None (the field stays open)."""
    v = values.get(label)
    if v not in (None, ""):
        return str(v)
    base = label.rsplit("_", 1)[0]
    if base != label:
        bv = values.get(base)
        if bv not in (None, ""):
            return str(bv)
    return None


async def create_signing_envelope(
    pdf_bytes: bytes, *, title: str, signer_name: str, signer_email: str,
    external_id: str | None = None,
) -> EnvelopeResult:
    """Create a signable v2 envelope from the agreement PDF with the Client as the sole SIGNER,
    place the signature + date fields BY ANCHOR, and distribute WITHOUT email (embedded-signing
    flow).

    Validated against Documenso Cloud v2: ``/envelope/create`` (multipart ``payload`` JSON +
    ``files``), ``/envelope/field/create-many`` (anchor-positioned fields — a ``placeholder``
    string resolved by ``findText`` over the PDF, not coordinates), ``/envelope/distribute``
    (``distributionMethod: NONE``). The recipient signing token, read back from the envelope, drives
    the embed. ``external_id`` (the proposal ref) is stamped on the envelope so the webhook can
    match it back deterministically, independent of the payload's id shape.
    """
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

        # 3) place the SIGNATURE + DATE fields for the recipient BY ANCHOR. Documenso resolves the
        #    position (page + x/y) from the marker via ``findText`` and whites the marker out; we
        #    only override the field box size (the marker's own text strip is too small to sign on).
        fields = [
            {
                "type": "SIGNATURE",
                "recipientId": recipient_id,
                "placeholder": CLIENT_SIGNATURE_ANCHOR,
                **_SIGNATURE_FIELD_SIZE,
            },
            {
                "type": "DATE",
                "recipientId": recipient_id,
                "placeholder": CLIENT_DATE_ANCHOR,
                **_DATE_FIELD_SIZE,
            },
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


# ── Direct-to-documenso: instantiate a signable document FROM AN EXISTING TEMPLATE ────────────────
# The through-docraptor path (``create_signing_envelope``) renders a PDF and attaches it to a fresh
# envelope, placing SIGNATURE/DATE by anchor. The direct path is the inverse: the template ALREADY
# carries its document, recipients, and fields — so we only instantiate, distribute without email,
# and read the signer token back.


async def _resolve_template_envelope_id(client: httpx.AsyncClient, documenso_template_id: str) -> str:
    """Resolve a numeric Documenso template id (e.g. ``13986``) to its prefixed envelope id
    (e.g. ``envelope_wunounvkihrueorc``).

    ``business.documenso_templates`` stores only the numeric template id, and ``/envelope/use``
    requires the prefixed ``envelopeId`` (the numeric id 400s on the envelope endpoints) — so the
    envelope id must be fetched live: ``GET /api/v2/template/{id}`` → ``.envelopeId``.
    """
    resp = await client.get(f"/api/v2/template/{documenso_template_id}")
    _raise_for_status(resp, "template/get")
    envelope_id = _dig(resp.json(), "envelopeId", "id")
    if not envelope_id:
        raise DocumensoError(
            f"template/{documenso_template_id}: no envelopeId in {resp.text[:300]}"
        )
    return str(envelope_id)


# ── Template-use lane: prefilled signable document via /template/use → distribute(NONE) → PENDING ───
# The CANONICAL direct-to-documenso path: it uses the template's stored PDF as-is — prefill the
# template's fields, bind the prospect as recipient, then distribute WITHOUT email so the document
# lands PENDING (signable, no email sent). SIGNATURE/DATE are left for the signer.
#
# CONFIRMED against Documenso v2 + live OpenAPI:
#   * ``POST /api/v2/template/use`` — UNLIKE ``/envelope/use``, ``recipients`` is REQUIRED, each entry
#     ``{id, email, name}`` where ``id`` is the TEMPLATE's recipient id (overriding email/name onto the
#     placeholder recipient). ``distributeDocument:false`` leaves it DRAFT until the distribute call.
#   * ``prefillFields`` is keyed by FIELD ID (not label), ``type`` LOWERCASED (``text``/``number``),
#     ``value`` always a STRING (even for NUMBER). A label can map to MULTIPLE field ids (e.g.
#     ``participant_company`` on two fields) — emit one prefill entry PER id. SIGNATURE/DATE and any
#     label not in ``field_values`` are skipped.
#   * ``/template/use`` returns the FULL new envelope — the prefixed ``envelopeId``, the numeric
#     document ``id``, ``recipients[].token``, and ``fields[]`` — so no document read-back is needed.
#   * ``POST /api/v2/envelope/distribute`` with ``meta.distributionMethod:NONE`` moves DRAFT → PENDING
#     and mints the signing surface WITHOUT sending email (the consumer app delivers the link).


async def create_document_from_template(
    documenso_template_id: str,
    *,
    recipient_email: str,
    recipient_name: str,
    field_values_by_label: dict[str, str] | None = None,
    external_id: str | None = None,
    title: str | None = None,
) -> EnvelopeResult:
    """Instantiate a signable document FROM A DOCUMENSO TEMPLATE via ``/template/use`` with the fields
    PREFILLED and LOCKED (readOnly), then distribute WITHOUT email so it lands ``PENDING`` (ready to
    sign, no email sent). The signer only applies the SIGNATURE; the DATE fields auto-stamp on signing.

    Resolves the template live (``GET /api/v2/template/{id}``) for its fields (``id``/``type``/
    ``fieldMeta.label``) and its recipient id, fans out each provided label to EVERY matching field id
    (lowercased type, value coerced to a string), overrides the template's recipient with the supplied
    ``email``/``name``, ``POST /api/v2/template/use`` with ``distributeDocument:false``, LOCKS every
    prefilled field on the NEW document via ``POST /api/v2/envelope/field/update-many`` (``readOnly:true``
    — operator-set terms are not signer-editable), then ``POST /api/v2/envelope/distribute`` with
    ``distributionMethod:NONE`` to mint the signer's signing surface without sending email.
    ``/template/use`` returns the full new envelope, so ``envelope_id`` (prefixed), ``document_id``
    (numeric), and ``client_token`` come straight off that response.

    readOnly can't be set at instantiation (neither ``/template/use``'s ``override`` nor ``prefillFields``
    expose it) and a TEMPLATE field can't be readOnly without static text — so the lock is applied on the
    DERIVED document, where the prefilled value satisfies Documenso's "read-only must have text" rule. The
    derived fields carry NEW ids and NO labels, so prefilled fields are identified by a non-empty value
    (``fieldMeta.text``/``value``); SIGNATURE/DATE carry no value and stay open for the signer.
    """
    async with _client() as client:
        # 1) resolve the template's fields + recipient id LIVE (the operator may be editing it; never
        #    hardcode the field set). ``GET /api/v2/template/{id}`` carries fields[] and recipients[].
        tmpl_resp = await client.get(f"/api/v2/template/{documenso_template_id}")
        _raise_for_status(tmpl_resp, "template/get")
        tmpl = tmpl_resp.json()

        # Bind the prospect to the PLACEHOLDER recipient — the template recipient with NO email set.
        # A recipient that already carries an email (e.g. the originator, added via "Add Myself") is a
        # FIXED signer: leave it untouched and OMIT it from ``recipients[]`` below, which keeps its
        # template default (CONFIRMED against Documenso v2 — recipients omitted from /template/use are
        # preserved). For a single-recipient template this still selects that one slot; fall back to the
        # SIGNER/first only if every recipient already has an email (no open placeholder).
        recips = tmpl.get("recipients") or []
        if not recips:
            raise DocumensoError(
                f"template/{documenso_template_id}: no recipients to instantiate"
            )
        placeholder = next(
            (r for r in recips if isinstance(r, dict) and not (_dig(r, "email") or "").strip()),
            None,
        ) or next(
            (r for r in recips if isinstance(r, dict) and str(_dig(r, "role") or "").upper() == "SIGNER"),
            recips[0],
        )
        recipient_id = _dig(placeholder if isinstance(placeholder, dict) else {}, "id")
        if recipient_id is None:
            raise DocumensoError(
                f"template/{documenso_template_id}: no recipient id to override"
            )

        # label -> [(field id, lowercased type), …] — a label can appear on MULTIPLE fields, so this
        # is a fan-out, not a 1:1 map. SIGNATURE/DATE carry no label and are excluded here implicitly.
        by_label: dict[str, list[tuple[Any, str]]] = {}
        for fld in tmpl.get("fields") or []:
            if not isinstance(fld, dict):
                continue
            lab = (fld.get("fieldMeta") or {}).get("label")
            if not lab:
                continue
            by_label.setdefault(lab, []).append(
                (fld.get("id"), str(fld.get("type") or "").lower())
            )

        # 2) build prefillFields by iterating the TEMPLATE's labelled fields (the source of truth for
        #    what exists). A label may sit on MULTIPLE fields (fan-out). Each value is resolved exact-
        #    first, else from its BASE name — a label split for placement (e.g. ``participant_company_one``
        #    / ``participant_company_two``) draws from the single ``participant_company`` value.
        #    (TEXT → value; NUMBER → value as a string like "90".)
        vals = field_values_by_label or {}
        prefill_fields: list[dict[str, Any]] = []
        for label, entries in by_label.items():
            value = _prefill_value_for_label(label, vals)
            if value is None:
                continue
            for field_id, field_type in entries:
                prefill_fields.append({"id": field_id, "type": field_type, "value": value})

        # 3) POST /template/use — distributeDocument:false (DRAFT for now); recipients[] is REQUIRED,
        #    override the template recipient's email/name with the participant. Step 4 distributes it.
        payload: dict[str, Any] = {
            "templateId": int(documenso_template_id),
            "recipients": [
                {"id": recipient_id, "email": recipient_email, "name": recipient_name}
            ],
            "distributeDocument": False,
        }
        if prefill_fields:
            payload["prefillFields"] = prefill_fields
        if external_id:
            payload["externalId"] = external_id
        if title:
            # Override the derived document's title — otherwise it inherits the TEMPLATE's filename
            # (e.g. "AO_Term_Plain_v1.pdf"), which surfaces in the signer's "Are you sure?" confirmation
            # modal and the downloaded PDF. CONFIRMED against Documenso v2: ``/template/use`` accepts
            # ``override.title`` (``z.string().min(1).max(255)``, optional, no required siblings); cap at
            # 255 defensively.
            payload["override"] = {"title": title[:255]}

        created = await client.post("/api/v2/template/use", json=payload)
        _raise_for_status(created, "template/use")
        body = created.json()
        # ``/template/use`` returns the FULL new envelope — the prefixed ``envelopeId``, the numeric
        # document ``id``, and ``recipients[].token`` — so the durable handle and signer token come
        # straight off this response (no document read-back needed).
        envelope_id = _dig(body, "envelopeId")
        numeric_id = _dig(body, "id")
        if not envelope_id:
            raise DocumensoError(f"template/use: no envelopeId in {created.text[:300]}")
        # Both signers carry a token; select the CLIENT's by the email we just bound (the prospect) —
        # NOT "first SIGNER", which is ambiguous now that the originator is a second recipient.
        token = _extract_client_token(body, recipient_email)

        # 4) LOCK the prefilled fields on the NEW document — set ``readOnly:true`` so the operator-set
        #    terms (fee, term, company, name, title) are NOT signer-editable. readOnly can't be set at
        #    instantiation, and a template field can't be readOnly without static text, so it MUST be
        #    applied here on the derived document, where the prefilled value satisfies the "read-only
        #    must have text" rule. Derived fields have NEW ids and NO labels → identify the prefilled
        #    ones by a non-empty value; carry the full fieldMeta + geometry back so nothing is clobbered.
        new_fields = (await client.get(f"/api/v2/envelope/{envelope_id}")).json().get("fields") or []
        locks: list[dict[str, Any]] = []
        for fld in new_fields:
            if not isinstance(fld, dict):
                continue
            ftype = fld.get("type")
            meta = fld.get("fieldMeta") or {}
            value = meta.get("text") if ftype == "TEXT" else meta.get("value") if ftype == "NUMBER" else None
            if ftype not in ("TEXT", "NUMBER") or not str(value or "").strip():
                continue
            locked_meta = {k: v for k, v in meta.items() if v is not None}
            locked_meta["readOnly"] = True
            entry: dict[str, Any] = {"id": fld.get("id"), "type": ftype, "fieldMeta": locked_meta}
            for k in ("positionX", "positionY", "width", "height"):
                if fld.get(k) is not None:
                    entry[k] = float(fld[k])
            if fld.get("page") is not None:
                entry["page"] = int(fld["page"])
            locks.append(entry)
        if locks:
            locked_resp = await client.post(
                "/api/v2/envelope/field/update-many",
                json={"envelopeId": envelope_id, "data": locks},
            )
            _raise_for_status(locked_resp, "envelope/field/update-many")

        # 5) distribute WITHOUT email — DRAFT → PENDING, minting the signer's signing surface (the
        #    embed needs it). Same no-email distribute the envelope/use lane uses; the consumer app
        #    delivers the link.
        distributed = await client.post(
            "/api/v2/envelope/distribute",
            json={"envelopeId": envelope_id, "meta": {"distributionMethod": "NONE"}},
        )
        _raise_for_status(distributed, "envelope/distribute")

    return EnvelopeResult(
        envelope_id=str(envelope_id),
        document_id=int(numeric_id) if numeric_id is not None and str(numeric_id).isdigit() else None,
        client_token=token,
    )


@dataclass(frozen=True)
class DocumentReadResult:
    """A live read of a Documenso document by its NUMERIC id (the (opportunity, document) sign-token
    lane). ``external_id`` is the opportunity UUID stamped at originate — the caller pair-gates on it
    before handing back ``signing_token``."""

    document_id: int
    envelope_id: str | None       # the prefixed v2 handle (envelope_…)
    external_id: str | None       # the opportunity UUID stamped at originate
    status: str | None            # Documenso document status (PENDING / COMPLETED / …)
    signing_token: str | None     # the FIRST SIGNER's signing token (single-signer / fallback)
    # All (email, token) pairs — the caller picks the client's vs the originator's on a two-signer doc.
    recipient_tokens: tuple[tuple[str, str], ...] = ()


async def read_document(document_id: str) -> DocumentReadResult:
    """Read a Documenso document by its NUMERIC id via ``GET /api/v2/document/{numeric}``.

    This is the document-keyed read for the ``(opportunity_id, document_id)`` signing link. The
    numeric id resolves directly on the *document* endpoint (the prefixed ``envelope_…`` handle 400s
    there — verified 2026-06-17), and the response carries ``externalId`` (the opportunity UUID
    stamped at originate), the prefixed ``envelopeId``, the document ``status``, and the SIGNER
    recipient's ``token``. The caller asserts ``external_id == opportunity_id`` BEFORE returning the
    token, so a guessed numeric id with a wrong/missing UUID never yields a signing surface.
    """
    async with _client() as client:
        resp = await client.get(f"/api/v2/document/{document_id}")
    _raise_for_status(resp, "document/get")
    body = resp.json()
    numeric = _dig(body, "id")
    resolved = (
        int(numeric)
        if numeric is not None and str(numeric).isdigit()
        else int(document_id)
        if str(document_id).isdigit()
        else 0
    )
    return DocumentReadResult(
        document_id=resolved,
        envelope_id=_str_or_none(_dig(body, "envelopeId")),
        external_id=_str_or_none(_dig(body, "externalId")),
        status=_str_or_none(_dig(body, "status")),
        signing_token=_extract_signer_token(body),
        recipient_tokens=_recipient_email_tokens(body),
    )


async def get_template_text_field_labels(documenso_template_id: str) -> list[str]:
    """The TEXT-field LABELS of a live Documenso template, in field order, de-duplicated.

    Pulls the ACTUAL fields from Documenso (the source of truth) so the staging value form matches the
    template exactly — no stored/denormalized list to drift. Resolves the numeric template id → its
    envelope id → the envelope's fields, and returns each TEXT field's ``fieldMeta.label`` (SIGNATURE
    and DATE fields are excluded; the label is what was set at field-create time).
    """
    async with _client() as client:
        envelope_id = await _resolve_template_envelope_id(client, documenso_template_id)
        env = (await client.get(f"/api/v2/envelope/{envelope_id}")).json()
    labels: list[str] = []
    seen: set[str] = set()
    for f in env.get("fields") or []:
        if not isinstance(f, dict) or f.get("type") != "TEXT":
            continue
        label = str((f.get("fieldMeta") or {}).get("label") or "").strip()
        if label and label not in seen:
            seen.add(label)
            labels.append(label)
    return labels


# Field types that carry a fillable DEFAULT value (signatures/dates do not), each mapped to the
# fieldMeta key that holds its default: TEXT→text, NUMBER→value, DROPDOWN→defaultValue.
_DEFAULT_META_KEY = {"TEXT": "text", "NUMBER": "value", "DROPDOWN": "defaultValue"}


def _field_default_value(field: dict[str, Any]) -> str | None:
    """The default baked into a field today (per its type), or None when unset."""
    key = _DEFAULT_META_KEY.get(str(field.get("type")))
    if not key:
        return None
    v = (field.get("fieldMeta") or {}).get(key)
    return str(v) if v not in (None, "") else None


async def get_template_fields(documenso_template_id: str) -> list[dict[str, Any]]:
    """The EDITABLE fields of a live Documenso template — ``id``, ``type``, ``label``, ``recipient_id``,
    ``page``, and the field's current ``default``. SIGNATURE/DATE are excluded (no default to set).
    The read side of the Settings "Documenso Templates" defaults editor; the live template is the
    source of truth so the editor never drifts.
    """
    async with _client() as client:
        envelope_id = await _resolve_template_envelope_id(client, documenso_template_id)
        env = (await client.get(f"/api/v2/envelope/{envelope_id}")).json()
    fields: list[dict[str, Any]] = []
    for f in env.get("fields") or []:
        if not isinstance(f, dict) or str(f.get("type")) not in _DEFAULT_META_KEY:
            continue
        fields.append(
            {
                "id": f.get("id"),
                "type": f.get("type"),
                "label": (f.get("fieldMeta") or {}).get("label"),
                "recipient_id": f.get("recipientId"),
                "page": f.get("page"),
                "default": _field_default_value(f),
            }
        )
    return fields


async def set_template_field_defaults(documenso_template_id: str, defaults: dict[int, str]) -> int:
    """Write DEFAULT values onto a template's fields — modifies the actual Documenso TEMPLATE.

    ``defaults`` maps field id → value; each is written to the right fieldMeta key for the field's
    type (TEXT→``text``, NUMBER→``value``, DROPDOWN→``defaultValue``), MERGED into the field's existing
    meta so label/type survive. Future documents instantiated from the template (``/envelope/use``)
    inherit these; already-created documents are untouched. Returns the number of fields written.

    Sends the FULL field (id, type, recipient, position, merged fieldMeta) to
    ``/api/v2/envelope/field/update-many`` so the update can't drop existing field properties.
    """
    async with _client() as client:
        envelope_id = await _resolve_template_envelope_id(client, documenso_template_id)
        env = (await client.get(f"/api/v2/envelope/{envelope_id}")).json()
        by_id = {f.get("id"): f for f in (env.get("fields") or []) if isinstance(f, dict)}
        data: list[dict[str, Any]] = []
        for field_id, value in defaults.items():
            f = by_id.get(field_id)
            if not isinstance(f, dict):
                continue
            key = _DEFAULT_META_KEY.get(str(f.get("type")))
            if not key:
                continue
            meta = dict(f.get("fieldMeta") or {})
            meta[key] = value
            data.append(
                {
                    "id": field_id,
                    "type": f.get("type"),
                    "recipientId": f.get("recipientId"),
                    "page": f.get("page"),
                    "positionX": float(f.get("positionX")),
                    "positionY": float(f.get("positionY")),
                    "width": float(f.get("width")),
                    "height": float(f.get("height")),
                    "fieldMeta": meta,
                }
            )
        if not data:
            return 0
        resp = await client.post(
            "/api/v2/envelope/field/update-many", json={"envelopeId": envelope_id, "data": data}
        )
        _raise_for_status(resp, "envelope/field/update-many")
    return len(data)


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
