"""Agreements — the contractual instrument, child of a deal (``business.agreements``).

  POST   /api/v1/deals/{handle}/agreements       service-token — create a DRAFT
  GET    /api/v1/deals/{handle}/agreements       service-token — list (newest first)
  GET    /api/v1/agreements/by-handle/{ahandle}  service-token — resolve the public 8-char handle
  PATCH  /api/v1/agreements/{agreement_id}       service-token — draft-only partial update
  POST   /api/v1/agreements/{agreement_id}/generate  service-token — mint the Documenso document
  GET    /api/v1/templates                        service-token — selectable template mirror rows

Ontology (operator ruling 2026-07-19): the Agreement owns signatory, template binding,
field_values, the generated document, and the lifecycle ``draft|generated|sent|signed|void``.
The Deal owns none of that; generate NEVER touches ``business.deals.status``. State routes ONLY
through ``business.agreements`` — ``deal_document_configs`` is superseded (never read here).

``agreement_handle`` (LEFT(id,8)) is the public capability: generate stamps it as the Documenso
``externalId``, which makes the EXISTING pair-gated public endpoints
(``/api/v1/documenso/sign-token/{externalId}/{documentId}`` + ``/sign-state/...``) serve the new
first-party ``/sign/{agreement_handle}`` flow with no new public surface.

Concurrency: generate claims atomically (``UPDATE … WHERE status='draft'`` + COMMIT) BEFORE the
Documenso mint — a double-fire's second caller gets 409; on DocumensoError the claim is
compensated back to 'draft'. PATCH is last-write-wins by ruling (single-operator surface).
Commit ownership: every endpoint commits its own connection before returning.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from ..db import get_db_connection
from ..deals import originate, queries as deal_queries
from ..deals.models import TemplateOption
from ..service_token import require_service_token
from ..services import documenso_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["agreements"])

_ROW_COLS = (
    "a.id::text AS agreement_id, a.agreement_handle, a.deal_id::text AS deal_id, "
    "a.template_documenso_id, a.field_values, a.signatory_contact_id::text AS signatory_contact_id, "
    "a.status, a.documenso_envelope_id, a.documenso_document_id, a.signing_token, "
    "a.created_at, a.generated_at, a.sent_at, a.signed_at, a.voided_at"
)


class AgreementCreate(BaseModel):
    """Draft creation. Signatory: either an existing ``signatory_contact_id`` or inline
    first/last/email/title (upserted as a contact on the deal's account)."""

    template_documenso_id: int | str | None = None
    field_values: dict[str, Any] | None = None
    signatory_contact_id: str | None = None
    signatory_first: str | None = None
    signatory_last: str | None = None
    signatory_email: str | None = None
    signatory_title: str | None = None


class AgreementPatch(BaseModel):
    """Draft-only partial update. ``field_values`` is a FULL replace (last-write-wins by
    ruling — single-operator surface, no optimistic locking)."""

    template_documenso_id: int | str | None = None
    field_values: dict[str, Any] | None = None
    signatory_contact_id: str | None = None
    signatory_first: str | None = None
    signatory_last: str | None = None
    signatory_email: str | None = None
    signatory_title: str | None = None


def _iso(row: dict) -> dict:
    for k in ("created_at", "generated_at", "sent_at", "signed_at", "voided_at"):
        if row.get(k) is not None:
            row[k] = row[k].isoformat()
    return row


async def _deal_by_handle(cur, handle: str) -> dict | None:
    await cur.execute(
        "SELECT id::text AS id, account_id::text AS account_id, deal_handle "
        "FROM business.deals WHERE deal_handle = %s LIMIT 1",
        (handle,),
    )
    return await cur.fetchone()


async def _upsert_signatory_contact(cur, *, account_id: str, first: str | None, last: str | None,
                                    email: str, title: str | None) -> str:
    """The contact upsert pattern from deals/create.py — dedupe on (account_id, lower(email))."""
    await cur.execute(
        """
        INSERT INTO business.contacts AS c
            (account_id, first_name, last_name, email, title, is_primary)
        VALUES (
            %(account_id)s::uuid, %(first)s, %(last)s, %(email)s, %(title)s,
            NOT EXISTS (SELECT 1 FROM business.contacts x
                        WHERE x.account_id = %(account_id)s::uuid
                          AND x.is_primary AND x.deleted_at IS NULL)
        )
        ON CONFLICT (account_id, lower(email)) WHERE deleted_at IS NULL AND email IS NOT NULL
        DO UPDATE SET
            first_name = COALESCE(c.first_name, EXCLUDED.first_name),
            last_name  = COALESCE(c.last_name,  EXCLUDED.last_name),
            title      = COALESCE(c.title,       EXCLUDED.title),
            updated_at = now()
        RETURNING id::text AS id
        """,
        {"account_id": account_id, "first": first, "last": last, "email": email, "title": title},
    )
    return (await cur.fetchone())["id"]


async def _resolve_signatory(cur, body: AgreementCreate | AgreementPatch,
                             account_id: str) -> str | None:
    """Explicit contact id wins; else inline email upserts a contact on the deal's account."""
    if body.signatory_contact_id:
        return body.signatory_contact_id
    email = (body.signatory_email or "").strip()
    if email:
        if "@" not in email:
            raise HTTPException(status_code=422, detail="signatory_email is not a valid email")
        return await _upsert_signatory_contact(
            cur, account_id=account_id,
            first=(body.signatory_first or "").strip() or None,
            last=(body.signatory_last or "").strip() or None,
            email=email, title=(body.signatory_title or "").strip() or None,
        )
    return None


@router.post("/deals/{handle}/agreements", dependencies=[Depends(require_service_token)])
async def create_agreement(handle: str, body: AgreementCreate) -> dict:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            deal = await _deal_by_handle(cur, handle)
            if deal is None:
                raise HTTPException(status_code=404, detail="deal not found")
            signatory_id = await _resolve_signatory(cur, body, deal["account_id"])
            await cur.execute(
                f"""
                INSERT INTO business.agreements AS a
                    (deal_id, template_documenso_id, field_values, signatory_contact_id)
                VALUES (%s::uuid, %s, %s, %s::uuid)
                RETURNING {_ROW_COLS}
                """,
                (deal["id"],
                 str(body.template_documenso_id) if body.template_documenso_id is not None else None,
                 Jsonb(body.field_values or {}),
                 signatory_id),
            )
            row = await cur.fetchone()
        await conn.commit()
    return _iso(row)


@router.get("/deals/{handle}/agreements", dependencies=[Depends(require_service_token)])
async def list_agreements(handle: str) -> list[dict]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            deal = await _deal_by_handle(cur, handle)
            if deal is None:
                raise HTTPException(status_code=404, detail="deal not found")
            await cur.execute(
                f"SELECT {_ROW_COLS} FROM business.agreements a "
                "WHERE a.deal_id = %s::uuid ORDER BY a.created_at DESC",
                (deal["id"],),
            )
            rows = await cur.fetchall()
    return [_iso(r) for r in rows]


@router.get("/agreements/{agreement_id}", dependencies=[Depends(require_service_token)])
async def get_agreement(agreement_id: str) -> dict:
    """One agreement by id (the BFF reads template_documenso_id to resolve its gc field plan)."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_ROW_COLS} FROM business.agreements a WHERE a.id = %s::uuid LIMIT 1",
                (agreement_id,),
            )
            row = await cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="agreement not found")
    return _iso(row)


@router.get("/agreements/by-handle/{ahandle}", dependencies=[Depends(require_service_token)])
async def agreement_by_handle(ahandle: str) -> dict:
    """Resolve the PUBLIC 8-char agreement_handle → the full row + deal identity. The gc BFF's
    public /sign/{handle} route calls this (service-token) and decides what reaches the client."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {_ROW_COLS}, d.deal_handle, d.company_name
                  FROM business.agreements a
                  JOIN business.deals d ON d.id = a.deal_id
                 WHERE a.agreement_handle = %s
                 LIMIT 1
                """,
                (ahandle,),
            )
            row = await cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="agreement not found")
    return _iso(row)


@router.patch("/agreements/{agreement_id}", dependencies=[Depends(require_service_token)])
async def patch_agreement(agreement_id: str, body: AgreementPatch) -> dict:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT a.id::text AS id, a.status, d.account_id::text AS account_id "
                "FROM business.agreements a JOIN business.deals d ON d.id = a.deal_id "
                "WHERE a.id = %s::uuid LIMIT 1",
                (agreement_id,),
            )
            existing = await cur.fetchone()
            if existing is None:
                raise HTTPException(status_code=404, detail="agreement not found")
            if existing["status"] != "draft":
                raise HTTPException(status_code=409, detail=f"agreement is {existing['status']} — drafts only")
            signatory_id = await _resolve_signatory(cur, body, existing["account_id"])
            sets, params = [], {"id": agreement_id}
            if body.template_documenso_id is not None:
                sets.append("template_documenso_id = %(tpl)s")
                params["tpl"] = str(body.template_documenso_id)
            if body.field_values is not None:
                sets.append("field_values = %(vals)s")
                params["vals"] = Jsonb(body.field_values)
            if signatory_id is not None:
                sets.append("signatory_contact_id = %(sig)s::uuid")
                params["sig"] = signatory_id
            if not sets:
                raise HTTPException(status_code=422, detail="nothing to update")
            await cur.execute(
                f"UPDATE business.agreements a SET {', '.join(sets)} "
                f"WHERE a.id = %(id)s::uuid RETURNING {_ROW_COLS}",
                params,
            )
            row = await cur.fetchone()
        await conn.commit()
    return _iso(row)


async def _generate_inputs(cur, agreement_id: str) -> dict | None:
    """Everything generate needs, one read: the agreement, its deal handles, the ARCHETYPE's mint
    rules + verbatim envelope, and the signatory's email/name.

    Mint rules resolve at the ARCHETYPE level (ruled 2026-07-21): the template's mirrored
    ``external_id`` carries the ontology address ``{archetype_key}/{version}`` stamped at push, so
    ``split_part(external_id, '/', 1)`` names the archetype and its ``global_agreement_archetype_variables``
    text rows ARE the field plan — one definition covers every template birthed under the archetype.
    The per-template ``gc.documenso_template_field_rules`` are retired as a rule source. No archetype
    defaults exist: values come solely from the agreement's ``field_values`` (a read-only label with
    no value still 422s before the claim)."""
    await cur.execute(
        f"""
        SELECT {_ROW_COLS}, d.deal_handle, d.company_name,
               (SELECT jsonb_object_agg(av.documenso_field_label_to_use, jsonb_build_object(
                          'default_document_field_value', NULL,
                          'read_only', av.post_mint_read_only IS TRUE,
                          'required', av.post_mint_required IS TRUE))
                  FROM gc.global_agreement_archetype_variables av
                  JOIN gc.global_agreement_archetypes ga ON ga.id = av.archetype_id
                 WHERE ga.key = split_part(env.external_id, '/', 1)
                   AND av.field_type = 'text') AS gc_field_settings,
               env.documenso_response                     AS template_response,
               env.title                                  AS template_title,
               c.email                                                     AS recipient_email,
               NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), '') AS recipient_name
          FROM business.agreements a
          JOIN business.deals d ON d.id = a.deal_id
     LEFT JOIN gc.documenso_envelopes env
            ON env.documenso_id::text = a.template_documenso_id
           AND env.type = 'template' AND env.deleted_at IS NULL
     LEFT JOIN business.contacts c ON c.id = a.signatory_contact_id
         WHERE a.id = %s::uuid
         LIMIT 1
        """,
        (agreement_id,),
    )
    return await cur.fetchone()


class GenerateBody(BaseModel):
    """Optional caller-supplied FIELD PLAN. When present it REPLACES the HQX prefill config —
    the caller (e.g. the gc BFF, resolving gc.documenso_template_field_rules) is the truth.
    Shape per label: {default_document_field_value, read_only, required?}. ``required`` (when
    given) is applied to the derived document's field in the same update-many pass as the lock."""

    field_settings: dict[str, dict] | None = None


@router.post("/agreements/{agreement_id}/generate", dependencies=[Depends(require_service_token)])
async def generate_agreement(agreement_id: str, body: GenerateBody | None = None) -> dict:
    """Mint the prefilled, PENDING Documenso document for a draft agreement.

    externalId = agreement_handle (the public capability) — this is what pair-gates the
    existing public sign-token/sign-state endpoints for the /sign/{agreement_handle} flow.
    Never touches business.deals.status."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            inp = await _generate_inputs(cur, agreement_id)
            if inp is None:
                raise HTTPException(status_code=404, detail="agreement not found")
            if inp["status"] != "draft":
                raise HTTPException(status_code=409, detail=f"agreement is {inp['status']}")
            if not inp["template_documenso_id"]:
                raise HTTPException(status_code=422, detail="no template bound")
            if not inp["recipient_email"]:
                raise HTTPException(status_code=422, detail="no signatory contact with an email")

            # Pure resolution BEFORE the claim — a 422 here leaves the draft untouched.
            # Resolution order: caller-supplied plan > gc.documenso_template_field_rules (the
            # operative truth, read directly — same database). The legacy prefill config is retired.
            field_settings = (body.field_settings if body and body.field_settings is not None
                              else inp["gc_field_settings"]) or {}
            values = originate.resolve_field_values(field_settings, inp["field_values"] or {})
            locked = originate.locked_labels(field_settings)
            missing_locked = locked - set(values)
            if missing_locked:
                raise HTTPException(
                    status_code=422,
                    detail=("operator-term field(s) marked read-only but with no value: "
                            f"{', '.join(sorted(missing_locked))} — set a default in the prefill "
                            "config or a value on the agreement before generating"),
                )
            editable_labels = set(values) - locked
            required_by_label = {
                label: fs["required"]
                for label, fs in field_settings.items()
                if isinstance(fs, dict) and isinstance(fs.get("required"), bool)
            }
            prospect_rid = originate.derive_prospect_recipient_id(inp["template_response"])

            # ATOMIC CLAIM (review B-3): flip draft→generated and COMMIT before the mint —
            # a concurrent second caller finds 0 rows and 409s instead of double-minting.
            await cur.execute(
                "UPDATE business.agreements SET status = 'generated', generated_at = now() "
                "WHERE id = %s::uuid AND status = 'draft' RETURNING id",
                (agreement_id,),
            )
            if await cur.fetchone() is None:
                raise HTTPException(status_code=409, detail="agreement already claimed by a concurrent generate")
        await conn.commit()

    try:
        result = await documenso_client.create_document_from_template(
            str(inp["template_documenso_id"]),
            recipient_email=inp["recipient_email"],
            recipient_name=inp["recipient_name"] or inp["recipient_email"],
            field_values_by_label=values,
            external_id=inp["agreement_handle"],  # the public sign capability
            title=inp["template_title"] or "Agreement",
            prospect_recipient_id=prospect_rid,
            editable_labels=editable_labels,
            required_by_label=required_by_label,
        )
        if result.document_id is None:
            raise documenso_client.DocumensoError("documenso returned no numeric document id")
    except documenso_client.DocumensoError as e:
        # COMPENSATE: release the claim so the operator can retry.
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE business.agreements SET status = 'draft', generated_at = NULL "
                    "WHERE id = %s::uuid AND status = 'generated'",
                    (agreement_id,),
                )
            await conn.commit()
        raise HTTPException(status_code=502, detail=f"documenso: {e}") from e

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE business.agreements a
                   SET documenso_envelope_id = %s, documenso_document_id = %s, signing_token = %s
                 WHERE a.id = %s::uuid
             RETURNING {_ROW_COLS}
                """,
                (result.envelope_id, result.document_id, result.client_token, agreement_id),
            )
            row = await cur.fetchone()
        await conn.commit()
    out = _iso(row)
    out["sign_link"] = f"/sign/{row['agreement_handle']}"
    return out


@router.get("/templates", dependencies=[Depends(require_service_token)])
async def list_templates() -> list[TemplateOption]:
    """The selectable Documenso template mirror rows (review B-2 — the standalone route)."""
    async with get_db_connection() as conn:
        rows = await deal_queries.list_org_templates(conn, None)
    return [TemplateOption(**t) for t in rows]


@router.get("/templates/{template_id}/fields", dependencies=[Depends(require_service_token)])
async def template_fields(template_id: int) -> dict:
    """The template's field vocabulary for the origination-details editor: per label, the
    ARCHETYPE mint rules (resolved via the template's ``external_id`` ontology address — the same
    rows generate resolves against; per-template field rules are retired). Falls back to the
    mirrored envelope's labelled value fields when the template isn't archetype-addressed (labels
    only, empty defaults). 404 on a template the mirror doesn't carry."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT env.documenso_id, env.title,
                       (SELECT jsonb_object_agg(av.documenso_field_label_to_use, jsonb_build_object(
                                  'default_document_field_value', NULL,
                                  'read_only', av.post_mint_read_only IS TRUE))
                          FROM gc.global_agreement_archetype_variables av
                          JOIN gc.global_agreement_archetypes ga ON ga.id = av.archetype_id
                         WHERE ga.key = split_part(env.external_id, '/', 1)
                           AND av.field_type = 'text') AS field_settings,
                       env.documenso_response
                  FROM gc.documenso_envelopes env
                 WHERE env.documenso_id = %s AND env.type = 'template' AND env.deleted_at IS NULL
                 LIMIT 1
                """,
                (template_id,),
            )
            row = await cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="template not found")
    fields: list[dict] = []
    settings = row["field_settings"] or {}
    if settings:
        for label, fs in settings.items():
            if isinstance(fs, dict):
                fields.append({
                    "label": label,
                    "default": str(fs.get("default_document_field_value") or ""),
                    "read_only": fs.get("read_only") is True,
                })
    else:
        seen: set[str] = set()
        for f in (row["documenso_response"] or {}).get("fields", []):
            if not isinstance(f, dict):
                continue
            # Documenso stores TEXT/NUMBER labels under fieldMeta.label; top-level "label" is a
            # legacy shape kept as a fallback.
            meta = f.get("fieldMeta")
            label = (meta.get("label") if isinstance(meta, dict) else None) or f.get("label")
            if isinstance(label, str) and label.strip() and label not in seen:
                seen.add(label)
                fields.append({"label": label, "default": "", "read_only": False})
    fields.sort(key=lambda f: (f["read_only"], f["label"].lower()))
    return {"documenso_id": row["documenso_id"], "title": row["title"], "fields": fields}



