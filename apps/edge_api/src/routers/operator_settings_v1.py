"""Operator settings — the per-operator cockpit configuration surface (service-token).

  GET /api/v1/operator-settings/{auth_user_id}   service-token — resolve effective settings
  PUT /api/v1/operator-settings/{auth_user_id}   service-token — merge-upsert settings

The platform-api BFF validates the operator's Supabase session upstream and asserts the trusted
``auth_user_id`` (the JWT sub) on the path; edge_api TRUSTS it and never re-validates — the same trust
model agent_runs uses. This RETIRES the BFF's direct Supabase service-role access to
``public.operator_settings``: the BFF becomes a pass-through (platform-app -> platform-api -> edge_api),
so edge_api is the single writer over the shared HQX_ Postgres and the table's RLS is no longer
load-bearing — the service token + the BFF's session check ARE the access boundary.

``auth_user_id`` is typed ``UUID`` so a malformed id is rejected at the edge (422) BEFORE any ``::uuid``
cast — the pooled connection is never put at risk of an aborted transaction.
"""
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from ..db import get_db_connection
from ..operator_settings import queries
from ..operator_settings.models import OperatorSettings, OperatorSettingsUpsert
from ..service_token import require_service_token

router = APIRouter(
    prefix="/api/v1/operator-settings",
    tags=["operator-settings"],
    dependencies=[Depends(require_service_token)],
)


@router.get("/{auth_user_id}")
async def get_operator_settings(auth_user_id: UUID) -> OperatorSettings:
    """The operator's effective originate settings — resolved defaults when nothing is saved yet
    (``updated_at`` null), so the BFF always receives a usable pathway."""
    async with get_db_connection() as conn:
        settings = await queries.get_settings(conn, str(auth_user_id))
    return OperatorSettings(**settings)


@router.put("/{auth_user_id}")
async def put_operator_settings(
    auth_user_id: UUID, body: OperatorSettingsUpsert
) -> OperatorSettings:
    """Merge-upsert the operator's settings (omitted fields preserve their existing value) and return
    the resulting effective row."""
    async with get_db_connection() as conn:
        settings = await queries.upsert_settings(
            conn,
            auth_user_id=str(auth_user_id),
            render_mode=body.render_mode,
            direct_to_documenso_lane=body.direct_to_documenso_lane,
        )
    return OperatorSettings(**settings)
