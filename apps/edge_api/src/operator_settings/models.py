"""Operator settings — request/response shapes for the per-operator cockpit config.

The two value domains mirror the DB CHECK constraints in ``sql/operator_settings.sql`` (the canonical
record of the allowed strings); declaring them as ``Literal`` rejects a bad value with a clean 422 at
the edge instead of letting it reach a CHECK violation that would ALSO abort the pooled transaction
(the same hazard the UUID guards elsewhere defend against).
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

RenderMode = Literal["through-docraptor", "direct-to-documenso"]
DirectToDocumensoLane = Literal["envelope-distribute", "prefill-document-from-template"]

# DB-default mirror — kept in lockstep with the column DEFAULTs in sql/operator_settings.sql, which
# remain the database-level system-of-record. Used to resolve effective settings when an operator has
# no row yet (GET) and to seed the INSERT branch of the upsert, so the default lives in exactly one
# place per layer and the BFF never re-encodes it.
DEFAULT_RENDER_MODE: RenderMode = "through-docraptor"
DEFAULT_DIRECT_TO_DOCUMENSO_LANE: DirectToDocumensoLane = "envelope-distribute"


class OperatorSettings(BaseModel):
    """An operator's effective originate configuration. ``updated_at`` is null when no row exists yet
    and these are the resolved defaults (never persisted); it is set once the operator first saves."""

    render_mode: RenderMode
    direct_to_documenso_lane: DirectToDocumensoLane
    updated_at: datetime | None = None


class OperatorSettingsUpsert(BaseModel):
    """The cockpit save payload. Both fields are OPTIONAL and MERGE: an omitted field preserves the
    operator's existing value on update (and falls to the DB default on first insert), so toggling
    ``render_mode`` alone never clobbers a previously-chosen ``direct_to_documenso_lane``. Values are
    constrained to the same domains the DB CHECK enforces."""

    render_mode: RenderMode | None = None
    direct_to_documenso_lane: DirectToDocumensoLane | None = None
