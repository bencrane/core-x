"""Company-profile snapshots — the Dossier's append-only "Save Profile" surface.

  POST /api/v1/company-profiles/{domain}/snapshots   service-token — append one immutable snapshot
  GET  /api/v1/company-profiles/{domain}/snapshots/latest  service-token — the latest snapshot (or 404)

The platform-api BFF brokers these with the operator session (it never touches the DB directly).
The canonical current profile (business.company_profiles) is separate and untouched; this surface
only ever APPENDS — the read resolves the latest row by domain.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..company_profiles import queries
from ..company_profiles.models import CompanyProfileSnapshot, CompanyProfileSnapshotCreate
from ..db import get_db_connection
from ..service_token import require_service_token

router = APIRouter(prefix="/api/v1/company-profiles", tags=["company-profiles"])


@router.post("/{domain}/snapshots", dependencies=[Depends(require_service_token)])
async def create_snapshot(domain: str, body: CompanyProfileSnapshotCreate) -> CompanyProfileSnapshot:
    """Append one immutable dossier snapshot for ``domain``. Returns the persisted row."""
    if not (domain or "").strip():
        raise HTTPException(status_code=422, detail="domain required")
    async with get_db_connection() as conn:
        return await queries.insert_snapshot(conn, domain=domain, body=body)


@router.get("/{domain}/snapshots/latest", dependencies=[Depends(require_service_token)])
async def latest_snapshot(domain: str) -> CompanyProfileSnapshot:
    """The most recent snapshot for ``domain``. 404 when the operator has never saved one."""
    async with get_db_connection() as conn:
        snap = await queries.get_latest_by_domain(conn, domain)
    if snap is None:
        raise HTTPException(status_code=404, detail="no snapshot for domain")
    return snap
