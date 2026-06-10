"""Company-profile snapshot model — the append-only dossier history.

Every "Save Profile" on the Dossier appends one immutable ``business.company_profile_snapshots``
row; the page loads the LATEST snapshot for a domain when one exists, else the canonical
``business.company_profiles`` seed. The snapshot is a SUPERSET of the seed: it also carries the
Main Contact (signer/title/email) and the per-section Verified map.
"""
from __future__ import annotations

import datetime as _dt

from pydantic import BaseModel, Field


class CompanyProfileSnapshotCreate(BaseModel):
    """Operator → engine: the full Dossier form captured on Save Profile. ``domain`` is the
    resolution key (lowercased by the engine). Every field is optional — the operator may save a
    partially-filled dossier."""

    company: str | None = None
    signer_name: str | None = None
    title: str | None = None
    email: str | None = None
    hq: str | None = None
    headcount: str | None = None
    est_revenue_range: str | None = None
    overview: str | None = None
    focus: list[str] = Field(default_factory=list)
    industries: list[str] = Field(default_factory=list)
    geographies: list[str] = Field(default_factory=list)
    verified: dict[str, bool] = Field(default_factory=dict)
    saved_by: str | None = None


class CompanyProfileSnapshot(BaseModel):
    """A persisted snapshot row — the create payload plus identity + the immutable timestamp.
    Datetimes are ISO strings at the projection boundary (the consumer renders, never computes)."""

    id: int
    domain: str
    company: str | None = None
    signer_name: str | None = None
    title: str | None = None
    email: str | None = None
    hq: str | None = None
    headcount: str | None = None
    est_revenue_range: str | None = None
    overview: str | None = None
    focus: list[str] = Field(default_factory=list)
    industries: list[str] = Field(default_factory=list)
    geographies: list[str] = Field(default_factory=list)
    verified: dict[str, bool] = Field(default_factory=dict)
    saved_by: str | None = None
    created_at: str | None = None

    @classmethod
    def from_row(cls, row: dict) -> "CompanyProfileSnapshot":
        created = row.get("created_at")
        return cls(
            **{**row, "created_at": created.isoformat() if isinstance(created, _dt.datetime) else created}
        )
