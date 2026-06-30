"""MIRROR-template default row + set-default request for the Set-Template-as-Default picker.

The picker lists every MIRROR template (business.documenso_envelopes, type='template', non-deleted)
and marks ONE as the operator's Confirm & Originate default — recorded in the operator-owned
business.documenso_template_defaults (keyed by the mirror's numeric documenso_id). snake_case is the
verbatim mirror convention; the BFF passes it straight through (no remap).
"""
from __future__ import annotations

from pydantic import BaseModel


class DefaultTemplateRow(BaseModel):
    """One mirrored TEMPLATE envelope, flagged with whether it is the operator's default."""

    documenso_id: int
    title: str | None = None
    status: str | None = None          # lowercased Documenso status, VERBATIM (e.g. 'draft')
    is_default: bool = False           # the operator's Confirm & Originate default (one per plane)


class SetDefaultRequest(BaseModel):
    """Mark a MIRROR template as the operator's Confirm & Originate default. ``documenso_id`` is the
    mirror's numeric id; only a live, non-deleted template may be set."""

    documenso_id: int
