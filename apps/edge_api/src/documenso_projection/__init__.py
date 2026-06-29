"""Documenso ENVELOPE projector — mirror webhook events into business.documenso_envelopes.

Async, fire-and-forget. The webhook route lands the RAW event (system of record) and returns 200,
then schedules ``project_envelope_event`` as a FastAPI BackgroundTask: it pulls the FULL live envelope
(GET /api/v2/envelope/{id}) and upserts a VERBATIM mirror row (status/type lowercased-only, never
remapped; ``documenso_response`` stored exactly as the API returns it). DELETE events soft-delete with
NO API pull. The projector NEVER writes business.documenso_template_configs.
"""
from . import queries
from .projector import project_envelope_event
from .queries import list_template_documenso_ids, list_template_mirror
from .resync import resync_template_by_documenso_id

__all__ = [
    "project_envelope_event",
    "resync_template_by_documenso_id",
    "list_template_mirror",
    "list_template_documenso_ids",
    "queries",
]
