"""Documenso ENVELOPE projector — mirror webhook events into business.documenso_envelopes.

Async, fire-and-forget. The webhook route lands the RAW event (system of record) and returns 200,
then schedules ``project_envelope_event`` as a FastAPI BackgroundTask: it pulls the FULL live envelope
(GET /api/v2/envelope/{id}) and upserts a VERBATIM mirror row (status/type lowercased-only, never
remapped; ``documenso_response`` stored exactly as the API returns it). DELETE events soft-delete with
NO API pull. The projector NEVER writes business.documenso_template_configs.
"""
from .projector import project_envelope_event

__all__ = ["project_envelope_event"]
