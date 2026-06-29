"""Documenso template document PREFILL CONFIGS — the "Manage Documenso Templates" prefill editor.

``business.documenso_template_document_prefill_configs`` (one row per template, keyed by
``template_documenso_id``) holds the OPERATOR-OWNED prefill state: ``field_settings``, keyed by field
LABEL, each value an ARBITRARY object stored verbatim. Phase 1 sets per label
``default_document_field_value`` + ``read_only``; Phase 2 adds ``source`` — both pass through.

The default lives HERE, in our config, and is applied at ORIGINATE later (model B: deal override ??
default). It is NOT baked onto the Documenso template. The template dropdown + editable fields are read
off the verbatim envelope MIRROR (``business.documenso_envelopes``, ``type='template'``), NOT the legacy
``business.documenso_templates`` registry.

This package is the ONLY writer of the prefill-config table; the webhook projector / resync MUST NEVER
touch it.
"""
from . import queries
from .queries import (
    get_prefill_config,
    get_template_value_fields,
    upsert_prefill_config,
)

__all__ = [
    "queries",
    "get_template_value_fields",
    "get_prefill_config",
    "upsert_prefill_config",
]
