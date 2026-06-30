"""Operator-owned MIRROR-template DEFAULT store (business.documenso_template_defaults).

Records which business.documenso_envelopes template is the operator's Confirm & Originate default.
App-owned — the projector / on-demand re-grab NEVER write it (same boundary as
business.documenso_template_document_prefill_configs). The default cannot live on the mirror
(projector-owned, verbatim) nor on the legacy business.documenso_templates registry (mirror-path
templates aren't in it), so it lives here keyed by the mirror's numeric documenso_id.
"""
