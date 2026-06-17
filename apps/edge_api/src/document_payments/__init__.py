"""Document payments — the direct-to-documenso engagement-fee collection (Stripe ACH).

Self-contained and keyed by the ``(opportunity_id 8-char handle, document_id)`` PAIR — the same pair
the prospect signing link uses. No relationship to the legacy proposal-``ref`` payment path; the
amount is resolved from ``opportunity_specific_content.field_values['fee_amount']`` and an intent is
minted only once the document is signed. edge_api owns the whole flow; the platform-api BFF is a
pass-through. System-of-record DDL: ``apps/edge_api/sql/document_payments.sql``.
"""
