"""Regression: the /sign/{agreement_handle} payment lane resolves fee + signer classification.

Three invariants, all pure (no network, no DB), pinned after the 2026-07-19 audit found the lane
unpayable end-to-end:

1. FEE KEYS — the mint's amount resolver must read the agreement-flow template label
   (``PrepaidFee``) as well as the legacy opportunity key (``fee_amount``); the agreement fallback
   feeds it the generate-time merge (prefill-config defaults ⊕ agreement overrides), so a
   default-only fee (the common case — the fee label is read_only with a config default and no
   per-agreement override) must resolve to a positive charge.

2. PER-UNIT LABELS NEVER CHARGE — ``PricePerIntro`` (a per-unit rate, also on the template) must
   not be treated as the payable-at-signing amount.

3. PROVIDER DOMAINS — the sign-gate classifier keys on recipient email domain. The template's
   Provider slot signs from its placeholder ``provider@example.com`` (fixed-email recipients pass
   through generate untouched), and the operator's own mailbox is ``@engineereddemand.com``. Both
   domains MUST be classified provider-side: a miss lets the provider's countersignature flip the
   COUNTERPARTY gate — the prospect's pay step opens before the prospect signed (fail-open).
"""
from __future__ import annotations

from apps.edge_api.src.deals.originate import resolve_field_values
from apps.edge_api.src.document_payments.amount import resolve_fee_cents
from apps.edge_api.src.documenso_webhooks.queries import _PROVIDER_SIGNING_DOMAINS

# The live template's prefill config shape (template 14503): fee is a read_only config default.
_FIELD_SETTINGS = {
    "PrepaidFee": {"read_only": True, "default_document_field_value": "$36,000"},
    "PricePerIntro": {"read_only": True, "default_document_field_value": "$3,000."},
    "Full Name": {"read_only": False, "default_document_field_value": ""},
}


def test_prepaid_fee_label_resolves_cents() -> None:
    assert resolve_fee_cents({"PrepaidFee": "$36,000"}) == 3_600_000


def test_fee_amount_takes_precedence_over_prepaid_fee() -> None:
    assert resolve_fee_cents({"fee_amount": "$35,000", "PrepaidFee": "$36,000"}) == 3_500_000


def test_agreement_merge_resolves_default_only_fee() -> None:
    """A default-only fee (no per-agreement override) must survive the generate-time merge and charge."""
    merged = resolve_field_values(_FIELD_SETTINGS, {"Full Name": "Jane Prospect"})
    assert merged["PrepaidFee"] == "$36,000"
    assert resolve_fee_cents(merged) == 3_600_000


def test_agreement_override_wins_over_config_default() -> None:
    merged = resolve_field_values(_FIELD_SETTINGS, {"PrepaidFee": "$40,000"})
    assert resolve_fee_cents(merged) == 4_000_000


def test_per_unit_label_never_charges() -> None:
    """PricePerIntro alone is NOT a payable amount — 0 → the router 409s instead of charging a rate."""
    assert resolve_fee_cents({"PricePerIntro": "$3,000."}) == 0


def test_provider_slot_placeholder_domain_is_provider_side() -> None:
    assert "example.com" in _PROVIDER_SIGNING_DOMAINS


def test_operator_mailbox_domain_is_provider_side() -> None:
    assert "engineereddemand.com" in _PROVIDER_SIGNING_DOMAINS
