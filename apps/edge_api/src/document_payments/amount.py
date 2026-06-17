"""Charge-amount resolution for the direct-to-documenso document fee.

NEVER hardcoded, NEVER from the browser: parsed server-side from the opportunity's
``opportunity_specific_content.field_values['fee_amount']`` — the SAME value prefilled (and locked
read-only) into the Documenso document the prospect signed, so charge == signed fee by construction.

``fee_amount`` is a human display string (e.g. ``"$35,000"`` / ``"$35,000.00"``); this normalizes it
to integer cents. A value carrying no parseable positive amount returns 0, which the router treats as
"no payable amount" (409) rather than charging a guess.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

FEE_KEY = "fee_amount"


def resolve_fee_cents(field_values: dict | None) -> int:
    """Parse ``field_values['fee_amount']`` → integer cents. Returns 0 when absent/unparseable/≤0."""
    raw = (field_values or {}).get(FEE_KEY)
    if raw is None:
        return 0
    # Keep digits and the decimal point only — strips ``$``, thousands commas, whitespace, ``/month`` etc.
    cleaned = re.sub(r"[^0-9.]", "", str(raw))
    if not cleaned or cleaned == ".":
        return 0
    try:
        dollars = Decimal(cleaned)
    except InvalidOperation:
        return 0
    if dollars <= 0:
        return 0
    return int((dollars * 100).quantize(Decimal("1")))
