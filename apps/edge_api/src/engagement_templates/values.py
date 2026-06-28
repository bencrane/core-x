"""Derive + format the operator-entered values a tokenized engagement template bakes in.

The render lane substitutes ``{{ name }}`` tokens with the strings produced here. ``price_per_introduction``
is DERIVED (amount / introductions), never entered, so the per-introduction price can't drift from the
total paid and the count.
"""
from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal


def usd(amount: Decimal) -> str:
    """US dollars, thousands-grouped. Whole amounts drop the cents (``$25,000``); fractional keep two
    (``$833.33``)."""
    cents = Decimal(amount).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"${cents:,.0f}" if cents == cents.to_integral_value() else f"${cents:,.2f}"


def prepaid_introduction_tokens(
    *, amount: Decimal | None, introductions: int | None, term_days: int | None
) -> dict[str, str]:
    """``{{token}} -> value`` map for the prepaid-introductions template. ``amount`` is total USD paid;
    ``introductions`` the whole count; ``price_per_introduction`` is derived = amount / introductions.
    Raises ``ValueError`` on a missing or non-positive input."""
    if amount is None or introductions is None or term_days is None:
        raise ValueError("amount, introductions, and term_days are all required")
    amount = Decimal(amount)
    if amount <= 0:
        raise ValueError("amount must be positive")
    if introductions <= 0:
        raise ValueError("introductions must be a positive whole number")
    if term_days <= 0:
        raise ValueError("term_days must be a positive whole number")
    price_per_introduction = amount / Decimal(introductions)
    return {
        "amount": usd(amount),
        "introductions": str(introductions),
        "price_per_introduction": usd(price_per_introduction),
        "term_days": str(term_days),
    }
