"""Preset engagement packages — the "lock the price + term" options for the AO term-only mandate.

SERVER-OWNED: the UI sends a package KEY; the price + term are resolved HERE so the browser can
never set the dollar amount that lands in a legal agreement (mirrors how the charge amount is
resolved server-side). The UI dropdown is populated from ``options()``.

⚠️ PLACEHOLDER pricing — set the real numbers before any mandate is sent to a counterparty.
``term_fee_cents`` is the TOTAL Engagement Fee for the Initial Term (minor units / cents).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Package:
    key: str
    label: str
    term_fee_cents: int   # total Engagement Fee for the Initial Term, in cents
    duration_months: int  # the Initial Term


# ⚠️ PLACEHOLDER amounts — operator to confirm. Keyed; the key is the only thing the client sends.
PACKAGES: dict[str, Package] = {
    "term_3mo":  Package("term_3mo",  "3-Month Term — $7,500",     750_000,  3),
    "term_6mo":  Package("term_6mo",  "6-Month Term — $13,500",  1_350_000,  6),
    "term_12mo": Package("term_12mo", "12-Month Term — $24,000", 2_400_000, 12),
}


def get(key: str) -> Package | None:
    return PACKAGES.get(key)


def options() -> list[dict]:
    """Dropdown payload for the Applications action control."""
    return [
        {
            "key": p.key,
            "label": p.label,
            "term_fee_cents": p.term_fee_cents,
            "duration_months": p.duration_months,
        }
        for p in PACKAGES.values()
    ]


def format_usd(cents: int) -> str:
    """Own money formatter (standalone — not the proposal pathway's). Whole dollars when round."""
    dollars = cents / 100
    return f"${dollars:,.0f}" if cents % 100 == 0 else f"${dollars:,.2f}"
