"""Unit tests for the engagement-template baked-value path (government-contracted prepaid-introductions).

No network. Pins the money-bearing contract:
  1. ``values.usd`` — whole-dollar drops cents, fractional keeps two, HALF_UP rounding, thousands group.
  2. ``values.prepaid_introduction_tokens`` — derivation (price = amount / introductions) + validation.
  3. ``render`` token substitution — the leftover/unknown-token guard hard-errors (``MissingTokenError``)
     so a blank-input bug can never reach a billed render as literal ``{{…}}``; the happy path bakes the
     formatted values into the real prepaid-introductions HTML.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from apps.edge_api.src.engagement_templates import catalog, render, values


# ── values.usd ──────────────────────────────────────────────────────────────────────────────────────
def test_usd_whole_drops_cents():
    assert values.usd(Decimal("25000")) == "$25,000"


def test_usd_fractional_keeps_two():
    assert values.usd(Decimal("833.333")) == "$833.33"


def test_usd_half_up_rounding():
    assert values.usd(Decimal("0.005")) == "$0.01"


def test_usd_thousands_grouping():
    assert values.usd(Decimal("1250000.5")) == "$1,250,000.50"


# ── values.prepaid_introduction_tokens ───────────────────────────────────────────────────────────────
def test_tokens_happy_path():
    assert values.prepaid_introduction_tokens(
        amount=Decimal("25000"), introductions=25, term_days=90
    ) == {
        "amount": "$25,000",
        "introductions": "25",
        "price_per_introduction": "$1,000",
        "term_days": "90",
    }


def test_tokens_non_divisible_price_rounds():
    tokens = values.prepaid_introduction_tokens(
        amount=Decimal("25000"), introductions=30, term_days=90
    )
    assert tokens["price_per_introduction"] == "$833.33"


@pytest.mark.parametrize(
    "kw",
    [
        {"amount": None, "introductions": 25, "term_days": 90},
        {"amount": Decimal("25000"), "introductions": None, "term_days": 90},
        {"amount": Decimal("25000"), "introductions": 25, "term_days": None},
        {"amount": Decimal("0"), "introductions": 25, "term_days": 90},
        {"amount": Decimal("-1"), "introductions": 25, "term_days": 90},
        {"amount": Decimal("25000"), "introductions": 0, "term_days": 90},
        {"amount": Decimal("25000"), "introductions": 25, "term_days": 0},
    ],
)
def test_tokens_rejects_bad_input(kw):
    with pytest.raises(ValueError):
        values.prepaid_introduction_tokens(**kw)


# ── render token substitution ─────────────────────────────────────────────────────────────────────────
def _prepaid_dir():
    return catalog.resolve(
        "docraptor-to-documenso-template",
        "prepaid-introductions",
        "v1",
        brand="government-contracted",
    )


def test_assemble_without_values_raises_missing_token():
    # A tokenized template rendered with no values must hard-error, not emit literal {{…}}.
    with pytest.raises(render.MissingTokenError):
        render.assemble_html(_prepaid_dir(), "plain", tokens=None)


def test_assemble_with_values_bakes_text():
    tokens = values.prepaid_introduction_tokens(
        amount=Decimal("25000"), introductions=25, term_days=90
    )
    html, style = render.assemble_html(_prepaid_dir(), "plain", tokens=tokens)
    assert style == "plain"
    assert render._TOKEN_RE.search(html) is None  # nothing left unsubstituted
    assert "$25,000" in html
    assert "$1,000" in html
    assert "90 days" in html


def test_substitute_unknown_token_raises():
    with pytest.raises(render.MissingTokenError):
        render._substitute_tokens("a {{ nope }} b", {"amount": "$1"})
