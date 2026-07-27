"""Agreement-payment mode — the operator's post-signature collection selection.

WHAT IT DECIDES. After a prospect signs, either (a) they continue to the Stripe payment page on one
or both rails, or (b) there is NO payment page at all and the signed screen is terminal (the operator
collects by ACH/wire out-of-band, with the instructions delivered by email).

WHERE IT LIVES. ``gc.operator_settings`` — one jsonb blob per key — under the key
``agreement-payment``, per the operator ruling of 2026-07-22: ALL operator settings live there (the
``public.operator_settings`` table is an unrelated older table and is NOT used for this). The cockpit
writes the blob through the BFF (``PUT /api/v1/hq/operator-settings/agreement-payment``); edge_api
reads it here over the same pooled HQX connection it already uses for the other ``gc.*`` tables.

WHY SERVER-SIDE IS LOAD-BEARING. Disabling a rail is enforced at MINT time — the PaymentIntent is
created with only the permitted ``payment_method_types``. Hiding a tab in the browser would be
cosmetic: the intent would still accept the hidden rail for anyone who reached it. Likewise
``remittance`` makes the mint itself refuse, so the payment surface cannot be reached by URL.

GRAIN. Global, not per-operator: single-operator platform, and the prospect-facing mint has no
operator session, so one blob carries the platform-wide selection (same reasoning as the Stripe
test/live selection).
"""
from __future__ import annotations

import json
import logging
from typing import Literal

logger = logging.getLogger("edge_api.agreement_payment_mode")

# The operator_settings key this blob lives under (registered in the BFF's OPERATOR_SETTING_KEYS
# allowlist — an unregistered key 404s on write).
SETTINGS_KEY = "agreement-payment"

# card-ach   — both rails (the shipped behavior, and the default for a row-less platform)
# ach-only   — bank debit only; the card rail is not minted
# card-only  — card only; the bank-debit rail is not minted
# remittance — NO payment surface; the signed screen is terminal and collection happens out-of-band
AgreementPaymentMode = Literal["card-ach", "ach-only", "card-only", "remittance"]

DEFAULT_MODE: AgreementPaymentMode = "card-ach"

_VALID: frozenset[str] = frozenset(("card-ach", "ach-only", "card-only", "remittance"))

# Stripe payment_method_types per mode. Order is meaningful downstream only as documentation — the
# Element's tab order is driven by the browser's paymentMethodOrder, not by this list.
_RAILS: dict[str, list[str]] = {
    "card-ach": ["card", "us_bank_account"],
    "ach-only": ["us_bank_account"],
    "card-only": ["card"],
    "remittance": [],
}


def rails_for_mode(mode: str) -> list[str]:
    """The Stripe ``payment_method_types`` for a mode. Empty list ⇒ no payable surface."""
    return list(_RAILS.get(mode, _RAILS[DEFAULT_MODE]))


def collects_by_stripe(mode: str) -> bool:
    """Whether this mode has a payable Stripe surface at all."""
    return bool(rails_for_mode(mode))


async def get_agreement_payment_mode(conn) -> str:
    """The operator's selected mode, or ``DEFAULT_MODE`` when unset/unrecognized.

    FAIL-SAFE TOWARD THE SHIPPED BEHAVIOR: a missing row, a malformed blob, or an unknown string all
    resolve to ``card-ach``. A settings read is never allowed to take the payment surface down — and
    the alternative (defaulting to ``remittance``) would silently strand prospects on a terminal
    screen that promises an email the platform may not yet send.
    """
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT value FROM gc.operator_settings WHERE key = %(key)s",
                {"key": SETTINGS_KEY},
            )
            row = await cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        logger.warning("agreement-payment mode read failed (%s); using %s", exc, DEFAULT_MODE)
        return DEFAULT_MODE
    if not row or row[0] is None:
        return DEFAULT_MODE
    blob = row[0]
    # psycopg may hand back jsonb already-decoded (dict) or as text depending on the adapter set.
    if isinstance(blob, (str, bytes, bytearray)):
        try:
            blob = json.loads(blob)
        except (ValueError, TypeError):
            return DEFAULT_MODE
    if not isinstance(blob, dict):
        return DEFAULT_MODE
    mode = blob.get("mode")
    return mode if isinstance(mode, str) and mode in _VALID else DEFAULT_MODE
