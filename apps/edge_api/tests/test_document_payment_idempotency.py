"""Regression: the document-payment mint must not reuse the fixed idempotency key on a retry after a
hard failure. No network, no DB — pins the pure key-derivation invariant.

Bug (availability): a ``failed``/``canceled`` row skips the reuse block in the mint and falls straight
to ``create_payment_intent``. The prior failed intent already burned ``pay_document_{document_id}``
against ITS amount, so if the operator edits ``fee_amount`` before the retry, replaying that fixed key
with a new amount is a Stripe 400 idempotency_error → 502, wedged for the full 24h key TTL. Stripe
rejects the mismatched replay (it never charges a stale amount), so this is availability, not money.

Fix: ``_mint_idempotency_key`` namespaces the key by the prior intent id on the failed/canceled path —
fresh across the fee change, yet stable within a single retry burst.
"""
from __future__ import annotations

from apps.edge_api.src.routers.document_payments_v1 import _mint_idempotency_key

_DOC = "doc_abc123"
_BASE = "pay_document_doc_abc123"


def test_pristine_pair_uses_fixed_key() -> None:
    """First mint (no prior intent) keys off the document id alone — Stripe idempotency dedupes retries."""
    assert _mint_idempotency_key(_DOC, "none", None) == _BASE


def test_open_intent_falls_through_to_fixed_key() -> None:
    """An open (non-failed) status mints with the base key — a same-amount replay returns the same intent."""
    assert _mint_idempotency_key(_DOC, "requires_payment", "pi_open") == _BASE


def test_failed_retry_namespaces_key_by_prior_intent() -> None:
    """The regression guard: a failed retry must NOT reuse the fixed key (else a fee edit → 400 → 502)."""
    key = _mint_idempotency_key(_DOC, "failed", "pi_failed_1")
    assert key != _BASE
    assert "pi_failed_1" in key


def test_canceled_retry_namespaces_key_by_prior_intent() -> None:
    key = _mint_idempotency_key(_DOC, "canceled", "pi_canceled_1")
    assert key != _BASE
    assert "pi_canceled_1" in key


def test_distinct_failure_generations_yield_distinct_keys() -> None:
    """Each failure generation gets its own key namespace, so a fee change after each failure can never
    collide with a spent key."""
    assert _mint_idempotency_key(_DOC, "failed", "pi_gen1") != _mint_idempotency_key(
        _DOC, "failed", "pi_gen2"
    )


def test_same_failed_intent_is_stable_within_a_retry_burst() -> None:
    """Idempotent within a burst: a double-submit keying off the same persisted failed intent yields the
    same key, so Stripe returns the SAME new intent rather than duplicating the charge surface."""
    assert _mint_idempotency_key(_DOC, "failed", "pi_x") == _mint_idempotency_key(
        _DOC, "failed", "pi_x"
    )


def test_failed_without_intent_falls_back_to_fixed_key() -> None:
    """Defensive: a failed status with no persisted intent (shouldn't occur — status only turns
    failed/canceled via a webhook on a minted intent) degrades to the base key, never a crash."""
    assert _mint_idempotency_key(_DOC, "failed", None) == _BASE
