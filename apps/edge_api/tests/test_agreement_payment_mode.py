"""Agreement-payment mode — the operator's post-signature collection selection.

Pins three invariants, no network and no DB:

1. RAIL DERIVATION. Each mode maps to the exact Stripe ``payment_method_types`` the mint may use, and
   ``remittance`` maps to NO rails (the mint must refuse rather than create an unpayable intent).

2. IDEMPOTENCY-KEY NAMESPACING BY RAILS (availability regression, same class as the fee-change bug in
   ``test_document_payment_idempotency``). Changing the mode cancels a rail-stale intent and falls
   through to a fresh mint. Without the rail set in the key, that mint replays a BURNED key with
   different ``payment_method_types`` — Stripe answers 400 idempotency_error → 502, wedging the payment
   surface for the full 24h key TTL. The suffix must therefore change with the selection while staying
   stable for every mint under one selection.

3. FAIL-SAFE RESOLUTION. A missing row, malformed blob, or unrecognized string resolves to the shipped
   dual-rail default — never to ``remittance``, which would silently strand prospects on a terminal
   screen promising an email the platform may not send.
"""
from __future__ import annotations

import asyncio

from apps.edge_api.src import agreement_payment_mode as mode_mod
from apps.edge_api.src.routers.document_payments_v1 import _mint_idempotency_key

_DOC = "doc_abc123"
_BASE = "pay_document_doc_abc123"


# ── 1. rail derivation ────────────────────────────────────────────────────────────────────────────


def test_card_ach_mints_both_rails() -> None:
    assert mode_mod.rails_for_mode("card-ach") == ["card", "us_bank_account"]


def test_ach_only_omits_the_card_rail() -> None:
    """The point of the setting: a disabled rail is ABSENT from the intent, not hidden in the browser."""
    assert mode_mod.rails_for_mode("ach-only") == ["us_bank_account"]
    assert "card" not in mode_mod.rails_for_mode("ach-only")


def test_card_only_omits_the_bank_rail() -> None:
    assert mode_mod.rails_for_mode("card-only") == ["card"]
    assert "us_bank_account" not in mode_mod.rails_for_mode("card-only")


def test_remittance_has_no_payable_rails() -> None:
    assert mode_mod.rails_for_mode("remittance") == []
    assert mode_mod.collects_by_stripe("remittance") is False


def test_every_stripe_mode_reports_payable() -> None:
    for m in ("card-ach", "ach-only", "card-only"):
        assert mode_mod.collects_by_stripe(m) is True


def test_unknown_mode_falls_back_to_the_default_rails() -> None:
    """An unrecognized string must not silently yield an empty rail list (which reads as remittance)."""
    assert mode_mod.rails_for_mode("nonsense") == mode_mod.rails_for_mode(mode_mod.DEFAULT_MODE)
    assert mode_mod.collects_by_stripe("nonsense") is True


def test_rails_are_a_copy_not_the_shared_table() -> None:
    """Callers must not be able to mutate the module's rail table through a returned list."""
    got = mode_mod.rails_for_mode("card-ach")
    got.append("sepa_debit")
    assert mode_mod.rails_for_mode("card-ach") == ["card", "us_bank_account"]


# ── 2. idempotency-key namespacing by rails ───────────────────────────────────────────────────────


def test_key_without_rails_is_unchanged() -> None:
    """Back-compat: the 3-arg form keeps the historic key exactly (the existing suite asserts it)."""
    assert _mint_idempotency_key(_DOC, "none", None) == _BASE


def test_distinct_rail_sets_yield_distinct_keys() -> None:
    """THE REGRESSION GUARD: a mode change must not replay the key burned under the previous rails."""
    card_ach = _mint_idempotency_key(_DOC, "none", None, ["card", "us_bank_account"])
    ach_only = _mint_idempotency_key(_DOC, "none", None, ["us_bank_account"])
    card_only = _mint_idempotency_key(_DOC, "none", None, ["card"])
    assert len({card_ach, ach_only, card_only}) == 3


def test_key_is_stable_for_a_given_rail_set() -> None:
    """Idempotent within one selection — a double-submit must return the SAME intent, not duplicate it."""
    a = _mint_idempotency_key(_DOC, "none", None, ["card", "us_bank_account"])
    b = _mint_idempotency_key(_DOC, "none", None, ["card", "us_bank_account"])
    assert a == b


def test_key_is_insensitive_to_rail_ORDER() -> None:
    """Stripe does not promise list order back, so the namespace must be order-free — otherwise the same
    selection could produce two keys and mint two intents."""
    assert _mint_idempotency_key(_DOC, "none", None, ["card", "us_bank_account"]) == (
        _mint_idempotency_key(_DOC, "none", None, ["us_bank_account", "card"])
    )


def test_failed_retry_still_namespaces_by_prior_intent_with_rails() -> None:
    """The two namespaces compose: a hard-failure retry under a rail set is distinct from both a
    pristine mint on those rails and a retry on different rails."""
    retry = _mint_idempotency_key(_DOC, "failed", "pi_1", ["us_bank_account"])
    assert "pi_1" in retry
    assert retry != _mint_idempotency_key(_DOC, "none", None, ["us_bank_account"])
    assert retry != _mint_idempotency_key(_DOC, "failed", "pi_1", ["card"])


# ── 3. fail-safe resolution ───────────────────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    async def execute(self, *_a, **_k):
        return None

    async def fetchone(self):
        return self._row

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False


class _FakeConn:
    """Minimal stand-in for the pooled psycopg connection: one row, or an raising cursor."""

    def __init__(self, row=None, raises: Exception | None = None):
        self._row = row
        self._raises = raises

    def cursor(self):
        if self._raises:
            raise self._raises
        return _FakeCursor(self._row)


def _resolve(conn) -> str:
    return asyncio.run(mode_mod.get_agreement_payment_mode(conn))


def test_no_row_resolves_to_default() -> None:
    assert _resolve(_FakeConn(row=None)) == mode_mod.DEFAULT_MODE


def test_null_value_resolves_to_default() -> None:
    assert _resolve(_FakeConn(row=(None,))) == mode_mod.DEFAULT_MODE


def test_dict_blob_resolves() -> None:
    assert _resolve(_FakeConn(row=({"mode": "remittance"},))) == "remittance"


def test_json_text_blob_resolves() -> None:
    """psycopg may hand jsonb back as text depending on the adapter set — both shapes must work."""
    assert _resolve(_FakeConn(row=('{"mode": "ach-only"}',))) == "ach-only"


def test_malformed_json_resolves_to_default() -> None:
    assert _resolve(_FakeConn(row=("{not json",))) == mode_mod.DEFAULT_MODE


def test_non_object_blob_resolves_to_default() -> None:
    assert _resolve(_FakeConn(row=("[1,2,3]",))) == mode_mod.DEFAULT_MODE


def test_unrecognized_mode_string_resolves_to_default() -> None:
    assert _resolve(_FakeConn(row=({"mode": "bitcoin"},))) == mode_mod.DEFAULT_MODE


def test_non_string_mode_resolves_to_default() -> None:
    assert _resolve(_FakeConn(row=({"mode": 7},))) == mode_mod.DEFAULT_MODE


def test_db_failure_resolves_to_default_never_raises() -> None:
    """A settings read must never take the payment surface down — degrade to the shipped behavior."""
    assert _resolve(_FakeConn(raises=RuntimeError("pool exhausted"))) == mode_mod.DEFAULT_MODE


def test_default_is_a_payable_mode() -> None:
    """Load-bearing: every fail-safe path above lands here, so the default must NOT be remittance."""
    assert mode_mod.DEFAULT_MODE == "card-ach"
    assert mode_mod.collects_by_stripe(mode_mod.DEFAULT_MODE) is True
