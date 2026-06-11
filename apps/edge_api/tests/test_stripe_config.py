"""Stripe key resolution — the mode-suffixed Doppler convention (STRIPE_*_LIVE/_TEST + STRIPE_MODE).

core-x/prd carries STRIPE_SECRET_KEY_LIVE/_TEST + STRIPE_PUBLISHABLE_KEY_LIVE/_TEST (NOT the bare
names). Pins that edge_api resolves them by STRIPE_MODE, defaults to 'test', and still honors a bare
override if some env sets it directly.
"""
from __future__ import annotations

from apps.edge_api.src import config


def test_test_mode_resolves_test_keys(monkeypatch) -> None:
    monkeypatch.setenv("STRIPE_MODE", "test")
    monkeypatch.setenv("STRIPE_SECRET_KEY_TEST", "sk_test_x")
    monkeypatch.setenv("STRIPE_PUBLISHABLE_KEY_TEST", "pk_test_x")
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    monkeypatch.delenv("STRIPE_PUBLISHABLE_KEY", raising=False)
    assert config.stripe_secret_key() == "sk_test_x"
    assert config.stripe_publishable_key() == "pk_test_x"


def test_live_mode_resolves_live_keys(monkeypatch) -> None:
    monkeypatch.setenv("STRIPE_MODE", "live")
    monkeypatch.setenv("STRIPE_SECRET_KEY_LIVE", "sk_live_x")
    monkeypatch.setenv("STRIPE_SECRET_KEY_TEST", "sk_test_x")
    assert config.stripe_secret_key() == "sk_live_x"


def test_default_mode_is_test(monkeypatch) -> None:
    monkeypatch.delenv("STRIPE_MODE", raising=False)
    monkeypatch.setenv("STRIPE_SECRET_KEY_TEST", "sk_test_default")
    monkeypatch.setenv("STRIPE_SECRET_KEY_LIVE", "sk_live_default")
    assert config.stripe_mode() == "test"
    assert config.stripe_secret_key() == "sk_test_default"


def test_bare_name_fallback(monkeypatch) -> None:
    monkeypatch.setenv("STRIPE_MODE", "test")
    monkeypatch.delenv("STRIPE_SECRET_KEY_TEST", raising=False)
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_bare")
    assert config.stripe_secret_key() == "sk_bare"
