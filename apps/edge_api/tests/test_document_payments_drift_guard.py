"""Regression: a schema-drift DB error on the document-payment surface must degrade to a clean 503,
never leak as a raw 500. No network — the DB layer is faked.

THE OUTAGE THIS PINS: the ``rail`` column was added to the committed DDL (PR #518) but never applied
to prod, so ``SELECT … rail FROM business.document_payments`` raised ``undefined_column`` → unhandled →
FastAPI 500 on EVERY mint + state poll. The direct-to-documenso payment surface went fully dark.

The durable fix is the startup schema apply (``src/migrate.py``). This guard is the seatbelt: the two
public endpoints wrap their DB work in ``_payments_db``, which converts the schema-drift error family
(undefined column/table/function, invalid schema) into a retryable 503 — and ONLY that family, so an
unrelated DB fault is never masked as "temporarily unavailable".
"""
from __future__ import annotations

import asyncio
import contextlib

import psycopg
import pytest
from fastapi import HTTPException

from apps.edge_api.src.routers import document_payments_v1 as mod


class _FakeConn:
    """Sentinel — the faked queries never actually touch it."""


def _fake_get_db_connection():
    @contextlib.asynccontextmanager
    async def _cm():
        yield _FakeConn()

    return _cm()


def test_get_state_degrades_to_503_on_missing_column(monkeypatch) -> None:
    """The exact outage: ``get_payment`` hits a column the live schema lacks → 503, not 500."""
    monkeypatch.setattr(mod, "get_db_connection", _fake_get_db_connection)

    async def _boom(conn, document_id):
        raise psycopg.errors.UndefinedColumn('column "rail" does not exist')

    monkeypatch.setattr(mod.pay_queries, "get_payment", _boom)

    with pytest.raises(HTTPException) as ei:
        asyncio.run(mod.get_document_payment_state("opp12345", "9999"))
    assert ei.value.status_code == 503
    assert "unavailable" in ei.value.detail.lower()


def test_get_state_degrades_to_503_on_missing_table(monkeypatch) -> None:
    """A dropped/never-created table is the same drift class → 503."""
    monkeypatch.setattr(mod, "get_db_connection", _fake_get_db_connection)

    async def _boom(conn, document_id):
        raise psycopg.errors.UndefinedTable('relation "business.document_payments" does not exist')

    monkeypatch.setattr(mod.pay_queries, "get_payment", _boom)

    with pytest.raises(HTTPException) as ei:
        asyncio.run(mod.get_document_payment_state("opp12345", "9999"))
    assert ei.value.status_code == 503


def test_get_state_non_drift_error_is_not_masked(monkeypatch) -> None:
    """A NON-drift fault (e.g. the pool can't reach the DB) must NOT be dressed up as a 503 — the guard
    is surgical, so an operational failure still surfaces as itself (→ 500) rather than a misleading
    'temporarily unavailable' that hides a real outage."""
    monkeypatch.setattr(mod, "get_db_connection", _fake_get_db_connection)

    async def _boom(conn, document_id):
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(mod.pay_queries, "get_payment", _boom)

    with pytest.raises(psycopg.OperationalError):
        asyncio.run(mod.get_document_payment_state("opp12345", "9999"))


def test_get_state_ok_passes_through(monkeypatch) -> None:
    """Happy path is untouched: a healthy read returns the state model verbatim (incl. the rail)."""
    monkeypatch.setattr(mod, "get_db_connection", _fake_get_db_connection)

    async def _ok(conn, document_id):
        return {
            "opportunity_id": "opp12345",
            "payment_status": "succeeded",
            "amount_cents": 7_500_000,
            "currency": "usd",
            "paid_at": None,
            "rail": "us_bank_account",
        }

    monkeypatch.setattr(mod.pay_queries, "get_payment", _ok)

    out = asyncio.run(mod.get_document_payment_state("opp12345", "9999"))
    assert out.payment_status == "succeeded"
    assert out.rail == "us_bank_account"
    assert out.amount_cents == 7_500_000
