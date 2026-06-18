"""Unit guards for the startup schema-apply runner (``src/migrate.py``). No network, no DB — the
end-to-end self-heal / idempotency / advisory-lock / fail-loud behavior is exercised against a real
Postgres in the PR's verification (a throwaway docker DB + a prod idempotent no-op), not in CI.
"""
from __future__ import annotations

import asyncio

from apps.edge_api.src import migrate


def test_sql_files_sorted_and_discovered() -> None:
    """Deterministic apply order = filename ascending, and the real DDL files are found (so a newly
    committed sql/*.sql is auto-applied with no list to maintain)."""
    files = migrate.sql_files()
    assert files == sorted(files), "apply order must be deterministic (filename ascending)"
    names = {p.name for p in files}
    assert "document_payments.sql" in names, "the rail-outage file must be in the apply set"
    assert all(p.suffix == ".sql" for p in files)


def test_apply_lock_key_is_a_valid_advisory_lock_arg() -> None:
    """pg_advisory_lock takes a signed bigint; the key must fit and be stable/non-zero."""
    assert 0 < migrate._APPLY_LOCK_KEY < 2**63


def test_skip_flag_short_circuits_without_touching_a_db(monkeypatch) -> None:
    """EDGE_API_SKIP_DB_MIGRATE=1 is the escape hatch: run_migrations must return immediately and never
    open a connection (no DSN env set here, so a connect attempt would raise — proving it short-circuits)."""
    monkeypatch.setenv("EDGE_API_SKIP_DB_MIGRATE", "1")
    monkeypatch.delenv("HQX_DB_URL_DIRECT", raising=False)
    monkeypatch.delenv("HQX_DB_URL_POOLED", raising=False)
    asyncio.run(migrate.run_migrations())  # returns cleanly; no exception, no connection
