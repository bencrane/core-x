"""Startup schema apply (``src/migrate.py``) — the durable fix for code↔schema drift.

Two layers:
  * PURE (always run): the skip-flag short-circuit and file discovery/ordering.
  * INTEGRATION (only when ``EDGE_API_TEST_DB_DSN`` points at a throwaway Postgres): proves the runner's
    load-bearing contract against a real server — a multi-statement file (including a dollar-quoted
    ``DO $$ … $$`` block, which a naive split on ';' would shred) applies as one script, and a second
    pass is a clean no-op (idempotent / self-healing). Spin one up with:
        docker run -d -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test -p 55432:5432 postgres:16-alpine
        EDGE_API_TEST_DB_DSN=postgresql://postgres:test@127.0.0.1:55432/test pytest apps/edge_api/tests/test_migrate.py
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from apps.edge_api.src import config, db, migrate

_TEST_DSN = os.environ.get("EDGE_API_TEST_DB_DSN")


# ── PURE ──────────────────────────────────────────────────────────────────────────────────────────
def test_skip_flag_short_circuits_without_touching_db(monkeypatch) -> None:
    """EDGE_API_SKIP_DB_MIGRATE is the escape hatch: run_migrations must return WITHOUT acquiring a
    connection (so an unrelated hotfix can ship even if the DB is unreachable)."""
    monkeypatch.setenv("EDGE_API_SKIP_DB_MIGRATE", "1")
    assert config.db_migrate_on_boot() is False

    def _explode():
        raise AssertionError("run_migrations must not touch the DB when skipping")

    monkeypatch.setattr(migrate, "get_db_connection", _explode)
    asyncio.run(migrate.run_migrations())  # returns cleanly


def test_skip_flag_default_is_enabled(monkeypatch) -> None:
    monkeypatch.delenv("EDGE_API_SKIP_DB_MIGRATE", raising=False)
    assert config.db_migrate_on_boot() is True


def test_sql_files_discovered_sorted_and_nonempty() -> None:
    """The apply set is auto-discovered (sorted glob) — a newly committed sql/*.sql is picked up with
    zero wiring. Pins that discovery resolves the real dir, is sorted, and includes the file the outage
    was about."""
    files = migrate.sql_files()
    assert files, "expected committed sql/*.sql files to be discovered"
    assert files == sorted(files)
    names = {p.name for p in files}
    assert "document_payments.sql" in names
    assert all(p.suffix == ".sql" for p in files)


# ── INTEGRATION (requires a throwaway Postgres) ─────────────────────────────────────────────────────
_SYNTHETIC = {
    # multi-statement: schema + table + additive column, all idempotent
    "001_basic.sql": (
        "CREATE SCHEMA IF NOT EXISTS migtest;\n"
        "CREATE TABLE IF NOT EXISTS migtest.t (id integer PRIMARY KEY);\n"
        "ALTER TABLE migtest.t ADD COLUMN IF NOT EXISTS extra text;\n"
    ),
    # dollar-quoted DO block (semicolons INSIDE the body) + guarded constraint — the exact pattern the
    # real files use; mis-handling the ';' split would break this on the server.
    "002_doblock.sql": (
        "CREATE INDEX IF NOT EXISTS migtest_t_extra_idx ON migtest.t (extra);\n"
        "DO $$ BEGIN\n"
        "    IF NOT EXISTS (\n"
        "        SELECT 1 FROM pg_constraint WHERE conname = 'migtest_t_extra_chk'\n"
        "    ) THEN\n"
        "        ALTER TABLE migtest.t ADD CONSTRAINT migtest_t_extra_chk CHECK (extra IS NULL OR extra <> '');\n"
        "    END IF;\n"
        "END $$;\n"
    ),
}


async def _apply_twice_and_verify(tmp_dir: Path) -> tuple[bool, int]:
    orig_dir = migrate.SQL_DIR
    migrate.SQL_DIR = tmp_dir
    try:
        await db.init_pool()
        await migrate.run_migrations()   # pass 1 — fresh
        await migrate.run_migrations()   # pass 2 — must be a clean no-op (idempotent)
        async with db.get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema='migtest' AND table_name='t' AND column_name='extra'"
                )
                has_col = (await cur.fetchone()) is not None
                await cur.execute("SELECT count(*) FROM pg_constraint WHERE conname='migtest_t_extra_chk'")
                n_constraints = (await cur.fetchone())[0]
        return has_col, n_constraints
    finally:
        await db.close_pool()
        migrate.SQL_DIR = orig_dir


@pytest.mark.skipif(not _TEST_DSN, reason="set EDGE_API_TEST_DB_DSN to a throwaway Postgres to run")
def test_runner_applies_multistatement_and_is_idempotent(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("EDGE_API_SKIP_DB_MIGRATE", raising=False)
    monkeypatch.setenv("HQX_DB_URL_POOLED", _TEST_DSN)
    for name, body in _SYNTHETIC.items():
        (tmp_path / name).write_text(body)
    has_col, n_constraints = asyncio.run(_apply_twice_and_verify(tmp_path))
    assert has_col, "additive column from a multi-statement file must be applied"
    assert n_constraints == 1, "DO-block constraint must apply exactly once across two passes (idempotent)"
