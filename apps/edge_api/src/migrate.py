"""Startup schema apply — re-run the committed DDL (``sql/*.sql``) against the control-plane Postgres on boot.

WHY THIS EXISTS: edge_api has no migration framework. The ``sql/*.sql`` files were applied to the
control-plane Postgres BY HAND. A column added to a committed file (``document_payments.rail``, PR
#518) was never applied to prod, so a ``SELECT … rail FROM business.document_payments`` raised
``undefined_column`` → unhandled → FastAPI 500 on every document-payment read (mint + state poll). The
direct-to-documenso payment surface went fully dark with "PAYMENT IS TEMPORARILY UNAVAILABLE".

This closes the drift window. The DDL is idempotent (``CREATE … IF NOT EXISTS`` / ``ADD COLUMN IF NOT
EXISTS`` / guarded ``DO``-block constraints / ``ON CONFLICT DO NOTHING``), so re-applying it on every
boot is safe and SELF-HEALING: whatever a deploy's code expects, the live schema is brought to match
*before* the version serves traffic. A newly committed ``sql/*.sql`` file is auto-discovered (sorted
glob) and applied on the next deploy with zero extra wiring — there is no list to keep in sync.

FAIL-LOUD: a failure here re-raises out of the FastAPI lifespan, so the boot fails and the new
container never goes healthy. Railway keeps the PRIOR healthy deployment on traffic rather than
promoting a version whose code outruns the live schema — that is the whole point (a red deploy beats a
500-storm). Emergency escape hatch for an unrelated hotfix: ``EDGE_API_SKIP_DB_MIGRATE=1`` (see
``config.db_migrate_on_boot``).

CONCURRENCY: the apply runs over the DIRECT (session-mode, port 5432) DSN — ``HQX_DB_URL_DIRECT`` — NOT
the request pool's transaction-pooler DSN (port 6543). A rolling deploy boots replicas concurrently; a
single Postgres SESSION advisory lock (``pg_advisory_lock``) serializes them, so two replicas never run
``CREATE``/``ALTER`` against the same object at once (no ``tuple concurrently updated`` /
``relation already exists`` races). The lock MUST be held on a session-mode connection: on the
transaction pooler a session lock can unlock on a different backend than it locked, leaking the lock —
so the lock is taken ONLY over the direct DSN. If the connection dies mid-apply the lock is
auto-released with the session, so a crash cannot wedge the next boot. Local dev (no
``HQX_DB_URL_DIRECT``) falls back to the pooled DSN and SKIPS the lock — a single dev process has no
replica to race.

MECHANICS: each file is sent as a SINGLE script via psycopg's simple-query protocol — ``cur.execute(sql)``
with NO params — so a multi-statement file (including dollar-quoted ``DO $$ … $$`` blocks) parses on the
server intact; a naive split on ``;`` would shred those blocks. Each file runs in its own transaction
(applies whole or not at all). Files apply in filename order; the suite targets an ALREADY-PROVISIONED
control plane — cross-file FKs reference upstream-owned tables (``business.organizations``,
``business.documenso_templates``) that already exist in prod — not a bare database.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import psycopg

from . import config

log = logging.getLogger("edge_api.migrate")

# ``src/migrate.py`` → parent ``src`` → parent ``apps/edge_api`` → ``sql/`` (baked into the image by the
# Dockerfile's ``COPY apps/edge_api apps/edge_api``).
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# Fixed, edge_api-specific 64-bit key for the boot-time apply's session advisory lock. ASCII "edge"
# (0x65646765) — any value works as long as it is stable and unique to this apply; concurrent boots
# block on pg_advisory_lock(this) and apply one-at-a-time.
_APPLY_LOCK_KEY = 0x65646765


def sql_files() -> list[Path]:
    """The committed DDL files, in deterministic apply order (filename ascending)."""
    return sorted(SQL_DIR.glob("*.sql"))


async def _apply_all(dsn: str, files: list[Path], *, advisory_lock: bool) -> None:
    """Apply ``files`` over a dedicated ``dsn`` connection, optionally under a session advisory lock.

    The testable core: opens its OWN short-lived connection (not the request pool), so it has no boot
    ordering dependency and the lock lives on a real session. Re-raises on the first failure.
    """
    # autocommit so the advisory-lock acquire/release run at session scope (outside any txn) and persist
    # across the per-file transactions below; each file still gets its own explicit transaction.
    conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    try:
        if advisory_lock:
            await conn.execute("SELECT pg_advisory_lock(%s)", (_APPLY_LOCK_KEY,))
            log.info("startup DDL apply: session advisory lock %s acquired", _APPLY_LOCK_KEY)
        try:
            for path in files:
                sql = path.read_text()
                if not sql.strip():
                    continue
                try:
                    # Own transaction per file (whole-or-nothing). NO params → simple-query protocol →
                    # the entire multi-statement file (incl. DO $$ … $$) runs server-side intact.
                    async with conn.transaction():
                        async with conn.cursor() as cur:
                            await cur.execute(sql)
                except Exception:
                    log.exception("startup DDL apply FAILED on %s — failing boot", path.name)
                    raise
                log.debug("startup DDL apply: %s ok", path.name)
        finally:
            if advisory_lock:
                await conn.execute("SELECT pg_advisory_unlock(%s)", (_APPLY_LOCK_KEY,))
    finally:
        await conn.close()


async def run_migrations() -> None:
    """Apply every ``sql/*.sql`` to the control-plane Postgres in filename order, before serving.

    Prefers the DIRECT (session-mode) DSN so the apply can hold a real session advisory lock; falls back
    to the pooled DSN (no lock) for local dev. Re-raises on the first failure so the caller (the FastAPI
    lifespan) fails the boot loudly.
    """
    if not config.db_migrate_on_boot():
        log.warning(
            "EDGE_API_SKIP_DB_MIGRATE set — startup DDL apply SKIPPED. The live schema may drift from "
            "committed sql/*.sql; unset and redeploy to re-sync."
        )
        return

    files = sql_files()
    if not files:
        log.warning("startup DDL apply: no sql/*.sql found under %s — nothing to apply", SQL_DIR)
        return

    direct = os.environ.get("HQX_DB_URL_DIRECT")
    dsn = direct or os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError(
            "Neither HQX_DB_URL_DIRECT nor HQX_DB_URL_POOLED is set — edge_api cannot apply schema."
        )
    if not direct:
        log.warning(
            "HQX_DB_URL_DIRECT unset — applying schema over the pooled DSN WITHOUT a session advisory "
            "lock (local dev). Set HQX_DB_URL_DIRECT in every deployed environment for the "
            "concurrent-replica guard."
        )

    log.info(
        "startup DDL apply: applying %d file(s) from %s (advisory-lock=%s)",
        len(files),
        SQL_DIR,
        bool(direct),
    )
    await _apply_all(dsn, files, advisory_lock=bool(direct))
    log.info(
        "startup DDL apply: %d file(s) applied OK — live schema in sync with committed DDL", len(files)
    )
