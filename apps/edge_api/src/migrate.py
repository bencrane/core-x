"""Startup schema apply — re-run the committed DDL (``sql/*.sql``) against ``HQX_DB_URL_POOLED`` on boot.

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

MECHANICS: each file is sent as a SINGLE script via psycopg's simple-query protocol — ``cur.execute(sql)``
with NO params. That is what lets a multi-statement file (including dollar-quoted ``DO $$ … $$`` blocks)
parse on the server intact; a naive split on ``;`` would shred those blocks. Each file runs in its own
transaction (applies whole or not at all). Files apply in filename order; the suite targets an
ALREADY-PROVISIONED control plane — cross-file FKs reference upstream-owned tables (``business.organizations``,
``business.documenso_templates``) that already exist in prod — not a bare database.
"""
from __future__ import annotations

import logging
from pathlib import Path

from . import config
from .db import get_db_connection

log = logging.getLogger("edge_api.migrate")

# ``src/migrate.py`` → parent ``src`` → parent ``apps/edge_api`` → ``sql/`` (baked into the image by the
# Dockerfile's ``COPY apps/edge_api apps/edge_api``).
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def sql_files() -> list[Path]:
    """The committed DDL files, in deterministic apply order (filename ascending)."""
    return sorted(SQL_DIR.glob("*.sql"))


async def run_migrations() -> None:
    """Apply every ``sql/*.sql`` to ``HQX_DB_URL_POOLED`` in filename order. Re-raises on the first
    failure so the caller (the FastAPI lifespan) fails the boot loudly."""
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

    log.info("startup DDL apply: applying %d file(s) from %s", len(files), SQL_DIR)
    async with get_db_connection() as conn:
        for path in files:
            sql = path.read_text()
            try:
                # Own transaction per file (whole-or-nothing). NO params → psycopg simple-query
                # protocol → the entire multi-statement file (incl. DO $$ … $$) runs server-side.
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(sql)
            except Exception:
                log.exception("startup DDL apply FAILED on %s — failing boot", path.name)
                raise
            log.debug("startup DDL apply: %s ok", path.name)
    log.info("startup DDL apply: %d file(s) applied OK — live schema in sync with committed DDL", len(files))
