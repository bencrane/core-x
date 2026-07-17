"""Async Postgres pool for catalyst_api market routers — the SAME database hq-x uses.

Copied from hq-x ``app/db.py``. Reads ``HQX_DB_URL_POOLED`` from the environment
(Doppler ``core-x/prd``) — the Supabase transaction-pooler DSN (port 6543), so
prepared statements are disabled (``prepare_threshold=None``). edge_api writes the
``business.agent_runs`` ledger to the identical rows hq-x reads — compute
relocation, not data migration.
"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from psycopg_pool import AsyncConnectionPool

_pool: AsyncConnectionPool | None = None


async def init_pool() -> None:
    global _pool
    if _pool is not None:
        return
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError(
            "HQX_DB_URL_POOLED not set — edge_api cannot reach the database."
        )
    # Supabase pooled connections (port 6543) run in transaction-pooling mode,
    # which is incompatible with psycopg3's prepared-statement cache.
    _pool = AsyncConnectionPool(
        conninfo=dsn,
        min_size=2,
        max_size=10,
        open=False,
        kwargs={"prepare_threshold": None},
    )
    await _pool.open()
    await _pool.wait()


async def close_pool() -> None:
    global _pool
    if _pool is None:
        return
    await _pool.close()
    _pool = None


@asynccontextmanager
async def get_db_connection() -> AsyncIterator:
    if _pool is None:
        raise RuntimeError("DB pool is not initialized")
    async with _pool.connection() as conn:
        yield conn
