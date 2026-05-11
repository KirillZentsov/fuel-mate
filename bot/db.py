"""
Database connection pool for the bot.

A single asyncpg.Pool is created at startup and shared across all handlers
via the get_pool() accessor. The pool handles concurrent queries from
parallel handler invocations (each Telegram message becomes a coroutine).

Pool sizing:
  - min_size=2  — keeps 2 connections warm even when idle (low cold-start tax).
  - max_size=10 — Supabase Free tier allows 60 concurrent direct connections;
                  a single bot won't get near this. 10 is plenty.

Failure mode:
  - On startup, init_pool() retries 3 times with backoff before giving up.
  - On query failure (network blip), asyncpg auto-reconnects under the hood.
  - On hard DB outage, the pool raises asyncpg.InterfaceError; handlers
    catch this and surface a friendly message. We do NOT crash the bot.
"""
import asyncio
import json
import logging
from typing import Optional

import asyncpg

from bot import config

log = logging.getLogger(__name__)

# Module-level singleton. Initialized once on bot startup.
_pool: Optional[asyncpg.Pool] = None


async def init_pool() -> asyncpg.Pool:
    """
    Create the pool. Called once from bot.main on startup.

    Retries on failure — the bot is useless without a DB, so we'd rather
    crash on startup than try to limp along.
    """
    global _pool
    if _pool is not None:
        return _pool

    last_error: Exception | None = None
    backoffs = [2, 5, 15]

    for attempt, backoff in enumerate(backoffs, start=1):
        try:
            _pool = await asyncpg.create_pool(
                config.DATABASE_URL,
                min_size=2,
                max_size=10,
                # Default command timeout — long enough for slow Supabase
                # queries on free tier, short enough to fail fast.
                command_timeout=30,
                init=_register_codecs,
            )
            log.info("Database pool initialised (min=2, max=10).")
            return _pool
        except (asyncpg.PostgresError, OSError) as exc:
            last_error = exc
            log.warning(
                "Pool init attempt %d/%d failed: %s", attempt, len(backoffs), exc,
            )
            if attempt < len(backoffs):
                await asyncio.sleep(backoff)

    raise RuntimeError(f"Could not connect to database after retries: {last_error}")


def get_pool() -> asyncpg.Pool:
    """
    Return the active pool. Raises if init_pool() hasn't been called.

    Repositories use this; they don't take pool as an argument because
    that would clutter every signature. The trade-off is that get_pool()
    is implicit global state — but it's a singleton with one well-defined
    lifecycle, so this is fine for the project size.
    """
    if _pool is None:
        raise RuntimeError(
            "Database pool not initialised. Call init_pool() at startup."
        )
    return _pool


async def close_pool() -> None:
    """Cleanly close the pool. Called from bot.main on shutdown."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        log.info("Database pool closed.")

async def _register_codecs(conn: asyncpg.Connection) -> None:
    """
    Register custom codecs on each new connection.

    asyncpg by default returns JSONB columns as strings (raw JSON text).
    We register codecs to auto-decode JSONB into Python dicts (and lists)
    on read, and auto-encode dicts to JSON on write.

    This runs once per new connection in the pool.
    """
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    # Also register for plain `json` type, in case we ever use it
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
