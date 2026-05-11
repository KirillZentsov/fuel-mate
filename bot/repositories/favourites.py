"""
Repository for app.favourites.

Spec section 7.5 + CLAUDE.md rule: "Don't UPSERT favourites — use proper
INSERT and check count for the 3/3 limit."

Why not UPSERT: we want explicit signal when the user is at the limit,
so the bot can present a "replace one" dialog (spec section 7.5 — replace
flow). UPSERT would silently overwrite which is the wrong UX.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from bot import config
from bot.db import get_pool


@dataclass
class FavouriteRow:
    """Joined row: favourite + its station's display data + current price."""
    user_id: int
    station_id: str
    name: Optional[str]
    brand: Optional[str]
    postcode: Optional[str]
    is_24h: bool
    is_temp_closed: bool
    # Current price for the user's chosen fuel — may be None if fuel
    # is not sold there or pricing data is missing.
    current_price_pence: Optional[Decimal]
    # Snapshot from the last alert (for delta calculation in cards).
    last_notified_pence: Optional[Decimal]


# ──────────────────────────────────────────────────────────────────────
# Read
# ──────────────────────────────────────────────────────────────────────

async def list_for_user(user_id: int, fuel_type: str) -> list[FavouriteRow]:
    """
    Return all favourites for a user, joined with their station and current
    price for the user's fuel. Sorted by created_at ASC (oldest first).

    Stations that no longer exist in mart.stations are excluded — see spec
    section 10.5 'Favourite station deleted from gov.uk data'.
    """
    pool = get_pool()
    # Pick the right last_notified_* column based on fuel_type.
    # We can't parametrise column names in SQL — use a CASE expression.
    rows = await pool.fetch(
        """
        SELECT
            f.user_id,
            f.station_id,
            s.name,
            s.brand,
            s.postcode,
            COALESCE(s.is_24h, FALSE)         AS is_24h,
            COALESCE(s.is_temp_closed, FALSE) AS is_temp_closed,
            pc.price_pence                     AS current_price_pence,
            CASE $2::text
                WHEN 'e10' THEN f.last_notified_e10
                WHEN 'e5'  THEN f.last_notified_e5
                WHEN 'b7s' THEN f.last_notified_b7s
                WHEN 'b7p' THEN f.last_notified_b7p
            END                                AS last_notified_pence
        FROM app.favourites f
        JOIN mart.stations  s  ON s.station_id = f.station_id
        LEFT JOIN mart.prices_current pc
            ON pc.station_id = f.station_id AND pc.fuel_type = $2
        WHERE f.user_id = $1
        ORDER BY f.created_at ASC
        """,
        user_id, fuel_type,
    )
    return [FavouriteRow(**dict(r)) for r in rows]


async def count_for_user(user_id: int) -> int:
    """How many favourites this user has. Used for limit checks in handlers."""
    pool = get_pool()
    n = await pool.fetchval(
        "SELECT count(*) FROM app.favourites WHERE user_id = $1",
        user_id,
    )
    return int(n or 0)


async def is_favourite(user_id: int, station_id: str) -> bool:
    """Is this station already tracked? Used to flip the Track/Remove button."""
    pool = get_pool()
    n = await pool.fetchval(
        """
        SELECT 1 FROM app.favourites
        WHERE user_id = $1 AND station_id = $2
        """,
        user_id, station_id,
    )
    return n is not None


# ──────────────────────────────────────────────────────────────────────
# Mutate
# ──────────────────────────────────────────────────────────────────────

async def add(user_id: int, station_id: str) -> bool:
    """
    Add a station to favourites. Returns True if added, False if the user
    is already at the 3/3 limit.

    Note: the caller (handler) checks `is_favourite()` first to avoid
    a unique-constraint violation on duplicates. We additionally guard
    the limit here as a defence in depth.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Advisory lock on user_id serialises concurrent `add` calls from
            # the same user (rare, but possible if they double-tap "Track").
            # The lock auto-releases on transaction end. Other users can
            # add favourites in parallel — only same-user is blocked.
            await conn.execute("SELECT pg_advisory_xact_lock($1)", user_id)

            current = await conn.fetchval(
                "SELECT count(*) FROM app.favourites WHERE user_id = $1",
                user_id,
            )
            if current >= config.MAX_FAVOURITES:
                return False
            await conn.execute(
                """
                INSERT INTO app.favourites (user_id, station_id)
                VALUES ($1, $2)
                ON CONFLICT (user_id, station_id) DO NOTHING
                """,
                user_id, station_id,
            )
            return True


async def remove(user_id: int, station_id: str) -> None:
    """Remove a single station from favourites. No-op if not present."""
    pool = get_pool()
    await pool.execute(
        "DELETE FROM app.favourites WHERE user_id = $1 AND station_id = $2",
        user_id, station_id,
    )


async def replace(user_id: int, old_station_id: str, new_station_id: str) -> None:
    """
    Atomically swap one favourite for another.

    Used by the "Your favourites are full (3/3) — replace one?" dialog
    (spec section 10.5). DELETE + INSERT in one transaction so we never
    end up with 2 or 4 favourites due to a partial failure.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM app.favourites WHERE user_id = $1 AND station_id = $2",
                user_id, old_station_id,
            )
            await conn.execute(
                """
                INSERT INTO app.favourites (user_id, station_id)
                VALUES ($1, $2)
                """,
                user_id, new_station_id,
            )


# ──────────────────────────────────────────────────────────────────────
# Cleanup
# ──────────────────────────────────────────────────────────────────────

async def remove_orphans(user_id: int) -> int:
    """
    Delete favourites pointing to stations that no longer exist in mart.

    Spec section 10.5: 'Favourite station deleted from gov.uk data — skip
    from display, send notice'. The notice is the handler's job; this
    function just does the DELETE.

    Returns the number of orphans removed.
    """
    pool = get_pool()
    result = await pool.execute(
        """
        DELETE FROM app.favourites f
        WHERE f.user_id = $1
          AND NOT EXISTS (
              SELECT 1 FROM mart.stations s WHERE s.station_id = f.station_id
          )
        """,
        user_id,
    )
    # asyncpg returns "DELETE N" — parse the count
    parts = result.split()
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return 0
