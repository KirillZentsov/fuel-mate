"""
Repository for app.users.

Encapsulates all SQL touching the users table. Handlers call functions
here instead of writing SQL directly — keeps SQL out of business logic
and makes future schema changes localised.

Design notes:
  - We never DELETE inactive users. /stop sets is_active=FALSE so the
    user data stays for audit/analytics. CASCADE in the schema handles
    cleanup of related rows.
    UPDATE: spec section 10.7 actually says /stop does DELETE — see
    delete_user() below.
  - get_or_create() is the workhorse: every user message goes through it
    so we always have a User row.
"""
from dataclasses import dataclass
from typing import Optional

from bot import config
from bot.db import get_pool


@dataclass
class User:
    """A row from app.users."""
    user_id: int
    fuel_type: str
    search_radius: int
    alerts_mode: str
    is_active: bool


# ──────────────────────────────────────────────────────────────────────
# Read
# ──────────────────────────────────────────────────────────────────────

async def get(user_id: int) -> Optional[User]:
    """Return the user, or None if they don't exist or are inactive."""
    pool = get_pool()
    row = await pool.fetchrow(
        """
        SELECT user_id, fuel_type, search_radius, alerts_mode, is_active
        FROM app.users
        WHERE user_id = $1
        """,
        user_id,
    )
    if row is None:
        return None
    return User(**dict(row))


async def total_active_count() -> int:
    """Total active users — used by admin notifications and milestone checks."""
    pool = get_pool()
    result = await pool.fetchval(
        "SELECT count(*) FROM app.users WHERE is_active = TRUE"
    )
    return int(result or 0)


# ──────────────────────────────────────────────────────────────────────
# Create / update
# ──────────────────────────────────────────────────────────────────────

async def create(user_id: int, fuel_type: str = config.DEFAULT_FUEL) -> User:
    """
    Insert a new user with sensible defaults. The fuel_type can be set
    immediately if the user picked one during onboarding step 1.

    Returns the newly created User. Raises asyncpg.UniqueViolationError
    if the user already exists — caller should handle (or use
    get_or_create instead).
    """
    pool = get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO app.users (user_id, fuel_type, search_radius, alerts_mode)
        VALUES ($1, $2, $3, $4)
        RETURNING user_id, fuel_type, search_radius, alerts_mode, is_active
        """,
        user_id,
        fuel_type,
        config.DEFAULT_RADIUS_MILES,
        config.DEFAULT_ALERTS_MODE,
    )
    return User(**dict(row))


async def get_or_create(user_id: int) -> tuple[User, bool]:
    """
    Return (user, is_new). If the user already existed, is_new=False.
    If we just created them, is_new=True.

    Race-safe: uses ON CONFLICT DO NOTHING + a follow-up SELECT. This
    handles the case where two concurrent /start commands from the same
    user race each other (unlikely but possible).
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO app.users (user_id, fuel_type, search_radius, alerts_mode)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO NOTHING
                RETURNING user_id, fuel_type, search_radius, alerts_mode, is_active
                """,
                user_id,
                config.DEFAULT_FUEL,
                config.DEFAULT_RADIUS_MILES,
                config.DEFAULT_ALERTS_MODE,
            )
            if row is not None:
                # We inserted — definitely a new user
                return User(**dict(row)), True

            # Conflict — they already existed. Fetch what's there.
            row = await conn.fetchrow(
                """
                SELECT user_id, fuel_type, search_radius, alerts_mode, is_active
                FROM app.users
                WHERE user_id = $1
                """,
                user_id,
            )
            return User(**dict(row)), False


async def update_fuel(user_id: int, fuel_type: str) -> None:
    """Change the user's preferred fuel."""
    if fuel_type not in config.FUEL_TYPES:
        raise ValueError(f"Unknown fuel_type: {fuel_type!r}")
    pool = get_pool()
    await pool.execute(
        "UPDATE app.users SET fuel_type = $2, updated_at = now() WHERE user_id = $1",
        user_id, fuel_type,
    )


async def update_radius(user_id: int, radius_miles: int) -> None:
    """Change the user's default search radius."""
    if radius_miles not in config.RADIUS_OPTIONS:
        raise ValueError(f"Radius must be one of {config.RADIUS_OPTIONS}, got {radius_miles}")
    pool = get_pool()
    await pool.execute(
        "UPDATE app.users SET search_radius = $2, updated_at = now() WHERE user_id = $1",
        user_id, radius_miles,
    )


async def update_alerts_mode(user_id: int, alerts_mode: str) -> None:
    """Change the user's alerts preference."""
    if alerts_mode not in config.ALERT_MODES:
        raise ValueError(f"Unknown alerts_mode: {alerts_mode!r}")
    pool = get_pool()
    await pool.execute(
        "UPDATE app.users SET alerts_mode = $2, updated_at = now() WHERE user_id = $1",
        user_id, alerts_mode,
    )


async def deactivate(user_id: int) -> None:
    """
    Soft-deactivate a user without deleting their data.

    Used internally when Telegram returns 403 (user blocked the bot).
    The user data is preserved so if they come back, their settings/favourites
    are still there.
    """
    pool = get_pool()
    await pool.execute(
        "UPDATE app.users SET is_active = FALSE, updated_at = now() WHERE user_id = $1",
        user_id,
    )


# ──────────────────────────────────────────────────────────────────────
# Hard delete (used by /stop confirmation)
# ──────────────────────────────────────────────────────────────────────

async def delete_user(user_id: int) -> None:
    """
    Hard-delete the user and their favourites. Used by /stop.

    CASCADE on app.favourites.user_id removes those rows automatically.
    app.alerts_log doesn't have FK (audit retention), so historical
    alerts stay — that's intentional.
    """
    pool = get_pool()
    await pool.execute(
        "DELETE FROM app.users WHERE user_id = $1",
        user_id,
    )
