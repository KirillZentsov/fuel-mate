"""
Stage 5 of the ETL pipeline: refresh the mart layer from latest staging.

This is the operational layer the bot reads from. The contract:
  - mart.stations:        latest snapshot per station_id (SCD type 1, UPSERT)
  - mart.prices_current:  latest price per (station_id, fuel_type) (UPSERT)
  - mart.prices_history:  one row per actual price change (append-only)

All four SQL statements run in a single transaction. If any fails, the
whole mart stays at the previous state — no half-applied refresh.

Reference: spec section 6.7.
"""
import logging
from dataclasses import dataclass

import asyncpg

from etl import config

log = logging.getLogger(__name__)


@dataclass
class RefreshResult:
    """Output of the mart refresh stage."""
    stations_upserted: int      # rows affected in mart.stations
    prices_changed: int         # rows inserted into mart.prices_history
    current_upserted: int       # rows affected in mart.prices_current


async def refresh_mart(dump_id: int) -> RefreshResult:
    """
    Pull data from staging for the given dump_id into mart tables.

    Args:
        dump_id: BIGINT id of the just-loaded dump (from raw.fuel_data_dumps).

    Returns:
        RefreshResult with row counts.

    Raises:
        asyncpg.PostgresError on any failure. Caller decides how to handle.
    """
    if config.DRY_RUN:
        log.info("DRY_RUN — skipping mart refresh.")
        return RefreshResult(stations_upserted=0, prices_changed=0, current_upserted=0)

    conn: asyncpg.Connection = await asyncpg.connect(config.DATABASE_URL)
    try:
        async with conn.transaction():
            stations_n = await _upsert_stations(conn, dump_id)
            changes_n  = await _insert_price_changes(conn, dump_id)
            current_n  = await _upsert_prices_current(conn, dump_id)
    finally:
        await conn.close()

    log.info(
        "Mart refresh: stations upserted=%d, prices changed=%d, current upserted=%d",
        stations_n, changes_n, current_n,
    )
    return RefreshResult(
        stations_upserted=stations_n,
        prices_changed=changes_n,
        current_upserted=current_n,
    )


# ──────────────────────────────────────────────────────────────────────
# Per-statement helpers
# ──────────────────────────────────────────────────────────────────────

async def _upsert_stations(conn: asyncpg.Connection, dump_id: int) -> int:
    """
    Replace mart.stations rows from the latest dump.

    DISTINCT ON (station_id) protects us if a single dump somehow contains
    duplicate station_ids — we keep the most recently loaded row.
    """
    result = await conn.execute(
        """
        INSERT INTO mart.stations (
            station_id, name, brand, postcode, address, city,
            latitude, longitude,
            is_supermarket, is_24h, is_temp_closed, is_perm_closed,
            opening_hours, amenities,
            forecourt_updated_at, last_updated_at
        )
        SELECT DISTINCT ON (station_id)
            station_id, name, brand, postcode, address, city,
            latitude, longitude,
            COALESCE(is_supermarket, FALSE),
            COALESCE(is_24h, FALSE),
            COALESCE(is_temp_closed, FALSE),
            COALESCE(is_perm_closed, FALSE),
            opening_hours, amenities,
            forecourt_updated_at, now()
        FROM staging.stations
        WHERE dump_id = $1
        ORDER BY station_id, loaded_at DESC
        ON CONFLICT (station_id) DO UPDATE SET
            name                 = EXCLUDED.name,
            brand                = EXCLUDED.brand,
            postcode             = EXCLUDED.postcode,
            address              = EXCLUDED.address,
            city                 = EXCLUDED.city,
            latitude             = EXCLUDED.latitude,
            longitude            = EXCLUDED.longitude,
            is_supermarket       = EXCLUDED.is_supermarket,
            is_24h               = EXCLUDED.is_24h,
            is_temp_closed       = EXCLUDED.is_temp_closed,
            is_perm_closed       = EXCLUDED.is_perm_closed,
            opening_hours        = EXCLUDED.opening_hours,
            amenities            = EXCLUDED.amenities,
            forecourt_updated_at = EXCLUDED.forecourt_updated_at,
            last_updated_at      = now()
        """,
        dump_id,
    )
    return _affected(result)


async def _insert_price_changes(conn: asyncpg.Connection, dump_id: int) -> int:
    """
    Append to mart.prices_history only when the price actually changed.

    Compare against mart.prices_current (the previous snapshot). A row goes
    in history if:
      - the (station, fuel) is new (no current row)
      - or the price differs from the current value

    NOTE: at this point in the transaction, mart.stations has already been
    upserted, so the FK constraint mart.prices_history.station_id is safe.
    """
    result = await conn.execute(
        """
        INSERT INTO mart.prices_history (
            station_id, fuel_type, price_pence,
            forecourt_updated_at, dump_id
        )
        SELECT
            sp.station_id, sp.fuel_type, sp.price_pence,
            sp.forecourt_updated_at, sp.dump_id
        FROM staging.prices sp
        LEFT JOIN mart.prices_current pc
            ON pc.station_id = sp.station_id
           AND pc.fuel_type  = sp.fuel_type
        WHERE sp.dump_id = $1
          AND (pc.price_pence IS NULL OR pc.price_pence <> sp.price_pence)
        """,
        dump_id,
    )
    return _affected(result)


async def _upsert_prices_current(conn: asyncpg.Connection, dump_id: int) -> int:
    """
    Refresh mart.prices_current to reflect the latest dump.

    Note: a station might lose a fuel between dumps (e.g. ran out of E5).
    For MVP we don't delete those — they stay in prices_current until the
    next dump that does include the fuel. Acceptable: the bot filters
    on prices_current.last_updated_at indirectly through the search query,
    and stale fuel prices appear with a warning banner anyway.
    """
    result = await conn.execute(
        """
        INSERT INTO mart.prices_current (
            station_id, fuel_type, price_pence,
            forecourt_updated_at, last_updated_at
        )
        SELECT
            station_id, fuel_type, price_pence,
            forecourt_updated_at, now()
        FROM staging.prices
        WHERE dump_id = $1
        ON CONFLICT (station_id, fuel_type) DO UPDATE SET
            price_pence          = EXCLUDED.price_pence,
            forecourt_updated_at = EXCLUDED.forecourt_updated_at,
            last_updated_at      = now()
        """,
        dump_id,
    )
    return _affected(result)


def _affected(result_str: str) -> int:
    """
    Parse asyncpg's command tag like 'INSERT 0 50' into the row count.

    asyncpg's execute() returns the postgres command tag as a string:
      - 'INSERT 0 50'   for INSERT (the 0 is the OID, ignored)
      - 'UPDATE 50'     for UPDATE
    For ON CONFLICT...DO UPDATE inserts, postgres reports as INSERT.
    """
    parts = result_str.split()
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return 0
