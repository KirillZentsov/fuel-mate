"""
Repository for mart.stations + mart.prices_current.

This is the read-heavy part of the bot — every "Find Fuel" search and
every detail card hits these tables.

Performance approach for radius search:
  1. Filter in SQL by latitude/longitude bounding box (cheap, indexed).
  2. Compute exact haversine distance in Python (precise).
  3. Filter to within the requested radius and sort.

We don't use PostGIS — Supabase Free doesn't guarantee it and the extra
dependency isn't worth it for a pre-filter. Bounding-box + haversine is
the canonical pattern for this problem at our scale.

For an 8000-row dataset and a 15-mile radius, the bounding box typically
returns 50-200 candidates; haversine on those is sub-millisecond.
"""
import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from bot import config
from bot.db import get_pool
from shared.geo import haversine_miles


@dataclass
class StationResult:
    """A station + price returned from a search, with distance computed."""
    station_id: str
    name: Optional[str]
    brand: Optional[str]
    postcode: Optional[str]
    address: Optional[str]
    city: Optional[str]
    latitude: float
    longitude: float
    is_24h: bool
    is_temp_closed: bool
    price_pence: Decimal
    price_updated_at: Optional[datetime]
    distance_miles: float


@dataclass
class StationDetail:
    """Full station data for the detail card (single station lookup)."""
    station_id: str
    name: Optional[str]
    brand: Optional[str]
    postcode: Optional[str]
    address: Optional[str]
    city: Optional[str]
    latitude: float
    longitude: float
    is_24h: bool
    is_temp_closed: bool
    is_perm_closed: bool
    opening_hours: dict
    amenities: dict
    price_pence: Optional[Decimal]
    price_updated_at: Optional[datetime]


# ──────────────────────────────────────────────────────────────────────
# Search by coordinates
# ──────────────────────────────────────────────────────────────────────

async def search_within_radius(
    latitude: float,
    longitude: float,
    radius_miles: int,
    fuel_type: str,
    limit: int = config.MAX_RESULTS,
) -> list[StationResult]:
    """
    Find stations selling `fuel_type` within `radius_miles` of (lat, lon).

    Returns up to `limit` results, sorted by distance ascending. Stations
    with permanent closure are excluded; temp_closed are kept so the bot
    can show them (sorted to bottom by handlers if needed).

    Stations with no price for the chosen fuel are excluded — a station
    that doesn't sell your fuel isn't useful to show (spec section 10.4).
    """
    # Step 1: bounding box for SQL pre-filter.
    # 1 degree of latitude ≈ 69 miles. Longitude varies by latitude.
    lat_delta = radius_miles / 69.0
    # cos() saves us from showing too-distant stations at high latitudes.
    # max(0.1, ...) avoids div-by-zero near the poles (we won't see those, but defensive).
    lon_delta = radius_miles / (69.0 * max(0.1, math.cos(math.radians(latitude))))

    pool = get_pool()
    rows = await pool.fetch(
        """
        SELECT
            s.station_id, s.name, s.brand, s.postcode, s.address, s.city,
            s.latitude, s.longitude,
            COALESCE(s.is_24h, FALSE)         AS is_24h,
            COALESCE(s.is_temp_closed, FALSE) AS is_temp_closed,
            pc.price_pence,
            pc.forecourt_updated_at AS price_updated_at
        FROM mart.stations s
        JOIN mart.prices_current pc
            ON pc.station_id = s.station_id AND pc.fuel_type = $5
        WHERE COALESCE(s.is_perm_closed, FALSE) = FALSE
          AND s.latitude  BETWEEN $1::float - $3::float AND $1::float + $3::float
          AND s.longitude BETWEEN $2::float - $4::float AND $2::float + $4::float
        """,
        latitude, longitude, lat_delta, lon_delta, fuel_type,
    )

    # Step 2: compute exact distance, filter to radius, sort
    results: list[StationResult] = []
    for r in rows:
        dist = haversine_miles(latitude, longitude, r["latitude"], r["longitude"])
        if dist > radius_miles:
            continue
        results.append(StationResult(
            station_id=r["station_id"],
            name=r["name"],
            brand=r["brand"],
            postcode=r["postcode"],
            address=r["address"],
            city=r["city"],
            latitude=r["latitude"],
            longitude=r["longitude"],
            is_24h=r["is_24h"],
            is_temp_closed=r["is_temp_closed"],
            price_pence=r["price_pence"],
            price_updated_at=r["price_updated_at"],
            distance_miles=dist,
        ))

    results.sort(key=lambda r: r.distance_miles)
    return results[:limit]


# ──────────────────────────────────────────────────────────────────────
# Lookup by ID
# ──────────────────────────────────────────────────────────────────────

async def get_by_id(station_id: str, fuel_type: str) -> Optional[StationDetail]:
    """
    Fetch full station details for the detail card. Includes the current
    price for the user's fuel.
    """
    pool = get_pool()
    row = await pool.fetchrow(
        """
        SELECT
            s.station_id, s.name, s.brand, s.postcode, s.address, s.city,
            s.latitude, s.longitude,
            COALESCE(s.is_24h, FALSE)         AS is_24h,
            COALESCE(s.is_temp_closed, FALSE) AS is_temp_closed,
            COALESCE(s.is_perm_closed, FALSE) AS is_perm_closed,
            s.opening_hours, s.amenities,
            pc.price_pence,
            pc.forecourt_updated_at AS price_updated_at
        FROM mart.stations s
        LEFT JOIN mart.prices_current pc
            ON pc.station_id = s.station_id AND pc.fuel_type = $2
        WHERE s.station_id = $1
        """,
        station_id, fuel_type,
    )
    if row is None:
        return None
    return StationDetail(**dict(row))


async def find_by_id_prefix(prefix: str, fuel_type: str) -> Optional[StationDetail]:
    """
    Look up a station by station_id prefix.

    Used when the bot receives a callback_data like 'det:4882e3fee979cfef:...'.
    Telegram caps callback_data at 64 bytes, so we use the first 16 hex chars
    of the station_id (SHA-256 hash). Collision probability with 8000
    stations is effectively zero (16 hex chars = 64 bits = 1.8e19 possibilities).

    If somehow there ARE multiple matches, returns None — the handler should
    fall back to asking the user to retry. Better to be safe than to show
    the wrong station.
    """
    pool = get_pool()
    rows = await pool.fetch(
        """
        SELECT s.station_id
        FROM mart.stations s
        WHERE s.station_id LIKE $1 || '%'
        LIMIT 2
        """,
        prefix,
    )
    if len(rows) != 1:
        return None
    return await get_by_id(rows[0]["station_id"], fuel_type)


# ──────────────────────────────────────────────────────────────────────
# Data freshness check
# ──────────────────────────────────────────────────────────────────────

async def latest_data_age_days() -> Optional[int]:
    """
    Return age of the most recently loaded forecourt data, in days.

    Used for the 'Data may be outdated' banner (spec section 10.2).
    Returns None if there's no data at all (fresh DB).
    """
    pool = get_pool()
    age_days = await pool.fetchval(
        """
        SELECT EXTRACT(DAY FROM (now() - MAX(forecourt_updated_at)))::int
        FROM mart.stations
        WHERE forecourt_updated_at IS NOT NULL
        """
    )
    return int(age_days) if age_days is not None else None
