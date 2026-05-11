"""
Parser for the UK government's fuel-prices CSV.

CSV source:
    https://www.fuel-finder.service.gov.uk/internal/v1.0.2/csv/get-latest-fuel-prices-csv

The CSV has ~68 columns with deeply nested names like
'forecourts.opening_times.usual_days.monday.open_time'. Our database schema
is much simpler. This module bridges the two: it reads the CSV row by row
and yields ParsedStation objects shaped to match staging.stations + staging.prices.

Design choices:
  - Stream parsing (csv.DictReader). No bulk-load into RAM.
  - Each row → one ParsedStation. Each ParsedStation contains a list of
    ParsedPrice objects (0-4 prices, one per fuel sold).
  - Dataclasses for type-safety and IDE-helpfulness.
  - All timestamps converted to timezone-aware UTC datetimes.
  - Booleans use a tri-state: True / False / None (unknown).
  - Fuels limited to e10, e5, b7s, b7p — matching FUEL_TYPES in spec section 11.3.
    B10 and HVO ignored (not exposed to users).

Used by:
  - etl.load_staging — loads parsed records into staging.stations & staging.prices
"""
from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterator

log = logging.getLogger(__name__)


# Fuels we care about. Keys are CSV column suffixes (uppercase, as in CSV).
# Values are the lowercase codes we store in the database (matching
# app.users.fuel_type values: 'e10', 'e5', 'b7s', 'b7p').
_FUELS: dict[str, str] = {
    "E10": "e10",
    "E5":  "e5",
    "B7S": "b7s",
    "B7P": "b7p",
    # 'B10' and 'HVO' deliberately omitted — not user-selectable in MVP.
}

_DAYS = (
    "monday", "tuesday", "wednesday", "thursday",
    "friday", "saturday", "sunday",
)


# ──────────────────────────────────────────────────────────────────────
# Output dataclasses — shape matches staging.stations / staging.prices
# ──────────────────────────────────────────────────────────────────────

@dataclass
class ParsedPrice:
    """A single fuel price for a station, ready for staging.prices insert."""
    station_id: str
    fuel_type: str           # one of: 'e10', 'e5', 'b7s', 'b7p'
    price_pence: Decimal     # NUMERIC(6,1) compatible
    forecourt_updated_at: datetime | None  # per-fuel timestamp, UTC


@dataclass
class ParsedStation:
    """A single station, ready for staging.stations insert + child prices."""
    station_id: str
    name: str | None
    brand: str | None
    postcode: str | None
    address: str | None
    city: str | None
    latitude: float | None
    longitude: float | None
    is_supermarket: bool | None
    is_24h: bool | None
    is_temp_closed: bool | None
    is_perm_closed: bool | None
    opening_hours: dict       # JSONB-shape — see _build_opening_hours()
    amenities: dict           # JSONB-shape — see _build_amenities()
    forecourt_updated_at: datetime | None
    prices: list[ParsedPrice] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────

def parse_fuel_csv(path: str | Path) -> Iterator[ParsedStation]:
    """
    Stream-parse a fuel-prices CSV file. Yields one ParsedStation per row.

    Bad rows (missing station_id, malformed coordinates, etc.) are logged
    and skipped — they don't stop the iterator. This is by design: a
    daily ETL run shouldn't fail because one of 8000 rows has a typo.

    Args:
        path: filesystem path to the CSV file (gzipped not supported here —
              the caller is expected to decompress first).

    Yields:
        ParsedStation, one per non-skipped row.
    """
    path = Path(path)
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        # Sanity check — the gov.uk format we know has these key columns.
        # If they're missing, the format probably changed and we should
        # bail loudly rather than silently producing nonsense.
        required = {"forecourts.node_id", "forecourt_update_timestamp"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"CSV is missing required columns: {missing}. "
                f"Has the gov.uk format changed?"
            )

        for line_num, row in enumerate(reader, start=2):  # line 1 is header
            try:
                station = _row_to_station(row)
                if station is None:
                    continue
                yield station
            except Exception as exc:  # noqa: BLE001 — defensive parser
                # Don't let one bad row kill the whole ETL run.
                # Log with line number so we can grep the raw CSV later.
                log.warning(
                    "Skipping CSV line %d due to parse error: %s",
                    line_num, exc,
                )
                continue


# ──────────────────────────────────────────────────────────────────────
# Row → ParsedStation
# ──────────────────────────────────────────────────────────────────────

def _row_to_station(row: dict[str, str]) -> ParsedStation | None:
    """Convert one CSV row into a ParsedStation. Returns None to skip."""
    station_id = (row.get("forecourts.node_id") or "").strip()
    if not station_id:
        return None  # silently skip rows with no ID

    # Concatenate two address lines, dropping empties
    address = _join_nonempty(
        row.get("forecourts.location.address_line_1"),
        row.get("forecourts.location.address_line_2"),
        sep=", ",
    )

    opening_hours = _build_opening_hours(row)
    amenities = _build_amenities(row)
    prices = list(_extract_prices(row, station_id))

    return ParsedStation(
        station_id=station_id,
        name=_str_or_none(row.get("forecourts.trading_name")),
        brand=_str_or_none(row.get("forecourts.brand_name")),
        postcode=_str_or_none(row.get("forecourts.location.postcode")),
        address=address,
        city=_str_or_none(row.get("forecourts.location.city")),
        latitude=_parse_float(row.get("forecourts.location.latitude")),
        longitude=_parse_float(row.get("forecourts.location.longitude")),
        is_supermarket=_parse_bool(row.get("forecourts.is_supermarket_service_station")),
        is_24h=_derive_is_24h(opening_hours),
        is_temp_closed=_parse_bool(row.get("forecourts.temporary_closure")),
        is_perm_closed=_parse_bool(row.get("forecourts.permanent_closure")),
        opening_hours=opening_hours,
        amenities=amenities,
        forecourt_updated_at=_parse_js_timestamp(row.get("forecourt_update_timestamp")),
        prices=prices,
    )


def _extract_prices(row: dict[str, str], station_id: str) -> Iterator[ParsedPrice]:
    """
    Yield up to 4 ParsedPrice objects (one per fuel with a non-empty price).

    Each fuel has 3 columns in the CSV:
        forecourts.fuel_price.<FUEL>                         — pence (decimal)
        forecourts.price_submission_timestamp.<FUEL>         — when retailer entered it
        forecourts.price_change_effective_timestamp.<FUEL>   — when it took effect

    We use the 'effective' timestamp (decision: per-fuel forecourt_updated_at
    in staging.prices). Submission timestamp is discarded for MVP — could be
    added later as a separate column if useful for analytics.
    """
    for csv_fuel, db_fuel in _FUELS.items():
        price_str = row.get(f"forecourts.fuel_price.{csv_fuel}")
        price = _parse_decimal(price_str)
        if price is None:
            continue  # station doesn't sell this fuel
        ts = _parse_js_timestamp(
            row.get(f"forecourts.price_change_effective_timestamp.{csv_fuel}")
        )
        yield ParsedPrice(
            station_id=station_id,
            fuel_type=db_fuel,
            price_pence=price,
            forecourt_updated_at=ts,
        )


# ──────────────────────────────────────────────────────────────────────
# Opening hours: 21 CSV cols → JSONB-friendly dict
# ──────────────────────────────────────────────────────────────────────

def _build_opening_hours(row: dict[str, str]) -> dict:
    """
    Convert the 21 per-day CSV columns into a structured dict:

        {
          "monday":    {"open": "07:00", "close": "22:00", "is_24h": false},
          ...
          "sunday":    {...},
          "bank_holiday": {...} | None,
        }

    Special case: when open_time, close_time and is_24_hours are all the
    'no data' values (00:00:00, 00:00:00, False) we store None for that day.
    Without this, a station that simply hasn't reported its hours would
    look 'closed all week' to the bot.
    """
    result: dict[str, dict | None] = {}

    for day in _DAYS:
        prefix = f"forecourts.opening_times.usual_days.{day}"
        open_t = row.get(f"{prefix}.open_time", "")
        close_t = row.get(f"{prefix}.close_time", "")
        is_24 = _parse_bool(row.get(f"{prefix}.is_24_hours"))

        result[day] = _normalise_day(open_t, close_t, is_24)

    # Bank holidays — same shape, but stored under one key
    bh_prefix = "forecourts.opening_times.bank_holiday.standard"
    result["bank_holiday"] = _normalise_day(
        row.get(f"{bh_prefix}.open_time", ""),
        row.get(f"{bh_prefix}.close_time", ""),
        _parse_bool(row.get(f"{bh_prefix}.is_24_hours")),
    )

    return result


def _normalise_day(open_t: str, close_t: str, is_24: bool | None) -> dict | None:
    """Normalise one day's three fields into the storage format, or None."""
    open_t = (open_t or "").strip()
    close_t = (close_t or "").strip()
    is_24h_flag = bool(is_24)

    # 'No data' sentinel: open/close both 00:00:00 and not flagged 24h
    if open_t in ("", "00:00:00") and close_t in ("", "00:00:00") and not is_24h_flag:
        return None

    return {
        # Drop the seconds — 'HH:MM' is what users will see
        "open":   _trim_seconds(open_t),
        "close":  _trim_seconds(close_t),
        "is_24h": is_24h_flag,
    }


def _trim_seconds(time_str: str) -> str:
    """'07:00:00' -> '07:00'. Empty input -> empty string."""
    if not time_str:
        return ""
    parts = time_str.split(":")
    if len(parts) >= 2:
        return f"{parts[0]}:{parts[1]}"
    return time_str


def _derive_is_24h(opening_hours: dict) -> bool | None:
    """
    A station is 24h if all 7 weekdays are flagged is_24h=True.
    Bank holidays not considered (they're a separate thing).
    Returns None if we have no data for any day.
    """
    weekday_data = [opening_hours.get(d) for d in _DAYS]
    if all(d is None for d in weekday_data):
        return None  # truly no data
    return all(
        isinstance(d, dict) and d.get("is_24h") is True
        for d in weekday_data
    )


# ──────────────────────────────────────────────────────────────────────
# Amenities: 8 CSV cols → flat dict
# ──────────────────────────────────────────────────────────────────────

def _build_amenities(row: dict[str, str]) -> dict:
    """
    Flatten the nested 'forecourts.amenities.*' columns into a flat dict.

    Field-name mapping decisions:
      - 'air_pump_or_screenwash' is renamed to 'air_pump' for brevity.
        Most users associate this with tyre air pump; CSV combines both
        but we just store the boolean as-is.
      - 'fuel_and_energy_services.*' fields are flattened: kept names like
        adblue_pumps, adblue_packaged, lpg_pumps without the prefix.
      - 'vehicle_services.car_wash' stored as 'car_wash'.
    """
    return {
        "customer_toilets":      _parse_bool_default_false(row.get("forecourts.amenities.customer_toilets")),
        "car_wash":              _parse_bool_default_false(row.get("forecourts.amenities.vehicle_services.car_wash")),
        "air_pump":              _parse_bool_default_false(row.get("forecourts.amenities.air_pump_or_screenwash")),
        "water_filling":         _parse_bool_default_false(row.get("forecourts.amenities.water_filling")),
        "twenty_four_hour_fuel": _parse_bool_default_false(row.get("forecourts.amenities.twenty_four_hour_fuel")),
        "adblue_pumps":          _parse_bool_default_false(row.get("forecourts.amenities.fuel_and_energy_services.adblue_pumps")),
        "adblue_packaged":       _parse_bool_default_false(row.get("forecourts.amenities.fuel_and_energy_services.adblue_packaged")),
        "lpg_pumps":             _parse_bool_default_false(row.get("forecourts.amenities.fuel_and_energy_services.lpg_pumps")),
    }


# ──────────────────────────────────────────────────────────────────────
# Low-level parsing helpers
# ──────────────────────────────────────────────────────────────────────

def _str_or_none(value: str | None) -> str | None:
    """Strip and return, or None if empty."""
    if value is None:
        return None
    s = value.strip()
    return s if s else None


def _join_nonempty(*parts: str | None, sep: str = ", ") -> str | None:
    """Join non-empty parts with sep. Returns None if nothing to join."""
    cleaned = [p.strip() for p in parts if p and p.strip()]
    return sep.join(cleaned) if cleaned else None


def _parse_bool(value: str | None) -> bool | None:
    """
    Parse a tri-state boolean. The CSV uses 'True', 'False', or '' (unknown).
    """
    if value is None:
        return None
    s = value.strip()
    if s == "True":
        return True
    if s == "False":
        return False
    if s == "":
        return None
    # Unexpected value — be strict and return None rather than guessing
    log.debug("Unexpected boolean value: %r", value)
    return None


def _parse_bool_default_false(value: str | None) -> bool:
    """Like _parse_bool but treats unknown/empty as False — used for amenities
    where 'unknown' is functionally equivalent to 'not present'."""
    parsed = _parse_bool(value)
    return parsed if parsed is True else False


def _parse_float(value: str | None) -> float | None:
    """Parse a coordinate. Returns None on empty or malformed input."""
    if value is None or not value.strip():
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _parse_decimal(value: str | None) -> Decimal | None:
    """
    Parse a price in pence to Decimal. Returns None on empty / malformed.

    We use Decimal (not float) to avoid binary-rounding errors when storing
    in NUMERIC(6,1). asyncpg passes Decimal through losslessly.
    """
    if value is None or not value.strip():
        return None
    try:
        return Decimal(value.strip())
    except (ValueError, ArithmeticError):
        return None


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    """
    Parse an ISO-style timestamp like '2026-04-27 15:56:05'.

    The forecourt_update_timestamp field doesn't carry a timezone; per spec
    section 5.3 we assume UTC.
    """
    if value is None or not value.strip():
        return None
    try:
        # Replace ' ' with 'T' so fromisoformat is happy
        s = value.strip().replace(" ", "T")
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    # Attach UTC if naive (we assume UTC per spec)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# Pattern for the JS Date.toString() format used in per-fuel timestamps.
# Example input: 'Mon Apr 27 2026 14:58:00 GMT+0000 (Coordinated Universal Time)'
# We only need the first 24 characters: 'Mon Apr 27 2026 14:58:00'
_JS_TS_FORMAT = "%a %b %d %Y %H:%M:%S"
_JS_TS_LEN = 24


def _parse_js_timestamp(value: str | None) -> datetime | None:
    """
    Parse a JavaScript-style Date.toString() timestamp.

    Format: 'Mon Apr 27 2026 14:58:00 GMT+0000 (Coordinated Universal Time)'
    We strip the trailing GMT+ZZZZ and timezone-name suffix (always UTC in
    practice for this CSV) and parse the leading 24 characters.
    """
    if value is None or not value.strip():
        return None
    s = value.strip()
    if len(s) < _JS_TS_LEN:
        return None
    try:
        dt = datetime.strptime(s[:_JS_TS_LEN], _JS_TS_FORMAT)
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc)
