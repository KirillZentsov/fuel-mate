"""
Parsers for fuel-prices data — both CSV (legacy) and JSON (current API).

Two data paths feed the same downstream pipeline:

1. **JSON API** (current, primary):
   gov.uk Fuel Finder API at fuel-finder.service.gov.uk returns two
   paginated JSON endpoints — `/api/v1/pfs` (station metadata) and
   `/api/v1/pfs/fuel-prices` (prices). Used by ETL in production.

2. **CSV file** (legacy, kept for offline use):
   The old `internal/v1.0.2/csv/get-latest-fuel-prices-csv` endpoint
   was killed by gov.uk's WAF in early 2026. We still parse CSVs for
   manual replay (e.g. testing against an old gov.uk file), so the
   CSV path is preserved.

Both paths produce identical `ParsedStation` + `ParsedPrice` objects,
so `etl.load_staging` doesn't care which input was used.

Design decisions retained from the CSV-only era:
  - Dataclasses for type-safety.
  - All timestamps converted to timezone-aware UTC datetimes.
  - Tri-state booleans (True / False / None).
  - Only e10, e5, b7s, b7p fuels reach the DB. B10 and HVO are ignored
    (CLAUDE.md rule — these aren't user-selectable in the bot).

Used by:
  - etl.load_staging — loads parsed records into staging tables.
"""
from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Iterator

log = logging.getLogger(__name__)


# Fuel-type mapping. Both CSV columns and the JSON API use the upper-case
# form on the left. We normalise to the lower-case short codes on the right
# (matching app.users.fuel_type domain values).
#
# Two keys per fuel — the CSV used short forms ("B7S", "B7P"), the JSON API
# uses the full names ("B7_STANDARD", "B7_PREMIUM"). Doc examples are mixed-
# case ("B7_Standard") but real responses uppercase — we accept both via
# `.upper()` at call sites.
_FUEL_MAP: dict[str, str] = {
    # CSV-style names
    "E10": "e10",
    "E5":  "e5",
    "B7S": "b7s",
    "B7P": "b7p",
    # JSON API names
    "B7_STANDARD": "b7s",
    "B7_PREMIUM":  "b7p",
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
    opening_hours: dict       # JSONB-shape — see _build_opening_hours_*()
    amenities: dict           # JSONB-shape — see _build_amenities_*()
    forecourt_updated_at: datetime | None
    prices: list[ParsedPrice] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────
# Public API — JSON path (primary)
# ──────────────────────────────────────────────────────────────────────

def parse_api_response(
    stations_raw: Iterable[dict],
    prices_raw: Iterable[dict],
) -> list[ParsedStation]:
    """
    Convert raw API responses into ParsedStation objects.

    Args:
        stations_raw: list of objects from `/api/v1/pfs`.
        prices_raw:   list of objects from `/api/v1/pfs/fuel-prices`.

    Returns:
        List of ParsedStation, each with `.prices` populated from the
        prices endpoint. Stations without any sellable fuel are still
        returned — the bot needs them to display "this station exists
        but doesn't sell your fuel".

    Skips malformed entries with a logged warning — one bad row should
    not kill a 7800-row ETL run.
    """
    # Index prices by node_id for O(1) attachment.
    prices_by_station: dict[str, list[ParsedPrice]] = {}
    for raw in prices_raw:
        try:
            node_id = (raw.get("node_id") or "").strip()
            if not node_id:
                continue
            parsed = list(_extract_prices_json(node_id, raw.get("fuel_prices") or []))
            if parsed:
                prices_by_station[node_id] = parsed
        except Exception as exc:  # noqa: BLE001 — defensive parser
            log.warning("Skipping prices entry for node_id=%s: %s",
                        raw.get("node_id"), exc)

    # Now walk stations and attach.
    stations: list[ParsedStation] = []
    for raw in stations_raw:
        try:
            station = _api_station_to_parsed(raw)
            if station is None:
                continue
            station.prices = prices_by_station.get(station.station_id, [])
            stations.append(station)
        except Exception as exc:  # noqa: BLE001
            log.warning("Skipping station entry node_id=%s: %s",
                        raw.get("node_id"), exc)

    # Diagnostic: prices we couldn't match to any station. Usually 0 — if
    # nonzero, there's a referential mismatch between the two endpoints
    # (likely a station that exists in prices but is missing from /pfs).
    matched_ids = {s.station_id for s in stations}
    orphan_prices = set(prices_by_station) - matched_ids
    if orphan_prices:
        log.warning(
            "%d price records had no matching station in /pfs response. "
            "First 3 node_ids: %s",
            len(orphan_prices),
            list(orphan_prices)[:3],
        )

    return stations


def _api_station_to_parsed(raw: dict) -> ParsedStation | None:
    """Convert one /pfs response object to ParsedStation. None to skip."""
    station_id = (raw.get("node_id") or "").strip()
    if not station_id:
        return None

    loc = raw.get("location") or {}

    address = _join_nonempty(
        loc.get("address_line_1"),
        loc.get("address_line_2"),
        sep=", ",
    )

    opening_hours = _build_opening_hours_json(raw.get("opening_times") or {})
    amenities = _build_amenities_json(raw.get("amenities") or [])

    return ParsedStation(
        station_id=station_id,
        name=_str_or_none(raw.get("trading_name")),
        brand=_str_or_none(raw.get("brand_name")),
        postcode=_str_or_none(loc.get("postcode")),
        address=address,
        city=_str_or_none(loc.get("city")),
        latitude=_to_float(loc.get("latitude")),
        longitude=_to_float(loc.get("longitude")),
        is_supermarket=_to_bool(raw.get("is_supermarket_service_station")),
        is_24h=_derive_is_24h(opening_hours),
        is_temp_closed=_to_bool(raw.get("temporary_closure")),
        is_perm_closed=_to_bool(raw.get("permanent_closure")),
        opening_hours=opening_hours,
        amenities=amenities,
        # The /pfs endpoint doesn't carry an overall station "last updated"
        # timestamp — prices have their own per-fuel ones. We leave this
        # None for API-sourced data; the bot will fall back to the per-fuel
        # timestamps for staleness checks.
        forecourt_updated_at=None,
        prices=[],  # filled in by parse_api_response()
    )


def _extract_prices_json(station_id: str, fuel_prices: list[dict]) -> Iterator[ParsedPrice]:
    """Yield ParsedPrice objects from one /pfs/fuel-prices station entry."""
    for fp in fuel_prices:
        raw_fuel = (fp.get("fuel_type") or "").upper()
        db_fuel = _FUEL_MAP.get(raw_fuel)
        if db_fuel is None:
            # B10, HVO, or unknown fuel — silently skip (per CLAUDE.md).
            continue
        price = _to_decimal(fp.get("price"))
        if price is None:
            continue
        ts = _parse_iso_timestamp(fp.get("price_change_effective_timestamp"))
        yield ParsedPrice(
            station_id=station_id,
            fuel_type=db_fuel,
            price_pence=price,
            forecourt_updated_at=ts,
        )


def _build_opening_hours_json(opening_times: dict) -> dict:
    """
    Convert the /pfs `opening_times` object to our DB shape.

    API format:
        {
          "usual_days": {
            "monday":   {"open": "06:00:00", "close": "22:00:00", "is_24_hours": false},
            ...
          },
          "bank_holiday": {"open_time": "06:00:00", "close_time": "22:00:00", "is_24_hours": false}
        }

    Our DB JSONB shape:
        {
          "monday":      {"open": "06:00", "close": "22:00", "is_24h": false},
          ...
          "bank_holiday":{"open": "06:00", "close": "22:00", "is_24h": false}
        }

    Bank_holiday in the API uses different keys (open_time/close_time vs
    the usual_days' open/close). We normalise both shapes here.

    'No data' sentinel: if open=close=00:00 and not flagged 24h, treat as
    missing (None) so the bot doesn't show "closed all week" for stations
    that just haven't reported.
    """
    result: dict[str, dict | None] = {}

    usual = opening_times.get("usual_days") or {}
    for day in _DAYS:
        day_data = usual.get(day) or {}
        open_t = day_data.get("open") or ""
        close_t = day_data.get("close") or ""
        is_24 = bool(day_data.get("is_24_hours"))
        result[day] = _normalise_day(open_t, close_t, is_24)

    bh = opening_times.get("bank_holiday") or {}
    if bh:
        result["bank_holiday"] = _normalise_day(
            bh.get("open_time") or "",
            bh.get("close_time") or "",
            bool(bh.get("is_24_hours")),
        )
    else:
        result["bank_holiday"] = None

    return result


def _build_amenities_json(amenities_list: list) -> dict:
    """
    Convert the /pfs `amenities` array into our flat boolean dict.

    The API returns a list of present amenity names. Anything not in the
    list is False. We hard-code the 8-amenity domain so dict keys are stable.

    Mapping: API's `air_pump_or_screenwash` → our `air_pump`. Renamed at
    this boundary so the rest of the system isn't tied to the API's naming.
    """
    present = {(name or "").strip().lower() for name in amenities_list}

    return {
        "customer_toilets":      "customer_toilets" in present,
        "car_wash":              "car_wash" in present,
        "air_pump":              "air_pump_or_screenwash" in present,
        "water_filling":         "water_filling" in present,
        "twenty_four_hour_fuel": "twenty_four_hour_fuel" in present,
        "adblue_pumps":          "adblue_pumps" in present,
        "adblue_packaged":       "adblue_packaged" in present,
        "lpg_pumps":             "lpg_pumps" in present,
    }


# ──────────────────────────────────────────────────────────────────────
# Public API — CSV path (legacy / offline replay)
# ──────────────────────────────────────────────────────────────────────

def parse_fuel_csv(path: str | Path) -> Iterator[ParsedStation]:
    """
    Stream-parse a fuel-prices CSV file. Yields one ParsedStation per row.

    Same output type as `parse_api_response`. Used for `--local-csv` runs
    of the ETL (manual replay of an old gov.uk CSV file).

    Bad rows are logged and skipped — they don't stop the iterator.
    """
    path = Path(path)
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        required = {"forecourts.node_id", "forecourt_update_timestamp"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"CSV is missing required columns: {missing}. "
                f"Has the gov.uk format changed?"
            )

        for line_num, row in enumerate(reader, start=2):
            try:
                station = _csv_row_to_station(row)
                if station is None:
                    continue
                yield station
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "Skipping CSV line %d due to parse error: %s",
                    line_num, exc,
                )
                continue


def _csv_row_to_station(row: dict[str, str]) -> ParsedStation | None:
    """Convert one CSV row into a ParsedStation. Returns None to skip."""
    station_id = (row.get("forecourts.node_id") or "").strip()
    if not station_id:
        return None

    address = _join_nonempty(
        row.get("forecourts.location.address_line_1"),
        row.get("forecourts.location.address_line_2"),
        sep=", ",
    )

    opening_hours = _build_opening_hours_csv(row)
    amenities = _build_amenities_csv(row)
    prices = list(_extract_prices_csv(row, station_id))

    return ParsedStation(
        station_id=station_id,
        name=_str_or_none(row.get("forecourts.trading_name")),
        brand=_str_or_none(row.get("forecourts.brand_name")),
        postcode=_str_or_none(row.get("forecourts.location.postcode")),
        address=address,
        city=_str_or_none(row.get("forecourts.location.city")),
        latitude=_to_float(row.get("forecourts.location.latitude")),
        longitude=_to_float(row.get("forecourts.location.longitude")),
        is_supermarket=_parse_csv_bool(row.get("forecourts.is_supermarket_service_station")),
        is_24h=_derive_is_24h(opening_hours),
        is_temp_closed=_parse_csv_bool(row.get("forecourts.temporary_closure")),
        is_perm_closed=_parse_csv_bool(row.get("forecourts.permanent_closure")),
        opening_hours=opening_hours,
        amenities=amenities,
        forecourt_updated_at=_parse_js_timestamp(row.get("forecourt_update_timestamp")),
        prices=prices,
    )


def _extract_prices_csv(row: dict[str, str], station_id: str) -> Iterator[ParsedPrice]:
    """Yield up to 4 ParsedPrice objects from a CSV row."""
    for csv_fuel in ("E10", "E5", "B7S", "B7P"):
        db_fuel = _FUEL_MAP[csv_fuel]
        price_str = row.get(f"forecourts.fuel_price.{csv_fuel}")
        price = _to_decimal(price_str)
        if price is None:
            continue
        ts = _parse_js_timestamp(
            row.get(f"forecourts.price_change_effective_timestamp.{csv_fuel}")
        )
        yield ParsedPrice(
            station_id=station_id,
            fuel_type=db_fuel,
            price_pence=price,
            forecourt_updated_at=ts,
        )


def _build_opening_hours_csv(row: dict[str, str]) -> dict:
    """CSV-flavour of the opening-hours builder."""
    result: dict[str, dict | None] = {}

    for day in _DAYS:
        prefix = f"forecourts.opening_times.usual_days.{day}"
        open_t = row.get(f"{prefix}.open_time", "")
        close_t = row.get(f"{prefix}.close_time", "")
        is_24 = _parse_csv_bool(row.get(f"{prefix}.is_24_hours"))
        result[day] = _normalise_day(open_t, close_t, bool(is_24))

    bh_prefix = "forecourts.opening_times.bank_holiday.standard"
    result["bank_holiday"] = _normalise_day(
        row.get(f"{bh_prefix}.open_time", ""),
        row.get(f"{bh_prefix}.close_time", ""),
        bool(_parse_csv_bool(row.get(f"{bh_prefix}.is_24_hours"))),
    )

    return result


def _build_amenities_csv(row: dict[str, str]) -> dict:
    """CSV-flavour of the amenities builder."""
    def b(col: str) -> bool:
        v = _parse_csv_bool(row.get(col))
        return v is True

    return {
        "customer_toilets":      b("forecourts.amenities.customer_toilets"),
        "car_wash":              b("forecourts.amenities.vehicle_services.car_wash"),
        "air_pump":              b("forecourts.amenities.air_pump_or_screenwash"),
        "water_filling":         b("forecourts.amenities.water_filling"),
        "twenty_four_hour_fuel": b("forecourts.amenities.twenty_four_hour_fuel"),
        "adblue_pumps":          b("forecourts.amenities.fuel_and_energy_services.adblue_pumps"),
        "adblue_packaged":       b("forecourts.amenities.fuel_and_energy_services.adblue_packaged"),
        "lpg_pumps":             b("forecourts.amenities.fuel_and_energy_services.lpg_pumps"),
    }


# ──────────────────────────────────────────────────────────────────────
# Shared helpers (used by both paths)
# ──────────────────────────────────────────────────────────────────────

def _normalise_day(open_t: str, close_t: str, is_24h_flag: bool) -> dict | None:
    """
    Normalise one day's open/close/is_24h into our storage format, or None.

    Sentinel for "no data": when open and close are both 00:00 and the day
    isn't flagged 24h, we store None — better than misrepresenting the
    station as "closed all week".
    """
    open_t = (open_t or "").strip()
    close_t = (close_t or "").strip()

    is_blank_zero = (
        open_t in ("", "00:00:00", "00:00") and
        close_t in ("", "00:00:00", "00:00") and
        not is_24h_flag
    )
    if is_blank_zero:
        return None

    return {
        "open":   _trim_seconds(open_t),
        "close":  _trim_seconds(close_t),
        "is_24h": is_24h_flag,
    }


def _trim_seconds(time_str: str) -> str:
    """'07:00:00' -> '07:00'. '07:00' stays '07:00'. Empty stays empty."""
    if not time_str:
        return ""
    parts = time_str.split(":")
    if len(parts) >= 2:
        return f"{parts[0]}:{parts[1]}"
    return time_str


def _derive_is_24h(opening_hours: dict) -> bool | None:
    """A station is 24h iff all 7 weekdays are flagged is_24h=True."""
    weekday_data = [opening_hours.get(d) for d in _DAYS]
    if all(d is None for d in weekday_data):
        return None
    return all(
        isinstance(d, dict) and d.get("is_24h") is True
        for d in weekday_data
    )


def _str_or_none(value) -> str | None:
    """Stringify, strip, return; or None if empty."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _join_nonempty(*parts, sep: str = ", ") -> str | None:
    cleaned = [str(p).strip() for p in parts if p and str(p).strip()]
    return sep.join(cleaned) if cleaned else None


def _to_bool(value) -> bool | None:
    """Tri-state bool from a JSON value (actual bool, null, or string)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("true", "yes", "1"):
            return True
        if s in ("false", "no", "0"):
            return False
    return None


def _parse_csv_bool(value: str | None) -> bool | None:
    """CSV uses literal 'True'/'False'/''. None for unknown."""
    if value is None:
        return None
    s = value.strip()
    if s == "True":
        return True
    if s == "False":
        return False
    return None


def _to_float(value) -> float | None:
    """Float from JSON number or string. None on empty/malformed."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _to_decimal(value) -> Decimal | None:
    """
    Decimal from JSON number or string. None on empty/malformed.

    Decimal (not float) to avoid binary rounding in NUMERIC(6,1).
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        # str() avoids float→Decimal precision artefacts.
        return Decimal(str(value).strip())
    except (ValueError, ArithmeticError):
        return None


def _parse_iso_timestamp(value) -> datetime | None:
    """
    Parse an ISO 8601 timestamp like '2026-02-17T16:00:00.000Z'.

    Handles:
      - Z suffix (RFC 3339, what the API returns)
      - +HH:MM suffix (RFC 3339 alternative)
      - Space separator (legacy CSV format)
      - Naive (no tz) — assumed UTC
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    s = s.replace(" ", "T")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ──────────────────────────────────────────────────────────────────────
# JS-Date.toString format — kept for legacy CSV compatibility
# ──────────────────────────────────────────────────────────────────────
#
# Example: 'Mon Apr 27 2026 14:58:00 GMT+0000 (Coordinated Universal Time)'
# Only the first 24 characters matter: 'Mon Apr 27 2026 14:58:00'.

_JS_TS_FORMAT = "%a %b %d %Y %H:%M:%S"
_JS_TS_LEN = 24


def _parse_js_timestamp(value: str | None) -> datetime | None:
    """Parse a JS Date.toString() timestamp. Used by the CSV parser only."""
    if value is None or not str(value).strip():
        return None
    s = str(value).strip()
    if len(s) < _JS_TS_LEN:
        return None
    try:
        dt = datetime.strptime(s[:_JS_TS_LEN], _JS_TS_FORMAT)
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc)
