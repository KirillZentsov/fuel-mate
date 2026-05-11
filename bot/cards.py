"""
Message formatters: results table, detail card, favourite card.

Telegram supports HTML — we use <b>, <i>, <code>, <pre>, <s>. The <pre>
block renders monospace, which is what we need for the results table.

All visible text comes from bot.messages — this module only assembles
data into the right shape.
"""
from datetime import datetime, timezone
from html import escape
from typing import Optional, Iterable
from urllib.parse import quote

from bot import config, messages
from bot.repositories.stations import StationDetail, StationResult
from bot.repositories.favourites import FavouriteRow


# ──────────────────────────────────────────────────────────────────────
# Results table
# ──────────────────────────────────────────────────────────────────────

# Column widths for the monospaced results table. Tuned for typical
# Telegram chat width on phones (~30-32 chars before wrap on iOS).
# Format: "1  149.9p  3.2mi  TESCO"
#         num space  price   dist   brand (truncated)
_TABLE_HEADERS = ("#", "Price", "Dist", "Brand")
_BRAND_MAX = 12   # truncate brand to this many chars + '…'


def format_results_table(
    results: Iterable[StationResult],
    page: int = 1,
    page_size: int = config.RESULTS_PAGE_SIZE,
) -> str:
    """
    Build a monospace text table for the results page.

    Returns just the body — wrap in `messages.RESULTS_TABLE` when sending.
    """
    rows = list(results)
    start = (page - 1) * page_size
    end = start + page_size
    page_rows = rows[start:end]

    if not page_rows:
        return ""

    lines = []
    for i, r in enumerate(page_rows, start=start + 1):
        brand = (r.brand or "—")
        if len(brand) > _BRAND_MAX:
            brand = brand[: _BRAND_MAX - 1] + "…"
        # Right-align price and distance for visual alignment
        line = f"{i:>2}  {r.price_pence:>5.1f}p  {r.distance_miles:>4.1f}mi  {brand}"
        lines.append(line)

    return "\n".join(lines)


def format_results_header(count: int, radius: int, fuel: str) -> str:
    """Header line above the results table."""
    return messages.RESULTS_HEADER.format(
        count=count,
        radius=radius,
        fuel=messages.FUEL_LABELS.get(fuel, fuel),
    )


# ──────────────────────────────────────────────────────────────────────
# Detail card (single station)
# ──────────────────────────────────────────────────────────────────────

def format_detail_card(s: StationDetail, fuel: str) -> str:
    """
    Build the multiline detail card for one station.

    Layout (per spec section 9.1):
      <status_line>
      <b>{name}</b>
      <i>{brand}</i>
      ⛽ {fuel_label}: <b>{price}p</b>
      📍 {postcode}
      🛒 {amenities}                  (if any)
      ⚠️ Price updated {N} days ago    (if stale)
    """
    lines: list[str] = []

    # Status line
    lines.append(_status_line(s))

    # Name
    if s.name:
        lines.append(f"<b>{escape(s.name)}</b>")

    # Brand (only if present and different from name)
    if s.brand and s.brand.lower() != (s.name or "").lower():
        lines.append(f"<i>{escape(s.brand)}</i>")

    # Price
    if s.price_pence is not None:
        fuel_label = messages.FUEL_LABELS.get(fuel, fuel)
        lines.append(f"⛽ {fuel_label}: <b>{s.price_pence}p</b>")

    # Postcode
    if s.postcode:
        lines.append(f"📍 <code>{escape(s.postcode)}</code>")

    # Amenities (only show those that are True, abbreviated)
    amenity_text = _format_amenities(s.amenities)
    if amenity_text:
        lines.append(f"🛒 {amenity_text}")

    # Price staleness warning
    days = _days_since(s.price_updated_at)
    if days is not None and days >= config.STALE_PRICE_DAYS:
        lines.append(messages.PRICE_STALE_WARNING.format(days=days))

    return "\n".join(lines)


def navigate_url(latitude: float, longitude: float, name: str | None = None) -> str:
    """
    Build a Google Maps URL for the 'Navigate' button.

    Using the public maps URL — no API key needed, opens in any device's
    default map app via Google Maps redirect.
    """
    label = quote(name) if name else f"{latitude},{longitude}"
    return f"https://www.google.com/maps/search/?api=1&query={label}&query_place_id=&center={latitude},{longitude}"


# ──────────────────────────────────────────────────────────────────────
# Favourite card
# ──────────────────────────────────────────────────────────────────────

def format_favourite_card(f: FavouriteRow, fuel: str) -> str:
    """
    Build a 3-line card for one favourite station.

    Layout (per spec section 9.1):
      <status_line>
      <b>{name}</b> · <i>{brand}</i> · {postcode}
      ⛽ <b>{price}p</b> {price_change}    (if changed since last alert)
    """
    # Status line — favourites use a simplified version
    if f.is_temp_closed:
        status = messages.STATUS_TEMP_CLOSED
    elif f.is_24h:
        status = messages.STATUS_24H
    else:
        status = messages.STATUS_OPEN_NOW   # eventually-consistent OK

    parts = []
    if f.name:
        parts.append(f"<b>{escape(f.name)}</b>")
    if f.brand:
        parts.append(f"<i>{escape(f.brand)}</i>")
    if f.postcode:
        parts.append(f"<code>{escape(f.postcode)}</code>")
    second_line = " · ".join(parts) if parts else "—"

    if f.current_price_pence is not None:
        price_line = f"⛽ <b>{f.current_price_pence}p</b>"
        delta = _price_delta(f.current_price_pence, f.last_notified_pence)
        if delta is not None:
            price_line += f"  {delta}"
    else:
        # Station no longer sells this fuel
        fuel_label = messages.FUEL_LABELS.get(fuel, fuel)
        price_line = f"⛽ {fuel_label}: <i>not available</i>"

    return f"{status}\n{second_line}\n{price_line}"


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def _status_line(s: StationDetail) -> str:
    """Compute the right status emoji + label for a station."""
    if s.is_temp_closed:
        return messages.STATUS_TEMP_CLOSED
    if s.is_24h:
        return messages.STATUS_24H
    if _is_open_now(s.opening_hours):
        return messages.STATUS_OPEN_NOW
    return messages.STATUS_CLOSED


def _is_open_now(opening_hours: dict) -> bool:
    """
    Check whether the station is open at this UK-local moment.

    Conservative: if we have no data for the current day, return True
    (don't falsely show 'closed'). Stations without any opening hours
    will fall back to 'open' rather than annoying users.
    """
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("Europe/London"))
    except Exception:
        # Fallback if zoneinfo data missing — use UTC, accept slight inaccuracy
        now = datetime.now(timezone.utc)

    weekday = ("monday", "tuesday", "wednesday", "thursday",
               "friday", "saturday", "sunday")[now.weekday()]
    day = (opening_hours or {}).get(weekday)
    if day is None:
        return True   # no data — assume open
    if day.get("is_24h"):
        return True
    open_str = day.get("open") or ""
    close_str = day.get("close") or ""
    if not open_str or not close_str:
        return True
    now_t = now.strftime("%H:%M")
    # Lexicographic compare works for HH:MM
    if open_str <= close_str:
        return open_str <= now_t < close_str
    # Wraparound past midnight (e.g. 22:00–06:00)
    return now_t >= open_str or now_t < close_str


def _format_amenities(amenities: dict) -> str:
    """Comma-separated friendly list of present amenities."""
    if not amenities:
        return ""
    short_names = {
        "customer_toilets":      "Toilets",
        "car_wash":              "Car wash",
        "air_pump":              "Air/Screenwash",
        "water_filling":         "Water",
        "twenty_four_hour_fuel": "24h fuel",
        "adblue_pumps":          "AdBlue pump",
        "adblue_packaged":       "AdBlue (bottled)",
        "lpg_pumps":             "LPG",
    }
    present = [name for k, name in short_names.items() if amenities.get(k)]
    return ", ".join(present)


def _days_since(when: Optional[datetime]) -> Optional[int]:
    """Whole days between `when` and now. Returns None if `when` is None."""
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - when
    return max(0, delta.days)


def _price_delta(current, last_notified) -> Optional[str]:
    """
    Format a price delta as '↓ -2.0p' or '↑ +1.5p' if there's been a change.
    Returns None if no delta or no baseline.
    """
    if current is None or last_notified is None:
        return None
    diff = float(current - last_notified)
    if diff == 0:
        return None
    if diff < 0:
        return messages.PRICE_CHANGE_DOWN.format(delta=abs(round(diff, 1)))
    return messages.PRICE_CHANGE_UP.format(delta=round(diff, 1))
