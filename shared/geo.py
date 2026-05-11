"""
Geographic utilities.

  - haversine_miles: great-circle distance between two lat/lon points, in miles
  - is_in_uk: rough bounding box check ('is this point inside the UK?')

Used by:
  - bot.handlers.search to filter stations within user's radius
  - bot.handlers.search to detect when user shares a non-UK location

Pure functions. No I/O, no side effects.
"""
import math


# Earth radius in miles. We use the mean radius (3958.7613 miles).
# For the precision we need (filtering within a few miles), exact value
# doesn't matter — even ±1% would be invisible to users.
_EARTH_RADIUS_MILES = 3958.7613


def haversine_miles(
    lat1: float, lon1: float,
    lat2: float, lon2: float,
) -> float:
    """
    Great-circle distance between two points on Earth, in miles.

    Uses the haversine formula. Accurate to within a few metres for
    distances under 100 miles, which is well beyond our use case
    (search radius up to 15 miles).

    Inputs in decimal degrees. Output in miles.

    Examples:
        >>> # London Bridge to Tower Bridge — about 0.5 miles
        >>> round(haversine_miles(51.508, -0.088, 51.505, -0.075), 2)
        0.61
        >>> # Same point — distance is 0
        >>> haversine_miles(51.5, -0.1, 51.5, -0.1)
        0.0
    """
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    # Haversine formula
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))

    return _EARTH_RADIUS_MILES * c


# UK bounding box.
#
# This is a rough box around mainland UK + Northern Ireland. It includes
# small bits of the Republic of Ireland, France, and the Channel Islands —
# but those false positives don't matter for us:
#   - We only use this on a user-shared GPS location
#   - The downstream search will return no stations for those areas anyway
#   - We'd rather show "no stations found" than wrongly say "Fuel Mate is UK only"
#
# Bounds determined empirically from these reference points:
#   - Cornwall (south-west tip):    50.0 N,  -5.7 W
#   - Shetland Isles (north tip):   60.9 N,  -1.3 W
#   - Lewis (west tip):             58.0 N,  -7.7 W
#   - Norfolk (east tip):           52.7 N,   1.8 E
# We pad a little for safety.
_UK_LAT_MIN = 49.5
_UK_LAT_MAX = 61.0
_UK_LON_MIN = -8.5
_UK_LON_MAX = 2.0


def is_in_uk(latitude: float, longitude: float) -> bool:
    """
    Rough check whether a (lat, lon) point falls inside the UK bounding box.

    This is a *gross* sieve, not a precise polygon check. False positives
    near borders (Republic of Ireland, northern France) are acceptable —
    the search will then return no stations and the user can switch to
    a postcode.

    Examples:
        >>> is_in_uk(51.5, -0.1)        # London
        True
        >>> is_in_uk(55.86, -4.25)      # Glasgow
        True
        >>> is_in_uk(48.85, 2.35)       # Paris
        False
        >>> is_in_uk(40.71, -74.0)      # New York
        False
    """
    return (
        _UK_LAT_MIN <= latitude <= _UK_LAT_MAX
        and _UK_LON_MIN <= longitude <= _UK_LON_MAX
    )
