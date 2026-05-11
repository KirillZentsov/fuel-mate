"""
UK postcode validation and normalization.

Used by:
  - bot.handlers.search — validate user input before lookup
  - bot.repositories.postcodes — normalize before CSV lookup

These are pure functions. No I/O, no side effects.
"""
import re

# UK postcode regex.
#
# Pattern breakdown:
#   ^                  start
#   [A-Z]{1,2}         1-2 letters (area: e.g. "M", "SW", "CO")
#   \d                 1 digit  (district)
#   [A-Z\d]?           optional letter or digit (district extension, rare)
#   \s?                optional space  ← we tolerate "CO49ED" and "CO4 9ED"
#   \d                 1 digit (sector)
#   [A-Z]{2}           2 letters (unit)
#   $                  end
#
# This matches all current UK postcodes. There are a few historical exceptions
# (GIR 0AA, BFPO codes for armed forces) but they are irrelevant for fuel
# stations.
#
# IGNORECASE flag — we accept lowercase input ("co4 9ed") and normalize later.
_UK_POSTCODE_RE = re.compile(
    r"^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$",
    re.IGNORECASE,
)


def is_valid_uk_postcode(text: str) -> bool:
    """
    Check whether a string is a syntactically valid UK postcode.

    This is a *format* check — it does NOT verify that the postcode actually
    exists. For real existence, look it up in the postcodes CSV.

    Examples:
        >>> is_valid_uk_postcode("CO4 9ED")
        True
        >>> is_valid_uk_postcode("co49ed")
        True
        >>> is_valid_uk_postcode("hello")
        False
    """
    if not text:
        return False
    return bool(_UK_POSTCODE_RE.match(text.strip()))


def normalize_postcode(text: str) -> str:
    """
    Convert any reasonable user input to canonical 'AAA NNN' uppercase form.

    Steps:
      1. Strip whitespace and uppercase.
      2. Remove ALL internal spaces (handles 'CO 4 9 ED' or 'CO  4 9ED').
      3. Insert a single space before the last 3 characters (the 'inward' part).

    This is the format used everywhere downstream (postcodes CSV, display).

    Examples:
        >>> normalize_postcode("co4 9ed")
        'CO4 9ED'
        >>> normalize_postcode("CO49ED")
        'CO4 9ED'
        >>> normalize_postcode("  CO 4 9 ED  ")
        'CO4 9ED'

    Note: this function does NOT validate. Always run is_valid_uk_postcode()
    first if input might be garbage.
    """
    cleaned = text.strip().upper().replace(" ", "")
    if len(cleaned) < 5:
        # Too short to be a valid postcode anyway — return as-is so the
        # validator can reject it. Don't try to insert a space.
        return cleaned
    return f"{cleaned[:-3]} {cleaned[-3:]}"
