"""
UK postcode repository — coordinate lookup by postcode.

The full UK postcode list is ~1.7M rows. Storing it in Supabase would
eat the 500MB free tier; instead we keep it in the bot's RAM (~250MB
loaded). At process start we read data/postcodes.csv.gz once and build
a dict for O(1) lookup.

CSV format expected:
    postcode,latitude,longitude
    AB10 1AB,57.149,-2.094
    ...

The file may be plain .csv or gzipped (.csv.gz). Auto-detected by extension.

Fallback behaviour: if the file is missing, load a small set of hardcoded
sample postcodes. This lets the bot run for local development before the
full file is in place. In production the file MUST exist; we log a
prominent warning at startup.
"""
import csv
import gzip
import logging
from pathlib import Path
from typing import Optional

from bot import config
from shared.postcode_validator import normalize_postcode

log = logging.getLogger(__name__)


# Module-level cache. Built once at startup.
_postcode_to_coords: dict[str, tuple[float, float]] = {}
_loaded: bool = False


# Sample postcodes used when the real CSV is missing. Covers a handful of
# UK regions so basic search still works for development.
_SAMPLE_POSTCODES: dict[str, tuple[float, float]] = {
    "SW1A 1AA": (51.501, -0.142),    # Buckingham Palace, London
    "EC1A 1BB": (51.520, -0.097),    # central London
    "M1 1AE":   (53.480, -2.242),    # Manchester
    "B1 1AA":   (52.479, -1.903),    # Birmingham
    "G1 1AA":   (55.861, -4.250),    # Glasgow
    "EH1 1AA":  (55.953, -3.190),    # Edinburgh
    "BT1 1AA":  (54.597, -5.930),    # Belfast
    "CF10 1AA": (51.479, -3.179),    # Cardiff
    "LS1 1AA":  (53.797, -1.541),    # Leeds
    "L1 1AA":   (53.405, -2.991),    # Liverpool
    "NE1 1AA":  (54.973, -1.614),    # Newcastle
    "BS1 1AA":  (51.453, -2.589),    # Bristol
    "CO4 9ED":  (51.872, 0.917),     # Colchester (used in spec examples)
}


def load_postcodes() -> int:
    """
    Read postcodes file into the module-level cache.

    Called once from bot.main at startup. Returns the number of postcodes
    loaded so the caller can log it.

    Behaviour:
      - file exists  → parse it, populate cache
      - file missing → populate cache with _SAMPLE_POSTCODES, warn loudly
    """
    global _loaded

    path = Path(config.POSTCODES_PATH)
    if not path.exists():
        log.warning(
            "Postcodes file not found at %s — using built-in sample of %d "
            "postcodes for development. Postcode search will be very limited.",
            path, len(_SAMPLE_POSTCODES),
        )
        _postcode_to_coords.update(_SAMPLE_POSTCODES)
        _loaded = True
        return len(_postcode_to_coords)

    log.info("Loading postcodes from %s …", path)
    open_fn = gzip.open if path.suffix == ".gz" else open

    count = 0
    with open_fn(path, "rt", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        # Validate columns
        required = {"postcode", "latitude", "longitude"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"Postcodes CSV is missing columns: {missing}. "
                f"Expected: postcode, latitude, longitude"
            )

        for row in reader:
            try:
                pc = normalize_postcode(row["postcode"])
                lat = float(row["latitude"])
                lon = float(row["longitude"])
            except (ValueError, KeyError):
                # Skip malformed rows silently — millions of rows means
                # a few bad ones aren't worth crashing over.
                continue
            _postcode_to_coords[pc] = (lat, lon)
            count += 1

    _loaded = True
    log.info("Loaded %d postcodes into RAM.", count)
    return count


def lookup(postcode: str) -> Optional[tuple[float, float]]:
    """
    Look up coordinates for a postcode. Returns None if not found.

    The input is normalised (uppercased, single-space format) before lookup
    so callers don't have to worry about it.

    Args:
        postcode: any reasonable form — 'co4 9ed', 'CO49ED', 'CO4 9ED' all work.

    Returns:
        (latitude, longitude) tuple, or None if postcode isn't in the dataset.
    """
    if not _loaded:
        raise RuntimeError(
            "Postcodes not loaded. Call load_postcodes() at bot startup."
        )

    normalized = normalize_postcode(postcode)
    return _postcode_to_coords.get(normalized)


def is_loaded() -> bool:
    """Whether load_postcodes() has been called. Useful for healthchecks."""
    return _loaded
