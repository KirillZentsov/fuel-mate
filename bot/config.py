"""
Bot configuration.

Loads environment variables (locally from .env, in production from Railway
Variables) and exposes runtime constants.

We deliberately mirror some constants from etl/config.py (e.g. fuel codes,
alert thresholds). The two processes never share memory, and copy-pasting
the dozen-or-so constants is simpler than introducing a shared config module.
If a constant ever drifts between bot and ETL, that's a config bug — easy
to spot in code review.
"""
import os

from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    """Read an env var or raise. Used for variables the bot cannot start without."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable {name!r} is not set. "
            f"Locally: copy .env.example to .env and fill values. "
            f"In production: configure variables in Railway."
        )
    return value


def _optional(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name) or default


# ─── Required ─────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN: str = _required("TELEGRAM_BOT_TOKEN")
DATABASE_URL: str = _required("DATABASE_URL")

# ─── Optional ─────────────────────────────────────────────────────────

ADMIN_CHAT_ID: str | None = _optional("ADMIN_CHAT_ID")
LOG_LEVEL: str = _optional("LOG_LEVEL", "INFO") or "INFO"


# ─── Postcodes file path ──────────────────────────────────────────────
# Loaded at bot startup. Optional: bot still runs without it, postcode
# search just returns "not found" for everything until the file is in place.

POSTCODES_PATH = _optional("POSTCODES_PATH", "data/postcodes.csv.gz")


# ─── User-facing limits ───────────────────────────────────────────────

MAX_FAVOURITES: int = 3              # spec section 11.3
MAX_RESULTS: int = 15                # max stations shown in one search
RESULTS_PAGE_SIZE: int = 5           # how many fit on a page


# ─── Defaults applied to new users ────────────────────────────────────

DEFAULT_FUEL: str = "e10"
DEFAULT_RADIUS_MILES: int = 5
DEFAULT_ALERTS_MODE: str = "big_only"


# ─── Search behaviour ─────────────────────────────────────────────────

RADIUS_OPTIONS: list[int] = [1, 3, 5, 10, 15]
ALERT_BIG_THRESHOLD_PENCE: float = 2.0
STALE_PRICE_DAYS: int = 7            # show "price updated N days ago" warning
STALE_DATA_WARNING_DAYS: int = 3     # show "data may be outdated" banner


# ─── Domain enumerations ──────────────────────────────────────────────

FUEL_TYPES: dict[str, str] = {
    "e10": "Unleaded",
    "e5":  "Super Unleaded",
    "b7s": "Diesel",
    "b7p": "Premium Diesel",
}

ALERT_MODES: dict[str, str] = {
    "all_changes": "All changes",
    "big_only":    "Big changes only (≥2p)",
    "off":         "Off",
}
