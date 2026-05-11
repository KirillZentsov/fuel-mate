"""
ETL pipeline configuration.

Loads environment variables (locally from .env, in CI from injected secrets).
Holds runtime constants for retries, timeouts, alert thresholds.

We fail fast if a required variable is missing — better to crash at startup
than to discover at stage 6 that ADMIN_CHAT_ID was never set.
"""
import os

from dotenv import load_dotenv

# Load .env locally; no-op in CI (where env vars are injected).
load_dotenv()


def _required(name: str) -> str:
    """Read an env var or raise. Used for variables the ETL cannot run without."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable {name!r} is not set. "
            f"Locally: copy .env.example to .env and fill values. "
            f"In CI: configure secrets/variables in GitHub Actions."
        )
    return value


def _optional(name: str, default: str | None = None) -> str | None:
    """Read an env var with a fallback. None means 'unset'."""
    return os.environ.get(name) or default


# ─── Core ─────────────────────────────────────────────────────────────
# These are required for any production run.

DATABASE_URL: str = _required("DATABASE_URL")

# ─── Telegram (admin notifier) ────────────────────────────────────────
# Optional: if missing, admin_notifier silently no-ops, ETL still runs.
# This makes local development easier (you don't need a real bot).

TELEGRAM_BOT_TOKEN: str | None = _optional("TELEGRAM_BOT_TOKEN")
ADMIN_CHAT_ID: str | None = _optional("ADMIN_CHAT_ID")

# ─── gov.uk source ────────────────────────────────────────────────────

FUEL_PRICES_CSV_URL: str = _optional(
    "FUEL_PRICES_CSV_URL",
    "https://www.fuel-finder.service.gov.uk/internal/v1.0.2/csv/get-latest-fuel-prices-csv",
) or ""

# ─── GitHub release archival ──────────────────────────────────────────
# Optional: if missing, ETL skips upload_release stage. Useful for local runs.

GITHUB_TOKEN: str | None = _optional("GITHUB_TOKEN")
GITHUB_REPO: str | None = _optional("GITHUB_REPO")

# ─── Modes ────────────────────────────────────────────────────────────

# When True, ETL runs through download/parse but skips DB writes.
# Useful for verifying CSV format without touching the database.
DRY_RUN: bool = _optional("ETL_DRY_RUN", "false").lower() == "true"


# ─── Timing & retry constants ─────────────────────────────────────────

# Download
DOWNLOAD_TIMEOUT_SECONDS: int = 60
DOWNLOAD_MAX_RETRIES: int = 3
DOWNLOAD_RETRY_BACKOFF_SECONDS: list[int] = [5, 15, 45]
# CSV must be at least this large to be considered valid (sanity check)
DOWNLOAD_MIN_CSV_BYTES: int = 1024

# Pipeline thresholds for warning notifications
ETL_SLOW_THRESHOLD_SECONDS: int = 30
ETL_FRESH_DATA_THRESHOLD_HOURS: int = 24

# Alerts (used in stage 4b — kept here for completeness)
ALERT_DUPLICATE_WINDOW_HOURS: int = 4
ALERT_QUIET_HOURS_START: int = 22  # UK local time
ALERT_QUIET_HOURS_END: int = 8


# ─── Bot behaviour constants ──────────────────────────────────────────
# Mirrored in bot/config.py for clarity. Kept identical.

ALERT_BIG_THRESHOLD_PENCE: float = 2.0
