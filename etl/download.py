"""
Stage 1 of the ETL pipeline: fetch the latest fuel-prices data.

Two source modes:

  1. **API (default, production):** call gov.uk Fuel Finder API via
     `etl.api_client` and parse the JSON response. This is what runs in
     GitHub Actions every 6 hours.

  2. **Local CSV (offline replay):** read a local CSV file (the legacy
     gov.uk format). Used for `python -m etl.pipeline --local-csv`.

Both modes return the same `FetchResult` — the rest of the pipeline doesn't
need to know how the data arrived.

A canonical sha256 is computed for deduplication. For API runs it digests
the sorted list of (station_id + fuel_type + price + timestamp) tuples —
this gives a stable identifier that ignores cosmetic differences (batch
ordering, missing optional fields) but changes whenever real data changes.
"""
import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path

from etl import config
from etl.api_client import (
    fetch_all as api_fetch_all,
    FuelFinderError,
)
from shared.csv_parser import (
    parse_api_response,
    parse_fuel_csv,
    ParsedStation,
)

log = logging.getLogger(__name__)


class DownloadError(Exception):
    """Raised when fetch fails. Caught by the pipeline orchestrator."""


@dataclass
class FetchResult:
    """Output of the fetch stage."""
    stations: list[ParsedStation]   # already-parsed, ready for load_staging
    sha256: str                     # canonical digest for dedup
    source: str                     # 'api' or 'csv:path/to/file'


# ──────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────

async def fetch_data(local_csv_path: str | Path | None = None) -> FetchResult:
    """
    Fetch the latest fuel data — either from the API or a local CSV.

    Args:
        local_csv_path: optional path to a local CSV file. When provided,
            the network is skipped.

    Returns:
        FetchResult with parsed stations + canonical sha256 + source tag.

    Raises:
        DownloadError on any unrecoverable failure.
    """
    if local_csv_path is not None:
        return _fetch_from_csv(Path(local_csv_path))
    return await _fetch_from_api()


# Backwards-compatible alias — old code calls `download_csv()`.
# We keep the name so `etl.pipeline` doesn't need a coordinated rename.
async def download_csv(local_csv_path: str | Path | None = None) -> FetchResult:
    """Deprecated alias for fetch_data(). Will be removed in a future revision."""
    return await fetch_data(local_csv_path=local_csv_path)


# ──────────────────────────────────────────────────────────────────────
# API path
# ──────────────────────────────────────────────────────────────────────

async def _fetch_from_api() -> FetchResult:
    """Pull stations + prices from the gov.uk Fuel Finder API."""
    if not config.FUEL_FINDER_CLIENT_ID or not config.FUEL_FINDER_CLIENT_SECRET:
        raise DownloadError(
            "API credentials not configured. Set FUEL_FINDER_CLIENT_ID and "
            "FUEL_FINDER_CLIENT_SECRET (locally in .env, in production in "
            "GitHub Secrets / Railway env vars)."
        )

    log.info("Fetching from Fuel Finder API…")
    try:
        stations_raw, prices_raw = await api_fetch_all(
            client_id=config.FUEL_FINDER_CLIENT_ID,
            client_secret=config.FUEL_FINDER_CLIENT_SECRET,
        )
    except FuelFinderError as exc:
        raise DownloadError(f"API fetch failed: {exc}") from exc

    if not stations_raw:
        raise DownloadError("API returned 0 stations. Aborting (likely outage).")

    stations = parse_api_response(stations_raw, prices_raw)
    if not stations:
        raise DownloadError(
            "Parser returned 0 ParsedStation objects from non-empty API "
            "response. Has the response schema changed?"
        )

    sha = _canonical_sha256(stations)

    log.info(
        "API fetch parsed: %d stations, %d total price records. sha256=%s…",
        len(stations),
        sum(len(s.prices) for s in stations),
        sha[:12],
    )
    return FetchResult(stations=stations, sha256=sha, source="api")


# ──────────────────────────────────────────────────────────────────────
# CSV path (offline replay)
# ──────────────────────────────────────────────────────────────────────

def _fetch_from_csv(path: Path) -> FetchResult:
    """Read and parse a local CSV. Used by `--local-csv`."""
    if not path.exists():
        raise DownloadError(f"Local CSV not found: {path}")

    log.info("Reading local CSV: %s", path)
    stations = list(parse_fuel_csv(path))
    if not stations:
        raise DownloadError(f"CSV produced 0 stations: {path}")

    # For CSV runs we keep the file-content sha as before — this preserves
    # the existing dedup behaviour for CSV-replay scenarios.
    sha = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(64 * 1024):
            sha.update(chunk)
    digest = sha.hexdigest()

    log.info(
        "CSV parsed: %d stations, %d total price records. sha256=%s…",
        len(stations),
        sum(len(s.prices) for s in stations),
        digest[:12],
    )
    return FetchResult(stations=stations, sha256=digest, source=f"csv:{path.name}")


# ──────────────────────────────────────────────────────────────────────
# Canonical hashing for dedup
# ──────────────────────────────────────────────────────────────────────

def _canonical_sha256(stations: list[ParsedStation]) -> str:
    """
    Compute a sha256 that's stable across batch orderings and immune to
    cosmetic fields (price_last_updated, network timing, etc.).

    We digest the sorted list of (station_id, fuel_type, price, effective_ts)
    tuples — these are the *substantive* fields that should change if and
    only if the dataset has actually changed.

    Why not hash the raw response: the API may return batches in different
    orders, and `price_last_updated` ticks even when nothing else changes.
    Hashing those would produce a different digest every run, defeating
    dedup.
    """
    canonical = []
    for s in sorted(stations, key=lambda x: x.station_id):
        prices_sorted = sorted(s.prices, key=lambda p: p.fuel_type)
        canonical.append({
            "id": s.station_id,
            "prices": [
                {
                    "fuel": p.fuel_type,
                    "price": str(p.price_pence),
                    "ts": p.forecourt_updated_at.isoformat()
                          if p.forecourt_updated_at else None,
                }
                for p in prices_sorted
            ],
        })

    payload = json.dumps(canonical, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
