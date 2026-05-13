"""
Stages 3 + 4 of the ETL pipeline:

  Stage 3 — register the dump in raw.fuel_data_dumps (or detect duplicate).
  Stage 4 — bulk-load parsed records into staging.stations and staging.prices.

Both stages share the same database connection so we can use a transaction
for the staging inserts: if the bulk copy fails midway, the dump_id row
is also rolled back.

Public API:
  - `load_stations_into_staging(stations, sha256, file_name, release_url)`
    — primary entry point. Accepts a list of ParsedStation from any
    source (JSON API, CSV file, or unit test fixtures).
  - `load_csv_into_staging(csv_path, sha256, file_name, release_url)`
    — convenience wrapper: parses a CSV file and calls the primary API.
    Used by `--local-csv` runs of the pipeline.

If the sha256 was already loaded, the function returns with
`is_duplicate=True` and skips the staging write — the orchestrator treats
that as a no-op duplicate and short-circuits.
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import asyncpg

from etl import config
from shared.csv_parser import parse_fuel_csv, ParsedStation

log = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Output of the staging load stage."""
    dump_id: int | None       # None if the sha256 was a duplicate
    is_duplicate: bool        # True if we skipped because sha256 already loaded
    station_count: int        # rows written to staging.stations (0 if duplicate)
    price_count: int          # rows written to staging.prices  (0 if duplicate)


# Column order MUST match the target tables in 0003_staging_tables.sql.
# We omit `id` (BIGSERIAL) and `loaded_at` (DEFAULT now()) — Postgres fills both.
_STATION_COLS = (
    "dump_id", "station_id", "name", "brand", "postcode", "address", "city",
    "latitude", "longitude", "is_supermarket", "is_24h",
    "is_temp_closed", "is_perm_closed", "opening_hours", "amenities",
    "forecourt_updated_at",
)

_PRICE_COLS = (
    "dump_id", "station_id", "fuel_type", "price_pence", "forecourt_updated_at",
)


# ──────────────────────────────────────────────────────────────────────
# Primary entry point — accepts pre-parsed records
# ──────────────────────────────────────────────────────────────────────

async def load_stations_into_staging(
    stations: list[ParsedStation],
    sha256: str,
    file_name: str = "api-fetch",
    release_url: str | None = None,
) -> LoadResult:
    """
    Bulk-load parsed stations + their prices into staging.

    This is the source-agnostic entry point. The caller has already parsed
    raw data from somewhere (gov.uk API, local CSV, fixture) into the
    ParsedStation dataclass.

    Workflow:
      1. Check raw.fuel_data_dumps for sha256 — short-circuit on duplicate.
      2. Open a transaction.
      3. INSERT raw.fuel_data_dumps row → get dump_id.
      4. COPY two lists into staging.{stations,prices}.
      5. Commit.

    Args:
        stations: list of ParsedStation, each with `.prices`.
        sha256: digest used for dedup. For API runs, computed over the
            canonical JSON. For CSV runs, the file digest.
        file_name: friendly identifier for raw.fuel_data_dumps.file_name.
        release_url: optional GitHub Release URL for audit.

    Returns:
        LoadResult.

    Raises:
        asyncpg.PostgresError on any DB failure.
    """
    if config.DRY_RUN:
        log.info("DRY_RUN — would load %d stations.", len(stations))
        station_records, price_records = _records_from_parsed(stations, dump_id=0)
        return LoadResult(
            dump_id=None, is_duplicate=False,
            station_count=len(station_records),
            price_count=len(price_records),
        )

    if not stations:
        raise ValueError(
            "Got 0 stations to load. Treating as malformed source — aborting."
        )

    conn: asyncpg.Connection = await asyncpg.connect(config.DATABASE_URL)
    try:
        existing = await conn.fetchval(
            "SELECT id FROM raw.fuel_data_dumps WHERE sha256 = $1",
            sha256,
        )
        if existing is not None:
            log.info(
                "Source with sha256 %s… already loaded as dump_id=%d. Skipping.",
                sha256[:12], existing,
            )
            return LoadResult(
                dump_id=existing, is_duplicate=True,
                station_count=0, price_count=0,
            )

        # Build COPY-ready records with placeholder dump_id; we'll fix it
        # after the INSERT below.
        station_records, price_records = _records_from_parsed(stations, dump_id=None)

        forecourt_min, forecourt_max = _ts_range(station_records)

        async with conn.transaction():
            dump_id: int = await conn.fetchval(
                """
                INSERT INTO raw.fuel_data_dumps
                  (file_name, release_url, sha256, row_count,
                   forecourt_min_ts, forecourt_max_ts)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
                """,
                file_name, release_url, sha256, len(station_records),
                forecourt_min, forecourt_max,
            )

            # Fill the placeholder dump_id with the real one
            station_records = [(dump_id, *r[1:]) for r in station_records]
            price_records = [(dump_id, *r[1:]) for r in price_records]

            await conn.copy_records_to_table(
                "stations",
                schema_name="staging",
                columns=_STATION_COLS,
                records=station_records,
            )
            await conn.copy_records_to_table(
                "prices",
                schema_name="staging",
                columns=_PRICE_COLS,
                records=price_records,
            )

        log.info(
            "Loaded dump_id=%d: %d stations, %d prices.",
            dump_id, len(station_records), len(price_records),
        )
        return LoadResult(
            dump_id=dump_id, is_duplicate=False,
            station_count=len(station_records),
            price_count=len(price_records),
        )
    finally:
        await conn.close()


# ──────────────────────────────────────────────────────────────────────
# Convenience wrapper for CSV path (legacy / --local-csv)
# ──────────────────────────────────────────────────────────────────────

async def load_csv_into_staging(
    csv_path: Path,
    sha256: str,
    file_name: str | None = None,
    release_url: str | None = None,
) -> LoadResult:
    """
    Parse a CSV file and forward to the primary loader.

    Kept for backward compatibility with `--local-csv` invocations.
    """
    file_name = file_name or csv_path.name
    stations = list(parse_fuel_csv(csv_path))
    return await load_stations_into_staging(
        stations=stations,
        sha256=sha256,
        file_name=file_name,
        release_url=release_url,
    )


# ──────────────────────────────────────────────────────────────────────
# Helpers — convert ParsedStation/ParsedPrice into asyncpg-ready tuples
# ──────────────────────────────────────────────────────────────────────

def _records_from_parsed(
    stations: Iterable[ParsedStation],
    dump_id: int | None,
) -> tuple[list[tuple], list[tuple]]:
    """
    Build COPY-ready tuples from ParsedStation objects.

    For JSONB columns (opening_hours, amenities), asyncpg expects a JSON
    string, not a Python dict. We serialize here.

    The dump_id can be None — the caller overwrites it after INSERT.
    """
    station_records: list[tuple] = []
    price_records: list[tuple] = []

    for s in stations:
        station_records.append((
            dump_id,
            s.station_id,
            s.name,
            s.brand,
            s.postcode,
            s.address,
            s.city,
            s.latitude,
            s.longitude,
            s.is_supermarket,
            s.is_24h,
            s.is_temp_closed,
            s.is_perm_closed,
            json.dumps(s.opening_hours),
            json.dumps(s.amenities),
            s.forecourt_updated_at,
        ))
        for p in s.prices:
            price_records.append((
                dump_id,
                p.station_id,
                p.fuel_type,
                p.price_pence,
                p.forecourt_updated_at,
            ))

    return station_records, price_records


def _ts_range(
    station_records: list[tuple],
) -> tuple[datetime | None, datetime | None]:
    """Compute min/max forecourt_updated_at across all parsed stations."""
    # Position 15 in the tuple layout = forecourt_updated_at (0-indexed)
    timestamps = [r[15] for r in station_records if r[15] is not None]
    if not timestamps:
        return (None, None)
    return (min(timestamps), max(timestamps))
