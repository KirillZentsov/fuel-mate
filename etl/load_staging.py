"""
Stages 3 + 4 of the ETL pipeline:

  Stage 3 — register the dump in raw.fuel_data_dumps (or detect duplicate).
  Stage 4 — bulk-load parsed records into staging.stations and staging.prices.

Both stages share the same database connection so we can use a transaction
for the staging inserts: if the bulk copy fails midway, the dump_id row
is also rolled back.

Returns the dump_id (BIGINT) so downstream stages can reference it. If the
sha256 was already loaded, returns None and skips the staging write — the
orchestrator treats that as 'no-op duplicate' and short-circuits.
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

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


async def load_csv_into_staging(
    csv_path: Path,
    sha256: str,
    file_name: str | None = None,
    release_url: str | None = None,
) -> LoadResult:
    """
    Stream the CSV into staging tables.

    Workflow:
      1. Open connection.
      2. Check raw.fuel_data_dumps for this sha256 — short-circuit if found.
      3. Open transaction.
      4. Parse CSV (lazy iterator from shared.csv_parser).
      5. Build station + price record tuples in two lists.
      6. INSERT raw.fuel_data_dumps row → get dump_id.
      7. COPY both lists into staging.{stations,prices}.
      8. Commit.

    Args:
        csv_path: path to the CSV (already validated by download stage).
        sha256: digest of the file. Used for dedup.
        file_name: human-friendly name to record (default: csv_path.name).
        release_url: optional GitHub Release URL for audit.

    Returns:
        LoadResult — see dataclass above.

    Raises:
        asyncpg.PostgresError on any DB failure (caller logs + retries pipeline).
    """
    file_name = file_name or csv_path.name

    if config.DRY_RUN:
        log.info("DRY_RUN — parsing CSV but not writing to DB.")
        # In dry-run we still parse to validate, but skip DB entirely
        stations, prices = _parse_to_records(csv_path, dump_id=0)
        return LoadResult(
            dump_id=None, is_duplicate=False,
            station_count=len(stations), price_count=len(prices),
        )

    conn: asyncpg.Connection = await asyncpg.connect(config.DATABASE_URL)
    try:
        # Stage 3a — duplicate check
        existing = await conn.fetchval(
            "SELECT id FROM raw.fuel_data_dumps WHERE sha256 = $1",
            sha256,
        )
        if existing is not None:
            log.info("CSV with sha256 %s… already loaded as dump_id=%d. Skipping.",
                     sha256[:12], existing)
            return LoadResult(
                dump_id=existing, is_duplicate=True,
                station_count=0, price_count=0,
            )

        # Parse CSV into in-memory record lists. Tested: 50 rows produces
        # 50 stations + 157 prices. At 8000 stations expect ~5-6 MB RAM.
        # Important: we parse BEFORE opening the transaction so a parser
        # error doesn't leave a half-applied dump_id row.
        # We don't have dump_id yet — pass placeholder, fix after INSERT.
        stations, prices = _parse_to_records(csv_path, dump_id=None)
        if not stations:
            raise ValueError(
                "CSV produced 0 stations. Treating as malformed source — aborting."
            )

        # Compute aggregates needed for raw.fuel_data_dumps row
        forecourt_min, forecourt_max = _ts_range(stations)

        async with conn.transaction():
            # Stage 3b — register dump
            dump_id: int = await conn.fetchval(
                """
                INSERT INTO raw.fuel_data_dumps
                  (file_name, release_url, sha256, row_count,
                   forecourt_min_ts, forecourt_max_ts)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
                """,
                file_name, release_url, sha256, len(stations),
                forecourt_min, forecourt_max,
            )

            # Inject the real dump_id into the records (it was placeholder)
            stations = [(dump_id, *r[1:]) for r in stations]
            prices = [(dump_id, *r[1:]) for r in prices]

            # Stage 4 — bulk insert via COPY
            await conn.copy_records_to_table(
                "stations",
                schema_name="staging",
                columns=_STATION_COLS,
                records=stations,
            )
            await conn.copy_records_to_table(
                "prices",
                schema_name="staging",
                columns=_PRICE_COLS,
                records=prices,
            )

        log.info(
            "Loaded dump_id=%d: %d stations, %d prices.",
            dump_id, len(stations), len(prices),
        )
        return LoadResult(
            dump_id=dump_id, is_duplicate=False,
            station_count=len(stations), price_count=len(prices),
        )
    finally:
        await conn.close()


# ──────────────────────────────────────────────────────────────────────
# Helpers — convert ParsedStation/ParsedPrice into asyncpg-ready tuples
# ──────────────────────────────────────────────────────────────────────

def _parse_to_records(
    csv_path: Path,
    dump_id: int | None,
) -> tuple[list[tuple], list[tuple]]:
    """
    Walk the CSV via parse_fuel_csv() and build COPY-ready record tuples.

    For JSONB columns (opening_hours, amenities), asyncpg expects a JSON
    string, not a Python dict. We serialize here.

    The dump_id can be None — caller will overwrite once it's known.
    """
    import json

    station_records: list[tuple] = []
    price_records: list[tuple] = []

    for s in parse_fuel_csv(csv_path):
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
    # Position 15 in our tuple layout = forecourt_updated_at (0-indexed)
    timestamps = [r[15] for r in station_records if r[15] is not None]
    if not timestamps:
        return (None, None)
    return (min(timestamps), max(timestamps))
