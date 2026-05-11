"""
Tests for etl.load_staging — pure-logic functions only.

Database integration is verified manually (see deployment/smoke-test docs).
Here we only test the parts that don't require a live Postgres connection:

  - _parse_to_records: maps ParsedStation/Price into asyncpg-ready tuples
  - _ts_range: computes min/max forecourt_updated_at from records

This keeps the test suite fast and runnable in CI without service mocks.
"""
import json
from datetime import datetime, timezone

from etl.load_staging import _parse_to_records, _ts_range


class TestParseToRecords:
    def test_returns_two_lists(self, sample_csv_path):
        stations, prices = _parse_to_records(sample_csv_path, dump_id=42)
        assert isinstance(stations, list)
        assert isinstance(prices, list)

    def test_station_count_matches_csv(self, sample_csv_path):
        stations, _ = _parse_to_records(sample_csv_path, dump_id=42)
        assert len(stations) == 12  # fixture has 12 rows

    def test_price_count_matches_csv(self, sample_csv_path):
        # 12 stations, between 0 and 4 fuels each — exact count is data-dependent.
        # Just verify it's reasonable.
        _, prices = _parse_to_records(sample_csv_path, dump_id=42)
        assert 30 <= len(prices) <= 48

    def test_dump_id_in_first_position(self, sample_csv_path):
        stations, prices = _parse_to_records(sample_csv_path, dump_id=42)
        for s in stations:
            assert s[0] == 42
        for p in prices:
            assert p[0] == 42

    def test_dump_id_none_passthrough(self, sample_csv_path):
        # dump_id=None is the placeholder mode used before the DB INSERT.
        stations, prices = _parse_to_records(sample_csv_path, dump_id=None)
        for s in stations:
            assert s[0] is None
        for p in prices:
            assert p[0] is None

    def test_jsonb_columns_serialized_as_strings(self, sample_csv_path):
        # asyncpg COPY needs strings for JSONB; make sure we serialize.
        # Position 13 = opening_hours, 14 = amenities (in our tuple layout).
        stations, _ = _parse_to_records(sample_csv_path, dump_id=42)
        for s in stations:
            opening_hours_str = s[13]
            amenities_str = s[14]
            assert isinstance(opening_hours_str, str)
            assert isinstance(amenities_str, str)
            # And valid JSON
            json.loads(opening_hours_str)
            json.loads(amenities_str)

    def test_station_record_length_matches_columns(self, sample_csv_path):
        # Sanity check: 16 fields per station record (matching _STATION_COLS)
        from etl.load_staging import _STATION_COLS
        stations, _ = _parse_to_records(sample_csv_path, dump_id=42)
        assert len(stations[0]) == len(_STATION_COLS)

    def test_price_record_length_matches_columns(self, sample_csv_path):
        from etl.load_staging import _PRICE_COLS
        _, prices = _parse_to_records(sample_csv_path, dump_id=42)
        assert len(prices[0]) == len(_PRICE_COLS)


class TestTsRange:
    def test_empty_returns_none_pair(self):
        assert _ts_range([]) == (None, None)

    def test_records_with_timestamps(self):
        # Build minimal tuples with a timestamp at position 15
        # (matching _STATION_COLS layout)
        ts1 = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        ts3 = datetime(2026, 5, 1, 8, 0, tzinfo=timezone.utc)
        records = [
            tuple([None] * 15 + [ts1]),
            tuple([None] * 15 + [ts2]),
            tuple([None] * 15 + [ts3]),
        ]
        lo, hi = _ts_range(records)
        assert lo == ts3
        assert hi == ts2

    def test_records_with_some_none_timestamps(self):
        # Some stations may have no forecourt_updated_at — should be skipped
        ts = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)
        records = [
            tuple([None] * 15 + [None]),
            tuple([None] * 15 + [ts]),
            tuple([None] * 15 + [None]),
        ]
        assert _ts_range(records) == (ts, ts)

    def test_all_none_timestamps(self):
        records = [tuple([None] * 16) for _ in range(3)]
        assert _ts_range(records) == (None, None)
