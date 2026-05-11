"""
Tests for shared.csv_parser.

Run against tests/fixtures/sample_fuel_data.csv (12 real stations from
the gov.uk feed). The fixture is intentionally chosen to cover several
edge cases:

  Row 0 (Tesco Pontypool):    no E5 price; opening hours are 'all zeros' (no data)
  Row 1 (Gulf Winchcombe):    24/7 (all 7 days is_24_hours=True)
  Row 2 (TOTAL Scorton):      regular hours 07:00-22:00 every day
  Row 5 (Dolbears Garage):    non-standard brand name
  Row 8 (Texaco Spar Drumahoe):  Northern Ireland station
  Row 11 (Logan Brothers):    no E10 price
"""
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from shared.csv_parser import (
    parse_fuel_csv,
    ParsedStation,
    ParsedPrice,
)


@pytest.fixture
def all_stations(sample_csv_path):
    """Parse the entire fixture once per test."""
    return list(parse_fuel_csv(sample_csv_path))


@pytest.fixture
def stations_by_id(all_stations):
    """Lookup by station_id for cherry-picking specific cases."""
    return {s.station_id: s for s in all_stations}


# ─────────────────────── basic parsing ───────────────────────

class TestBasic:
    def test_parses_all_rows(self, all_stations):
        # 12 stations in the fixture
        assert len(all_stations) == 12

    def test_each_station_has_required_fields(self, all_stations):
        for s in all_stations:
            assert isinstance(s, ParsedStation)
            # station_id is the only field that must always be present
            assert s.station_id
            assert isinstance(s.station_id, str)

    def test_returns_iterator(self, sample_csv_path):
        # parse_fuel_csv must be lazy (generator), not a list
        result = parse_fuel_csv(sample_csv_path)
        assert not isinstance(result, list)
        # but iterable
        first = next(iter(result))
        assert first.station_id


# ─────────────────────── station fields ───────────────────────

class TestStationFields:
    def test_name_extracted(self, all_stations):
        # First station is Tesco Pontypool
        first = all_stations[0]
        assert first.name == "PONTYPOOL SUPERSTORE - PETROL FILLING STATION"

    def test_brand_extracted(self, all_stations):
        first = all_stations[0]
        assert first.brand == "TESCO"

    def test_postcode_extracted(self, all_stations):
        first = all_stations[0]
        assert first.postcode == "NP4 6JU"

    def test_coordinates_parsed_as_floats(self, all_stations):
        first = all_stations[0]
        assert isinstance(first.latitude, float)
        assert isinstance(first.longitude, float)
        # Pontypool is roughly 51.7 N, -3.0 W
        assert 51.0 < first.latitude < 52.0
        assert -3.5 < first.longitude < -2.5

    def test_address_lines_concatenated(self, all_stations):
        first = all_stations[0]
        assert first.address == "LOWER BRIDGE STREET"  # row 0 has no line_2

    def test_supermarket_flag(self, all_stations):
        first = all_stations[0]  # Tesco Pontypool, is_supermarket = False
        assert first.is_supermarket is False

    def test_closure_flags_parsed(self, all_stations):
        first = all_stations[0]
        assert first.is_temp_closed is False
        # Permanent_closure is '' in the CSV — should become None
        assert first.is_perm_closed is None


# ─────────────────────── opening hours ───────────────────────

class TestOpeningHours:
    def test_24h_station_all_days_flagged(self, all_stations):
        # Row 1 = Gulf Winchcombe, 24/7
        gulf = all_stations[1]
        for day in ("monday", "tuesday", "wednesday", "thursday",
                    "friday", "saturday", "sunday"):
            assert gulf.opening_hours[day] is not None
            assert gulf.opening_hours[day]["is_24h"] is True

    def test_24h_derived_flag_true(self, all_stations):
        gulf = all_stations[1]
        assert gulf.is_24h is True

    def test_regular_hours_extracted(self, all_stations):
        # Row 2 = TOTAL Scorton, 07:00-22:00 every day
        total = all_stations[2]
        monday = total.opening_hours["monday"]
        assert monday["open"] == "07:00"
        assert monday["close"] == "22:00"
        assert monday["is_24h"] is False

    def test_24h_derived_flag_false_for_regular(self, all_stations):
        total = all_stations[2]
        assert total.is_24h is False

    def test_seconds_trimmed_from_times(self, all_stations):
        # CSV format is "07:00:00", we store "07:00"
        total = all_stations[2]
        for day_data in (total.opening_hours[d] for d in ("monday",)):
            # No colons-and-seconds at end
            assert day_data["open"].count(":") == 1
            assert day_data["close"].count(":") == 1

    def test_no_data_day_is_none(self, all_stations):
        # Row 0 = Tesco Pontypool, all zeros in opening hours
        tesco = all_stations[0]
        # All weekdays should be None
        for day in ("monday", "tuesday", "wednesday", "thursday",
                    "friday", "saturday", "sunday"):
            assert tesco.opening_hours[day] is None, \
                f"Expected None for {day}, got {tesco.opening_hours[day]}"

    def test_no_data_station_is_24h_is_none(self, all_stations):
        tesco = all_stations[0]
        # All days are None — we can't derive 24h status
        assert tesco.is_24h is None

    def test_bank_holiday_key_present(self, all_stations):
        for s in all_stations:
            assert "bank_holiday" in s.opening_hours


# ─────────────────────── amenities ───────────────────────

class TestAmenities:
    def test_amenities_dict_has_expected_keys(self, all_stations):
        first = all_stations[0]
        expected = {
            "customer_toilets", "car_wash", "air_pump", "water_filling",
            "twenty_four_hour_fuel", "adblue_pumps", "adblue_packaged",
            "lpg_pumps",
        }
        assert set(first.amenities.keys()) == expected

    def test_amenities_are_booleans(self, all_stations):
        for s in all_stations:
            for k, v in s.amenities.items():
                assert isinstance(v, bool), f"{s.station_id}.{k} is {type(v)}"

    def test_amenities_extracted_correctly(self, all_stations):
        # Row 0 = Tesco: customer_toilets=True, car_wash=False, water_filling=True
        tesco = all_stations[0]
        assert tesco.amenities["customer_toilets"] is True
        assert tesco.amenities["car_wash"] is False
        assert tesco.amenities["water_filling"] is True


# ─────────────────────── prices ───────────────────────

class TestPrices:
    def test_prices_use_decimal(self, all_stations):
        for s in all_stations:
            for p in s.prices:
                assert isinstance(p.price_pence, Decimal)

    def test_only_four_fuel_types(self, all_stations):
        # B10 and HVO should be excluded entirely
        for s in all_stations:
            for p in s.prices:
                assert p.fuel_type in {"e10", "e5", "b7s", "b7p"}

    def test_missing_fuel_means_no_price_record(self, all_stations):
        # Row 0 (Tesco) has no E5 price — so no ParsedPrice with fuel_type='e5'
        tesco = all_stations[0]
        e5_records = [p for p in tesco.prices if p.fuel_type == "e5"]
        assert e5_records == []

    def test_missing_e10_handled(self, all_stations):
        # Row 11 (Logan Brothers) has no E10 price
        logan = all_stations[11]
        e10_records = [p for p in logan.prices if p.fuel_type == "e10"]
        assert e10_records == []

    def test_prices_lowercase_fuel_codes(self, all_stations):
        # We store lowercase (e10), CSV uses uppercase (E10)
        first = all_stations[0]
        for p in first.prices:
            assert p.fuel_type == p.fuel_type.lower()

    def test_specific_price_value(self, all_stations):
        # Tesco Pontypool E10 = 152.9p
        tesco = all_stations[0]
        e10 = next(p for p in tesco.prices if p.fuel_type == "e10")
        assert e10.price_pence == Decimal("152.9")

    def test_price_station_id_matches_parent(self, all_stations):
        for s in all_stations:
            for p in s.prices:
                assert p.station_id == s.station_id


# ─────────────────────── timestamps ───────────────────────

class TestTimestamps:
    def test_station_timestamp_is_utc(self, all_stations):
        # forecourt_update_timestamp is in 'YYYY-MM-DD HH:MM:SS' format,
        # we treat it as UTC
        first = all_stations[0]
        assert first.forecourt_updated_at is not None
        assert first.forecourt_updated_at.tzinfo == timezone.utc

    def test_per_fuel_timestamp_parsed(self, all_stations):
        # CSV format: 'Mon Apr 27 2026 14:58:00 GMT+0000 (Coordinated...)'
        first = all_stations[0]
        prices_with_ts = [p for p in first.prices if p.forecourt_updated_at]
        assert prices_with_ts, "Expected at least one price with a timestamp"
        for p in prices_with_ts:
            assert isinstance(p.forecourt_updated_at, datetime)
            assert p.forecourt_updated_at.tzinfo == timezone.utc


# ─────────────────────── error handling ───────────────────────

class TestErrorHandling:
    def test_missing_required_columns_raises(self, tmp_path):
        # Build a CSV that's missing 'forecourts.node_id'
        bad = tmp_path / "bad.csv"
        bad.write_text("col1,col2\nval1,val2\n", encoding="utf-8")
        with pytest.raises(ValueError, match="missing required columns"):
            list(parse_fuel_csv(bad))

    def test_empty_node_id_skips_row(self, tmp_path):
        # Build a minimal valid-format CSV with one row that has empty node_id
        bad = tmp_path / "empty_id.csv"
        bad.write_text(
            "forecourts.node_id,forecourt_update_timestamp\n"
            ",2026-01-01 00:00:00\n",
            encoding="utf-8",
        )
        result = list(parse_fuel_csv(bad))
        assert result == []  # row was skipped, not raised
