"""
Tests for the JSON parser path in shared.csv_parser.

These tests use fixtures derived from real Fuel Finder API responses
(captured during integration testing). They cover:

  - Happy path: well-formed stations + prices merge correctly
  - Fuel type mapping: B7_STANDARD → b7s, etc.
  - Skipped fuels: B10 and HVO are dropped
  - opening_times normalisation: HH:MM:SS → HH:MM, is_24_hours → is_24h
  - bank_holiday key remapping (open_time → open)
  - amenities: array → flat dict with `air_pump_or_screenwash` → `air_pump`
  - Stations with no prices are still included
  - Orphan price records (no matching station) are logged, not crashing
  - Timestamps: 2026-02-17T16:00:00.000Z parses to tz-aware UTC datetime

The tests run without the network — fixtures are inline dicts.
"""
from decimal import Decimal
from datetime import datetime, timezone

import pytest

from shared.csv_parser import (
    parse_api_response,
    ParsedStation,
    ParsedPrice,
)


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def station_basic():
    """A typical /pfs response object — 24/7 station with all amenities."""
    return {
        "node_id": "abc123" + "0" * 58,  # 64-char hex shape
        "public_phone_number": "+442071930000",
        "trading_name": "Supertest Garage",
        "is_same_trading_and_brand_name": False,
        "brand_name": "SUPERTEST",
        "temporary_closure": False,
        "permanent_closure": False,
        "permanent_closure_date": None,
        "is_motorway_service_station": False,
        "is_supermarket_service_station": False,
        "location": {
            "address_line_1": "14 London Road",
            "address_line_2": "Fuelville",
            "city": "Loughborough",
            "country": "England",
            "county": "Leicestershire",
            "postcode": "LE11 9AA",
            "latitude": 52.7721,
            "longitude": -1.2062,
        },
        "amenities": [
            "customer_toilets",
            "car_wash",
            "air_pump_or_screenwash",
            "adblue_pumps",
        ],
        "opening_times": {
            "usual_days": {
                d: {"open": "00:00:00", "close": "00:00:00", "is_24_hours": True}
                for d in ("monday", "tuesday", "wednesday", "thursday",
                          "friday", "saturday", "sunday")
            },
            "bank_holiday": {
                "type": "standard",
                "open_time": "00:00:00",
                "close_time": "00:00:00",
                "is_24_hours": True,
            },
        },
        "fuel_types": ["E10", "E5", "B7_STANDARD"],
    }


@pytest.fixture
def prices_basic(station_basic):
    """Matching /pfs/fuel-prices object — three fuels with current prices."""
    nid = station_basic["node_id"]
    return {
        "node_id": nid,
        "public_phone_number": "+442071930000",
        "trading_name": "Supertest Garage",
        "fuel_prices": [
            {
                "fuel_type": "E10",
                "price": 132.9,
                "price_last_updated": "2026-02-17T16:03:04.938Z",
                "price_change_effective_timestamp": "2026-02-17T16:00:00.000Z",
            },
            {
                "fuel_type": "E5",
                "price": 144.9,
                "price_last_updated": "2026-02-17T16:03:04.938Z",
                "price_change_effective_timestamp": "2026-02-17T16:00:00.000Z",
            },
            {
                "fuel_type": "B7_STANDARD",
                "price": 141.9,
                "price_last_updated": "2026-02-17T16:03:04.938Z",
                "price_change_effective_timestamp": "2026-02-17T16:00:00.000Z",
            },
        ],
    }


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────

class TestHappyPath:
    def test_basic_merge(self, station_basic, prices_basic):
        """Stations and prices merge into one ParsedStation with prices attached."""
        result = parse_api_response([station_basic], [prices_basic])
        assert len(result) == 1
        s = result[0]
        assert isinstance(s, ParsedStation)
        assert s.station_id == station_basic["node_id"]
        assert s.name == "Supertest Garage"
        assert s.brand == "SUPERTEST"
        assert s.postcode == "LE11 9AA"
        assert s.latitude == pytest.approx(52.7721)
        assert s.longitude == pytest.approx(-1.2062)
        assert len(s.prices) == 3

    def test_address_concatenation(self, station_basic, prices_basic):
        """Address_line_1 + _2 are joined with ', '."""
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].address == "14 London Road, Fuelville"

    def test_address_line_2_optional(self, station_basic, prices_basic):
        """Missing/empty address_line_2 leaves just line_1."""
        station_basic["location"]["address_line_2"] = None
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].address == "14 London Road"


# ──────────────────────────────────────────────────────────────────────
# Fuel type mapping
# ──────────────────────────────────────────────────────────────────────

class TestFuelTypes:
    def test_b7_standard_maps_to_b7s(self, station_basic, prices_basic):
        """API's B7_STANDARD → DB's 'b7s'."""
        result = parse_api_response([station_basic], [prices_basic])
        codes = {p.fuel_type for p in result[0].prices}
        assert "b7s" in codes

    def test_b7_premium_maps_to_b7p(self, station_basic, prices_basic):
        prices_basic["fuel_prices"] = [{
            "fuel_type": "B7_PREMIUM",
            "price": 155.9,
            "price_last_updated": "2026-02-17T16:03:04.938Z",
            "price_change_effective_timestamp": "2026-02-17T16:00:00.000Z",
        }]
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].prices[0].fuel_type == "b7p"

    def test_b10_ignored(self, station_basic, prices_basic):
        """B10 is not in our domain — should be dropped silently."""
        prices_basic["fuel_prices"].append({
            "fuel_type": "B10",
            "price": 199.9,
            "price_last_updated": "2026-02-17T16:03:04.938Z",
            "price_change_effective_timestamp": "2026-02-17T16:00:00.000Z",
        })
        result = parse_api_response([station_basic], [prices_basic])
        codes = {p.fuel_type for p in result[0].prices}
        assert "B10" not in codes
        assert "b10" not in codes
        assert len(result[0].prices) == 3  # B10 dropped, others kept

    def test_hvo_ignored(self, station_basic, prices_basic):
        """HVO is not in our domain — drop silently."""
        prices_basic["fuel_prices"].append({
            "fuel_type": "HVO",
            "price": 150.0,
            "price_last_updated": "2026-02-17T16:03:04.938Z",
            "price_change_effective_timestamp": "2026-02-17T16:00:00.000Z",
        })
        result = parse_api_response([station_basic], [prices_basic])
        assert len(result[0].prices) == 3

    def test_case_insensitive_fuel_type(self, station_basic, prices_basic):
        """Lowercased fuel_type still matches (be defensive)."""
        prices_basic["fuel_prices"] = [{
            "fuel_type": "b7_standard",
            "price": 141.9,
            "price_last_updated": "2026-02-17T16:03:04.938Z",
            "price_change_effective_timestamp": "2026-02-17T16:00:00.000Z",
        }]
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].prices[0].fuel_type == "b7s"

    def test_price_is_decimal(self, station_basic, prices_basic):
        """Prices arrive as JSON floats; we store as Decimal."""
        result = parse_api_response([station_basic], [prices_basic])
        for p in result[0].prices:
            assert isinstance(p.price_pence, Decimal)


# ──────────────────────────────────────────────────────────────────────
# Opening hours normalisation
# ──────────────────────────────────────────────────────────────────────

class TestOpeningHours:
    def test_24h_when_all_days_flagged(self, station_basic, prices_basic):
        """All 7 days `is_24_hours: true` → station-level is_24h=True."""
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].is_24h is True

    def test_not_24h_with_normal_hours(self, station_basic, prices_basic):
        """Normal opening hours → is_24h=False."""
        for d in ("monday", "tuesday", "wednesday", "thursday", "friday",
                  "saturday", "sunday"):
            station_basic["opening_times"]["usual_days"][d] = {
                "open": "06:00:00", "close": "22:00:00", "is_24_hours": False,
            }
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].is_24h is False
        assert result[0].opening_hours["monday"]["open"] == "06:00"
        assert result[0].opening_hours["monday"]["close"] == "22:00"
        assert result[0].opening_hours["monday"]["is_24h"] is False

    def test_seconds_stripped(self, station_basic, prices_basic):
        """API gives HH:MM:SS — we store HH:MM."""
        station_basic["opening_times"]["usual_days"]["monday"] = {
            "open": "06:00:01", "close": "23:00:01", "is_24_hours": False,
        }
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].opening_hours["monday"]["open"] == "06:00"
        assert result[0].opening_hours["monday"]["close"] == "23:00"

    def test_bank_holiday_key_remap(self, station_basic, prices_basic):
        """bank_holiday's open_time/close_time → open/close."""
        station_basic["opening_times"]["bank_holiday"] = {
            "type": "bank holiday",
            "open_time": "08:00:00",
            "close_time": "20:00:00",
            "is_24_hours": False,
        }
        result = parse_api_response([station_basic], [prices_basic])
        bh = result[0].opening_hours["bank_holiday"]
        assert bh is not None
        assert bh["open"] == "08:00"
        assert bh["close"] == "20:00"
        assert bh["is_24h"] is False

    def test_all_zeros_means_no_data(self, station_basic, prices_basic):
        """A day with open=close=00:00 and not 24h is treated as missing."""
        station_basic["opening_times"]["usual_days"]["sunday"] = {
            "open": "00:00:00", "close": "00:00:00", "is_24_hours": False,
        }
        # Override the rest of the days to something normal so is_24h derive
        # doesn't trip on the no-data sentinel.
        for d in ("monday", "tuesday", "wednesday", "thursday", "friday",
                  "saturday"):
            station_basic["opening_times"]["usual_days"][d] = {
                "open": "06:00:00", "close": "22:00:00", "is_24_hours": False,
            }
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].opening_hours["sunday"] is None


# ──────────────────────────────────────────────────────────────────────
# Amenities mapping
# ──────────────────────────────────────────────────────────────────────

class TestAmenities:
    def test_present_amenities_are_true(self, station_basic, prices_basic):
        """Amenities in the array become True in our dict."""
        result = parse_api_response([station_basic], [prices_basic])
        a = result[0].amenities
        assert a["customer_toilets"] is True
        assert a["car_wash"] is True
        assert a["adblue_pumps"] is True

    def test_absent_amenities_are_false(self, station_basic, prices_basic):
        """Anything not in the API array is False in our dict."""
        result = parse_api_response([station_basic], [prices_basic])
        a = result[0].amenities
        assert a["lpg_pumps"] is False
        assert a["water_filling"] is False
        assert a["twenty_four_hour_fuel"] is False

    def test_air_pump_or_screenwash_renamed(self, station_basic, prices_basic):
        """API name `air_pump_or_screenwash` becomes our `air_pump`."""
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].amenities["air_pump"] is True
        assert "air_pump_or_screenwash" not in result[0].amenities

    def test_empty_amenities_list(self, station_basic, prices_basic):
        """Empty amenities array → all False, no crash."""
        station_basic["amenities"] = []
        result = parse_api_response([station_basic], [prices_basic])
        assert all(v is False for v in result[0].amenities.values())


# ──────────────────────────────────────────────────────────────────────
# Timestamps
# ──────────────────────────────────────────────────────────────────────

class TestTimestamps:
    def test_price_timestamp_parsed_as_utc(self, station_basic, prices_basic):
        """ISO 8601 with Z suffix → tz-aware UTC datetime."""
        result = parse_api_response([station_basic], [prices_basic])
        ts = result[0].prices[0].forecourt_updated_at
        assert ts is not None
        assert ts.tzinfo == timezone.utc
        assert ts.year == 2026 and ts.month == 2 and ts.day == 17

    def test_station_timestamp_is_none(self, station_basic, prices_basic):
        """API doesn't have a station-level update timestamp."""
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].forecourt_updated_at is None


# ──────────────────────────────────────────────────────────────────────
# Edge cases
# ──────────────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_station_without_prices(self, station_basic):
        """A station with no matching price record → ParsedStation with empty prices."""
        result = parse_api_response([station_basic], [])
        assert len(result) == 1
        assert result[0].prices == []

    def test_orphan_prices_logged(self, prices_basic, caplog):
        """Prices for non-existent stations → warning, no crash."""
        result = parse_api_response([], [prices_basic])
        assert result == []
        # The warning should mention orphan price records
        assert any("no matching station" in r.message for r in caplog.records)

    def test_empty_inputs(self):
        """Two empty arrays → empty output, no crash."""
        result = parse_api_response([], [])
        assert result == []

    def test_missing_node_id_skipped(self, station_basic, prices_basic):
        """A station entry with empty node_id is silently dropped."""
        station_basic["node_id"] = ""
        result = parse_api_response([station_basic], [prices_basic])
        assert result == []

    def test_malformed_entry_isolated(self, station_basic, prices_basic, caplog):
        """One bad entry doesn't kill processing of good entries."""
        bad_station = {"node_id": "bad", "location": "not_a_dict"}  # boom on .get
        result = parse_api_response([bad_station, station_basic], [prices_basic])
        # Good station survived
        assert len(result) == 1
        assert result[0].station_id == station_basic["node_id"]

    def test_temporary_closure_tri_state(self, station_basic, prices_basic):
        """temporary_closure: bool true/false → tri-state mapping."""
        station_basic["temporary_closure"] = True
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].is_temp_closed is True

    def test_permanent_closure_null_means_none(self, station_basic, prices_basic):
        """permanent_closure: null → None in tri-state."""
        station_basic["permanent_closure"] = None
        result = parse_api_response([station_basic], [prices_basic])
        assert result[0].is_perm_closed is None
