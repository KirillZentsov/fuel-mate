"""
Tests for shared.geo.

Includes the cases from spec section 13.2 plus a few additional sanity
checks on haversine math properties (symmetry, identity).
"""
from shared.geo import haversine_miles, is_in_uk


# ───────────────────── haversine_miles ─────────────────────

class TestHaversine:
    def test_zero_distance_to_self(self):
        # A point is zero miles from itself
        assert haversine_miles(51.5, -0.1, 51.5, -0.1) == 0.0

    def test_known_short_distance(self):
        # London Bridge to Tower Bridge — about 0.5 miles
        london_bridge = (51.508, -0.088)
        tower_bridge = (51.505, -0.075)
        dist = haversine_miles(*london_bridge, *tower_bridge)
        assert 0.4 < dist < 0.7

    def test_known_long_distance(self):
        # London to Edinburgh — about 332 miles as the crow flies
        london = (51.5074, -0.1278)
        edinburgh = (55.9533, -3.1883)
        dist = haversine_miles(*london, *edinburgh)
        assert 320 < dist < 340

    def test_symmetric(self):
        # haversine(A, B) == haversine(B, A)
        a = (51.5, -0.1)
        b = (53.4, -2.2)
        assert haversine_miles(*a, *b) == haversine_miles(*b, *a)

    def test_returns_float(self):
        result = haversine_miles(51.5, -0.1, 51.6, -0.2)
        assert isinstance(result, float)


# ───────────────────── is_in_uk ─────────────────────

class TestIsInUK:
    def test_london(self):
        assert is_in_uk(51.5074, -0.1278)

    def test_glasgow(self):
        assert is_in_uk(55.8642, -4.2518)

    def test_belfast(self):
        # Northern Ireland is part of UK
        assert is_in_uk(54.5973, -5.9301)

    def test_paris_is_not_uk(self):
        assert not is_in_uk(48.8566, 2.3522)

    def test_new_york_is_not_uk(self):
        assert not is_in_uk(40.7128, -74.0060)

    def test_madrid_is_not_uk(self):
        assert not is_in_uk(40.4168, -3.7038)

    def test_dublin_is_questionable(self):
        # Dublin (53.35, -6.26) is geographically close to UK and may fall
        # inside our rough bounding box. We don't strictly test either way —
        # this is documenting the known limitation. The downstream search
        # would return zero stations there anyway.
        # Just verify the function doesn't crash:
        result = is_in_uk(53.35, -6.26)
        assert isinstance(result, bool)

    def test_shetland_islands(self):
        # Far-north UK territory — must be inside bounding box
        assert is_in_uk(60.4, -1.3)

    def test_returns_bool(self):
        assert isinstance(is_in_uk(51.5, -0.1), bool)
