"""
Tests for shared.postcode_validator.

Mostly mirrors the cases listed in spec section 13.2. Adds a few edge cases
around whitespace and short input.
"""
from shared.postcode_validator import is_valid_uk_postcode, normalize_postcode


# ───────────────────── is_valid_uk_postcode ─────────────────────

class TestIsValid:
    def test_full_format(self):
        assert is_valid_uk_postcode("CO4 9ED")
        assert is_valid_uk_postcode("SW1A 1AA")  # Buckingham Palace
        assert is_valid_uk_postcode("M1 1AE")    # Manchester (1-letter area)
        assert is_valid_uk_postcode("EC1A 1BB")  # London (with letter in district)

    def test_no_space(self):
        # Both spaced and unspaced forms valid
        assert is_valid_uk_postcode("CO49ED")
        assert is_valid_uk_postcode("SW1A1AA")

    def test_lowercase(self):
        assert is_valid_uk_postcode("co4 9ed")
        assert is_valid_uk_postcode("sw1a 1aa")

    def test_mixed_case(self):
        assert is_valid_uk_postcode("Co4 9eD")

    def test_invalid_too_short(self):
        assert not is_valid_uk_postcode("CO4")
        assert not is_valid_uk_postcode("ABC")

    def test_invalid_too_long(self):
        assert not is_valid_uk_postcode("CO4 9ED 9ED")

    def test_invalid_random_text(self):
        assert not is_valid_uk_postcode("hello world")
        assert not is_valid_uk_postcode("123456")
        assert not is_valid_uk_postcode("ABCDEF")  # all letters, no digits

    def test_invalid_empty_string(self):
        assert not is_valid_uk_postcode("")

    def test_invalid_whitespace_only(self):
        assert not is_valid_uk_postcode("   ")

    def test_strips_surrounding_whitespace(self):
        # Surrounding whitespace shouldn't reject — leading/trailing is benign
        assert is_valid_uk_postcode("  CO4 9ED  ")


# ───────────────────── normalize_postcode ─────────────────────

class TestNormalize:
    def test_lowercase_to_upper(self):
        assert normalize_postcode("co4 9ed") == "CO4 9ED"

    def test_no_space_inserts_space(self):
        assert normalize_postcode("CO49ED") == "CO4 9ED"

    def test_extra_internal_spaces(self):
        assert normalize_postcode("CO 4 9 ED") == "CO4 9ED"
        assert normalize_postcode("CO  4  9ED") == "CO4 9ED"

    def test_strips_surrounding_whitespace(self):
        assert normalize_postcode("  CO4 9ED  ") == "CO4 9ED"

    def test_idempotent(self):
        # Running normalize twice on the same input gives the same result
        once = normalize_postcode("co49ed")
        twice = normalize_postcode(once)
        assert once == twice == "CO4 9ED"

    def test_short_input_returns_as_is(self):
        # No space inserted if input is too short to be a real postcode.
        # Caller should validate first; this is just defensive.
        assert normalize_postcode("ab") == "AB"

    def test_long_postcodes(self):
        assert normalize_postcode("sw1a1aa") == "SW1A 1AA"
        assert normalize_postcode("ec1a 1bb") == "EC1A 1BB"
