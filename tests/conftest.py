"""
Shared pytest fixtures.

This file is automatically discovered by pytest. Fixtures defined here
are available to every test in the tests/ directory.
"""
import os
from pathlib import Path

import pytest


# Set dummy env vars BEFORE any test imports anything from etl/.
# etl.config validates DATABASE_URL at import-time and would crash otherwise.
# Using setdefault so a real value (e.g. for integration tests) is preserved.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test_dummy")


@pytest.fixture
def sample_csv_path() -> Path:
    """
    Path to the sample CSV (12 real stations from the gov.uk feed).

    The file is committed to the repo because parser tests need to
    run offline in CI without external dependencies.
    """
    return Path(__file__).parent / "fixtures" / "sample_fuel_data.csv"
