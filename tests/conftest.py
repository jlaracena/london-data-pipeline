"""Shared pytest fixtures."""

import pytest


@pytest.fixture
def london_coords() -> dict[str, float]:
    """Return the WGS-84 coordinates for central London."""
    return {"latitude": 51.5074, "longitude": -0.1278}


@pytest.fixture
def london_location() -> dict[str, str]:
    """Return common location strings used across extractor tests."""
    return {"city": "London", "country": "GB", "country_name": "United Kingdom"}
