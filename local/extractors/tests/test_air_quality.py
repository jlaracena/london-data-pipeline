"""Tests for the air quality extractor (Open-Meteo AQ API)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from extractors.air_quality import extract_air_quality


_MOCK_RESPONSE = {
    "hourly": {
        "time":             ["2026-03-05T00:00", "2026-03-05T01:00"],
        "pm10":             [12.5, 13.0],
        "pm2_5":            [8.1, 8.4],
        "nitrogen_dioxide": [22.3, 21.0],
        "ozone":            [55.0, 54.0],
    }
}


def test_extract_success(mocker):
    mock_response = MagicMock()
    mock_response.json.return_value = _MOCK_RESPONSE
    mock_response.raise_for_status.return_value = None

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.return_value = mock_response

    mocker.patch("extractors.air_quality.httpx.Client", return_value=mock_client)

    records = extract_air_quality()

    # 2 hours × 4 pollutants = 8 records
    assert len(records) == 8
    params = {r["parameter"] for r in records}
    assert params == {"pm10", "pm25", "no2", "o3"}
    assert all(r["location_city"] == "London" for r in records)
    assert all(r["unit"] == "µg/m³" for r in records)
    assert all(r["_source"] == "open-meteo-aq" for r in records)


def test_extract_api_failure(mocker):
    import httpx

    mocker.patch("time.sleep")

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "500", request=MagicMock(), response=MagicMock()
    )

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.return_value = mock_response

    mocker.patch("extractors.air_quality.httpx.Client", return_value=mock_client)

    with pytest.raises(httpx.HTTPStatusError):
        extract_air_quality()
