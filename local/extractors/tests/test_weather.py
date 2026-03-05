"""Tests for the Open-Meteo weather extractor."""

import pytest
import httpx

from extractors.weather import extract_weather

_MOCK_RESPONSE = {
    "hourly": {
        "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
        "temperature_2m": [5.1, 4.8],
        "windspeed_10m": [12.3, 11.0],
        "weathercode": [3, 1],
    }
}


def test_extract_success(mocker):
    """Happy path: API returns 200 with valid payload."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = _MOCK_RESPONSE
    mock_response.raise_for_status.return_value = None

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_response
    mocker.patch("extractors.weather.httpx.Client", return_value=mock_client)

    records = extract_weather()

    assert len(records) == 2
    assert records[0]["location_city"] == "London"
    assert records[0]["temperature_2m"] == 5.1
    assert records[0]["windspeed_10m"] == 12.3
    assert records[0]["weathercode"] == 3
    assert records[0]["forecast_date"] == "2024-01-01T00:00"
    assert records[0]["_source"] == "open-meteo"
    assert "ingested_at" in records[0]


def test_extract_api_failure(mocker):
    """API 5xx: extractor should raise after exhausting retries."""
    mocker.patch("time.sleep")  # make tenacity retries instant

    mock_request = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.status_code = 500

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value.raise_for_status.side_effect = (
        httpx.HTTPStatusError("Server Error", request=mock_request, response=mock_response)
    )
    mocker.patch("extractors.weather.httpx.Client", return_value=mock_client)

    with pytest.raises(httpx.HTTPStatusError):
        extract_weather()
