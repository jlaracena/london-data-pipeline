"""Tests for the OpenAQ air quality extractor."""

import pytest
import httpx

from extractors.air_quality import extract_air_quality

_MOCK_RESPONSE = {
    "meta": {"found": 2, "page": 1, "limit": 100},
    "results": [
        {
            "locationId": 42,
            "location": "London Marylebone Road",
            "parameter": "pm25",
            "value": 12.5,
            "unit": "µg/m³",
            "date": {"utc": "2024-01-01T12:00:00Z", "local": "2024-01-01T12:00:00+00:00"},
            "city": "London",
        },
        {
            "locationId": 42,
            "location": "London Marylebone Road",
            "parameter": "no2",
            "value": 30.1,
            "unit": "µg/m³",
            "date": {"utc": "2024-01-01T12:00:00Z", "local": "2024-01-01T12:00:00+00:00"},
            "city": "London",
        },
    ],
}


def test_extract_success(mocker):
    """Happy path: API returns 200 with valid payload."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = _MOCK_RESPONSE
    mock_response.raise_for_status.return_value = None

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_response
    mocker.patch("extractors.air_quality.httpx.Client", return_value=mock_client)

    records = extract_air_quality()

    assert len(records) == 2
    assert records[0]["location_city"] == "London"
    assert records[0]["location_id"] == 42
    assert records[0]["parameter"] == "pm25"
    assert records[0]["value"] == 12.5
    assert records[0]["unit"] == "µg/m³"
    assert records[0]["measured_at"] == "2024-01-01T12:00:00Z"
    assert records[0]["_source"] == "openaq"
    assert "ingested_at" in records[0]


def test_extract_api_failure(mocker):
    """API 5xx: extractor should raise after exhausting retries."""
    mocker.patch("time.sleep")

    mock_request = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.status_code = 500

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value.raise_for_status.side_effect = (
        httpx.HTTPStatusError("Server Error", request=mock_request, response=mock_response)
    )
    mocker.patch("extractors.air_quality.httpx.Client", return_value=mock_client)

    with pytest.raises(httpx.HTTPStatusError):
        extract_air_quality()
