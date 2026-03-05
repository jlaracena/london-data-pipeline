"""Tests for the RestCountries extractor."""

import pytest
import httpx

from extractors.countries import extract_countries

_MOCK_RESPONSE = [
    {
        "cca2": "GB",
        "name": {"common": "United Kingdom", "official": "United Kingdom of Great Britain and Northern Ireland"},
        "capital": ["London"],
        "population": 67215293,
        "area": 242900.0,
        "region": "Europe",
        "languages": {"eng": "English"},
        "currencies": {"GBP": {"name": "Pound sterling", "symbol": "£"}},
    }
]


def test_extract_success(mocker):
    """Happy path: API returns 200 with GB country data."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = _MOCK_RESPONSE
    mock_response.raise_for_status.return_value = None

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_response
    mocker.patch("extractors.countries.httpx.Client", return_value=mock_client)

    records = extract_countries()

    assert len(records) == 1
    r = records[0]
    assert r["country_code"] == "GB"
    assert r["country_name"] == "United Kingdom"
    assert r["capital"] == "London"
    assert r["population"] == 67215293
    assert r["area_km2"] == 242900.0
    assert r["region"] == "Europe"
    assert r["languages"] == ["English"]
    assert r["currencies"] == ["GBP"]
    assert r["_source"] == "restcountries"
    assert "ingested_at" in r


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
    mocker.patch("extractors.countries.httpx.Client", return_value=mock_client)

    with pytest.raises(httpx.HTTPStatusError):
        extract_countries()
