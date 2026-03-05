"""Tests for the TfL BikePoint extractor."""

import pytest
import httpx

from extractors.tfl import extract_tfl

_MOCK_RESPONSE = [
    {
        "id": "BikePoints_1",
        "commonName": "River Street , Clerkenwell",
        "lat": 51.529163,
        "lon": -0.10981,
        "additionalProperties": [
            {"key": "NbBikes", "value": "5"},
            {"key": "NbEmptyDocks", "value": "14"},
            {"key": "NbDocks", "value": "19"},
        ],
    },
    {
        "id": "BikePoints_2",
        "commonName": "Phillimore Gardens, Kensington",
        "lat": 51.499607,
        "lon": -0.197574,
        "additionalProperties": [
            {"key": "NbBikes", "value": "3"},
            {"key": "NbEmptyDocks", "value": "23"},
            {"key": "NbDocks", "value": "26"},
        ],
    },
]


def test_extract_success(mocker):
    """Happy path: API returns 200 with valid station data."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = _MOCK_RESPONSE
    mock_response.raise_for_status.return_value = None

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_response
    mocker.patch("extractors.tfl.httpx.Client", return_value=mock_client)

    records = extract_tfl()

    assert len(records) == 2
    assert records[0]["station_id"] == "BikePoints_1"
    assert records[0]["station_name"] == "River Street , Clerkenwell"
    assert records[0]["nb_bikes"] == 5
    assert records[0]["nb_empty_docks"] == 14
    assert records[0]["nb_docks"] == 19
    assert records[0]["_source"] == "tfl-bikepoint"
    assert "ingested_at" in records[0]
    assert "snapshot_date" in records[0]


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
    mocker.patch("extractors.tfl.httpx.Client", return_value=mock_client)

    with pytest.raises(httpx.HTTPStatusError):
        extract_tfl()
