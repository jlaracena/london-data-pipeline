"""Tests for the UK Police crime extractor."""

import pytest
import httpx

from extractors.crime import extract_crime

_MOCK_RESPONSE = [
    {
        "id": 98765432,
        "persistent_id": "abc123def456",
        "category": "anti-social-behaviour",
        "location_type": "Force",
        "location": {
            "latitude": "51.507400",
            "longitude": "-0.127800",
            "street": {"id": 883498, "name": "On or near Palace Of Westminster"},
        },
        "outcome_status": None,
        "month": "2024-01",
    },
    {
        "id": 98765433,
        "persistent_id": "",
        "category": "bicycle-theft",
        "location_type": "Force",
        "location": {
            "latitude": "51.510000",
            "longitude": "-0.130000",
            "street": {"id": 883499, "name": "On or near Whitehall"},
        },
        "outcome_status": {"category": "Investigation complete; no suspect identified", "date": "2024-02-01"},
        "month": "2024-01",
    },
]


def test_extract_success(mocker):
    """Happy path: API returns 200 with crime records."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = _MOCK_RESPONSE
    mock_response.raise_for_status.return_value = None

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_response
    mocker.patch("extractors.crime.httpx.Client", return_value=mock_client)

    records = extract_crime()

    assert len(records) == 2
    assert records[0]["crime_id"] == 98765432
    assert records[0]["category"] == "anti-social-behaviour"
    assert records[0]["lat"] == 51.5074
    assert records[0]["lon"] == -0.1278
    assert records[0]["street_name"] == "On or near Palace Of Westminster"
    assert records[0]["outcome_status"] is None
    assert records[0]["month"] == "2024-01"
    assert records[0]["_source"] == "uk-police"
    assert "ingested_at" in records[0]

    # Second record: has outcome_status and empty persistent_id
    assert records[1]["crime_id"] == 98765433
    assert records[1]["persistent_id"] is None  # empty string → None
    assert records[1]["outcome_status"] == "Investigation complete; no suspect identified"


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
    mocker.patch("extractors.crime.httpx.Client", return_value=mock_client)

    with pytest.raises(httpx.HTTPStatusError):
        extract_crime()
