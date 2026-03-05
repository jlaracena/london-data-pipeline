"""Tests for the UK Bank Holidays extractor."""

import pytest
import httpx

from extractors.bank_holidays import extract_bank_holidays

_MOCK_RESPONSE = {
    "england-and-wales": {
        "division": "england-and-wales",
        "events": [
            {"title": "New Year's Day", "date": "2024-01-01", "notes": "", "bunting": True},
            {"title": "Good Friday", "date": "2024-03-29", "notes": "", "bunting": False},
        ],
    },
    "scotland": {
        "division": "scotland",
        "events": [
            {"title": "New Year's Day", "date": "2024-01-01", "notes": "", "bunting": True},
        ],
    },
}


def test_extract_success(mocker):
    """Happy path: API returns 200 with all three divisions."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = _MOCK_RESPONSE
    mock_response.raise_for_status.return_value = None

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_response
    mocker.patch("extractors.bank_holidays.httpx.Client", return_value=mock_client)

    records = extract_bank_holidays()

    assert len(records) == 3  # 2 england-and-wales + 1 scotland
    divisions = {r["division"] for r in records}
    assert "england-and-wales" in divisions
    assert "scotland" in divisions

    ew = next(r for r in records if r["title"] == "New Year's Day" and r["division"] == "england-and-wales")
    assert ew["holiday_date"] == "2024-01-01"
    assert ew["bunting"] is True
    assert ew["_source"] == "gov-uk-bank-holidays"
    assert "ingested_at" in ew


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
    mocker.patch("extractors.bank_holidays.httpx.Client", return_value=mock_client)

    with pytest.raises(httpx.HTTPStatusError):
        extract_bank_holidays()
