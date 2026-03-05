"""Tests for the NewsAPI extractor."""

import pytest
import httpx

from extractors.news import extract_news

_MOCK_RESPONSE = {
    "status": "ok",
    "totalResults": 2,
    "articles": [
        {
            "source": {"id": "bbc-news", "name": "BBC News"},
            "title": "London e-bike scheme expands",
            "description": "The city adds 500 new electric bikes.",
            "url": "https://bbc.co.uk/news/1",
            "publishedAt": "2024-01-01T09:00:00Z",
        },
        {
            "source": {"id": None, "name": "The Guardian"},
            "title": "Micro-mobility surge in London",
            "description": "Demand for e-bikes hits record high.",
            "url": "https://theguardian.com/news/2",
            "publishedAt": "2024-01-01T08:00:00Z",
        },
    ],
}


def test_extract_success(mocker):
    """Happy path: API returns 200 with valid articles."""
    mocker.patch.dict("os.environ", {"NEWS_API_KEY": "test-key"})

    mock_response = mocker.Mock()
    mock_response.json.return_value = _MOCK_RESPONSE
    mock_response.raise_for_status.return_value = None

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_response
    mocker.patch("extractors.news.httpx.Client", return_value=mock_client)

    records = extract_news()

    assert len(records) == 2
    assert records[0]["title"] == "London e-bike scheme expands"
    assert records[0]["source_name"] == "BBC News"
    assert records[0]["published_at"] == "2024-01-01T09:00:00Z"
    assert records[0]["_source"] == "newsapi"
    assert len(records[0]["article_id"]) == 32  # MD5 hex digest
    assert "ingested_at" in records[0]


def test_skip_articles_with_missing_fields(mocker):
    """Articles with None title or URL should be excluded from output."""
    mocker.patch.dict("os.environ", {"NEWS_API_KEY": "test-key"})

    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "articles": [
            {"source": {}, "title": None, "url": "https://example.com/1", "publishedAt": None},
            {"source": {}, "title": "Valid", "url": None, "publishedAt": None},
            {"source": {}, "title": "Valid", "url": "https://example.com/3", "publishedAt": None},
        ]
    }
    mock_response.raise_for_status.return_value = None

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_response
    mocker.patch("extractors.news.httpx.Client", return_value=mock_client)

    records = extract_news()

    assert len(records) == 1
    assert records[0]["title"] == "Valid"


def test_extract_api_failure(mocker):
    """API 5xx: extractor should raise after exhausting retries."""
    mocker.patch("time.sleep")
    mocker.patch.dict("os.environ", {"NEWS_API_KEY": "test-key"})

    mock_request = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.status_code = 500

    mock_client = mocker.MagicMock()
    mock_client.__enter__.return_value.get.return_value.raise_for_status.side_effect = (
        httpx.HTTPStatusError("Server Error", request=mock_request, response=mock_response)
    )
    mocker.patch("extractors.news.httpx.Client", return_value=mock_client)

    with pytest.raises(httpx.HTTPStatusError):
        extract_news()


def test_missing_api_key_raises(mocker):
    """Missing NEWS_API_KEY should raise ValueError before any HTTP call."""
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(ValueError, match="NEWS_API_KEY"):
        extract_news()
