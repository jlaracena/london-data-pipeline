"""NewsAPI extractor for London micro-mobility news."""

import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

_BASE_URL = "https://newsapi.org/v2/everything"
_SOURCE = "newsapi"
_QUERY = "London electric bike OR micro-mobility OR forest ebikes"


def _is_retryable(exc: BaseException) -> bool:
    """Return True for HTTP 5xx errors and network-level failures."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError))


@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
    reraise=True,
)
def extract_news() -> list[dict[str, Any]]:
    """Extract recent London micro-mobility news articles from NewsAPI.

    Reads the API key from the NEWS_API_KEY environment variable.

    Returns:
        List of flat dicts, one per article, matching the raw_news schema.
        Articles with a missing title or URL are silently skipped.

    Raises:
        ValueError: If NEWS_API_KEY is not set.
        httpx.HTTPStatusError: After 3 failed attempts on 5xx responses.
        httpx.ConnectError: After 3 failed attempts on connection failures.
    """
    api_key = os.environ.get("NEWS_API_KEY")
    if not api_key:
        raise ValueError("NEWS_API_KEY environment variable is not set")

    logger.info("Starting news extraction from NewsAPI")
    ingested_at = datetime.now(timezone.utc)

    params: dict[str, Any] = {
        "q": _QUERY,
        "sortBy": "publishedAt",
        "pageSize": 20,
        "language": "en",
    }

    with httpx.Client(timeout=30) as client:
        response = client.get(
            _BASE_URL,
            params=params,
            headers={"X-Api-Key": api_key},
        )
        response.raise_for_status()
        data = response.json()

    articles: list[dict[str, Any]] = data.get("articles", [])
    records: list[dict[str, Any]] = []

    for article in articles:
        url: str | None = article.get("url")
        title: str | None = article.get("title")

        if not url or not title:
            logger.debug("Skipping article with missing title or URL")
            continue

        article_id = hashlib.md5(url.encode()).hexdigest()
        records.append(
            {
                "ingested_at": ingested_at,
                "article_id": article_id,
                "title": title,
                "description": article.get("description"),
                "url": url,
                "published_at": article.get("publishedAt"),
                "source_name": (article.get("source") or {}).get("name"),
                "_source": _SOURCE,
            }
        )

    logger.info("News extraction complete — %d articles", len(records))
    return records
