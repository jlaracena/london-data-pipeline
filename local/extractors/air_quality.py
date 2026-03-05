"""OpenAQ air quality extractor for London."""

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.openaq.org/v2/measurements"
_LOCATION_CITY = "London"
_SOURCE = "openaq"


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
def extract_air_quality() -> list[dict[str, Any]]:
    """Extract recent air quality measurements for London from OpenAQ.

    Returns:
        List of flat dicts, one per measurement, matching the raw_air_quality schema.

    Raises:
        httpx.HTTPStatusError: After 3 failed attempts on 5xx responses.
        httpx.ConnectError: After 3 failed attempts on connection failures.
    """
    logger.info("Starting air quality extraction from OpenAQ")
    ingested_at = datetime.now(timezone.utc)

    params: dict[str, Any] = {
        "city": "London",
        "limit": 100,
        "sort": "desc",
        "order_by": "datetime",
    }

    with httpx.Client(timeout=30) as client:
        response = client.get(_BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

    results: list[dict[str, Any]] = data.get("results", [])
    records = [
        {
            "ingested_at": ingested_at,
            "location_city": _LOCATION_CITY,
            "location_id": r["locationId"],
            "parameter": r["parameter"],
            "value": r["value"],
            "unit": r["unit"],
            "measured_at": r["date"]["utc"],
            "_source": _SOURCE,
        }
        for r in results
    ]

    logger.info("Air quality extraction complete — %d records", len(records))
    return records
