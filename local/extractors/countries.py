"""RestCountries extractor for United Kingdom reference data."""

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

_BASE_URL = "https://restcountries.com/v3.1/alpha/GB"
_SOURCE = "restcountries"


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
def extract_countries() -> list[dict[str, Any]]:
    """Extract static reference data for the United Kingdom from RestCountries.

    Returns:
        A single-element list with a flat dict matching the raw_countries schema.

    Raises:
        httpx.HTTPStatusError: After 3 failed attempts on 5xx responses.
        httpx.ConnectError: After 3 failed attempts on connection failures.
    """
    logger.info("Starting countries extraction from RestCountries")
    ingested_at = datetime.now(timezone.utc)

    with httpx.Client(timeout=30) as client:
        response = client.get(_BASE_URL)
        response.raise_for_status()
        data: list[dict[str, Any]] = response.json()

    country = data[0]
    record: dict[str, Any] = {
        "ingested_at": ingested_at,
        "country_code": country["cca2"],
        "country_name": country["name"]["common"],
        "capital": (country.get("capital") or [""])[0],
        "population": country.get("population"),
        "area_km2": country.get("area"),
        "region": country.get("region"),
        "languages": list((country.get("languages") or {}).values()),
        "currencies": list((country.get("currencies") or {}).keys()),
        "_source": _SOURCE,
    }

    logger.info("Countries extraction complete — country_code=%s", record["country_code"])
    return [record]
