"""Open-Meteo weather extractor for London."""

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.open-meteo.com/v1/forecast"
_LOCATION_CITY = "London"
_SOURCE = "open-meteo"


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
def extract_weather() -> list[dict[str, Any]]:
    """Extract hourly weather forecast for London from Open-Meteo.

    Returns:
        List of flat dicts, one per forecast hour, matching the raw_weather schema.

    Raises:
        httpx.HTTPStatusError: After 3 failed attempts on 5xx responses.
        httpx.ConnectError: After 3 failed attempts on connection failures.
    """
    logger.info("Starting weather extraction from Open-Meteo")
    ingested_at = datetime.now(timezone.utc)

    params = {
        "latitude": 51.5074,
        "longitude": -0.1278,
        "hourly": "temperature_2m,windspeed_10m,weathercode",
        "timezone": "UTC",
    }

    with httpx.Client(timeout=30) as client:
        response = client.get(_BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

    hourly = data["hourly"]
    times: list[str] = hourly["time"]
    temps: list[float | None] = hourly["temperature_2m"]
    winds: list[float | None] = hourly["windspeed_10m"]
    codes: list[int | None] = hourly["weathercode"]

    records = [
        {
            "ingested_at": ingested_at,
            "location_city": _LOCATION_CITY,
            "temperature_2m": temps[i],
            "windspeed_10m": winds[i],
            "weathercode": codes[i],
            "forecast_date": times[i],
            "_source": _SOURCE,
        }
        for i in range(len(times))
    ]

    logger.info("Weather extraction complete — %d hourly records", len(records))
    return records
