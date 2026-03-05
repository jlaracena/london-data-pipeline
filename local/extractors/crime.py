"""UK Police crime data extractor for central London.

Fetches street-level crimes near the centre of London (Westminster)
from the data.police.uk API. Useful for correlating safety conditions
with micro-mobility ridership patterns.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

_BASE_URL = "https://data.police.uk/api/crimes-street/all-crime"
_SOURCE = "uk-police"

# Central London coordinates (Westminster)
_LAT = 51.5074
_LNG = -0.1278


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError))


@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
    reraise=True,
)
def extract_crime() -> list[dict[str, Any]]:
    """Extract street-level crime data for central London from the UK Police API.

    Uses the latest available month by default (no ``date`` param).
    Returns one record per reported crime incident.

    Returns:
        List of flat dicts matching the raw_crime schema.

    Raises:
        httpx.HTTPStatusError: After 3 failed attempts on 5xx responses.
        httpx.ConnectError: After 3 failed attempts on connection failures.
    """
    logger.info("Starting UK Police crime extraction for central London")
    ingested_at = datetime.now(timezone.utc)

    params: dict[str, Any] = {"lat": _LAT, "lng": _LNG}

    with httpx.Client(timeout=60) as client:  # larger timeout — response can be big
        response = client.get(_BASE_URL, params=params)
        response.raise_for_status()
        crimes: list[dict[str, Any]] = response.json()

    records = [
        {
            "ingested_at": ingested_at,
            "crime_id": c["id"],
            "persistent_id": c.get("persistent_id") or None,
            "category": c.get("category"),
            "location_type": c.get("location_type"),
            "lat": float(c["location"]["latitude"]) if c.get("location") else None,
            "lon": float(c["location"]["longitude"]) if c.get("location") else None,
            "street_name": (c.get("location") or {}).get("street", {}).get("name"),
            "outcome_status": (c.get("outcome_status") or {}).get("category") if c.get("outcome_status") else None,
            "month": c.get("month"),
            "_source": _SOURCE,
        }
        for c in crimes
    ]

    logger.info("Crime extraction complete — %d incidents", len(records))
    return records
