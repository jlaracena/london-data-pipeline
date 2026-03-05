"""Transport for London (TfL) BikePoint extractor.

Fetches real-time availability of Santander Cycles docking stations across
London. Each run captures a daily snapshot — one record per station.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.tfl.gov.uk/BikePoint"
_SOURCE = "tfl-bikepoint"


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError))


def _get_prop(additional_properties: list[dict[str, Any]], key: str) -> int | None:
    """Extract a numeric value from TfL's additionalProperties list."""
    for prop in additional_properties:
        if prop.get("key") == key:
            raw = prop.get("value", "")
            return int(raw) if str(raw).isdigit() else None
    return None


@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
    reraise=True,
)
def extract_tfl() -> list[dict[str, Any]]:
    """Extract Santander Cycles docking station availability from TfL BikePoint API.

    Returns:
        List of flat dicts, one per station, matching the raw_tfl_bikepoints schema.

    Raises:
        httpx.HTTPStatusError: After 3 failed attempts on 5xx responses.
        httpx.ConnectError: After 3 failed attempts on connection failures.
    """
    logger.info("Starting TfL BikePoint extraction")
    now = datetime.now(timezone.utc)

    with httpx.Client(timeout=30) as client:
        response = client.get(_BASE_URL)
        response.raise_for_status()
        stations: list[dict[str, Any]] = response.json()

    records = [
        {
            "ingested_at": now,
            "snapshot_date": now.date(),
            "station_id": s["id"],
            "station_name": s.get("commonName"),
            "lat": s.get("lat"),
            "lon": s.get("lon"),
            "nb_bikes": _get_prop(s.get("additionalProperties", []), "NbBikes"),
            "nb_empty_docks": _get_prop(s.get("additionalProperties", []), "NbEmptyDocks"),
            "nb_docks": _get_prop(s.get("additionalProperties", []), "NbDocks"),
            "_source": _SOURCE,
        }
        for s in stations
    ]

    logger.info("TfL extraction complete — %d stations", len(records))
    return records
