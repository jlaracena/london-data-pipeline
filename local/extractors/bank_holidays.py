"""UK Government Bank Holidays extractor.

Fetches official UK public holidays for all three divisions
(England & Wales, Scotland, Northern Ireland) from the GOV.UK API.
This is static reference data that rarely changes — the upsert strategy
ensures re-runs never produce duplicates.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.gov.uk/bank-holidays.json"
_SOURCE = "gov-uk-bank-holidays"


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
def extract_bank_holidays() -> list[dict[str, Any]]:
    """Extract UK public holidays for all divisions from the GOV.UK API.

    Returns:
        List of flat dicts, one per holiday event per division,
        matching the raw_bank_holidays schema.

    Raises:
        httpx.HTTPStatusError: After 3 failed attempts on 5xx responses.
        httpx.ConnectError: After 3 failed attempts on connection failures.
    """
    logger.info("Starting UK Bank Holidays extraction from GOV.UK")
    ingested_at = datetime.now(timezone.utc)

    with httpx.Client(timeout=30) as client:
        response = client.get(_BASE_URL)
        response.raise_for_status()
        data: dict[str, Any] = response.json()

    records: list[dict[str, Any]] = []
    for division, payload in data.items():
        for event in payload.get("events", []):
            records.append(
                {
                    "ingested_at": ingested_at,
                    "division": division,
                    "title": event["title"],
                    "holiday_date": event["date"],  # stored as VARCHAR YYYY-MM-DD
                    "notes": event.get("notes") or None,
                    "bunting": event.get("bunting", False),
                    "_source": _SOURCE,
                }
            )

    logger.info("Bank Holidays extraction complete — %d events across %d divisions",
                len(records), len(data))
    return records
