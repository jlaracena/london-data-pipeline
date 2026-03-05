"""Air quality extractor — Open-Meteo Air Quality API.

Fetches hourly PM10, PM2.5, NO2 and O3 forecasts for London
using the same provider as the weather extractor (no API key required).

Source: https://air-quality-api.open-meteo.com/v1/air-quality
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

_BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
_PARAMS = [
    ("latitude",         "51.5074"),
    ("longitude",        "-0.1278"),
    ("hourly",           "pm10,pm2_5,nitrogen_dioxide,ozone"),
    ("timezone",         "UTC"),
    ("forecast_days",    "1"),
]
_POLLUTANTS = ["pm10", "pm2_5", "nitrogen_dioxide", "ozone"]
_PARAM_MAP  = {
    "pm10":             "pm10",
    "pm2_5":            "pm25",
    "nitrogen_dioxide": "no2",
    "ozone":            "o3",
}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
    reraise=True,
)
def extract_air_quality() -> list[dict[str, Any]]:
    """Return one record per pollutant per forecast hour for London."""
    logger.info("Starting air quality extraction from Open-Meteo Air Quality API")

    with httpx.Client(timeout=30) as client:
        response = client.get(_BASE_URL, params=_PARAMS)
        response.raise_for_status()

    data     = response.json()
    hourly   = data["hourly"]
    times    = hourly["time"]
    ingested = datetime.now(timezone.utc)
    records: list[dict[str, Any]] = []

    for i, ts in enumerate(times):
        measured_at = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
        for raw_key in _POLLUTANTS:
            value = hourly.get(raw_key, [None])[i]
            records.append({
                "ingested_at":   ingested,
                "location_city": "London",
                "location_id":   0,                   # single virtual station
                "parameter":     _PARAM_MAP[raw_key],
                "value":         value,
                "unit":          "µg/m³",
                "measured_at":   measured_at,
                "_source":       "open-meteo-aq",
            })

    logger.info("Air quality extraction complete — %d records", len(records))
    return records
