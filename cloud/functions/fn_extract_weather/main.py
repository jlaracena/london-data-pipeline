"""Cloud Function — fn-extract-weather.

HTTP-triggered function that extracts hourly weather forecasts for London
from Open-Meteo and loads them into BigQuery via a MERGE deduplication strategy.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

import functions_framework
from quality import validate
from pipeline_logger import log_run
import httpx
from google.cloud import bigquery
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
_SOURCE = "open-meteo"
_TABLE = "raw_weather"


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
def _extract() -> list[dict[str, Any]]:
    """Fetch hourly weather data from Open-Meteo."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    params = {
        "latitude": 51.5074,
        "longitude": -0.1278,
        "hourly": "temperature_2m,windspeed_10m,weathercode",
        "timezone": "UTC",
    }
    with httpx.Client(timeout=30) as client:
        resp = client.get(_OPEN_METEO_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    hourly = data["hourly"]
    return [
        {
            "ingested_at": ingested_at,
            "location_city": "London",
            "temperature_2m": hourly["temperature_2m"][i],
            "windspeed_10m": hourly["windspeed_10m"][i],
            "weathercode": hourly["weathercode"][i],
            "forecast_date": hourly["time"][i],
            "_source": _SOURCE,
        }
        for i in range(len(hourly["time"]))
    ]


def _load_to_bq(records: list[dict[str, Any]], project: str, dataset: str) -> int:
    """Insert records into BigQuery using a MERGE to deduplicate on (forecast_date, location_city)."""
    client = bigquery.Client(project=project)
    staging_table = f"{project}.{dataset}._staging_{_TABLE}"
    target_table = f"{project}.{dataset}.{_TABLE}"

    # Write to staging (always overwrite)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("location_city", "STRING"),
            bigquery.SchemaField("temperature_2m", "FLOAT64"),
            bigquery.SchemaField("windspeed_10m", "FLOAT64"),
            bigquery.SchemaField("weathercode", "INT64"),
            bigquery.SchemaField("forecast_date", "TIMESTAMP"),
            bigquery.SchemaField("_source", "STRING"),
        ],
    )
    client.load_table_from_json(records, staging_table, job_config=job_config).result()

    # MERGE into target
    merge_sql = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON T.forecast_date = S.forecast_date AND T.location_city = S.location_city
        WHEN NOT MATCHED THEN
          INSERT (ingested_at, location_city, temperature_2m, windspeed_10m, weathercode, forecast_date, _source)
          VALUES (S.ingested_at, S.location_city, S.temperature_2m, S.windspeed_10m, S.weathercode, S.forecast_date, S._source)
    """
    result = client.query(merge_sql).result()
    return result.num_dml_affected_rows or 0


@functions_framework.http
def handler(request: Any) -> tuple[dict[str, Any], int]:
    """HTTP entry point for the weather extractor Cloud Function."""
    project = os.environ.get("BQ_PROJECT", "")
    dataset = os.environ.get("BQ_DATASET", "raw")

    started_at = datetime.now(timezone.utc)
    report = None
    try:
        logger.info("Starting weather extraction")
        records = _extract()
        report = validate(_TABLE, records)
        if not report.passed:
            logger.error("Quality check FAILED for %s: %s", _TABLE, report.issues())
            log_run(project, dataset, _TABLE, started_at, len(records), 0, report, "warning")
            return {"status": "error", "quality": report.to_dict()}, 500
        inserted = _load_to_bq(records, project, dataset)
        log_run(project, dataset, _TABLE, started_at, len(records), inserted, report, "success")
        logger.info("Weather load complete — rows_inserted=%d", inserted)
        return {"status": "ok", "rows_inserted": inserted, "quality": report.to_dict()}, 200
    except Exception as exc:
        logger.exception("Weather extraction failed: %s", exc)
        from quality import QualityReport
        log_run(project, dataset, _TABLE, started_at, 0, 0,
                report or QualityReport(table=_TABLE, total=0), "failed")
        return {"status": "error", "message": str(exc)}, 500
