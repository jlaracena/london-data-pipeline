"""Cloud Function — fn-extract-airquality.

HTTP-triggered function that extracts hourly air quality forecasts for London
from the Open-Meteo Air Quality API and loads them into BigQuery via MERGE.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

import functions_framework
from quality import validate, QualityReport
from pipeline_logger import log_run
import httpx
from google.cloud import bigquery
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_AQ_URL  = "https://air-quality-api.open-meteo.com/v1/air-quality"
_PARAMS  = [
    ("latitude",      "51.5074"),
    ("longitude",     "-0.1278"),
    ("hourly",        "pm10,pm2_5,nitrogen_dioxide,ozone"),
    ("timezone",      "UTC"),
    ("forecast_days", "1"),
]
_PARAM_MAP = {"pm10": "pm10", "pm2_5": "pm25", "nitrogen_dioxide": "no2", "ozone": "o3"}
_SOURCE    = "open-meteo-aq"
_TABLE     = "raw_air_quality"


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
    ingested_at = datetime.now(timezone.utc).isoformat()
    with httpx.Client(timeout=30) as client:
        resp = client.get(_AQ_URL, params=_PARAMS)
        resp.raise_for_status()
    data   = resp.json()
    hourly = data["hourly"]
    times  = hourly["time"]
    records: list[dict[str, Any]] = []
    for i, ts in enumerate(times):
        for raw_key, param_code in _PARAM_MAP.items():
            records.append({
                "ingested_at":   ingested_at,
                "location_city": "London",
                "location_id":   0,
                "parameter":     param_code,
                "value":         hourly.get(raw_key, [None])[i],
                "unit":          "µg/m³",
                "measured_at":   ts + ":00" if len(ts) == 16 else ts,
                "_source":       _SOURCE,
            })
    return records


def _load_to_bq(records: list[dict[str, Any]], project: str, dataset: str) -> int:
    client = bigquery.Client(project=project)
    staging = f"{project}.{dataset}._staging_{_TABLE}"
    target  = f"{project}.{dataset}.{_TABLE}"

    client.load_table_from_json(records, staging, job_config=bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("ingested_at",   "TIMESTAMP"),
            bigquery.SchemaField("location_city", "STRING"),
            bigquery.SchemaField("location_id",   "INT64"),
            bigquery.SchemaField("parameter",     "STRING"),
            bigquery.SchemaField("value",         "FLOAT64"),
            bigquery.SchemaField("unit",          "STRING"),
            bigquery.SchemaField("measured_at",   "TIMESTAMP"),
            bigquery.SchemaField("_source",       "STRING"),
        ],
    )).result()

    result = client.query(f"""
        MERGE `{target}` T USING `{staging}` S
        ON T.location_id = S.location_id
           AND T.parameter = S.parameter
           AND T.measured_at = S.measured_at
        WHEN NOT MATCHED THEN
          INSERT (ingested_at, location_city, location_id, parameter, value, unit, measured_at, _source)
          VALUES (S.ingested_at, S.location_city, S.location_id, S.parameter, S.value, S.unit, S.measured_at, S._source)
    """).result()
    return result.num_dml_affected_rows or 0


@functions_framework.http
def handler(request: Any) -> tuple[dict[str, Any], int]:
    project = os.environ.get("BQ_PROJECT", "")
    dataset = os.environ.get("BQ_DATASET", "raw")
    started_at = datetime.now(timezone.utc)
    report = None
    try:
        records = _extract()
        report = validate(_TABLE, records)
        if not report.passed:
            logger.error("Quality check FAILED for %s: %s", _TABLE, report.issues())
            log_run(project, dataset, _TABLE, started_at, len(records), 0, report, "warning")
            return {"status": "error", "quality": report.to_dict()}, 500
        inserted = _load_to_bq(records, project, dataset)
        log_run(project, dataset, _TABLE, started_at, len(records), inserted, report, "success")
        logger.info("Air quality load complete — rows_inserted=%d", inserted)
        return {"status": "ok", "rows_inserted": inserted, "quality": report.to_dict()}, 200
    except Exception as exc:
        logger.exception("Air quality extraction failed: %s", exc)
        log_run(project, dataset, _TABLE, started_at, 0, 0,
                report or QualityReport(table=_TABLE, total=0), "failed")
        return {"status": "error", "message": str(exc)}, 500
