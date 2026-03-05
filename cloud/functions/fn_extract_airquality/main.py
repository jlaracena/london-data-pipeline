"""Cloud Function — fn-extract-airquality.

HTTP-triggered function that extracts air quality measurements for London
from OpenAQ and loads them into BigQuery via a MERGE deduplication strategy.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

import functions_framework
import httpx
from google.cloud import bigquery
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_OPENAQ_URL = "https://api.openaq.org/v2/measurements"
_SOURCE = "openaq"
_TABLE = "raw_air_quality"


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
    """Fetch air quality measurements from OpenAQ."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    params: dict[str, Any] = {"city": "London", "limit": 100, "sort": "desc", "order_by": "datetime"}
    with httpx.Client(timeout=30) as client:
        resp = client.get(_OPENAQ_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    return [
        {
            "ingested_at": ingested_at,
            "location_city": "London",
            "location_id": r["locationId"],
            "parameter": r["parameter"],
            "value": r["value"],
            "unit": r["unit"],
            "measured_at": r["date"]["utc"],
            "_source": _SOURCE,
        }
        for r in data.get("results", [])
    ]


def _load_to_bq(records: list[dict[str, Any]], project: str, dataset: str) -> int:
    """Insert records into BigQuery using MERGE to deduplicate on (location_id, parameter, measured_at)."""
    client = bigquery.Client(project=project)
    staging_table = f"{project}.{dataset}._staging_{_TABLE}"
    target_table = f"{project}.{dataset}.{_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("location_city", "STRING"),
            bigquery.SchemaField("location_id", "INT64"),
            bigquery.SchemaField("parameter", "STRING"),
            bigquery.SchemaField("value", "FLOAT64"),
            bigquery.SchemaField("unit", "STRING"),
            bigquery.SchemaField("measured_at", "TIMESTAMP"),
            bigquery.SchemaField("_source", "STRING"),
        ],
    )
    client.load_table_from_json(records, staging_table, job_config=job_config).result()

    merge_sql = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON T.location_id = S.location_id
           AND T.parameter = S.parameter
           AND T.measured_at = S.measured_at
        WHEN NOT MATCHED THEN
          INSERT (ingested_at, location_city, location_id, parameter, value, unit, measured_at, _source)
          VALUES (S.ingested_at, S.location_city, S.location_id, S.parameter, S.value, S.unit, S.measured_at, S._source)
    """
    result = client.query(merge_sql).result()
    return result.num_dml_affected_rows or 0


@functions_framework.http
def handler(request: Any) -> tuple[dict[str, Any], int]:
    """HTTP entry point for the air quality extractor Cloud Function."""
    project = os.environ.get("BQ_PROJECT", "")
    dataset = os.environ.get("BQ_DATASET", "raw")

    try:
        logger.info("Starting air quality extraction")
        records = _extract()
        inserted = _load_to_bq(records, project, dataset)
        logger.info("Air quality load complete — rows_inserted=%d", inserted)
        return {"status": "ok", "rows_inserted": inserted}, 200
    except Exception as exc:
        logger.exception("Air quality extraction failed: %s", exc)
        return {"status": "error", "message": str(exc)}, 500
