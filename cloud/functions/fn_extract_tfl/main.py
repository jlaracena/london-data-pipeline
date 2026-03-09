"""Cloud Function — fn-extract-tfl.

HTTP-triggered function that extracts Santander Cycles docking station
availability from the TfL BikePoint API and loads it into BigQuery.
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

_TFL_URL = "https://api.tfl.gov.uk/BikePoint"
_SOURCE = "tfl-bikepoint"
_TABLE = "raw_tfl_bikepoints"


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError))


def _get_prop(props: list[dict[str, Any]], key: str) -> int | None:
    for p in props:
        if p.get("key") == key:
            raw = p.get("value", "")
            return int(raw) if str(raw).isdigit() else None
    return None


@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
    reraise=True,
)
def _extract() -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    with httpx.Client(timeout=30) as client:
        resp = client.get(_TFL_URL)
        resp.raise_for_status()
        stations: list[dict[str, Any]] = resp.json()

    return [
        {
            "ingested_at": now.isoformat(),
            "snapshot_date": now.date().isoformat(),
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


def _load_to_bq(records: list[dict[str, Any]], project: str, dataset: str) -> int:
    client = bigquery.Client(project=project)
    staging_table = f"{project}.{dataset}._staging_{_TABLE}"
    target_table = f"{project}.{dataset}.{_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("snapshot_date", "DATE"),
            bigquery.SchemaField("station_id", "STRING"),
            bigquery.SchemaField("station_name", "STRING"),
            bigquery.SchemaField("lat", "FLOAT64"),
            bigquery.SchemaField("lon", "FLOAT64"),
            bigquery.SchemaField("nb_bikes", "INT64"),
            bigquery.SchemaField("nb_empty_docks", "INT64"),
            bigquery.SchemaField("nb_docks", "INT64"),
            bigquery.SchemaField("_source", "STRING"),
        ],
    )
    client.load_table_from_json(records, staging_table, job_config=job_config).result()

    merge_sql = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON T.station_id = S.station_id AND T.snapshot_date = S.snapshot_date
        WHEN NOT MATCHED THEN
          INSERT ROW
    """
    result = client.query(merge_sql).result()
    return result.num_dml_affected_rows or 0


@functions_framework.http
def handler(request: Any) -> tuple[dict[str, Any], int]:
    """HTTP entry point for the TfL BikePoint extractor Cloud Function."""
    project = os.environ.get("BQ_PROJECT", "")
    dataset = os.environ.get("BQ_DATASET", "raw")
    started_at = datetime.now(timezone.utc)
    report = None
    try:
        logger.info("Starting TfL extraction")
        records = _extract()
        report = validate(_TABLE, records)
        if not report.passed:
            logger.error("Quality check FAILED for %s: %s", _TABLE, report.issues())
            log_run(project, dataset, _TABLE, started_at, len(records), 0, report, "warning")
            return {"status": "error", "quality": report.to_dict()}, 500
        inserted = _load_to_bq(records, project, dataset)
        log_run(project, dataset, _TABLE, started_at, len(records), inserted, report, "success")
        logger.info("TfL load complete — rows_inserted=%d", inserted)
        return {"status": "ok", "rows_inserted": inserted, "quality": report.to_dict()}, 200
    except Exception as exc:
        logger.exception("TfL extraction failed: %s", exc)
        log_run(project, dataset, _TABLE, started_at, 0, 0,
                report or QualityReport(table=_TABLE, total=0), "failed")
        return {"status": "error", "message": str(exc)}, 500
