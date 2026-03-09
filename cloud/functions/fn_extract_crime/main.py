"""Cloud Function — fn-extract-crime.

HTTP-triggered function that extracts street-level crime data for central
London from the UK Police API and loads it into BigQuery.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

import functions_framework
from quality import validate, QualityReport
from pipeline_logger import log_run
import httpx
import google.auth
import google.auth.transport.requests
from google.cloud import bigquery
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_POLICE_URL = "https://data.police.uk/api/crimes-street/all-crime"
_SOURCE = "uk-police"
_TABLE = "raw_crime"
_DBT_JOB_REGION = "europe-west2"
_DBT_JOB_NAME = "dbt-run"


def _trigger_dbt(project: str) -> None:
    """Trigger the dbt Cloud Run Job after successful crime data load."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(google.auth.transport.requests.Request())
    url = (
        f"https://run.googleapis.com/v2/projects/{project}"
        f"/locations/{_DBT_JOB_REGION}/jobs/{_DBT_JOB_NAME}:run"
    )
    with httpx.Client(timeout=30) as client:
        resp = client.post(url, headers={"Authorization": f"Bearer {credentials.token}"}, json={})
        resp.raise_for_status()
    logger.info("dbt Cloud Run Job triggered — execution started")


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
    params: dict[str, Any] = {"lat": 51.5074, "lng": -0.1278}
    with httpx.Client(timeout=60) as client:
        resp = client.get(_POLICE_URL, params=params)
        resp.raise_for_status()
        crimes: list[dict[str, Any]] = resp.json()

    return [
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


def _load_to_bq(records: list[dict[str, Any]], project: str, dataset: str) -> int:
    client = bigquery.Client(project=project)
    staging_table = f"{project}.{dataset}._staging_{_TABLE}"
    target_table = f"{project}.{dataset}.{_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("crime_id", "INT64"),
            bigquery.SchemaField("persistent_id", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("location_type", "STRING"),
            bigquery.SchemaField("lat", "FLOAT64"),
            bigquery.SchemaField("lon", "FLOAT64"),
            bigquery.SchemaField("street_name", "STRING"),
            bigquery.SchemaField("outcome_status", "STRING"),
            bigquery.SchemaField("month", "STRING"),
            bigquery.SchemaField("_source", "STRING"),
        ],
    )
    client.load_table_from_json(records, staging_table, job_config=job_config).result()

    merge_sql = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON T.crime_id = S.crime_id
        WHEN NOT MATCHED THEN
          INSERT ROW
    """
    result = client.query(merge_sql).result()
    return result.num_dml_affected_rows or 0


@functions_framework.http
def handler(request: Any) -> tuple[dict[str, Any], int]:
    """HTTP entry point for the crime extractor Cloud Function."""
    project = os.environ.get("BQ_PROJECT", "")
    dataset = os.environ.get("BQ_DATASET", "raw")
    started_at = datetime.now(timezone.utc)
    report = None
    try:
        logger.info("Starting crime extraction")
        records = _extract()
        report = validate(_TABLE, records)
        if not report.passed:
            logger.error("Quality check FAILED for %s: %s", _TABLE, report.issues())
            log_run(project, dataset, _TABLE, started_at, len(records), 0, report, "warning")
            return {"status": "error", "quality": report.to_dict()}, 500
        inserted = _load_to_bq(records, project, dataset)
        log_run(project, dataset, _TABLE, started_at, len(records), inserted, report, "success")
        logger.info("Crime load complete — rows_inserted=%d", inserted)
        try:
            _trigger_dbt(project)
        except Exception as dbt_exc:
            logger.warning("dbt trigger failed (pipeline data is safe): %s", dbt_exc)
        return {"status": "ok", "rows_inserted": inserted, "quality": report.to_dict()}, 200
    except Exception as exc:
        logger.exception("Crime extraction failed: %s", exc)
        log_run(project, dataset, _TABLE, started_at, 0, 0,
                report or QualityReport(table=_TABLE, total=0), "failed")
        return {"status": "error", "message": str(exc)}, 500
