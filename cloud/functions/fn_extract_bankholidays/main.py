"""Cloud Function — fn-extract-bankholidays.

HTTP-triggered function that extracts UK public holidays from the GOV.UK API
and loads them into BigQuery. Static reference data — deduplication via MERGE
ensures re-runs are safe.
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

_GOVUK_URL = "https://www.gov.uk/bank-holidays.json"
_SOURCE = "gov-uk-bank-holidays"
_TABLE = "raw_bank_holidays"


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
        resp = client.get(_GOVUK_URL)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()

    records: list[dict[str, Any]] = []
    for division, payload in data.items():
        for event in payload.get("events", []):
            records.append({
                "ingested_at": ingested_at,
                "division": division,
                "title": event["title"],
                "holiday_date": event["date"],
                "notes": event.get("notes") or None,
                "bunting": event.get("bunting", False),
                "_source": _SOURCE,
            })
    return records


def _load_to_bq(records: list[dict[str, Any]], project: str, dataset: str) -> int:
    client = bigquery.Client(project=project)
    staging_table = f"{project}.{dataset}._staging_{_TABLE}"
    target_table = f"{project}.{dataset}.{_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("division", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("holiday_date", "STRING"),
            bigquery.SchemaField("notes", "STRING"),
            bigquery.SchemaField("bunting", "BOOL"),
            bigquery.SchemaField("_source", "STRING"),
        ],
    )
    client.load_table_from_json(records, staging_table, job_config=job_config).result()

    merge_sql = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON T.division = S.division
           AND T.holiday_date = S.holiday_date
           AND T.title = S.title
        WHEN NOT MATCHED THEN
          INSERT ROW
    """
    result = client.query(merge_sql).result()
    return result.num_dml_affected_rows or 0


@functions_framework.http
def handler(request: Any) -> tuple[dict[str, Any], int]:
    """HTTP entry point for the bank holidays extractor Cloud Function."""
    project = os.environ.get("BQ_PROJECT", "")
    dataset = os.environ.get("BQ_DATASET", "raw")
    try:
        logger.info("Starting bank holidays extraction")
        records = _extract()
        inserted = _load_to_bq(records, project, dataset)
        logger.info("Bank holidays load complete — rows_inserted=%d", inserted)
        return {"status": "ok", "rows_inserted": inserted}, 200
    except Exception as exc:
        logger.exception("Bank holidays extraction failed: %s", exc)
        return {"status": "error", "message": str(exc)}, 500
