"""Cloud Function — fn-extract-countries.

HTTP-triggered function that extracts United Kingdom reference data from
RestCountries and loads it into BigQuery via a MERGE deduplication strategy.
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

_RESTCOUNTRIES_URL = "https://restcountries.com/v3.1/alpha/GB"
_SOURCE = "restcountries"
_TABLE = "raw_countries"


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
    """Fetch UK reference data from RestCountries."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    with httpx.Client(timeout=30) as client:
        resp = client.get(_RESTCOUNTRIES_URL)
        resp.raise_for_status()
        data: list[dict[str, Any]] = resp.json()

    country = data[0]
    return [
        {
            "ingested_at": ingested_at,
            "country_code": country["cca2"],
            "country_name": country["name"]["common"],
            "capital": (country.get("capital") or [""])[0],
            "population": country.get("population"),
            "area_km2": country.get("area"),
            "region": country.get("region"),
            "languages": list((country.get("languages") or {}).values()),
            "currencies": list((country.get("currencies") or {}).keys()),
            "_source": _SOURCE,
        }
    ]


def _load_to_bq(records: list[dict[str, Any]], project: str, dataset: str) -> int:
    """Insert records into BigQuery using MERGE to deduplicate on (country_code)."""
    client = bigquery.Client(project=project)
    staging_table = f"{project}.{dataset}._staging_{_TABLE}"
    target_table = f"{project}.{dataset}.{_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("country_code", "STRING"),
            bigquery.SchemaField("country_name", "STRING"),
            bigquery.SchemaField("capital", "STRING"),
            bigquery.SchemaField("population", "INT64"),
            bigquery.SchemaField("area_km2", "FLOAT64"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("languages", "STRING", mode="REPEATED"),
            bigquery.SchemaField("currencies", "STRING", mode="REPEATED"),
            bigquery.SchemaField("_source", "STRING"),
        ],
    )
    client.load_table_from_json(records, staging_table, job_config=job_config).result()

    merge_sql = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON T.country_code = S.country_code
        WHEN MATCHED THEN
          UPDATE SET
            ingested_at = S.ingested_at, country_name = S.country_name,
            capital = S.capital, population = S.population,
            area_km2 = S.area_km2, region = S.region,
            languages = S.languages, currencies = S.currencies,
            _source = S._source
        WHEN NOT MATCHED THEN
          INSERT ROW
    """
    result = client.query(merge_sql).result()
    return result.num_dml_affected_rows or 0


@functions_framework.http
def handler(request: Any) -> tuple[dict[str, Any], int]:
    """HTTP entry point for the countries extractor Cloud Function."""
    project = os.environ.get("BQ_PROJECT", "")
    dataset = os.environ.get("BQ_DATASET", "raw")

    started_at = datetime.now(timezone.utc)
    report = None
    try:
        logger.info("Starting countries extraction")
        records = _extract()
        report = validate(_TABLE, records)
        if not report.passed:
            logger.error("Quality check FAILED for %s: %s", _TABLE, report.issues())
            log_run(project, dataset, _TABLE, started_at, len(records), 0, report, "warning")
            return {"status": "error", "quality": report.to_dict()}, 500
        inserted = _load_to_bq(records, project, dataset)
        log_run(project, dataset, _TABLE, started_at, len(records), inserted, report, "success")
        logger.info("Countries load complete — rows_inserted=%d", inserted)
        return {"status": "ok", "rows_inserted": inserted, "quality": report.to_dict()}, 200
    except Exception as exc:
        logger.exception("Countries extraction failed: %s", exc)
        log_run(project, dataset, _TABLE, started_at, 0, 0,
                report or QualityReport(table=_TABLE, total=0), "failed")
        return {"status": "error", "message": str(exc)}, 500
