"""Airflow DAG — forest_pipeline_local.

Daily pipeline that extracts London city data from seven public APIs,
validates data quality, loads into PostgreSQL, and logs observability
metrics to the pipeline_runs audit table.

Task graph:
    extract_and_load (TaskGroup, parallel)
    ├── extract_load_weather
    ├── extract_load_air_quality
    ├── extract_load_news
    ├── extract_load_countries
    ├── extract_load_tfl
    ├── extract_load_bank_holidays
    └── extract_load_crime
            │
    validate_loads
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from extractors.air_quality import extract_air_quality
from extractors.bank_holidays import extract_bank_holidays
from extractors.countries import extract_countries
from extractors.crime import extract_crime
from extractors.news import extract_news
from extractors.tfl import extract_tfl
from extractors.weather import extract_weather
from loaders.postgres_loader import load_to_postgres
from utils.data_quality import validate

logger = logging.getLogger(__name__)

# ── DAG default args ──────────────────────────────────────────────────────────

default_args = {
    "owner": "forest",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Observability helpers ──────────────────────────────────────────────────────


def _log_pipeline_run(
    dag_run_id: str,
    table_name: str,
    rows_extracted: int,
    rows_inserted: int,
    quality: Any,
    status: str,
    started_at: datetime,
) -> None:
    """Write one row to the pipeline_runs audit table."""
    dsn = os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"]
    finished_at = datetime.now(timezone.utc)

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs
                    (dag_run_id, table_name, rows_extracted, rows_inserted,
                     rows_skipped, quality_passed, null_violations,
                     range_violations, duplicate_keys, started_at, finished_at, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    dag_run_id,
                    table_name,
                    rows_extracted,
                    rows_inserted,
                    rows_extracted - rows_inserted,
                    quality.passed,
                    quality.null_violations or None,
                    quality.range_violations or None,
                    quality.duplicate_keys,
                    started_at,
                    finished_at,
                    status,
                ),
            )


def _run(
    table_name: str,
    extractor: Any,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Generic extract → validate → load pipeline for one table.

    Raises ValueError if data quality hard checks fail.
    Always writes a row to pipeline_runs regardless of outcome.
    """
    dag_run_id: str = context["run_id"]
    started_at = datetime.now(timezone.utc)
    status = "success"

    try:
        records = extractor()
        report = validate(table_name, records)

        if not report.passed:
            status = "failed"
            _log_pipeline_run(dag_run_id, table_name, len(records), 0, report, status, started_at)
            raise ValueError(
                f"Quality check failed for {table_name}: {report.issues()}"
            )

        inserted = load_to_postgres(table_name, records)
        status = "warning" if report.duplicate_keys > 0 else "success"
        _log_pipeline_run(dag_run_id, table_name, len(records), inserted, report, status, started_at)
        return {"table": table_name, "extracted": len(records), "inserted": inserted}

    except Exception:
        status = "failed"
        raise


# ── Task callables ─────────────────────────────────────────────────────────────


def _extract_and_load_weather(**context: Any) -> dict[str, Any]:
    return _run("raw_weather", extract_weather, context)


def _extract_and_load_air_quality(**context: Any) -> dict[str, Any]:
    return _run("raw_air_quality", extract_air_quality, context)


def _extract_and_load_news(**context: Any) -> dict[str, Any]:
    return _run("raw_news", extract_news, context)


def _extract_and_load_countries(**context: Any) -> dict[str, Any]:
    return _run("raw_countries", extract_countries, context)


def _extract_and_load_tfl(**context: Any) -> dict[str, Any]:
    return _run("raw_tfl_bikepoints", extract_tfl, context)


def _extract_and_load_bank_holidays(**context: Any) -> dict[str, Any]:
    return _run("raw_bank_holidays", extract_bank_holidays, context)


def _extract_and_load_crime(**context: Any) -> dict[str, Any]:
    return _run("raw_crime", extract_crime, context)


def _validate_loads(**context: Any) -> None:
    """Query every raw table for row counts and last ingestion timestamp.

    Prints a formatted summary report and fails the task if any table
    has 0 rows or if quality failed on any run in this DAG execution.
    """
    dag_run_id: str = context["run_id"]
    dsn = os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"]

    raw_tables = [
        "raw_weather", "raw_air_quality", "raw_news", "raw_countries",
        "raw_tfl_bikepoints", "raw_bank_holidays", "raw_crime",
    ]
    empty_tables: list[str] = []
    failed_quality: list[str] = []

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:

            # ── Row count check ───────────────────────────────────────────────
            logger.info("=" * 65)
            logger.info("VALIDATION REPORT — forest_pipeline_local")
            logger.info("=" * 65)
            for table in raw_tables:
                cur.execute(f"SELECT COUNT(*), MAX(ingested_at) FROM {table}")  # noqa: S608
                row_count, last_ingested = cur.fetchone()
                logger.info(
                    "  %-24s  rows=%-8d  last_ingested=%s",
                    table, row_count, last_ingested,
                )
                if row_count == 0:
                    empty_tables.append(table)

            # ── Quality check from audit log ──────────────────────────────────
            cur.execute(
                """
                SELECT table_name, status, null_violations, range_violations
                FROM pipeline_runs
                WHERE dag_run_id = %s AND status = 'failed'
                """,
                (dag_run_id,),
            )
            for row in cur.fetchall():
                failed_quality.append(row[0])

            # ── Pipeline runs summary ─────────────────────────────────────────
            logger.info("-" * 65)
            cur.execute(
                """
                SELECT table_name, rows_extracted, rows_inserted,
                       rows_skipped, quality_passed, status
                FROM pipeline_runs
                WHERE dag_run_id = %s
                ORDER BY started_at
                """,
                (dag_run_id,),
            )
            for row in cur.fetchall():
                logger.info(
                    "  %-24s  extracted=%-6d inserted=%-6d skipped=%-6d quality=%-5s status=%s",
                    *row,
                )
            logger.info("=" * 65)

    errors: list[str] = []
    if empty_tables:
        errors.append(f"Empty tables: {empty_tables}")
    if failed_quality:
        errors.append(f"Quality failures: {failed_quality}")

    if errors:
        raise ValueError("Validation failed — " + " | ".join(errors))


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="forest_pipeline_local",
    description="Ingest London city data from 7 APIs into PostgreSQL with quality checks",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["forest", "local", "ingestion"],
) as dag:

    with TaskGroup(group_id="extract_and_load") as extract_and_load:
        PythonOperator(task_id="extract_load_weather",       python_callable=_extract_and_load_weather,       provide_context=True)
        PythonOperator(task_id="extract_load_air_quality",   python_callable=_extract_and_load_air_quality,   provide_context=True)
        PythonOperator(task_id="extract_load_news",          python_callable=_extract_and_load_news,          provide_context=True)
        PythonOperator(task_id="extract_load_countries",     python_callable=_extract_and_load_countries,     provide_context=True)
        PythonOperator(task_id="extract_load_tfl",           python_callable=_extract_and_load_tfl,           provide_context=True)
        PythonOperator(task_id="extract_load_bank_holidays", python_callable=_extract_and_load_bank_holidays, provide_context=True)
        PythonOperator(task_id="extract_load_crime",         python_callable=_extract_and_load_crime,         provide_context=True)

    validate_loads = PythonOperator(
        task_id="validate_loads",
        python_callable=_validate_loads,
        provide_context=True,
    )

    extract_and_load >> validate_loads
