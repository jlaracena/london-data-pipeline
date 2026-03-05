"""Airflow DAG — forest_pipeline_local.

Daily pipeline that extracts London city data from seven public APIs and loads
it into the local PostgreSQL data warehouse.

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
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from extractors.weather import extract_weather
from extractors.air_quality import extract_air_quality
from extractors.news import extract_news
from extractors.countries import extract_countries
from extractors.tfl import extract_tfl
from extractors.bank_holidays import extract_bank_holidays
from extractors.crime import extract_crime
from loaders.postgres_loader import load_to_postgres

logger = logging.getLogger(__name__)

# ── DAG default args ──────────────────────────────────────────────────────────

default_args = {
    "owner": "forest",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Task callables ─────────────────────────────────────────────────────────────


def _extract_and_load_weather() -> dict[str, int]:
    records = extract_weather()
    inserted = load_to_postgres("raw_weather", records)
    return {"extracted": len(records), "inserted": inserted}


def _extract_and_load_air_quality() -> dict[str, int]:
    records = extract_air_quality()
    inserted = load_to_postgres("raw_air_quality", records)
    return {"extracted": len(records), "inserted": inserted}


def _extract_and_load_news() -> dict[str, int]:
    records = extract_news()
    inserted = load_to_postgres("raw_news", records)
    return {"extracted": len(records), "inserted": inserted}


def _extract_and_load_countries() -> dict[str, int]:
    records = extract_countries()
    inserted = load_to_postgres("raw_countries", records)
    return {"extracted": len(records), "inserted": inserted}


def _extract_and_load_tfl() -> dict[str, int]:
    records = extract_tfl()
    inserted = load_to_postgres("raw_tfl_bikepoints", records)
    return {"extracted": len(records), "inserted": inserted}


def _extract_and_load_bank_holidays() -> dict[str, int]:
    records = extract_bank_holidays()
    inserted = load_to_postgres("raw_bank_holidays", records)
    return {"extracted": len(records), "inserted": inserted}


def _extract_and_load_crime() -> dict[str, int]:
    records = extract_crime()
    inserted = load_to_postgres("raw_crime", records)
    return {"extracted": len(records), "inserted": inserted}


def _validate_loads() -> None:
    """Query each raw table for row counts and last ingestion timestamp.

    Logs a summary report and fails the task if any table has 0 rows.
    """
    import psycopg2
    import os

    dsn = os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"]
    tables = [
        "raw_weather", "raw_air_quality", "raw_news", "raw_countries",
        "raw_tfl_bikepoints", "raw_bank_holidays", "raw_crime",
    ]
    empty_tables: list[str] = []

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cursor:
            logger.info("=" * 60)
            logger.info("VALIDATION REPORT — forest_pipeline_local")
            logger.info("=" * 60)
            for table in tables:
                cursor.execute(
                    f"SELECT COUNT(*), MAX(ingested_at) FROM {table}"  # noqa: S608
                )
                row_count, last_ingested = cursor.fetchone()
                logger.info(
                    "%-22s  rows=%-8d  last_ingested=%s",
                    table,
                    row_count,
                    last_ingested,
                )
                if row_count == 0:
                    empty_tables.append(table)
            logger.info("=" * 60)

    if empty_tables:
        raise ValueError(
            f"Validation failed — the following tables have 0 rows: {empty_tables}"
        )


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="forest_pipeline_local",
    description="Ingest London city data from 7 APIs into PostgreSQL",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["forest", "local", "ingestion"],
) as dag:

    with TaskGroup(group_id="extract_and_load") as extract_and_load:
        t_weather = PythonOperator(
            task_id="extract_load_weather",
            python_callable=_extract_and_load_weather,
        )
        t_air_quality = PythonOperator(
            task_id="extract_load_air_quality",
            python_callable=_extract_and_load_air_quality,
        )
        t_news = PythonOperator(
            task_id="extract_load_news",
            python_callable=_extract_and_load_news,
        )
        t_countries = PythonOperator(
            task_id="extract_load_countries",
            python_callable=_extract_and_load_countries,
        )
        t_tfl = PythonOperator(
            task_id="extract_load_tfl",
            python_callable=_extract_and_load_tfl,
        )
        t_bank_holidays = PythonOperator(
            task_id="extract_load_bank_holidays",
            python_callable=_extract_and_load_bank_holidays,
        )
        t_crime = PythonOperator(
            task_id="extract_load_crime",
            python_callable=_extract_and_load_crime,
        )

    validate_loads = PythonOperator(
        task_id="validate_loads",
        python_callable=_validate_loads,
    )

    extract_and_load >> validate_loads
