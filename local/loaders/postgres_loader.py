"""PostgreSQL UPSERT loader using psycopg2.

Resolves the connection string from the Airflow connection environment variable
AIRFLOW_CONN_<CONN_ID_UPPER> and performs idempotent batch inserts via
INSERT ... ON CONFLICT DO NOTHING.
"""


from __future__ import annotations
import logging
import os
from typing import Any

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# Per-table conflict targets that mirror the UNIQUE constraints in init_tables.sql
_CONFLICT_TARGETS: dict[str, str] = {
    "raw_weather": "(forecast_date, location_city)",
    "raw_air_quality": "(location_id, parameter, measured_at)",
    "raw_news": "(article_id)",
    "raw_countries": "(country_code)",
    "raw_tfl_bikepoints": "(station_id, snapshot_date)",
    "raw_bank_holidays": "(division, holiday_date, title)",
    "raw_crime": "(crime_id)",
}


def _resolve_dsn(conn_id: str) -> str:
    """Read and return the connection DSN from the Airflow env variable.

    Args:
        conn_id: Airflow connection id (e.g. ``"postgres_default"``).

    Returns:
        PostgreSQL DSN URI string.

    Raises:
        ValueError: If the environment variable is not set.
    """
    env_key = f"AIRFLOW_CONN_{conn_id.upper()}"
    dsn = os.environ.get(env_key)
    if not dsn:
        raise ValueError(
            f"Environment variable '{env_key}' is not set. "
            "Set AIRFLOW_CONN_POSTGRES_DEFAULT in your .env file."
        )
    return dsn.replace("postgresql+psycopg2://", "postgresql://")


def load_to_postgres(
    table_name: str,
    records: list[dict[str, Any]],
    conn_id: str = "postgres_default",
) -> int:
    """Upsert a batch of records into a PostgreSQL raw table.

    Uses ``INSERT ... ON CONFLICT DO NOTHING`` to ensure idempotency.
    Rows that already exist (matching the table's unique constraint) are
    silently skipped.

    Args:
        table_name: Target table name (must exist in ``_CONFLICT_TARGETS``).
        records: List of flat dicts; keys must match table column names.
        conn_id: Airflow connection id used to resolve the DSN.

    Returns:
        Number of rows actually inserted (excluding skipped duplicates).

    Raises:
        ValueError: For an unsupported table name or missing env var.
        psycopg2.DatabaseError: On any database error.
    """
    if not records:
        logger.info("load_to_postgres(%s): no records to load", table_name)
        return 0

    conflict_target = _CONFLICT_TARGETS.get(table_name)
    if conflict_target is None:
        raise ValueError(
            f"No conflict target defined for table '{table_name}'. "
            f"Supported tables: {list(_CONFLICT_TARGETS)}"
        )

    dsn = _resolve_dsn(conn_id)
    columns = list(records[0].keys())
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    sql = (
        f"INSERT INTO {table_name} ({col_list}) "
        f"VALUES %s "
        f"ON CONFLICT {conflict_target} DO NOTHING "
        f"RETURNING 1"
    )

    values = [tuple(r[c] for c in columns) for r in records]

    logger.info(
        "Loading %d records into %s (conflict target: %s)",
        len(records),
        table_name,
        conflict_target,
    )

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cursor:
            result = psycopg2.extras.execute_values(
                cursor, sql, values, fetch=True
            )
            inserted = len(result)

    skipped = len(records) - inserted
    logger.info(
        "load_to_postgres(%s): inserted=%d, skipped=%d",
        table_name,
        inserted,
        skipped,
    )
    return inserted
