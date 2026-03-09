"""Writes a single row to pipeline_runs after each Cloud Function execution."""

from __future__ import annotations

import uuid
import logging
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)

_TABLE = "pipeline_runs"


def log_run(
    project: str,
    dataset: str,
    table_name: str,
    started_at: datetime,
    rows_extracted: int,
    rows_inserted: int,
    quality_report: Any,
    status: str,
) -> None:
    """Insert one audit row into pipeline_runs. Failures are logged but not re-raised."""
    try:
        finished_at = datetime.now(timezone.utc)
        row = {
            "run_id":           str(uuid.uuid4()),
            "table_name":       table_name,
            "rows_extracted":   rows_extracted,
            "rows_inserted":    rows_inserted,
            "duplicate_keys":   quality_report.duplicate_keys,
            "quality_passed":   quality_report.passed,
            "null_violations":  quality_report.null_violations,
            "range_violations": quality_report.range_violations,
            "started_at":       started_at.isoformat(),
            "finished_at":      finished_at.isoformat(),
            "status":           status,
        }
        client = bigquery.Client(project=project)
        target = f"{project}.{dataset}.{_TABLE}"
        errors = client.insert_rows_json(target, [row])
        if errors:
            logger.warning("pipeline_runs insert errors: %s", errors)
        else:
            logger.info("pipeline_runs logged: table=%s status=%s", table_name, status)
    except Exception as exc:
        logger.warning("pipeline_runs logging failed (non-fatal): %s", exc)
