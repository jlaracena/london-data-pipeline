"""Data quality validation utilities for the Forest eBikes pipeline.

Runs three categories of checks on every record batch before it is loaded:
  1. Null checks   — required fields must not be None.
  2. Duplicate keys — within-batch duplicates are logged (upsert handles them).
  3. Range checks  — numeric fields must fall within expected bounds.

Usage::

    from utils.data_quality import validate

    report = validate("raw_weather", records)
    if not report.passed:
        raise ValueError(f"Quality check failed: {report.issues()}")
"""

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

# Fields that must never be None — failure causes the task to raise
_REQUIRED_FIELDS: dict[str, list[str]] = {
    "raw_weather":        ["forecast_date", "location_city", "temperature_2m"],
    "raw_air_quality":    ["location_id", "parameter", "measured_at", "value"],
    "raw_news":           ["article_id", "title", "url"],
    "raw_countries":      ["country_code", "country_name"],
    "raw_tfl_bikepoints": ["station_id", "snapshot_date", "nb_docks"],
    "raw_bank_holidays":  ["division", "title", "holiday_date"],
    "raw_crime":          ["crime_id", "category", "month"],
}

# Fields that form the unique key per table
_UNIQUE_KEYS: dict[str, list[str]] = {
    "raw_weather":        ["forecast_date", "location_city"],
    "raw_air_quality":    ["location_id", "parameter", "measured_at"],
    "raw_news":           ["article_id"],
    "raw_countries":      ["country_code"],
    "raw_tfl_bikepoints": ["station_id", "snapshot_date"],
    "raw_bank_holidays":  ["division", "holiday_date", "title"],
    "raw_crime":          ["crime_id"],
}

# Numeric range checks: (field_name, min_value, max_value)
_RANGE_CHECKS: dict[str, list[tuple[str, float, float]]] = {
    "raw_weather": [
        ("temperature_2m", -50.0,  60.0),
        ("windspeed_10m",    0.0, 300.0),
        ("weathercode",      0.0,  99.0),
    ],
    "raw_air_quality": [
        ("value", 0.0, 10_000.0),
    ],
    "raw_tfl_bikepoints": [
        ("lat",      51.0,  52.0),
        ("lon",      -1.0,   0.5),
        ("nb_bikes",  0.0, 100.0),
        ("nb_docks",  1.0, 100.0),
    ],
}


# ── Report dataclass ──────────────────────────────────────────────────────────

@dataclass
class QualityReport:
    """Result of data quality checks for a single batch.

    Attributes:
        table:            Target table name.
        total_records:    Number of records in the batch.
        null_violations:  List of human-readable null-check failures.
        duplicate_keys:   Count of duplicate keys within the batch.
        range_violations: List of human-readable range-check failures.
        passed:           False if any hard check (null or range) failed.
    """

    table: str
    total_records: int
    null_violations: list[str] = field(default_factory=list)
    duplicate_keys: int = 0
    range_violations: list[str] = field(default_factory=list)
    passed: bool = True

    def issues(self) -> list[str]:
        """Return all hard-failure messages."""
        return self.null_violations + self.range_violations

    def log(self) -> None:
        """Write a summary line to the logger."""
        status = "PASSED ✓" if self.passed else "FAILED ✗"
        logger.info(
            "Quality [%s] %s | records=%d | nulls=%s | dupes=%d | range=%s",
            self.table,
            status,
            self.total_records,
            self.null_violations or "none",
            self.duplicate_keys,
            self.range_violations or "none",
        )


# ── Public API ────────────────────────────────────────────────────────────────

def validate(table_name: str, records: list[dict[str, Any]]) -> QualityReport:
    """Run all quality checks on a record batch and return a :class:`QualityReport`.

    Args:
        table_name: Target raw table name (must match keys in config dicts above).
        records:    List of flat dicts produced by an extractor.

    Returns:
        :class:`QualityReport` — ``passed=False`` if any hard check fails.
    """
    if not records:
        logger.warning("Quality check skipped — empty batch for %s", table_name)
        return QualityReport(table=table_name, total_records=0)

    report = QualityReport(table=table_name, total_records=len(records))

    # ── 1. Null checks ────────────────────────────────────────────────────────
    for field_name in _REQUIRED_FIELDS.get(table_name, []):
        null_count = sum(1 for r in records if r.get(field_name) is None)
        if null_count > 0:
            report.null_violations.append(f"{field_name}: {null_count} null(s)")
            report.passed = False

    # ── 2. Within-batch duplicate detection (informational only) ─────────────
    key_fields = _UNIQUE_KEYS.get(table_name, [])
    if key_fields:
        keys = [tuple(r.get(k) for k in key_fields) for r in records]
        report.duplicate_keys = len(keys) - len(set(keys))
        if report.duplicate_keys > 0:
            logger.warning(
                "Quality [%s]: %d duplicate key(s) in batch — upsert will deduplicate",
                table_name,
                report.duplicate_keys,
            )

    # ── 3. Range checks ───────────────────────────────────────────────────────
    for field_name, min_val, max_val in _RANGE_CHECKS.get(table_name, []):
        out_of_range = [
            r for r in records
            if r.get(field_name) is not None
            and not (min_val <= float(r[field_name]) <= max_val)
        ]
        if out_of_range:
            report.range_violations.append(
                f"{field_name}: {len(out_of_range)} value(s) outside [{min_val}, {max_val}]"
            )
            report.passed = False

    report.log()
    return report
