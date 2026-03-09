"""Shared data quality validation for Forest eBikes Cloud Functions."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_REQUIRED: dict[str, list[str]] = {
    "raw_weather":        ["forecast_date", "location_city", "temperature_2m"],
    "raw_air_quality":    ["parameter", "value", "measured_at"],
    "raw_news":           ["article_id", "title", "url"],
    "raw_countries":      ["country_code", "country_name"],
    "raw_tfl_bikepoints": ["station_id", "snapshot_date", "nb_docks"],
    "raw_bank_holidays":  ["division", "title", "holiday_date"],
    "raw_crime":          ["crime_id", "category", "month"],
}

_UNIQUE_KEYS: dict[str, list[str]] = {
    "raw_weather":        ["forecast_date", "location_city"],
    "raw_air_quality":    ["location_id", "parameter", "measured_at"],
    "raw_news":           ["article_id"],
    "raw_countries":      ["country_code"],
    "raw_tfl_bikepoints": ["station_id", "snapshot_date"],
    "raw_bank_holidays":  ["division", "holiday_date", "title"],
    "raw_crime":          ["crime_id"],
}

_RANGES: dict[str, list[tuple[str, float, float]]] = {
    "raw_weather": [
        ("temperature_2m", -50.0, 60.0),
        ("windspeed_10m",    0.0, 300.0),
        ("weathercode",      0.0,  99.0),
    ],
    "raw_air_quality": [("value", 0.0, 10_000.0)],
    "raw_tfl_bikepoints": [
        ("lat",      51.0,  52.0),
        ("lon",      -1.0,   0.5),
        ("nb_bikes",  0.0, 100.0),
        ("nb_docks",  1.0, 100.0),
    ],
}


@dataclass
class QualityReport:
    table: str
    total: int
    null_violations: list[str] = field(default_factory=list)
    range_violations: list[str] = field(default_factory=list)
    duplicate_keys: int = 0
    passed: bool = True

    def issues(self) -> list[str]:
        return self.null_violations + self.range_violations

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed":           self.passed,
            "total_records":    self.total,
            "null_violations":  self.null_violations,
            "range_violations": self.range_violations,
            "duplicate_keys":   self.duplicate_keys,
        }


def validate(table: str, records: list[dict[str, Any]]) -> QualityReport:
    if not records:
        logger.warning("Quality check skipped — empty batch for %s", table)
        return QualityReport(table=table, total=0)

    report = QualityReport(table=table, total=len(records))

    for field_name in _REQUIRED.get(table, []):
        nulls = sum(1 for r in records if r.get(field_name) is None)
        if nulls:
            report.null_violations.append(f"{field_name}: {nulls} null(s)")
            report.passed = False

    key_fields = _UNIQUE_KEYS.get(table, [])
    if key_fields:
        keys = [tuple(r.get(k) for k in key_fields) for r in records]
        report.duplicate_keys = len(keys) - len(set(keys))
        if report.duplicate_keys:
            logger.warning("Quality [%s]: %d duplicate key(s) — MERGE will deduplicate",
                           table, report.duplicate_keys)

    for field_name, lo, hi in _RANGES.get(table, []):
        bad = [r for r in records
               if r.get(field_name) is not None
               and not (lo <= float(r[field_name]) <= hi)]
        if bad:
            report.range_violations.append(
                f"{field_name}: {len(bad)} value(s) outside [{lo}, {hi}]")
            report.passed = False

    logger.info("Quality [%s] %s | records=%d | nulls=%s | dupes=%d | range=%s",
                table, "PASSED ✓" if report.passed else "FAILED ✗",
                report.total, report.null_violations or "none",
                report.duplicate_keys, report.range_violations or "none")
    return report
