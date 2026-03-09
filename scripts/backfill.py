"""
Backfill script — loads last 2 months of historical data into BigQuery.

Covers:
  - raw_weather     : Open-Meteo archive API (free, no key)
  - raw_air_quality : Open-Meteo AQ API with past_days=60
  - raw_crime       : UK Police API queried by month

Usage:
  pip install httpx google-cloud-bigquery
  python scripts/backfill.py
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT   = "peppy-oven-288419"
DATASET   = "raw"
LAT, LON  = 51.5074, -0.1278

# ── BigQuery helpers ──────────────────────────────────────────────────────────

def _merge(client: bigquery.Client, records: list[dict], table: str, merge_keys: list[str], schema: list[bigquery.SchemaField]) -> int:
    staging = f"{PROJECT}.{DATASET}._staging_{table}"
    target  = f"{PROJECT}.{DATASET}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )
    client.load_table_from_json(records, staging, job_config=job_config).result()

    on_clause     = " AND ".join(f"T.{k} = S.{k}" for k in merge_keys)
    col_names     = ", ".join(f.name for f in schema)
    col_values    = ", ".join(f"S.{f.name}" for f in schema)
    merge_sql = f"""
        MERGE `{target}` T
        USING `{staging}` S ON {on_clause}
        WHEN NOT MATCHED THEN INSERT ({col_names}) VALUES ({col_values})
    """
    result = client.query(merge_sql).result()
    return result.num_dml_affected_rows or 0


# ── Weather ───────────────────────────────────────────────────────────────────

def backfill_weather(client: bigquery.Client) -> None:
    logger.info("Backfilling raw_weather (Jan–Mar 2026)...")
    ingested_at = datetime.now(timezone.utc).isoformat()

    resp = httpx.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude":   LAT,
            "longitude":  LON,
            "start_date": "2026-01-01",
            "end_date":   "2026-03-07",
            "hourly":     "temperature_2m,windspeed_10m,weathercode",
            "timezone":   "UTC",
        },
        timeout=60,
    )
    resp.raise_for_status()
    hourly = resp.json()["hourly"]

    records = [
        {
            "ingested_at":   ingested_at,
            "location_city": "London",
            "temperature_2m": hourly["temperature_2m"][i],
            "windspeed_10m":  hourly["windspeed_10m"][i],
            "weathercode":    hourly["weathercode"][i],
            "forecast_date":  hourly["time"][i],
            "_source":        "open-meteo-archive",
        }
        for i in range(len(hourly["time"]))
    ]

    schema = [
        bigquery.SchemaField("ingested_at",    "TIMESTAMP"),
        bigquery.SchemaField("location_city",  "STRING"),
        bigquery.SchemaField("temperature_2m", "FLOAT64"),
        bigquery.SchemaField("windspeed_10m",  "FLOAT64"),
        bigquery.SchemaField("weathercode",    "INT64"),
        bigquery.SchemaField("forecast_date",  "TIMESTAMP"),
        bigquery.SchemaField("_source",        "STRING"),
    ]
    inserted = _merge(client, records, "raw_weather", ["forecast_date", "location_city"], schema)
    logger.info("raw_weather: %d records fetched, %d inserted", len(records), inserted)


# ── Air Quality ───────────────────────────────────────────────────────────────

def backfill_airquality(client: bigquery.Client) -> None:
    logger.info("Backfilling raw_air_quality (past 60 days)...")
    ingested_at = datetime.now(timezone.utc).isoformat()

    param_map = {"pm10": "pm10", "pm2_5": "pm25", "nitrogen_dioxide": "no2", "ozone": "o3"}

    resp = httpx.get(
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        params=[
            ("latitude",     str(LAT)),
            ("longitude",    str(LON)),
            ("hourly",       "pm10,pm2_5,nitrogen_dioxide,ozone"),
            ("timezone",     "UTC"),
            ("past_days",    "60"),
            ("forecast_days","1"),
        ],
        timeout=60,
    )
    resp.raise_for_status()
    hourly = resp.json()["hourly"]
    times  = hourly["time"]

    records = []
    for i, ts in enumerate(times):
        for raw_key, param_code in param_map.items():
            val = hourly.get(raw_key, [None] * len(times))[i]
            records.append({
                "ingested_at":   ingested_at,
                "location_city": "London",
                "location_id":   0,
                "parameter":     param_code,
                "value":         val,
                "unit":          "µg/m³",
                "measured_at":   ts,
                "_source":       "open-meteo-aq",
            })

    schema = [
        bigquery.SchemaField("ingested_at",   "TIMESTAMP"),
        bigquery.SchemaField("location_city", "STRING"),
        bigquery.SchemaField("location_id",   "INT64"),
        bigquery.SchemaField("parameter",     "STRING"),
        bigquery.SchemaField("value",         "FLOAT64"),
        bigquery.SchemaField("unit",          "STRING"),
        bigquery.SchemaField("measured_at",   "TIMESTAMP"),
        bigquery.SchemaField("_source",       "STRING"),
    ]
    inserted = _merge(client, records, "raw_air_quality", ["location_id", "parameter", "measured_at"], schema)
    logger.info("raw_air_quality: %d records fetched, %d inserted", len(records), inserted)


# ── Crime ─────────────────────────────────────────────────────────────────────

def backfill_crime(client: bigquery.Client) -> None:
    months = ["2025-11", "2025-12", "2026-01", "2026-02"]
    all_records: list[dict] = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    for month in months:
        logger.info("Fetching crime for %s...", month)
        resp = httpx.get(
            "https://data.police.uk/api/crimes-street/all-crime",
            params={"lat": LAT, "lng": LON, "date": month},
            timeout=60,
        )
        if resp.status_code == 404:
            logger.warning("Crime data not available for %s (404) — skipping", month)
            continue
        resp.raise_for_status()
        crimes = resp.json()

        for c in crimes:
            all_records.append({
                "ingested_at":    ingested_at,
                "crime_id":       c["id"],
                "persistent_id":  c.get("persistent_id") or None,
                "category":       c.get("category"),
                "location_type":  c.get("location_type"),
                "lat":            float(c["location"]["latitude"]) if c.get("location") else None,
                "lon":            float(c["location"]["longitude"]) if c.get("location") else None,
                "street_name":    (c.get("location") or {}).get("street", {}).get("name"),
                "outcome_status": (c.get("outcome_status") or {}).get("category") if c.get("outcome_status") else None,
                "month":          c.get("month"),
                "_source":        "uk-police",
            })
        logger.info("  %s: %d crimes fetched", month, len(crimes))

    schema = [
        bigquery.SchemaField("ingested_at",    "TIMESTAMP"),
        bigquery.SchemaField("crime_id",       "INT64"),
        bigquery.SchemaField("persistent_id",  "STRING"),
        bigquery.SchemaField("category",       "STRING"),
        bigquery.SchemaField("location_type",  "STRING"),
        bigquery.SchemaField("lat",            "FLOAT64"),
        bigquery.SchemaField("lon",            "FLOAT64"),
        bigquery.SchemaField("street_name",    "STRING"),
        bigquery.SchemaField("outcome_status", "STRING"),
        bigquery.SchemaField("month",          "STRING"),
        bigquery.SchemaField("_source",        "STRING"),
    ]
    inserted = _merge(client, all_records, "raw_crime", ["crime_id"], schema)
    logger.info("raw_crime: %d records fetched, %d inserted", len(all_records), inserted)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    client = bigquery.Client(project=PROJECT)

    backfill_weather(client)
    backfill_airquality(client)
    backfill_crime(client)

    logger.info("Backfill complete.")
