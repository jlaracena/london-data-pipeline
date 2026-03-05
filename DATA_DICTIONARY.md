# Data Dictionary — Forest eBikes London Data Pipeline

All timestamps are UTC. All tables include `ingested_at TIMESTAMPTZ` (when the row
was written) and `_source VARCHAR` (API identifier for lineage tracking).

---

## Raw Layer (PostgreSQL / BigQuery `raw` dataset)

### `raw_weather`
Source: [Open-Meteo](https://api.open-meteo.com/v1/forecast)
Grain: One row per forecast hour per city
Schedule: Daily

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ingested_at` | TIMESTAMPTZ | NO | Pipeline ingestion timestamp |
| `location_city` | VARCHAR | NO | City name — always `"London"` |
| `temperature_2m` | FLOAT | YES | Air temperature at 2 m in °C |
| `windspeed_10m` | FLOAT | YES | Wind speed at 10 m in km/h |
| `weathercode` | INTEGER | YES | WMO weather interpretation code (0–99) |
| `forecast_date` | TIMESTAMPTZ | NO | Forecast hour (UTC) |
| `_source` | VARCHAR | NO | `"open-meteo"` |

**Unique constraint:** `(forecast_date, location_city)`
**Quality checks:** temperature_2m ∈ [-50, 60], windspeed_10m ∈ [0, 300], weathercode ∈ [0, 99]

---

### `raw_air_quality`
Source: [OpenAQ v2](https://api.openaq.org)
Grain: One row per pollutant measurement per station
Schedule: Daily (latest 100 measurements)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ingested_at` | TIMESTAMPTZ | NO | Pipeline ingestion timestamp |
| `location_city` | VARCHAR | NO | City name — always `"London"` |
| `location_id` | INTEGER | NO | OpenAQ station numeric ID |
| `parameter` | VARCHAR | NO | Pollutant code (e.g. `pm25`, `no2`, `o3`) |
| `value` | FLOAT | YES | Measured concentration |
| `unit` | VARCHAR | YES | Unit of measurement (e.g. `µg/m³`) |
| `measured_at` | TIMESTAMPTZ | NO | Measurement UTC timestamp |
| `_source` | VARCHAR | NO | `"openaq"` |

**Unique constraint:** `(location_id, parameter, measured_at)`
**Quality checks:** value ∈ [0, 10000]

---

### `raw_news`
Source: [NewsAPI](https://newsapi.org) — query: `"London electric bike OR micro-mobility OR forest ebikes"`
Grain: One row per article
Schedule: Daily (latest 20 articles)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ingested_at` | TIMESTAMPTZ | NO | Pipeline ingestion timestamp |
| `article_id` | VARCHAR(32) | NO | MD5 hex digest of article URL — dedup key |
| `title` | TEXT | YES | Article headline |
| `description` | TEXT | YES | Article summary / lead paragraph |
| `url` | TEXT | YES | Canonical article URL |
| `published_at` | TIMESTAMPTZ | YES | Publication UTC timestamp |
| `source_name` | VARCHAR | YES | Publisher name (e.g. `"BBC News"`) |
| `_source` | VARCHAR | NO | `"newsapi"` |

**Unique constraint:** `(article_id)`
**Quality checks:** article_id, title, url must not be null. Articles with missing title or URL are dropped by the extractor.

---

### `raw_countries`
Source: [RestCountries](https://restcountries.com/v3.1/alpha/GB)
Grain: One row per country (static reference)
Schedule: Daily (idempotent — upsert overwrites on re-run)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ingested_at` | TIMESTAMPTZ | NO | Pipeline ingestion timestamp |
| `country_code` | VARCHAR(10) | NO | ISO 3166-1 alpha-2 — always `"GB"` |
| `country_name` | VARCHAR | YES | Common English name |
| `capital` | VARCHAR | YES | Capital city name |
| `population` | BIGINT | YES | Latest population estimate |
| `area_km2` | FLOAT | YES | Total area in km² |
| `region` | VARCHAR | YES | Geographic region (always `"Europe"`) |
| `languages` | TEXT[] | YES | Array of official language names |
| `currencies` | TEXT[] | YES | Array of ISO 4217 currency codes |
| `_source` | VARCHAR | NO | `"restcountries"` |

**Unique constraint:** `(country_code)`

---

### `raw_tfl_bikepoints`
Source: [TfL BikePoint API](https://api.tfl.gov.uk/BikePoint)
Grain: One row per Santander Cycles station per day (daily snapshot)
Schedule: Daily (~800 stations)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ingested_at` | TIMESTAMPTZ | NO | Pipeline ingestion timestamp |
| `snapshot_date` | DATE | NO | Date of the availability snapshot |
| `station_id` | VARCHAR(50) | NO | TfL station ID (e.g. `"BikePoints_1"`) |
| `station_name` | VARCHAR | YES | Station name and street location |
| `lat` | FLOAT | YES | Station latitude (WGS-84) |
| `lon` | FLOAT | YES | Station longitude (WGS-84) |
| `nb_bikes` | INTEGER | YES | Bikes currently available |
| `nb_empty_docks` | INTEGER | YES | Empty docking points |
| `nb_docks` | INTEGER | YES | Total docking capacity |
| `_source` | VARCHAR | NO | `"tfl-bikepoint"` |

**Unique constraint:** `(station_id, snapshot_date)`
**Quality checks:** lat ∈ [51, 52], lon ∈ [-1, 0.5], nb_bikes ∈ [0, 100], nb_docks ∈ [1, 100]

---

### `raw_bank_holidays`
Source: [GOV.UK Bank Holidays API](https://www.gov.uk/bank-holidays.json)
Grain: One row per holiday event per division
Schedule: Daily (static — idempotent re-runs safe)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ingested_at` | TIMESTAMPTZ | NO | Pipeline ingestion timestamp |
| `division` | VARCHAR(50) | NO | UK division (`england-and-wales`, `scotland`, `northern-ireland`) |
| `title` | VARCHAR | NO | Official holiday name |
| `holiday_date` | VARCHAR(10) | NO | Date string in `YYYY-MM-DD` format |
| `notes` | TEXT | YES | Additional notes (e.g. `"Substitute day"`) |
| `bunting` | BOOLEAN | YES | True if official bunting is displayed |
| `_source` | VARCHAR | NO | `"gov-uk-bank-holidays"` |

**Unique constraint:** `(division, holiday_date, title)`

---

### `raw_crime`
Source: [UK Police API](https://data.police.uk/api/crimes-street/all-crime)
Grain: One row per reported street-level crime incident
Schedule: Daily (latest available month, central London)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ingested_at` | TIMESTAMPTZ | NO | Pipeline ingestion timestamp |
| `crime_id` | BIGINT | NO | Unique crime record ID from Police API |
| `persistent_id` | VARCHAR | YES | Cross-month persistent identifier (may be empty) |
| `category` | VARCHAR | NO | Crime category (e.g. `bicycle-theft`, `anti-social-behaviour`) |
| `location_type` | VARCHAR | YES | Location type (`Force`, `BTP`) |
| `lat` | FLOAT | YES | Approximate crime latitude |
| `lon` | FLOAT | YES | Approximate crime longitude |
| `street_name` | VARCHAR | YES | Nearest street name |
| `outcome_status` | VARCHAR | YES | Outcome category if resolved, NULL if open |
| `month` | VARCHAR(7) | YES | Reporting month in `YYYY-MM` format |
| `_source` | VARCHAR | NO | `"uk-police"` |

**Unique constraint:** `(crime_id)`

---

### `pipeline_runs` (Observability Audit Log)
Grain: One row per extract+load task execution
Purpose: Monitor pipeline health, detect regressions, track data volume trends

| Column | Type | Nullable | Description |
|---|---|---|---|
| `id` | SERIAL | NO | Auto-increment primary key |
| `dag_run_id` | VARCHAR | NO | Airflow DAG run identifier |
| `table_name` | VARCHAR | NO | Target raw table |
| `rows_extracted` | INTEGER | YES | Records returned by the extractor |
| `rows_inserted` | INTEGER | YES | Records actually written (post-upsert) |
| `rows_skipped` | INTEGER | YES | Duplicates silently skipped |
| `quality_passed` | BOOLEAN | YES | False if null or range checks failed |
| `null_violations` | TEXT[] | YES | List of null-check failure messages |
| `range_violations` | TEXT[] | YES | List of range-check failure messages |
| `duplicate_keys` | INTEGER | YES | Within-batch duplicate count |
| `started_at` | TIMESTAMPTZ | NO | Task start time |
| `finished_at` | TIMESTAMPTZ | YES | Task end time |
| `status` | VARCHAR(20) | YES | `success`, `warning`, or `failed` |

---

## Staging Layer (dbt views — BigQuery `mart` schema)

| Model | Source | Key transformation |
|---|---|---|
| `stg_weather` | `raw_weather` | Type casts + `weather_description` from weathercode CASE WHEN |
| `stg_air_quality` | `raw_air_quality` | Type casts + filter `value < 0` (sensor errors) |
| `stg_news` | `raw_news` | Type casts + `is_recent` flag (published within 7 days) |
| `stg_countries` | `raw_countries` | Type casts only |
| `stg_tfl_bikepoints` | `raw_tfl_bikepoints` | Type casts + `occupancy_rate = nb_bikes / nb_docks` |
| `stg_bank_holidays` | `raw_bank_holidays` | Parse date string + `is_england_wales` flag |
| `stg_crime` | `raw_crime` | Type casts + `is_resolved` + `is_bicycle_theft` flags |

---

## Mart Layer (dbt tables — BigQuery `mart` schema)

### `mart_city_conditions`
Daily weather + air quality aggregate for London.

| Column | Description |
|---|---|
| `date` | Calendar date (partition key) |
| `avg_temperature_celsius` | Mean hourly temperature for the day |
| `max_windspeed_kmh` | Peak wind speed for the day |
| `parameters_measured` | Array of distinct pollutants measured |

---

### `mart_news_enriched`
Recent micro-mobility news enriched with UK country metadata.

| Column | Description |
|---|---|
| `article_id` | MD5 dedup key |
| `title` | Article headline |
| `published_at` | Publication timestamp |
| `is_recent` | Always true (filter: last 7 days) |
| `country_name` | From `stg_countries` — `"United Kingdom"` |
| `region` | From `stg_countries` — `"Europe"` |
| `population` | UK population figure |

---

### `mart_safety_overview`
Monthly crime statistics + Santander Cycles availability + bank holiday flags.
Designed to help Forest eBikes identify safety conditions affecting micro-mobility demand.

| Column | Description |
|---|---|
| `month_date` | First day of reporting month (partition key) |
| `crime_category` | Crime type |
| `incident_count` | Total incidents that month |
| `bicycle_theft_count` | Bicycle thefts — key micro-mobility safety metric |
| `resolution_rate_pct` | % of incidents with a recorded outcome |
| `total_bikes_available` | Santander Cycles bikes available (TfL snapshot) |
| `avg_occupancy_pct` | Average dock occupancy across all stations |
| `has_bank_holiday` | True if month contains an England & Wales bank holiday |
