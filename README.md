# London Data Pipeline — Forest eBikes

End-to-end serverless data pipeline that ingests London city data from 7 public APIs
into BigQuery daily, transforms it with dbt, and visualises it in Looker Studio.

Built for the Forest eBikes Senior Data Engineer take-home assessment.

**Live Dashboard:** [London Micro-Mobility Intelligence Dashboard](https://lookerstudio.google.com/reporting/73a87d05-ee21-453c-8a5f-80fd6478e9a3)

---

## Architecture

```
Cloud Scheduler (06:00 UTC)
        │
        ▼ (7 parallel HTTP triggers)
Cloud Functions Gen 2  ──→  BigQuery (raw dataset)
        │                          │
        │                          ▼
        │                   dbt (Cloud Run Job)
        │                          │
        │                          ▼
        │                   BigQuery (mart dataset)
        │                          │
        └──────────────────────────▼
                            Looker Studio Dashboard
```

Full architecture diagram with Mermaid: [`ARCHITECTURE.md`](ARCHITECTURE.md)

---

## Data Sources

| API | Data | Note |
|-----|------|------|
| [Open-Meteo](https://api.open-meteo.com/v1/forecast) | Hourly weather forecast (temp, wind, weathercode) | No API key required |
| [Open-Meteo Air Quality](https://air-quality-api.open-meteo.com/v1/air-quality) | Hourly PM10, PM2.5, NO2, O3 | Replaces OpenAQ v2 (retired/410 Gone) — same provider, no key required |
| [NewsAPI](https://newsapi.org) | Latest articles on London micro-mobility | Free API key required |
| [RestCountries](https://restcountries.com/v3.1/alpha/GB) | UK reference data (population, languages, currencies) | No key required |
| [TfL BikePoint](https://api.tfl.gov.uk/BikePoint) | ~800 Santander Cycles stations — real-time bike availability | No key required |
| [GOV.UK Bank Holidays](https://www.gov.uk/bank-holidays.json) | England & Wales / Scotland / N. Ireland holidays | No key required |
| [UK Police API](https://data.police.uk/api/crimes-street/all-crime) | Street-level crime incidents in central London | No key required |

> **Note on Air Quality API:** The original assessment references OpenAQ (`api.openaq.org`).
> The v2 endpoint was permanently retired (HTTP 410). We use the Open-Meteo Air Quality API
> as a drop-in replacement — same data types (PM10, PM2.5, NO2, O3), no API key, higher reliability.

---

## Repository Structure

```
london-data-pipeline/
├── cloud/
│   ├── dags/                        # Airflow DAG (Cloud Composer reference)
│   ├── dbt/                         # dbt project
│   │   ├── models/
│   │   │   ├── staging/             # 7 staging views (type casts + light transforms)
│   │   │   └── marts/               # 3 mart tables (analytics-ready)
│   │   └── dbt_project.yml
│   ├── functions/
│   │   ├── shared/quality.py        # Shared data quality validator
│   │   ├── fn_extract_weather/
│   │   ├── fn_extract_airquality/
│   │   ├── fn_extract_news/
│   │   ├── fn_extract_countries/
│   │   ├── fn_extract_tfl/
│   │   ├── fn_extract_bankholidays/
│   │   └── fn_extract_crime/
│   └── terraform/
│       ├── main.tf
│       ├── variables.tf
│       └── modules/
│           ├── bigquery/            # Datasets + partitioned tables
│           ├── cloud_functions/     # 7 Gen 2 functions
│           ├── cloud_scheduler/     # Daily triggers (06:00 UTC)
│           ├── secret_manager/      # NewsAPI key
│           ├── cloud_run/           # dbt job
│           └── monitoring/          # Error alerts + email notifications
├── ARCHITECTURE.md
├── DATA_DICTIONARY.md
└── README.md
```

---

## GCP Project

**Project ID:** `peppy-oven-288419` | **Region:** `europe-west2` (London)

---

## Deployment

### Prerequisites

```bash
gcloud auth login
gcloud config set project peppy-oven-288419
terraform -version  # >= 1.5
```

### 1. Package Cloud Functions

```bash
cd cloud/functions
for fn in fn_extract_weather fn_extract_airquality fn_extract_news \
          fn_extract_countries fn_extract_tfl fn_extract_bankholidays fn_extract_crime; do
  cp shared/quality.py $fn/quality.py
  cd $fn && zip -q ${fn}.zip main.py requirements.txt quality.py && cd ..
done
```

### 2. Configure variables

```bash
cp cloud/terraform/terraform.tfvars.example cloud/terraform/terraform.tfvars
# Edit terraform.tfvars with your values
```

### 3. Deploy infrastructure

```bash
cd cloud/terraform
terraform init
terraform apply
```

### 4. Trigger pipeline manually

```bash
TOKEN=$(gcloud auth print-identity-token)
for fn in weather airquality news countries tfl bankholidays crime; do
  curl -s -X POST "https://fn-extract-${fn}-<hash>.a.run.app" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" -d "{}"
done
```

---

## Data Quality

Every Cloud Function validates records before loading:

- **Null checks** — required fields must not be None (hard failure → returns HTTP 500)
- **Range checks** — numeric fields must be within bounds (hard failure → returns HTTP 500)
- **Duplicate detection** — within-batch duplicates logged (BigQuery MERGE handles them)

Quality results are returned in every function response:

```json
{
  "status": "ok",
  "rows_inserted": 168,
  "quality": {
    "passed": true,
    "total_records": 168,
    "null_violations": [],
    "range_violations": [],
    "duplicate_keys": 0
  }
}
```

---

## Data Warehouse Design

### Raw layer (`raw` dataset) — 7 tables, partitioned by `ingested_at`

Idempotent loads via BigQuery `MERGE`. Each table has a defined deduplication key:

| Table | Dedup key |
|-------|-----------|
| `raw_weather` | `(forecast_date, location_city)` |
| `raw_air_quality` | `(location_id, parameter, measured_at)` |
| `raw_news` | `(article_id)` — MD5 of URL |
| `raw_countries` | `(country_code)` |
| `raw_tfl_bikepoints` | `(station_id, snapshot_date)` |
| `raw_bank_holidays` | `(division, holiday_date, title)` |
| `raw_crime` | `(crime_id)` |

### Mart layer (`mart` dataset) — 3 dbt tables

| Model | Description |
|-------|-------------|
| `mart_city_conditions` | Daily weather + air quality aggregate |
| `mart_news_enriched` | Recent news enriched with UK country metadata |
| `mart_safety_overview` | Monthly crime + TfL availability + bank holiday flag |

Full column-level documentation: [`DATA_DICTIONARY.md`](DATA_DICTIONARY.md)

---

## Monitoring

Cloud Monitoring alert fires when any Cloud Function logs an ERROR:
- Notification via email (configured in `terraform.tfvars`)
- Max 1 alert per hour to avoid noise
- View logs: GCP Console → Cloud Logging → filter `severity=ERROR`

---

## Cost Estimate

~**$2.50/month** — Cloud Scheduler ($0.10) + Cloud Functions ($1.50) + BigQuery ($0.52) + Secret Manager (<$0.01). Looker Studio is free.
