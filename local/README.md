# Forest eBikes — Local Data Pipeline

End-to-end data pipeline that collects London city data from four public APIs,
transforms it, and loads it into a local PostgreSQL data warehouse orchestrated
by Apache Airflow running in Docker.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         Docker Compose                           │
│                                                                  │
│  ┌─────────────┐    ┌──────────────────────────────────────┐    │
│  │  PostgreSQL  │    │           Apache Airflow              │    │
│  │             │    │  ┌────────────┐  ┌─────────────────┐ │    │
│  │  DB: forest │◄───┤  │ Scheduler  │  │   Webserver     │ │    │
│  │  ──────────  │    │  └─────┬──────┘  └─────────────────┘ │    │
│  │  raw_weather │    │        │ triggers                      │    │
│  │  raw_air_q.. │    │  ┌─────▼──────────────────────────┐  │    │
│  │  raw_news    │    │  │  DAG: forest_pipeline_local     │  │    │
│  │  raw_countri │    │  │                                 │  │    │
│  └─────────────┘    │  │  extract_load_weather  ──────►  │  │    │
│                      │  │  extract_load_air_quality ───►  │  │    │
│  APIs (external)     │  │  extract_load_news  ──────────► │  │    │
│  ┌───────────────┐   │  │  extract_load_countries ──────► │  │    │
│  │ Open-Meteo    │   │  │           │                     │  │    │
│  │ OpenAQ        │──►│  │  validate_loads  ◄──────────────┘  │    │
│  │ NewsAPI       │   │  └─────────────────────────────────┘  │    │
│  │ RestCountries │   └──────────────────────────────────────┘    │
│  └───────────────┘                                                │
└──────────────────────────────────────────────────────────────────┘
```

**Data flow:** APIs → Python extractors → `load_to_postgres()` (upsert) → raw tables → validate

## Prerequisites

- Docker Desktop (>= 4.x) with Compose V2
- 4 GB RAM allocated to Docker
- Free port 8080 (Airflow UI)
- A free NewsAPI key from [newsapi.org](https://newsapi.org)

## Setup & Run

**1. Clone and enter the local directory**
```bash
cd local/
```

**2. Create your `.env` file**
```bash
cp .env.example .env
# Edit .env and set NEWS_API_KEY to your actual key
```

**3. Set the Airflow UID (Linux/Mac)**
```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

**4. Initialise Airflow and start all services**
```bash
docker compose up airflow-init
docker compose up -d
```

**5. Verify all containers are healthy**
```bash
docker compose ps
```
All services should show `healthy` or `running`.

## Triggering the DAG

**Via Airflow UI (recommended)**
1. Open [http://localhost:8080](http://localhost:8080)
2. Login with `airflow` / `airflow`
3. Find `forest_pipeline_local` and toggle it **On**
4. Click ▶ **Trigger DAG**

**Via CLI**
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger forest_pipeline_local
```

**Monitor run**
```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs -d forest_pipeline_local
```

## Data Model

| Table | Grain | Dedup key | Schedule |
|---|---|---|---|
| `raw_weather` | One row per forecast hour | `(forecast_date, location_city)` | Daily |
| `raw_air_quality` | One row per measurement | `(location_id, parameter, measured_at)` | Daily |
| `raw_news` | One row per article | `(article_id)` MD5 of URL | Daily |
| `raw_countries` | One row per country | `(country_code)` | Daily |

All tables include `ingested_at TIMESTAMPTZ` and `_source VARCHAR` for lineage tracking.

Upsert strategy: `INSERT ... ON CONFLICT DO NOTHING` — idempotent re-runs never duplicate rows.

## Stopping

```bash
docker compose down          # stop, keep volumes
docker compose down -v       # stop and delete all data
```
