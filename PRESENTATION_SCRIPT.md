# Presentation Script — Forest eBikes Senior Data Engineer Take-Home
**Audience:** Head of Data, CTO, other heads/managers
**Duration:** ~60 minutes
**Language:** English
**Format:** Screen share (GitHub → GCP Console → Looker Studio)

---

## STRUCTURE

| # | Section | Time |
|---|---|---|
| 1 | Opening — What I built | 2 min |
| 2 | Architecture Overview | 5 min |
| 3 | Data Sources — 7 APIs | 5 min |
| 4 | Pipeline Implementation | 12 min |
| 5 | Data Warehouse Design | 8 min |
| 6 | dbt Transformations | 7 min |
| 7 | Data Quality | 5 min |
| 8 | Monitoring & Observability | 4 min |
| 9 | Infrastructure as Code (Terraform) | 3 min |
| 10 | Live Demo — Looker Studio | 7 min |
| 11 | Technical Decisions | 2 min |
| 12 | Q&A | open |

---

## 1. OPENING (2 min)

> *Open GitHub repo: https://github.com/jlaracena/london-data-pipeline*

"Thanks for having me. I want to start by giving you a quick overview of what I built, and then we'll go section by section.

The task asked for an end-to-end pipeline that ingests data from multiple APIs, transforms it, stores it in a data warehouse, and runs on a schedule.

I took that one step further: everything runs in production on GCP — not locally. It ran automatically this morning at 6 AM UTC, as it does every day.

Let me show you the architecture first, and then we'll go deep into each part."

---

## 2. ARCHITECTURE OVERVIEW (5 min)

> *Show ARCHITECTURE.md on GitHub — the Mermaid diagram renders automatically*

"The architecture has four layers:

**Layer 1 — Ingestion.** Seven Cloud Functions written in Python, one per API. Each function runs independently.

**Layer 2 — Orchestration.** Cloud Scheduler triggers all seven functions every day at 6 AM UTC, staggered five minutes apart.

**Layer 3 — Warehouse.** Google BigQuery. Two datasets: `raw` for the landing zone, and `mart` for analytics-ready tables.

**Layer 4 — Transformation.** dbt runs as a Cloud Run Job, picking up where the functions left off and producing three business-facing mart tables.

On top of that: Looker Studio for visualisation, Cloud Monitoring for alerting, Terraform for infrastructure as code, and Secret Manager for credentials.

The entire stack costs approximately **$2.50 per month** to run."

---

## 3. DATA SOURCES — 7 APIs (5 min)

> *Show the Data Sources table in README.md*

"The assessment required four APIs. I implemented all four, plus three additional ones that are directly relevant to Forest's business.

**Required APIs:**

- **Open-Meteo Weather** — hourly forecasts for London: temperature, wind speed, weather code. Useful to understand how weather conditions affect bike demand.

- **Air Quality** — the assessment referenced OpenAQ. I have to be transparent here: the OpenAQ v2 API was permanently retired and returns HTTP 410 Gone. I replaced it with the Open-Meteo Air Quality API, which provides the same pollutants — PM10, PM2.5, NO₂, O₃ — with no API key required and higher reliability. This is documented in the README.

- **NewsAPI** — latest articles about London micro-mobility, queried daily. Useful for brand monitoring and sector trend analysis.

- **RestCountries** — static reference data for the UK: population, languages, currencies, region. Used as a dimension table to enrich other datasets.

**Bonus APIs I added:**

- **TfL BikePoint API** — real-time availability for all ~800 Santander Cycles stations in London. This is Forest's most direct competitor, and understanding their station availability gives useful market context.

- **GOV.UK Bank Holidays** — England, Wales, Scotland, Northern Ireland. Bank holidays affect mobility patterns significantly.

- **UK Police API** — street-level crime incidents for central London, with a specific focus on bicycle theft — which is directly relevant to Forest's risk and operational planning."

---

## 4. PIPELINE IMPLEMENTATION (12 min)

> *Open cloud/functions/fn_extract_weather/main.py on GitHub*

"Every Cloud Function follows the exact same four-step pattern. I'll walk you through it using the weather function as the example.

### Step 1 — Extract

The function calls the API using `httpx`, which is a modern HTTP client with async support. Around the API call I wrap `tenacity` for automatic retries with exponential backoff: it tries up to three times, waiting 2, 4, then 8 seconds between attempts.

Importantly, the retry logic is selective — it only retries on HTTP 5xx errors and network failures. It does **not** retry on 4xx errors, because a bad request won't fix itself by retrying.

### Step 2 — Validate

> *Open cloud/functions/shared/quality.py*

Before writing a single byte to BigQuery, every record passes through `quality.py` — a shared module I copy into each function's package.

It runs three checks:
- **Null checks** — required fields must not be None
- **Range checks** — numeric values must be within physical bounds. For example, temperature between -50°C and 60°C, wind speed between 0 and 300 km/h.
- **Duplicate detection** — within the current batch, we detect records with the same deduplication key.

If any null or range check fails, the function returns HTTP 500 immediately and writes nothing to BigQuery. That's a hard failure — fail fast, don't corrupt the warehouse.

### Step 3 — Load with MERGE

```sql
MERGE `raw_weather` T
USING `_staging_raw_weather` S
ON T.forecast_date = S.forecast_date AND T.location_city = S.location_city
WHEN NOT MATCHED THEN INSERT (...)
```

The load uses a two-step approach: first load the batch to a staging table with WRITE_TRUNCATE, then run a MERGE into the final table. The MERGE guarantees **idempotency**: if the scheduler fires twice in the same day — which can happen with network retries — the second run inserts zero rows. The data is already there.

I call `.result()` after both operations, which blocks until BigQuery confirms the write. Without that, the function could return 200 before the data is actually committed.

### Step 4 — Return JSON

The function returns a structured response:

```json
{
  'status': 'ok',
  'rows_inserted': 168,
  'quality': {
    'passed': true,
    'null_violations': [],
    'range_violations': [],
    'duplicate_keys': 0
  }
}
```

This response is also logged to Cloud Logging, which feeds into our monitoring.

### Orchestration — Cloud Scheduler

> *Show GCP Console → Cloud Scheduler*

Seven jobs, all enabled, all ran this morning. Each job sends an authenticated HTTP POST to the corresponding Cloud Function, using OIDC tokens via a dedicated service account. Without the token, the function returns 403 — the endpoint is not publicly accessible.

The five-minute stagger between jobs is intentional — it prevents any potential lock contention on BigQuery during staging table operations."

---

## 5. DATA WAREHOUSE DESIGN (8 min)

> *Open GCP Console → BigQuery*

"The warehouse is structured in two layers.

### Raw Layer — 7 tables

> *Click on raw dataset, show raw_weather table schema*

Every raw table follows the same conventions:

- **`ingested_at`** — timestamp of when the pipeline wrote the row. This is the partition key on all tables — BigQuery partitions by day, which makes date-range queries significantly cheaper.
- **`_source`** — the API identifier, for lineage tracking.
- A **deduplication key** defined per table — the composite key used in the MERGE statement.

Let me show you the current data volumes:

| Table | Rows today |
|---|---|
| raw_weather | 1,704 |
| raw_air_quality | 5,856 |
| raw_crime | 17,409 |
| raw_tfl_bikepoints | 799 |
| raw_bank_holidays | 280 |
| raw_news | 18 |
| raw_countries | 1 |

The crime table has historical data from the backfill script I'll mention shortly.

### A note on primary and foreign keys

The assessment asks to define primary and foreign keys. BigQuery supports them as metadata since mid-2023, but does not enforce them at write time — they're informational. The equivalent in BigQuery is what I've implemented: MERGE deduplication keys in every function, plus `unique` and `not_null` tests in dbt that run on every deployment. I'll show those in a moment.

If the team uses a tool like Great Expectations or Monte Carlo, those dbt tests feed naturally into data quality monitoring.

### Mart Layer — 3 tables

> *Click on mart_mart dataset*

| Table | Rows | Description |
|---|---|---|
| mart_city_conditions | 71 | Daily weather + air quality aggregate |
| mart_news_enriched | 18 | Articles enriched with UK metadata |
| mart_safety_overview | 42 | Monthly crime + TfL bikes + bank holidays |

These tables are what Looker Studio reads. They're designed for direct BI consumption — no joins needed on the dashboard side.

### Backfill

I also wrote a backfill script in `scripts/backfill.py` that loaded two months of historical data for weather, air quality, and crime, to make the dashboard meaningful from day one."

---

## 6. DBT TRANSFORMATIONS (7 min)

> *Open cloud/dbt/ on GitHub*

"dbt is the transformation layer — it handles the `T` in ELT. It doesn't extract or load data; it transforms data that's already in BigQuery using versioned SQL.

### Why dbt?

Three reasons:
1. **Dependencies are explicit** — `{{ ref('stg_weather') }}` tells dbt that mart_city_conditions depends on stg_weather. It builds the DAG automatically.
2. **Tests are declarative** — you define `not_null` and `unique` constraints in YAML; dbt runs them as SQL assertions.
3. **Everything is in Git** — SQL, tests, documentation. A code review is a data review.

### Staging Layer — 7 views

The staging models sit between raw and mart. They're materialised as views — no storage cost, always fresh. Each staging model does light transformations:

- `stg_weather` — casts types and adds `weather_description` from the WMO weathercode using a CASE WHEN
- `stg_air_quality` — filters out negative sensor readings (`value < 0`) which indicate sensor errors
- `stg_tfl_bikepoints` — computes `occupancy_rate = nb_bikes / nb_docks`
- `stg_bank_holidays` — parses date strings and adds `is_england_wales` flag
- `stg_crime` — adds `is_resolved` (outcome_status is not null) and `is_bicycle_theft` (category = 'bicycle-theft') — the latter is directly relevant to Forest's operations
- `stg_news` — adds `is_recent` flag for articles published in the last 7 days

### Mart Layer — 3 tables

> *Show models/marts/ on GitHub*

**mart_city_conditions** joins weather and air quality by calendar date and produces one row per day with average temperature, max wind speed, and the list of pollutants measured.

**mart_news_enriched** joins news articles with the UK country reference — a CROSS JOIN because there's only one country row. It adds population, region, and country name to each article.

**mart_safety_overview** is the most complex one. It joins crime incidents with TfL bike availability and bank holiday flags, aggregated by month and crime category. The `bicycle_theft_count` column is the metric I'd highlight most — it shows monthly bicycle thefts alongside Santander Cycles dock occupancy and bank holiday periods.

### dbt runs as Cloud Run Job

dbt runs as a containerised job on Cloud Run — it spins up, runs all models, runs all tests, then shuts down. Last run was today at 01:53 UTC. No long-running server to maintain."

---

## 7. DATA QUALITY (5 min)

> *Open cloud/functions/shared/quality.py on GitHub*

"Data quality operates at two levels.

### Level 1 — Extraction time (quality.py)

Every function validates records before touching BigQuery.

```python
# Example — weather validation rules
NULL_RULES = {
    "raw_weather": ["forecast_date", "location_city"],
}
RANGE_RULES = {
    "raw_weather": {
        "temperature_2m": (-50, 60),
        "windspeed_10m": (0, 300),
        "weathercode": (0, 99),
    }
}
```

The validator returns a structured report. If `passed` is False, the function returns HTTP 500 — Cloud Scheduler will surface this as a failed job, Cloud Monitoring will fire an alert.

### Level 2 — Transformation time (dbt tests)

> *Open cloud/dbt/models/schema.yml on GitHub*

dbt runs `not_null` and `unique` tests on all primary columns across all 10 models. These run every time dbt deploys, which happens daily after the extraction functions complete.

For example, `stg_news` tests that `article_id` is both not_null and unique. If somehow a duplicate slipped through the MERGE, the dbt test catches it before the mart table is updated.

### Level 3 — Duplicate handling

BigQuery MERGE handles cross-run deduplication. quality.py handles within-batch deduplication. Together, these cover both scenarios."

---

## 8. MONITORING & OBSERVABILITY (4 min)

> *Show GCP Console → Cloud Monitoring*

"There are two monitoring layers.

### Cloud Monitoring

A log-based metric counts `severity=ERROR` log entries from any Cloud Function. An alert policy fires if there's more than zero errors in a five-minute window. The notification goes to email — configurable via Terraform variable.

Max one alert per hour to avoid noise during retries.

### pipeline_runs table

> *Open BigQuery → raw → pipeline_runs*

Every function execution writes a row to `pipeline_runs` — an observability audit log. It records:
- Which table was processed
- How many rows were extracted, inserted, and skipped
- Quality check results — null violations, range violations, duplicate count
- Start and end time
- Status: success, warning, or failed

This means I can query:

```sql
SELECT table_name, status, rows_inserted, finished_at - started_at AS duration
FROM raw.pipeline_runs
ORDER BY started_at DESC
```

And immediately see the health of every pipeline run over time. If rows_inserted suddenly drops to zero for a table that normally inserts 168 rows, something broke upstream — even if the function returned 200.

This pattern is what I'd call production-grade observability. It goes beyond just logging errors."

---

## 9. INFRASTRUCTURE AS CODE — TERRAFORM (3 min)

> *Open cloud/terraform/ on GitHub*

"Everything in GCP was provisioned with Terraform. The infrastructure is fully reproducible — I can tear it down and recreate it in a single `terraform apply`.

The Terraform project is split into six modules:

```
modules/
  bigquery/        ← datasets + partitioned tables with schemas
  cloud_functions/ ← zip packages + Gen 2 function deployment
  cloud_scheduler/ ← 7 jobs + service account + OIDC config
  secret_manager/  ← NewsAPI key storage
  cloud_run/       ← dbt job (image hosted on GHCR)
  monitoring/      ← log metric + alert policy + email channel
```

Each module is independently deployable. If I want to update just the Cloud Functions without touching the scheduler, I run `terraform apply -target=module.cloud_functions`.

The NewsAPI key is stored in Secret Manager and injected at runtime into the function via IAM-controlled access. It never appears in code or environment variables."

---

## 10. LIVE DEMO — LOOKER STUDIO (7 min)

> *Open Looker Studio dashboard: https://lookerstudio.google.com/reporting/73a87d05-ee21-453c-8a5f-80fd6478e9a3*

"Let me show you the live data.

### Page 1 — City Conditions

This page reads from `mart_city_conditions`. It shows two months of daily temperature data for London, with air quality parameters measured each day. All this data was loaded by the backfill script and updated daily since.

### Page 2 — Safety Overview

This reads from `mart_safety_overview`. The X axis is crime category — bicycle theft is the one I'd highlight first. You can see the incident count, resolution rate, and how Santander Cycles dock occupancy correlates with the data.

The `is_bicycle_theft` flag in the staging model means this can be filtered very specifically for Forest's use case.

### Page 3 — News

Eighteen articles from the last pipeline run. Source, publication date, headline — updated daily. Useful for brand monitoring without manual effort.

### Page 4 — TfL Bike Stations

A bubble map of all ~800 Santander Cycles stations in London, with real-time availability data. This is the competitive landscape. Forest could use this to identify zones where Santander's bikes are consistently unavailable — potential expansion opportunities.

### Page 5 — Pipeline Health

This reads directly from `pipeline_runs`. You can see every function execution, its status, how many rows it inserted, and whether quality checks passed. This page alone tells you if the pipeline is healthy without needing to open GCP Console."

---

## 11. TECHNICAL DECISIONS (2 min)

"A few choices worth mentioning explicitly.

**Cloud Scheduler over Airflow/Mage.** The assessment suggested Airflow or Mage locally. I chose Cloud Scheduler because the seven functions are independent — there are no cross-function dependencies that require a DAG. Cloud Composer, which is managed Airflow on GCP, costs around $300/month just to keep the environment alive. Cloud Scheduler costs $0.10/month. For this use case, it's the right tool. If the pipeline grows to 50+ sources with complex dependencies, I'd migrate to Composer or Prefect.

**BigQuery over PostgreSQL.** Serverless, scales to petabytes without configuration, native integration with dbt and Looker Studio, and the free tier covers this project's entire storage and query volume.

**Cloud Functions Gen 2 over Gen 1.** Gen 2 runs on Cloud Run internally, which means timeouts up to 60 minutes versus Gen 1's 9-minute limit, more memory, and concurrent instance support.

**OpenAQ replacement.** I was transparent about this from the start — it's documented in the README. The replacement API is the same provider as the weather API, which simplifies credentials and gives higher reliability."

---

## ANTICIPATED QUESTIONS

### "How would you scale this if Forest grew 10x?"

"The architecture already handles it well. BigQuery MERGE is set-based — it scales linearly with data volume. Daily partitioning means queries only scan the relevant day's data, not the full table. If we needed to process 10x more API sources, I'd add a Pub/Sub topic between Scheduler and Functions to handle fan-out, and consider moving to Composer for DAG visibility. The MERGE pattern and dbt incremental models stay the same."

### "What happens if an API goes down?"

"Three retries with exponential backoff inside the function. If all three fail, the function returns HTTP 500, which Cloud Scheduler marks as a failed job. Cloud Monitoring fires an email alert within five minutes. The raw table from the previous day is still there — downstream dbt models aren't affected because they use `MERGE` semantics too. No data is corrupted, just that day's data is missing for that source."

### "How do you handle schema changes in the APIs?"

"The MERGE uses an explicit column list — not SELECT *. If the API adds a new field, we just ignore it until we decide to add it to the schema. If it removes a field we depend on, the null check in quality.py will catch it and raise an alert before any bad data reaches the warehouse."

### "Why not use dbt for the extraction layer too?"

"dbt is a transformation tool — it can't make HTTP calls or handle authentication to external APIs. The Cloud Functions handle extraction and loading; dbt handles transformation. That's the correct ELT separation of concerns."

### "What would you do differently with more time?"

"Three things. First, I'd add dbt incremental models with `is_incremental()` so dbt only processes new raw rows instead of scanning the full table on every run. Second, I'd add a dead-letter queue via Pub/Sub so failed function runs are automatically retried without manual intervention. Third, I'd add unit tests for the Python extraction logic using pytest and mock HTTP responses — the current quality checks validate the output, but I'd also want to test the transformation logic in isolation."

### "The pipeline_runs table shows dag_run_id — you're using Airflow terminology. Why?"

"Good catch. The column was named with Airflow terminology as a forward-looking design — if the team migrates to Airflow or Prefect, the table schema stays compatible. Currently Cloud Scheduler populates it with the job execution timestamp."

---

## CLOSING

"To summarise: a fully serverless production pipeline on GCP, seven APIs, complete data quality layer, dbt for transformations, Terraform for reproducible infrastructure, Cloud Monitoring for alerting, pipeline observability with an audit log, and a live Looker Studio dashboard — all running for approximately $2.50 per month.

I'm happy to go deeper into any section. What would you like to explore?"

---

*[END OF SCRIPT]*
