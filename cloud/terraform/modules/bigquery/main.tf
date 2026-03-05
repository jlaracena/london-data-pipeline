resource "google_bigquery_dataset" "raw" {
  dataset_id    = "raw"
  friendly_name = "Forest Pipeline — Raw API data"
  description   = "Landing zone for data ingested from external APIs."
  location      = var.region
  labels        = var.labels
}

resource "google_bigquery_dataset" "mart" {
  dataset_id    = "mart"
  friendly_name = "Forest Pipeline — dbt Mart models"
  description   = "Transformed and aggregated data produced by dbt."
  location      = var.region
  labels        = var.labels
}

# ── raw_weather ───────────────────────────────────────────────────────────────
resource "google_bigquery_table" "raw_weather" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_weather"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  schema = jsonencode([
    { name = "ingested_at",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "location_city", type = "STRING",    mode = "REQUIRED" },
    { name = "temperature_2m", type = "FLOAT64",  mode = "NULLABLE" },
    { name = "windspeed_10m", type = "FLOAT64",   mode = "NULLABLE" },
    { name = "weathercode",   type = "INT64",     mode = "NULLABLE" },
    { name = "forecast_date", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "_source",       type = "STRING",    mode = "REQUIRED" },
  ])

  labels = var.labels
}

# ── raw_air_quality ───────────────────────────────────────────────────────────
resource "google_bigquery_table" "raw_air_quality" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_air_quality"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  schema = jsonencode([
    { name = "ingested_at",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "location_city", type = "STRING",    mode = "REQUIRED" },
    { name = "location_id",   type = "INT64",     mode = "REQUIRED" },
    { name = "parameter",     type = "STRING",    mode = "REQUIRED" },
    { name = "value",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "unit",          type = "STRING",    mode = "NULLABLE" },
    { name = "measured_at",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "_source",       type = "STRING",    mode = "REQUIRED" },
  ])

  labels = var.labels
}

# ── raw_news ──────────────────────────────────────────────────────────────────
resource "google_bigquery_table" "raw_news" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_news"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  schema = jsonencode([
    { name = "ingested_at",  type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "article_id",   type = "STRING",    mode = "REQUIRED" },
    { name = "title",        type = "STRING",    mode = "NULLABLE" },
    { name = "description",  type = "STRING",    mode = "NULLABLE" },
    { name = "url",          type = "STRING",    mode = "NULLABLE" },
    { name = "published_at", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "source_name",  type = "STRING",    mode = "NULLABLE" },
    { name = "_source",      type = "STRING",    mode = "REQUIRED" },
  ])

  labels = var.labels
}

# ── raw_countries ─────────────────────────────────────────────────────────────
resource "google_bigquery_table" "raw_countries" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_countries"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  schema = jsonencode([
    { name = "ingested_at",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "country_code",  type = "STRING",    mode = "REQUIRED" },
    { name = "country_name",  type = "STRING",    mode = "NULLABLE" },
    { name = "capital",       type = "STRING",    mode = "NULLABLE" },
    { name = "population",    type = "INT64",     mode = "NULLABLE" },
    { name = "area_km2",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "region",        type = "STRING",    mode = "NULLABLE" },
    { name = "languages",     type = "STRING",    mode = "REPEATED" },
    { name = "currencies",    type = "STRING",    mode = "REPEATED" },
    { name = "_source",       type = "STRING",    mode = "REQUIRED" },
  ])

  labels = var.labels
}

# ── raw_tfl_bikepoints ────────────────────────────────────────────────────────
resource "google_bigquery_table" "raw_tfl_bikepoints" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_tfl_bikepoints"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  schema = jsonencode([
    { name = "ingested_at",    type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "snapshot_date",  type = "DATE",      mode = "REQUIRED" },
    { name = "station_id",     type = "STRING",    mode = "REQUIRED" },
    { name = "station_name",   type = "STRING",    mode = "NULLABLE" },
    { name = "lat",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "lon",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "nb_bikes",       type = "INT64",     mode = "NULLABLE" },
    { name = "nb_empty_docks", type = "INT64",     mode = "NULLABLE" },
    { name = "nb_docks",       type = "INT64",     mode = "NULLABLE" },
    { name = "_source",        type = "STRING",    mode = "REQUIRED" },
  ])

  labels = var.labels
}

# ── raw_bank_holidays ─────────────────────────────────────────────────────────
resource "google_bigquery_table" "raw_bank_holidays" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_bank_holidays"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  schema = jsonencode([
    { name = "ingested_at",  type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "division",     type = "STRING",    mode = "REQUIRED" },
    { name = "title",        type = "STRING",    mode = "REQUIRED" },
    { name = "holiday_date", type = "STRING",    mode = "REQUIRED" },
    { name = "notes",        type = "STRING",    mode = "NULLABLE" },
    { name = "bunting",      type = "BOOL",      mode = "NULLABLE" },
    { name = "_source",      type = "STRING",    mode = "REQUIRED" },
  ])

  labels = var.labels
}

# ── raw_crime ─────────────────────────────────────────────────────────────────
resource "google_bigquery_table" "raw_crime" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_crime"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  schema = jsonencode([
    { name = "ingested_at",    type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "crime_id",       type = "INT64",     mode = "REQUIRED" },
    { name = "persistent_id",  type = "STRING",    mode = "NULLABLE" },
    { name = "category",       type = "STRING",    mode = "NULLABLE" },
    { name = "location_type",  type = "STRING",    mode = "NULLABLE" },
    { name = "lat",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "lon",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "street_name",    type = "STRING",    mode = "NULLABLE" },
    { name = "outcome_status", type = "STRING",    mode = "NULLABLE" },
    { name = "month",          type = "STRING",    mode = "NULLABLE" },
    { name = "_source",        type = "STRING",    mode = "REQUIRED" },
  ])

  labels = var.labels
}
