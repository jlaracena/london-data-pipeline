# Source code bucket — holds zipped function source archives
resource "google_storage_bucket" "function_source" {
  name                        = "${var.project_id}-forest-fn-source"
  location                    = var.region
  uniform_bucket_level_access = true
  labels                      = var.labels
}

locals {
  functions = {
    weather      = "fn_extract_weather"
    airquality   = "fn_extract_airquality"
    news         = "fn_extract_news"
    countries    = "fn_extract_countries"
    tfl          = "fn_extract_tfl"
    bankholidays = "fn_extract_bankholidays"
    crime        = "fn_extract_crime"
  }
}

# Upload source archives (zip files are expected to be pre-built by CI/CD)
resource "google_storage_bucket_object" "source" {
  for_each = local.functions
  name     = "${each.value}.zip"
  bucket   = google_storage_bucket.function_source.name
  source   = "${path.module}/../../../functions/${each.value}/${each.value}.zip"
}

# Cloud Functions Gen 2
resource "google_cloudfunctions2_function" "extractor" {
  for_each = local.functions
  name     = replace(each.value, "_", "-")
  location = var.region
  labels   = var.labels

  build_config {
    runtime     = "python311"
    entry_point = "handler"
    source {
      storage_source {
        bucket     = google_storage_bucket.function_source.name
        object     = google_storage_bucket_object.source[each.key].name
        generation = google_storage_bucket_object.source[each.key].generation
      }
    }
  }

  service_config {
    max_instance_count             = 3
    available_memory               = "256M"
    timeout_seconds                = 60
    all_traffic_on_latest_revision = true

    environment_variables = {
      BQ_PROJECT = var.project_id
      BQ_DATASET = var.bq_dataset
    }

    # NEWS_API_KEY is injected from Secret Manager only for the news function
    dynamic "secret_environment_variables" {
      for_each = each.key == "news" ? [1] : []
      content {
        key        = "NEWS_API_KEY"
        project_id = var.project_id
        secret     = var.news_api_key_id
        version    = "latest"
      }
    }
  }
}
