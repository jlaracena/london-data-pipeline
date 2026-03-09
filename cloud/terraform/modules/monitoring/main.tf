# Cloud Monitoring — log-based alerts for Cloud Function failures
# Fires a notification when any extractor function returns an error.

variable "project_id"        { type = string }
variable "alert_email"       { type = string }
variable "function_names" {
  type    = list(string)
  default = [
    "fn-extract-weather", "fn-extract-airquality", "fn-extract-news",
    "fn-extract-countries", "fn-extract-tfl", "fn-extract-bankholidays",
    "fn-extract-crime"
  ]
}

# Email notification channel
resource "google_monitoring_notification_channel" "email" {
  display_name = "Forest Pipeline Alerts"
  type         = "email"
  project      = var.project_id
  labels       = { email_address = var.alert_email }
}

# Log-based metric: counts ERROR severity logs from Cloud Functions
resource "google_logging_metric" "function_errors" {
  name    = "forest_pipeline_function_errors"
  project = var.project_id

  filter = <<-FILTER
    resource.type="cloud_run_revision"
    severity=ERROR
    labels."goog-managed-by"="cloudfunctions"
  FILTER

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    labels {
      key         = "function_name"
      value_type  = "STRING"
      description = "Name of the Cloud Function"
    }
  }

  label_extractors = {
    "function_name" = "EXTRACT(resource.labels.service_name)"
  }
}

# Alert policy: fires when any function logs an error
resource "google_monitoring_alert_policy" "function_errors" {
  display_name = "Forest Pipeline — Function Error Alert"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "Cloud Function error rate > 0"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/forest_pipeline_function_errors\" resource.type=\"cloud_run_revision\""
      duration        = "60s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_COUNT"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]

  documentation {
    content   = "A Cloud Function in the Forest eBikes pipeline logged an ERROR. Check Cloud Logging for details."
    mime_type = "text/markdown"
  }
}

output "notification_channel_id" {
  value = google_monitoring_notification_channel.email.name
}
