# Cloud Scheduler — triggers each Cloud Function daily at 06:00 UTC
# Replaces Cloud Composer for orchestration at a fraction of the cost.

variable "project_id"    { type = string }
variable "region"        { type = string }
variable "function_urls" { type = map(string) }

locals {
  schedules = {
    weather      = { cron = "0 6 * * *",  description = "Daily weather forecast ingestion" }
    airquality   = { cron = "5 6 * * *",  description = "Daily air quality ingestion" }
    news         = { cron = "10 6 * * *", description = "Daily news ingestion" }
    countries    = { cron = "15 6 * * *", description = "Daily countries reference refresh" }
    tfl          = { cron = "20 6 * * *", description = "Daily TfL BikePoints snapshot" }
    bankholidays = { cron = "25 6 * * *", description = "Daily bank holidays refresh" }
    crime        = { cron = "30 6 * * *", description = "Daily crime data ingestion" }
  }
}

resource "google_cloud_scheduler_job" "pipeline" {
  for_each = local.schedules

  name        = "forest-pipeline-${each.key}"
  description = each.value.description
  schedule    = each.value.cron
  time_zone   = "UTC"
  region      = var.region

  http_target {
    uri         = var.function_urls[each.key]
    http_method = "POST"
    body        = base64encode("{}")
    headers     = { "Content-Type" = "application/json" }

    oidc_token {
      service_account_email = google_service_account.scheduler.email
    }
  }

  retry_config {
    retry_count = 3
  }
}

resource "google_service_account" "scheduler" {
  account_id   = "forest-scheduler"
  display_name = "Forest Pipeline Cloud Scheduler SA"
  project      = var.project_id
}

resource "google_project_iam_member" "scheduler_invoker" {
  project = var.project_id
  role    = "roles/cloudfunctions.invoker"
  member  = "serviceAccount:${google_service_account.scheduler.email}"
}

output "job_names" {
  value = { for k, j in google_cloud_scheduler_job.pipeline : k => j.name }
}
