# ── Artifact Registry — Docker repository for dbt image ────────────────────────
resource "google_artifact_registry_repository" "dbt" {
  project       = var.project_id
  location      = var.region
  repository_id = "forest-pipeline"
  format        = "DOCKER"
  description   = "Docker images for Forest eBikes data pipeline"
}

# ── Service Account for the dbt Cloud Run job ──────────────────────────────────
resource "google_service_account" "dbt" {
  project      = var.project_id
  account_id   = "sa-dbt-runner"
  display_name = "dbt Cloud Run Job SA"
}

resource "google_project_iam_member" "dbt_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_project_iam_member" "dbt_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}

# ── Cloud Run Job — dbt runner ─────────────────────────────────────────────────
resource "google_cloud_run_v2_job" "dbt" {
  name     = "dbt-run"
  location = var.region
  project  = var.project_id
  labels   = var.labels

  template {
    template {
      service_account = google_service_account.dbt.email

      containers {
        image = var.dbt_image

        env {
          name  = "DBT_PROJECT"
          value = var.project_id
        }

        env {
          name  = "DBT_DATASET"
          value = var.bq_dataset
        }
      }

      max_retries = 1
    }
  }

  depends_on = [
    google_project_iam_member.dbt_bq_data_editor,
    google_project_iam_member.dbt_bq_job_user,
  ]
}
