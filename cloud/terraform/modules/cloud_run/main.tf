resource "google_cloud_run_v2_job" "dbt" {
  name     = "dbt-run"
  location = var.region
  project  = var.project_id
  labels   = var.labels

  template {
    template {
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
}
