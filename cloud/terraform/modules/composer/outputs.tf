output "airflow_uri" {
  description = "Airflow web UI URI for the Cloud Composer environment."
  value       = google_composer_environment.main.config[0].airflow_uri
}

output "gcs_bucket" {
  description = "GCS bucket used by the Composer environment for DAGs and plugins."
  value       = google_composer_environment.main.config[0].dag_gcs_prefix
}
