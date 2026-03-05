output "composer_airflow_uri" {
  description = "Airflow web UI URI for the Cloud Composer environment."
  value       = module.composer.airflow_uri
}

output "raw_dataset_id" {
  description = "BigQuery dataset ID for the raw layer."
  value       = module.bigquery.raw_dataset_id
}

output "mart_dataset_id" {
  description = "BigQuery dataset ID for the mart layer."
  value       = module.bigquery.mart_dataset_id
}

output "dbt_cloud_run_job_name" {
  description = "Name of the Cloud Run job that executes dbt."
  value       = module.cloud_run.job_name
}

output "function_urls" {
  description = "HTTP trigger URLs for each Cloud Function extractor."
  value       = module.cloud_functions.function_urls
}
