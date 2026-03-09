output "raw_dataset_id" {
  description = "BigQuery dataset ID for the raw layer."
  value       = module.bigquery.raw_dataset_id
}

output "mart_dataset_id" {
  description = "BigQuery dataset ID for the mart layer."
  value       = module.bigquery.mart_dataset_id
}

output "function_urls" {
  description = "HTTP trigger URLs for each Cloud Function extractor."
  value       = module.cloud_functions.function_urls
}

output "scheduler_jobs" {
  description = "Cloud Scheduler job names."
  value       = [for k, _ in module.cloud_scheduler.job_names : k]
}

output "artifact_registry_repo" {
  description = "Artifact Registry repo URL for pushing the dbt Docker image."
  value       = module.cloud_run.artifact_registry_repo
}

output "dbt_job_name" {
  description = "Cloud Run dbt job name."
  value       = module.cloud_run.job_name
}
