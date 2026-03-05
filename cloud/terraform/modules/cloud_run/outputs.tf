output "job_name" {
  description = "Name of the dbt Cloud Run job."
  value       = google_cloud_run_v2_job.dbt.name
}
