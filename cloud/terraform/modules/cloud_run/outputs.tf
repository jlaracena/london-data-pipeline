output "job_name" {
  description = "Name of the dbt Cloud Run job."
  value       = google_cloud_run_v2_job.dbt.name
}

output "artifact_registry_repo" {
  description = "Full Artifact Registry repository URL for pushing the dbt Docker image."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.dbt.repository_id}"
}
