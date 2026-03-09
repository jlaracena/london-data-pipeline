output "raw_dataset_id" {
  description = "BigQuery dataset ID for the raw layer."
  value       = google_bigquery_dataset.raw.dataset_id
}

output "mart_dataset_id" {
  description = "BigQuery dataset ID for the mart layer."
  value       = google_bigquery_dataset.mart.dataset_id
}
