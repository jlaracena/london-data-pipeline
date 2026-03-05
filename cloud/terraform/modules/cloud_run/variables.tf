variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region for the Cloud Run job."
  type        = string
}

variable "dbt_image" {
  description = "Artifact Registry image URI for the dbt runner container."
  type        = string
  default     = ""
}

variable "bq_dataset" {
  description = "BigQuery mart dataset ID passed to the dbt container."
  type        = string
}

variable "labels" {
  description = "Labels applied to the Cloud Run job."
  type        = map(string)
  default     = {}
}
