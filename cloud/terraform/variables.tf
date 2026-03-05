variable "gcp_project_id" {
  description = "GCP project ID where all resources will be created."
  type        = string
}

variable "region" {
  description = "GCP region for all resources. Defaults to europe-west2 (London)."
  type        = string
  default     = "europe-west2"
}

variable "news_api_key" {
  description = "NewsAPI.org API key stored in Secret Manager."
  type        = string
  sensitive   = true
}

variable "dbt_image" {
  description = "Full Artifact Registry image URI for the dbt Cloud Run job."
  type        = string
  default     = "europe-west2-docker.pkg.dev/YOUR_PROJECT/forest-pipeline/dbt:latest"
}
