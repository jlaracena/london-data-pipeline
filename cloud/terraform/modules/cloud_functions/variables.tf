variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region for Cloud Functions."
  type        = string
}

variable "news_api_key_id" {
  description = "Secret Manager secret ID for the NewsAPI key."
  type        = string
}

variable "bq_dataset" {
  description = "BigQuery raw dataset ID."
  type        = string
}

variable "labels" {
  description = "Labels applied to all Cloud Function resources."
  type        = map(string)
  default     = {}
}
