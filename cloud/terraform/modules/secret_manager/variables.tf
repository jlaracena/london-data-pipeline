variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "news_api_key" {
  description = "NewsAPI.org API key value."
  type        = string
  sensitive   = true
}

variable "labels" {
  description = "Labels applied to all Secret Manager resources."
  type        = map(string)
  default     = {}
}
