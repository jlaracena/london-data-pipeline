variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region for the Composer environment."
  type        = string
}

variable "composer_service_account" {
  description = "Service account email for the Composer environment nodes."
  type        = string
  default     = ""
}

variable "labels" {
  description = "Labels applied to the Composer environment."
  type        = map(string)
  default     = {}
}
