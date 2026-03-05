variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "BigQuery dataset location."
  type        = string
}

variable "labels" {
  description = "Labels applied to all BigQuery resources."
  type        = map(string)
  default     = {}
}
