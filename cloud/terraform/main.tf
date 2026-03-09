terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.region
}

# ── Shared labels applied to all resources ─────────────────────────────────────
locals {
  common_labels = {
    project = "forest-pipeline"
    env     = "cloud"
  }
}

# ── Modules ────────────────────────────────────────────────────────────────────

module "secret_manager" {
  source       = "./modules/secret_manager"
  project_id   = var.gcp_project_id
  news_api_key = var.news_api_key
  labels       = local.common_labels
}

module "bigquery" {
  source     = "./modules/bigquery"
  project_id = var.gcp_project_id
  region     = var.region
  labels     = local.common_labels
}

module "cloud_functions" {
  source          = "./modules/cloud_functions"
  project_id      = var.gcp_project_id
  region          = var.region
  news_api_key_id = module.secret_manager.news_api_key_secret_id
  bq_dataset      = module.bigquery.raw_dataset_id
  labels          = local.common_labels
}

module "cloud_scheduler" {
  source       = "./modules/cloud_scheduler"
  project_id   = var.gcp_project_id
  region       = var.region
  function_urls = module.cloud_functions.function_urls
}

module "cloud_run" {
  source     = "./modules/cloud_run"
  project_id = var.gcp_project_id
  region     = var.region
  dbt_image  = var.dbt_image
  bq_dataset = module.bigquery.mart_dataset_id
  labels     = local.common_labels
}

module "monitoring" {
  source       = "./modules/monitoring"
  project_id   = var.gcp_project_id
  alert_email  = var.alert_email
}
