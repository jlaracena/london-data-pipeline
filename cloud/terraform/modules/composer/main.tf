resource "google_composer_environment" "main" {
  name    = "forest-pipeline"
  region  = var.region
  project = var.project_id
  labels  = var.labels

  config {
    software_config {
      image_version = "composer-2-airflow-2"

      pypi_packages = {
        "apache-airflow-providers-google" = ">=10.0.0"
        "httpx"                           = ">=0.27.0"
        "tenacity"                        = ">=8.2.0"
      }

      env_variables = {
        FOREST_PIPELINE_ENV = "cloud"
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = var.composer_service_account
    }
  }
}
