resource "google_secret_manager_secret" "news_api_key" {
  secret_id = "news-api-key"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = var.labels
}

resource "google_secret_manager_secret_version" "news_api_key" {
  secret      = google_secret_manager_secret.news_api_key.id
  secret_data = var.news_api_key
}
