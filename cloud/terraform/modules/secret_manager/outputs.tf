output "news_api_key_secret_id" {
  description = "Secret Manager secret ID for the NewsAPI key."
  value       = google_secret_manager_secret.news_api_key.secret_id
}
