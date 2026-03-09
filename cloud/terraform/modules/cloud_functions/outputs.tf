output "function_urls" {
  description = "HTTP trigger URLs keyed by function short name."
  value = {
    for k, fn in google_cloudfunctions2_function.extractor :
    k => fn.service_config[0].uri
  }
}
