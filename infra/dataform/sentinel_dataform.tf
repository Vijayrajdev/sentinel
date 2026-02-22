# The Dataform Repository
resource "google_dataform_repository" "sentinel_repo" {
  provider = google-beta
  name     = "sentinel-forge-pipeline"
  region   = var.region
  project  = var.project_id

  git_remote_settings {
    url                = "https://github.com/Vijayrajdev/sentinel.git"
    default_branch     = "master"
    authentication_token_secret_version = "projects/${var.project_id}/secrets/${var.secret_id}/versions/latest"
  }
}

# The Workflow Configuration (The "Scheduler")
resource "google_dataform_repository_workflow_config" "daily_run" {
  provider       = google-beta
  project        = var.project_id
  region         = var.region
  repository     = google_dataform_repository.sentinel_repo.name
  name           = "daily-orders-job"
  
  # References the compiled production release
  release_config = "projects/${var.project_id}/locations/${var.region}/repositories/${google_dataform_repository.sentinel_repo.name}/releaseConfigs/production"

  invocation_config {
    included_tags = ["daily"] # This triggers every table tagged with "daily"
    transitive_dependencies_included = true
  }

  cron_schedule = "0 8 * * *" # Runs at 8:00 AM Daily
}