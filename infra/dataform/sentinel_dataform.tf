# infra/dataform/sentinel_dataform.tf

# The Dataform Repository
resource "google_dataform_repository" "sentinel_repo" {
  provider = google-beta
  name     = "sentinel-forge-pipeline"
  region   = var.region
  project  = var.project_id
  
  # The account that "deploys" or manages the repo settings
  service_account = "sentinel-deployer@${var.project_id}.iam.gserviceaccount.com"

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
  release_config = "projects/${var.project_id}/locations/${var.region}/repositories/${google_dataform_repository.sentinel_repo.name}/releaseConfigs/production"

  invocation_config {
    included_tags = ["daily"]
    transitive_dependencies_included = true
    
    # This account creates the actual BigQuery tables
    service_account = "sentinel-dbt-worker@${var.project_id}.iam.gserviceaccount.com"
  }

  cron_schedule = "0 8 * * *"
}