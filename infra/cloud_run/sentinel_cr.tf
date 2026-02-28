# ==============================================================================
# RESOURCES: SENTINEL-INSIGHT (BI & ANALYTICS AGENT)
# ==============================================================================

# 1. Artifact Registry for the Docker Image
resource "google_artifact_registry_repository" "insight_repo" {
  location      = var.region
  repository_id = "sentinel-insight-repo"
  description   = "Docker repository for Sentinel Insight UI"
  format        = "DOCKER"
}

# 2. Auto-Zip the Application Code
data "archive_file" "insight_app_zip" {
  type        = "zip"

  source_dir  = "${path.module}/sentinel_insights_function"
  output_path = "${path.module}/../tmp/sentinel_insights_function.zip"
}

# 3. Upload the Zip to your existing GCS Code Bucket
resource "google_storage_bucket_object" "insight_app_source" {
  name   = "insight_app_${data.archive_file.insight_app_zip.output_md5}.zip"
  bucket = var.code_bucket_name
  source = data.archive_file.insight_app_zip.output_path
}

# 4. Cloud Build Trigger (Runs automatically to build Docker image from the Zip)
resource "null_resource" "build_insight_image" {
  triggers = {
    source_hash = data.archive_file.insight_app_zip.output_md5
  }

  provisioner "local-exec" {
    # We route the logs to your existing bucket to bypass the default Google bucket IAM/VPC blocks
    command = "gcloud builds submit gs://${var.code_bucket_name}/${google_storage_bucket_object.insight_app_source.name} --tag ${var.region}-docker.pkg.dev/${var.project_id}/sentinel-insight-repo/insight-app:${data.archive_file.insight_app_zip.output_md5} --project ${var.project_id} --gcs-log-dir=gs://${var.code_bucket_name}/build-logs"
  }
  
  depends_on = [google_artifact_registry_repository.insight_repo]
}

# 5. Cloud Run Service (FastAPI + WebSockets)
resource "google_cloud_run_v2_service" "sentinel_insight_ui" {
  name     = "sentinel-insight-ui"
  location = var.region

  template {
    service_account = "sentinel-deployer@${var.project_id}.iam.gserviceaccount.com"
    
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/sentinel-insight-repo/insight-app:${data.archive_file.insight_app_zip.output_md5}"
      
      # Standard Environment Variables
      env {
        name  = "GCP_PROJECT"
        value = var.project_id
      }
      env {
        name  = "GCP_REGION"
        value = "global"
      }
      env {
        name  = "REPO_NAME"
        value = var.repo_name
      }
      env {
        name  = "AUDIT_TABLE"
        value = "${var.project_id}.sentinel_audit.${var.insights_audit}"
      }
      
      # Securely Mount the GitHub Token from Secret Manager
      env {
        name = "GITHUB_TOKEN"
        value_source {
          secret_key_ref {
            secret  = var.secret_id
            version = "latest"
          }
        }
      }
    }
  }

  depends_on = [null_resource.build_insight_image]
}

# 6. Allow public access to the UI (Or restrict via IAP)
resource "google_cloud_run_service_iam_member" "public_access" {
  location = google_cloud_run_v2_service.sentinel_insight_ui.location
  service  = google_cloud_run_v2_service.sentinel_insight_ui.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
