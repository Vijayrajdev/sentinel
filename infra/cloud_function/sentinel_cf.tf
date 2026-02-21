# ==============================================================================
# 1. PREPARE THE CODE
# ==============================================================================
# Zip the 'ingestion_function' folder into a single file
data "archive_file" "ingestion_function_zip" {
  type        = "zip"
  source_dir  = "${path.module}/ingestion_function"
  output_path = "${path.module}/../tmp/ingestion_function.zip"
}

# Zip the 'sentinel_forge_function' folder into a single file
data "archive_file" "sentinel_forge_function_zip" {
  type        = "zip"
  source_dir  = "${path.module}/sentinel_forge_function"
  output_path = "${path.module}/../tmp/sentinel_forge_function.zip"
}

# Upload the ingestion Zip file to your Code Bucket
resource "google_storage_bucket_object" "ingestion_source_code" {
  # Use MD5 in filename to force redeployment when code changes
  name   = "ingestion_function/src-${data.archive_file.ingestion_function_zip.output_md5}.zip"
  bucket = var.code_bucket_name
  source = data.archive_file.ingestion_function_zip.output_path
}

# Upload the data engineer Zip file to your Code Bucket
resource "google_storage_bucket_object" "sentinel_forge_source_code" {
  # Use MD5 in filename to force redeployment when code changes
  name   = "sentinel_forge_function/src-${data.archive_file.sentinel_forge_function_zip.output_md5}.zip"
  bucket = var.code_bucket_name
  source = data.archive_file.sentinel_forge_function_zip.output_path
}

# ==============================================================================
# 2. DEPLOY CLOUD FUNCTION (GEN 2)
# ==============================================================================
# Sentinel Ingestor
resource "google_cloudfunctions2_function" "sentinel_ingestor" {
  name        = var.ingestion_function_name
  location    = var.region
  description = "Sentinel Ingestion Framework - Event Driven ETL"

  # ----------------------------------------------------------------------------
  # A. BUILD CONFIGURATION (How to run the code)
  # ----------------------------------------------------------------------------
  build_config {
    runtime     = "python310"
    entry_point = var.ingestion_function_entry_point
    
    source {
      storage_source {
        bucket = var.code_bucket_name
        object = google_storage_bucket_object.ingestion_source_code.name
      }
    }
  }

  # ----------------------------------------------------------------------------
  # B. SERVICE CONFIGURATION (Runtime settings)
  # ----------------------------------------------------------------------------
  service_config {
    max_instance_count = 2
    min_instance_count = 0
    available_memory   = "1024Mi"
    timeout_seconds    = 540

    
    # Environment Variables accessed by os.environ.get() in Python
    environment_variables = {
      GCP_PROJECT        = var.project_id
      METADATA_DATASET   = "sentinel_audit"
      MASTER_TABLE       = "ingestion_master"
      LOGS_TABLE         = "ingestion_log"
      ARCHIVE_BUCKET     = var.ingestion_archive_bucket_name
      PUBSUB_TOPIC_DRIFT = var.pubsub_topic_id
    }

    # The Service Account that gives the function permission to use BigQuery/Storage
    # If you haven't created a custom one, use the default (though custom is safer)
    service_account_email = "sentinel-deployer@${var.project_id}.iam.gserviceaccount.com"
  }

  # ----------------------------------------------------------------------------
  # C. TRIGGER CONFIGURATION (Eventarc)
  # ----------------------------------------------------------------------------
  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.storage.object.v1.finalized" # Triggers on File Upload
    retry_policy   = "RETRY_POLICY_RETRY" # Retry if function crashes
    
    event_filters {
      attribute = "bucket"
      value     = var.ingestion_landing_bucket_name
    }
  }
}

# ==============================================================================
# 3. IAM PERMISSIONS (Optional but Recommended)
# ==============================================================================
# The Cloud Function needs permission to see the bucket
resource "google_cloud_run_service_iam_member" "invoker" {
  location = google_cloudfunctions2_function.sentinel_ingestor.location
  service  = google_cloudfunctions2_function.sentinel_ingestor.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:sentinel-deployer@${var.project_id}.iam.gserviceaccount.com"
}

# Sentinel Data Engineer
resource "google_cloudfunctions2_function" "sentinel_forge" {
  name        = var.sentinel_forge_function_name
  location    = var.region
  description = "AI Agent that fixes schema drift via GitHub PRs"

  # ----------------------------------------------------------------------------
  # A. BUILD CONFIGURATION (How to run the code)
  # ----------------------------------------------------------------------------
  build_config {
    runtime     = "python310"
    entry_point = var.sentinel_forge_function_entry_point
    
    source {
      storage_source {
        bucket = var.code_bucket_name
        object = google_storage_bucket_object.sentinel_forge_source_code.name
      }
    }
  }

  # ----------------------------------------------------------------------------
  # B. SERVICE CONFIGURATION (Runtime settings)
  # ----------------------------------------------------------------------------
  service_config {
    max_instance_count = 2
    min_instance_count = 0
    available_memory   = "512M"
    timeout_seconds    = 540

    # Secret: Github Personal Access Token
    secret_environment_variables {
      key        = "GITHUB_TOKEN" 
      project_id = var.project_id
      secret     = var.secret_id
      version    = "latest"
    }
    
    # Environment Variables accessed by os.environ.get() in Python
    environment_variables = {
      GCP_PROJECT        = var.project_id
      GCP_REGION         = var.region
      REPO_NAME          = var.repo_name
      AI_AUDIT_TABLE     = "sentinel_audit.ai_ops_log"
      SCHEMA_BASE_PATH   = var.schema_base_path
      TF_BASE_PATH       = var.tf_base_path
    }

    # The Service Account that gives the function permission to use BigQuery/Storage
    # If you haven't created a custom one, use the default (though custom is safer)
    service_account_email = "sentinel-deployer@${var.project_id}.iam.gserviceaccount.com"
  }

  # ----------------------------------------------------------------------------
  # C. TRIGGER CONFIGURATION (Pub/Sub Topic)
  # ----------------------------------------------------------------------------

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = var.pubsub_topic_id
    retry_policy   = "RETRY_POLICY_DO_NOT_RETRY" # Don't retry AI calls endlessly on error
  }
}
