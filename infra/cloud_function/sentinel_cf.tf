# ==============================================================================
# 1. PREPARE THE CODE
# ==============================================================================
# Zip the 'ingestion_function' folder into a single file
data "archive_file" "function_zip" {
  type        = "zip"
  source_dir  = "${path.module}/ingestion_function"
  output_path = "${path.module}/../tmp/function.zip"
}

# Upload the Zip file to your Code Bucket
resource "google_storage_bucket_object" "source_code" {
  # Use MD5 in filename to force redeployment when code changes
  name   = "src-${data.archive_file.function_zip.output_md5}.zip"
  bucket = var.code_bucket_name
  source = data.archive_file.function_zip.output_path
}

# ==============================================================================
# 2. DEPLOY CLOUD FUNCTION (GEN 2)
# ==============================================================================
resource "google_cloudfunctions2_function" "sentinel_ingestor" {
  name        = var.function_name
  location    = var.region
  description = "Sentinel Ingestion Framework - Event Driven ETL"

  # ----------------------------------------------------------------------------
  # A. BUILD CONFIGURATION (How to run the code)
  # ----------------------------------------------------------------------------
  build_config {
    runtime     = "python310"
    entry_point = var.function_entry_point
    
    source {
      storage_source {
        bucket = var.code_bucket_name
        object = google_storage_bucket_object.source_code.name
      }
    }
  }

  # ----------------------------------------------------------------------------
  # B. SERVICE CONFIGURATION (Runtime settings)
  # ----------------------------------------------------------------------------
  service_config {
    max_instance_count = 2
    min_instance_count = 0
    available_memory   = "256M"
    timeout_seconds    = 540
    
    # Environment Variables accessed by os.environ.get() in Python
    environment_variables = {
      GCP_PROJECT      = var.project_id
      METADATA_DATASET = "sentinel_audit"
      MASTER_TABLE     = "ingestion_master"
      LOGS_TABLE       = "ingestion_log"
      ARCHIVE_BUCKET   = var.archive_bucket_name
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
      value     = var.landing_bucket_name
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