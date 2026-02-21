# --------------------------------------------------------
# 1. CLOUD COMPOSER (sentinel_orchestrator)
# --------------------------------------------------------

resource "google_composer_environment" "sentinel_orchestrator" {
  name    = "sentinel-airflow-orchestrator"
  region  = var.region
  project = var.project_id

  storage_config {
    bucket = var.composer_bucket_name
  }

  config {
    software_config {
      image_version = "composer-2-airflow-2"
      
      env_variables = {
        SENTINEL_PROJECT_ID      = var.project_id
        DBT_PROFILES_DIR         = "/home/airflow/gcs/data/dbt/"
      }

      pypi_packages = {
        dbt-core     = "==1.7.18"
        dbt-bigquery = "==1.7.9"
      }
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = var.airflow_sa_email
    }
  }
}