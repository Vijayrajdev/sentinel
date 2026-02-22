# BigQuery Datasets (Raw, Audit, Refined)
locals {
  # Consolidating all datasets into one clean map of objects
  bq_datasets = {
    "raw" = {
      id   = "sentinel_raw"
      desc = "Raw landing zone for incoming data (e.g., orders_raw)"
      layer = "raw"
    }
    "audit" = {
      id   = "sentinel_audit"
      desc = "Audit logs and metadata for Sentinel-Forge & Sentinel-Ingestor tracking"
      layer = "audit"
    }
    "staging" = {
      id   = "sentinel_staging"
      desc = "Silver layer: Cleaned, casted, and deduplicated views"
      layer = "refined"
    }
    "marts" = {
      id   = "sentinel_marts"
      desc = "Gold layer: Final business-ready fact and dimension tables"
      layer = "refined"
    }
    "assertions" = {
      id   = "sentinel_assertions"
      desc = "Data quality results for the Sentinel-Forge agent to monitor"
      layer = "quality_control"
    }
  }
}

resource "google_bigquery_dataset" "sentinel_infrastructure" {
  for_each = local.bq_datasets

  dataset_id                 = each.value.id
  friendly_name              = each.value.id
  description                = each.value.desc
  location                   = var.region
  project                    = var.project_id
  delete_contents_on_destroy = true

  labels = {
    env          = "dev"
    layer        = each.value.layer
    orchestrator = "dataform"
  }
}