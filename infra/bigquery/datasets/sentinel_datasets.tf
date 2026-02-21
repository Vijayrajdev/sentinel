# BigQuery Datasets (Raw, Audit, Refined)
locals {
  datasets = {
    "raw"     = "raw"
    "audit"   = "audit"
  }
  refined_datasets = {
    "staging" = "staging"
    "marts"   = "marts"
  }
}

resource "google_bigquery_dataset" "sentinel_datasets" {
  for_each = local.datasets

  dataset_id                 = "sentinel_${each.key}"
  friendly_name              = "sentinel_${each.key}"
  description                = "This is a dataset for ${each.value} tables"
  location                   = "US"
  delete_contents_on_destroy = true

  labels = {
    env = "dev"
  }
}

resource "google_bigquery_dataset" "sentinel_refined_datasets" {
  for_each = local.refined_datasets

  dataset_id                 = "sentinel_${each.key}"
  friendly_name              = "sentinel_${each.key}"
  description                = "This is a dataset for ${each.value} tables"
  location                   = "US"
  delete_contents_on_destroy = true

  labels = {
    env          = "dev"
    layer        = "refined"
    orchestrator = "airflow-dbt"
  }
}
