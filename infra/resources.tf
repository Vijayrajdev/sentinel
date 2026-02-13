# 1. BigQuery Datasets (Raw, Audit, Refined)
resource "google_bigquery_dataset" "layers" {
  for_each   = toset(["raw_landing", "audit_logs", "refined_layer",])
  dataset_id = "sentinel_${each.key}"
  location   = "US"
  delete_contents_on_destroy = true 
}

# 2. Service Account for dbt
resource "google_service_account" "dbt_sa" {
  account_id   = "sentinel-dbt-sa"
  display_name = "Service Account for dbt Transformation"
}

# 3. Grant Permissions to dbt SA
resource "google_project_iam_member" "dbt_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.dbt_sa.email}"
}

# Output the SA email so you can use it later
output "dbt_service_account_email" {
  value = google_service_account.dbt_sa.email
}