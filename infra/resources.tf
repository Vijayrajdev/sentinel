# ==========================================
# MODULE 1: Create Datasets
# ==========================================
module "bq_datasets" {
  source     = "./bigquery/datasets"
}

# ==========================================
# MODULE 2: Create Tables
# ==========================================
module "bq_tables" {
  source     = "./bigquery/tables"
}

# ==========================================
# MODULE 3: Create Buckets
# ==========================================
module "buckets" {
  source     = "./cloud_storage"

  # Pass variables from Root -> Module
  region                  = var.region
  code_bucket_name        = var.code_bucket_name
  archive_bucket_name     = var.archive_bucket_name
  landing_bucket_name     = var.landing_bucket_name
}

# ==========================================
# MODULE 4: Create Cloud Function
# ==========================================
module "cloud_function" {
  source     = "./cloud_function"

  # Pass variables from Root -> Module
  project_id              = var.project_id
  region                  = var.region
  code_bucket_name        = var.code_bucket_name
  archive_bucket_name     = var.archive_bucket_name
  landing_bucket_name     = var.landing_bucket_name
  function_name           = var.function_name
  function_entry_point    = var.function_entry_point
}