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
  region                            = var.region
  code_bucket_name                  = var.code_bucket_name
  ingestion_archive_bucket_name     = var.ingestion_archive_bucket_name
  ingestion_landing_bucket_name     = var.ingestion_landing_bucket_name
}

# ==========================================
# MODULE 4: Create Cloud Function
# ==========================================
module "cloud_function" {
  source     = "./cloud_function"

  # Pass variables from Root -> Module
  project_id                        = var.project_id
  region                            = var.region
  code_bucket_name                  = var.code_bucket_name
  ingestion_archive_bucket_name     = var.ingestion_archive_bucket_name
  ingestion_landing_bucket_name     = var.ingestion_landing_bucket_name
  ingestion_function_name           = var.ingestion_function_name
  ingestion_function_entry_point    = var.ingestion_function_entry_point
  repo_name                         = var.repo_name
  pubsub_topic                      = var.pubsub_topic
  pubsub_topic_id                   = var.pubsub_topic_id
  tf_base_path                      = var.tf_base_path
  schema_base_path                  = var.schema_base_path
  secret_id                         = var.secret_id
}

# ==========================================
# MODULE 5: Create Pub/Sub
# ==========================================
module "pub_sub" {
  source     = "./pub_sub"

  # Pass variables from Root -> Module
  pubsub_topic                      = var.pubsub_topic
}

# ==========================================
# MODULE 5: Create Secret Container
# ==========================================
module "secret" {
  source     = "./secret_manager"

  # Pass variables from Root -> Module
  secret_id                         = var.secret_id
}