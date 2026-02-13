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