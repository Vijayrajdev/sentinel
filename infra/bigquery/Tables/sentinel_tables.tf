# BigQuery Table (Raw, Audit, Refined)
locals {
  tables = {
    "ingestion_master" = {
      partition_type = null 
    }
    "ingestion_log" = {
      partition_type = null  
    }
  }
}

resource "google_bigquery_table" "sentinel_audit_tables" {
  for_each = local.tables

  dataset_id    = "sentinel_audit"
  table_id      = each.key
  friendly_name = "sentinel_${each.key}"
  location      = "US"

  # 1. The Dynamic Block
  # This creates the 'time_partitioning' block ONLY if partition_type is not null
  dynamic "time_partitioning" {
    for_each = each.value.partition_type != null ? [1] : []
    
    content {
      type = each.value.partition_type
    }
  }

  labels = {
    env = "dev"
  }

  # Note: Since we changed locals to objects, we still refer to each.key for the filename
  schema = file("${path_module}/json/${each.key}.json")
}