# BigQuery Table (Raw, Audit, Refined)
locals {
  tables_audit = {
    "ingestion_master" = {
      partition_type = null 
    }
    "ingestion_log" = {
      partition_type = null  
    }
    "ai_ops_log" = {
      partition_type = null
    }
  }
  tables_raw = {
    "orders_raw" = {
      partition_type = null
    }
  }
}

resource "google_bigquery_table" "sentinel_audit_tables" {
  for_each = local.tables_audit

  dataset_id    = "sentinel_audit"
  table_id      = each.key
  friendly_name = "sentinel_${each.key}"

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
  schema = file("${path.module}/json/${each.key}.json")
}

resource "google_bigquery_table" "sentinel_raw_tables" {
  for_each = local.tables_raw

  dataset_id          = "sentinel_raw"
  table_id            = each.key
  friendly_name       = "sentinel_${each.key}"
  deletion_protection = false

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
  schema = file("${path.module}/json/${each.key}.json")
}