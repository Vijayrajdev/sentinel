# BigQuery Table (Raw, Audit, Refined)
locals {
  tables_audit = {
    "ingestion_master" = {
      partitioning = null
    }
    "ingestion_log" = {
      partitioning = null
    }
    "ai_ops_log" = {
      partitioning = null
    }
  }
  tables_raw = {
    "orders_raw" = {
      partitioning = null
    }
    "nyc_taxi_trips_raw" = {
      partitioning = null
    }
    "customer_raw" = {
      partitioning = {
        type  = "DAY"
        field = "batch_date"
      }
    }
  }
}

resource "google_bigquery_table" "sentinel_audit_tables" {
  for_each = local.tables_audit

  dataset_id          = "sentinel_audit"
  table_id            = each.key
  friendly_name       = "sentinel_${each.key}"
  deletion_protection = false

  # 1. The Dynamic Block
  # This creates the 'time_partitioning' block ONLY if partitioning is not null
  dynamic "time_partitioning" {
    for_each = each.value.partitioning != null ? { main = each.value.partitioning } : {}

    content {
      type  = time_partitioning.value.type
      field = try(time_partitioning.value.field, null)
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
  # This creates the 'time_partitioning' block ONLY if partitioning is not null
  dynamic "time_partitioning" {
    for_each = each.value.partitioning != null ? { main = each.value.partitioning } : {}

    content {
      type  = time_partitioning.value.type
      field = try(time_partitioning.value.field, null)
    }
  }

  labels = {
    env = "dev"
  }

  # Note: Since we changed locals to objects, we still refer to each.key for the filename
  schema = file("${path.module}/json/${each.key}.json")
}