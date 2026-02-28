# BigQuery Table (Raw, Audit, Refined)
locals {
  tables_audit = {
    "ingestion_master" = {
      partition_type = null
      partition_field = null
    }
    "ingestion_log" = {
      partition_type = null
      partition_field = null
    }
    "ai_ops_log" = {
      partition_type = null
      partition_field = null
    }
  }

  tables_raw = {
    "orders_raw" = {
      partition_type = null
      partition_field = null
    }
    "nyc_taxi_trips_raw" = {
      partition_type = null
      partition_field = null
    }
    "customer_raw" = {
      partition_type = "DAY"
      partition_field = "batch_date"
    }
    "order_items_raw" = {
      partition_type = "DAY"
      partition_field = "batch_date"
    }
  }

  tables_raw_hist = {
    "orders_raw" = {
      partition_type = null
      partition_field = null
      expiration_ms = 2592000000
    }
    "nyc_taxi_trips_raw" = {
      partition_type = null
      partition_field = null
      expiration_ms = 2592000000
    }
    "customer_raw" = {
      partition_type = "DAY"
      partition_field = "batch_date"
      expiration_ms = 2592000000
    }
    "order_items_raw" = {
      partition_type = "DAY"
      partition_field = "batch_date"
      expiration_ms = 2592000000
    }
  }
}

resource "google_bigquery_table" "sentinel_audit_tables" {
  for_each = local.tables_audit
  dataset_id = "sentinel_audit"
  table_id = each.key
  friendly_name = "sentinel_${each.key}"
  deletion_protection = false

  dynamic "time_partitioning" {
    for_each = each.value.partition_type != null ? [1] : []
    content {
      type = each.value.partition_type
      field = lookup(each.value, "partition_field", null)
      expiration_ms = lookup(each.value, "expiration_ms", null)
    }
  }

  labels = {
    env = "dev"
  }

  schema = file("${path.module}/json/${each.key}.json")
}

resource "google_bigquery_table" "sentinel_raw_tables" {
  for_each = local.tables_raw
  dataset_id = "sentinel_raw"
  table_id = each.key
  friendly_name = "sentinel_${each.key}"
  deletion_protection = false

  dynamic "time_partitioning" {
    for_each = each.value.partition_type != null ? [1] : []
    content {
      type = each.value.partition_type
      field = lookup(each.value, "partition_field", null)
      expiration_ms = lookup(each.value, "expiration_ms", null)
    }
  }

  labels = {
    env = "dev"
  }

  schema = file("${path.module}/json/${each.key}.json")
}

resource "google_bigquery_table" "sentinel_raw_hist_tables" {
  for_each = local.tables_raw_hist
  dataset_id = "sentinel_raw"
  table_id = "${each.key}_hist"
  friendly_name = "sentinel_${each.key}_hist"
  deletion_protection = false

  dynamic "time_partitioning" {
    for_each = each.value.partition_type != null ? [1] : []
    content {
      type = each.value.partition_type
      field = lookup(each.value, "partition_field", null)
      expiration_ms = lookup(each.value, "expiration_ms", null)
    }
  }

  labels = {
    env = "dev"
  }

  schema = file("${path.module}/json/${each.key}.json")
}