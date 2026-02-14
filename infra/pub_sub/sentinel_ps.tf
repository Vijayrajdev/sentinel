# ==============================================================================
# 1. COMMUNICATION LINE (Pub/Sub)
# ==============================================================================
# The Ingestor shouts here when it finds a new column.
resource "google_pubsub_topic" "schema_drift_events" {
  name = "schema-drift-events"

  labels = {
    env       = "dev"
    component = "sentinel-ai-ops"
  }
}

# Optional: Add a subscription just for debugging/viewing messages manually
resource "google_pubsub_subscription" "schema_drift_debug_sub" {
  name  = "schema-drift-debug-sub"
  topic = google_pubsub_topic.schema_drift_events.name

  # Expire messages after 7 days if not acked
  message_retention_duration = "604800s" 
}