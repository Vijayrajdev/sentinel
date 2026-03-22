resource "google_secret_manager_secret" "github_token_secret" {
  secret_id = var.secret_id # The name in GCP Console
  
  replication {
    auto {} # Automatically replicates to Google-managed locations
  }
}

resource "google_secret_manager_secret" "github_pvt_key" {
  secret_id = var.secret_id_bot # The name in GCP Console
  
  replication {
    auto {} # Automatically replicates to Google-managed locations
  }
}