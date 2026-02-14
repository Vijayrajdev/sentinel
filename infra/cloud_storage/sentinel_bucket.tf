# --------------------------------------------------------
# 1. LANDING ZONE (Where files arrive)
# --------------------------------------------------------
resource "google_storage_bucket" "landing_zone" {
  name          = var.landing_bucket_name
  location      = var.region
  force_destroy = true
}

# --------------------------------------------------------
# 2. ARCHIVE ZONE (Where files go after processing)
# --------------------------------------------------------
resource "google_storage_bucket" "archive_zone" {
  name          = var.archive_bucket_name
  location      = var.region
  force_destroy = true
}

# --------------------------------------------------------
# 3. CODE BUCKET (Where Function code lives)
# --------------------------------------------------------
resource "google_storage_bucket" "code_bucket" {
  name          = var.code_bucket_name
  location      = var.region
  force_destroy = true
}
