# --------------------------------------------------------
# 1. LANDING ZONE (Where files arrive)
# --------------------------------------------------------
resource "google_storage_bucket" "landing_zone" {
  name          = var.ingestion_landing_bucket_name
  location      = var.region
  force_destroy = true
}

# --------------------------------------------------------
# 2. ARCHIVE ZONE (Where files go after processing)
# --------------------------------------------------------
resource "google_storage_bucket" "archive_zone" {
  name          = var.ingestion_archive_bucket_name
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

# --------------------------------------------------------
# 3. COMPOSER BUCKET (Where DBT code lives)
# --------------------------------------------------------
resource "google_storage_bucket" "composer_storage" {
  # Bucket names must be globally unique, so we append the project ID
  name                        = var.composer_bucket_name
  location                    = var.region 
  storage_class               = "STANDARD" 
  uniform_bucket_level_access = true
  force_destroy               = false 

  labels = {
    env         = "dev"
    system      = "orchestration"
  }
}