variable "project_id" {
  description = "The GCP Project ID"
  type        = string
  default     = "sentinel-486707"
}

variable "region" {
  description = "Default GCP Region"
  type        = string
  default     = "us-central1"
}

variable "function_name" {
  description = "Name of the Cloud Function"
  type        = string
  default     = "sentinel-ingestor"
}

variable "function_entry_point" {
  description = "The Python function to call (must match main.py)"
  type        = string
  default     = "process_file"
}

# --- Buckets ---
variable "landing_bucket_name" {
  description = "Name of the existing Landing Zone bucket"
  type        = string
}

variable "archive_bucket_name" {
  description = "Name of the existing Archive Zone bucket"
  type        = string
}

variable "code_bucket_name" {
  description = "Name of the existing Code bucket"
  type        = string
}