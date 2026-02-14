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

variable "ingestion_function_name" {
  description = "Name of the Cloud Function"
  type        = string
  default     = "sentinel-ingestor"
}

variable "data_engineer_function_name" {
  description = "Name of the Cloud Function"
  type        = string
  default     = "sentinel-data-engineer"
}

variable "ingestion_function_entry_point" {
  description = "The Python function to call (must match main.py)"
  type        = string
  default     = "process_file"
}

variable "data_engineer_function_entry_point" {
  description = "The Python function to call (must match main.py)"
  type        = string
  default     = "ai_agent_main"
}

# --- Buckets ---
variable "ingestion_landing_bucket_name" {
  description = "Name of the existing Landing Zone bucket"
  type        = string
}

variable "ingestion_archive_bucket_name" {
  description = "Name of the existing Archive Zone bucket"
  type        = string
}

variable "code_bucket_name" {
  description = "Name of the existing Code bucket"
  type        = string
}

variable "github_token" {
  description = "Name of the existing Code bucket"
  type        = string
}

variable "repo_name" {
  description = "Name of the existing Github Repo"
  type        = string
}

variable "pubsub_topic" {
  description = "Name of the PUB/SUB Topic"
  type        = string
}