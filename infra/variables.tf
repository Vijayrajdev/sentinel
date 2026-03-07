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

variable "sentinel_forge_function_name" {
  description = "Name of the Cloud Function"
  type        = string
  default     = "sentinel-forge"
}

variable "ingestion_function_entry_point" {
  description = "The Python function to call (must match main.py)"
  type        = string
  default     = "process_file"
}

variable "sentinel_forge_function_entry_point" {
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

variable "repo_name" {
  description = "Name of the existing Github Repo"
  type        = string
}

variable "pubsub_topic" {
  description = "Name of the existing PUB/SUB Topic"
  type        = string
}

variable "pubsub_topic_id" {
  description = "Full ID of the Pub/Sub topic"
  type        = string
}

variable "schema_base_path" {
  description = "Terraform schema base path"
  type        = string
}

variable "tf_base_path" {
  description = "Terraform base path"
  type        = string
}

variable "secret_id" {
  description = "Sentinel github token secret id"
  type        = string
  sensitive   = true
}

variable "airflow_sa_email" {
  description = "The Service Account email created in the GCP Console for Composer"
  type        = string
}

variable "composer_bucket_name" {
  description = "Name of the existing Composer bucket"
  type        = string
}
