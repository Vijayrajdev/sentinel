variable "airflow_sa_email" {
  description = "The Service Account email created in the GCP Console for Composer"
  type        = string
}

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

variable "composer_bucket_name" {
  description = "Name of the existing Composer bucket"
  type        = string
}
