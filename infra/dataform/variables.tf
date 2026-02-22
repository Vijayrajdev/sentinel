variable "secret_id" {
  description = "Sentinel github token secret id"
  type        = string
  sensitive   = true
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