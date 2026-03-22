variable "secret_id" {
  description = "Sentinel github token secret id"
  type        = string
  sensitive   = true
}

variable "secret_id_bot" {
  description = "Sentinel-forge github token secret id"
  type        = string
  sensitive   = true
}
