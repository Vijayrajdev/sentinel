terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
  backend "gcs" {
    bucket  = "tf-state-sentinel"
    prefix  = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}