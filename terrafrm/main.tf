terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  # Configuration options
  credentials = "./keys/creds.json"
  project     = "de-camp-expert"
  region      = "us-central1"

}

resource "google_storage_bucket" "de-terraform-bucket" {
  name          = "de-camp-expert-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "bq_de_camp" { 
  dataset_id = var.bq_de_camp
  project    = "de-camp-expert"
}