terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.17.0"
    }
  }
}

provider "google" {
  # Configuration options
  project = var.project
  region  = var.region_SA
}


resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.region_SA
  force_destroy = true


  # Optional, but recommended settings:
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true


  versioning {
    enabled = true
  }


  lifecycle_rule {
    condition {
      age = 30
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



resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id                 = var.bq_dataset_name
  delete_contents_on_destroy = true
  location                   = var.location_US
}