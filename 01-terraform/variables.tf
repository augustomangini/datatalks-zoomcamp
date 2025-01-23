variable "project" {
  description = "projeto"
  default = "terraform-project-448518"
}

variable "region_SA" {
  description = "Local Southamerica"
  default = "southamerica-east1"
}

variable "location_US" {
  description = "Local USA"
  default = "US"
}

variable "bq_dataset_name" {
    description = "Meu set do bigquery"
    default = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Meu bucket no storage"
  default = "terraform-project-448518-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"
}