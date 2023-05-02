locals {
  data_lake_bucket = "de_zoomcamp_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "hip-plexus-374912"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  type = string
  default = "europe-west6"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "bazaraki"
}

variable "credentials" {
	description = "ServiceAccount Credentials filepath for GCP"
	default = "../gcp-credentials/gcp-de-zoomcamp-sa.json"
}