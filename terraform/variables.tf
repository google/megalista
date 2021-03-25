locals {
  time_zone   = "America/Sao_Paulo"
}

variable "bucket_name" {
    type = string
    description = "Google Cloud Storage Bucket to create"
}

variable "bq_ops_dataset" {
    type = string
    description = "Auxliary bigquery dataset for Megalista operations to create"
}

variable "developer_token" {
    type = string
    description = "Google Ads developer Token"
}

variable "client_id" {
    type = string
    description = "OAuth Client Id"
}

variable "client_secret" {
    type = string
    description = "OAuth Client Secret"
}

variable "access_token" {
    type = string
    description = "Access Token"
}

variable "refresh_token" {
    type = string
    description = "Refresh Token"
}

variable "setup_sheet_id" {
    type = string
    description = "Setup Sheet Id"
}

variable "location" {
  type        = string
  description = "GCP location https://cloud.google.com/compute/docs/regions-zones?hl=pt-br default us"
  default     = "us"
}

variable "region" {
  type        = string
  description = "GCP region https://cloud.google.com/compute/docs/regions-zones?hl=pt-br default us-central1"
  default     = "us-central1"
}

variable "zone" {
  type        = string
  description = "GCP zone https://cloud.google.com/compute/docs/regions-zones?hl=pt-br default us-central1"
  default     = "us-central1-f"
}

