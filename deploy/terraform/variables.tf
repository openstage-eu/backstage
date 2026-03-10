variable "hcloud_token" {
  type      = string
  sensitive = true
}

variable "s3_endpoint" {
  type = string
}

variable "s3_access_key" {
  type      = string
  sensitive = true
}

variable "s3_secret_key" {
  type      = string
  sensitive = true
}

variable "s3_bucket" {
  type    = string
  default = "openstage"
}

variable "github_token" {
  type      = string
  sensitive = true
  default   = ""
}

variable "dataverse_token" {
  type      = string
  sensitive = true
  default   = ""
}

variable "dataverse_url" {
  type    = string
  default = "https://dataverse.harvard.edu"
}

variable "server_type" {
  type    = string
  default = "cx53"
}

variable "dataset_persistent_id_eu" {
  type      = string
  sensitive = true
  default   = ""
}

variable "ssh_public_key" {
  type    = string
  default = ""
}

variable "cases" {
  type    = string
  default = "eu"
}

variable "pipeline_args" {
  description = "Extra arguments passed to flows.run (e.g. --download-mode incremental)"
  type        = string
  default     = ""
}
