terraform {
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.45"
    }
  }
}

provider "hcloud" {
  token = var.hcloud_token
}

resource "hcloud_ssh_key" "deploy" {
  name       = "backstage-deploy"
  public_key = var.ssh_public_key
}

resource "hcloud_server" "pipeline" {
  name        = "backstage-run-${formatdate("YYYYMMDD-hhmm", timestamp())}"
  server_type = var.server_type
  image       = "ubuntu-24.04"
  location    = "fsn1"
  ssh_keys    = [hcloud_ssh_key.deploy.id]

  user_data = templatefile("${path.module}/cloud-init.yml.tpl", {
    hcloud_token    = var.hcloud_token
    s3_endpoint     = var.s3_endpoint
    s3_access_key   = var.s3_access_key
    s3_secret_key   = var.s3_secret_key
    s3_bucket       = var.s3_bucket
    github_token               = var.github_token
    dataverse_token            = var.dataverse_token
    dataverse_url              = var.dataverse_url
    dataset_persistent_id_eu   = var.dataset_persistent_id_eu
    cases                      = var.cases
    pipeline_args              = var.pipeline_args
  })

  labels = {
    purpose = "backstage-pipeline"
    managed = "terraform"
  }
}
