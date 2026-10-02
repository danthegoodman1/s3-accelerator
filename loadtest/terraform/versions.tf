terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  # The staging account. Terraform refuses credentials for any other,
  # whichever profile or keys the environment names.
  allowed_account_ids = ["399785866736"]
  region              = var.region
  default_tags {
    tags = {
      Project = var.name
    }
  }
}
