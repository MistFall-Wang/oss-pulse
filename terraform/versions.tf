terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project    = "oss-pulse"
      ManagedBy  = "terraform"
      Repository = "github.com/MistFall-Wang/oss-pulse"
    }
  }
}
