# The results bucket: the code package goes up here, and the recording and the live run come back.
# Kept apart from the instance so `terraform destroy` on the run cannot take the results with it.

terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.80" }
  }
}

variable "region" {
  type    = string
  default = "eu-north-1"
}

provider "aws" {
  region = var.region
  default_tags { tags = { project = "north-wind" } }
}

data "aws_caller_identity" "me" {}

resource "aws_s3_bucket" "results" {
  bucket = "north-wind-${data.aws_caller_identity.me.account_id}"
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket                  = aws_s3_bucket.results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "results" {
  bucket = aws_s3_bucket.results.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

output "bucket" {
  value = aws_s3_bucket.results.bucket
}
