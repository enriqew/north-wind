# One machine for one live run: the same Compose stack as on a laptop, started, stopped and
# powered off by timers set at boot. `terraform destroy` removes all of it; the results are in the
# storage bucket by then.

terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.80" }
  }
}

provider "aws" {
  region = var.region
  default_tags { tags = { project = "north-wind" } }
}

data "aws_caller_identity" "me" {}

# Amazon Linux 2023 for ARM. The Flink, Kafka and Python images the stack uses are multi-arch.
data "aws_ssm_parameter" "ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# No inbound rule at all: access is through Session Manager, and the instance only calls out
# (BarentsWatch, the package registries, S3, SSM).
resource "aws_security_group" "live" {
  name        = "north-wind-live"
  description = "North Wind live run: outbound only"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_role" "live" {
  name = "north-wind-live"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.live.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Read the BarentsWatch credentials, read the package, write the results. Nothing else.
resource "aws_iam_role_policy" "live" {
  name = "north-wind-live"
  role = aws_iam_role.live.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.me.account_id}:parameter/north-wind/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::${var.bucket}"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "arn:aws:s3:::${var.bucket}/*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "live" {
  name = "north-wind-live"
  role = aws_iam_role.live.name
}

resource "aws_instance" "live" {
  ami                                  = data.aws_ssm_parameter.ami.value
  instance_type                        = var.instance_type
  subnet_id                            = sort(data.aws_subnets.default.ids)[0]
  vpc_security_group_ids               = [aws_security_group.live.id]
  iam_instance_profile                 = aws_iam_instance_profile.live.name
  associate_public_ip_address          = true
  instance_initiated_shutdown_behavior = "stop"

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.volume_gb
    encrypted   = true
  }

  user_data = templatefile("${path.module}/bootstrap.sh.tftpl", {
    region          = var.region
    bucket          = var.bucket
    name            = var.name
    window_start    = var.window_start
    window_end      = var.window_end
    start_at        = var.start_at
    stop_at         = var.stop_at
    poweroff_at     = var.poweroff_at
    compose_version = var.compose_version
  })
  user_data_replace_on_change = true

  tags = { Name = "north-wind-live-${var.name}" }
}

output "instance_id" {
  value = aws_instance.live.id
}
