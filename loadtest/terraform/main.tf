data "aws_availability_zones" "available" {
  state = "available"
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

data "aws_ec2_instance_type_offerings" "node" {
  location_type = "availability-zone"
  filter {
    name   = "instance-type"
    values = [var.node_type]
  }
}

data "aws_ec2_instance_type_offerings" "client" {
  location_type = "availability-zone"
  filter {
    name   = "instance-type"
    values = [var.client_type]
  }
}

data "aws_ami" "ubuntu" {
  count       = var.ami == "" ? 1 : 0
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-*-26.04-amd64-server-*"]
  }
  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

locals {
  zones = sort(setintersection(
    data.aws_availability_zones.available.names,
    data.aws_ec2_instance_type_offerings.node.locations,
    data.aws_ec2_instance_type_offerings.client.locations,
  ))
  zone = var.availability_zone != "" ? var.availability_zone : local.zones[0]
  ami  = var.ami != "" ? var.ami : data.aws_ami.ubuntu[0].id
}

resource "aws_vpc" "load" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = var.name }
}

resource "aws_subnet" "load" {
  vpc_id                  = aws_vpc.load.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = local.zone
  map_public_ip_on_launch = true
  tags                    = { Name = var.name }
}

resource "aws_internet_gateway" "load" {
  vpc_id = aws_vpc.load.id
}

resource "aws_route_table" "load" {
  vpc_id = aws_vpc.load.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.load.id
  }
}

resource "aws_route_table_association" "load" {
  subnet_id      = aws_subnet.load.id
  route_table_id = aws_route_table.load.id
}

# S3 traffic stays on AWS's network through a gateway endpoint, which costs
# nothing.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.load.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.load.id]
}

resource "aws_security_group" "load" {
  name   = var.name
  vpc_id = aws_vpc.load.id

  ingress {
    description = "SSH from the operator"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.operator_cidr]
  }

  ingress {
    description = "Everything between the load test hosts: gateways, nodes, gossip, metrics"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "load" {
  key_name   = var.name
  public_key = file(pathexpand(var.ssh_public_key))
}

resource "aws_placement_group" "load" {
  count    = var.cluster_placement ? 1 : 0
  name     = var.name
  strategy = "cluster"
}

resource "aws_instance" "node" {
  count                  = var.node_count + var.standby_count
  instance_type          = var.node_type
  ami                    = local.ami
  subnet_id              = aws_subnet.load.id
  vpc_security_group_ids = [aws_security_group.load.id]
  key_name               = aws_key_pair.load.key_name
  placement_group        = var.cluster_placement ? aws_placement_group.load[0].id : null

  root_block_device {
    volume_type = "gp3"
    volume_size = 64
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = { Name = "${var.name}-node-${count.index}" }
}

resource "aws_instance" "client" {
  count                  = var.client_count
  instance_type          = var.client_type
  ami                    = local.ami
  subnet_id              = aws_subnet.load.id
  vpc_security_group_ids = [aws_security_group.load.id]
  key_name               = aws_key_pair.load.key_name
  placement_group        = var.cluster_placement ? aws_placement_group.load[0].id : null

  root_block_device {
    volume_type = "gp3"
    volume_size = 64
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = { Name = "${var.name}-client-${count.index}" }
}

resource "random_id" "bucket" {
  byte_length = 4
}

# The dataset and every write the load test makes. Destroying the stack
# deletes the objects with it.
resource "aws_s3_bucket" "data" {
  bucket        = "${var.name}-${random_id.bucket.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Request metrics in CloudWatch: S3's own count of the requests and bytes
# the cache and the baselines cost it.
resource "aws_s3_bucket_metric" "data" {
  bucket = aws_s3_bucket.data.id
  name   = "EntireBucket"
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    id     = "abort-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# The origin's static key, which storage nodes sign S3 requests with, and
# which clients use for seeding and S3 baselines. It reaches only the
# load test's bucket.
resource "aws_iam_user" "origin" {
  name = "${var.name}-origin"
}

resource "aws_iam_user_policy" "origin" {
  name = "bucket"
  user = aws_iam_user.origin.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.data.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = "${aws_s3_bucket.data.arn}/*"
      },
    ]
  })
}

resource "aws_iam_access_key" "origin" {
  user = aws_iam_user.origin.name
}
