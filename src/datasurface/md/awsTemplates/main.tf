terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "aws_vpc_id" {
  description = "VPC to use"
  type = string
  default = ""
}

# Use a VPC
data "aws_vpc" "dms_vpc" {
  # DataPlatform Parameter
  id = "${var.aws_vpc_id}"
}

# Create a subnet in the VPC
resource "aws_subnet" "dms_subnet" {
  vpc_id     = data.aws_vpc.dms_vpc.id
  cidr_block = "10.0.1.0/24"
}

# Create a replication subnet group
resource "aws_dms_replication_subnet_group" "dms_repl_subnet_group" {
  replication_subnet_group_id   = "my-replication-subnet-group"
  replication_subnet_group_description = "My replication subnet group"
  subnet_ids = [aws_subnet.dms_subnet.id]
}

# IAM role for DMS to access resources
resource "aws_iam_role" "dms_access_for_staging_bucket" {
  name = "dms-access-for-endpoint"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "dms.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

# Get the secret from AWS Secrets Manager
data "aws_secretsmanager_secret_version" "source_aurora_credentials" {
  # Source database parameter
  secret_id = "rds!cluster-b3087781-457b-4ed4-9865-989c10a5600d"
}

data "aws_rds_cluster" "source_aurora" {
  # Source database parameter
  cluster_identifier = "arn:aws:rds:us-east-1:176502344450:cluster:nwdb-src"
}

# Get the DB subnet group of the Aurora cluster
data "aws_db_subnet_group" "source_aurora" {
  name = data.aws_rds_cluster.source_aurora.db_subnet_group_name
}

# Get the subnets of the DB subnet group
data "aws_subnet" "source_aurora" {
  for_each = toset(data.aws_db_subnet_group.source_aurora.subnet_ids)
  id       = each.value
}

# Source Aurora database
resource "aws_dms_endpoint" "source" {
  endpoint_id   = "aurora-source-endpoint"
  endpoint_type = "source"
  engine_name   = "aurora"
  username      = jsondecode(data.aws_secretsmanager_secret_version.source_aurora_credentials.secret_string)["username"]
  password      = jsondecode(data.aws_secretsmanager_secret_version.source_aurora_credentials.secret_string)["password"]
  # Source database parameters
  server_name   = data.aws_rds_cluster.source_aurora.endpoint
  port          = data.aws_rds_cluster.source_aurora.port
}

# Policy to allow DMS to access the S3 bucket
resource "aws_iam_role_policy" "dms_s3_access" {
  name = "dms-s3-access"
  role = aws_iam_role.dms_access_for_staging_bucket.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Effect": "Allow",
      "Resource": [
        "${aws_s3_bucket.staging_bucket.arn}",
        "${aws_s3_bucket.staging_bucket.arn}/*"
      ]
    }
  ]
}
EOF
}

# Staging bucket to hold AWS DMS output for all tables ingested
resource "aws_s3_bucket" "staging_bucket" {
  bucket = "my-staging-bucket"
}

resource "aws_s3_bucket_ownership_controls" "staging_bucket" {
  bucket = aws_s3_bucket.staging_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "staging_bucket" {
  depends_on = [aws_s3_bucket_ownership_controls.staging_bucket]

  # Staging parameter
  bucket = aws_s3_bucket.staging_bucket.id
  acl    = "private"
}

# Target S3 bucket
resource "aws_dms_endpoint" "target" {
  endpoint_id   = "s3-target-endpoint"
  endpoint_type = "target"
  engine_name   = "s3"
  service_access_role = aws_iam_role.dms_access_for_staging_bucket.arn
  s3_settings {
    bucket_name = aws_s3_bucket.staging_bucket.bucket
  }
}

# Create a security group for the DMS subnet
resource "aws_security_group" "dms_sg" {
  name   = "dms_sg"
  vpc_id = data.aws_vpc.dms_vpc.id
}

# Allow inbound traffic from the Aurora subnet
resource "aws_security_group_rule" "allow_aurora" {
  for_each          = data.aws_subnet.source_aurora
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"  # Allow all traffic
  cidr_blocks       = [each.value.cidr_block]
  security_group_id = aws_security_group.dms_sg.id
}

# DMS replication instance
resource "aws_dms_replication_instance" "dms_replication_instance" {
  replication_instance_id   = "test-dms-replication-instance-tf"
  replication_instance_class = "dms.r5.large"
  allocated_storage         = 20
  vpc_security_group_ids    = [aws_security_group.dms_sg.id]
  replication_subnet_group_id = aws_dms_replication_subnet_group.dms_repl_subnet_group.id
  publicly_accessible       = true
}

# DMS replication task

module "Ingest-source-aurora" {
  source                  = "./module/dms_ingest"
  replication_task_id     = "my-task"
  table_names             = ["table1", "table2"]
  source_endpoint_arn       = aws_dms_endpoint.source.endpoint_arn
  target_endpoint_arn       = aws_dms_endpoint.target.endpoint_arn
  replication_instance_arn  = aws_dms_replication_instance.dms_replication_instance.replication_instance_arn
}

# Staging bucket to hold AWS DMS output for all tables ingested
resource "aws_s3_bucket" "ingested_data_bucket" {
  # Staging parameter
  bucket = "ingested_data"
}

resource "aws_s3_bucket_ownership_controls" "ingested_data_bucket" {
  bucket = aws_s3_bucket.ingested_data_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "ingested_data_bucket" {
  depends_on = [aws_s3_bucket_ownership_controls.ingested_data_bucket]

  bucket = aws_s3_bucket.ingested_data_bucket.id
  acl    = "private"
}



locals {
  schema_aurora_table_x = ["firstName:string", "surname:string"]
  schema_aurora_table_y = ["firstName:string", "surname:string"]

  tables = {
    x = {
      name = "tableX",
      schema = ["firstName:string", "surname:string"],
      bucket = "x"
    },
    y = {
      name = "tableY",
      schema = ["firstName:string", "surname:string"],
      bucket = "y"
    }
  }
}

module "ingest_from_aurora_to_staging_bucket" {
  for_each = local.tables
  source        = "./module/glue_table"
  table_name    = each.value.name
  database_name = "ingestion_db"
  bucket_name   = "${aws_s3_bucket.ingested_data_bucket.bucket}/${each.value.bucket}"


  columns = [for c in each.value.schema : {
    name = split(":", c)[0]
    type = split(":", c)[1]
    comment = ""
  }]
}
