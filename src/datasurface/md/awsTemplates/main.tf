terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "AWS_VPC_ID" {
  description = "VPC to use"
  type = string
  default = ""
}

data "aws_secretsmanager_secret_version" "creds" {
  secret_id = "my-secret-id"
}

# Use a VPC
data "aws_vpc" "dms_vpc" {
  # DataPlatform Parameter
  id = var.AWS_VPC_ID
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
  secret_id = "my-secret-id"
}

data "aws_rds_cluster" "source_aurora" {
  # Source database parameter
  cluster_identifier = "my-cluster-identifier"
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
  # Staging parameter
  bucket = "mybucket"
  acl    = "private"

  tags = {
    Name = "My bucket"
    Environment = "Dev"
  }
}

# Target S3 bucket
resource "aws_dms_endpoint" "target" {
  endpoint_id   = "s3-target-endpoint"
  endpoint_type = "target"
  engine_name   = "s3"
  bucket_name   = aws_s3_bucket.staging_bucket.bucket
  service_access_role = aws_iam_role.dms_access_for_staging_bucket.arn
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
  bucket = "ingested_data"
  acl    = "private"

  tags = {
    Name = "My bucket"
    Environment = "Dev"
  }
}

module "source_aurora_x_ingest_table" {
  source        = "./module/glue_table"
  table_name    = "source_aurora_x"
  database_name = "ingestion_db"
  bucket_name   = "${aws_s3_bucket.ingested_data_bucket.bucket}/x"

  schema = ["firstName:string", "surname:string"]

  columns = [for c in schema : {
    name = split(":", c)[0]
    type = split(":", c)[1]
  }]
}

module "source_aurora_y_ingest_table" {
  source        = "./module/glue_table"
  table_name    = "source_aurora_y"
  database_name = "ingestion_db"
  bucket_name   = "${aws_s3_bucket.ingested_data_bucket.bucket}/y"

  schema = ["firstName:string", "surname:string"]

  columns = [for c in schema : {
    name = split(":", c)[0]
    type = split(":", c)[1]
  }]
}

