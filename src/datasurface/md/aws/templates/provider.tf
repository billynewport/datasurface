# Configure the AWS Provider
provider "aws" {
  region = "us-east-1"
//  access_key = jsondecode(data.aws_secretsmanager_secret_version.creds.secret_string)["access_key"]
//  secret_key = jsondecode(data.aws_secretsmanager_secret_version.creds.secret_string)["secret_key"]

  # Role for Terraform to use to do everything here
//  assume_role {
//    role_arn     = "arn:aws:iam::123456789012:role/ROLE_NAME"
//    session_name = "SESSION_NAME"
//    external_id  = "EXTERNAL_ID"
//  }
}

