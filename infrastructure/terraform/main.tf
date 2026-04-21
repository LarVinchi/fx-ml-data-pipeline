provider "aws" {
  region = "eu-north-1" 
}

variable "bucket_name" {
  type    = string
  default = "forex-datalake-bucket"
}

# 1. S3 Data Lake Bucket
resource "aws_s3_bucket" "datalake" {
  bucket = var.bucket_name
}

# 2. Database Catalog
resource "aws_glue_catalog_database" "forex_lakehouse" {
  name = "forex_lakehouse_db"
  description = "Stores metadata for the Medallion architecture (Bronze, Silver, Gold)"
}

# 3. Athena Workgroup for analysis
resource "aws_athena_workgroup" "forex_analysis" {
  name = "forex_analysis_workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.datalake.bucket}/athena_query_results/"
    }
  }
}

# --- IAM Role for AWS Glue Crawler ---
resource "aws_iam_role" "glue_crawler_role" {
  name = "forex_glue_crawler_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

# Attach standard Glue privileges
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Give Glue access to your Datalake (Using FullAccess for Capstone simplicity)
resource "aws_iam_role_policy_attachment" "s3_access" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess" 
}

# --- AWS Glue Crawler ---
# This automatically discovers your Parquet partitions and creates Athena tables
resource "aws_glue_crawler" "silver_crawler" {
  database_name = aws_glue_catalog_database.forex_lakehouse.name
  name          = "forex_silver_crawler"
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.datalake.bucket}/silver/"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}