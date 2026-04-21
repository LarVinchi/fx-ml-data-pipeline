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