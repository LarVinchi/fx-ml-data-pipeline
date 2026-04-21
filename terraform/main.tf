provider "aws" {
  region = "eu-north-1" 
}

# 1. Database Catalog
resource "aws_glue_catalog_database" "forex_lakehouse" {
  name = "forex_lakehouse_db"
}

# 2. Athena Workgroup for analysis
resource "aws_athena_workgroup" "forex_analysis" {
  name = "forex_analysis_workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.bucket_name}/athena_query_results/"
    }
  }
}

variable "bucket_name" {
  default = "forex-datalake-bucket"
}