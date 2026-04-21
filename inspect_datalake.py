"""
S3 Data Lake Inspector Utility.

This script scans the target S3 bucket and prints a tree-like 
directory structure of all stored objects, allowing the engineer 
to visually verify the Medallion Architecture (Bronze/Silver/Gold) 
and Hive partitioning strategy before downstream processing.
"""

import os
import boto3
from dotenv import load_dotenv

# Load AWS Credentials
load_dotenv()

def print_s3_tree(bucket_name: str):
    """Fetches and formats S3 object keys into a readable hierarchy."""
    print(f"\n🔍 Inspecting Data Lake: s3://{bucket_name}/\n")
    
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    region = os.getenv("AWS_DEFAULT_REGION", "eu-north-1").strip()

    s3_client = boto3.client(
        's3', 
        aws_access_key_id=access_key, 
        aws_secret_access_key=secret_key, 
        region_name=region
    )

    try:
        # We use a paginator in case you have thousands of files
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)

        file_count = 0
        
        for page in pages:
            if 'Contents' not in page:
                print("  [Bucket is Empty]")
                return
                
            for obj in page['Contents']:
                file_count += 1
                key = obj['Key']
                size_kb = obj['Size'] / 1024
                
                # Split the S3 key to create a visual indentation
                parts = key.split('/')
                depth = len(parts) - 1
                indent = "    " * depth
                filename = parts[-1]
                
                if filename: # Ignore empty folder marker objects
                    print(f"├── {indent}{filename} ({size_kb:.2f} KB)")
                else:
                    folder_name = parts[-2] + "/"
                    print(f"├── {indent}{folder_name}")
                    
        print(f"\n✅ Total Files Found: {file_count}\n")

    except Exception as e:
        print(f"❌ Error accessing S3 Bucket: {e}")

if __name__ == "__main__":
    # Ensure this matches the bucket name in your pipeline_config.yaml
    TARGET_BUCKET = "forex-datalake-bucket" 
    print_s3_tree(TARGET_BUCKET)