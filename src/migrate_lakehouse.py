"""
Medallion Architecture Migration Script.

This module upgrades a flat S3 bucket structure into a Hive-partitioned 
Lakehouse architecture (Bronze/Silver). It migrates raw JSON payloads 
to the Bronze layer and converts processed CSV files into highly compressed, 
columnar Parquet files for the Silver layer.

Functions:
    migrate_bronze: Copies raw JSONs into year/month partitioned folders.
    migrate_silver: Reads CSVs, converts to Parquet, and writes to partitioned folders.
    main: Orchestrates the migration process safely.
"""

import os
import io
import re
import pandas as pd
import boto3
from dotenv import load_dotenv
from loguru import logger

# Load environment variables for AWS authentication
load_dotenv()

def get_s3_client():
    """Initializes and returns an authenticated S3 client."""
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    region = os.getenv("AWS_DEFAULT_REGION", "eu-north-1").strip()
    
    return boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )

def migrate_bronze(s3_client, bucket: str):
    """
    Migrates raw JSON files to the Bronze Hive-partitioned layer.
    Source: raw/forex_js/snippet_YYYY_MM.json
    Target: bronze/forex_factory/year=YYYY/month=MM/snippet.json
    """
    logger.info("Starting Bronze Layer Migration...")
    prefix = "raw/forex_js/"
    
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        logger.warning("No raw files found to migrate.")
        return

    for obj in response['Contents']:
        old_key = obj['Key']
        if not old_key.endswith('.json'):
            continue
            
        # Extract YYYY and MM using regex
        match = re.search(r'snippet_(\d{4})_(\d{2})\.json', old_key)
        if match:
            year, month = match.groups()
            new_key = f"bronze/forex_factory/year={year}/month={month}/snippet.json"
            
            # Copy object to new location
            copy_source = {'Bucket': bucket, 'Key': old_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=new_key)
            logger.success(f"Migrated Bronze: {year}-{month}")

def migrate_silver(s3_client, bucket: str):
    """
    Migrates CSV files to the Silver layer, converting them to Parquet format.
    Source: processed/forex_csv/forex_data_YYYY_MM.csv
    Target: silver/forex_calendar/year=YYYY/month=MM/data.parquet
    """
    logger.info("Starting Silver Layer Migration (CSV to Parquet)...")
    prefix = "processed/forex_csv/"
    
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        logger.warning("No processed CSV files found to migrate.")
        return

    for obj in response['Contents']:
        old_key = obj['Key']
        if not old_key.endswith('.csv'):
            continue
            
        match = re.search(r'forex_data_(\d{4})_(\d{2})\.csv', old_key)
        if match:
            year, month = match.groups()
            new_key = f"silver/forex_calendar/year={year}/month={month}/data.parquet"
            
            try:
                # 1. Download CSV to memory
                csv_obj = s3_client.get_object(Bucket=bucket, Key=old_key)
                df = pd.read_csv(io.BytesIO(csv_obj['Body'].read()))
                
                # 2. Convert to Parquet in memory
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
                
                # 3. Upload Parquet to new Silver partition
                s3_client.put_object(
                    Bucket=bucket, 
                    Key=new_key, 
                    Body=parquet_buffer.getvalue()
                )
                logger.success(f"Converted & Migrated Silver Parquet: {year}-{month}")
            except Exception as e:
                logger.error(f"Failed to process {old_key}: {e}")

def main():
    bucket_name = "forex-datalake-bucket"
    s3_client = get_s3_client()
    
    logger.info(f"Initiating Lakehouse Migration for bucket: {bucket_name}")
    
    # We do not delete the old files automatically (safety first).
    # Once verified in the AWS Console, the old folders can be manually deleted.
    migrate_bronze(s3_client, bucket_name)
    migrate_silver(s3_client, bucket_name)
    
    logger.info("Migration Complete. Please verify partitions in AWS S3 Console.")

if __name__ == "__main__":
    main()