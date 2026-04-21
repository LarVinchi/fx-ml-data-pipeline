"""
Medallion Architecture Migration Script.

This module upgrades a flat S3 bucket structure into a Hive-partitioned 
Lakehouse architecture. It handles both Forex Factory (News) and 
Dukascopy (Price Action) datasets.

Layers:
    - Bronze: Raw JSON/CSV (Immutable, Hive-partitioned)
    - Silver: Cleaned Parquet (Structured, Columnar, Hive-partitioned)
"""

import os
import io
import re
import pandas as pd
import boto3
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

def get_s3_client():
    """Initializes and returns an authenticated S3 client."""
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "").strip(),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "").strip(),
        region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1").strip()
    )

def migrate_forex_factory(s3_client, bucket: str):
    """
    Migrates News data from raw/processed to bronze/silver.
    """
    logger.info("Migrating Forex Factory (News) Data...")
    
    # 1. Bronze (JSON)
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix="raw/forex_js/")
    if 'Contents' in response:
        for obj in response['Contents']:
            old_key = obj['Key']
            match = re.search(r'snippet_(\d{4})_(\d{2})\.json', old_key)
            if match:
                year, month = match.groups()
                new_key = f"bronze/forex_factory/year={year}/month={month}/snippet.json"
                s3_client.copy_object(CopySource={'Bucket': bucket, 'Key': old_key}, Bucket=bucket, Key=new_key)
                logger.success(f"Migrated Bronze News: {year}-{month}")

    # 2. Silver (CSV to Parquet)
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix="processed/forex_csv/")
    if 'Contents' in response:
        for obj in response['Contents']:
            old_key = obj['Key']
            match = re.search(r'forex_data_(\d{4})_(\d{2})\.csv', old_key)
            if match:
                year, month = match.groups()
                new_key = f"silver/forex_calendar/year={year}/month={month}/data.parquet"
                
                csv_obj = s3_client.get_object(Bucket=bucket, Key=old_key)
                df = pd.read_csv(io.BytesIO(csv_obj['Body'].read()))
                
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
                s3_client.put_object(Bucket=bucket, Key=new_key, Body=parquet_buffer.getvalue())
                logger.success(f"Converted Silver News: {year}-{month}")

def migrate_dukascopy(s3_client, bucket: str):
    """
    Migrates Price data from raw/dukascopy/ to bronze and silver.
    """
    logger.info("Migrating Dukascopy (Price) Data...")
    
    # Source: raw/dukascopy/pair=EURUSD/year=2023/raw_4h.csv
    prefix = "raw/dukascopy/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' not in response:
        logger.warning("No Dukascopy files found to migrate.")
        return

    for obj in response['Contents']:
        old_key = obj['Key']
        # Regex to capture Pair and Year
        match = re.search(r'pair=(\w+)/year=(\d{4})/raw_4h\.csv', old_key)
        
        if match:
            pair, year = match.groups()
            
            # 1. Migrate to Bronze (Direct Copy)
            bronze_key = f"bronze/dukascopy/pair={pair}/year={year}/raw_4h.csv"
            s3_client.copy_object(CopySource={'Bucket': bucket, 'Key': old_key}, Bucket=bucket, Key=bronze_key)
            logger.success(f"Migrated Bronze Price: {pair} {year}")

            # 2. Migrate to Silver (CSV to Parquet)
            silver_key = f"silver/price_action/pair={pair}/year={year}/data_4h.parquet"
            try:
                csv_obj = s3_client.get_object(Bucket=bucket, Key=old_key)
                df = pd.read_csv(io.BytesIO(csv_obj['Body'].read()))
                
                # Standardize Silver Column Names
                df.columns = [col.lower() for col in df.columns]
                
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
                s3_client.put_object(Bucket=bucket, Key=silver_key, Body=parquet_buffer.getvalue())
                logger.success(f"Converted Silver Price: {pair} {year}")
            except Exception as e:
                logger.error(f"Failed Silver Price Migration for {pair}: {e}")

def main():
    bucket_name = "forex-datalake-bucket"
    s3_client = get_s3_client()
    
    logger.info(f"🔄 Starting Unified Medallion Migration: {bucket_name}")
    
    migrate_forex_factory(s3_client, bucket_name)
    migrate_dukascopy(s3_client, bucket_name)
    
    logger.info("🏁 Migration Complete. Run 'aws s3 ls --recursive' to verify.")

if __name__ == "__main__":
    main()