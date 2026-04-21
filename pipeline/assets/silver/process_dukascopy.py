"""
@bruin.asset
name: process_dukascopy
type: python
image: python:3.9
depends:
  - ingest_dukascopy
"""

import os
import io
import yaml
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from loguru import logger

def load_config() -> dict:
    """Loads the centralized pipeline configuration."""
    config_path = os.path.join(os.path.dirname(__file__), "../../../config/pipeline_config.yaml")
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def get_years_in_range(start_str: str, end_str: str) -> list:
    """Extracts unique years from the master config date range."""
    start_year = pd.to_datetime(start_str).year
    end_year = pd.to_datetime(end_str).year
    return list(range(start_year, end_year + 1))

def main():
    logger.info("Starting Silver Layer Transformation for Dukascopy...")
    config = load_config()
    
    bucket = config["aws"]["s3_bucket"]
    bronze_prefix = config["aws"]["bronze_prefix"]
    silver_prefix = config["aws"]["silver_prefix"]
    
    pairs = config["dukascopy"]["currencies"]
    timeframe = config["dukascopy"]["timeframe"]
    target_years = get_years_in_range(config["data_range"]["start_date"], config["data_range"]["end_date"])
    
    s3_client = boto3.client('s3', region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"))

    for pair in pairs:
        for year in target_years:
            # Exact Medallion Paths
            bronze_key = f"{bronze_prefix}dukascopy/pair={pair}/year={year}/raw_{timeframe}.csv"
            silver_key = f"{silver_prefix}price_action/pair={pair}/year={year}/data_{timeframe}.parquet"
            
            # --- IDEMPOTENCY CHECK ---
            try:
                s3_client.head_object(Bucket=bucket, Key=silver_key)
                logger.info(f"⏭️ Silver Parquet for {pair} ({year}) already exists. Skipping.")
                continue
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    logger.error(f"S3 Error checking {silver_key}: {e}")
                    continue
            
            # --- PROCESS MISSING DATA ---
            try:
                # 1. Download Bronze CSV into memory
                obj_response = s3_client.get_object(Bucket=bucket, Key=bronze_key)
                df = pd.read_csv(io.BytesIO(obj_response['Body'].read()))
                
                if df.empty:
                    logger.warning(f"Bronze file {bronze_key} is empty. Skipping.")
                    continue

                # 2. Standardize Columns (Lowercase, strip spaces)
                df.columns = [str(col).strip().lower() for col in df.columns]
                
                # 3. Ensure Timestamp is proper UTC DateTime
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                elif 'date' in df.columns: # Fallback if raw data named it differently
                    df.rename(columns={'date': 'timestamp'}, inplace=True)
                    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

                # Optional: Calculate basic volatility feature here
                if all(col in df.columns for col in ['high', 'low']):
                    df['candle_volatility'] = df['high'] - df['low']

                # 4. Upload to Silver Layer as Parquet
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
                
                s3_client.put_object(
                    Bucket=bucket, 
                    Key=silver_key, 
                    Body=parquet_buffer.getvalue()
                )
                logger.success(f"✅ Silver Layer: Converted and uploaded {pair} ({year}) to S3.")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    logger.warning(f"⚠️ Bronze file not found: {bronze_key}. Run ingest_dukascopy first.")
                else:
                    logger.error(f"S3 Error processing {bronze_key}: {e}")
            except Exception as e:
                logger.error(f"❌ Transformation failed for {pair} ({year}): {e}")

if __name__ == "__main__":
    main()