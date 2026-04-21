"""
@bruin.asset
name: ingest_dukascopy
type: python
image: python:3.9
depends: []
"""

import os
import yaml
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from loguru import logger
import io

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

def fetch_dukascopy_data(pair: str, year: int, timeframe: str) -> pd.DataFrame:
    """
    Placeholder for the actual Dukascopy extraction logic.
    Replace this with your specific API call or download logic.
    """
    logger.info(f"Downloading {pair} data for {year} at {timeframe} timeframe...")
    # TODO: Insert your specific extraction code here.
    # Example: df = dukascopy_api.get_data(pair, year, timeframe)
    
    # Mocking a DataFrame for structural completeness
    df = pd.DataFrame({
        "timestamp": pd.date_range(start=f"{year}-01-01", periods=5, freq="H"),
        "open": [1.10, 1.11, 1.10, 1.12, 1.11],
        "high": [1.12, 1.12, 1.11, 1.13, 1.12],
        "low": [1.09, 1.10, 1.09, 1.11, 1.10],
        "close": [1.11, 1.10, 1.12, 1.11, 1.11],
        "volume": [1000, 1200, 1100, 1500, 1300]
    })
    return df

def main():
    logger.info("Initializing Dukascopy Ingestion (Bronze Layer)...")
    config = load_config()
    
    bucket = config["aws"]["s3_bucket"]
    bronze_prefix = config["aws"]["bronze_prefix"]
    
    pairs = config["dukascopy"]["currencies"]
    timeframe = config["dukascopy"]["timeframe"]
    target_years = get_years_in_range(config["data_range"]["start_date"], config["data_range"]["end_date"])
    
    s3_client = boto3.client('s3', region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"))

    for pair in pairs:
        for year in target_years:
            # Bronze layer path: bronze/dukascopy/pair=EURUSD/year=2023/raw.csv
            s3_key = f"{bronze_prefix}dukascopy/pair={pair}/year={year}/raw_{timeframe}.csv"
            
            # --- IDEMPOTENCY CHECK ---
            try:
                s3_client.head_object(Bucket=bucket, Key=s3_key)
                logger.info(f"⏭️ Bronze data for {pair} ({year}) already exists in S3. Skipping.")
                continue
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    logger.error(f"S3 Error checking {s3_key}: {e}")
                    continue
            
            # --- FETCH AND UPLOAD ---
            try:
                # 1. Fetch the raw data
                raw_df = fetch_dukascopy_data(pair, year, timeframe)
                
                if raw_df.empty:
                    logger.warning(f"No data returned for {pair} in {year}. Skipping upload.")
                    continue
                
                # 2. Upload directly to S3 as CSV
                csv_buffer = io.StringIO()
                raw_df.to_csv(csv_buffer, index=False)
                
                s3_client.put_object(
                    Bucket=bucket, 
                    Key=s3_key, 
                    Body=csv_buffer.getvalue()
                )
                logger.success(f"✅ Bronze Layer: Uploaded {pair} ({year}) to S3.")
                
            except Exception as e:
                logger.error(f"❌ Failed to ingest {pair} for {year}: {e}")

if __name__ == "__main__":
    main()